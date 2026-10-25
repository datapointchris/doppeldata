"""Synthetic datasets generated from a profile: in memory, as a stream, or written to files with a manifest.

Tables and readers cross from the engine through the Arrow PyCapsule interface, so Polars, DuckDB and pyarrow 14 or later read
them with no copy, and none of them is needed to generate. `generate` and `write` release the GIL while the engine works, and
stop between blocks on Ctrl-C.
"""

import dataclasses as dc
import json
import os
import re
import shutil
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from typing import Protocol

from doppeldata import _core
from doppeldata.errors import StoppedWrite
from doppeldata.errors import WriteFailed
from doppeldata.errors import not_installed
from doppeldata.errors import uninstalled
from doppeldata.manifest import FAULTS_FILE_NAME
from doppeldata.manifest import MANIFEST_FILE_NAME
from doppeldata.manifest import Manifest
from doppeldata.name_hinted_types import with_type_hints
from doppeldata.profile_format import Issue
from doppeldata.profile_format import Profile
from doppeldata.profile_format import UngenerableProfile
from doppeldata.published_schemas import output_formats
from doppeldata.scenario_format import Scenario
from doppeldata.scenario_format import composed
from doppeldata.scenario_format import manifest_record
from doppeldata.scenario_format import refuse_file_shaping
from doppeldata.short_profiles import ProfileSource
from doppeldata.short_profiles import profile_from

LARGEST_SEED = 2**64 - 1


class ArrowStreamExportable(Protocol):
    """Data exported through the Arrow PyCapsule interface: `pl.DataFrame(data)`, `pa.table(data)`, or DuckDB by variable name."""

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...


@dc.dataclass(frozen=True)
class ColumnSampler:
    """How one top-level column is drawn. `physical_type` is spelled as a profile spells it, as pyarrow writes types."""

    name: str
    physical_type: str
    sampler: str
    null_share: float


@dc.dataclass(frozen=True)
class PlannedRange:
    """One row range a write would make, keyed as the manifest keys the files it becomes.

    A partitioned range becomes one file per partition value it holds, at most `objects_at_most`, so its `path` has `*` for each
    value, as in `status=*/part-00000-1f2e3d4c.parquet`.
    """

    path: str
    range: int
    first_row: int
    range_rows: int
    objects_at_most: int


@dc.dataclass(frozen=True)
class WritePlan:
    """What `write` would do with the same arguments, known before anything is generated.

    `identity` and `dataset_digest` are what the manifest will record. `rows_written` is the identity's `dataset_rows` and
    every copy a scenario's `duplicates` adds. `estimated_bytes` comes from the write's first blocks encoded with its own
    settings, and leaves the manifest out. The shape of `to_json` may change before 1.0.
    """

    destination: str
    identity: dict[str, Any]
    dataset_digest: str
    format: str
    writer: dict[str, Any]
    layout: dict[str, Any]
    columns: list[ColumnSampler]
    ranges: list[PlannedRange]
    files_at_most: int
    rows_written: int
    estimated_bytes: int

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True)
class WriteChoices:
    """What a write lays over the format, settings and layout its profile records. Each None keeps the profile's.

    `partition_by=[]` writes no partitions, where None keeps the profile's keys.
    """

    format: str | None = None
    file_rows: int | None = None
    partition_by: list[str] | None = None
    partition_style: str | None = None
    writer: dict[str, Any] | None = None

    def engine_json(self, profile: Profile, scenario: Scenario | None = None) -> str:
        chosen = {key: value for key, value in dc.asdict(self).items() if value is not None}
        if profile.path:
            chosen['profile_path'] = str(profile.path)
        if scenario is not None:
            chosen['scenario'] = manifest_record(scenario)
        return json.dumps(chosen)


# A URL a Path has collapsed, such as s3://bucket to s3:/bucket. A Windows drive's single letter does not match.
COLLAPSED_URL = re.compile(r'[A-Za-z][A-Za-z0-9+.-]+:/(?!/)')


def destination_of(out: str | os.PathLike[str]) -> tuple[str, str, str | None]:
    """`out` as the engine writes to it, with its scheme, and its bucket for S3. Anything the engine would refuse is refused here.

    A Path collapses `s3://bucket` to `s3:/bucket`, which would name a local directory, so a Path holding a URL is refused.
    """
    destination = os.fspath(out)
    if not isinstance(out, str) and COLLAPSED_URL.match(destination):
        message = f'{destination} is a Path, which collapses a URL\'s //; pass the URL as a str, such as "s3://bucket/prefix/"'
        raise WriteFailed('url-in-a-path', message, kind='refused')
    scheme, bucket = _core.destination_scheme(destination)
    # boto3 resolves the credentials an S3 write signs with.
    if scheme == 's3' and uninstalled({'boto3': 'boto3'}):
        raise WriteFailed('boto3-missing', not_installed('writing to s3://', ['boto3']), kind='refused')
    return destination, scheme, bucket


# The rows a write to one file asks each file to hold, so its layout makes one range of every row.
EVERY_ROW = 2**63 - 1
# Ends the name of the directory beside a one-file write's target that the engine writes into.
STAGING_SUFFIX = '.doppeldata-staging'


def one_file_of(out: str | os.PathLike[str]) -> Path | None:
    """The file `out` names where it is a local path whose suffix is a format's name, else None. The path alone decides, and
    a trailing `/` names a directory whatever its suffix."""
    destination = os.fspath(out)
    if '://' in destination or COLLAPSED_URL.match(destination) or destination.endswith(('/', os.sep)):
        return None
    path = Path(destination)
    return path if path.suffix.lower().removeprefix('.') in output_formats() else None


def one_file_choices(choices: WriteChoices, target: Path) -> WriteChoices:
    """`choices` for a write of the one file `target`: the format its suffix names, every row in one range, and no partitions."""
    named = target.suffix.lower().removeprefix('.')
    if choices.format not in (None, named):
        message = f'{target.name} names the {named} format, not {choices.format}; name the file for its format, or write to a directory'
        raise WriteFailed('suffix-names-another-format', message, kind='refused')
    held = [name for name, value in (('partition_by', choices.partition_by), ('file_rows', choices.file_rows)) if value]
    if held:
        message = f'{target.name} is one file holding every row, so it takes no {" or ".join(held)}; write to a directory for those'
        raise WriteFailed('one-file-holds-every-row', message, kind='refused')
    return dc.replace(choices, format=named, file_rows=EVERY_ROW, partition_by=[])


def refuse_other_extension(target: Path, ranges_json: str) -> None:
    """Refuse writer settings whose file `target`'s suffix does not name, as gzip makes `csv.gz` and an IPC stream `arrows`."""
    named = target.suffix.lower().removeprefix('.')
    for planned in json.loads(ranges_json):
        written = planned['path'].partition('.')[2]
        if written != named:
            message = f'{target.name} names a .{named} file, and the writer settings given write .{written}; write to a directory for those'
            raise WriteFailed('suffix-names-another-format', message, kind='refused')


def one_file_scenario(scenario: Scenario | Sequence[Scenario] | None) -> Scenario | None:
    """The scenario `scenario=` names, refused where it acts across several files, which a write to one file cannot."""
    applied = composed(scenario)
    if applied is not None:
        refuse_file_shaping(applied, 'a write to one file', cannot='makes only one', across_files=True)
    return applied


def write_one_file(prepared: 'PreparedWrite', target: Path, *, threads: int, replace: bool, manifest: bool) -> Manifest:
    """Write the dataset as the one file `target`, and `_manifest.json` beside it only where `manifest` asks.

    The engine writes into `.{target.name}.doppeldata-staging` beside `target`, and the file is moved into place whole. A
    write that fails or that a signal stops removes that directory. One killed outright leaves it, and the next write to
    `target` holds it as it holds the file: refused, or removed under `replace`. A manifest beside `target` recording it
    alone describes the rows this write replaces, so it goes before the file moves, whether or not `manifest` writes
    another. One recording other files makes that directory their dataset, so it refuses the write. The manifest returned
    names `target`'s directory as its destination, and `target` as its one file. No rows write no file, as they write none
    in a directory. Under `malformed_rows` the write lists its faults in `_faults.ndjson` beside `target` every time, and
    one already there is held as the manifest is.
    """
    if target.is_dir():
        message = f'{target} is a directory; end the path with / to write the dataset into it'
        raise WriteFailed('destination-is-a-directory', message, kind='refused')
    recorded = target.parent / MANIFEST_FILE_NAME
    listed = [file['path'] for file in Manifest.from_json(recorded.read_text()).files] if recorded.is_file() else None
    faults_recorded = target.parent / FAULTS_FILE_NAME
    faulted = fault_files(faults_recorded) if faults_recorded.is_file() else None
    for sidecar, names in ((recorded, listed), (faults_recorded, faulted)):
        others = sorted(set(names or ()) - {target.name})
        if others:
            message = (
                f'{sidecar} records {", ".join(others)} as its dataset, so {target.name} cannot join it; write it to another directory'
            )
            raise WriteFailed('manifest-records-another-file', message, kind='refused')
    staging = target.parent / f'.{target.name}{STAGING_SUFFIX}'
    held = [
        (f'{target} already exists', target.exists()),
        (f'{recorded} already exists', listed is not None),
        (f'{faults_recorded} already exists', faulted is not None),
        (f'{staging} holds a write to {target.name} that was killed or is still running', staging.exists()),
    ]
    refusals = [message for message, holds in held if holds]
    if refusals and not replace:
        raise WriteFailed('destination-not-empty', refusals[0], kind='refused')
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.rmtree(staging, ignore_errors=True)
        staging.mkdir()
    except OSError as error:
        raise unwritable(target, error) from error
    try:
        written = Manifest.from_json(prepared.engine.write(prepared.options, str(staging), threads=threads, replace=False))
        files = [{**file, 'path': target.name} for file in written.files]
        moved = Manifest({**written.document, 'destination': str(target.parent), 'files': files})
        if manifest:
            (staging / MANIFEST_FILE_NAME).write_text(json.dumps(moved.document, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        staged_faults = staging / FAULTS_FILE_NAME
        if staged_faults.is_file():
            renamed = [{**json.loads(line), 'file': target.name} for line in staged_faults.read_text(encoding='utf-8').splitlines()]
            lines = ''.join(json.dumps(fault, ensure_ascii=False, separators=(',', ':')) + '\n' for fault in renamed)
            staged_faults.write_text(lines, encoding='utf-8')
        if listed is not None:
            recorded.unlink()
        if faulted is not None:
            faults_recorded.unlink()
        for file in written.files:
            os.replace(staging / file['path'], target)
        if not files:
            target.unlink(missing_ok=True)
        if manifest:
            os.replace(staging / MANIFEST_FILE_NAME, recorded)
        if staged_faults.is_file():
            os.replace(staged_faults, faults_recorded)
    except StoppedWrite as error:
        # The partial manifest was in the staging directory, which goes with everything else the write made.
        error.manifest_stored = False
        raise
    except OSError as error:
        raise unwritable(target, error) from error
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return moved


def fault_files(path: Path) -> list[str]:
    """Each file a `_faults.ndjson` names."""
    return sorted({json.loads(line)['file'] for line in path.read_text(encoding='utf-8').splitlines() if line})


def unwritable(target: Path, error: OSError) -> WriteFailed:
    """A one-file write's failure on the local filesystem, coded as the writers code a directory write's."""
    code = 'store-refused' if isinstance(error, PermissionError) else 'io'
    return WriteFailed(code, f'{target} could not be written: {error.strerror or error}', kind='failed')


@dc.dataclass(frozen=True)
class EngineArguments:
    """Everything the engine's plan is built from, so a process holding only these, such as a Spark executor, builds the same plan.

    `profile_json` is the profile as given, and the engine applies the scenario's patch to it. `scenario` is the scenario's
    JSON, digest and source, or None.
    """

    profile_json: str
    profile_digest: str
    seed: int
    rows: int
    source: str
    scenario: tuple[str, str, str] | None = None

    def plan(self) -> '_core.Plan':
        return _core.Plan(self.profile_json, self.profile_digest, self.seed, self.rows, self.source, scenario=self.scenario)


def engine_arguments(source_profile: ProfileSource, rows: int | None, seed: int, scenario: Scenario | None = None) -> EngineArguments:
    """The engine's arguments for `rows` rows of a profile under `scenario` from `seed`, refusing a patch the profile cannot
    take. No `rows` takes the row count the profile records, as the scenario's patch leaves it.

    The engine reads the profile with each typed text column that has no format of its own given its type's generator hint.
    The digest is the profile's as given.
    """
    profile = profile_from(source_profile)
    source = str(profile.path) if profile.path else f'profile {profile.document.get("name")!r}'
    effective = scenario.apply(profile) if scenario is not None else profile
    if rows is None:
        rows = (effective.document.get('dataset') or {}).get('row_count')
        if rows is None:
            issue = Issue('no-row-count', '/dataset/row_count', 'the profile records no row count, so rows must be given')
            raise UngenerableProfile(source, [issue])
    if rows < 0:
        raise ValueError(f'rows must be zero or more, not {rows}')
    if not 0 <= seed <= LARGEST_SEED:
        raise ValueError(f'seed must be between 0 and 2^64 - 1, not {seed}')
    applied = (json.dumps(scenario.to_plain(), allow_nan=False), scenario.digest, scenario.source) if scenario is not None else None
    profile_json = json.dumps(with_type_hints(profile.to_plain()), allow_nan=False)
    return EngineArguments(profile_json, profile.digest, seed, rows, source, applied)


def engine_plan(profile: ProfileSource, rows: int | None, seed: int, scenario: Scenario | None = None) -> '_core.Plan':
    """The engine's plan for `rows` rows of `profile` under `scenario` from `seed`."""
    return engine_arguments(profile, rows, seed, scenario).plan()


def rows_only(scenario: Scenario | Sequence[Scenario] | None, run: str, write_with: str) -> Scenario | None:
    """The scenario `scenario=` names, refused where it shapes files, because `run` writes none and `write_with` does."""
    applied = composed(scenario)
    if applied is not None:
        refuse_file_shaping(applied, run, write_with=write_with)
    return applied


@dc.dataclass(frozen=True)
class PreparedWrite:
    """A write's destination and the engine's options for it, resolved from the arguments `plan` and `write` take.

    `one_file` is the file a write to one local file makes, and `destination` is then its directory.
    """

    destination: str
    scheme: str
    bucket: str | None
    arguments: EngineArguments
    engine: '_core.Plan'
    choices_json: str
    options: '_core.WriteOptions'
    one_file: Path | None = None


def prepared_write(
    source: ProfileSource,
    out: str | os.PathLike[str],
    rows: int | None,
    seed: int,
    choices: WriteChoices,
    scenario: Scenario | Sequence[Scenario] | None = None,
) -> PreparedWrite:
    """What a write of `rows` rows of a profile under `scenario` from `seed` to `out` with `choices` runs on, refusing what the
    engine would refuse. A local `out` naming one file is resolved here, so every write and plan reads the one answer."""
    one_file = one_file_of(out)
    if one_file is None:
        applied = composed(scenario)
    else:
        out, choices, applied = one_file.parent, one_file_choices(choices, one_file), one_file_scenario(scenario)
    destination, scheme, bucket = destination_of(out)
    profile = profile_from(source)
    arguments = engine_arguments(profile, rows, seed, applied)
    engine = arguments.plan()
    choices_json = choices.engine_json(profile, applied)
    options = engine.write_options(choices_json)
    if one_file is not None:
        refuse_other_extension(one_file, engine.ranges_json(options))
    return PreparedWrite(destination, scheme, bucket, arguments, engine, choices_json, options, one_file)


def s3_store_of(destination: str, bucket: str | None) -> dict[str, Any] | None:
    """The S3 store's settings for `destination`, from boto3's chain in this process, or None for a destination not on S3."""
    if bucket is None:
        return None
    from doppeldata.s3_access import s3_store_settings

    return s3_store_settings(destination, bucket)


def generate(
    profile: ProfileSource,
    *,
    rows: int | None = None,
    seed: int = 0,
    threads: int = 0,
    scenario: Scenario | Sequence[Scenario] | None = None,
) -> ArrowStreamExportable:
    """Every row of the dataset, as one Arrow table in memory. `threads` of zero takes one per core.

    `profile` is a Profile, or anything `Profile.expand` takes: a list of column names, a dict of columns such as
    `{'email': sd.fakes.email, 'age': sd.integers(18, 90)}`, a short or full YAML file, or an example's name.

    The values are a pure function of the profile, the scenario, `seed` and `rows`, whatever `threads` is. `scenario` is a
    Scenario or a list composed in order. One that shapes files, as schema_drift, small_files and malformed_rows do, is
    refused as InvalidScenario, because nothing here writes files. `rows` counts the rows generated, so a scenario's
    `duplicates` returns more.
    """
    applied = rows_only(scenario, 'generate', 'doppeldata.write')
    table: ArrowStreamExportable = engine_plan(profile, rows, seed, applied).generate(threads=threads)
    return table


def stream(
    profile: ProfileSource,
    *,
    rows: int | None = None,
    seed: int = 0,
    threads: int = 0,
    scenario: Scenario | Sequence[Scenario] | None = None,
) -> ArrowStreamExportable:
    """The dataset as an Arrow RecordBatchReader, one batch per engine block of `_core.BLOCK_ROWS` rows, holding at most two
    batches per thread.

    It reads once. pyarrow 15 or later takes it as `pa.RecordBatchReader.from_stream(reader)`. Its threads check for no signal,
    so Ctrl-C reaches it only when its consumer returns to Python between batches. `profile` and `scenario` are taken as
    `generate` takes them.
    """
    applied = rows_only(scenario, 'stream', 'doppeldata.write')
    reader: ArrowStreamExportable = engine_plan(profile, rows, seed, applied).stream(threads=threads)
    return reader


def plan(
    profile: ProfileSource,
    out: str | os.PathLike[str],
    *,
    rows: int | None = None,
    seed: int = 0,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: list[str] | None = None,
    partition_style: str | None = None,
    writer: dict[str, Any] | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
) -> WritePlan:
    """What `write` would write with the same arguments: the identity, the format and settings, every range and each column's sampler.

    It reads nothing at `out`, and refuses what `write` would refuse from its arguments alone.
    """
    choices = WriteChoices(format, file_rows, partition_by, partition_style, writer)
    prepared = prepared_write(profile, out, rows, seed, choices, scenario)
    engine, options, target = prepared.engine, prepared.options, prepared.one_file
    settings = json.loads(options.settings_json)
    ranges = [PlannedRange(**planned) for planned in json.loads(engine.ranges_json(options))]
    if target is not None:
        ranges = [dc.replace(planned, path=target.name) for planned in ranges]
    return WritePlan(
        destination=prepared.destination,
        identity=json.loads(engine.identity_json),
        dataset_digest=engine.dataset_digest,
        format=settings['format'],
        writer=settings['writer'],
        layout=settings['layout'],
        columns=[ColumnSampler(*column) for column in engine.column_samplers()],
        ranges=ranges,
        files_at_most=sum(planned.objects_at_most for planned in ranges),
        rows_written=engine.rows_written,
        estimated_bytes=engine.estimate_bytes(options),
    )


def write(
    profile: ProfileSource,
    out: str | os.PathLike[str],
    *,
    rows: int | None = None,
    seed: int = 0,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: list[str] | None = None,
    partition_style: str | None = None,
    writer: dict[str, Any] | None = None,
    threads: int = 0,
    replace: bool = False,
    scenario: Scenario | Sequence[Scenario] | None = None,
    manifest: bool = False,
) -> Manifest:
    """Write the dataset to `out` and return its manifest. `profile` is taken as `generate` takes it.

    `out` is a local directory, an `s3://` or `s3a://` URL, or `memory://`, which keeps nothing but the manifest. Each holds
    `_manifest.json` beside the data, which claims the destination and records a write that stops. The format, its settings
    and the layout start from what the profile records, and each argument given is laid over them. `partition_by=[]` writes
    no partitions. A destination holding anything is refused unless `replace` is set, which deletes only what an earlier
    write recorded. S3 takes its credentials, region and endpoint from the AWS environment through boto3.

    A local path ending in a format's suffix, such as `people.csv`, is written as that one file in that format, and one
    ending in `/` as a directory. It gets `_manifest.json` beside it only where `manifest` is set, and `replace` writes over
    either. A directory holding another dataset's manifest refuses it.

    A write that fails raises WriteFailed, and one a signal stops, such as Ctrl-C, raises WriteInterrupted. Both are a
    StoppedWrite. Once a write to a directory has claimed it, either leaves `_manifest.partial.json` listing the files it
    completed. A write to one file that stops leaves nothing, unless it is killed outright, which leaves its staging
    directory beside the file for `replace` to remove.

    `scenario`, a Scenario or a list composed in order, bends the data: its patch applies to the profile, and its perturbations
    to the rows and the files. The manifest records it, and a layout that cannot hold it raises InvalidScenario. `rows` counts
    the rows generated, so a scenario's `duplicates` writes more.
    """
    choices = WriteChoices(format, file_rows, partition_by, partition_style, writer)
    prepared = prepared_write(profile, out, rows, seed, choices, scenario)
    if prepared.one_file is not None:
        return write_one_file(prepared, prepared.one_file, threads=threads, replace=replace, manifest=manifest)
    s3_store = s3_store_of(prepared.destination, prepared.bucket)
    written = prepared.engine.write(prepared.options, prepared.destination, threads=threads, replace=replace, s3_store=s3_store)
    return Manifest.from_json(written)
