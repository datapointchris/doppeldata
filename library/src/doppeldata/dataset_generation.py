"""One table's synthetic dataset generated from its profile: in memory, as a stream, or written to files with a manifest.

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

import rfc8785

from doppeldata import _core
from doppeldata import issue_codes
from doppeldata.column_overrides import Overrides
from doppeldata.column_overrides import overrides_json
from doppeldata.embedded_profiles import WrittenDataFile
from doppeldata.embedded_profiles import data_file_metadata
from doppeldata.embedded_profiles import written_data_file
from doppeldata.errors import InvalidArgument
from doppeldata.errors import SourceError
from doppeldata.errors import StoppedWrite
from doppeldata.errors import WriteFailed
from doppeldata.errors import not_installed
from doppeldata.errors import uninstalled
from doppeldata.issue_codes import Issue
from doppeldata.manifest import FAULTS_FILE_NAME
from doppeldata.manifest import MANIFEST_FILE_NAME
from doppeldata.manifest import Manifest
from doppeldata.name_hinted_types import with_type_hints
from doppeldata.privacy_policy.file_check import refuse_failed_check
from doppeldata.profile_format import Profile
from doppeldata.profile_format import UngenerableProfile
from doppeldata.profile_sources import ProfileSource
from doppeldata.profile_sources import profile_from
from doppeldata.published_schemas import output_formats
from doppeldata.published_schemas import partition_styles
from doppeldata.scenario_format import InvalidScenario
from doppeldata.scenario_format import Scenario
from doppeldata.scenario_format import composed
from doppeldata.scenario_format import manifest_record
from doppeldata.scenario_format import refuse_file_shaping

LARGEST_SEED = 2**64 - 1


class ArrowStreamExportable(Protocol):
    """Data exported through the Arrow PyCapsule interface: `pl.DataFrame(data)`, `pa.table(data)`, or DuckDB by variable name."""

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...


@dc.dataclass(frozen=True)
class SamplerParameter:
    """One profile field a column's sampler reads: the parameter it sets, the field's JSON pointer, and its value."""

    name: str
    field: str
    value: Any


@dc.dataclass(frozen=True)
class ColumnTrace:
    """How one top-level column is drawn, and what in the profile and the scenario decides it.

    `physical_type` is spelled as a profile spells it. `sampler_kind` is the sampler's kind, such as `numeric`, and `summary`
    says what draws the values in a few words. `parameters` holds each field the sampler reads that the profile holds. `origin` is
    `profiled`, `derived` from other columns, `overlaid` by the scenario, or `pinned` by an override. `reads` names the
    columns a derived column follows, `ops` the id of each scenario op writing it, and `patched_fields` each field the
    scenario's patch sets for it.
    """

    name: str
    physical_type: str
    sampler_kind: str
    summary: str
    parameters: list[SamplerParameter]
    null_fraction: float
    origin: str
    reads: list[str]
    ops: list[str]
    patched_fields: list[str]

    @classmethod
    def from_json(cls, plain: dict[str, Any]) -> 'ColumnTrace':
        return cls(**plain | {'parameters': [SamplerParameter(**parameter) for parameter in plain['parameters']]})


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
    settings, and leaves the manifest out. Under a scenario's `cdc`, `ranges` are the full-load files, `cdc` is the stream
    beside them as the manifest's `cdc` will describe it, and `files_at_most` and `estimated_bytes` count the change files
    and the expected table too. The shape of `to_json` may change before 1.0.
    """

    destination: str
    identity: dict[str, Any]
    dataset_digest: str
    format: str
    writer: dict[str, Any]
    layout: dict[str, Any]
    columns: list[ColumnTrace]
    ranges: list[PlannedRange]
    files_at_most: int
    rows_written: int
    estimated_bytes: int
    cdc: dict[str, Any] | None = None

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True)
class WriteChoices:
    """What a write lays over the format, settings and layout its profile records. Each None keeps the profile's.

    `partition_by=[]` writes no partitions, where None keeps the profile's keys. `embed_profile` writes the whole profile's
    canonical JSON, and the scenario's document, into each Parquet or Arrow IPC file's metadata.
    """

    format: str | None = None
    file_rows: int | None = None
    partition_by: list[str] | None = None
    partition_style: str | None = None
    writer: dict[str, Any] | None = None
    embed_profile: bool = False

    def __post_init__(self) -> None:
        if self.format is not None and self.format not in output_formats():
            message = f'{self.format!r} is not a format doppeldata writes'
            raise InvalidArgument(issue_codes.UNKNOWN_FORMAT, message, field='format', value=self.format, valid=output_formats())
        if self.partition_style is not None and self.partition_style not in partition_styles():
            message = f'{self.partition_style!r} is not a partition style doppeldata writes'
            valid = partition_styles()
            raise InvalidArgument(
                issue_codes.UNKNOWN_PARTITION_STYLE, message, field='partition_style', value=self.partition_style, valid=valid
            )

    def engine_json(self, profile: Profile, scenario: Scenario | None = None) -> str:
        chosen = {key: value for key, value in dc.asdict(self).items() if value is not None and key != 'embed_profile'}
        if self.embed_profile:
            chosen['embedded_profile'] = rfc8785.dumps(profile.to_plain()).decode()
            if scenario is not None:
                chosen['embedded_scenario'] = scenario.canonical().decode()
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
        raise WriteFailed(issue_codes.URL_IN_A_PATH, message)
    scheme, bucket = _core.destination_scheme(destination)
    # boto3 resolves the credentials an S3 write signs with.
    if scheme == 's3' and uninstalled({'boto3': 'boto3'}):
        raise WriteFailed(issue_codes.PACKAGES_MISSING, not_installed('writing to s3://', ['boto3']))
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
        raise WriteFailed(issue_codes.SUFFIX_NAMES_ANOTHER_FORMAT, message)
    held = [name for name, value in (('partition_by', choices.partition_by), ('file_rows', choices.file_rows)) if value]
    if held:
        message = f'{target.name} is one file holding every row, so it takes no {" or ".join(held)}; write to a directory for those'
        raise WriteFailed(issue_codes.ONE_FILE_HOLDS_EVERY_ROW, message)
    return dc.replace(choices, format=named, file_rows=EVERY_ROW, partition_by=[])


def refuse_other_extension(target: Path, ranges_json: str) -> None:
    """Refuse writer settings whose file `target`'s suffix does not name, as gzip makes `csv.gz` and an IPC stream `arrows`."""
    named = target.suffix.lower().removeprefix('.')
    for planned in json.loads(ranges_json):
        written = planned['path'].partition('.')[2]
        if written != named:
            message = f'{target.name} names a .{named} file, and the writer settings given write .{written}; write to a directory for those'
            raise WriteFailed(issue_codes.SUFFIX_NAMES_ANOTHER_FORMAT, message)


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
    in a directory. Under `malformed_rows` the write lists its faults beside `target` every time, in the list
    `_core.one_file_faults_name` names for it, such as `_orders.csv.faults.jsonl`. One already there describes the bytes
    this write replaces, so it is held as the file is.
    """
    if target.is_dir():
        message = f'{target} is a directory; end the path with / to write the dataset into it'
        raise WriteFailed(issue_codes.DESTINATION_IS_A_DIRECTORY, message)
    recorded = target.parent / MANIFEST_FILE_NAME
    listed = [file['path'] for file in Manifest.from_json(recorded.read_text()).files] if recorded.is_file() else None
    others = sorted(set(listed or ()) - {target.name})
    if others:
        message = f'{recorded} records {", ".join(others)} as its dataset, so {target.name} cannot join it; write it to another directory'
        raise WriteFailed(issue_codes.MANIFEST_RECORDS_ANOTHER_FILE, message)
    staging = target.parent / f'.{target.name}{STAGING_SUFFIX}'
    # Each record the engine leaves beside the data, as it is staged and where it lands beside `target`.
    sidecars = [
        (staging / MANIFEST_FILE_NAME, recorded),
        (staging / FAULTS_FILE_NAME, target.parent / _core.one_file_faults_name(target.name)),
    ]
    held = [
        (f'{target} already exists', target.exists()),
        *((f'{beside} already exists', beside.is_file()) for _, beside in sidecars),
        (f'{staging} holds a write to {target.name} that was killed or is still running', staging.exists()),
    ]
    refusals = [message for message, holds in held if holds]
    if refusals and not replace:
        raise WriteFailed(issue_codes.DESTINATION_NOT_EMPTY, refusals[0])
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
        else:
            (staging / MANIFEST_FILE_NAME).unlink(missing_ok=True)
        staged_faults = staging / FAULTS_FILE_NAME
        if staged_faults.is_file():
            renamed = [{**json.loads(line), 'file': target.name} for line in staged_faults.read_text(encoding='utf-8').splitlines()]
            lines = ''.join(json.dumps(fault, ensure_ascii=False, separators=(',', ':')) + '\n' for fault in renamed)
            staged_faults.write_text(lines, encoding='utf-8')
        # An old record beside `target` describes the bytes this write replaces, so it goes before they do.
        for _, beside in sidecars:
            beside.unlink(missing_ok=True)
        for file in written.files:
            os.replace(staging / file['path'], target)
        if not files:
            target.unlink(missing_ok=True)
        for staged, beside in sidecars:
            if staged.is_file():
                os.replace(staged, beside)
    except StoppedWrite as error:
        # The partial manifest was in the staging directory, which goes with everything else the write made.
        error.manifest_stored = False
        raise
    except OSError as error:
        raise unwritable(target, error) from error
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return moved


def unwritable(target: Path, error: OSError) -> WriteFailed:
    """A one-file write's failure on the local filesystem, coded as the writers code a directory write's."""
    code = issue_codes.STORE_REFUSED if isinstance(error, PermissionError) else issue_codes.LOCAL_IO
    return WriteFailed(code, f'{target} could not be written: {error.strerror or error}')


@dc.dataclass(frozen=True)
class GenerationSource:
    """The profile, rows, seed and scenario a call generates from. A Parquet or Arrow IPC file holding a profile gives each of
    the three its caller left None, as the file's write recorded it.

    `written` is that file. `regenerates` holds where the caller gave none of the three, so the plan must make the very
    dataset the file names.
    """

    profile: ProfileSource
    rows: int | None
    seed: int
    scenario: Scenario | Sequence[Scenario] | None
    written: WrittenDataFile | None = None
    regenerates: bool = False

    def checked(self, engine: '_core.Plan') -> '_core.Plan':
        """`engine`, refused as SourceError where it plans a dataset other than the one the file it regenerates names."""
        if self.written is None or not self.regenerates:
            return engine
        recorded = self.written.metadata.get(_core.DATASET_DIGEST_KEY)
        if engine.dataset_digest == recorded:
            return engine
        differing = self.written.identity_differences(json.loads(engine.identity_json)) or [_core.DATASET_DIGEST_KEY]
        message = (
            f'{self.written.path} names dataset {recorded}, but this release plans {engine.dataset_digest} from it, '
            f'because its {", ".join(differing)} differs; give a seed, rows or a scenario to generate another dataset from it'
        )
        raise SourceError(issue_codes.DATASET_IDENTITY_CHANGED, message, role='profile', value=recorded)


def generation_source(
    profile: ProfileSource, rows: int | None, seed: int | None, scenario: Scenario | Sequence[Scenario] | None
) -> GenerationSource:
    """The source each call that generates reads `profile` and the three values as, with a data file's write filling in any
    value left None. A seed left None is 0 for any other source."""
    written = None
    if isinstance(profile, str | os.PathLike) and Path(profile).is_file():
        metadata = data_file_metadata(profile)
        written = written_data_file(profile, metadata) if metadata is not None else None
    if written is None:
        return GenerationSource(profile, rows, 0 if seed is None else seed, scenario)
    return GenerationSource(
        written.profile,
        written.recorded(_core.DATASET_ROWS_KEY) if rows is None else rows,
        (written.recorded(_core.SEED_KEY) or 0) if seed is None else seed,
        written.scenario if scenario is None else scenario,
        written,
        regenerates=rows is None and seed is None and scenario is None,
    )


@dc.dataclass(frozen=True)
class EngineArguments:
    """Everything the engine's plan is built from, so a process holding only these, such as a Spark executor, builds the same plan.

    `profile_json` is the profile as given, and the engine applies the scenario's patch to it. `scenario` is the scenario's
    JSON, digest and source, or None. `overrides` is the JSON of each column `sample` pins and its values, or None.
    """

    profile_json: str
    profile_digest: str
    seed: int
    rows: int
    source: str
    scenario: tuple[str, str, str] | None = None
    overrides: str | None = None

    def plan(self) -> '_core.Plan':
        try:
            return _core.Plan(
                self.profile_json, self.profile_digest, self.seed, self.rows, self.source, scenario=self.scenario, overrides=self.overrides
            )
        except InvalidScenario as error:
            if self.scenario is None:
                raise
            columns = [column['name'] for column in json.loads(self.profile_json).get('columns') or []]
            issues = [named_columns(issue, json.loads(self.scenario[0]), columns) for issue in error.issues]
            raise InvalidScenario(error.source, issues) from None


def named_columns(issue: Issue, scenario: Any, columns: list[str]) -> Issue:
    """`issue` naming the column the scenario gave at its field, and the profile's nearest, where it names an unknown column."""
    if issue.code != issue_codes.UNKNOWN_SCENARIO_COLUMN or not issue.field:
        return issue
    given: Any = scenario
    for part in issue.field.split('/')[1:]:
        part = part.replace('~1', '/').replace('~0', '~')
        given = given[int(part)] if isinstance(given, list) else given.get(part)
    if not isinstance(given, str):
        return issue
    return dc.replace(issue, value=given, valid=tuple(columns))


def engine_arguments(
    source_profile: ProfileSource, rows: int | None, seed: int, scenario: Scenario | None = None, overrides: Overrides | None = None
) -> EngineArguments:
    """The engine's arguments for `rows` rows of a profile under `scenario` from `seed`, with each column `overrides` names
    pinned, refusing a patch the profile cannot take. No `rows` takes the row count the profile records, as the scenario's
    patch leaves it.

    The engine reads the profile with each typed text column that has no format of its own given its type's generator hint.
    The digest is the profile's as given.
    """
    profile = profile_from(source_profile)
    source = str(profile.path) if profile.path else f'profile {profile.document.get("name")!r}'
    effective = scenario.apply(profile) if scenario is not None else profile
    if rows is None:
        rows = (effective.document.get('dataset') or {}).get('rows')
        if rows is None:
            issue = Issue(issue_codes.NO_ROWS, '/dataset/rows', 'the profile records no row count, so rows must be given')
            raise UngenerableProfile(source, [issue])
    if rows < 0:
        raise InvalidArgument(issue_codes.INVALID_ROWS, f'rows must be zero or more, not {rows}', field='rows', value=rows)
    if not 0 <= seed <= LARGEST_SEED:
        raise InvalidArgument(issue_codes.INVALID_SEED, f'seed must be between 0 and 2^64 - 1, not {seed}', field='seed', value=seed)
    applied = (json.dumps(scenario.to_plain(), allow_nan=False), scenario.digest, scenario.source) if scenario is not None else None
    profile_json = json.dumps(with_type_hints(profile.to_plain()), allow_nan=False)
    return EngineArguments(profile_json, profile.digest, seed, rows, source, applied, overrides_json(overrides))


def engine_plan(
    profile: ProfileSource, rows: int | None, seed: int, scenario: Scenario | None = None, overrides: Overrides | None = None
) -> '_core.Plan':
    """The engine's plan for `rows` rows of `profile` under `scenario` from `seed`, each column `overrides` names pinned."""
    return engine_arguments(profile, rows, seed, scenario, overrides).plan()


def rows_only(scenario: Scenario | Sequence[Scenario] | None, run: str, write_with: str) -> Scenario | None:
    """The scenario `scenario=` names, refused where it shapes files, because `run` writes none and `write_with` does."""
    applied = composed(scenario)
    if applied is not None:
        refuse_file_shaping(applied, run, write_with=write_with)
    return applied


def rows_plan(
    profile: ProfileSource,
    rows: int | None,
    seed: int | None,
    scenario: Scenario | Sequence[Scenario] | None,
    run: str,
    overrides: Overrides | None = None,
) -> '_core.Plan':
    """The plan `run`, which returns rows and writes no file, generates from, each column `overrides` names pinned."""
    generation = generation_source(profile, rows, seed, scenario)
    applied = rows_only(generation.scenario, run, 'doppeldata.write')
    return generation.checked(engine_plan(generation.profile, generation.rows, generation.seed, applied, overrides))


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
    seed: int | None,
    choices: WriteChoices,
    scenario: Scenario | Sequence[Scenario] | None = None,
) -> PreparedWrite:
    """What a write of `rows` rows of a profile under `scenario` from `seed` to `out` with `choices` runs on, refusing what the
    engine would refuse. A local `out` naming one file is resolved here, so every write and plan reads the one answer."""
    generation = generation_source(source, rows, seed, scenario)
    one_file = one_file_of(out)
    if one_file is None:
        applied = composed(generation.scenario)
    else:
        out, choices, applied = one_file.parent, one_file_choices(choices, one_file), one_file_scenario(generation.scenario)
    destination, scheme, bucket = destination_of(out)
    profile = profile_from(generation.profile)
    if choices.embed_profile:
        refuse_failed_check(profile)
    arguments = engine_arguments(profile, generation.rows, generation.seed, applied)
    engine = generation.checked(arguments.plan())
    choices_json = choices.engine_json(profile, applied)
    options = engine.write_options(choices_json)
    engine.check_writable(options)
    if one_file is not None:
        refuse_other_extension(one_file, engine.ranges_json(options))
    return PreparedWrite(destination, scheme, bucket, arguments, engine, choices_json, options, one_file)


def s3_store_of(destination: str, bucket: str | None) -> dict[str, Any] | None:
    """The S3 store's settings for `destination`, from boto3's chain in this process, or None for a destination not on S3."""
    if bucket is None:
        return None
    from doppeldata.s3_access import s3_store_settings

    return s3_store_settings(destination, bucket)


def write_plan_of(engine: '_core.Plan', options: '_core.WriteOptions', destination: str, target: Path | None = None) -> WritePlan:
    """What writing `engine`'s dataset with `options` to `destination` would do, as the one file `target` where given."""
    settings = json.loads(engine.written_settings_json(options))
    ranges = [PlannedRange(**planned) for planned in json.loads(engine.ranges_json(options))]
    if target is not None:
        ranges = [dc.replace(planned, path=target.name) for planned in ranges]
    stream_json = engine.stream_json(options)
    stream = json.loads(stream_json) if stream_json is not None else None
    stream_files = stream['change_files_at_most'] + stream['expected_parts'] if stream is not None else 0
    return WritePlan(
        destination=destination,
        identity=json.loads(engine.identity_json),
        dataset_digest=engine.dataset_digest,
        format=settings['format'],
        writer=settings['writer'],
        layout=settings['layout'],
        columns=[ColumnTrace.from_json(column) for column in json.loads(engine.column_traces_json())],
        ranges=ranges,
        files_at_most=sum(planned.objects_at_most for planned in ranges) + stream_files,
        rows_written=engine.rows_written,
        estimated_bytes=engine.estimate_bytes(options),
        cdc=stream,
    )
