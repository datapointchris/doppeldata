"""The public `generate`, `sample`, `stream`, `plan`, `write` and `regenerate`, each taking one table's profile or a Bundle,
above the modules that generate each."""

import logging
import os
import time
from collections.abc import Sequence
from typing import Any
from typing import overload

from doppeldata import issue_codes
from doppeldata.aws_sessions import aws_profile_chosen
from doppeldata.bundle_generation import BundleRun
from doppeldata.bundle_generation import BundleWritePlan
from doppeldata.bundle_generation import generate_bundle
from doppeldata.bundle_generation import plan_bundle
from doppeldata.bundle_generation import run_of
from doppeldata.bundle_generation import write_bundle
from doppeldata.column_overrides import Overrides
from doppeldata.counted_nouns import counted
from doppeldata.dataset_generation import ArrowStreamExportable
from doppeldata.dataset_generation import WriteChoices
from doppeldata.dataset_generation import WritePlan
from doppeldata.dataset_generation import prepared_write
from doppeldata.dataset_generation import rows_plan
from doppeldata.dataset_generation import s3_store_of
from doppeldata.dataset_generation import write_one_file
from doppeldata.dataset_generation import write_plan_of
from doppeldata.errors import InvalidArgument
from doppeldata.errors import UnsupportedArgument
from doppeldata.errors import WriteFailed
from doppeldata.issue_codes import BUNDLE_GIVEN
from doppeldata.issue_codes import MANIFEST_ON_BUNDLE
from doppeldata.manifest import BundleManifest
from doppeldata.manifest import Manifest
from doppeldata.manifest_regeneration import manifest_at
from doppeldata.manifest_regeneration import recorded_location
from doppeldata.manifest_regeneration import regenerate_bundle
from doppeldata.manifest_regeneration import regenerate_table
from doppeldata.profile_format import Bundle
from doppeldata.profile_sources import ProfileSource
from doppeldata.scenario_format import Scenario

log = logging.getLogger(__name__)


@overload
def generate(
    profile: Bundle,
    *,
    rows: None = None,
    seed: int | None = None,
    threads: int = 0,
    scenario: Scenario | Sequence[Scenario] | None = None,
    scale: float | None = None,
) -> dict[str, ArrowStreamExportable]: ...


@overload
def generate(
    profile: ProfileSource,
    *,
    rows: int | None = None,
    seed: int | None = None,
    threads: int = 0,
    scenario: Scenario | Sequence[Scenario] | None = None,
    scale: None = None,
) -> ArrowStreamExportable: ...


def generate(
    profile: ProfileSource | Bundle,
    *,
    rows: int | None = None,
    seed: int | None = None,
    threads: int = 0,
    scenario: Scenario | Sequence[Scenario] | None = None,
    scale: float | None = None,
) -> ArrowStreamExportable | dict[str, ArrowStreamExportable]:
    """Every row of the dataset, as one Arrow table in memory. `threads` of zero takes one per core.

    `profile` is a Profile, or anything `Profile.expand` takes: a list of column names, a dict of columns such as
    `{'email': sd.fakes.email, 'age': sd.integers(18, 90)}`, a short or full YAML file, or an example's name. A Parquet or
    Arrow IPC file written with `embed_profile=True` gives its profile, and its write's seed, rows and scenario for each of
    the three left None. Given none of them, a file whose dataset this release would not make again raises SourceError. A
    seed left None is 0 for any other source.

    The values are a pure function of the profile, the scenario, `seed` and `rows`, whatever `threads` is. `scenario` is a
    Scenario or a list composed in order. One that shapes files, as schema_drift, small_files and malformed_rows do, is
    refused as InvalidScenario, because nothing here writes files. `rows` counts the rows generated, so a scenario's
    `duplicates` returns more.

    A Bundle, as `Bundle.load` reads a bundle file or directory and `Bundle.example` an example bundle, returns a dict of
    each table's name to its table, parents first, every foreign key drawn from its parent's keys. `scale` multiplies each
    table's row count, 1 where left None, and `rows` is refused. Each scenario names the table it bends, and a table's
    scenarios compose in order.
    """
    run = run_of(profile, rows, seed, scale)
    if isinstance(run, BundleRun):
        return generate_bundle(run, threads=threads, scenario=scenario)
    table: ArrowStreamExportable = rows_plan(run.profile, run.rows, run.seed, scenario, 'generate').generate(threads=threads)
    return table


def sample(
    profile: ProfileSource,
    *,
    rows: int | None = None,
    seed: int | None = None,
    threads: int = 0,
    scenario: Scenario | Sequence[Scenario] | None = None,
    overrides: Overrides | None = None,
) -> ArrowStreamExportable:
    """Rows of one table with some columns pinned, as one Arrow table in memory, such as a unit test arranges its input.

    `overrides` maps a column's name to one value for every row, or to an iterable other than a str or bytes holding one
    value per row. Each value is the column's own type: a str, int, float, Decimal, bool, date, datetime, time, or None for
    a null, and any other Python type raises TypeError. A value the column does not take or cannot hold raises
    UngenerableProfile, a ValueError whose issue is `invalid-override` at the column's pointer. So does a list not holding
    exactly `rows` values, and a list, struct or type-mix column, which no value pins.

    A pinned column takes its place in the draw order. So a conditional child, a lookup or a derived column of it reads the
    pinned values, and every column reading none holds the values `generate` gives. A copula member's group keeps the rest
    of its draws, so the pinned column's correlation with it is not held. A scenario's ops apply after the pins, so an op
    writing a pinned column moves it.

    The identity names the pins by their SHA-256, so the table's `doppeldata.dataset_sha256` is its own. Its schema carries
    that SHA-256 under `doppeldata.overrides_sha256`, and the pinned columns under `doppeldata.overridden_columns`. Without
    overrides, it is what `generate` returns. `profile`,
    `rows`, `seed`, `threads` and `scenario` are taken as `generate` takes them. A Bundle is refused.
    """
    if isinstance(profile, Bundle):
        raise UnsupportedArgument(
            BUNDLE_GIVEN, "sample takes one table's profile, and a bundle holds several; generate the bundle", field='profile'
        )
    table: ArrowStreamExportable = rows_plan(profile, rows, seed, scenario, 'sample', overrides).generate(threads=threads)
    return table


def stream(
    profile: ProfileSource,
    *,
    rows: int | None = None,
    seed: int | None = None,
    threads: int = 0,
    scenario: Scenario | Sequence[Scenario] | None = None,
) -> ArrowStreamExportable:
    """The dataset as an Arrow RecordBatchReader, one batch per engine block of `_core.BLOCK_ROWS` rows, holding at most two
    batches per thread.

    It reads once. pyarrow 15 or later takes it as `pa.RecordBatchReader.from_stream(reader)`. Its threads check for no signal,
    so Ctrl-C reaches it only when its consumer returns to Python between batches. `profile`, `rows`, `seed` and `scenario`
    are taken as `generate` takes them. A Bundle is refused, because a stream holds one table.
    """
    if isinstance(profile, Bundle):
        raise UnsupportedArgument(
            BUNDLE_GIVEN, 'a stream holds one table, and a bundle holds several; generate or write the bundle', field='profile'
        )
    reader: ArrowStreamExportable = rows_plan(profile, rows, seed, scenario, 'stream').stream(threads=threads)
    return reader


@overload
def plan(
    profile: Bundle,
    out: str | os.PathLike[str],
    *,
    rows: None = None,
    seed: int | None = None,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: None = None,
    partition_style: None = None,
    writer: dict[str, Any] | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
    embed_profile: bool = False,
    scale: float | None = None,
    aws_profile: str | None = None,
) -> BundleWritePlan: ...


@overload
def plan(
    profile: ProfileSource,
    out: str | os.PathLike[str],
    *,
    rows: int | None = None,
    seed: int | None = None,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: list[str] | None = None,
    partition_style: str | None = None,
    writer: dict[str, Any] | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
    embed_profile: bool = False,
    scale: None = None,
    aws_profile: str | None = None,
) -> WritePlan: ...


@overload
def plan(
    profile: ProfileSource | Bundle,
    out: str | os.PathLike[str],
    *,
    rows: int | None = None,
    seed: int | None = None,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: list[str] | None = None,
    partition_style: str | None = None,
    writer: dict[str, Any] | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
    embed_profile: bool = False,
    scale: float | None = None,
    aws_profile: str | None = None,
) -> WritePlan | BundleWritePlan: ...


def plan(
    profile: ProfileSource | Bundle,
    out: str | os.PathLike[str],
    *,
    rows: int | None = None,
    seed: int | None = None,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: list[str] | None = None,
    partition_style: str | None = None,
    writer: dict[str, Any] | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
    embed_profile: bool = False,
    scale: float | None = None,
    aws_profile: str | None = None,
) -> WritePlan | BundleWritePlan:
    """What `write` would write with the same arguments: the identity, the format and settings, every range and each column's sampler.

    It reads nothing at `out`, and refuses what `write` would refuse from its arguments alone, an `aws_profile` the AWS
    config lacks among them. A Bundle gives a BundleWritePlan, holding each table's WritePlan.
    """
    choices = WriteChoices(format, file_rows, partition_by, partition_style, writer, embed_profile)
    with aws_profile_chosen(aws_profile):
        run = run_of(profile, rows, seed, scale)
        if isinstance(run, BundleRun):
            return plan_bundle(run, out, choices=choices, scenario=scenario)
        prepared = prepared_write(run.profile, out, run.rows, run.seed, choices, scenario)
        return write_plan_of(prepared.engine, prepared.options, prepared.destination, prepared.scenario, prepared.one_file)


@overload
def write(
    profile: Bundle,
    out: str | os.PathLike[str],
    *,
    rows: None = None,
    seed: int | None = None,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: None = None,
    partition_style: None = None,
    writer: dict[str, Any] | None = None,
    threads: int = 0,
    replace: bool = False,
    scenario: Scenario | Sequence[Scenario] | None = None,
    manifest_beside: bool = False,
    embed_profile: bool = False,
    scale: float | None = None,
    aws_profile: str | None = None,
) -> BundleManifest: ...


@overload
def write(
    profile: ProfileSource,
    out: str | os.PathLike[str],
    *,
    rows: int | None = None,
    seed: int | None = None,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: list[str] | None = None,
    partition_style: str | None = None,
    writer: dict[str, Any] | None = None,
    threads: int = 0,
    replace: bool = False,
    scenario: Scenario | Sequence[Scenario] | None = None,
    manifest_beside: bool = False,
    embed_profile: bool = False,
    scale: None = None,
    aws_profile: str | None = None,
) -> Manifest: ...


@overload
def write(
    profile: ProfileSource | Bundle,
    out: str | os.PathLike[str],
    *,
    rows: int | None = None,
    seed: int | None = None,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: list[str] | None = None,
    partition_style: str | None = None,
    writer: dict[str, Any] | None = None,
    threads: int = 0,
    replace: bool = False,
    scenario: Scenario | Sequence[Scenario] | None = None,
    manifest_beside: bool = False,
    embed_profile: bool = False,
    scale: float | None = None,
    aws_profile: str | None = None,
) -> Manifest | BundleManifest: ...


def write(
    profile: ProfileSource | Bundle,
    out: str | os.PathLike[str],
    *,
    rows: int | None = None,
    seed: int | None = None,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: list[str] | None = None,
    partition_style: str | None = None,
    writer: dict[str, Any] | None = None,
    threads: int = 0,
    replace: bool = False,
    scenario: Scenario | Sequence[Scenario] | None = None,
    manifest_beside: bool = False,
    embed_profile: bool = False,
    scale: float | None = None,
    aws_profile: str | None = None,
) -> Manifest | BundleManifest:
    """Write the dataset to `out` and return its manifest. `profile` is taken as `generate` takes it.

    `out` is a local directory, an `s3://` or `s3a://` URL, or `memory://`, which keeps nothing but the manifest. Each holds
    `_manifest.json` beside the data, which claims the destination and records a write that stops. The format, its settings
    and the layout start from what the profile records, and each argument given is laid over them. `partition_by=[]` writes
    no partitions. A destination holding anything is refused unless `replace` is set, which deletes only what an earlier
    write recorded. S3 takes its credentials, region and endpoint through boto3, from the AWS profile `aws_profile` names,
    else from the AWS environment, and an `aws_profile` the AWS config lacks is refused before anything is written.

    A local path ending in a format's suffix, such as `people.csv`, is written as that one file in that format, and one
    ending in `/` as a directory. It gets `_manifest.json` beside it only where `manifest_beside` is set, and `replace` writes over
    either. A directory holding another dataset's manifest refuses it.

    A write that fails raises WriteFailed, and one a signal stops, such as Ctrl-C, raises WriteInterrupted. Both are a
    StoppedWrite. Once a write to a directory has claimed it, either leaves `_manifest.partial.json` listing the files it
    completed. A write to one file that stops leaves nothing, unless it is killed outright, which leaves its staging
    directory beside the file for `replace` to remove.

    `scenario`, a Scenario or a list composed in order, bends the data: its patch applies to the profile, and its perturbations
    to the rows and the files. The manifest records it, and a layout that cannot hold it raises InvalidScenario. `rows` counts
    the rows generated, so a scenario's `duplicates` writes more.

    `embed_profile` writes the whole profile's canonical JSON into each Parquet or Arrow IPC file's metadata, under
    `doppeldata.profile`, where `Profile.from_data_file` reads it back. It is the profile given, before any scenario's patch.
    The scenario's document goes under `doppeldata.scenario`, so the file alone regenerates the dataset as `profile`. Any
    other format raises WriteFailed. A profile made from data that fails `profile check` raises ProfileFailsCheck before a byte
    is written, as `export` refuses it, because the file carries every literal the profile holds.

    A Bundle writes each table under `out/<table>/` with its manifest, and at `out` the bundle's manifest naming each,
    returned as a BundleManifest. `scale` and `scenario` are taken as `generate` takes them for a bundle. Each table is
    partitioned as its profile records, so `partition_by` and `partition_style` are refused, and so are `embed_profile`, a
    one-file `out` and `manifest_beside`, because a bundle always writes its manifests.
    """
    choices = WriteChoices(format, file_rows, partition_by, partition_style, writer, embed_profile)
    with aws_profile_chosen(aws_profile):
        run = run_of(profile, rows, seed, scale)
        if isinstance(run, BundleRun):
            if manifest_beside:
                message = 'a bundle always writes its manifests, and manifest_beside asks for one beside a single file'
                raise InvalidArgument(MANIFEST_ON_BUNDLE, message, field='manifest_beside', value=manifest_beside)
            return write_bundle(run, out, choices=choices, threads=threads, replace=replace, scenario=scenario)
        prepared = prepared_write(run.profile, out, run.rows, run.seed, choices, scenario)
        started = time.monotonic()
        if prepared.one_file is not None:
            written = write_one_file(prepared, prepared.one_file, threads=threads, replace=replace, manifest_beside=manifest_beside)
        else:
            s3_store = s3_store_of(prepared.destination, prepared.bucket)
            store = prepared.engine.write(prepared.options, prepared.destination, threads=threads, replace=replace, s3_store=s3_store)
            written = Manifest.from_json(store)
    rows_written = sum(file['rows'] for file in written.files)
    target = prepared.one_file if prepared.one_file is not None else prepared.destination
    elapsed = time.monotonic() - started
    files = counted(len(written.files), f'{written.format} file')
    log.info(f'{target}: wrote {files} holding {counted(rows_written, "row")} in {elapsed:.2f} s')
    for file in written.files:
        log.debug(f'{file["path"]}: {counted(file["rows"], "row")}, {counted(file["bytes"], "byte")}')
    return written


@overload
def regenerate(
    profile: Bundle,
    manifest: str | os.PathLike[str] | BundleManifest,
    out: str | os.PathLike[str],
    *,
    threads: int = 0,
    replace: bool = False,
    aws_profile: str | None = None,
) -> BundleManifest: ...


@overload
def regenerate(
    profile: ProfileSource,
    manifest: str | os.PathLike[str] | Manifest,
    out: str | os.PathLike[str],
    *,
    threads: int = 0,
    replace: bool = False,
    aws_profile: str | None = None,
) -> Manifest: ...


@overload
def regenerate(
    profile: ProfileSource | Bundle,
    manifest: str | os.PathLike[str] | Manifest | BundleManifest,
    out: str | os.PathLike[str],
    *,
    threads: int = 0,
    replace: bool = False,
    aws_profile: str | None = None,
) -> Manifest | BundleManifest: ...


def regenerate(
    profile: ProfileSource | Bundle,
    manifest: str | os.PathLike[str] | Manifest | BundleManifest,
    out: str | os.PathLike[str],
    *,
    threads: int = 0,
    replace: bool = False,
    aws_profile: str | None = None,
) -> Manifest | BundleManifest:
    """Write the dataset `manifest` records again to `out` from `profile`, compare every file with its record, and return the
    fresh manifest.

    `manifest` is a `_manifest.json` as a local path or an `s3://` or `s3a://` URL, or a Manifest or BundleManifest as
    `write` returns it. The manifest settles every input but the profile, so `profile` is the one input given, taken as
    `generate` takes it. A profile whose SHA-256 is not the one the manifest records is refused as WriteFailed, naming both.
    So is a plan whose dataset identity differs for another reason, such as a release that moved the stream version, naming
    the identity keys that differ.

    `out` takes any destination `write` writes to except one file. It is refused where it holds the manifest file named, or
    for a Manifest or BundleManifest the one at the destination it records. A run there with replace would store the fresh
    manifest over the record it compares with. `replace`, `threads`
    and `aws_profile` are taken as `write` takes them. A partial manifest is refused, and so are one whose files embed the
    profile and one recording a write to one file.

    A file whose record differs from the manifest's in any field raises WriteFailed once the fresh manifest is stored. Each
    issue sits at the pointer to that file's record, such as `/files/3`, holds its path as `value`, and says what differed.
    A bundle's manifest regenerates each table under `out/<table>/`, parents first, with `profile` the Bundle. Every table's
    manifest is checked before any table is written. A manifest of one table of a bundle regenerates that table alone from
    the Bundle.
    """
    with aws_profile_chosen(aws_profile):
        recorded = manifest if isinstance(manifest, Manifest | BundleManifest) else manifest_at(manifest)
        location = recorded_location(recorded) if isinstance(manifest, Manifest | BundleManifest) else os.fspath(manifest)
        started = time.monotonic()
        if isinstance(recorded, BundleManifest):
            if not isinstance(profile, Bundle):
                message = "the manifest is a bundle's, and the profile is one table; give the bundle"
                raise WriteFailed(issue_codes.IDENTITY_MISMATCH, message)
            return regenerate_bundle(profile, recorded, out, manifest_location=location, threads=threads, replace=replace)
        written = regenerate_table(profile, recorded, out, manifest_location=location, threads=threads, replace=replace)
    elapsed = time.monotonic() - started
    files = counted(len(written.files), f'{written.format} file')
    log.info(f'{written.document["destination"]}: wrote {files} again, each matching its record, in {elapsed:.2f} s')
    return written
