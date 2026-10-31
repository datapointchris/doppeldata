"""The public `generate`, `stream`, `plan` and `write`, each taking one table's profile or a Bundle, above the modules that
generate each."""

import os
from collections.abc import Sequence
from typing import Any
from typing import overload

from doppeldata.bundle_generation import BundleRun
from doppeldata.bundle_generation import BundleWritePlan
from doppeldata.bundle_generation import generate_bundle
from doppeldata.bundle_generation import plan_bundle
from doppeldata.bundle_generation import run_of
from doppeldata.bundle_generation import write_bundle
from doppeldata.dataset_generation import ArrowStreamExportable
from doppeldata.dataset_generation import WriteChoices
from doppeldata.dataset_generation import WritePlan
from doppeldata.dataset_generation import prepared_write
from doppeldata.dataset_generation import rows_plan
from doppeldata.dataset_generation import s3_store_of
from doppeldata.dataset_generation import write_one_file
from doppeldata.dataset_generation import write_plan_of
from doppeldata.manifest import BundleManifest
from doppeldata.manifest import Manifest
from doppeldata.profile_format import Bundle
from doppeldata.profile_sources import ProfileSource
from doppeldata.scenario_format import Scenario


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
        raise TypeError('a stream holds one table, and a bundle holds several; generate or write the bundle')
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
) -> WritePlan: ...


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
) -> WritePlan | BundleWritePlan:
    """What `write` would write with the same arguments: the identity, the format and settings, every range and each column's sampler.

    It reads nothing at `out`, and refuses what `write` would refuse from its arguments alone. A Bundle gives a
    BundleWritePlan, holding each table's WritePlan.
    """
    choices = WriteChoices(format, file_rows, partition_by, partition_style, writer, embed_profile)
    run = run_of(profile, rows, seed, scale)
    if isinstance(run, BundleRun):
        return plan_bundle(run, out, choices=choices, scenario=scenario)
    prepared = prepared_write(run.profile, out, run.rows, run.seed, choices, scenario)
    return write_plan_of(prepared.engine, prepared.options, prepared.destination, prepared.one_file)


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
    manifest: bool = False,
    embed_profile: bool = False,
    scale: float | None = None,
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
    manifest: bool = False,
    embed_profile: bool = False,
    scale: None = None,
) -> Manifest: ...


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
    manifest: bool = False,
    embed_profile: bool = False,
    scale: float | None = None,
) -> Manifest | BundleManifest:
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

    `embed_profile` writes the whole profile's canonical JSON into each Parquet or Arrow IPC file's metadata, under
    `doppeldata.profile`, where `Profile.from_data_file` reads it back. It is the profile given, before any scenario's patch.
    The scenario's document goes under `doppeldata.scenario`, so the file alone regenerates the dataset as `profile`. Any
    other format raises WriteFailed. A profile made from data that fails `profile check` raises CheckFailed before a byte is
    written, as `export` refuses it, because the file carries every literal the profile holds.

    A Bundle writes each table under `out/<table>/` with its manifest, and at `out` the bundle's manifest naming each,
    returned as a BundleManifest. `scale` and `scenario` are taken as `generate` takes them for a bundle. Each table is
    partitioned as its profile records, so `partition_by` and `partition_style` are refused, and so are `embed_profile`, a
    one-file `out` and `manifest`, which a bundle always writes.
    """
    choices = WriteChoices(format, file_rows, partition_by, partition_style, writer, embed_profile)
    run = run_of(profile, rows, seed, scale)
    if isinstance(run, BundleRun):
        if manifest:
            raise ValueError('a bundle always writes its manifests, and manifest asks for one beside a single file')
        return write_bundle(run, out, choices=choices, threads=threads, replace=replace, scenario=scenario)
    prepared = prepared_write(run.profile, out, run.rows, run.seed, choices, scenario)
    if prepared.one_file is not None:
        return write_one_file(prepared, prepared.one_file, threads=threads, replace=replace, manifest=manifest)
    s3_store = s3_store_of(prepared.destination, prepared.bucket)
    written = prepared.engine.write(prepared.options, prepared.destination, threads=threads, replace=replace, s3_store=s3_store)
    return Manifest.from_json(written)
