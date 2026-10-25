"""Synthetic datasets generated from a profile: in memory, as a stream, or written to files with a manifest.

Tables and readers cross from the engine through the Arrow PyCapsule interface, so Polars, DuckDB and pyarrow 14 or later read
them with no copy, and none of them is needed to generate. `generate` and `write` release the GIL while the engine works, and
stop between blocks on Ctrl-C.
"""

import dataclasses as dc
import importlib.util
import json
import os
import re
from typing import Any
from typing import Protocol

from doppeldata import _core
from doppeldata.errors import WriteFailed
from doppeldata.manifest import Manifest
from doppeldata.profile_format import Issue
from doppeldata.profile_format import Profile
from doppeldata.profile_format import UngenerableProfile

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

    `identity` and `dataset_digest` are what the manifest will record. `estimated_bytes` comes from the write's first blocks
    encoded with its own settings, and leaves the manifest out. The shape of `to_json` may change before 1.0.
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

    def engine_json(self, profile: Profile) -> str:
        chosen = {key: value for key, value in dc.asdict(self).items() if value is not None}
        if profile.path:
            chosen['profile_path'] = str(profile.path)
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
    if scheme == 's3' and importlib.util.find_spec('boto3') is None:
        message = 'writing to s3:// resolves credentials through boto3, which doppeldata does not install; run `pip install boto3`'
        raise WriteFailed('boto3-missing', message, kind='refused')
    return destination, scheme, bucket


@dc.dataclass(frozen=True)
class EngineArguments:
    """Everything the engine's plan is built from, so a process holding only these, such as a Spark executor, builds the same plan."""

    profile_json: str
    profile_digest: str
    seed: int
    rows: int
    source: str

    def plan(self) -> '_core.Plan':
        return _core.Plan(self.profile_json, self.profile_digest, self.seed, self.rows, self.source)


def engine_arguments(profile: Profile, rows: int | None, seed: int) -> EngineArguments:
    """The engine's arguments for `rows` rows of `profile` from `seed`. No `rows` takes the row count the profile records."""
    if not isinstance(profile, Profile):
        raise TypeError(f'generation takes a Profile, such as Profile.load(path), not {type(profile).__name__}')
    source = str(profile.path) if profile.path else f'profile {profile.document.get("name")!r}'
    if rows is None:
        rows = (profile.document.get('dataset') or {}).get('row_count')
        if rows is None:
            issue = Issue('no-row-count', '/dataset/row_count', 'the profile records no row count, so rows must be given')
            raise UngenerableProfile(source, [issue])
    if rows < 0:
        raise ValueError(f'rows must be zero or more, not {rows}')
    if not 0 <= seed <= LARGEST_SEED:
        raise ValueError(f'seed must be between 0 and 2^64 - 1, not {seed}')
    return EngineArguments(json.dumps(profile.to_plain(), allow_nan=False), profile.digest, seed, rows, source)


def engine_plan(profile: Profile, rows: int | None, seed: int) -> '_core.Plan':
    """The engine's plan for `rows` rows of `profile` from `seed`. No `rows` takes the row count the profile records."""
    return engine_arguments(profile, rows, seed).plan()


@dc.dataclass(frozen=True)
class PreparedWrite:
    """A write's destination and the engine's options for it, resolved from the arguments `plan` and `write` take."""

    destination: str
    scheme: str
    bucket: str | None
    arguments: EngineArguments
    engine: '_core.Plan'
    choices_json: str
    options: '_core.WriteOptions'


def prepared_write(profile: Profile, out: str | os.PathLike[str], rows: int | None, seed: int, choices: WriteChoices) -> PreparedWrite:
    """What a write of `rows` rows of `profile` from `seed` to `out` with `choices` runs on, refusing what the engine would refuse."""
    destination, scheme, bucket = destination_of(out)
    arguments = engine_arguments(profile, rows, seed)
    engine = arguments.plan()
    choices_json = choices.engine_json(profile)
    return PreparedWrite(destination, scheme, bucket, arguments, engine, choices_json, engine.write_options(choices_json))


def s3_store_of(destination: str, bucket: str | None) -> dict[str, Any] | None:
    """The S3 store's settings for `destination`, from boto3's chain in this process, or None for a destination not on S3."""
    if bucket is None:
        return None
    from doppeldata.s3_access import s3_store_settings

    return s3_store_settings(destination, bucket)


def generate(profile: Profile, *, rows: int | None = None, seed: int = 0, threads: int = 0) -> ArrowStreamExportable:
    """Every row of the dataset, as one Arrow table in memory. `threads` of zero takes one per core.

    The values are a pure function of the profile, `seed` and `rows`, whatever `threads` is.
    """
    table: ArrowStreamExportable = engine_plan(profile, rows, seed).generate(threads=threads)
    return table


def stream(profile: Profile, *, rows: int | None = None, seed: int = 0, threads: int = 0) -> ArrowStreamExportable:
    """The dataset as an Arrow RecordBatchReader, one batch per engine block of `_core.BLOCK_ROWS` rows, holding at most two
    batches per thread.

    It reads once. pyarrow 15 or later takes it as `pa.RecordBatchReader.from_stream(reader)`. Its threads check for no signal,
    so Ctrl-C reaches it only when its consumer returns to Python between batches.
    """
    reader: ArrowStreamExportable = engine_plan(profile, rows, seed).stream(threads=threads)
    return reader


def plan(
    profile: Profile,
    out: str | os.PathLike[str],
    *,
    rows: int | None = None,
    seed: int = 0,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: list[str] | None = None,
    partition_style: str | None = None,
    writer: dict[str, Any] | None = None,
) -> WritePlan:
    """What `write` would write with the same arguments: the identity, the format and settings, every range and each column's sampler.

    It reads nothing at `out`, and refuses what `write` would refuse from its arguments alone.
    """
    prepared = prepared_write(profile, out, rows, seed, WriteChoices(format, file_rows, partition_by, partition_style, writer))
    engine, options = prepared.engine, prepared.options
    settings = json.loads(options.settings_json)
    ranges = [PlannedRange(**planned) for planned in json.loads(engine.ranges_json(options))]
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
        estimated_bytes=engine.estimate_bytes(options),
    )


def write(
    profile: Profile,
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
) -> Manifest:
    """Write the dataset to `out`, with `_manifest.json` beside it, and return the manifest.

    `out` is a local directory, an `s3://` or `s3a://` URL, or `memory://`, which keeps nothing but the manifest. The format,
    its settings and the layout start from what the profile records, and each argument given is laid over them.
    `partition_by=[]` writes no partitions. A destination holding anything is refused unless `replace` is set, which deletes
    only what an earlier write recorded. S3 takes its credentials, region and endpoint from the AWS environment through boto3.

    A write that fails raises WriteFailed, and one a signal stops, such as Ctrl-C, raises WriteInterrupted. Both are a
    StoppedWrite. Once the write has claimed its destination, either leaves `_manifest.partial.json` listing the files it completed.
    """
    prepared = prepared_write(profile, out, rows, seed, WriteChoices(format, file_rows, partition_by, partition_style, writer))
    s3_store = s3_store_of(prepared.destination, prepared.bucket)
    written = prepared.engine.write(prepared.options, prepared.destination, threads=threads, replace=replace, s3_store=s3_store)
    return Manifest.from_json(written)
