"""Synthetic datasets generated from a profile: in memory, as a stream, or written to files with a manifest.

Tables and readers cross from the engine through the Arrow PyCapsule interface, so Polars, DuckDB and pyarrow 14 or later read
them with no copy, and none of them is needed to generate. Each call releases the GIL while the engine works.
"""

import dataclasses as dc
import importlib.util
import json
import os
from typing import Any
from typing import Protocol

from doppeldata import _core
from doppeldata.errors import WriteFailed
from doppeldata.manifest import Manifest
from doppeldata.profile_format import Profile

LARGEST_SEED = 2**64 - 1


class ArrowStreamExportable(Protocol):
    """Data exported through the Arrow PyCapsule interface: `pl.DataFrame(data)`, `pa.table(data)`, or DuckDB by variable name."""

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...


@dc.dataclass(frozen=True)
class ColumnSampler:
    """How one top-level column is drawn."""

    name: str
    arrow_type: str
    sampler: str
    null_share: float


@dc.dataclass(frozen=True)
class PlannedFile:
    """One file a write would hold. A partitioned write puts it under one directory per partition value in its range."""

    path: str
    range: int
    first_row: int
    rows: int


@dc.dataclass(frozen=True)
class WritePlan:
    """What `write` would do with the same arguments, known before anything is generated.

    `identity` and `dataset_digest` are what the manifest will record. `estimated_bytes` comes from the first four blocks encoded
    with the write's own settings, and leaves the manifest out.
    """

    destination: str
    identity: dict[str, Any]
    dataset_digest: str
    format: str
    writer: dict[str, Any]
    layout: dict[str, Any]
    columns: list[ColumnSampler]
    files: list[PlannedFile]
    estimated_bytes: int

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


def engine_plan(profile: Profile, rows: int | None, seed: int) -> '_core.Plan':
    """The engine's plan for `rows` rows of `profile` from `seed`. No `rows` takes the row count the profile records."""
    if not isinstance(profile, Profile):
        raise TypeError(f'generation takes a Profile, such as Profile.load(path), not {type(profile).__name__}')
    if rows is None:
        rows = (profile.document.get('dataset') or {}).get('row_count')
        if rows is None:
            raise ValueError('the profile records no dataset.row_count, so rows must be given')
    if rows < 0:
        raise ValueError(f'rows must be zero or more, not {rows}')
    if not 0 <= seed <= LARGEST_SEED:
        raise ValueError(f'seed must be between 0 and 2^64 - 1, not {seed}')
    source = str(profile.path) if profile.path else f'profile {profile.document.get("name")!r}'
    return _core.Plan(json.dumps(profile.to_plain(), allow_nan=False), profile.digest, seed, rows, source)


def generate(profile: Profile, *, rows: int | None = None, seed: int = 0, threads: int = 0) -> ArrowStreamExportable:
    """Every row of the dataset, as one Arrow table in memory. `threads` of zero takes one per core.

    The values are a pure function of the profile, `seed` and `rows`, whatever `threads` is.
    """
    table: ArrowStreamExportable = engine_plan(profile, rows, seed).generate(threads=threads)
    return table


def stream(profile: Profile, *, rows: int | None = None, seed: int = 0, threads: int = 0) -> ArrowStreamExportable:
    """The dataset as an Arrow RecordBatchReader, one batch per 65,536 rows, holding at most two batches per thread.

    It reads once. pyarrow 15 or later takes it as `pa.RecordBatchReader.from_stream(reader)`.
    """
    reader: ArrowStreamExportable = engine_plan(profile, rows, seed).stream(threads=threads)
    return reader


def write_choices(
    profile: Profile,
    file_format: str | None,
    file_rows: int | None,
    partition_by: list[str] | None,
    partition_style: str | None,
    writer: dict[str, Any] | None,
) -> str:
    choices = {
        'format': file_format,
        'writer': writer,
        'file_rows': file_rows,
        'partition_by': partition_by,
        'partition_style': partition_style,
        'profile_path': str(profile.path) if profile.path else None,
    }
    return json.dumps({key: value for key, value in choices.items() if value is not None})


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
    """What `write` would write with the same arguments: the identity, the format and settings, every file and each column's sampler.

    It reads nothing at `out`.
    """
    engine = engine_plan(profile, rows, seed)
    options = engine.write_options(write_choices(profile, format, file_rows, partition_by, partition_style, writer))
    settings = json.loads(options.settings_json)
    return WritePlan(
        destination=os.fspath(out),
        identity=json.loads(engine.identity_json),
        dataset_digest=engine.dataset_digest,
        format=settings['format'],
        writer=settings['writer'],
        layout=settings['layout'],
        columns=[ColumnSampler(*column) for column in engine.column_samplers()],
        files=[PlannedFile(**file) for file in json.loads(engine.layout_json(options))['files']],
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
    """Write the dataset to `out`, a local directory or an `s3://` URL, with `_manifest.json` beside it, and return the manifest.

    The format, its settings and the layout start from what the profile records, and each argument given is laid over them.
    `partition_by=[]` writes no partitions. A destination holding anything is refused unless `replace` is set, which deletes
    only what an earlier write recorded. S3 takes its credentials, region and endpoint from the AWS environment through boto3.

    A write that fails raises WriteFailed, and one Ctrl-C stops raises WriteInterrupted. Either, once the write has claimed its
    destination, leaves `_manifest.partial.json` listing the files it completed.
    """
    engine = engine_plan(profile, rows, seed)
    options = engine.write_options(write_choices(profile, format, file_rows, partition_by, partition_style, writer))
    destination = os.fspath(out)
    s3_store = None
    if destination.startswith('s3://'):
        if importlib.util.find_spec('boto3') is None:
            message = 'writing to s3:// resolves credentials through boto3, which doppeldata does not install; run `pip install boto3`'
            raise WriteFailed('boto3-missing', message, exit_code=2)
        from doppeldata.s3_access import s3_store_settings

        s3_store = s3_store_settings(destination)
    return Manifest.from_json(engine.write(options, destination, threads=threads, replace=replace, s3_store=s3_store))
