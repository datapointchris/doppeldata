"""How each Parquet file was written, from its footer. No value statistic is read: a footer's min and max are real values."""

import collections
import concurrent.futures
import json
import random
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from doppeldata.errors import SourceError
from doppeldata.profiling.backend import ParquetFooter
from doppeldata.profiling.backend import SourceFile
from doppeldata.profiling.sources import s3_errors
from doppeldata.s3_access import endpoint_named
from doppeldata.s3_access import s3_session

# The most footers read, from a seeded sample of the files beyond that, because each footer on S3 is a remote read.
FOOTERS_READ = 512
FOOTER_THREADS = 16
FOOTER_SEED = 0
# One ranged read of a file's last bytes holds most footers. A longer footer takes a second read of the rest.
TAIL_BYTES = 64 * 1024
# A Parquet file ends in its footer's length, four bytes little-endian, then this.
PARQUET_MAGIC = b'PAR1'
SPARK_VERSION_KEY = b'org.apache.spark.version'
TIME_UNITS = {'milliseconds': 'ms', 'microseconds': 'us', 'nanoseconds': 'ns'}


def read_footers(files: tuple[SourceFile, ...]) -> tuple[ParquetFooter, ...]:
    """Each chosen file's footer. On S3, through the boto3 client that lists the source, so its timeouts, retries and
    region redirects apply, and its failures map as a listing's do.
    """
    chosen = (
        list(files)
        if len(files) <= FOOTERS_READ
        else sorted(random.Random(FOOTER_SEED).sample(list(files), FOOTERS_READ), key=lambda file: file.path)
    )
    if not chosen or not chosen[0].path.startswith('s3://'):
        with concurrent.futures.ThreadPoolExecutor(FOOTER_THREADS) as pool:
            return tuple(pool.map(lambda file: footer_of(parsed(file.path, file.path)), chosen))
    _, client = s3_session()
    with s3_errors(chosen[0].path, endpoint_named(client), doing='read'), concurrent.futures.ThreadPoolExecutor(FOOTER_THREADS) as pool:
        return tuple(pool.map(lambda file: footer_of(parsed(pa.BufferReader(tail_of(client, file)), file.path)), chosen))


def tail_of(client: Any, file: SourceFile) -> bytes:
    """An S3 object's footer, its length and the magic that ends it, in one ranged read, or two for a long footer."""
    bucket, key = file.path.removeprefix('s3://').split('/', 1)
    tail = ranged(client, bucket, key, max(0, file.size - TAIL_BYTES), file.size) if file.size >= 8 else b''
    if len(tail) < 8 or tail[-4:] != PARQUET_MAGIC:
        raise SourceError(f'the footer of {file.path} could not be read: the file does not end in {PARQUET_MAGIC.decode()}', exit_code=1)
    length = int.from_bytes(tail[-8:-4], 'little')
    if length + 8 > len(tail) and length + 8 <= file.size:
        tail = ranged(client, bucket, key, file.size - length - 8, file.size - len(tail)) + tail
    return tail


def ranged(client: Any, bucket: str, key: str, start: int, end: int) -> bytes:
    """The object's bytes from `start` up to `end`."""
    body: bytes = client.get_object(Bucket=bucket, Key=key, Range=f'bytes={start}-{end - 1}')['Body'].read()
    return body


def parsed(source: str | pa.BufferReader, path: str) -> pq.FileMetaData:
    try:
        return pq.read_metadata(source)
    except (OSError, pa.ArrowException) as error:
        raise SourceError(f'the footer of {path} could not be read: {error}', exit_code=1) from error


def footer_of(metadata: pq.FileMetaData) -> ParquetFooter:
    groups = [metadata.row_group(index) for index in range(metadata.num_row_groups)]
    chunks = [group.column(index) for group in groups for index in range(group.num_columns)]
    paths = [metadata.schema.column(index).path for index in range(len(metadata.schema))]
    codecs = collections.Counter(chunk.compression for chunk in chunks)
    dictionary = [path for index, path in enumerate(paths) if groups and all(group.column(index).has_dictionary_page for group in groups)]
    declared = {tuple(paths[sorting.column_index] for sorting in group.sorting_columns or ()) for group in groups}
    kv = metadata.metadata or {}
    return ParquetFooter(
        rows=metadata.num_rows,
        created_by=metadata.created_by or None,
        format_version=metadata.format_version,
        spark_version=kv[SPARK_VERSION_KEY].decode() if SPARK_VERSION_KEY in kv else None,
        row_groups=tuple(
            (group.num_rows, sum(group.column(index).total_compressed_size for index in range(group.num_columns))) for group in groups
        ),
        codec=codecs.most_common(1)[0][0] if codecs else None,
        columns=tuple(paths),
        dictionary_columns=tuple(dictionary),
        statistics=most(chunk.is_stats_set for chunk in chunks),
        page_index=most(chunk.has_column_index and chunk.has_offset_index for chunk in chunks),
        sorting_columns=declared.pop() if len(declared) == 1 else (),
        timestamp_units={path: unit for index, path in enumerate(paths) if (unit := stored_unit(metadata.schema.column(index)))},
        decimal_scales={path: column.scale for index, path in enumerate(paths) if is_decimal(column := metadata.schema.column(index))},
    )


def most(flags: Any) -> bool:
    counted = collections.Counter(bool(flag) for flag in flags)
    return counted[True] > counted[False]


def stored_unit(column: pq.ColumnSchema) -> str | None:
    if column.physical_type == 'INT96':
        return 'int96'
    logical = json.loads(column.logical_type.to_json())
    return TIME_UNITS.get(logical.get('timeUnit')) if logical.get('Type') == 'Timestamp' else None


def is_decimal(column: pq.ColumnSchema) -> bool:
    return json.loads(column.logical_type.to_json()).get('Type') == 'Decimal'
