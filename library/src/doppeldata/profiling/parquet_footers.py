"""How each Parquet file was written, from its footer. No value statistic is read: a footer's min and max are real values."""

import collections
import concurrent.futures
import json
import random
from typing import Any

import pyarrow.fs
import pyarrow.parquet as pq

from doppeldata.errors import SourceError
from doppeldata.profiling.backend import ParquetFooter
from doppeldata.profiling.backend import SourceFile
from doppeldata.s3_access import S3_CLIENT_CONFIG
from doppeldata.s3_access import s3_session

# The most footers read, from a seeded sample of the files beyond that, because each footer on S3 is a remote read.
FOOTERS_READ = 512
FOOTER_THREADS = 16
FOOTER_SEED = 0
SPARK_VERSION_KEY = b'org.apache.spark.version'
TIME_UNITS = {'milliseconds': 'ms', 'microseconds': 'us', 'nanoseconds': 'ns'}


def read_footers(files: tuple[SourceFile, ...]) -> tuple[ParquetFooter, ...]:
    chosen = (
        list(files)
        if len(files) <= FOOTERS_READ
        else sorted(random.Random(FOOTER_SEED).sample(list(files), FOOTERS_READ), key=lambda file: file.path)
    )
    filesystem = s3_filesystem(chosen[0].path) if chosen and chosen[0].path.startswith('s3://') else None
    with concurrent.futures.ThreadPoolExecutor(FOOTER_THREADS) as pool:
        return tuple(pool.map(lambda file: footer_of(metadata_of(file.path, filesystem)), chosen))


def metadata_of(path: str, filesystem: pyarrow.fs.FileSystem | None) -> pq.FileMetaData:
    try:
        if filesystem is not None:
            return pq.read_metadata(path.removeprefix('s3://'), filesystem=filesystem)
        return pq.read_metadata(path)
    except (OSError, pyarrow.ArrowException) as error:
        raise SourceError(f'the footer of {path} could not be read: {error}', exit_code=1) from error


def s3_filesystem(url: str) -> pyarrow.fs.S3FileSystem:
    """An S3 filesystem with the credentials boto3 resolves, calling the endpoint boto3 resolves for the bucket's region."""
    session, client = s3_session()
    credentials = session.get_credentials()
    bucket = url.removeprefix('s3://').split('/', 1)[0]
    head = client.head_bucket(Bucket=bucket)
    region = head['ResponseMetadata']['HTTPHeaders'].get('x-amz-bucket-region') or client.meta.region_name
    endpoint = session.client('s3', region_name=region, config=S3_CLIENT_CONFIG).meta.endpoint_url
    if credentials is None:
        return pyarrow.fs.S3FileSystem(anonymous=True, region=region, endpoint_override=endpoint)
    frozen = credentials.get_frozen_credentials()
    return pyarrow.fs.S3FileSystem(
        access_key=frozen.access_key, secret_key=frozen.secret_key, session_token=frozen.token, region=region, endpoint_override=endpoint
    )


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
