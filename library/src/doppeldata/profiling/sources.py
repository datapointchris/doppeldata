"""The data a profile describes: files on disk or under an `s3://` prefix, or a table already in memory."""

import os
from collections.abc import Iterator
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

import boto3

from doppeldata.profiling.backend import TEXT_FORMATS
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import SourceError
from doppeldata.profiling.backend import SourceFile
from doppeldata.profiling.backend import SourceFormat

SUFFIX_FORMATS = {
    '.parquet': SourceFormat.PARQUET,
    '.pq': SourceFormat.PARQUET,
    '.arrow': SourceFormat.ARROW,
    '.feather': SourceFormat.ARROW,
    '.ipc': SourceFormat.ARROW,
    '.csv': SourceFormat.CSV,
    '.tsv': SourceFormat.TSV,
    '.tab': SourceFormat.TSV,
    '.json': SourceFormat.JSON,
    '.ndjson': SourceFormat.NDJSON,
    '.jsonl': SourceFormat.NDJSON,
}
# DuckDB reads a text file through any of these, so the format is the suffix before it.
COMPRESSION_SUFFIXES = ('.gz', '.zst', '.bz2')


def resolve(data: Any) -> Source:
    """The source behind `data`: a path, an `s3://` URL, a list of them, or an Arrow-exporting table."""
    if hasattr(data, '__arrow_c_stream__'):
        return Source(format=None, table=data)
    items = [os.fspath(data)] if isinstance(data, str | os.PathLike) else [os.fspath(item) for item in data]
    if not items:
        raise SourceError('no data was named; name a file, a directory or an s3:// prefix')
    files = tuple(file for item in items for file in (s3_files(item) if item.startswith('s3://') else local_files(Path(item))))
    formats = sorted({found.value for found in map(format_of, (file.path for file in files)) if found})
    if len(formats) > 1:
        raise SourceError(f'{", ".join(items)} holds files of {len(formats)} formats, {", ".join(formats)}; profile one format at a time')
    source_format = format_of(files[0].path)
    if source_format is None:
        raise SourceError(f'{files[0].path} is not a format doppeldata reads; name a file ending in one of {", ".join(SUFFIX_FORMATS)}')
    if source_format in TEXT_FORMATS and any(file.path.startswith('s3://') for file in files):
        message = f'{source_format.value} is read from local files only; copy it down first with `aws s3 cp --recursive {items[0]} <dir>`'
        raise SourceError(message)
    return Source(format=source_format, files=files, name=name_of(items[0]))


def format_of(path: str) -> SourceFormat | None:
    name = PurePosixPath(path).name.lower()
    for suffix in COMPRESSION_SUFFIXES:
        name = name.removesuffix(suffix)
    return SUFFIX_FORMATS.get(PurePosixPath(name).suffix)


def name_of(item: str) -> str:
    """A profile's name from what was named: a file's stem, or a directory's or prefix's last segment."""
    last = PurePosixPath(urlparse(item).path if item.startswith('s3://') else item).name or 'table'
    return last.split('.')[0] if format_of(last) else last


def is_hidden(relative: PurePosixPath) -> bool:
    """A dot file, or a marker such as Spark's `_SUCCESS`, or anything under such a directory."""
    return any(part.startswith(('.', '_')) for part in relative.parts)


def local_files(path: Path) -> list[SourceFile]:
    if path.is_file():
        return [local_file(path)]
    if not path.is_dir():
        raise SourceError(f'{path} does not exist')
    found = sorted(
        candidate
        for candidate in path.rglob('*')
        if candidate.is_file() and format_of(candidate.name) and not is_hidden(PurePosixPath(candidate.relative_to(path).as_posix()))
    )
    if not found:
        raise SourceError(f'{path} holds no file doppeldata reads, one ending in {", ".join(SUFFIX_FORMATS)}')
    return [local_file(candidate) for candidate in found]


def local_file(path: Path) -> SourceFile:
    stat = path.stat()
    return SourceFile(str(path), stat.st_size, str(stat.st_mtime_ns))


def s3_files(url: str) -> list[SourceFile]:
    """The objects at an `s3://` URL, listed with the credentials and endpoint boto3 resolves from the environment."""
    parsed = urlparse(url)
    bucket, key = parsed.netloc, parsed.path.lstrip('/')
    client = boto3.client('s3')
    if format_of(key):
        head = client.head_object(Bucket=bucket, Key=key)
        return [SourceFile(url, head['ContentLength'], head['LastModified'].isoformat())]
    prefix = key.rstrip('/') + '/' if key else ''
    found = sorted(s3_objects(client, bucket, prefix), key=lambda file: file.path)
    if not found:
        raise SourceError(f'{url} holds no object doppeldata reads, one ending in {", ".join(SUFFIX_FORMATS)}')
    return found


def s3_objects(client: Any, bucket: str, prefix: str) -> Iterator[SourceFile]:
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get('Contents', []):
            relative = PurePosixPath(entry['Key'].removeprefix(prefix))
            if format_of(entry['Key']) and not is_hidden(relative):
                yield SourceFile(f's3://{bucket}/{entry["Key"]}', entry['Size'], entry['LastModified'].isoformat())
