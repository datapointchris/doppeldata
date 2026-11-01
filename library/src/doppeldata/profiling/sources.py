"""The data a profile describes: files on disk or under an `s3://` prefix, or a table already in memory."""

import contextlib
import logging
import os
from collections.abc import Iterator
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

import botocore.exceptions

from doppeldata import issue_codes
from doppeldata.aws_access import AWS_ERRORS
from doppeldata.aws_access import CREDENTIAL_ADVICE
from doppeldata.aws_access import CREDENTIAL_ERRORS
from doppeldata.aws_access import answered
from doppeldata.aws_access import no_answer
from doppeldata.aws_access import unanswered
from doppeldata.errors import InvalidArgument
from doppeldata.errors import SourceError
from doppeldata.manifest import MANIFEST_FILE_NAME
from doppeldata.manifest import PARTIAL_MANIFEST_FILE_NAME
from doppeldata.manifest import Manifest
from doppeldata.manifest import unfinished
from doppeldata.profiling.backend import FORMAT_TRAITS
from doppeldata.profiling.backend import TEXT_CODEC_SUFFIXES
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import SourceFile
from doppeldata.profiling.backend import SourceFormat
from doppeldata.s3_access import endpoint_named
from doppeldata.s3_access import s3_session

log = logging.getLogger(__name__)

SUFFIX_FORMATS = {suffix: source_format for source_format, traits in FORMAT_TRAITS.items() for suffix in traits.suffixes}
# The format of a text file compressed with one of these is the suffix before it.
COMPRESSION_SUFFIXES = tuple(TEXT_CODEC_SUFFIXES)
# Hadoop's names for S3, which a Spark user writes.
HADOOP_S3_SCHEMES = ('s3a://', 's3n://')
# A doppeldata write claims its destination with this file, and removes it once every file is written.
UNFINISHED_WRITE = PurePosixPath(PARTIAL_MANIFEST_FILE_NAME)


def resolve(data: Any) -> Source:
    """The source behind `data`: a path, an `s3://` URL, a list of them, an Arrow-exporting table or a Spark DataFrame."""
    if hasattr(data, '__arrow_c_stream__') or is_spark_frame(data):
        return Source(format=None, table=data)
    items = [s3_url(os.fspath(item)) for item in ([data] if isinstance(data, str | os.PathLike) else data)]
    if not items:
        raise SourceError(issue_codes.DATA_NOT_NAMED, 'no data was named; name a file, a directory or an s3:// prefix')
    files = tuple(file for item in items for file in (s3_files(item) if item.startswith('s3://') else local_files(Path(item))))
    formats = sorted({found.value for found in map(format_of, (file.path for file in files)) if found})
    if len(formats) > 1:
        message = f'{", ".join(items)} holds files of {len(formats)} formats, {", ".join(formats)}; profile one format at a time'
        raise SourceError(issue_codes.MIXED_FORMATS, message, value=formats)
    source_format = format_of(files[0].path)
    if source_format is None:
        message = f'{files[0].path} is not a format doppeldata reads; name a file ending in one of {", ".join(SUFFIX_FORMATS)}'
        raise SourceError(issue_codes.NO_READABLE_FILE, message, value=files[0].path, valid=list(SUFFIX_FORMATS))
    return Source(format=source_format, files=files, name=name_of(items[0]), roots=tuple(items))


def s3_url(item: str) -> str:
    """`item` with Hadoop's `s3a://` or `s3n://` written as `s3://`, the scheme boto3 lists and Glue and EMR read."""
    for scheme in HADOOP_S3_SCHEMES:
        if item.startswith(scheme):
            return 's3://' + item.removeprefix(scheme)
    return item


def is_spark_frame(data: Any) -> bool:
    """Whether `data` is a Spark DataFrame, classic or over Connect, told apart without importing pyspark."""
    return type(data).__module__.startswith('pyspark.sql') and hasattr(data, 'sparkSession')


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


def partition_keys(relative: PurePosixPath) -> tuple[str, ...]:
    """The keys of the `key=value` directories in a path relative to the root that was named."""
    return tuple(part.split('=', 1)[0] for part in relative.parent.parts if '=' in part)


def local_files(path: Path) -> list[SourceFile]:
    if path.is_file():
        return [local_file(path, PurePosixPath(path.name))]
    if not path.is_dir():
        raise SourceError(issue_codes.DATA_MISSING, f'{path} does not exist', value=str(path))
    found = sorted(
        (candidate, PurePosixPath(candidate.relative_to(path).as_posix()))
        for candidate in path.rglob('*')
        if candidate.is_file() and format_of(candidate.name)
    )
    if any(relative == UNFINISHED_WRITE for _, relative in found):
        raise SourceError(issue_codes.DATA_UNFINISHED, unfinished(str(path)), value=str(path))
    kept = [local_file(candidate, relative) for candidate, relative in found if not is_hidden(relative)]
    if not kept:
        message = f'{path} holds no file doppeldata reads, one ending in {", ".join(SUFFIX_FORMATS)}'
        raise SourceError(issue_codes.NO_READABLE_FILE, message, value=str(path), valid=list(SUFFIX_FORMATS))
    return kept


def local_file(path: Path, relative: PurePosixPath) -> SourceFile:
    stat = path.stat()
    # Resolved, so the same files fingerprint alike from any working directory.
    return SourceFile(str(path.resolve()), stat.st_size, str(stat.st_mtime_ns), partition_keys(relative))


@contextlib.contextmanager
def s3_errors(url: str, endpoint: str, *, doing: str = 'listed') -> Iterator[None]:
    """Each boto3 failure under `url` as a SourceError naming the endpoint, and exiting 4 when no answer came. `doing` says
    what could not be done to `url`, such as `read`.
    """
    try:
        yield
    except CREDENTIAL_ERRORS as error:
        raise SourceError(issue_codes.NO_CREDENTIALS, f'{url} could not be {doing}: {error}; {CREDENTIAL_ADVICE}') from error
    except AWS_ERRORS as error:
        if unanswered(error):
            raise SourceError(issue_codes.NO_ANSWER, no_answer(endpoint, 'S3', error)) from error
        if isinstance(error, botocore.exceptions.ClientError):
            raise SourceError(issue_codes.STORE, f'{url} could not be {doing}: {endpoint} answered {answered(error)}') from error
        raise SourceError(issue_codes.STORE, f'{url} could not be {doing} through {endpoint}: {error}') from error


def s3_files(url: str) -> list[SourceFile]:
    """The objects at an `s3://` URL, listed with the credentials and endpoint boto3 resolves from the environment."""
    parsed = urlparse(url)
    bucket, key = parsed.netloc, parsed.path.lstrip('/')
    try:
        _, client = s3_session()
    except InvalidArgument as error:
        raise SourceError(error.code, f'{url} could not be listed: {error.message}', role='environment') from error
    log.debug('%s: listing through %s', url, endpoint_named(client))
    with s3_errors(url, endpoint_named(client)):
        # Spark writes a table as a `name.parquet/` directory, so a key with a suffix may still be a prefix.
        if format_of(key) and not key.endswith('/') and (head := object_head(client, bucket, key)):
            return [SourceFile(url, head['ContentLength'], head['ETag'])]
        prefix = key.rstrip('/') + '/' if key else ''
        found = sorted(s3_objects(client, bucket, prefix), key=lambda file: file.path)
    if not found:
        message = f'{url} holds no object doppeldata reads, one ending in {", ".join(SUFFIX_FORMATS)}'
        raise SourceError(issue_codes.NO_READABLE_FILE, message, value=url, valid=list(SUFFIX_FORMATS))
    return found


def s3_manifest(url: str) -> Manifest | None:
    """The manifest a doppeldata write left under the `s3://` prefix `url`, or None where it holds none."""
    parsed = urlparse(url)
    prefix = parsed.path.strip('/')
    key = f'{prefix}/{MANIFEST_FILE_NAME}' if prefix else MANIFEST_FILE_NAME
    try:
        _, client = s3_session()
    except InvalidArgument as error:
        raise SourceError(error.code, f'{url} could not be read: {error.message}', role='environment') from error
    with s3_errors(url, endpoint_named(client), doing='read'):
        try:
            body: bytes = client.get_object(Bucket=parsed.netloc, Key=key)['Body'].read()
        except botocore.exceptions.ClientError as error:
            if error.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
                return None
            raise
    return Manifest.from_json(body.decode('utf-8'))


def s3_holds(url: str, name: str) -> bool:
    """Whether the `s3://` prefix `url` holds an object named `name` at its root."""
    parsed = urlparse(url)
    prefix = parsed.path.strip('/')
    try:
        _, client = s3_session()
    except InvalidArgument as error:
        raise SourceError(error.code, f'{url} could not be read: {error.message}', role='environment') from error
    with s3_errors(url, endpoint_named(client), doing='read'):
        return object_head(client, parsed.netloc, f'{prefix}/{name}' if prefix else name) is not None


def object_head(client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    """The object's metadata, or None when no object has that key."""
    try:
        head: dict[str, Any] = client.head_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as error:
        if error.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
            return None
        raise
    return head


def check_unchanged(source: Source) -> None:
    """Raise when a file of `source` changed, appeared or went away while it was profiled."""
    if not source.roots:
        return
    changed = sorted({file.path for file in set(resolve(list(source.roots)).files) ^ set(source.files)})
    if changed:
        listed = ', '.join(changed[:5]) + (f' and {len(changed) - 5} more' if len(changed) > 5 else '')
        raise SourceError(issue_codes.DATA_CHANGED, f'{listed} changed while it was profiled; profile it again once it stops changing')


def s3_objects(client: Any, bucket: str, prefix: str) -> Iterator[SourceFile]:
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get('Contents', []):
            relative = PurePosixPath(entry['Key'].removeprefix(prefix))
            if relative == UNFINISHED_WRITE:
                raise SourceError(issue_codes.DATA_UNFINISHED, unfinished(f's3://{bucket}/{prefix}'), value=f's3://{bucket}/{prefix}')
            if format_of(entry['Key']) and not is_hidden(relative):
                yield SourceFile(f's3://{bucket}/{entry["Key"]}', entry['Size'], entry['ETag'], partition_keys(relative))
