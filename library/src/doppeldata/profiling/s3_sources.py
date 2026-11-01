"""The objects under an `s3://` prefix a profile describes, listed and read through boto3. Only an `s3://` source imports
this module, so a profile of local files or of a table needs no boto3."""

import contextlib
import logging
from collections.abc import Iterator
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
from doppeldata.manifest import Manifest
from doppeldata.manifest import unfinished
from doppeldata.profiling.backend import SourceFile
from doppeldata.profiling.sources import SUFFIX_FORMATS
from doppeldata.profiling.sources import UNFINISHED_WRITE
from doppeldata.profiling.sources import format_of
from doppeldata.profiling.sources import is_hidden
from doppeldata.profiling.sources import partition_keys
from doppeldata.s3_access import endpoint_named
from doppeldata.s3_access import s3_session

log = logging.getLogger(__name__)


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
    log.debug(f'{url}: listing through {endpoint_named(client)}')
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


def s3_objects(client: Any, bucket: str, prefix: str) -> Iterator[SourceFile]:
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get('Contents', []):
            relative = PurePosixPath(entry['Key'].removeprefix(prefix))
            if relative == UNFINISHED_WRITE:
                raise SourceError(issue_codes.DATA_UNFINISHED, unfinished(f's3://{bucket}/{prefix}'), value=f's3://{bucket}/{prefix}')
            if format_of(entry['Key']) and not is_hidden(relative):
                yield SourceFile(f's3://{bucket}/{entry["Key"]}', entry['Size'], entry['ETag'], partition_keys(relative))
