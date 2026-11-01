"""The objects under an `s3://` prefix a profile describes, listed through boto3. Only an `s3://` source imports this
module, so a profile of local files or of a table needs no boto3."""

import logging
from collections.abc import Iterator
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

from doppeldata import issue_codes
from doppeldata.errors import SourceError
from doppeldata.manifest import unfinished
from doppeldata.profiling.backend import SourceFile
from doppeldata.profiling.sources import SUFFIX_FORMATS
from doppeldata.profiling.sources import UNFINISHED_WRITE
from doppeldata.profiling.sources import format_of
from doppeldata.profiling.sources import is_hidden
from doppeldata.profiling.sources import partition_keys
from doppeldata.s3_access import endpoint_named
from doppeldata.s3_access import object_head
from doppeldata.s3_access import read_errors
from doppeldata.s3_access import reading_client

log = logging.getLogger(__name__)


def s3_files(url: str) -> list[SourceFile]:
    """The objects at an `s3://` URL, listed with the credentials and endpoint boto3 resolves from the environment."""
    parsed = urlparse(url)
    bucket, key = parsed.netloc, parsed.path.lstrip('/')
    client = reading_client(url, doing='listed')
    log.debug(f'{url}: listing through {endpoint_named(client)}')
    with read_errors(url, endpoint_named(client)):
        # Spark writes a table as a `name.parquet/` directory, so a key with a suffix may still be a prefix.
        if format_of(key) and not key.endswith('/') and (head := object_head(client, bucket, key)):
            return [SourceFile(url, head['ContentLength'], head['ETag'])]
        prefix = key.rstrip('/') + '/' if key else ''
        found = sorted(s3_objects(client, bucket, prefix), key=lambda file: file.path)
    if not found:
        message = f'{url} holds no object doppeldata reads, one ending in {", ".join(SUFFIX_FORMATS)}'
        raise SourceError(issue_codes.NO_READABLE_FILE, message, value=url, valid=list(SUFFIX_FORMATS))
    return found


def s3_objects(client: Any, bucket: str, prefix: str) -> Iterator[SourceFile]:
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get('Contents', []):
            relative = PurePosixPath(entry['Key'].removeprefix(prefix))
            if relative == UNFINISHED_WRITE:
                raise SourceError(issue_codes.DATA_UNFINISHED, unfinished(f's3://{bucket}/{prefix}'), value=f's3://{bucket}/{prefix}')
            if format_of(entry['Key']) and not is_hidden(relative):
                yield SourceFile(f's3://{bucket}/{entry["Key"]}', entry['Size'], entry['ETag'], partition_keys(relative))
