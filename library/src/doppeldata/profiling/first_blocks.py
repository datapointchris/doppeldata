"""A text file's first block, which its dialect and its JSON layout are read from, on disk or on S3."""

import concurrent.futures
import contextlib
import tempfile
from collections.abc import Iterator
from collections.abc import Sequence
from pathlib import PurePosixPath
from typing import Any

import pyarrow as pa

from doppeldata import _core
from doppeldata import issue_codes
from doppeldata.errors import SourceError

# Held by the writer too, which starts no invalid_utf8 fault inside it.
FIRST_BLOCK_BYTES = _core.FIRST_BLOCK_BYTES
# The compressed bytes read of an S3 object to give a first block, since text compresses to well under a quarter.
COMPRESSED_BYTES_READ = 4 * FIRST_BLOCK_BYTES
# The codec pyarrow decompresses each compressed suffix with.
CODECS = {'.gz': 'gzip', '.zst': 'zstd', '.bz2': 'bz2'}
# The S3 objects whose first blocks are read at once.
BLOCK_THREADS = 16


def first_block(path: str) -> bytes:
    """The file's first bytes, decompressed by its suffix. One that cannot be read or decompressed raises a SourceError
    naming it."""
    if path.startswith('s3://'):
        return s3_first_block(path, s3_client(path))
    try:
        with pa.input_stream(path, compression='detect') as stream:
            return bytes(stream.read(FIRST_BLOCK_BYTES))
    except (OSError, pa.ArrowException) as error:
        raise SourceError(issue_codes.DATA_UNREADABLE, f'{path} could not be read: {error}') from error


def first_blocks(paths: Sequence[str]) -> list[bytes]:
    """Each file's first block, as `first_block` reads it, those on S3 through one client on `BLOCK_THREADS` threads."""
    if not any(path.startswith('s3://') for path in paths):
        return [first_block(path) for path in paths]
    client = s3_client(next(path for path in paths if path.startswith('s3://')))
    with concurrent.futures.ThreadPoolExecutor(BLOCK_THREADS) as pool:
        return list(pool.map(lambda path: s3_first_block(path, client) if path.startswith('s3://') else first_block(path), paths))


def s3_client(url: str) -> Any:
    from doppeldata import s3_access

    return s3_access.reading_client(url)


def s3_first_block(url: str, client: Any) -> bytes:
    """An S3 object's first bytes, read in one ranged request through a boto3 client of the AWS environment's, as the
    listing's is."""
    from doppeldata import s3_access

    bucket, key = url.removeprefix('s3://').split('/', 1)
    codec = CODECS.get(PurePosixPath(key).suffix.lower())
    end = (COMPRESSED_BYTES_READ if codec else FIRST_BLOCK_BYTES) - 1
    with s3_access.read_errors(url, s3_access.endpoint_named(client), doing='read'):
        raw: bytes = client.get_object(Bucket=bucket, Key=key, Range=f'bytes=0-{end}')['Body'].read()
    if codec is None:
        return raw
    decompressed = pa.CompressedInputStream(pa.BufferReader(raw), codec)
    held = bytearray()
    try:
        while len(held) < FIRST_BLOCK_BYTES and (chunk := decompressed.read(FIRST_BLOCK_BYTES - len(held))):
            held += chunk
    except (OSError, pa.ArrowException) as error:
        # A block cut mid-stream ends in an error once its whole bytes are read, and what came before it stands.
        if not held:
            message = f'the first {len(raw)} bytes of {url} decompressed as {codec} to nothing: {error}'
            raise SourceError(issue_codes.DATA_UNREADABLE, message) from error
    return bytes(held)


@contextlib.contextmanager
def local_copy(path: str, block: bytes) -> Iterator[str]:
    """A local file a sniffer can open: the file itself, or a temporary file holding an S3 object's first block."""
    if not path.startswith('s3://'):
        yield path
        return
    # A block cut mid-record ends at its last line break, so the sniffer reads whole records.
    if len(block) >= FIRST_BLOCK_BYTES and b'\n' in block:
        block = block[: block.rfind(b'\n') + 1]
    with tempfile.NamedTemporaryFile(suffix='.csv') as copy:
        copy.write(block)
        copy.flush()
        yield copy.name
