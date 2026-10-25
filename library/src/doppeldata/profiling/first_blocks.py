"""A text file's first block, which its dialect and its JSON layout are read from, on disk or on S3."""

import contextlib
import importlib
import tempfile
from collections.abc import Iterator
from pathlib import PurePosixPath

import pyarrow as pa

FIRST_BLOCK_BYTES = 64 * 1024
# The compressed bytes read of an S3 object to give a first block, since text compresses to well under a quarter.
COMPRESSED_BYTES_READ = 4 * FIRST_BLOCK_BYTES
# The codec pyarrow decompresses each compressed suffix with.
CODECS = {'.gz': 'gzip', '.zst': 'zstd', '.bz2': 'bz2'}


def first_block(path: str) -> bytes:
    """The file's first bytes, decompressed by its suffix."""
    if path.startswith('s3://'):
        return s3_first_block(path)
    with pa.input_stream(path, compression='detect') as stream:
        return bytes(stream.read(FIRST_BLOCK_BYTES))


def s3_first_block(url: str) -> bytes:
    """An S3 object's first bytes, read in one ranged request through the boto3 client that lists the source."""
    sources = importlib.import_module('doppeldata.profiling.sources')
    s3_access = importlib.import_module('doppeldata.s3_access')
    bucket, key = url.removeprefix('s3://').split('/', 1)
    codec = CODECS.get(PurePosixPath(key).suffix.lower())
    end = (COMPRESSED_BYTES_READ if codec else FIRST_BLOCK_BYTES) - 1
    _, client = s3_access.s3_session()
    with sources.s3_errors(url, s3_access.endpoint_named(client), doing='read'):
        raw: bytes = client.get_object(Bucket=bucket, Key=key, Range=f'bytes=0-{end}')['Body'].read()
    if codec is None:
        return raw
    decompressed = pa.CompressedInputStream(pa.BufferReader(raw), codec)
    held = bytearray()
    # A block cut mid-stream ends in an error once its whole bytes are read, and what came before it stands.
    with contextlib.suppress(OSError):
        while len(held) < FIRST_BLOCK_BYTES and (chunk := decompressed.read(FIRST_BLOCK_BYTES - len(held))):
            held += chunk
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
