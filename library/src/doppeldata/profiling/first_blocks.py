"""A text file's first block, which its dialect and its JSON layout are read from."""

import pyarrow as pa

FIRST_BLOCK_BYTES = 64 * 1024


def first_block(path: str) -> bytes:
    """The file's first bytes, decompressed by its suffix."""
    with pa.input_stream(path, compression='detect') as stream:
        return bytes(stream.read(FIRST_BLOCK_BYTES))
