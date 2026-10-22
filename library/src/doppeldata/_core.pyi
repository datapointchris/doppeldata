"""
doppeldata's engine, compiled.
"""

from typing import Final

FORMAT_VERSION: Final[int]
"""
The profile format version the engine reads.
"""

VERSION: Final[str]
"""
The engine's release, which is the tag the wheel was built from.
"""

def schema_file_names() -> dict[str, str]:
    """
    The file name under ``doppeldata/schemas/`` of each JSON Schema the library ships, by name.
    """
