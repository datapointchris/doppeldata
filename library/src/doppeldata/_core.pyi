"""
doppeldata's engine, compiled.
"""

from typing import Final

from _typeshed import SupportsGetItem

FORMAT_VERSION: Final[int]
"""
The profile format version the engine reads.
"""

VERSION: Final[str]
"""
The engine's release, which is the tag the wheel was built from.
"""

def generalize_pattern(values: SupportsGetItem[int, str]) -> str | None:
    """
    The anchored regular expression grex generalizes the values to, with each digit as a digit
    class, or None when it would hold a literal or a character class only some values have.
    """

def schema_file_names() -> dict[str, str]:
    """
    The file name under ``doppeldata/schemas/`` of each JSON Schema the library ships, by name.
    """
