"""
doppeldata's engine, compiled.
"""

from typing import Final

VERSION: Final[str]
"""
The engine's release, which is the tag the wheel was built from.
"""

def profile_digest(canonical: bytes) -> str:
    """
    Reads a profile from its canonical JSON and returns ``sha256:<hex>`` over those bytes, or
    raises ValueError when the engine cannot read it.
    """
