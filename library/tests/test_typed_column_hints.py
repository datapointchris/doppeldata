"""The generator hint the library fills a typed column with, pinned beside the stream version it generates under.

The engine reads only `generator_hint`, so its own determinism test never sees this fill. `crates/check-stream-version-raised.sh`
reads both pins here and refuses a new hash at a released stream version.
"""

import hashlib
import json

from doppeldata import _core
from doppeldata.name_hinted_types import GENERAL
from doppeldata.name_hinted_types import with_type_hints

PINNED_STREAM_VERSION = 12
PINNED_TYPE_HINTS = 'fd356e89a3704105bd6ea2848c1c9f68ceed6a6a887744298f3c7e27ca97e632'


def test_a_typed_column_naming_no_hint_takes_a_hint_that_changes_only_with_the_stream_version():
    """A hand-written `type: email` column holding no hint, one for each type the general pack declares."""
    types = dict.fromkeys(hinted.type for hinted in GENERAL)
    columns = [
        {'name': f'typed_{semantic_type}', 'physical_type': 'string', 'logical_type': 'string', 'semantic': {'type': semantic_type}}
        for semantic_type in types
    ]
    filled = with_type_hints({'kind': 'table', 'name': 'hints', 'columns': columns})
    digest = hashlib.sha256(json.dumps(filled['columns'], sort_keys=True).encode()).hexdigest()
    assert (_core.STREAM_VERSION, digest) == (PINNED_STREAM_VERSION, PINNED_TYPE_HINTS), (
        'a hint the library fills changed; a change to a released stream version raises STREAM_VERSION'
    )
