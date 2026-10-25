"""The generator hint the library fills a typed column with, pinned beside the stream version it generates under.

The engine reads only `generator_hint`, so its own determinism test never sees this fill. `crates/check-stream-version-raised.sh`
reads both pins here and refuses a new hash at a released stream version.
"""

import hashlib
import json

from doppeldata import _core
from doppeldata.name_hinted_types import GENERAL
from doppeldata.name_hinted_types import with_type_hints

PINNED_STREAM_VERSION = 8
PINNED_TYPE_HINTS = 'f47515ec7fbd475e9970f882957ee562f22417fb4632f569292108b88542d3e3'


def test_a_typed_column_naming_no_hint_takes_a_hint_that_changes_only_with_the_stream_version():
    """A hand-written `type: email` column, under a name its type hints and under one that hints nothing."""
    names = [hinted.name_hints[0] for hinted in GENERAL] + [f'typed_{hinted.type}' for hinted in GENERAL]
    types = [hinted.type for hinted in GENERAL] * 2
    columns = [
        {'name': name, 'physical_type': 'string', 'logical_type': 'string', 'semantic': {'type': semantic_type}}
        for name, semantic_type in zip(names, types, strict=True)
    ]
    filled = with_type_hints({'profile_version': 1, 'kind': 'table', 'name': 'hints', 'columns': columns})
    digest = hashlib.sha256(json.dumps(filled['columns'], sort_keys=True).encode()).hexdigest()
    assert (_core.STREAM_VERSION, digest) == (PINNED_STREAM_VERSION, PINNED_TYPE_HINTS), (
        'a hint the library fills changed; a change to a released stream version raises STREAM_VERSION'
    )
