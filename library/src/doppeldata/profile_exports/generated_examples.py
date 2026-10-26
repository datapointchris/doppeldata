"""Example values for a contract, generated from the profile rather than read from the data, so none is a real value.

The rows are written as NDJSON and read back with the standard library, so no Arrow library is needed.
"""

import json
import tempfile
from pathlib import Path
from typing import Any

from doppeldata.profile_format import Profile

EXAMPLES = 3
EXAMPLE_SEED = 0
# Enough rows that a column with nulls still yields EXAMPLES values.
EXAMPLE_ROWS = 64


def generated_examples(profile: Profile) -> dict[str, list[Any]]:
    """Up to EXAMPLES distinct values of each column, in the order the rows generated with EXAMPLE_SEED hold them."""
    from doppeldata.dataset_generation import write

    with tempfile.TemporaryDirectory(prefix='doppeldata-examples-') as directory:
        target = Path(directory) / 'examples.ndjson'
        write(profile, target, rows=EXAMPLE_ROWS, seed=EXAMPLE_SEED, format='ndjson', partition_by=[], writer={'compression': 'none'})
        rows = [json.loads(line) for line in target.read_text(encoding='utf-8').splitlines() if line]
    examples: dict[str, list[Any]] = {}
    for row in rows:
        for name, value in row.items():
            found = examples.setdefault(name, [])
            if value is not None and len(found) < EXAMPLES and value not in found:
                found.append(value)
    return examples
