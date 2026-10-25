"""A text source's columns: the share of raw values parsing as each type, and the type each column is read as."""

import dataclasses as dc

from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import ParsedType
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import Table
from doppeldata.profiling.statistic_catalog import DATE_FORMATS
from doppeldata.profiling.statistic_catalog import TIMESTAMP_FORMATS

# The rows each date and timestamp layout is tried on, before the best one counts every row.
LAYOUT_TRIAL_ROWS = 10_000
# The order a tie between two types' shares goes in.
RAW_TYPES = ('integer', 'decimal', 'boolean', 'date', 'timestamp', 'string')


@dc.dataclass(frozen=True)
class TextColumn:
    """A text column's raw nulls, its non-null values by the type each parses as, and the type it is read as."""

    null_count: int
    counts: dict[str, int]
    parsed: ParsedType

    @property
    def non_null(self) -> int:
        return sum(self.counts.values())


def type_text_columns(backend: Backend, table: Table, specs: list[ColumnSpec], seed: int) -> dict[str, TextColumn]:
    layouts = best_layouts(backend, table, specs, seed)
    requests = [Request(None, 'rows')]
    for spec in specs:
        requests += [Request(spec.name, name) for name in ('null_count', 'raw_integer', 'raw_decimal', 'raw_boolean')]
        requests += [Request(spec.name, f'raw_{raw}', layout) for raw in ('date', 'timestamp') if (layout := layouts[spec.name, raw])]
    counted = backend.aggregate(table, requests)
    rows = counted[Request(None, 'rows')]
    columns = {}
    for spec in specs:
        nulls = counted[Request(spec.name, 'null_count')]
        counts = {raw: counted.get(Request(spec.name, f'raw_{raw}', layouts.get((spec.name, raw))), 0) or 0 for raw in RAW_TYPES[:-1]}
        counts['string'] = rows - nulls - sum(counts.values())
        dominant = max(RAW_TYPES, key=lambda raw: (counts[raw], -RAW_TYPES.index(raw))) if rows > nulls else 'string'
        if dominant == 'decimal':
            # An integer is a decimal too, so in a decimal column it is no foreign value.
            counts['decimal'] += counts['integer']
            counts['integer'] = 0
        columns[spec.name] = TextColumn(nulls, counts, ParsedType(dominant, layouts.get((spec.name, dominant))))
    return columns


def best_layouts(backend: Backend, table: Table, specs: list[ColumnSpec], seed: int) -> dict[tuple[str, str], str | None]:
    """For each column, the date and the timestamp layout that read the most sampled values, or None where none reads one."""
    trial_table = backend.scan(Source(format=None, table=backend.sample(table, LAYOUT_TRIAL_ROWS, seed)))
    candidates = {'date': DATE_FORMATS, 'timestamp': TIMESTAMP_FORMATS}
    trials = [Request(spec.name, f'raw_{raw}', layout) for spec in specs for raw, layouts in candidates.items() for layout in layouts]
    tried = backend.aggregate(trial_table, trials)
    best: dict[tuple[str, str], str | None] = {}
    for spec in specs:
        for raw, layouts in candidates.items():
            scores = [(tried[Request(spec.name, f'raw_{raw}', layout)] or 0, -index, layout) for index, layout in enumerate(layouts)]
            count, _, layout = max(scores)
            best[spec.name, raw] = layout if count > 0 else None
    return best
