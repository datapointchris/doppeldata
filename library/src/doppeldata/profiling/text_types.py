"""A text source's columns: the share of raw values parsing as each type, and the type each column is read as."""

import dataclasses as dc

from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import ParsedType
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import Table
from doppeldata.profiling.statistic_catalog import RAW_TYPE_FOLDS
from doppeldata.profiling.statistic_catalog import RAW_TYPE_LAYOUTS
from doppeldata.profiling.statistic_catalog import READ_AS

# The rows each date and timestamp layout is tried on, before the best one counts every row.
LAYOUT_TRIAL_ROWS = 10_000

Layouts = dict[tuple[str, str], str | None]


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
        requests += [
            Request(spec.name, 'null_count'),
            Request(spec.name, 'raw_type_counts', column_layouts(layouts, spec.name)),
            Request(spec.name, 'padded_digits'),
        ]
    counted = backend.aggregate(table, requests)
    columns = {}
    for spec in specs:
        nulls = counted[Request(spec.name, 'null_count')]
        counts = dict(zip(READ_AS, counted[Request(spec.name, 'raw_type_counts', column_layouts(layouts, spec.name))], strict=True))
        dominant = max(READ_AS, key=lambda raw: (counts[raw], -READ_AS.index(raw))) if any(counts.values()) else 'string'
        if counted[Request(spec.name, 'padded_digits')]:
            dominant = 'string'
        for folded in RAW_TYPE_FOLDS.get(dominant, ()):
            counts[dominant] += counts[folded]
            counts[folded] = 0
        columns[spec.name] = TextColumn(nulls, counts, ParsedType(dominant, layouts.get((spec.name, dominant))))
    return columns


def column_layouts(layouts: Layouts, name: str) -> tuple[tuple[str, str], ...]:
    """The column's best layout for each raw type read through one, leaving out a type no layout read."""
    return tuple((raw, layout) for raw in RAW_TYPE_LAYOUTS if (layout := layouts[name, raw]))


def best_layouts(backend: Backend, table: Table, specs: list[ColumnSpec], seed: int) -> Layouts:
    """For each column, the date and the timestamp layout that read the most sampled values, or None where none reads one."""
    trial_table = backend.scan(Source(format=None, table=backend.sample(table, LAYOUT_TRIAL_ROWS, seed)))
    trials = [
        Request(spec.name, 'raw_type_counts', ((raw, layout),))
        for spec in specs
        for raw, layouts in RAW_TYPE_LAYOUTS.items()
        for layout in layouts
    ]
    tried = backend.aggregate(trial_table, trials)
    best: Layouts = {}
    for spec in specs:
        for raw, layouts in RAW_TYPE_LAYOUTS.items():
            read = READ_AS.index(raw)
            scores = [
                (tried[Request(spec.name, 'raw_type_counts', ((raw, layout),))][read], -index, layout)
                for index, layout in enumerate(layouts)
            ]
            count, _, layout = max(scores)
            best[spec.name, raw] = layout if count > 0 else None
    return best
