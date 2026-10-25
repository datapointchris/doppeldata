"""A text source's columns: the share of raw values parsing as each type, and the type each column is read as."""

import dataclasses as dc

from doppeldata.profiling import json_paths
from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import ParsedType
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Table
from doppeldata.profiling.statistic_catalog import RAW_TYPE_FOLDS
from doppeldata.profiling.statistic_catalog import RAW_TYPE_LAYOUTS
from doppeldata.profiling.statistic_catalog import READ_AS

# The rows each date and timestamp layout is tried on, before the best one counts every row.
LAYOUT_TRIAL_ROWS = 10_000
# What a delimited source writes for a missing value beside an empty field, which DuckDB reads as null.
NULL_TOKENS = ('NA', 'N/A', 'NULL', 'null', '\\N', 'None')
# The token an empty field is counted under.
EMPTY = ''

Layouts = dict[tuple[str, str], str | None]


@dc.dataclass(frozen=True)
class TextColumn:
    """A text column's raw nulls, its non-null values by the type each parses as, and the type it is read as.

    `missing_tokens` holds the rows of each null token the column writes, with an empty field's under `EMPTY`.
    """

    null_count: int
    counts: dict[str, int]
    parsed: ParsedType
    missing_tokens: dict[str, int] = dc.field(default_factory=dict)

    @property
    def non_null(self) -> int:
        return sum(self.counts.values())


def null_tokens(backend: Backend, table: Table, specs: list[ColumnSpec]) -> dict[str, dict[str, int]]:
    """Each column's rows by the null token they hold, an empty field's under `EMPTY`, leaving out a token no row holds."""
    requests = [
        request for spec in specs for request in (Request(spec.name, 'null_count'), Request(spec.name, 'listed_value_counts', NULL_TOKENS))
    ]
    counted = backend.aggregate(table, requests)
    tokens = {}
    for spec in specs:
        held = {EMPTY: counted[Request(spec.name, 'null_count')]} | dict(
            zip(NULL_TOKENS, counted[Request(spec.name, 'listed_value_counts', NULL_TOKENS)], strict=True)
        )
        tokens[spec.name] = {token: rows for token, rows in held.items() if rows}
    return tokens


def type_text_columns(backend: Backend, table: Table, specs: list[ColumnSpec], seed: int, *, json: bool = False) -> dict[str, TextColumn]:
    """Each text column's raw type counts and the type it is read as. A JSON path's values are typed by `json_raw_type_counts`."""
    layouts = best_layouts(backend, table, specs, seed)
    requests = [Request(None, 'rows')]
    for spec in specs:
        requests += [Request(spec.name, 'null_count'), type_counts(spec.name, layouts, json=json)]
        if not json:
            requests.append(Request(spec.name, 'padded_digits'))
    counted = backend.aggregate(table, requests)
    columns = {}
    for spec in specs:
        nulls = counted[Request(spec.name, 'null_count')]
        counts = dict(zip(READ_AS, counted[type_counts(spec.name, layouts, json=json)], strict=True))
        read = {raw: counts[raw] + sum(counts[folded] for folded in RAW_TYPE_FOLDS.get(raw, ())) for raw in READ_AS}
        dominant = max(READ_AS, key=lambda raw: (read[raw], -READ_AS.index(raw))) if any(counts.values()) else 'string'
        if not json and counted[Request(spec.name, 'padded_digits')]:
            dominant = 'string'
        for folded in RAW_TYPE_FOLDS.get(dominant, ()):
            counts[dominant] += counts[folded]
            counts[folded] = 0
        type_column = json_paths.type_column(spec.name) if json else None
        columns[spec.name] = TextColumn(
            nulls, counts, ParsedType(dominant, layouts.get((spec.name, dominant)), json_type_column=type_column)
        )
    return columns


def type_counts(name: str, layouts: Layouts, *, json: bool) -> Request:
    if json:
        return Request(name, 'json_raw_type_counts', (column_layouts(layouts, name), json_paths.type_column(name)))
    return Request(name, 'raw_type_counts', column_layouts(layouts, name))


def column_layouts(layouts: Layouts, name: str) -> tuple[tuple[str, str], ...]:
    """The column's best layout for each raw type read through one, leaving out a type no layout read."""
    return tuple((raw, layout) for raw in RAW_TYPE_LAYOUTS if (layout := layouts[name, raw]))


def best_layouts(backend: Backend, table: Table, specs: list[ColumnSpec], seed: int) -> Layouts:
    """For each column, the date and the timestamp layout that read the most sampled values, or None where none reads one."""
    trial_table = backend.sampled(table, LAYOUT_TRIAL_ROWS, seed)
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
