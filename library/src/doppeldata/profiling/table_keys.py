"""A table's keys and repeated rows, counted over every row it holds, whether or not its columns were sampled."""

import dataclasses as dc
from typing import Any

from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Table
from doppeldata.profiling.recorded_counts import rate
from doppeldata.profiling.statistic_catalog import ColumnKind

# A column or pair is a key where at least this share of its rows hold a value no other row holds.
KEY_SHARE = 0.99
# A single column's rows held once are counted where its distinct values reach this share of the rows: below KEY_SHARE
# by more than three of HyperLogLog++'s 1% standard errors, so Spark's estimate passes every key Polars' count does.
NOMINATED_SHARE = 0.95
# The pairs of columns tried as a composite key on the dependence sample, where no single column is unique.
COMPOSITE_PAIRS = 20
KEY_KINDS = frozenset({ColumnKind.INTEGER, ColumnKind.STRING})
COMPOSITE_KINDS = KEY_KINDS | {ColumnKind.DATE, ColumnKind.DATETIME}


@dc.dataclass(frozen=True)
class KeyColumn:
    """What a key is sought from in one column: its spec, its distinct values over every row, and whether the profiled
    rows hold a null in it."""

    spec: ColumnSpec
    distinct: int
    has_nulls: bool


def table_keys(backend: Backend, full: Table, sample: Table, columns: list[KeyColumn], rows: int, k_min: int) -> dict[str, Any]:
    """The dataset's `duplicate_row_rate` and `keys`, from `full`, which holds every row, and `sample`, the dependence
    sample, which nominates a composite key before every row confirms it. A table of fewer than k_min rows is keyed as
    any other, and the privacy policy withholds its keys."""
    names = tuple(column.spec.name for column in columns)
    singles = [
        column for column in columns if column.spec.kind in KEY_KINDS and not column.has_nulls and column.distinct >= NOMINATED_SHARE * rows
    ]
    requests = [Request(None, 'distinct_rows', names)] + [Request(None, 'unique_rows', (column.spec.name,)) for column in singles]
    counted = backend.aggregate(full, requests) if names else {}
    found: dict[str, Any] = {}
    if names and rows:
        repeated = rows - counted[Request(None, 'distinct_rows', names)]
        found['duplicate_row_rate'] = rate(repeated, rows) if repeated >= k_min else 0.0
    unique = {column.spec.name: counted[Request(None, 'unique_rows', (column.spec.name,))] for column in singles}
    keys = [key for name, rows_unique in unique.items() if (key := written_key((name,), rows_unique, rows, k_min))]
    # A column no two rows share keys the table alone, and a nearly unique one, such as a free-text comment, leaves the
    # pair a child refers to still to find.
    if not any(rows_unique == rows for rows_unique in unique.values()):
        single = {key['columns'][0] for key in keys}
        keys += composite_keys(backend, full, sample, [column for column in columns if column.spec.name not in single], rows, k_min)
    if keys:
        primary = next((key for key in keys if key['uniqueness'] == 1.0), None)
        section: dict[str, Any] = {}
        if primary:
            section['primary'] = primary
        candidates = [key for key in keys if key is not primary]
        if candidates:
            section['candidates'] = candidates
        found['keys'] = section
    return found


def composite_keys(backend: Backend, full: Table, sample: Table, columns: list[KeyColumn], rows: int, k_min: int) -> list[dict[str, Any]]:
    """The pairs of columns no other row's pair repeats, tried most distinct first among those whose distinct values
    could make every row's pair unique, nominated where the sample repeats none, and confirmed over every row. A pair
    names its columns in table order, so an estimated distinct count never reorders it."""
    position = {column.spec.name: at for at, column in enumerate(columns)}
    eligible = sorted(
        (column for column in columns if column.spec.kind in COMPOSITE_KINDS and not column.has_nulls and column.distinct > 1),
        key=lambda column: -column.distinct,
    )
    pairs = [
        tuple(sorted((first.spec.name, second.spec.name), key=position.__getitem__))
        for at, first in enumerate(eligible)
        for second in eligible[at + 1 :]
        if first.distinct * second.distinct >= rows
    ][:COMPOSITE_PAIRS]
    if not pairs:
        return []
    tried = backend.aggregate(sample, [Request(None, 'rows'), *(Request(None, 'distinct_rows', pair) for pair in pairs)])
    nominated = [pair for pair in pairs if tried[Request(None, 'distinct_rows', pair)] == tried[Request(None, 'rows')]]
    confirmed = backend.aggregate(full, [Request(None, 'unique_rows', pair) for pair in nominated])
    return [key for pair in nominated if (key := written_key(pair, confirmed[Request(None, 'unique_rows', pair)], rows, k_min))]


def written_key(columns: tuple[str, ...], unique: int, rows: int, k_min: int) -> dict[str, Any] | None:
    """A key as the profile writes it, or None where its uniqueness is under `KEY_SHARE`. Fewer than k_min rows sharing
    a value are not stated, so the key is then written as unique."""
    shared = rows - unique
    uniqueness = 1.0 if shared < k_min else rate(unique, rows)
    return {'columns': list(columns), 'uniqueness': uniqueness} if uniqueness >= KEY_SHARE else None
