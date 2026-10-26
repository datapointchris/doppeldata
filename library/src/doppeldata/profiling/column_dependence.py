"""How a table's columns depend on each other: correlations, conditional tables and functional dependencies.

Each correlation is measured on the dependence sample, the seeded sample the fits read. Each count a conditional
table or a dependency rests on is counted over the rows the profile describes, so the floor holds it exactly.
"""

import collections
import dataclasses as dc
import math
from typing import Any

from doppeldata.column_name_words import ZIP_HINTS
from doppeldata.column_name_words import hint_runs_in
from doppeldata.column_name_words import name_words
from doppeldata.privacy_policy.literal_fields import listed_values
from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import Table
from doppeldata.profiling.recorded_counts import NUMERIC_TOP_VALUES_THRESHOLD
from doppeldata.profiling.recorded_counts import RATE_DIGITS
from doppeldata.profiling.recorded_counts import TOP_VALUES_THRESHOLD
from doppeldata.profiling.recorded_counts import rate
from doppeldata.profiling.recorded_counts import significant
from doppeldata.profiling.statistic_catalog import NUMERIC
from doppeldata.profiling.statistic_catalog import TEMPORAL
from doppeldata.profiling.statistic_catalog import ColumnKind
from doppeldata.profiling.table_keys import KEY_SHARE

# A pair is recorded where its correlation's magnitude reaches this, and the strongest this many are kept.
CORRELATION_FLOOR = 0.3
CORRELATIONS_KEPT = 200
# A pair is measured only where this many sampled rows hold both its values, or k_min where that is more, so a value at
# CORRELATION_FLOOR lies three standard errors from none.
PAIR_ROWS = 100
# Cochran's rule for a contingency table's chi-squared: Cramér's V is measured where the sample holds this many rows per
# cell, and η where it holds this many per category.
ROWS_PER_CELL = 5
# A string or integer column holding no more distinct values than a string lists is categorical, as a boolean is.
CATEGORICAL_VALUES = TOP_VALUES_THRESHOLD
# A conditional table is given the column of a correlated categorical pair holding fewer values, where it holds this many at most.
GIVEN_VALUES = 50
# A categorical pair whose sampled Cramér's V reaches this is counted over every row, and gets a conditional table where
# the V of those counts reaches CORRELATION_FLOOR. So which tables stand follows the counts, not either engine's sample.
CONDITIONAL_TRIED = 0.2
# The functional dependencies tried, those a name hints first, then from the left-hand column holding fewest values.
DEPENDENCY_TESTS = 100
# A dependency is recorded where the left-hand values k_min rows hold cover at least this share of the rows holding
# both columns, and at most this share of those rows hold a left-hand value holding two right-hand values.
DEPENDENCY_SUPPORT = 0.5
DEPENDENCY_VIOLATION = 0.01
DEPENDENT_VALUES = NUMERIC_TOP_VALUES_THRESHOLD
DETERMINING_KINDS = frozenset({ColumnKind.INTEGER, ColumnKind.STRING, ColumnKind.BOOLEAN, ColumnKind.DATE, ColumnKind.DECIMAL})
# Words naming a place a ZIP code lies in, which a ZIP column is tried as determining first.
PLACE_HINTS = ('state', 'city', 'county', 'region')
SPEARMAN = 'spearman'
CRAMERS_V = 'cramers_v'
CORRELATION_RATIO = 'correlation_ratio'
RANKED = 'ranked'
CATEGORICAL = 'categorical'


@dc.dataclass(frozen=True)
class DependenceColumn:
    """One column as its dependence on the others is sought: its spec, its distinct values over every row, and its
    entry as the statistics pass wrote it, before the policy withholds anything."""

    spec: ColumnSpec
    distinct: int
    entry: dict[str, Any]

    @property
    def name(self) -> str:
        return self.spec.name


@dc.dataclass(frozen=True)
class Dependence:
    """The dataset's `correlations`, `conditionals` and `dependencies`, and the fewest rows a cell of each conditional
    table rests on, by its given and child column, which the policy's audit records."""

    sections: dict[str, Any]
    cell_floors: dict[tuple[str, str], int]


def column_dependence(
    backend: Backend, sample: Table, described: Table, columns: list[DependenceColumn], rows: int, k_min: int
) -> Dependence:
    """The dependence between `columns`, measured on `sample` and counted on `described`, which holds `rows` rows."""
    position = {column.name: index for index, column in enumerate(columns)}
    measured = measured_pairs(backend, sample, columns, k_min)
    correlations = kept_correlations(measured, position)
    sections: dict[str, Any] = {}
    if correlations:
        sections['correlations'] = correlations
    conditionals, floors = conditional_tables(backend, described, columns, measured, k_min)
    if conditionals:
        sections['conditionals'] = conditionals
    dependencies = functional_dependencies(backend, described, columns, rows, k_min, position)
    if dependencies:
        sections['dependencies'] = dependencies
    return Dependence(sections, floors)


def role(column: DependenceColumn) -> str | None:
    """Whether a column is ranked, categorical, or neither: a constant, a counting sequence, or text of many values."""
    kind = column.spec.kind
    if column.distinct < 2 or (column.entry.get('numeric') or {}).get('sequence'):
        return None
    if kind is ColumnKind.BOOLEAN or (kind in (ColumnKind.STRING, ColumnKind.INTEGER) and column.distinct <= CATEGORICAL_VALUES):
        return CATEGORICAL
    if kind in NUMERIC or kind in TEMPORAL:
        return RANKED
    return None


def measured_pairs(
    backend: Backend, sample: Table, columns: list[DependenceColumn], k_min: int
) -> list[tuple[str, str, str, float | None]]:
    """Every pair's correlation on the sample, as `(a, b, method, value)` in the order of the columns. Two ranked
    columns take Spearman's, two categorical ones the bias-corrected Cramér's V, and a numeric and a categorical one
    the bias-corrected correlation ratio η of the numeric column's ranks, with the numeric column as `a`. The value is
    None where fewer than `PAIR_ROWS` or k_min sampled rows hold the pair, or fewer than `ROWS_PER_CELL` per cell of
    a contingency table or per category. η is None too where the rows expected off the numeric column's most common
    value number fewer than `ROWS_PER_CELL` per category, since a value most rows hold ties their ranks."""
    least = max(PAIR_ROWS, k_min)
    ranked = [column for column in columns if role(column) == RANKED]
    categorical = [column for column in columns if role(column) == CATEGORICAL]
    rank_pairs = [(first.name, second.name) for at, first in enumerate(ranked) for second in ranked[at + 1 :]]
    category_pairs = [(first.name, second.name) for at, first in enumerate(categorical) for second in categorical[at + 1 :]]
    ratio_pairs = [(numeric.name, other.name) for numeric in ranked if numeric.spec.kind in NUMERIC for other in categorical]
    spread = {column.name: off_mode_share(column) for column in ranked}
    found = [
        (a, b, SPEARMAN, value if held >= least else None)
        for (a, b), (value, held) in zip(rank_pairs, backend.rank_correlations(sample, rank_pairs), strict=True)
    ]
    held_cells = [both_held(cells) for cells in backend.cross_counts(sample, category_pairs)]
    found += [
        (
            a,
            b,
            CRAMERS_V,
            cramers_v(cells) if sum(count for _, _, count in cells) >= max(least, ROWS_PER_CELL * table_cells(cells)) else None,
        )
        for (a, b), cells in zip(category_pairs, held_cells, strict=True)
    ]
    found += [
        (a, b, CORRELATION_RATIO, correlation_ratio(groups) if enough_spread(groups, spread[a], least) else None)
        for (a, b), groups in zip(ratio_pairs, backend.rank_moments(sample, ratio_pairs), strict=True)
    ]
    return found


def both_held(cells: list[tuple[str | None, str | None, int]]) -> list[tuple[str, str, int]]:
    """The cells of the rows holding both values."""
    return [(first, second, count) for first, second, count in cells if first is not None and second is not None]


def table_cells(cells: list[tuple[str, str, int]]) -> int:
    """The cells of the contingency table `cells` fills: one per pair of values either side holds."""
    return len({first for first, _, _ in cells}) * len({second for _, second, _ in cells})


def off_mode_share(column: DependenceColumn) -> float:
    """The share of a numeric column's values its most common value leaves, as its zero rate and top values state it."""
    numeric = column.entry.get('numeric') or {}
    stated = [numeric.get('zero_rate') or 0.0, *(entry['rate'] for entry in numeric.get('top_values') or [])]
    return 1 - max(stated)


def enough_spread(groups: list[tuple[int, float, float]], off_mode: float, least: int) -> bool:
    """Whether η's categories hold `least` rows, `ROWS_PER_CELL` per category, and as many expected off the numeric
    column's most common value, which holds the share `1 - off_mode` of its values."""
    rows, floor = sum(count for count, _, _ in groups), ROWS_PER_CELL * len(groups)
    return rows >= max(least, floor) and rows * off_mode >= floor


def kept_correlations(found: list[tuple[str, str, str, float | None]], position: dict[str, int]) -> list[dict[str, Any]]:
    """Every pair whose correlation reaches the floor, the strongest kept, in the order of the columns."""
    strong = [(a, b, method, value) for a, b, method, value in found if value is not None and abs(value) >= CORRELATION_FLOOR]
    kept = sorted(strong, key=lambda pair: -abs(pair[3]))[:CORRELATIONS_KEPT]
    kept.sort(key=lambda pair: (min(position[pair[0]], position[pair[1]]), max(position[pair[0]], position[pair[1]])))
    return [{'a': a, 'b': b, 'method': method, 'value': significant(value, RATE_DIGITS) + 0.0} for a, b, method, value in kept]


def cramers_v(cells: list[tuple[str, str, int]]) -> float | None:
    """Bergsma's bias-corrected Cramér's V of a contingency table's cells, or None where either side holds one value."""
    rows = sum(count for _, _, count in cells)
    by_first, by_second = collections.Counter[str](), collections.Counter[str]()
    for first, second, count in cells:
        by_first[first] += count
        by_second[second] += count
    r, k = len(by_first), len(by_second)
    if rows < 2 or r < 2 or k < 2:
        return None
    chi_squared = rows * (sum(count * count / (by_first[first] * by_second[second]) for first, second, count in cells) - 1)
    phi_squared = max(0.0, chi_squared / rows - (k - 1) * (r - 1) / (rows - 1))
    corrected = min(k - (k - 1) ** 2 / (rows - 1), r - (r - 1) ** 2 / (rows - 1)) - 1
    return math.sqrt(phi_squared / corrected) if corrected > 0 else None


def correlation_ratio(groups: list[tuple[int, float, float]]) -> float | None:
    """η from each category's rows, mean and population variance, its square corrected as Kelley's ε² corrects η² for
    the share of variance categories explain by chance. None where one category or no variance is held, or the
    categories are as many as the rows."""
    rows, held = sum(count for count, _, _ in groups), len(groups)
    if held < 2 or rows <= held:
        return None
    mean = sum(count * group_mean for count, group_mean, _ in groups) / rows
    between = sum(count * (group_mean - mean) ** 2 for count, group_mean, _ in groups)
    within = sum(count * variance for count, _, variance in groups)
    total = between + within
    if total <= 0:
        return None
    return math.sqrt(max(0.0, 1 - within / total * (rows - 1) / (rows - held)))


def conditional_tables(
    backend: Backend, described: Table, columns: list[DependenceColumn], measured: list[tuple[str, str, str, float | None]], k_min: int
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], int]]:
    """A conditional table for each categorical pair whose column of fewer values holds `GIVEN_VALUES` at most, and
    whose Cramér's V reaches `CONDITIONAL_TRIED` on the sample and `CORRELATION_FLOOR` over every row."""
    by_name = {column.name: column for column in columns}
    pairs: list[tuple[str, str, list[str], list[str]]] = []
    for a, b, method, value in measured:
        if method != CRAMERS_V or value is None or value < CONDITIONAL_TRIED:
            continue
        first, second = by_name[a], by_name[b]
        fewer, more = (first, second) if first.distinct <= second.distinct else (second, first)
        fewer_values, more_values = listed_values(fewer.entry), listed_values(more.entry)
        if fewer.distinct <= GIVEN_VALUES and fewer_values and more_values:
            pairs.append((fewer.name, more.name, fewer_values, more_values))
    tables, floors = [], {}
    counted = backend.cross_counts(described, [(given, child) for given, child, _, _ in pairs])
    for (given, child, given_values, child_values), cells in zip(pairs, counted, strict=True):
        if (cramers_v(both_held(cells)) or 0.0) < CORRELATION_FLOOR:
            continue
        written = conditional_table(given, child, cells, given_values, child_values, k_min)
        if written:
            tables.append(written[0])
            floors[(given, child)] = written[1]
    return tables, floors


def conditional_table(
    given: str,
    child: str,
    cells: list[tuple[str | None, str | None, int]],
    given_values: list[str],
    child_values: list[str],
    k_min: int,
) -> tuple[dict[str, Any], int] | None:
    """The distribution of `child` within each listed value of `given`, and the fewest rows a kept cell holds.

    Only the child's listed values are distributed. A cell under k_min rows is left out, and a given value left with
    no cell joins the values `other` describes. Then no difference a reader can take against a listed count names 1 to
    k_min - 1 rows. A child value whose rows outside the kept cells number that many is left out of every row. A row
    whose given values' rows outside its kept cells number that many loses its least kept cell. `cells` counts every
    row, a null as None, so those rows outside include a null and an unlisted value on the other side.
    """
    listed_given, listed_child = set(given_values), set(child_values)
    by_given: dict[str, collections.Counter[str]] = collections.defaultdict(collections.Counter)
    given_rows, child_rows = collections.Counter[str](), collections.Counter[str]()
    for given_value, child_value, count in cells:
        if given_value is not None:
            given_rows[given_value] += count
            if child_value in listed_child:
                by_given[given_value][child_value] += count
        if child_value is not None:
            child_rows[child_value] += count
    order = {value: index for index, value in enumerate(child_values)}
    domain, folded, dropped = set(child_values), set[str](), set[tuple[str | None, str]]()
    while True:
        members: dict[str | None, list[str]] = {value: [value] for value in given_values if value not in folded}
        members[None] = [value for value in given_rows if value not in listed_given or value in folded]
        kept: dict[str | None, dict[str, int]] = {}
        for row, values in members.items():
            counts = sum((by_given[value] for value in values), collections.Counter[str]())
            kept[row] = {
                value: count for value, count in counts.items() if value in domain and count >= k_min and (row, value) not in dropped
            }
        empty = {row for row, counts in kept.items() if row is not None and not counts}
        if empty:
            folded |= {row for row in empty if row is not None}
            continue
        outside = {value: child_rows[value] - sum(counts.get(value, 0) for counts in kept.values()) for value in domain}
        small = {value for value, rows_outside in outside.items() if 0 < rows_outside < k_min}
        if small:
            domain -= small
            continue
        thin = [
            (row, counts)
            for row, counts in kept.items()
            if counts and 0 < sum(given_rows[value] for value in members[row]) - sum(counts.values()) < k_min
        ]
        if not thin:
            break
        dropped |= {(row, min(counts, key=lambda value: (counts[value], order[value]))) for row, counts in thin}
    listed_rows = {row: counts for row, counts in kept.items() if row is not None}
    if not listed_rows:
        return None

    def shares(counts: dict[str, int]) -> dict[str, float]:
        held = sum(counts.values())
        return {value: rate(counts[value], held) for value in sorted(counts, key=order.__getitem__)}

    written: dict[str, Any] = {'given': given, 'column': child, 'table': {row: shares(counts) for row, counts in listed_rows.items()}}
    if kept[None]:
        written['other'] = shares(kept[None])
    return written, min(count for counts in kept.values() for count in counts.values())


def functional_dependencies(
    backend: Backend, described: Table, columns: list[DependenceColumn], rows: int, k_min: int, position: dict[str, int]
) -> list[dict[str, Any]]:
    """Each column one other determines, counted over the left-hand values k_min rows hold."""
    determining = [column for column in columns if column.spec.kind in DETERMINING_KINDS and 2 <= column.distinct < KEY_SHARE * rows]
    dependent = [
        column
        for column in columns
        if column.spec.kind not in (ColumnKind.DATETIME, ColumnKind.OTHER) and 2 <= column.distinct <= DEPENDENT_VALUES
    ]
    tests = [(lhs, rhs) for lhs in determining for rhs in dependent if lhs is not rhs and lhs.distinct > rhs.distinct]
    tests.sort(key=lambda test: (not place_hinted(*test), test[0].distinct, position[test[0].name], position[test[1].name]))
    by_lhs: dict[str, list[str]] = {}
    for determined_by, determines in tests[:DEPENDENCY_TESTS]:
        by_lhs.setdefault(determined_by.name, []).append(determines.name)
    found = []
    for lhs, dependents in by_lhs.items():
        counted = backend.dependency_counts(described, lhs, dependents, k_min)
        for rhs, (complete, supported, violating) in zip(dependents, counted, strict=True):
            if complete and supported >= DEPENDENCY_SUPPORT * complete and violating <= DEPENDENCY_VIOLATION * supported:
                found.append({'kind': 'functional', 'lhs': [lhs], 'rhs': rhs, 'violation_rate': rate(violating, supported)})
    return sorted(found, key=lambda dependency: (position[dependency['lhs'][0]], position[dependency['rhs']]))


def place_hinted(lhs: DependenceColumn, rhs: DependenceColumn) -> bool:
    """Whether the names say a ZIP code determines the place it lies in."""
    zip_named = any(hint_runs_in(name_words(lhs.name), hint, at_end=True) for hint in ZIP_HINTS)
    return zip_named and any(hint_runs_in(name_words(rhs.name), hint) for hint in PLACE_HINTS)
