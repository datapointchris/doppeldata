"""How a table's columns depend on each other: correlations, conditional tables and functional dependencies.

Each correlation is measured on the dependence sample, the seeded sample the fits read. Each count a conditional
table or a dependency rests on is counted over the rows the profile describes, so the floor holds it exactly.
"""

import collections
import dataclasses as dc
import math
from typing import Any

from doppeldata import _core
from doppeldata.column_name_words import ZIP_HINTS
from doppeldata.column_name_words import hint_runs_in
from doppeldata.column_name_words import name_words
from doppeldata.privacy_policy.literal_fields import listed_values
from doppeldata.profile_format import FORMAT_VERSION
from doppeldata.profile_format import GIVEN_LISTS
from doppeldata.profile_format import engine_plan
from doppeldata.profile_format import quantile_level
from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import Table
from doppeldata.profiling.recorded_counts import NUMERIC_TOP_VALUES_THRESHOLD
from doppeldata.profiling.recorded_counts import RATE_DIGITS
from doppeldata.profiling.recorded_counts import TOP_VALUES_THRESHOLD
from doppeldata.profiling.recorded_counts import clear_of_the_ends
from doppeldata.profiling.recorded_counts import rate
from doppeldata.profiling.recorded_counts import rows_to_the_nearer_end
from doppeldata.profiling.recorded_counts import significant
from doppeldata.profiling.recorded_counts import written_number
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
# the V of those counts reaches CORRELATION_FLOOR. The sample picks which pairs are counted, so two engines' samples can
# state different tables. The counts decide every cell.
CONDITIONAL_TRIED = 0.2
# A numeric column's quantiles are stated within each value of a categorical column holding at most this many values,
# where the bias-corrected η² of the pair reaches this share of the numeric column's rank variance.
GIVEN_QUANTILE_VALUES = 20
SHARE_EXPLAINED = 0.1
# A column's null or zero rate is stated within each value of a categorical column where Cohen's w of the rows holding
# it against the column's values reaches this, Cohen's small effect, its chance share taken off as Cramér's V's is.
RATE_DEPENDENCE = 0.1
# A pair is counted over every row where the sample's w reaches this, as a categorical pair is at CONDITIONAL_TRIED.
RATE_TRIED = 0.05
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
class FrozenPairs:
    """The pairs an earlier profile states, which a re-profile states again past every relevance cut: each correlation
    and correlation ratio by its columns and method, and each entry of a list naming a parent by its given and child."""

    correlations: frozenset[tuple[str, str, str]] = frozenset()
    given: dict[str, frozenset[tuple[str, str]]] = dc.field(default_factory=dict)

    @classmethod
    def of(cls, document: dict[str, Any]) -> 'FrozenPairs':
        dataset = document.get('dataset') or {}
        correlations = {(entry['a'], entry['b'], entry['method']) for entry in dataset.get('correlations') or []}
        correlations |= {(entry['a'], entry['b'], CORRELATION_RATIO) for entry in dataset.get('correlation_ratios') or []}
        given = {name: frozenset((entry['given'], entry['column']) for entry in dataset.get(name) or []) for name in GIVEN_LISTS}
        return cls(frozenset(correlations), given)

    def correlated(self, a: str, b: str, method: str) -> bool:
        return (a, b, method) in self.correlations or (b, a, method) in self.correlations

    def gives(self, name: str, given: str, child: str) -> bool:
        return (given, child) in self.given.get(name, frozenset())


@dc.dataclass(frozen=True)
class Dependence:
    """The dataset's `correlations`, `correlation_ratios`, `conditionals`, `conditional_quantiles`, `conditional_rates`
    and `dependencies`, and the fewest rows a literal of each entry naming a parent or a lookup table rests on, which
    the policy's audit records. A floor is keyed by its list's name and its given and child columns, or by
    `dependencies` and a lookup's left- and right-hand columns."""

    sections: dict[str, Any]
    cell_floors: dict[tuple[str, str, str], int]


def column_dependence(
    backend: Backend,
    sample: Table,
    described: Table,
    columns: list[DependenceColumn],
    rows: int,
    k_min: int,
    frozen: FrozenPairs | None = None,
    derived: dict[str, dict[str, Any]] | None = None,
) -> Dependence:
    """The dependence between `columns`, measured on `sample` and counted on `described`, which holds `rows` rows, each
    column `derived` names computed by its entry there. Each pair `frozen` holds is stated wherever its floors allow,
    whatever its strength.

    The engine's plan of the draft decides what the generator draws. Its copula groups, after the lookups and tables,
    decide which quantiles and zero rates a copula would dilute. Each entry its plan of the whole draft sets aside, as a
    table of a column a lookup draws or an entry closing a cycle, is left out with its floor.
    """
    frozen = frozen or FrozenPairs()
    derived = derived or {}
    position = {column.name: index for index, column in enumerate(columns)}
    measured = measured_pairs(backend, sample, columns, k_min)
    correlations, ratios = kept_correlations(measured, position, frozen)
    dependencies, looked_up = functional_dependencies(backend, described, columns, rows, k_min, position)
    tables, table_floors = conditional_tables(backend, described, columns, measured, k_min, frozen)
    before_warps = {'correlations': correlations, 'dependencies': dependencies, 'conditionals': tables}
    copula = copula_pairs(correlations, draft_plan(columns, derived, before_warps).copula_groups())
    quantiles, quantile_floors = conditional_quantiles(backend, described, columns, measured, k_min, frozen, copula)
    curve_parents = {entry['column']: entry['given'] for entry in quantiles}
    rates, rate_floors = conditional_rates(backend, sample, described, columns, k_min, frozen, curve_parents, copula)
    sections = {
        'correlations': correlations,
        'correlation_ratios': ratios,
        'conditionals': tables,
        'conditional_quantiles': quantiles,
        'conditional_rates': rates,
        'dependencies': dependencies,
    }
    sections = without_set_aside(sections, draft_plan(columns, derived, sections).set_aside())
    floors: dict[tuple[str, str, str], int] = {}
    for name, floored in (('conditionals', table_floors), ('conditional_quantiles', quantile_floors)):
        floors |= {(name, entry['given'], entry['column']): floored[(entry['given'], entry['column'])] for entry in sections.get(name, [])}
    for entry in sections.get('conditional_rates', []):
        by_rate = rate_floors[(entry['given'], entry['column'])]
        floors[('conditional_rates', entry['given'], entry['column'])] = min(by_rate[name] for name in by_rate if name in entry)
    for dependency in sections.get('dependencies', []):
        if 'table' in dependency:
            lhs, rhs = dependency['lhs'][0], dependency['rhs']
            floors[('dependencies', lhs, rhs)] = looked_up[(lhs, rhs)]
    return Dependence(sections, floors)


def draft_plan(columns: list[DependenceColumn], derived: dict[str, dict[str, Any]], sections: dict[str, Any]) -> _core.Plan:
    """The engine's plan of a draft holding `columns`, each column `derived` names computed by its entry there, and the
    dataset's `sections`."""
    document = {
        'profile_version': FORMAT_VERSION,
        'kind': 'table',
        'name': 'draft',
        'dataset': {name: entries for name, entries in sections.items() if entries},
        'columns': [column.entry | ({'derived': derived[column.name]} if column.name in derived else {}) for column in columns],
    }
    return engine_plan(document, 'the draft profile')


def without_set_aside(sections: dict[str, Any], set_aside: list[str]) -> dict[str, Any]:
    """The dataset's `sections`, each list left out where it is empty, without each entry or field whose JSON pointer
    `set_aside` holds. A rate set aside leaves `other` too, and an entry of rates keeping neither is left out."""
    aside: dict[tuple[str, int], set[str | None]] = {}
    for pointer in set_aside:
        _, _, listed, at, *field = pointer.split('/')
        aside.setdefault((listed, int(at)), set()).add(field[0] if field else None)
    kept: dict[str, Any] = {}
    for name, entries in sections.items():
        left = []
        for index, entry in enumerate(entries):
            fields = aside.get((name, index), set())
            if None in fields:
                continue
            entry = {key: value for key, value in entry.items() if key not in fields}
            other = {key: value for key, value in (entry.get('other') or {}).items() if key not in fields}
            if name == 'conditional_rates':
                if not {'zero_rate', 'null_rate'} & entry.keys():
                    continue
                entry.pop('other', None)
                if other:
                    entry['other'] = other
            left.append(entry)
        if left:
            kept[name] = left
    return kept


@dc.dataclass(frozen=True)
class CopulaPairs:
    """The copula group of each column the engine draws through one, by its place among the plan's groups, and the most
    of each member's rank variance one partner in its group explains, as the squared correlation."""

    groups: dict[str, int]
    explained: dict[str, float]

    def share_group(self, a: str, b: str) -> bool:
        """Whether one copula draws both columns. A member's levels are drawn given every other member's, so no warp
        of them holds a quantile or a zero rate given another member's value."""
        return a in self.groups and self.groups[a] == self.groups.get(b)


def copula_pairs(correlations: list[dict[str, Any]], groups: list[list[str]]) -> CopulaPairs:
    """The copula `groups` a plan draws, and what of each member's rank variance its strongest partner explains."""
    group_of = {name: at for at, group in enumerate(groups) for name in group}
    explained: dict[str, float] = {}
    for stated in correlations:
        a, b = stated['a'], stated['b']
        if stated['method'] == SPEARMAN and a in group_of and group_of[a] == group_of.get(b):
            for name in (a, b):
                explained[name] = max(explained.get(name, 0.0), stated['value'] ** 2)
    return CopulaPairs(group_of, explained)


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
    columns take Spearman's, and so do a ranked and a categorical integer column, which a copula can then hold. Two
    categorical ones take the bias-corrected Cramér's V, and a numeric and a categorical one
    the bias-corrected correlation ratio η of the numeric column's ranks, with the numeric column as `a`. The value is
    None where fewer than `PAIR_ROWS` or k_min sampled rows hold the pair, or fewer than `ROWS_PER_CELL` per cell of
    a contingency table or per category. η is None too where the rows expected off the numeric column's most common
    value number fewer than `ROWS_PER_CELL` per category, since a value most rows hold ties their ranks."""
    least = max(PAIR_ROWS, k_min)
    ranked = [column for column in columns if role(column) == RANKED]
    categorical = [column for column in columns if role(column) == CATEGORICAL]
    ordinal = {column.name for column in categorical if column.spec.kind is ColumnKind.INTEGER}
    with_ordinal = [column for column in columns if role(column) == RANKED or column.name in ordinal]
    rank_pairs = [
        (first.name, second.name)
        for at, first in enumerate(with_ordinal)
        for second in with_ordinal[at + 1 :]
        if first.name not in ordinal or second.name not in ordinal
    ]
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


def kept_correlations(
    found: list[tuple[str, str, str, float | None]], position: dict[str, int], frozen: FrozenPairs
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Every pair whose correlation reaches the floor, the strongest kept, and every pair `frozen` holds, in the order
    of the columns: the dataset's `correlations`, and its `correlation_ratios`, whose `a` and `b` follow the columns'
    order too."""
    measured = [(a, b, method, value) for a, b, method, value in found if value is not None]
    held = [(a, b, method, value) for a, b, method, value in measured if frozen.correlated(a, b, method)]
    strong = [
        (a, b, method, value) for a, b, method, value in measured if abs(value) >= CORRELATION_FLOOR and not frozen.correlated(a, b, method)
    ]
    kept = held + sorted(strong, key=lambda pair: -abs(pair[3]))[: max(0, CORRELATIONS_KEPT - len(held))]
    kept = [(a, b, method, value) if position[a] < position[b] else (b, a, method, value) for a, b, method, value in kept]
    kept.sort(key=lambda pair: (position[pair[0]], position[pair[1]]))
    correlations = [
        {'a': a, 'b': b, 'method': method, 'value': significant(value, RATE_DIGITS) + 0.0}
        for a, b, method, value in kept
        if method != CORRELATION_RATIO
    ]
    ratios = [
        {'a': a, 'b': b, 'value': significant(value, RATE_DIGITS) + 0.0} for a, b, method, value in kept if method == CORRELATION_RATIO
    ]
    return correlations, ratios


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
    backend: Backend,
    described: Table,
    columns: list[DependenceColumn],
    measured: list[tuple[str, str, str, float | None]],
    k_min: int,
    frozen: FrozenPairs,
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], int]]:
    """A conditional table for each categorical pair whose column of fewer values holds `GIVEN_VALUES` at most, and
    whose Cramér's V reaches `CONDITIONAL_TRIED` on the sample and `CORRELATION_FLOOR` over every row, and for each
    pair `frozen` holds whose columns list values. The generator draws a column from one table, so a child keeps the
    pair `frozen` holds, else its strongest. The fewest rows a kept cell holds, by its given and child column."""
    by_name = {column.name: column for column in columns}
    pairs: list[tuple[str, str, list[str], list[str]]] = []
    for a, b, method, value in measured:
        if method != CRAMERS_V:
            continue
        first, second = by_name[a], by_name[b]
        fewer, more = (first, second) if first.distinct <= second.distinct else (second, first)
        held = frozen.gives('conditionals', fewer.name, more.name) or frozen.gives('conditionals', more.name, fewer.name)
        if not held and (value is None or value < CONDITIONAL_TRIED or fewer.distinct > GIVEN_VALUES):
            continue
        if held and not frozen.gives('conditionals', fewer.name, more.name):
            fewer, more = more, fewer
        fewer_values, more_values = listed_values(fewer.entry), listed_values(more.entry)
        if fewer_values and more_values:
            pairs.append((fewer.name, more.name, fewer_values, more_values))
    strongest: dict[str, tuple[tuple[bool, float], dict[str, Any], int]] = {}
    counted = backend.cross_counts(described, [(given, child) for given, child, _, _ in pairs])
    for (given, child, given_values, child_values), cells in zip(pairs, counted, strict=True):
        held, strength = frozen.gives('conditionals', given, child), cramers_v(both_held(cells)) or 0.0
        if not held and strength < CORRELATION_FLOOR:
            continue
        written = conditional_table(given, child, cells, given_values, child_values, k_min)
        if written and (child not in strongest or (held, strength) > strongest[child][0]):
            strongest[child] = ((held, strength), *written)
    return [table for _, table, _ in strongest.values()], {(table['given'], child): floor for child, (_, table, floor) in strongest.items()}


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
    no cell joins the values `other` describes. Each share is of the row's rows holding a child value, so the share its
    cells leave is the rows of child values it does not state, which the generator spreads over the child values no row
    states. Then no difference a reader can take against a listed count names 1 to k_min - 1 rows. A child value whose rows
    outside the kept cells number that many is left out of every row. A row whose given values' rows, or those of them
    holding a child value, outside its kept cells number that many loses its least kept cell. `cells` counts every row,
    a null as None, so those rows outside include a null and an unlisted value on the other side.
    """
    listed_given, listed_child = set(given_values), set(child_values)
    by_given: dict[str, collections.Counter[str]] = collections.defaultdict(collections.Counter)
    given_rows, held_rows, child_rows = collections.Counter[str](), collections.Counter[str](), collections.Counter[str]()
    for given_value, child_value, count in cells:
        if given_value is not None:
            given_rows[given_value] += count
            if child_value is not None:
                held_rows[given_value] += count
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
            if counts
            and any(0 < sum(rows[value] for value in members[row]) - sum(counts.values()) < k_min for rows in (given_rows, held_rows))
        ]
        if not thin:
            break
        dropped |= {(row, min(counts, key=lambda value: (counts[value], order[value]))) for row, counts in thin}
    listed_rows = {row: counts for row, counts in kept.items() if row is not None}
    if not listed_rows:
        return None

    def shares(row: str | None, counts: dict[str, int]) -> dict[str, float]:
        held = sum(held_rows[value] for value in members[row])
        return {value: rate(counts[value], held) for value in sorted(counts, key=order.__getitem__)}

    table = {row: shares(row, counts) for row, counts in listed_rows.items()}
    written: dict[str, Any] = {'given': given, 'column': child, 'table': table}
    if kept[None]:
        written['other'] = shares(None, kept[None])
    return written, min(count for counts in kept.values() for count in counts.values())


def conditional_quantiles(
    backend: Backend,
    described: Table,
    columns: list[DependenceColumn],
    measured: list[tuple[str, str, str, float | None]],
    k_min: int,
    frozen: FrozenPairs,
    copula: CopulaPairs,
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], int]]:
    """The quantiles of each numeric column within each value of a categorical column holding `GIVEN_QUANTILE_VALUES`
    at most, where the squared η of the pair reaches `SHARE_EXPLAINED` on the sample, or `frozen` holds the pair. The
    generator draws a column from one parent's quantiles, so a child keeps the pair `frozen` holds, else its strongest.

    A copula draws a column through its partners, and a parent's quantiles would dilute what it holds. So no pair one
    copula group draws is stated, nor one whose squared η falls short of the squared correlation of the child's partner.

    Each quantile is one the child states, k_min rows from both ends of its group's values. A value whose group holds
    none folds into `other`, every listed value's own rows otherwise beside it, and `other` is left out where it holds
    none. The fewest rows a stated quantile lies from an end, by given and child column.
    """
    by_name = {column.name: column for column in columns}
    options: dict[str, list[tuple[bool, float, int, str]]] = {}
    for at, (child, given, method, value) in enumerate(measured):
        held = frozen.gives('conditional_quantiles', given, child)
        parent, numeric = by_name[given], by_name[child]
        explained = value * value if value is not None else 0.0
        drawn_apart = not copula.share_group(given, child) and explained > copula.explained.get(child, 0.0)
        if (
            method == CORRELATION_RATIO
            and (held or (explained >= SHARE_EXPLAINED and drawn_apart))
            and listed_values(parent.entry)
            and (numeric.entry.get('numeric') or {}).get('quantiles')
            and (held or parent.distinct <= GIVEN_QUANTILE_VALUES)
        ):
            options.setdefault(child, []).append((held, value or 0.0, -at, given))
    for ranked in options.values():
        ranked.sort(reverse=True)
    found: dict[str, tuple[int, dict[str, Any]]] = {}
    floors = {}
    # Each round tries every child's strongest parent left, until each has an entry or no parent left.
    while options:
        tried = [(child, ranked.pop(0)) for child, ranked in options.items()]
        pairs = [(child, given) for child, (_, _, _, given) in tried]
        for (child, (_, _, at, given)), written_entry in zip(
            tried, quantile_entries(backend, described, by_name, pairs, k_min), strict=True
        ):
            if written_entry is not None:
                found[child] = (-at, written_entry[0])
                floors[(given, child)] = written_entry[1]
        options = {child: ranked for child, ranked in options.items() if ranked and child not in found}
    return [entry for _, entry in sorted(found.values(), key=lambda placed: placed[0])], floors


def quantile_entries(
    backend: Backend, described: Table, by_name: dict[str, DependenceColumn], pairs: list[tuple[str, str]], k_min: int
) -> list[tuple[dict[str, Any], int] | None]:
    """Each `(child, given)` pair's conditional quantiles entry and the fewest rows a stated quantile lies from an end,
    or None where no listed value keeps a quantile. A value too thin to keep one is counted again within `other`."""
    levels = [{name: quantile_level(name) for name in by_name[child].entry['numeric']['quantiles']} for child, _ in pairs]
    keep = [list(listed_values(by_name[given].entry) or []) for _, given in pairs]

    def counted(indexes: list[int]) -> list[dict[str | None, tuple[int, list[float]]]]:
        requests = [(*pairs[at], keep[at], tuple(levels[at].values())) for at in indexes]
        if not requests:
            return []
        return [{group: (rows, values) for group, rows, values in found} for found in backend.grouped_quantiles(described, requests)]

    groups = counted(list(range(len(pairs))))
    thinned = []
    for at, by_group in enumerate(groups):
        thin = {group for group in keep[at] if group in by_group and not clear_quantiles(levels[at], *by_group[group], k_min)}
        if thin:
            keep[at] = [group for group in keep[at] if group not in thin]
            thinned.append(at)
    for at, by_group in zip(thinned, counted(thinned), strict=True):
        groups[at] = by_group
    entries: list[tuple[dict[str, Any], int] | None] = []
    for at, (child, given) in enumerate(pairs):
        integral = by_name[child].spec.kind is ColumnKind.INTEGER
        kept = {group: clear_quantiles(levels[at], *groups[at][group], k_min) for group in [*keep[at], None] if group in groups[at]}
        kept = {group: quantiles for group, quantiles in kept.items() if quantiles}
        listed = {group: written(quantiles, integral) for group, quantiles in kept.items() if group is not None}
        if not listed:
            entries.append(None)
            continue
        entry: dict[str, Any] = {'given': given, 'column': child, 'quantiles': listed}
        if None in kept:
            entry['other'] = written(kept[None], integral)
        fewest = min(
            rows_to_the_nearer_end(levels[at][name], groups[at][group][0]) for group, quantiles in kept.items() for name in quantiles
        )
        entries.append((entry, fewest))
    return entries


def clear_quantiles(levels: dict[str, float], rows: int, at: list[float], k_min: int) -> dict[str, float]:
    """Each named quantile, at the levels in `levels`' order, that sits k_min rows from both ends of a group of `rows`."""
    return {name: at[index] for index, (name, level) in enumerate(levels.items()) if clear_of_the_ends(level, rows, k_min)}


def written(quantiles: dict[str, float], integral: bool) -> dict[str, int | float]:
    return {name: written_number(value, integral) for name, value in quantiles.items()}


def conditional_rates(
    backend: Backend,
    sample: Table,
    described: Table,
    columns: list[DependenceColumn],
    k_min: int,
    frozen: FrozenPairs,
    curve_parents: dict[str, str],
    copula: CopulaPairs,
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], dict[str, int]]]:
    """Each column's null rate, and a numeric column's zero rate, within each value of a categorical column listing
    `GIVEN_VALUES` at most, where the rate depends on the value by `RATE_DEPENDENCE` over `described`, and by
    `RATE_TRIED` on `sample`, or `frozen` holds the pair. A pair one copula group draws takes its zeros from it, so
    states no zero rate.

    The generator draws each rate of a column from one parent. So a child keeps, for each rate, the pair `frozen`
    holds, else for its zero rate the parent `curve_parents` draws its quantiles from, else its strongest. A rate rests
    on its group's rows, and is stated only where those rows, the rows it counts and the rest each number 0 or at least
    k_min. A value breaking that folds into `other`, and the least listed value too where `other` breaks it. The fewest
    rows any stated count of each rate holds, by given and child column and then the rate's name.
    """
    givens = [
        column for column in columns if role(column) == CATEGORICAL and column.distinct <= GIVEN_VALUES and listed_values(column.entry)
    ]
    given_values = {parent.name: listed_values(parent.entry) or [] for parent in givens}
    numeric = {column.name for column in columns if column.spec.kind in NUMERIC}

    def zeros_stated(given: str, child: str) -> bool:
        return child in numeric and (frozen.gives('conditional_rates', given, child) or not copula.share_group(given, child))

    pairs = [(parent.name, child.name) for parent in givens for child in columns if child is not parent and rated(child)]
    tried = [
        (given, child)
        for (given, child), groups in zip(pairs, backend.grouped_rates(sample, pairs), strict=True)
        if frozen.gives('conditional_rates', given, child)
        or any(rate_dependence(list(by_value.values())) >= RATE_TRIED for _, by_value in kind_counts(groups, zeros_stated(given, child)))
    ]
    best: dict[tuple[str, str], tuple[tuple[bool, bool, float], str, tuple[dict[str, float], float | None, int]]] = {}
    for (given, child), groups in zip(tried, backend.grouped_rates(described, tried), strict=True):
        held = frozen.gives('conditional_rates', given, child)
        for name, by_value in kind_counts(groups, zeros_stated(given, child)):
            rates = parent_rates(by_value, given_values[given], k_min)
            strength = rate_dependence(list(by_value.values()))
            if rates is None or not (held or strength >= RATE_DEPENDENCE):
                continue
            rank = (held, name == 'zero_rate' and curve_parents.get(child) == given, strength)
            if (child, name) not in best or rank > best[(child, name)][0]:
                best[(child, name)] = (rank, given, rates)
    found: list[dict[str, Any]] = []
    floors: dict[tuple[str, str], dict[str, int]] = {}
    for given, rated_column in tried:
        kept = {name: rates for (column, name), (_, chosen, rates) in best.items() if column == rated_column and chosen == given}
        if not kept:
            continue
        entry: dict[str, Any] = {'given': given, 'column': rated_column}
        other: dict[str, float] = {}
        for name in ('null_rate', 'zero_rate'):
            if name in kept:
                entry[name], other_rate, _ = kept[name]
                if other_rate is not None:
                    other[name] = other_rate
        if other:
            entry['other'] = other
        found.append(entry)
        floors[(given, rated_column)] = {name: floor for name, (_, _, floor) in kept.items()}
    return found, floors


def kind_counts(groups: list[tuple[str | None, int, int, int]], zeros: bool) -> list[tuple[str, dict[str, tuple[int, int]]]]:
    """Each rate a child may state, by name, with each listed parent value's rows and the rows the rate counts: its
    null rate, and its zero rate where `zeros`. A zero rate counts among the rows holding a value."""
    nulls = {value: (rows, null) for value, rows, null, _ in groups if value is not None}
    if not zeros:
        return [('null_rate', nulls)]
    return [('null_rate', nulls), ('zero_rate', {value: (rows - null, zero) for value, rows, null, zero in groups if value is not None})]


def rate_dependence(groups: list[tuple[int, int]]) -> float:
    """Cohen's w of a rate against the values of a column, from each value's rows and the rows the rate counts: the
    root of chi-squared over the rows, less the chi-squared its degrees of freedom expect by chance."""
    rows, counted = sum(held for held, _ in groups), sum(count for _, count in groups)
    if not rows or counted in (0, rows):
        return 0.0
    share = counted / rows
    chi_squared = sum((count - held * share) ** 2 / (held * share * (1 - share)) for held, count in groups if held)
    return math.sqrt(max(0.0, chi_squared - (len(groups) - 1)) / rows)


def rated(column: DependenceColumn) -> bool:
    """Whether a column states nulls or zeros a parent could spread, and is no counting sequence."""
    numeric = column.entry.get('numeric') or {}
    return bool(column.entry.get('null_rate') or numeric.get('zero_rate')) and not numeric.get('sequence')


def parent_rates(
    by_value: dict[str, tuple[int, int]], given_values: list[str], k_min: int
) -> tuple[dict[str, float], float | None, int] | None:
    """Each listed parent value's rate, `counted` of `rows`, the rate of every other value as one, or None where it holds
    no rows, and the fewest rows any stated count holds. None where no listed value keeps a rate."""

    def floored(rows: int, counted: int) -> bool:
        return rows >= k_min and all(count == 0 or count >= k_min for count in (counted, rows - counted))

    folded: set[str] = set()
    order = {value: index for index, value in enumerate(given_values)}
    while True:
        kept = {value: by_value[value] for value in given_values if value in by_value and value not in folded}
        broken = {value for value, (rows, counted) in kept.items() if not floored(rows, counted)}
        if broken:
            folded |= broken
            continue
        others = [counts for value, counts in by_value.items() if value not in kept]
        other = (sum(rows for rows, _ in others), sum(counted for _, counted in others))
        if other[0] and not floored(*other):
            if not kept:
                return None
            folded.add(min(kept, key=lambda value: (kept[value][0], -order[value])))
            continue
        break
    if not kept:
        return None
    stated = [*kept.values(), *([other] if other[0] else [])]
    fewest = min(count for rows, counted in stated for count in (rows, counted, rows - counted) if count)
    return {value: rate(counted, rows) for value, (rows, counted) in kept.items()}, rate(other[1], other[0]) if other[0] else None, fewest


def functional_dependencies(
    backend: Backend, described: Table, columns: list[DependenceColumn], rows: int, k_min: int, position: dict[str, int]
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], int]]:
    """Each column one other determines, counted over the left-hand values k_min rows hold, with its lookup table where
    both columns list values, and the fewest rows a lookup's key holds, by its left- and right-hand column."""
    found = determined_columns(backend, described, columns, rows, k_min, position)
    by_name = {column.name: column for column in columns}
    listed = {
        (dependency['lhs'][0], dependency['rhs']): (lhs_values, rhs_values)
        for dependency in found
        if (lhs_values := listed_values(by_name[dependency['lhs'][0]].entry))
        and (rhs_values := listed_values(by_name[dependency['rhs']].entry))
    }
    floors = {}
    for (lhs, rhs), cells in zip(listed, backend.cross_counts(described, list(listed)), strict=True):
        looked_up = lookup_table(cells, *listed[(lhs, rhs)], k_min)
        if looked_up is None:
            continue
        table, floors[(lhs, rhs)] = looked_up
        next(dependency for dependency in found if dependency['lhs'] == [lhs] and dependency['rhs'] == rhs)['table'] = table
    return found, floors


def lookup_table(
    cells: list[tuple[str | None, str | None, int]], lhs_values: list[str], rhs_values: list[str], k_min: int
) -> tuple[dict[str, str], int] | None:
    """Each listed left-hand value k_min rows hold, mapped to the listed right-hand value most of its rows hold, and the
    fewest rows a mapped value holds. A right-hand value whose rows outside the mapped values' number 1 to k_min - 1
    is left out of every mapping, as a conditional table leaves such a child value out, and None where nothing is
    left."""
    order = {value: index for index, value in enumerate(rhs_values)}
    by_lhs: dict[str, collections.Counter[str]] = collections.defaultdict(collections.Counter)
    rhs_rows = collections.Counter[str]()
    for lhs, rhs, count in cells:
        if rhs is not None:
            rhs_rows[rhs] += count
            if lhs in lhs_values and rhs in order:
                by_lhs[lhs][rhs] += count
    mapped = {}
    for lhs, counts in by_lhs.items():
        rhs, held = min(counts.items(), key=lambda item: (-item[1], order[item[0]]))
        if sum(counts.values()) >= k_min:
            mapped[lhs] = (rhs, held)
    while mapped:
        covered = collections.Counter[str]()
        for rhs, held in mapped.values():
            covered[rhs] += held
        small = {rhs for rhs in covered if 0 < rhs_rows[rhs] - covered[rhs] < k_min}
        if not small:
            break
        mapped = {lhs: (rhs, held) for lhs, (rhs, held) in mapped.items() if rhs not in small}
    if not mapped:
        return None
    lhs_order = {value: index for index, value in enumerate(lhs_values)}
    table = {lhs: mapped[lhs][0] for lhs in sorted(mapped, key=lhs_order.__getitem__)}
    return table, min(sum(by_lhs[lhs].values()) for lhs in mapped)


def determined_columns(
    backend: Backend, described: Table, columns: list[DependenceColumn], rows: int, k_min: int, position: dict[str, int]
) -> list[dict[str, Any]]:
    """Each column one other determines, counted over the left-hand values k_min rows hold."""
    determining = [column for column in columns if column.spec.kind in DETERMINING_KINDS and 2 <= column.distinct < KEY_SHARE * rows]
    dependent = [
        column
        for column in columns
        if column.spec.kind not in (ColumnKind.DATETIME, ColumnKind.OTHER) and 2 <= column.distinct <= DEPENDENT_VALUES
    ]
    # Of two columns holding as many values, as a code and its name do, the earlier is tried as determining the later.
    tests = [
        (lhs, rhs)
        for lhs in determining
        for rhs in dependent
        if lhs is not rhs and (lhs.distinct, position[rhs.name]) > (rhs.distinct, position[lhs.name])
    ]
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
