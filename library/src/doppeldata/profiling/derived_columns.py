"""Columns computed from others of their table: a date or timestamp a lag after another of its kind, and a number two
other numbers give by `+ - * /`.

Each is screened on the dependence sample and confirmed over every row the profile describes. A lag is kept where the
column is at or after its base on `ORDERED` of the rows holding both, and the base's variance and the lag's sum to the
column's within `SPREAD_KEPT` of it, as they do where the lag is drawn apart from the base. An expression is kept where
its result, rounded to the column's scale, is the column on `HOLDS` of the rows holding all three columns. A row where
the column is zero and the result rounds to zero is left out, since an expression of two mostly zero columns holds
there whatever its operator.
"""

import itertools
import math
import re
import statistics
from typing import TYPE_CHECKING
from typing import Any

from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import Table
from doppeldata.profiling.column_dependence import PAIR_ROWS
from doppeldata.profiling.column_dependence import DependenceColumn
from doppeldata.profiling.recorded_counts import clear_of_the_ends
from doppeldata.profiling.recorded_counts import rate
from doppeldata.profiling.statistic_catalog import NUMERIC
from doppeldata.profiling.statistic_catalog import QUANTILES
from doppeldata.profiling.statistic_catalog import ColumnKind

if TYPE_CHECKING:
    import pyarrow as pa

ORDERED = 0.99
SPREAD_KEPT = 0.1
HOLDS = 0.98
# An expression is screened on the first rows of the sample, then on all of it.
SCREEN_ROWS = 1_000
# An expression's two columns are among the numeric columns this close before it in column order, as a total follows
# its quantity and price. The candidates grow as the cube of the columns tried.
OPERAND_WINDOW = 12
# The most expressions confirmed over every row.
EXPRESSIONS_CONFIRMED = 200
# The microseconds in the unit a lag is written in, by the column's kind.
LAG_UNITS = {ColumnKind.DATE: (86_400_000_000, 'd'), ColumnKind.DATETIME: (1_000_000, 's')}
UNQUOTED_NAME = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')


def derived_columns(
    backend: Backend, sample: Table, sample_rows: int, described: Table, columns: list[DependenceColumn], k_min: int
) -> dict[str, dict[str, Any]]:
    """Each column's `derived` entry, by name, screened on `sample`, which holds `sample_rows` rows at most, and
    confirmed on `described`."""
    usable = [column for column in columns if column.distinct >= 2 and not (column.entry.get('numeric') or {}).get('sequence')]
    temporal = [column for column in usable if column.spec.kind in LAG_UNITS]
    numeric = [column for column in usable if column.spec.kind in NUMERIC]
    if len(temporal) < 2 and len(numeric) < 3:
        return {}
    held = backend.sample(sample, sample_rows, 0)
    return lags(backend, sample, held, described, temporal, k_min) | expressions(backend, held, described, numeric, k_min)


def lags(
    backend: Backend, sample: Table, held: 'pa.Table', described: Table, columns: list[DependenceColumn], k_min: int
) -> dict[str, dict[str, Any]]:
    """Each date or timestamp a lag after another of its kind, from the base whose median lag is smallest. Where each
    of two is at or after the other, the later in column order is the one derived, and no base is derived from its own
    column."""
    least = max(PAIR_ROWS, k_min)
    position = {column.name: index for index, column in enumerate(columns)}
    tried = [(base.name, child.name) for base, child in itertools.permutations(columns, 2) if base.spec.kind is child.spec.kind]
    ordered = {pair for pair, counts in zip(tried, backend.ordered_shares(sample, tried), strict=True) if is_ordered(*counts, least)}
    epochs = {name: epoch_micros(held.column(name)) for pair in ordered for name in pair}
    medians = {}
    for base, child in ordered:
        if (child, base) in ordered and position[base] > position[child]:
            continue
        both = [(at, then) for at, then in zip(epochs[base], epochs[child], strict=True) if at is not None and then is not None]
        lagged = [then - at for at, then in both]
        if lagged and spread_kept([at for at, _ in both], [then for _, then in both], lagged):
            medians[(base, child)] = statistics.median_low(lagged)
    bases: dict[str, str] = {}
    for child in sorted({child for _, child in medians}, key=position.__getitem__):
        options = sorted((lag, position[base], base) for (base, named), lag in medians.items() if named == child)
        picked = next((base for _, _, base in options if not reaches(bases, base, child)), None)
        if picked is not None:
            bases[child] = picked
    chosen = [(base, child) for child, base in bases.items()]
    confirmed = [pair for pair, counts in zip(chosen, backend.ordered_shares(described, chosen), strict=True) if is_ordered(*counts, least)]
    kinds = {column.name: column.spec.kind for column in columns}
    found = {}
    for (base, child), (rows, at, low, high) in zip(
        confirmed, backend.lag_quantiles(described, confirmed, tuple(QUANTILES.values()), k_min), strict=True
    ):
        per, unit = LAG_UNITS[kinds[child]]

        def written(micros: float, per: int = per, unit: str = unit) -> str:
            return f'{round(micros / per)}{unit}'

        lag: dict[str, Any] = {}
        quantiles = {
            name: written(value)
            for (name, level), value in zip(QUANTILES.items(), at, strict=True)
            if clear_of_the_ends(level, rows, k_min)
        }
        if quantiles:
            lag['quantiles'] = quantiles
        if rows >= 2 * k_min - 1 and low is not None and high is not None:
            lag['min'], lag['max'] = written(low), written(high)
        if lag:
            found[child] = {'kind': 'timedelta', 'base': base, 'lag': lag}
    return found


def is_ordered(rows: int, at_or_after: int, least: int) -> bool:
    return rows >= least and at_or_after >= ORDERED * rows


def spread_kept(base: list[int], child: list[int], lagged: list[int]) -> bool:
    """Whether the base's variance and the lag's sum to the column's within `SPREAD_KEPT` of it."""
    spread = variance(child)
    return spread > 0 and abs(variance(base) + variance(lagged) - spread) <= SPREAD_KEPT * spread


def variance(values: list[int]) -> float:
    mean = math.fsum(values) / len(values)
    return math.fsum((value - mean) ** 2 for value in values) / len(values)


def reaches(bases: dict[str, str], start: str, target: str) -> bool:
    """Whether following each column's base from `start` arrives at `target`."""
    at: str | None = start
    while at is not None:
        if at == target:
            return True
        at = bases.get(at)
    return False


def epoch_micros(values: 'pa.ChunkedArray') -> list[int | None]:
    """A date or timestamp column as microseconds since the epoch, a null as None."""
    import pyarrow as pa

    kind = values.type
    if pa.types.is_date32(kind):
        return [None if days is None else days * LAG_UNITS[ColumnKind.DATE][0] for days in values.cast(pa.int32()).to_pylist()]
    if pa.types.is_date64(kind):
        return [None if millis is None else millis * 1_000 for millis in values.cast(pa.int64()).to_pylist()]
    return values.cast(pa.timestamp('us', kind.tz)).cast(pa.int64()).to_pylist()


def expressions(
    backend: Backend, held: 'pa.Table', described: Table, columns: list[DependenceColumn], k_min: int
) -> dict[str, dict[str, Any]]:
    """Each numeric column one expression of two numeric columns before it gives, the expression holding on most rows
    of the sample where several do, and the nearest columns where they tie."""
    import pyarrow as pa

    least = max(PAIR_ROWS, k_min)
    values = {column.name: held.column(column.name).cast(pa.float64()).combine_chunks() for column in columns if '`' not in column.name}
    first = {name: column.slice(0, SCREEN_ROWS) for name, column in values.items()}
    best: dict[str, tuple[float, tuple[str, str, str, str, float]]] = {}
    for at, target in enumerate(columns):
        scale = target_scale(target)
        if scale is None or target.name not in values:
            continue
        tolerance = 0.5 * 10.0**-scale
        window = [column.name for column in columns[max(0, at - OPERAND_WINDOW) : at] if column.name in values]
        for a, b in itertools.combinations(reversed(window), 2):
            for left, symbol, right in ((a, '+', b), (a, '*', b), (a, '-', b), (b, '-', a), (a, '/', b), (b, '/', a)):
                informative, matched = matches(first[left], symbol, first[right], first[target.name], tolerance)
                if not informative or informative - matched > (1 - HOLDS) * SCREEN_ROWS:
                    continue
                informative, matched = matches(values[left], symbol, values[right], values[target.name], tolerance)
                share = matched / informative if informative else 0.0
                if informative >= least and share >= HOLDS and share > best.get(target.name, (0.0,))[0]:
                    best[target.name] = (share, (left, symbol, right, target.name, tolerance))
    candidates = [candidate for _, candidate in sorted(best.values(), key=lambda found: -found[0])[:EXPRESSIONS_CONFIRMED]]
    found: dict[str, dict[str, Any]] = {}
    confirmed = backend.expression_matches(described, candidates)
    for (left, symbol, right, given, _), (rows, informative, matched) in zip(candidates, confirmed, strict=True):
        if informative >= least and matched >= HOLDS * informative:
            expr = f'{quoted(left)} {symbol} {quoted(right)}'
            found[given] = {'kind': 'expression', 'expr': expr, 'violation_rate': rate(informative - matched, rows)}
    return found


def target_scale(column: DependenceColumn) -> int | None:
    """The decimal places a column's values are rounded to, which an expression giving it is rounded to as well. None for
    a float column stating none."""
    if column.spec.kind is ColumnKind.INTEGER:
        return 0
    scale = (column.entry.get('numeric') or {}).get('scale')
    return int(scale) if scale is not None else None


def matches(left: 'pa.Array', symbol: str, right: 'pa.Array', target: 'pa.Array', tolerance: float) -> tuple[int, int]:
    """The rows where all three hold a value, less those whose target and `left symbol right` both lie within
    `tolerance` of zero, and those of the rest whose target lies within `tolerance` of it. A division by zero holds for
    no row, as `Backend.expression_matches` counts."""
    import pyarrow as pa
    import pyarrow.compute as pc

    if symbol == '/':
        computed = pc.if_else(pc.not_equal(right, 0), pc.divide(left, right), pa.scalar(None, pa.float64()))
    else:
        computed = {'+': pc.add, '-': pc.subtract, '*': pc.multiply}[symbol](left, right)
    held = pc.and_kleene(pc.and_kleene(pc.is_valid(left), pc.is_valid(right)), pc.is_valid(target))
    near_zero = pc.fill_null(pc.and_kleene(pc.equal(target, 0), pc.less_equal(pc.abs(computed), tolerance)), False)
    informative = pc.and_kleene(held, pc.invert(near_zero))
    close = pc.fill_null(pc.less_equal(pc.abs(pc.subtract(target, computed)), tolerance), False)
    return int(pc.sum(informative).as_py() or 0), int(pc.sum(pc.and_kleene(informative, close)).as_py() or 0)


def quoted(name: str) -> str:
    return name if UNQUOTED_NAME.fullmatch(name) else f'`{name}`'
