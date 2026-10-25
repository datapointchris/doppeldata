"""The six scores a check measures between the real data and the data checked, under SDMetrics' names and definitions.

Each follows SDMetrics 0.32.0's definition (https://docs.sdv.dev/sdmetrics) and reads a seeded sample of each side's
rows. They are computed here in Polars, because importing SDMetrics loads copulas, which is under the Business
Source License 1.1.
"""

import dataclasses as dc
import itertools
import math
from collections.abc import Callable
from collections.abc import Iterator
from typing import TYPE_CHECKING
from typing import Final

from doppeldata.profile_comparison.metrics import Bound
from doppeldata.profile_comparison.metrics import Compared
from doppeldata.profile_comparison.metrics import Document
from doppeldata.profile_comparison.metrics import Unmeasured
from doppeldata.profiling import SampledTable
from doppeldata.profiling.statistic_catalog import NUMERIC
from doppeldata.profiling.statistic_catalog import TEMPORAL
from doppeldata.profiling.statistic_catalog import ColumnKind
from doppeldata.profiling.table_profile import SAMPLE_SEED
from doppeldata.profiling.table_profile import profiled_kind

if TYPE_CHECKING:
    import polars as pl

DEFAULT_SAMPLE_ROWS = 100_000
# Spearman's rank correlation, because one extreme real value swings Pearson's from sample to sample.
CORRELATION: Final = 'spearman'
# KSComplement rounds every value to one digit under a double's precision, so a value one rounding apart is one value.
KS_DECIMALS = 14
# TVComplement counts a value only the checked data holds this many times in the real data, so its share is defined.
UNSEEN_COUNT = 1e-6
# ContingencySimilarity cuts a continuous column at this many equal-width bins of the real values' range.
CONTINGENCY_BINS = 10


@dc.dataclass(frozen=True)
class Score:
    """One score: its SDMetrics name, its summary, its reference threshold and whether it scores a pair of columns.

    A score decides nothing unless a thresholds file sets its threshold, so `threshold` is what its rows are read against.
    """

    name: str
    summary: str
    threshold: float
    pairs: bool
    bound: Bound = Bound.AT_LEAST

    def passes(self, value: float, threshold: float) -> bool:
        return value >= threshold if self.bound is Bound.AT_LEAST else value <= threshold


@dc.dataclass(frozen=True)
class Scored:
    """One score's outcome for one column, or a pair of columns where `paired_with` names the second."""

    score: Score
    column: str
    paired_with: str | None
    compared: Compared | Unmeasured


@dc.dataclass(frozen=True)
class ScoredColumns:
    """The columns both samples hold that a score reads, in the source profile's order, and each shape's."""

    ordered: list[str]
    continuous: list[str]
    discrete: list[str]


SCORES = (
    Score(
        'KSComplement',
        "one minus the Kolmogorov-Smirnov distance between a numeric or temporal column's real and checked values",
        0.8,
        False,
    ),
    Score(
        'BoundaryAdherence',
        "the share of a numeric or temporal column's checked values inside the real values' minimum and maximum",
        0.99,
        False,
    ),
    Score('TVComplement', "one minus the total variation distance between a categorical column's real and checked shares", 0.9, False),
    Score('CategoryCoverage', "the share of a categorical column's real values that the checked data holds", 0.9, False),
    Score(
        'CorrelationSimilarity',
        "one minus half the gap between two numeric or temporal columns' Spearman correlation in the real and checked data",
        0.8,
        True,
    ),
    Score(
        'ContingencySimilarity',
        "one minus the total variation distance between two columns' joint shares where one is categorical, "
        "a numeric or temporal one cut at ten equal-width bins of the real values' range",
        0.8,
        True,
    ),
)
SCORES_BY_NAME = {score.name: score for score in SCORES}


def scored_columns(source: Document, real: SampledTable, checked: SampledTable) -> ScoredColumns:
    """Each column of the source profile that both samples hold and a score reads.

    A numeric or temporal column is continuous, except a sequence, which draws nothing. A boolean, and a string the
    profile lists the values of, is discrete. A string with no value list, an identifier or free text, is left out.
    """
    held = set(real.table.column_names) & set(checked.table.column_names)
    ordered, continuous, discrete = [], [], []
    for column in source.get('columns') or []:
        if column['name'] not in held:
            continue
        kind = profiled_kind(column)
        if kind in NUMERIC | TEMPORAL and not (column.get('numeric') or {}).get('sequence'):
            continuous.append(column['name'])
        elif kind is ColumnKind.BOOLEAN or (kind is ColumnKind.STRING and (column.get('string') or {}).get('top_values')):
            discrete.append(column['name'])
        else:
            continue
        ordered.append(column['name'])
    return ScoredColumns(ordered, continuous, discrete)


def frame(sampled: SampledTable, columns: ScoredColumns, rows: int) -> 'pl.DataFrame':
    """`rows` of the sample's scored columns, each temporal column a timestamp in microseconds and every number a double.

    A timestamp is scored as its integer count, so two sides at different units would compare a thousandfold apart.
    """
    import polars as pl
    import polars.selectors as cs

    continuous = cs.by_name(columns.continuous, require_all=False)
    table = pl.DataFrame(sampled.table).select(columns.ordered)
    if table.height > rows:
        table = table.sample(rows, seed=SAMPLE_SEED)
    table = table.with_columns(cs.datetime(time_zone='*').dt.convert_time_zone('UTC').dt.replace_time_zone(None))
    return table.with_columns(
        (continuous & (cs.date() | cs.datetime())).cast(pl.Datetime('us')),
        (continuous & cs.numeric()).cast(pl.Float64),
    )


def scores(real: 'pl.DataFrame', checked: 'pl.DataFrame', columns: ScoredColumns) -> Iterator[Scored]:
    """Every score for each column, then for each pair, each in the source profile's order."""
    real, checked = numeric(real, columns), numeric(checked, columns)
    single: dict[str, tuple[Callable[[pl.Series, pl.Series], Compared | Unmeasured], list[str]]] = {
        'KSComplement': (ks_complement, columns.continuous),
        'BoundaryAdherence': (boundary_adherence, columns.continuous),
        'TVComplement': (tv_complement, columns.discrete),
        'CategoryCoverage': (category_coverage, columns.discrete),
    }
    for name, (score, named) in single.items():
        for column in named:
            outcome = unscored(real, checked, [column]) or score(real[column], checked[column])
            yield Scored(SCORES_BY_NAME[name], column, None, outcome)
    pairs = list(itertools.combinations(columns.ordered, 2))
    for a, b in pairs:
        if a in columns.continuous and b in columns.continuous:
            outcome = unscored(real, checked, [a, b]) or correlation_similarity(real, checked, a, b)
            yield Scored(SCORES_BY_NAME['CorrelationSimilarity'], a, b, outcome)
    for a, b in pairs:
        if a in columns.discrete or b in columns.discrete:
            binned = [name for name in (a, b) if name in columns.continuous]
            outcome = unscored(real, checked, [a, b]) or contingency_similarity(real, checked, a, b, binned)
            yield Scored(SCORES_BY_NAME['ContingencySimilarity'], a, b, outcome)


def unscored(real: 'pl.DataFrame', checked: 'pl.DataFrame', names: list[str]) -> Unmeasured | None:
    """Why a column or pair has no score, where a side's sample holds no row with a value in each of them."""
    import polars as pl

    for side, named in ((real, "the real data's"), (checked, "the checked data's")):
        if side.select(pl.all_horizontal(pl.col(name).is_not_null() for name in names).any()).item() is not True:
            where = 'the column' if len(names) == 1 else 'both columns'
            return Unmeasured(f'{named} sample holds no row with a value in {where}')
    return None


def numeric(table: 'pl.DataFrame', columns: ScoredColumns) -> 'pl.DataFrame':
    """Each continuous column as doubles, a timestamp as its count of microseconds, and NaN as null."""
    import polars as pl
    import polars.selectors as cs

    continuous = cs.by_name(columns.continuous, require_all=False)
    table = table.with_columns((continuous & cs.temporal()).cast(pl.Int64))
    return table.with_columns(continuous.cast(pl.Float64).fill_nan(None))


def ks_complement(real: 'pl.Series', checked: 'pl.Series') -> Compared | Unmeasured:
    """One minus the largest gap between the two empirical CDFs, read through each distinct value of either side."""
    import polars as pl

    a, b = (side.drop_nulls().round(KS_DECIMALS) for side in (real, checked))
    merged = pl.concat(
        [
            a.to_frame('value').with_columns(real=pl.lit(1), checked=pl.lit(0)),
            b.to_frame('value').with_columns(real=pl.lit(0), checked=pl.lit(1)),
        ]
    )
    by_value = merged.group_by('value').agg(pl.sum('real'), pl.sum('checked')).sort('value')
    gap = by_value.select((pl.col('real').cum_sum() / a.len() - pl.col('checked').cum_sum() / b.len()).abs().max()).item()
    return Compared(1 - gap)


def boundary_adherence(real: 'pl.Series', checked: 'pl.Series') -> Compared | Unmeasured:
    """The share of checked values inside the real values' minimum and maximum.

    Where the real data holds a null, a checked null is left out. Otherwise it counts as outside.
    """
    if real.null_count():
        checked = checked.drop_nulls()
    inside = checked.is_between(real.min(), real.max()).sum()
    return Compared(float(inside) / checked.len())


def shares(series: 'pl.Series') -> dict[object, int]:
    counted = series.drop_nulls().value_counts()
    return dict(zip(counted[series.name].to_list(), counted['count'].to_list(), strict=True))


def tv_complement(real: 'pl.Series', checked: 'pl.Series') -> Compared | Unmeasured:
    """One minus half the summed gap in share over the real values, and the values only the checked data holds."""
    ours: dict[object, float] = dict(shares(real))
    theirs = shares(checked)
    for value in theirs:
        ours.setdefault(value, UNSEEN_COUNT)
    ours_total, theirs_total = sum(ours.values()), sum(theirs.values())
    gap = sum(abs(theirs.get(value, 0) / theirs_total - count / ours_total) for value, count in ours.items())
    return Compared(1 - 0.5 * gap)


def category_coverage(real: 'pl.Series', checked: 'pl.Series') -> Compared | Unmeasured:
    """The share of the real data's distinct values the checked data holds."""
    ours, theirs = set(shares(real)), set(shares(checked))
    return Compared(len(ours & theirs) / len(ours))


def correlation_similarity(real: 'pl.DataFrame', checked: 'pl.DataFrame', a: str, b: str) -> Compared | Unmeasured:
    """One minus half the gap between the two sides' Spearman correlation, over the rows holding both values."""
    import polars as pl

    found = []
    for side in (real, checked):
        pair = side.select(a, b).drop_nulls()
        found.append(pair.select(pl.corr(a, b, method=CORRELATION)).item())
    if any(value is None or not math.isfinite(value) for value in found):
        return Unmeasured("a column holds one value in a side's sample, so it has no correlation")
    ours, theirs = found
    return Compared(1 - abs(ours - theirs) / 2, ours, theirs)


def contingency_similarity(real: 'pl.DataFrame', checked: 'pl.DataFrame', a: str, b: str, binned: list[str]) -> Compared | Unmeasured:
    """One minus the total variation distance between the two sides' shares of each pair of values, nulls included.

    Each continuous column is cut at the edges `bin_edges` takes from the real values.
    """
    import polars as pl

    keys = []
    for name in (a, b):
        if name in binned:
            column = pl.col(name)
            edges = bin_edges(real[name])
            keys.append(
                pl.when(column.is_null())
                .then(len(edges) + 2)
                .otherwise(pl.sum_horizontal(column >= edge for edge in edges) + 1)
                .alias(name)
            )
        else:
            keys.append(pl.col(name))
    merged = pl.concat(
        [
            real.select(*keys).with_columns(real=pl.lit(1), checked=pl.lit(0)),
            checked.select(*keys).with_columns(real=pl.lit(0), checked=pl.lit(1)),
        ]
    )
    by_pair = merged.group_by(a, b).agg(pl.sum('real'), pl.sum('checked'))
    gap = by_pair.select((pl.col('real') / real.height - pl.col('checked') / checked.height).abs().sum()).item()
    return Compared(1 - gap / 2)


def bin_edges(values: 'pl.Series') -> list[float]:
    """The interior edges of `CONTINGENCY_BINS` equal-width bins over the finite values' range, as numpy's
    `histogram_bin_edges` places them. A range of one value is widened by half a unit each side.
    """
    held = values.drop_nulls()
    finite = held.filter(held.is_finite())
    low, high = finite.min(), finite.max()
    if not isinstance(low, float) or not isinstance(high, float):
        return []
    if low == high:
        low, high = low - 0.5, high + 0.5
    step = (high - low) / CONTINGENCY_BINS
    return [at * step + low for at in range(1, CONTINGENCY_BINS)]
