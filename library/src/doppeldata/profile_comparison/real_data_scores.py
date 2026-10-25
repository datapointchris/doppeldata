"""The scores a check measures between the real data and the data checked, under SDMetrics' names and definitions.

Each follows SDMetrics 0.32.0's definition (https://docs.sdv.dev/sdmetrics) and reads a seeded sample of each side's
rows. They are computed here in Polars, because importing SDMetrics loads copulas, which is under the Business
Source License 1.1. A score added to `SCORES` reaches the report, `--thresholds` and `data check --help` with no other
change.
"""

import dataclasses as dc
import itertools
import math
from collections.abc import Callable
from collections.abc import Iterator
from typing import TYPE_CHECKING
from typing import Final

from doppeldata.privacy_policy import Policy
from doppeldata.privacy_policy.column_treatment import ColumnFacts
from doppeldata.privacy_policy.column_treatment import Treatment
from doppeldata.privacy_policy.column_treatment import column_treatment
from doppeldata.profile_comparison.metrics import Bound
from doppeldata.profile_comparison.metrics import Compared
from doppeldata.profile_comparison.metrics import Document
from doppeldata.profile_comparison.metrics import Thresholded
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
# A column a detector typed with one of these is never scored, so its real values never reach the HTML.
WITHHELD_SENSITIVITIES = frozenset({'identifier', 'sensitive'})


@dc.dataclass(frozen=True)
class ScoredColumns:
    """The source profile's columns a score reads, in its order, and each shape's."""

    ordered: list[str]
    continuous: list[str]
    discrete: list[str]


ScoreMeasure = Callable[['pl.DataFrame', 'pl.DataFrame', tuple[str, ...], ScoredColumns], Compared | Unmeasured]


@dc.dataclass(frozen=True)
class Score(Thresholded):
    """One score: `reads` yields each column, or pair of columns, it scores, and `measure` scores one of them.

    A score decides nothing unless a thresholds file sets its threshold, and a pair's is set under `defaults` alone.
    """

    pairs: bool
    reads: Callable[[ScoredColumns], Iterator[tuple[str, ...]]]
    measure: ScoreMeasure


@dc.dataclass(frozen=True)
class Scored:
    """One score's outcome for one column, or a pair of columns where `paired_with` names the second."""

    score: Score
    column: str
    paired_with: str | None
    compared: Compared | Unmeasured


def scored_columns(source: Document) -> ScoredColumns:
    """Each column of the source profile a score reads, and so the HTML shows.

    A numeric or temporal column is continuous, except a sequence, which draws nothing. A boolean, and a string the
    profile lists the values of, is discrete. A string with no value list, such as free text, is left out. So is a
    column the profile types as an identifier or sensitive, and one its privacy policy holds to more than `k_min`, such
    as a date under `healthcare`, because the HTML prints a column's real extremes and categories.
    """
    policy = Policy((source.get('privacy') or {}).get('policy') or Policy.DEFAULT)
    ordered, continuous, discrete = [], [], []
    for column in source.get('columns') or []:
        facts = ColumnFacts.of(column)
        if facts.sensitivity in WITHHELD_SENSITIVITIES or column_treatment(facts, policy) is not Treatment.AT_K_MIN:
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
    """`rows` of the scored columns the sample holds, each temporal one a timestamp in microseconds and every number a double.

    A timestamp is scored as its integer count, so two sides at different units would compare a thousandfold apart.
    """
    import polars as pl
    import polars.selectors as cs

    continuous = cs.by_name(columns.continuous, require_all=False)
    table = pl.DataFrame(sampled.table).select(name for name in columns.ordered if name in sampled.table.column_names)
    if table.height > rows:
        table = table.sample(rows, seed=SAMPLE_SEED)
    table = table.with_columns(cs.datetime(time_zone='*').dt.convert_time_zone('UTC').dt.replace_time_zone(None))
    return table.with_columns(
        (continuous & (cs.date() | cs.datetime())).cast(pl.Datetime('us')),
        (continuous & cs.numeric()).cast(pl.Float64),
    )


def scores(real: 'pl.DataFrame', checked: 'pl.DataFrame', columns: ScoredColumns) -> Iterator[Scored]:
    """Each of `SCORES` for each column or pair it reads, in the source profile's order."""
    real, checked = numeric(real, columns), numeric(checked, columns)
    for score in SCORES:
        for names in score.reads(columns):
            outcome = unscored(real, checked, names) or score.measure(real, checked, names, columns)
            yield Scored(score, names[0], names[1] if score.pairs else None, outcome)


def unscored(real: 'pl.DataFrame', checked: 'pl.DataFrame', names: tuple[str, ...]) -> Unmeasured | None:
    """Why a column or pair has no score: a side lacks one of them, or its sample holds no row with a value in each."""
    import polars as pl

    for side, whose in ((real, 'the real data'), (checked, 'the checked data')):
        lacking = [name for name in names if name not in side.columns]
        if lacking:
            return Unmeasured(f'{whose} holds no column {", ".join(lacking)}')
        if side.select(pl.all_horizontal(pl.col(name).is_not_null() for name in names).any()).item() is not True:
            where = 'the column' if len(names) == 1 else 'both columns'
            return Unmeasured(f"{whose}'s sample holds no row with a value in {where}")
    return None


def numeric(table: 'pl.DataFrame', columns: ScoredColumns) -> 'pl.DataFrame':
    """Each continuous column as doubles, a timestamp as its count of microseconds, and NaN as null."""
    import polars as pl
    import polars.selectors as cs

    continuous = cs.by_name(columns.continuous, require_all=False)
    table = table.with_columns((continuous & cs.temporal()).cast(pl.Int64))
    return table.with_columns(continuous.cast(pl.Float64).fill_nan(None))


def each_continuous(columns: ScoredColumns) -> Iterator[tuple[str, ...]]:
    return ((name,) for name in columns.continuous)


def each_discrete(columns: ScoredColumns) -> Iterator[tuple[str, ...]]:
    return ((name,) for name in columns.discrete)


def continuous_pairs(columns: ScoredColumns) -> Iterator[tuple[str, ...]]:
    return (pair for pair in itertools.combinations(columns.ordered, 2) if all(name in columns.continuous for name in pair))


def pairs_holding_discrete(columns: ScoredColumns) -> Iterator[tuple[str, ...]]:
    return (pair for pair in itertools.combinations(columns.ordered, 2) if any(name in columns.discrete for name in pair))


def on_column(score: Callable[['pl.Series', 'pl.Series'], Compared | Unmeasured]) -> ScoreMeasure:
    """A score of one column, read from each side's frame."""
    return lambda real, checked, names, _: score(real[names[0]], checked[names[0]])


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


def correlation_similarity(
    real: 'pl.DataFrame', checked: 'pl.DataFrame', names: tuple[str, ...], _: ScoredColumns
) -> Compared | Unmeasured:
    """One minus half the gap between the two sides' `CORRELATION` coefficient, over the rows holding both values."""
    import polars as pl

    a, b = names
    found = []
    for side in (real, checked):
        pair = side.select(a, b).drop_nulls()
        found.append(pair.select(pl.corr(a, b, method=CORRELATION)).item())
    if any(value is None or not math.isfinite(value) for value in found):
        return Unmeasured("a column holds one value in a side's sample, so it has no correlation")
    ours, theirs = found
    return Compared(1 - abs(ours - theirs) / 2, ours, theirs)


def contingency_similarity(
    real: 'pl.DataFrame', checked: 'pl.DataFrame', names: tuple[str, ...], columns: ScoredColumns
) -> Compared | Unmeasured:
    """One minus the total variation distance between the two sides' shares of each pair of values, nulls included.

    Each continuous column is cut at the edges `bin_edges` takes from the real values.
    """
    import polars as pl

    keys = []
    for name in names:
        if name in columns.continuous:
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
    by_pair = merged.group_by(*names).agg(pl.sum('real'), pl.sum('checked'))
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


SCORES = (
    Score(
        'KSComplement',
        "one minus the Kolmogorov-Smirnov distance between a numeric or temporal column's real and checked values",
        Bound.AT_LEAST,
        0.8,
        False,
        each_continuous,
        on_column(ks_complement),
    ),
    Score(
        'BoundaryAdherence',
        "the share of a numeric or temporal column's checked values inside the real values' minimum and maximum",
        Bound.AT_LEAST,
        0.99,
        False,
        each_continuous,
        on_column(boundary_adherence),
    ),
    Score(
        'TVComplement',
        "one minus the total variation distance between a categorical column's real and checked shares",
        Bound.AT_LEAST,
        0.9,
        False,
        each_discrete,
        on_column(tv_complement),
    ),
    Score(
        'CategoryCoverage',
        "the share of a categorical column's real values that the checked data holds",
        Bound.AT_LEAST,
        0.9,
        False,
        each_discrete,
        on_column(category_coverage),
    ),
    Score(
        'CorrelationSimilarity',
        f"one minus half the gap between two numeric or temporal columns' {CORRELATION.capitalize()} correlation "
        'in the real and checked data',
        Bound.AT_LEAST,
        0.8,
        True,
        continuous_pairs,
        correlation_similarity,
    ),
    Score(
        'ContingencySimilarity',
        "one minus the total variation distance between two columns' joint shares where one is categorical, "
        f"a numeric or temporal one cut at {CONTINGENCY_BINS} equal-width bins of the real values' range",
        Bound.AT_LEAST,
        0.8,
        True,
        pairs_holding_discrete,
        contingency_similarity,
    ),
)
SCORES_BY_NAME = {score.name: score for score in SCORES}
