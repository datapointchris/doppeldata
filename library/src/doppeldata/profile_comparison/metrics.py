"""The metrics a check and a diff measure, as data: each one's name, summary, bound, default threshold and measurement.

A metric is a function from two profiles to values, each for one column or for the table. A metric
added to `METRICS` reaches the reports, `--thresholds` and the CLI's help with no other change.
"""

import dataclasses as dc
import enum
import itertools
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from typing import Any
from typing import NamedTuple

from doppeldata.profile_format import quantile_level

Document = dict[str, Any]
Column = dict[str, Any]
Shares = tuple[float, float]
TEMPORAL_SHARES = ('by_month', 'by_dow', 'by_hour')


class Bound(enum.Enum):
    AT_MOST = 'at_most'
    AT_LEAST = 'at_least'


class Compared(NamedTuple):
    """One column's value, and the statistic it compares where it compares one: the source profile's and the other's."""

    value: float
    expected: float | None = None
    observed: float | None = None


class Unmeasured(NamedTuple):
    """Why a metric has no value where the source profile states what it compares: the other profile cannot supply it."""

    reason: str
    expected: float | None = None


@dc.dataclass(frozen=True)
class Measurement:
    """One metric's outcome for one column, or for the table where `column` is None."""

    column: str | None
    compared: Compared | Unmeasured


@dc.dataclass(frozen=True)
class Metric:
    name: str
    summary: str
    bound: Bound
    threshold: float
    measure: Callable[[Document, Document], Iterator[Measurement]]

    def passes(self, value: float, threshold: float) -> bool:
        return value <= threshold if self.bound is Bound.AT_MOST else value >= threshold


def per_column(
    measure_column: Callable[[Column, Column], Compared | Unmeasured | None],
) -> Callable[[Document, Document], Iterator[Measurement]]:
    """A table metric measuring each top-level column both profiles hold, in the source profile's order.

    `measure_column` returns None where the source states nothing the metric compares, so the metric does not apply.
    """

    def measure(source: Document, observed: Document) -> Iterator[Measurement]:
        theirs = {column['name']: column for column in observed.get('columns') or []}
        for column in source.get('columns') or []:
            if column['name'] in theirs:
                compared = measure_column(column, theirs[column['name']])
                if compared is not None:
                    yield Measurement(column['name'], compared)

    return measure


@dc.dataclass(frozen=True)
class Stated:
    """What a profile states about its CDF at `value`: the least and greatest share below it, and through it."""

    value: float
    below: Shares
    through: Shares


def stated_points(numeric: dict[str, Any]) -> list[Stated]:
    """Every point a numeric block states its CDF at: each quantile, each histogram edge its counts pin, and zero.

    A p-quantile q states only that the share below q is at most p and the share through q at least p, so a value
    several quantiles share is a jump they all sit on. Every rate is a share of the non-null values, as the counts are.
    """
    quantiles = {quantile_level(name): value for name, value in (numeric.get('quantiles') or {}).items()}
    points = [Stated(value, (0.0, level), (level, 1.0)) for level, value in quantiles.items()]
    histogram = numeric.get('histogram') or {}
    edges, counts, overflow = histogram.get('edges') or [], histogram.get('counts') or [], histogram.get('overflow')
    below, above = (overflow['below'], overflow['above']) if overflow else (0, 0)
    total = below + sum(counts) + above
    if total and len(edges) == len(counts) + 1:
        cumulative = [share / total for share in itertools.accumulate(counts, initial=below)]
        # Without overflow, the outer bins hold the rows beyond the trimmed extremes, so the outer edges pin nothing.
        pinned = range(len(edges) - 1) if overflow else range(1, len(edges) - 1)
        points += [Stated(edges[at], (cumulative[at], cumulative[at]), (cumulative[at], 1.0)) for at in pinned]
        if overflow:
            # The last bin holds its upper edge, so its count reaches through that edge rather than below it.
            points.append(Stated(edges[-1], (0.0, cumulative[-1]), (cumulative[-1], cumulative[-1])))
    negative, zero = numeric.get('negative_rate'), numeric.get('zero_rate')
    if negative is not None and zero is not None:
        points.append(Stated(0.0, (negative, negative), (negative + zero, negative + zero)))
    return points


def allowed(points: list[Stated], x: float) -> tuple[Shares, Shares]:
    """The least and greatest share below `x`, and through `x`, of any CDF holding every stated point."""
    below = (
        max([p.through[0] for p in points if p.value < x] + [p.below[0] for p in points if p.value == x], default=0.0),
        min([p.below[1] for p in points if p.value >= x], default=1.0),
    )
    through = (
        max([p.through[0] for p in points if p.value <= x], default=0.0),
        min([p.below[1] for p in points if p.value > x] + [p.through[1] for p in points if p.value == x], default=1.0),
    )
    return below, through


def apart(a: Shares, b: Shares) -> float:
    """How far apart two ranges of share are, zero where they overlap."""
    return max(0.0, b[0] - a[1], a[0] - b[1])


def kolmogorov_smirnov(source: Column, observed: Column) -> Compared | Unmeasured | None:
    """The largest gap between two numeric columns' CDFs that their profiles force, at the points the source states.

    At each point, each side's share below it and through it is the range its own stated points allow, with nothing
    interpolated between them. So the value is a lower bound on the KS distance between the two datasets.
    """
    a, b = source.get('numeric'), observed.get('numeric')
    # A sequence draws nothing, so it holds its start and step at any row count rather than a distribution.
    if not a or a.get('sequence'):
        return None
    mine, theirs = stated_points(a), stated_points(b or {})
    if not mine:
        return None
    if not theirs:
        return Unmeasured('the other profile states no quantile, histogram edge or zero share for the column')
    gaps = []
    for x in {point.value for point in mine}:
        (below_mine, through_mine), (below_theirs, through_theirs) = allowed(mine, x), allowed(theirs, x)
        gaps += [apart(below_mine, below_theirs), apart(through_mine, through_theirs)]
    return Compared(max(gaps))


def category_shares(column: Column) -> dict[str, float] | None:
    """A categorical column's share of values in each listed category: a string column's top values, or a boolean's two."""
    string, boolean = column.get('string') or {}, column.get('boolean') or {}
    if string.get('top_values'):
        return {entry['value']: entry['rate'] for entry in string['top_values']}
    if boolean.get('true_rate') is not None:
        return {'true': boolean['true_rate'], 'false': 1 - boolean['true_rate']}
    return None


def share_distance(p: Mapping[Any, float], q: Mapping[Any, float]) -> float:
    """Half the summed difference in share, over `p`'s categories and every other value as one more."""
    listed = {category: q.get(category, 0.0) for category in p}
    other_p, other_q = max(0.0, 1 - sum(p.values())), max(0.0, 1 - sum(listed.values()))
    return (sum(abs(p[category] - listed[category]) for category in p) + abs(other_p - other_q)) / 2


def total_variation(source: Column, observed: Column) -> Compared | Unmeasured | None:
    """The share distance over a categorical column's values, or the largest over a date or timestamp's month, weekday and hour."""
    a, b = source.get('temporal') or {}, observed.get('temporal') or {}
    if any(a.get(shares) for shares in TEMPORAL_SHARES):
        distances = [
            share_distance(dict(enumerate(a[shares])), dict(enumerate(b[shares])))
            for shares in TEMPORAL_SHARES
            if a.get(shares) and b.get(shares)
        ]
        return Compared(max(distances)) if distances else Unmeasured('the other profile holds none of the month, weekday or hour shares')
    p, q = category_shares(source), category_shares(observed)
    if p is None:
        return None
    if q is None:
        return Unmeasured('the other profile lists no top values or true rate for the column')
    return Compared(share_distance(p, q))


def null_rate_change(source: Column, observed: Column) -> Compared | Unmeasured | None:
    a, b = source.get('null_rate'), observed.get('null_rate')
    return rate_change(a, b, 'the other profile holds no null rate for the column')


def zero_rate_change(source: Column, observed: Column) -> Compared | Unmeasured | None:
    a, b = ((column.get('numeric') or {}).get('zero_rate') for column in (source, observed))
    return rate_change(a, b, 'the other profile holds no zero rate for the column, which it writes from k_min values')


def rate_change(a: float | None, b: float | None, missing: str) -> Compared | Unmeasured | None:
    """The absolute difference of two rates, Unmeasured with `missing` where only the source holds one."""
    if a is None:
        return None
    if b is None:
        return Unmeasured(missing, a)
    return Compared(abs(b - a), a, b)


def distinct_ratio_change(source: Column, observed: Column) -> Compared | Unmeasured | None:
    """How far the observed distinct ratio falls outside the range `distinct_ratio_range` allows, relative to its nearer end.

    At the source's own row count the range is the source's ratio, so the value is the relative change in ratio. A source
    with no values has no ratio to measure against.
    """
    ours, theirs = ((column.get('distinct') or {}) for column in (source, observed))
    a, b = ours.get('ratio'), theirs.get('ratio')
    if not a:
        return None
    if b is None:
        return Unmeasured('the other profile counts no distinct values for the column, because it holds none', a)
    if not ours.get('approx') or not theirs.get('approx'):
        return Compared(abs(b - a) / a, a, b)
    low, high = distinct_ratio_range(ours['approx'], a, theirs['approx'] / b)
    outside = (low - b) / low if b < low else (b - high) / high if b > high else 0.0
    return Compared(outside, a, b)


def distinct_ratio_range(count: int, ratio: float, rows: float) -> tuple[float, float]:
    """The distinct ratios `rows` rows may hold of a source whose `count` distinct values are `ratio` of its rows.

    One profile cannot show whether a column keeps its ratio at another size or keeps its count, so the range holds both.
    One end is the ratio held. Above the source's rows the other is `count` saturated. Below them it is what a random
    sample of the source holds where each value fills an equal share of its rows: `count · (1 − (1 − rows/N)^(N/count))`.
    """
    source_rows = count / ratio
    if rows >= source_rows:
        return count / rows, ratio
    sampled = count * (1 - (1 - rows / source_rows) ** (source_rows / count))
    return ratio, sampled / rows


def boundary_adherence(source: Column, observed: Column) -> Compared | Unmeasured | None:
    """The share of observed values inside the source's outer histogram edges, from the rows counted outside them.

    The outer edges are the source's trimmed extremes, or half a unit beyond them in an integral column. Only a
    histogram counted against the source's edges carries `overflow`, so a profile binned afresh is Unmeasured.
    """
    numeric = source.get('numeric') or {}
    edges, counted = numeric.get('histogram'), (observed.get('numeric') or {}).get('histogram')
    if not edges or numeric.get('sequence'):
        return None
    if not counted or 'overflow' not in counted or (counted['edges'][0], counted['edges'][-1]) != (edges['edges'][0], edges['edges'][-1]):
        return Unmeasured("the other profile was not counted against the source's histogram edges")
    outside = counted['overflow']['below'] + counted['overflow']['above']
    total = outside + sum(counted['counts'])
    return Compared(1 - outside / total) if total else Unmeasured('the other profile counts no values for the column')


def relative_change(expected: float, observed: float | None, missing: str) -> Compared | Unmeasured:
    if observed is None:
        return Unmeasured(missing, expected)
    return Compared(abs(observed - expected) / expected, expected, observed)


def layout_change(source: Document, observed: Document) -> Iterator[Measurement]:
    """How far the files fall from the source's layout, relative to it.

    The table's row takes the larger of the file count's change and the median rows per file's. The count expected is the
    source's scaled to the rows checked, and the median is at most those rows. Each Hive key's row takes the largest change
    in a value's share of rows, over the values the source lists.
    """
    ours = (source.get('source') or {}).get('layout') or {}
    theirs = (observed.get('source') or {}).get('layout') or {}
    if not ours:
        return
    if not theirs:
        yield Measurement(None, Unmeasured('the other profile records no file layout, as a profile of a table in memory does'))
        return
    source_rows, rows = ((document.get('dataset') or {}).get('row_count') for document in (source, observed))
    scale = rows / source_rows if source_rows and rows is not None else 1.0
    table: list[Compared | Unmeasured] = []
    if count := (ours.get('files') or {}).get('count'):
        table.append(
            relative_change(max(1.0, count * scale), (theirs.get('files') or {}).get('count'), 'the other profile counts no files')
        )
    if median := (ours.get('rows_per_file') or {}).get('p50'):
        expected = min(median, rows) if rows else median
        table.append(relative_change(expected, (theirs.get('rows_per_file') or {}).get('p50'), 'the other profile counts no rows per file'))
    if table:
        unmeasured = [compared for compared in table if isinstance(compared, Unmeasured)]
        measured = [compared for compared in table if isinstance(compared, Compared)]
        yield Measurement(None, unmeasured[0] if unmeasured else max(measured, key=lambda compared: compared.value))
    keys = {key['name']: key for key in (theirs.get('partitioning') or {}).get('keys') or []}
    for key in (ours.get('partitioning') or {}).get('keys') or []:
        shares = (key.get('values') or {}).get('shares')
        if not shares:
            continue
        held = ((keys.get(key['name']) or {}).get('values') or {}).get('shares')
        if held is None:
            yield Measurement(key['name'], Unmeasured(f"the other profile's files hold no shares of {key['name']}'s values"))
            continue
        changes = [
            Compared(abs(held.get(value, 0.0) - share) / share, share, held.get(value, 0.0)) for value, share in shares.items() if share
        ]
        if changes:
            yield Measurement(key['name'], max(changes, key=lambda compared: compared.value))


METRICS = (
    Metric(
        'ks',
        'the Kolmogorov-Smirnov distance, the largest gap between two cumulative distributions at the points the source states',
        Bound.AT_MOST,
        0.05,
        per_column(kolmogorov_smirnov),
    ),
    Metric(
        'tvd',
        "the total variation distance between the shares of the source's listed values, or a date's months, weekdays and hours",
        Bound.AT_MOST,
        0.05,
        per_column(total_variation),
    ),
    Metric('null_rate_delta', 'the difference in the share of values that are null', Bound.AT_MOST, 0.01, per_column(null_rate_change)),
    Metric(
        'zero_rate_delta',
        'the difference in the share of non-null values that are zero',
        Bound.AT_MOST,
        0.01,
        per_column(zero_rate_change),
    ),
    Metric(
        'distinct_ratio_delta',
        'how far the distinct values per row fall outside the range the source allows at this many rows, relative to it',
        Bound.AT_MOST,
        0.10,
        per_column(distinct_ratio_change),
    ),
    Metric(
        'boundary_adherence',
        "the share of values inside the source's outer histogram edges",
        Bound.AT_LEAST,
        0.99,
        per_column(boundary_adherence),
    ),
    Metric(
        'layout',
        "the largest change in the file count, the median rows per file or a Hive key's value shares, relative to the source's",
        Bound.AT_MOST,
        0.10,
        layout_change,
    ),
)
METRICS_BY_NAME = {metric.name: metric for metric in METRICS}
