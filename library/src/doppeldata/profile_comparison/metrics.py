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

Document = dict[str, Any]
Column = dict[str, Any]
Shares = tuple[float, float]
TEMPORAL_SHARES = ('by_month', 'by_dow', 'by_hour')


class Bound(enum.Enum):
    AT_MOST = 'at_most'
    AT_LEAST = 'at_least'


class Compared(NamedTuple):
    """One column's value, and the statistic it compares where it compares one: the source's and the observed profile's."""

    value: float
    source: float | None = None
    observed: float | None = None


@dc.dataclass(frozen=True)
class Measurement:
    """One value a metric measured, for one column, or for the table where `column` is None."""

    column: str | None
    compared: Compared


@dc.dataclass(frozen=True)
class Metric:
    name: str
    summary: str
    bound: Bound
    threshold: float
    measure: Callable[[Document, Document], Iterator[Measurement]]

    def passes(self, value: float, threshold: float) -> bool:
        return value <= threshold if self.bound is Bound.AT_MOST else value >= threshold


def per_column(measure_column: Callable[[Column, Column], Compared | None]) -> Callable[[Document, Document], Iterator[Measurement]]:
    """A table metric measuring each top-level column both profiles hold, in the source profile's order."""

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


def quantile_level(name: str) -> float | None:
    """The level a quantile's name stands for: `p05` is 0.05, `p999` is 0.999."""
    digits = name.removeprefix('p')
    return int(digits) / 10 ** len(digits) if name.startswith('p') and digits.isdigit() else None


def stated_points(numeric: dict[str, Any]) -> list[Stated]:
    """Every point a numeric block states its CDF at: each quantile, each histogram edge its counts pin, and zero.

    A p-quantile q states only that the share below q is at most p and the share through q at least p, so a value
    several quantiles share is a jump they all sit on. Every rate is a share of the non-null values, as the counts are.
    """
    points = [
        Stated(value, (0.0, level), (level, 1.0))
        for name, value in (numeric.get('quantiles') or {}).items()
        if (level := quantile_level(name)) is not None
    ]
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


def kolmogorov_smirnov(source: Column, observed: Column) -> Compared | None:
    """The largest gap between two numeric columns' CDFs that their profiles force, at the points the source states.

    At each point, each side's share below it and through it is the range its own stated points allow, with nothing
    interpolated between them. So the value is a lower bound on the KS distance between the two datasets.
    """
    a, b = source.get('numeric'), observed.get('numeric')
    # A sequence draws nothing, so it holds its start and step at any row count rather than a distribution.
    if not a or not b or a.get('sequence'):
        return None
    mine, theirs = stated_points(a), stated_points(b)
    if not mine or not theirs:
        return None
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


def total_variation(source: Column, observed: Column) -> Compared | None:
    """The share distance over a categorical column's values, or the largest over a date or timestamp's month, weekday and hour."""
    a, b = source.get('temporal') or {}, observed.get('temporal') or {}
    if a and b:
        distances = [
            share_distance(dict(enumerate(a[shares])), dict(enumerate(b[shares])))
            for shares in TEMPORAL_SHARES
            if a.get(shares) and b.get(shares)
        ]
        return Compared(max(distances)) if distances else None
    p, q = category_shares(source), category_shares(observed)
    if p is None or q is None:
        return None
    return Compared(share_distance(p, q))


def null_rate_change(source: Column, observed: Column) -> Compared | None:
    a, b = source.get('null_rate'), observed.get('null_rate')
    if a is None or b is None:
        return None
    return Compared(abs(b - a), a, b)


def zero_rate_change(source: Column, observed: Column) -> Compared | None:
    a, b = ((column.get('numeric') or {}).get('zero_rate') for column in (source, observed))
    if a is None or b is None:
        return None
    return Compared(abs(b - a), a, b)


def distinct_ratio_change(source: Column, observed: Column) -> Compared | None:
    """The change in distinct ratio relative to the source's. A source with no values has no ratio to measure against."""
    a, b = ((column.get('distinct') or {}).get('ratio') for column in (source, observed))
    if not a or b is None:
        return None
    return Compared(abs(b - a) / a, a, b)


def boundary_adherence(source: Column, observed: Column) -> Compared | None:
    """The share of observed values inside the source's outer histogram edges, from the rows counted outside them.

    The outer edges are the source's trimmed extremes, or half a unit beyond them in an integral column. Only a
    histogram counted against the source's edges carries `overflow`, so a profile binned afresh is not measured.
    """
    numeric = source.get('numeric') or {}
    edges, counted = numeric.get('histogram'), (observed.get('numeric') or {}).get('histogram')
    if not edges or not counted or 'overflow' not in counted or numeric.get('sequence'):
        return None
    if (counted['edges'][0], counted['edges'][-1]) != (edges['edges'][0], edges['edges'][-1]):
        return None
    outside = counted['overflow']['below'] + counted['overflow']['above']
    total = outside + sum(counted['counts'])
    return Compared(1 - outside / total) if total else None


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
        "the difference in distinct values per row, relative to the source's",
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
)
METRICS_BY_NAME = {metric.name: metric for metric in METRICS}
