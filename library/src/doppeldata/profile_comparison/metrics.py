"""The metrics a check and a diff measure, as data: each one's name, bound, default threshold and measurement.

A metric is a function from two profiles to values, each for one column or for the table. A metric
added to `METRICS` reaches the reports and `--thresholds` with no other change.
"""

import bisect
import dataclasses as dc
import enum
import json
from collections.abc import Callable
from collections.abc import Iterator
from typing import Any
from typing import NamedTuple

from doppeldata import _core

Document = dict[str, Any]
Column = dict[str, Any]
Knots = list[tuple[float, float]]


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


# The CDF the generator draws each kind of column from, by the profile block that holds its statistics.
CDF_KNOTS: dict[str, Callable[[str], Knots]] = {'numeric': _core.numeric_cdf_knots, 'temporal': _core.temporal_cdf_knots}


def share_up_to(knots: Knots, values: list[float], x: float, *, strictly: bool) -> float:
    """The share of the distribution at or below `x`, or strictly below it, read from its inverse CDF's knots.

    Two knots at one value are a jump at that value, so the share strictly below it is the lower level.
    """
    found = (bisect.bisect_left if strictly else bisect.bisect_right)(values, x) - 1
    if found < 0:
        return 0.0
    if found == len(knots) - 1:
        return knots[found][0]
    (low_level, low), (high_level, high) = knots[found], knots[found + 1]
    return low_level + (x - low) / (high - low) * (high_level - low_level)


def largest_cdf_gap(a: Knots, b: Knots) -> float:
    """The supremum of the two CDFs' difference. Both are linear between knots, so it falls at a knot's value or just below one."""
    values_a, values_b = [value for _, value in a], [value for _, value in b]
    gaps = (
        abs(share_up_to(a, values_a, x, strictly=strictly) - share_up_to(b, values_b, x, strictly=strictly))
        for x in sorted({*values_a, *values_b})
        for strictly in (False, True)
    )
    return max(gaps, default=0.0)


def kolmogorov_smirnov(source: Column, observed: Column) -> Compared | None:
    """The largest gap between the CDFs the generator would draw a numeric, date or timestamp column from, by each profile."""
    for block, knots_of in CDF_KNOTS.items():
        a, b = source.get(block), observed.get(block)
        if a and b:
            # A sequence draws nothing, so it holds its start and step at any row count rather than a distribution.
            if a.get('sequence'):
                return None
            return Compared(largest_cdf_gap(knots_of(json.dumps(a)), knots_of(json.dumps(b))))
    return None


def category_shares(column: Column) -> dict[str, float] | None:
    """A categorical column's share of values in each listed category: a string column's top values, or a boolean's two."""
    string, boolean = column.get('string') or {}, column.get('boolean') or {}
    if string.get('top_values'):
        return {entry['value']: entry['rate'] for entry in string['top_values']}
    if boolean.get('true_rate') is not None:
        return {'true': boolean['true_rate'], 'false': 1 - boolean['true_rate']}
    return None


def total_variation(source: Column, observed: Column) -> Compared | None:
    """Half the summed difference in share over the source's categories, and over every other value as one more."""
    p, listed = category_shares(source), category_shares(observed)
    if p is None or listed is None:
        return None
    q = {category: listed.get(category, 0.0) for category in p}
    other_p, other_q = max(0.0, 1 - sum(p.values())), max(0.0, 1 - sum(q.values()))
    return Compared((sum(abs(p[category] - q[category]) for category in p) + abs(other_p - other_q)) / 2)


def null_rate_change(source: Column, observed: Column) -> Compared | None:
    a, b = source.get('null_rate'), observed.get('null_rate')
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

    The outer edges are the source's trimmed extremes, or half a unit beyond them in an integer column. Only a
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
    Metric('ks', Bound.AT_MOST, 0.05, per_column(kolmogorov_smirnov)),
    Metric('tvd', Bound.AT_MOST, 0.05, per_column(total_variation)),
    Metric('null_rate_delta', Bound.AT_MOST, 0.01, per_column(null_rate_change)),
    Metric('distinct_ratio_delta', Bound.AT_MOST, 0.10, per_column(distinct_ratio_change)),
    Metric('boundary_adherence', Bound.AT_LEAST, 0.99, per_column(boundary_adherence)),
)
METRICS_BY_NAME = {metric.name: metric for metric in METRICS}
