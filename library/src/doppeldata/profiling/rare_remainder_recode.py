"""The listing pre-pass. Each column whose kind lists values is counted before any statistic, and a remainder of 1 to
k_min - 1 rows outside its list is rewritten as its modal value. Every statistic is then computed over the rewritten
rows, so no count the profile states differs from another by the rows rewritten. This is global recoding, as sdcMicro
and mu-Argus apply it."""

import dataclasses as dc
import functools
import math
from collections.abc import Mapping
from typing import Any

from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Table
from doppeldata.profiling.profiled_columns import Answers
from doppeldata.profiling.recorded_counts import FlooredValues
from doppeldata.profiling.recorded_counts import Recode
from doppeldata.profiling.recorded_counts import floored_values
from doppeldata.profiling.statistic_catalog import COLUMN_STATISTICS
from doppeldata.profiling.statistic_catalog import VALUE_LISTS


@dc.dataclass(frozen=True)
class ListedCounts:
    """What the pre-pass counted, and the recode it found.

    `stats` holds the rows, and each statistic the catalog counts `before_recode`, over the rows as read. `counted`
    holds each listed column's values as its list writes them, after the recode: an earlier profile's list in its
    order, or else the values most held first.
    """

    stats: Answers
    counted: dict[str, FlooredValues]

    @property
    def recodes(self) -> dict[str, Recode]:
        """Each column whose remainder is rewritten, and how."""
        return {name: floored.recode for name, floored in self.counted.items() if floored.recode}


def listed_counts(
    backend: Backend, table: Table, specs: list[ColumnSpec], frozen: Mapping[str, tuple[Any, ...]], k_min: int
) -> ListedCounts:
    """Each column of a kind that lists values counted over `table`: against the values `frozen` holds for it, an
    earlier profile's list, or else where it holds no more distinct values than its kind's listing threshold.

    A remainder the counts do not name whole, as where a float's NaN takes a place, folds in a listed value instead.
    """
    requests = [Request(None, 'rows')]
    for spec in specs:
        requests += [Request(spec.name, stat.name) for stat in COLUMN_STATISTICS if stat.before_recode and spec.kind in stat.kinds]
        if spec.name in frozen:
            requests.append(Request(spec.name, 'listed_value_counts', frozen[spec.name]))
    answers = backend.aggregate(table, requests)
    stats: Answers = {(request.column, request.statistic): answer for request, answer in answers.items() if request.argument is None}
    counted: dict[str, FlooredValues] = {}
    for spec in specs:
        listing = VALUE_LISTS.get(spec.kind)
        if listing is None:
            continue
        values = stats[(None, 'rows')] - stats[(spec.name, 'null_count')]
        if spec.name in frozen:
            listed = list(zip(frozen[spec.name], answers[Request(spec.name, 'listed_value_counts', frozen[spec.name])], strict=True))
            counted[spec.name] = floored_values(listed, values, k_min, functools.partial(most_held, backend, table, spec.name))
        elif stats[(spec.name, 'distinct_count')] <= listing.threshold:
            # Spark estimates the distinct count, so the values counted past the threshold name a remainder it missed.
            found = most_held(backend, table, spec.name, listing.threshold + k_min - 1)
            counted[spec.name] = floored_values(found[: listing.threshold], values, k_min, functools.partial(leading, found))
    return ListedCounts(stats, counted)


def most_held(backend: Backend, table: Table, name: str, most: int) -> list[tuple[Any, int]]:
    """The column's `most` values most held, with the rows of each, leaving out a float's NaN and infinities, which
    every statistic reads as null."""
    return [(value, count) for value, count in backend.top_k(table, name, most) if not isinstance(value, float) or math.isfinite(value)]


def leading(ranked: list[tuple[Any, int]], most: int) -> list[tuple[Any, int]]:
    """The first `most` of counts already ranked most held first."""
    return ranked[:most]
