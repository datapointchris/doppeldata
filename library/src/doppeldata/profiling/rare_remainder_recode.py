"""The listing pre-pass. Each column whose kind lists values is counted before any statistic, and a remainder of 1 to
k_min - 1 rows outside its list is rewritten as its modal value. Every statistic is then computed over the rewritten
rows, so no count the profile states differs from another by the rows rewritten. This is global recoding, as sdcMicro
and mu-Argus apply it."""

import dataclasses as dc
import math
from collections.abc import Mapping
from typing import Any

from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import Recode
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Table
from doppeldata.profiling.profiled_columns import Answers
from doppeldata.profiling.statistic_catalog import NUMERIC
from doppeldata.profiling.statistic_catalog import PROFILED
from doppeldata.profiling.statistic_catalog import VALUE_LISTS


@dc.dataclass(frozen=True)
class ListedCounts:
    """What the pre-pass counted, and the recode it found.

    `stats` holds the rows, and each profiled column's null and distinct counts, over the rows before the recode, so a
    column's distinct count is its own. `counted` holds each listed column's values and the rows each holds after the
    recode: an earlier profile's list in its order, or else the values most held first. `recodes` holds each column
    whose remainder is rewritten.
    """

    stats: Answers
    counted: dict[str, list[tuple[Any, int]]]
    recodes: dict[str, Recode]


def listed_counts(
    backend: Backend, table: Table, specs: list[ColumnSpec], frozen: Mapping[str, tuple[Any, ...]], k_min: int
) -> ListedCounts:
    """Each column of a kind that lists values counted over `table`: against the values `frozen` holds for it, an
    earlier profile's list, or else where it holds no more distinct values than its kind's listing threshold.

    A remainder of fewer than k_min rows holds fewer than k_min values, so the values counted past the threshold always
    name it. A remainder the counts do not name whole, as where a float's NaN takes a place, is left to `kept_values`.
    """
    requests = [Request(None, 'rows')]
    for spec in specs:
        if spec.kind in PROFILED:
            requests += [Request(spec.name, 'null_count'), Request(spec.name, 'distinct_count')]
        if spec.name in frozen:
            requests.append(Request(spec.name, 'listed_value_counts', frozen[spec.name]))
    answers = backend.aggregate(table, requests)
    stats: Answers = {(request.column, request.statistic): answer for request, answer in answers.items() if request.argument is None}
    counted: dict[str, list[tuple[Any, int]]] = {}
    recodes: dict[str, Recode] = {}
    for spec in specs:
        listing = VALUE_LISTS.get(spec.kind)
        if listing is None:
            continue
        values = stats[(None, 'rows')] - stats[(spec.name, 'null_count')]
        if spec.name in frozen:
            listed = list(zip(frozen[spec.name], answers[Request(spec.name, 'listed_value_counts', frozen[spec.name])], strict=True))
            counted[spec.name], recode = frozen_recode(backend, table, spec, listed, values, k_min)
        elif stats[(spec.name, 'distinct_count')] <= listing.threshold:
            found = finite(spec, backend.top_k(table, spec.name, listing.threshold + k_min - 1))
            kept, recode = recoded(found, [(value, count) for value, count in found if count < k_min], values, k_min)
            counted[spec.name] = (kept if recode else found)[: listing.threshold]
        else:
            continue
        if recode:
            recodes[spec.name] = recode
    return ListedCounts(stats, counted, recodes)


def frozen_recode(
    backend: Backend, table: Table, spec: ColumnSpec, listed: list[tuple[Any, int]], values: int, k_min: int
) -> tuple[list[tuple[Any, int]], Recode | None]:
    """An earlier profile's values with the rows each holds after the recode, and the recode. A value outside the list
    is not counted by it, so the remainder's values are named by the column's most held values past those kept."""
    kept = [(value, count) for value, count in listed if count >= k_min]
    if not 0 < values - sum(count for _, count in kept) < k_min:
        return listed, None
    held = {value for value, _ in kept}
    found = finite(spec, backend.top_k(table, spec.name, len(kept) + k_min - 1))
    after, recode = recoded(listed, [(value, count) for value, count in found if value not in held], values, k_min)
    if recode is None:
        return listed, None
    rows = dict(after)
    return [(value, rows.get(value, 0)) for value, _ in listed], recode


def recoded(
    counted: list[tuple[Any, int]], rest: list[tuple[Any, int]], values: int, k_min: int
) -> tuple[list[tuple[Any, int]], Recode | None]:
    """The values at least k_min rows hold, the most held of them, the first where several tie, taking the rows of
    `rest`, and the recode. None where the rows outside them are 0 or at least k_min, or `rest` does not hold them all."""
    kept = [(value, count) for value, count in counted if count >= k_min]
    other = values - sum(count for _, count in kept)
    if not kept or not 0 < other < k_min or sum(count for _, count in rest) != other:
        return kept, None
    modal = max(range(len(kept)), key=lambda at: (kept[at][1], -at))
    kept[modal] = (kept[modal][0], kept[modal][1] + other)
    return kept, Recode(tuple(value for value, _ in rest), kept[modal][0])


def finite(spec: ColumnSpec, counted: list[tuple[Any, int]]) -> list[tuple[Any, int]]:
    """The counts leaving out a float's NaN and infinities, which every statistic reads as null."""
    if spec.kind not in NUMERIC:
        return counted
    return [(value, count) for value, count in counted if math.isfinite(value)]
