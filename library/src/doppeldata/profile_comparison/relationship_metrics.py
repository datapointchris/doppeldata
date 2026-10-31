"""The metrics a bundle check measures on each relationship, from the child rows each parent key holds in the generated
tables: how they spread over the parents, the share naming no parent, and the busiest parents' share, each against what
the relationship states.

Each takes the counts `Backend.foreign_key_counts` returns: `(in_parent, child_rows, values)`, how many key values the
parent holds or not that `child_rows` child rows hold, a parent key no child row holds counted at 0.
"""

import math
from typing import Any

from doppeldata.profile_comparison.metrics import Bound
from doppeldata.profile_comparison.metrics import Compared
from doppeldata.profile_comparison.metrics import Thresholded
from doppeldata.profile_comparison.metrics import Unmeasured
from doppeldata.profile_comparison.metrics import allowed
from doppeldata.profile_comparison.metrics import apart
from doppeldata.profile_comparison.metrics import stated_points
from doppeldata.profiling.foreign_keys import TOP_SHARES
from doppeldata.profiling.foreign_keys import rank_value
from doppeldata.profiling.foreign_keys import sum_of_busiest

Counts = list[tuple[bool, int, int]]

FANOUT_KS = Thresholded(
    'fanout_ks',
    'the Kolmogorov-Smirnov distance between the child rows per parent key the relationship states as quantiles and those '
    'generated, at the stated quantiles',
    Bound.AT_MOST,
    0.05,
)
ORPHAN_RATE_DELTA = Thresholded(
    'orphan_rate_delta',
    "the difference between the share of child rows holding a key no parent holds and the relationship's orphan_rate",
    Bound.AT_MOST,
    0.005,
)
TOP_SHARE_DELTA = Thresholded(
    'top_share_delta',
    "the largest difference in the busiest parents' share of the child rows that name a parent, across top1, top10 and top100",
    Bound.AT_MOST,
    0.001,
)
RELATIONSHIP_METRICS = (FANOUT_KS, ORPHAN_RATE_DELTA, TOP_SHARE_DELTA)
RELATIONSHIP_METRICS_BY_NAME = {metric.name: metric for metric in RELATIONSHIP_METRICS}


def parents_of(counts: Counts) -> list[tuple[int, int]]:
    """The child rows each parent key holds, paired with how many keys hold that many, rising."""
    return sorted((rows, values) for held, rows, values in counts if held)


def fanout_ks(relationship: dict[str, Any], counts: Counts) -> Compared | Unmeasured | None:
    """The largest gap between the stated quantiles' CDF and the generated child rows per parent key, at each stated value.
    None where the relationship states no quantile."""
    quantiles = (relationship.get('fanout') or {}).get('quantiles')
    if not quantiles:
        return None
    parents = parents_of(counts)
    keys = sum(values for _, values in parents)
    if not keys:
        return Unmeasured('the parent table holds no key')
    points = stated_points({'quantiles': quantiles})
    gaps = []
    for x in {point.value for point in points}:
        below, through = allowed(points, x)
        under = sum(values for rows, values in parents if rows < x) / keys
        upto = sum(values for rows, values in parents if rows <= x) / keys
        gaps += [apart(below, (under, under)), apart(through, (upto, upto))]
    return Compared(max(gaps))


def orphan_rate_delta(relationship: dict[str, Any], counts: Counts) -> Compared | Unmeasured:
    """The generated orphan rate's distance from the stated one, which is 0 where the relationship states none."""
    child_rows = sum(rows * values for _, rows, values in counts)
    expected = relationship.get('orphan_rate') or 0.0
    if not child_rows:
        return Unmeasured('no child row holds a key', expected)
    observed = sum(rows * values for held, rows, values in counts if not held) / child_rows
    return Compared(abs(observed - expected), expected, observed)


def top_share_delta(relationship: dict[str, Any], counts: Counts, scale: float) -> Compared | Unmeasured | None:
    """The stated top share the generated one is furthest from, and how far. None where the relationship states none.

    A scale keeps the share of the same fraction of the parents, so the top 10 of 40,000 parents are the top 5 of 20,000
    at scale 0.5. A share falling on a fraction of a parent takes that fraction of the next one's rows, and one falling on
    less than one parent is not compared.
    """
    shares = (relationship.get('fanout') or {}).get('top_share')
    if not shares:
        return None
    busiest = sorted(parents_of(counts), reverse=True)
    matched = sum(rows * values for rows, values in busiest)
    if not matched:
        return Unmeasured('no child row holds a key a parent holds')
    worst = None
    for name, share in shares.items():
        ranks = TOP_SHARES[name] * scale
        if ranks < 1:
            continue
        whole = math.floor(ranks)
        held = sum_of_busiest(busiest, whole) + (ranks - whole) * rank_value(busiest, whole)
        observed = held / matched
        if worst is None or abs(observed - share) > worst.value:
            worst = Compared(abs(observed - share), share, observed)
    return worst if worst is not None else Unmeasured(f'at scale {scale:g}, each stated top share falls on less than one parent')


def measured(relationship: dict[str, Any], counts: Counts, scale: float) -> dict[str, Compared | Unmeasured | None]:
    """Each relationship metric's value on one relationship, by name, at `scale` times its parent table's stated rows."""
    return {
        FANOUT_KS.name: fanout_ks(relationship, counts),
        ORPHAN_RATE_DELTA.name: orphan_rate_delta(relationship, counts),
        TOP_SHARE_DELTA.name: top_share_delta(relationship, counts, scale),
    }
