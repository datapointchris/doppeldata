"""The foreign keys between a bundle's tables: a column whose name refers to another table's key, kept where nearly every
value it holds is one of that key's. Each is counted over every row of both tables."""

import dataclasses as dc
import math
from typing import Any

from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import Table
from doppeldata.profiling.recorded_counts import RATE_DIGITS
from doppeldata.profiling.recorded_counts import rate
from doppeldata.profiling.recorded_counts import significant
from doppeldata.profiling.statistic_catalog import QUANTILES
from doppeldata.profiling.table_keys import KEY_KINDS
from doppeldata.profiling.table_keys import KEY_SHARE

# A reference is a foreign key where the parent holds at least this share of its distinct values.
CONTAINMENT_FLOOR = 0.95
# The busiest parents whose share of the child rows is stated, by the field naming it.
TOP_SHARES = {'top1': 1, 'top10': 10, 'top100': 100}
# The ranks a Zipf law's normalizing sums are added up over term by term, the rest taken by an integral.
SUMMED_RANKS = 1000
FIT_STEPS = 80
LARGEST_EXPONENT = 20.0


@dc.dataclass(frozen=True)
class BundleMember:
    """One table of a bundle: its name, its every row at the types its profile gives them, its columns, and its profile."""

    name: str
    table: Table
    specs: list[ColumnSpec]
    document: dict[str, Any]


@dc.dataclass(frozen=True)
class Reference:
    """A child column found to refer to a parent's key, and the relationship written for it."""

    child: BundleMember
    column: str
    parent: BundleMember
    key: str
    written: dict[str, Any]


def relationships(backend: Backend, members: list[BundleMember], k_min: int) -> list[dict[str, Any]]:
    """Every foreign key between the members, in the order of the tables and their columns."""
    found = []
    for child in members:
        for parent in members:
            if parent is child:
                continue
            kinds = {spec.name: spec.kind for spec in parent.specs}
            for key in single_keys(parent.document):
                for spec in child.specs:
                    if spec.kind not in KEY_KINDS or spec.kind is not kinds.get(key) or not refers_to(spec.name, parent.name, key):
                        continue
                    counts = backend.foreign_key_counts(child.table, spec.name, parent.table, key)
                    written = written_relationship(f'{child.name}.{spec.name}', f'{parent.name}.{key}', counts, k_min)
                    if written:
                        found.append(Reference(child, spec.name, parent, key, written))
    return [reference.written for reference in one_way(found, members)]


def one_way(found: list[Reference], members: list[BundleMember]) -> list[Reference]:
    """`found` with two keys that refer to each other kept in one direction: toward the table whose primary key it is
    where only one is, else toward the table it is named for, as `user_id` is for `users`, else toward the key holding
    more of the other's values, else toward the earlier table."""
    order = {member.name: index for index, member in enumerate(members)}
    by_ends = {(reference.child.name, reference.column, reference.parent.name, reference.key): reference for reference in found}

    def standing(reference: Reference) -> tuple[bool, bool, float, int]:
        toward_primary = (
            primary_key(reference.parent.document) == reference.key and primary_key(reference.child.document) != reference.column
        )
        named_for_parent = squeezed(reference.key) == singular(squeezed(reference.parent.name)) + 'id'
        return toward_primary, named_for_parent, reference.written['containment'], -order[reference.parent.name]

    def kept(reference: Reference) -> bool:
        mirror = by_ends.get((reference.parent.name, reference.key, reference.child.name, reference.column))
        return mirror is None or standing(reference) > standing(mirror)

    return [reference for reference in found if kept(reference)]


def primary_key(document: dict[str, Any]) -> str | None:
    """The column a table profile names as its primary key, where that is one column."""
    primary = ((document.get('dataset') or {}).get('keys') or {}).get('primary')
    return primary['columns'][0] if primary and len(primary['columns']) == 1 else None


def single_keys(document: dict[str, Any]) -> list[str]:
    """The columns a table profile names as single-column keys, the primary first."""
    keys = (document.get('dataset') or {}).get('keys') or {}
    every = [keys['primary']] if keys.get('primary') else []
    every += keys.get('candidates') or []
    return [key['columns'][0] for key in every if len(key['columns']) == 1 and (key.get('uniqueness') or 0) >= KEY_SHARE]


def squeezed(name: str) -> str:
    return ''.join(character for character in name.lower() if character.isalnum())


def singular(name: str) -> str:
    if name.endswith('ies'):
        return name[:-3] + 'y'
    return name[:-1] if name.endswith('s') and not name.endswith('ss') else name


def refers_to(column: str, parent_table: str, key: str) -> bool:
    """Whether a column's name refers to a parent's key: the key's own name other than a bare `id`, or the parent's
    singular name and the key's, as `customer_id` refers to `customers.id`. Case and separators are ignored."""
    written, keyed = squeezed(column), squeezed(key)
    if written == keyed and keyed != 'id':
        return True
    named = singular(squeezed(parent_table))
    return written in (named + keyed, named + 'id')


def written_relationship(child: str, parent: str, counts: list[tuple[bool, int, int]], k_min: int) -> dict[str, Any] | None:
    """The relationship the counts make, or None where the parent holds too few of the child's values.

    Orphan rows fewer than k_min are written as none, so neither rate names them.
    """
    child_rows = sum(rows * values for _, rows, values in counts)
    child_values = sum(values for _, rows, values in counts if rows)
    contained = sum(values for held, rows, values in counts if held and rows)
    orphan_rows = sum(rows * values for held, rows, values in counts if not held)
    if not child_values or contained / child_values < CONTAINMENT_FLOOR:
        return None
    floored = orphan_rows < k_min
    written: dict[str, Any] = {
        'from': child,
        'to': parent,
        'containment': 1.0 if floored else rate(contained, child_values),
        'orphan_rate': 0.0 if floored else rate(orphan_rows, child_rows),
        'cardinality': 'one_to_one' if all(rows <= 1 for _, rows, values in counts if values) else 'many_to_one',
    }
    fanout = fanout_of(sorted((rows, values) for held, rows, values in counts if held), k_min)
    if fanout:
        written['fanout'] = fanout
    return written


def fanout_of(parents: list[tuple[int, int]], k_min: int) -> dict[str, Any]:
    """The child rows of each parent value, as quantiles held k_min parents from each end, the busiest parents' shares of
    the child rows where each of them holds k_min, and a Zipf exponent. `parents` pairs child rows with the parents
    holding that many, rising."""
    held = sum(values for _, values in parents)
    matched = sum(rows * values for rows, values in parents)
    fanout: dict[str, Any] = {}
    quantiles = {}
    for name, level in QUANTILES.items():
        position = level * (held - 1)
        if position >= k_min - 1 and held - 1 - position >= k_min - 1:
            quantiles[name] = rank_value(parents, round(position))
    if quantiles:
        fanout['quantiles'] = quantiles
    busiest = sorted(parents, reverse=True)
    shares = {}
    for name, count in TOP_SHARES.items():
        if count <= held and matched and rank_value(busiest, count - 1) >= k_min:
            shares[name] = rate(sum_of_busiest(busiest, count), matched)
    if shares:
        fanout['top_share'] = shares
    exponent = zipf_exponent([(rows, values) for rows, values in busiest if rows])
    if exponent is not None:
        fanout['zipf_s'] = significant(exponent, RATE_DIGITS) + 0.0
    return fanout


def rank_value(runs: list[tuple[int, int]], index: int) -> int:
    """The value at an index of the values `runs` holds, each run a value and how many times it is held, in order."""
    for value, times in runs:
        if index < times:
            return value
        index -= times
    return runs[-1][0]


def sum_of_busiest(busiest: list[tuple[int, int]], count: int) -> int:
    total = 0
    for rows, values in busiest:
        taken = min(values, count)
        total += rows * taken
        count -= taken
        if not count:
            break
    return total


def zipf_exponent(busiest: list[tuple[int, int]]) -> float | None:
    """The exponent s maximizing the likelihood of the child rows under a Zipf law over the parents ranked busiest first,
    where a row falls on rank r in proportion to r^-s. `busiest` pairs child rows with the parents holding that many,
    falling. None where fewer than two parents hold a row."""
    parents = sum(values for _, values in busiest)
    rows = sum(count * values for count, values in busiest)
    if parents < 2:
        return None
    # Each run of parents holding as many rows fills ranks a to a + n - 1, whose logs sum to lgamma(a + n) - lgamma(a).
    weighted_log, first = 0.0, 1
    for count, values in busiest:
        weighted_log += count * (math.lgamma(first + values) - math.lgamma(first))
        first += values
    target = weighted_log / rows

    def mean_log_rank(s: float) -> float:
        total, logged = zipf_sums(s, parents)
        return logged / total

    # The mean log rank a law gives falls as s rises, from the uniform law's at 0.
    low, high = 0.0, LARGEST_EXPONENT
    if mean_log_rank(low) <= target:
        return 0.0
    for _ in range(FIT_STEPS):
        middle = (low + high) / 2
        if mean_log_rank(middle) > target:
            low = middle
        else:
            high = middle
    return (low + high) / 2


def zipf_sums(s: float, ranks: int) -> tuple[float, float]:
    """The sums over ranks 1 to `ranks` of r^-s and of r^-s ln r: the first `SUMMED_RANKS` term by term, and the rest by
    the integral over the ranks' midpoints."""
    summed = min(ranks, SUMMED_RANKS)
    total = sum(rank**-s for rank in range(1, summed + 1))
    logged = sum(rank**-s * math.log(rank) for rank in range(2, summed + 1))
    if ranks > summed:
        low, high = summed + 0.5, ranks + 0.5
        if abs(1 - s) < 1e-9:
            total += math.log(high) - math.log(low)
            logged += (math.log(high) ** 2 - math.log(low) ** 2) / 2
        else:
            power = 1 - s

            def antiderivative(x: float) -> float:
                return x**power * (math.log(x) / power - 1 / power**2)

            total += (high**power - low**power) / power
            logged += antiderivative(high) - antiderivative(low)
    return total, logged
