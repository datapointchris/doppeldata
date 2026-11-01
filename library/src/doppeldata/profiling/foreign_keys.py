"""The foreign keys between a bundle's tables: columns whose names refer to another table's key of one or two columns, kept
where nearly every value they hold is one of that key's, the columns a child copies from its parent row, and the column
numbering each parent's child rows. Each is counted over every row of both tables."""

import dataclasses as dc
import itertools
import math
import re
from typing import Any

from doppeldata import _core
from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import Table
from doppeldata.profiling.recorded_counts import RATE_DIGITS
from doppeldata.profiling.recorded_counts import rate
from doppeldata.profiling.recorded_counts import significant
from doppeldata.profiling.statistic_catalog import QUANTILES
from doppeldata.profiling.statistic_catalog import ColumnKind
from doppeldata.profiling.table_keys import KEY_KINDS
from doppeldata.profiling.table_keys import KEY_SHARE

# A reference is a foreign key where the parent holds at least this share of its distinct values.
CONTAINMENT_FLOOR = 0.95
# A child column is inherited where at least this share of its joined rows equal their parent row's column.
AGREEMENT_FLOOR = 0.99
# The longest table alias a column's name begins with, as `ps_` begins every column of TPC-H's `partsupp`.
LONGEST_ALIAS = 3
# A key's squeezed name that names a thing and its id or key, which a column may name after a role, as `PULocationID`
# names `LocationID` as the pickup's.
ROLED_KEY = re.compile(r'[a-z]{2,}(?:id|key)')
# The busiest parents whose share of the child rows is stated, by the field naming it, as the engine pins them.
TOP_SHARES = dict(_core.top_share_ranks())
# The ranks a Zipf law's normalizing sums are added up over term by term, the rest taken by an integral.
SUMMED_RANKS = 1000
FIT_STEPS = 80
LARGEST_EXPONENT = 20.0
# The relative difference between a fan-out's mean log rank and the uniform law's within which the two are one.
EVEN_TOLERANCE = 1e-9


@dc.dataclass(frozen=True)
class BundleMember:
    """One table of a bundle: its name, its every row at the types its profile gives them, its columns, and its profile."""

    name: str
    table: Table
    specs: list[ColumnSpec]
    document: dict[str, Any]


@dc.dataclass(frozen=True)
class Reference:
    """Child columns found to refer to a parent's key, the relationship written for them, and the child rows of the
    last parent its busiest stated top share counts, None where it states none."""

    child: BundleMember
    columns: tuple[str, ...]
    parent: BundleMember
    keys: tuple[str, ...]
    written: dict[str, Any]
    top_share_rows: int | None


@dc.dataclass(frozen=True)
class FoundRelationships:
    """Every foreign key found between a bundle's tables as the bundle writes it, and by its index, the fewest child rows
    any parent its top shares count holds, where it states one."""

    written: list[dict[str, Any]] = dc.field(default_factory=list)
    top_share_floors: dict[int, int] = dc.field(default_factory=dict)


def relationships(backend: Backend, members: list[BundleMember], k_min: int) -> FoundRelationships:
    """Every foreign key between the members, in the order of the tables and their columns, each with the columns its
    child copies from the parent row."""
    found = []
    for child in members:
        for parent in members:
            if parent is child:
                continue
            for keys in parent_keys(parent.document):
                for columns in referring_columns(child, parent, keys):
                    measured = measured_relationship(backend, child, columns, parent, keys, k_min, contained_only=True)
                    if measured:
                        found.append(Reference(child, columns, parent, keys, *measured))
    kept = one_way(found, members)
    drawn = {(reference.child.name, column) for reference in kept for column in reference.columns}
    for reference in kept:
        inherited = inherited_columns(backend, reference.child, reference.columns, reference.parent, reference.keys, k_min)
        if inherited:
            reference.written['inherited'] = inherited
        ordinal = ordinal_column(backend, reference.child, reference.columns, drawn)
        if ordinal:
            reference.written['ordinal'] = ordinal
    floors = {index: reference.top_share_rows for index, reference in enumerate(kept) if reference.top_share_rows is not None}
    return FoundRelationships([reference.written for reference in kept], floors)


def referring_columns(child: BundleMember, parent: BundleMember, keys: tuple[str, ...]) -> list[tuple[str, ...]]:
    """Each choice of the child's columns, one for each key column, whose names refer to it at its kind."""
    kinds = {spec.name: spec.kind for spec in parent.specs}
    choices = [
        [
            spec.name
            for spec in child.specs
            if spec.kind in KEY_KINDS and spec.kind is kinds.get(key) and refers_to(spec.name, child.name, parent.name, key)
        ]
        for key in keys
    ]
    return [columns for columns in itertools.product(*choices) if len(set(columns)) == len(columns)]


def measured_relationship(
    backend: Backend,
    child: BundleMember,
    columns: tuple[str, ...],
    parent: BundleMember,
    keys: tuple[str, ...],
    k_min: int,
    *,
    contained_only: bool,
) -> tuple[dict[str, Any], int | None] | None:
    """The relationship of `columns` to the parent's `keys`, counted over every row of both, and the child rows of the
    last parent its busiest stated top share counts. Where `contained_only`, None where the parent holds under
    `CONTAINMENT_FLOOR` of the child's values."""
    counts = backend.foreign_key_counts(child.table, list(columns), parent.table, list(keys))
    child_refs = [f'{child.name}.{column}' for column in columns]
    parent_refs = [f'{parent.name}.{key}' for key in keys]
    return written_relationship(child_refs, parent_refs, counts, k_min, contained_only=contained_only)


def inherited_columns(
    backend: Backend, child: BundleMember, columns: tuple[str, ...], parent: BundleMember, keys: tuple[str, ...], k_min: int
) -> list[dict[str, Any]]:
    """The child's columns equal to a column of the parent row they join on at least `AGREEMENT_FLOOR` of the joined rows,
    each with the parent column it agrees with most. A pair is of one kind, neither column is a key the join is taken on,
    and the parent column holds more than one value. Fewer than k_min disagreeing rows are written as none."""
    distinct = {column['name']: (column.get('distinct') or {}).get('approx_values') or 0 for column in parent.document.get('columns') or []}
    candidates = [spec for spec in parent.specs if spec.name not in keys and distinct.get(spec.name, 0) >= 2]
    pairs = [(spec.name, held.name) for spec in child.specs if spec.name not in columns for held in candidates if held.kind is spec.kind]
    if not pairs:
        return []
    joined, equal = backend.inherited_counts(child.table, list(columns), parent.table, list(keys), pairs)
    if not joined:
        return []
    best: dict[str, tuple[int, str]] = {}
    for (column, held), agreeing in zip(pairs, equal, strict=True):
        if agreeing >= AGREEMENT_FLOOR * joined and agreeing > best.get(column, (-1, ''))[0]:
            best[column] = (agreeing, held)
    return [
        {'child': column, 'parent': held, 'agreement_fraction': 1.0 if joined - agreeing < k_min else rate(agreeing, joined)}
        for column, (agreeing, held) in best.items()
    ]


def ordinal_column(backend: Backend, child: BundleMember, columns: tuple[str, ...], drawn: set[tuple[str, str]]) -> str | None:
    """The child's integer column numbering every key's rows from 1 to their count, the first in table order where several
    do. A candidate's profile gives its least value as 1 and two or more distinct values, and no relationship draws it."""
    profiled = {column['name']: column for column in child.document.get('columns') or []}

    def eligible(spec: ColumnSpec) -> bool:
        profile = profiled.get(spec.name) or {}
        low = ((profile.get('numeric') or {}).get('extremes') or {}).get('low')
        distinct = (profile.get('distinct') or {}).get('approx_values') or 0
        return spec.kind is ColumnKind.INTEGER and (child.name, spec.name) not in drawn and low == 1 and distinct >= 2

    candidates = [spec.name for spec in child.specs if spec.name not in columns and eligible(spec)]
    if not candidates:
        return None
    keys, numbered = backend.ordinal_counts(child.table, list(columns), candidates)
    return next((name for name, count in zip(candidates, numbered, strict=True) if keys and count == keys), None)


def one_way(found: list[Reference], members: list[BundleMember]) -> list[Reference]:
    """`found` with two keys that refer to each other kept in one direction: toward the table whose primary key it is
    where only one is, else toward the table it is named for, as `user_id` is for `users`, else toward the key holding
    more of the other's values, else toward the earlier table."""
    order = {member.name: index for index, member in enumerate(members)}
    by_ends = {(reference.child.name, reference.columns, reference.parent.name, reference.keys): reference for reference in found}

    def standing(reference: Reference) -> tuple[bool, bool, float, int]:
        toward_primary = (
            primary_key(reference.parent.document) == reference.keys and primary_key(reference.child.document) != reference.columns
        )
        named_for_parent = squeezed(''.join(reference.keys)) == singular(squeezed(reference.parent.name)) + 'id'
        return toward_primary, named_for_parent, reference.written['containment'], -order[reference.parent.name]

    def kept(reference: Reference) -> bool:
        mirror = by_ends.get((reference.parent.name, reference.keys, reference.child.name, reference.columns))
        return mirror is None or standing(reference) > standing(mirror)

    return [reference for reference in found if kept(reference)]


def primary_key(document: dict[str, Any]) -> tuple[str, ...] | None:
    """The columns a table profile names as its primary key."""
    primary = ((document.get('dataset') or {}).get('keys') or {}).get('primary')
    return tuple(primary['columns']) if primary else None


def parent_keys(document: dict[str, Any]) -> list[tuple[str, ...]]:
    """The keys of one or two columns a table profile names, the primary first."""
    keys = (document.get('dataset') or {}).get('keys') or {}
    every = [keys['primary']] if keys.get('primary') else []
    every += keys.get('candidates') or []
    return [tuple(key['columns']) for key in every if len(key['columns']) <= 2 and (key.get('uniqueness') or 0) >= KEY_SHARE]


def squeezed(name: str) -> str:
    return ''.join(character for character in name.lower() if character.isalnum())


def singular(name: str) -> str:
    if name.endswith('ies'):
        return name[:-3] + 'y'
    return name[:-1] if name.endswith('s') and not name.endswith('ss') else name


def unaliased(column: str, table: str) -> str:
    """`column` without the table alias its name begins with: up to `LONGEST_ALIAS` letters and a `_`, the first the table's
    first letter and each in order in its name, as `ps_partkey` is of `partsupp` and `l_orderkey` of `lineitem`."""
    alias, separator, rest = column.partition('_')
    lowered, named = alias.lower(), table.lower()
    if not separator or not rest or not alias.isalpha() or len(alias) > LONGEST_ALIAS or not named.startswith(lowered[0]):
        return column
    letters = iter(named)
    return rest if all(letter in letters for letter in lowered) else column


def refers_to(column: str, child_table: str, parent_table: str, key: str) -> bool:
    """Whether a column's name refers to a parent's key: the key's own name other than a bare `id`, the parent's
    singular name and the key's, as `customer_id` refers to `customers.id`, or a role and a key naming a thing and its
    id, as `PULocationID` refers to `LocationID`. Case, separators and each table's alias are ignored, so `o_custkey` of
    `orders` refers to `c_custkey` of `customer`."""
    for written, keyed in ((column, key), (unaliased(column, child_table), unaliased(key, parent_table))):
        written, keyed = squeezed(written), squeezed(keyed)
        if written == keyed and keyed != 'id':
            return True
        named = singular(squeezed(parent_table))
        if written in (named + keyed, named + 'id'):
            return True
        if written.endswith(keyed) and ROLED_KEY.fullmatch(keyed):
            return True
    return False


def written_relationship(
    child: list[str], parent: list[str], counts: list[tuple[bool, int, int]], k_min: int, *, contained_only: bool = True
) -> tuple[dict[str, Any], int | None] | None:
    """The relationship the counts make, and the child rows of the last parent its busiest stated top share counts, or
    where `contained_only`, None where the parent holds too few of the child's values.

    Orphan rows fewer than k_min are written as none, so neither rate names them.
    """
    child_rows = sum(rows * values for _, rows, values in counts)
    child_values = sum(values for _, rows, values in counts if rows)
    contained = sum(values for held, rows, values in counts if held and rows)
    orphan_rows = sum(rows * values for held, rows, values in counts if not held)
    if contained_only and (not child_values or contained / child_values < CONTAINMENT_FLOOR):
        return None
    floored = orphan_rows < k_min
    written: dict[str, Any] = {
        'from': child,
        'to': parent,
        'containment': 1.0 if floored else rate(contained, child_values),
        'orphan_fraction': 0.0 if floored else rate(orphan_rows, child_rows),
        'cardinality': 'one_to_one' if all(rows <= 1 for _, rows, values in counts if values) else 'many_to_one',
    }
    fanout, top_share_rows = fanout_of(sorted((rows, values) for held, rows, values in counts if held), k_min)
    if fanout:
        written['fanout'] = fanout
    return written, top_share_rows


def fanout_of(parents: list[tuple[int, int]], k_min: int) -> tuple[dict[str, Any], int | None]:
    """The child rows of each parent value, as quantiles held k_min parents from each end, the busiest parents' shares of
    the child rows where each of them holds k_min, and a Zipf exponent. Beside it, the child rows of the last parent the
    busiest stated share counts, None where none is stated. `parents` pairs child rows with the parents holding that
    many, rising."""
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
    fewest = None
    for name, count in TOP_SHARES.items():
        last = rank_value(busiest, count - 1) if count <= held and matched else 0
        if last >= k_min:
            shares[name] = rate(sum_of_busiest(busiest, count), matched)
            fewest = last if fewest is None else min(fewest, last)
    if shares:
        fanout['top_fractions'] = shares
    exponent = zipf_exponent([(rows, values) for rows, values in busiest if rows])
    if exponent is not None:
        fanout['zipf_s'] = significant(exponent, RATE_DIGITS) + 0.0
    return fanout, fewest


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

    # The mean log rank a law gives falls as s rises, from the uniform law's at 0. An even fan-out's target is the uniform
    # law's, computed by another formula, so rounding alone must not move it off 0.
    low, high = 0.0, LARGEST_EXPONENT
    if mean_log_rank(low) <= target * (1 + EVEN_TOLERANCE):
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
