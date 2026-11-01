"""What a profile states as a contract: each fact an export carries, and how much of the data it holds for.

A seed declares its facts, so each holds for every row. A profile made from data states its extremes and lengths trimmed
at `k_min` rows from each end, and its listed values, pattern and format as shares of the rows. A strict format, such as
a Table Schema, carries a fact only where it holds for every row, and names each fact it leaves out. DQDL and ODCS
quality checks carry a share as a floor `STANDARD_ERRORS` standard errors below it, at the rows it was measured over.
"""

import dataclasses as dc
import json
import math
import re
from collections.abc import Iterator
from collections.abc import Mapping
from typing import Any

from doppeldata import _core
from doppeldata.bundle_generation import bundle_arguments
from doppeldata.privacy_policy import DEFAULT_K_MIN
from doppeldata.profile_exports.type_names import SEMANTIC_FORMATS
from doppeldata.profile_exports.type_names import TYPE_MIX_JSON_TYPES
from doppeldata.profile_exports.type_names import decimal_of
from doppeldata.profile_exports.type_names import timestamp_of
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import Profile
from doppeldata.profile_format import StatedRelationship
from doppeldata.profile_format import UngenerableProfile
from doppeldata.profile_format import described_rows
from doppeldata.profile_format import engine_plan
from doppeldata.profile_format import listed_section
from doppeldata.profiling.recorded_counts import RATE_DIGITS
from doppeldata.profiling.table_profile import MOMENT_DIGITS
from doppeldata.seed_profiles.declared_columns import LIST_LENGTH_LEVELS

# A tolerance is this many standard errors of the measurement.
STANDARD_ERRORS = 3
# The places a share's floor or ceiling is written to, rounded away from the share so the profiled data still meets it.
SHARE_PLACES = 6
TEXT_SOURCES = ('csv', 'tsv', 'json', 'ndjson')
JSON_SOURCES = ('json', 'ndjson')
# The character standing for each class of character in a shape, which a stored pattern is tried against.
SHAPE_CHARACTERS = {'9': '0', 'A': 'A', 'a': 'a'}
# The `generated.declared` facts a seed holds only where its schema declared them.
UNDEFAULTED_FACTS = frozenset({'top_values', 'pattern'})


def half_unit(value: float, digits: int) -> float:
    """Half the last place of `value` written to `digits` significant digits: how far the written value may be off."""
    if not value or not math.isfinite(value):
        return 0.0
    return 0.5 * 10.0 ** (math.floor(math.log10(abs(value))) - digits + 1)


def floored(value: float) -> float:
    return math.floor(value * 10**SHARE_PLACES) / 10**SHARE_PLACES


def ceiled(value: float) -> float:
    return math.ceil(value * 10**SHARE_PLACES) / 10**SHARE_PLACES


@dc.dataclass(frozen=True)
class Share:
    """A share of rows that a fact holds for, measured over `rows`, or declared where `rows` is None. `rounding` is how
    far the profile's written figures may be off, which widens the tolerance. `short_of` says which rows the share does
    not count, where a share of 1 still leaves some unstated: a sample's, or those the privacy floor writes as none."""

    value: float
    rows: int | None
    rounding: float = 0.0
    short_of: str | None = None

    @property
    def whole(self) -> bool:
        return self.value >= 1.0 and self.short_of is None

    def tolerance(self) -> float:
        """`STANDARD_ERRORS` standard errors of a proportion at `rows`, at least one row's worth, and nothing for a declared
        share."""
        if not self.rows:
            return 0.0
        share = min(max(self.value, 0.0), 1.0)
        return max(STANDARD_ERRORS * math.sqrt(share * (1 - share) / self.rows), 1 / self.rows) + self.rounding

    def least(self) -> float:
        """The least share data like the profiled data holds."""
        return floored(max(0.0, self.value - self.tolerance()))

    def most(self) -> float:
        """The greatest share data like the profiled data holds."""
        return ceiled(min(1.0, self.value + self.tolerance()))

    def of(self, other: 'Share') -> 'Share':
        """The share holding both this and `other`, `other` measured over the rows this one holds for."""
        return Share(self.value * other.value, self.rows, self.rounding + other.rounding, self.short_of or other.short_of)

    def caveat(self) -> str:
        """`short_of` as the end of a note naming this share, or nothing."""
        return f', {self.short_of}' if self.short_of else ''


@dc.dataclass(frozen=True)
class Moment:
    """A column's mean or standard deviation, and how far it may move: `STANDARD_ERRORS` standard errors and its written
    rounding."""

    value: float
    margin: float

    def bounds(self) -> tuple[float, float]:
        return self.value - self.margin, self.value + self.margin


@dc.dataclass(frozen=True)
class ContractColumn:
    """One column's contract facts. `low` and `high` are the extremes the profile states, and `exact` says they and the
    lengths hold for every row, as a seed's and an exact measurement of every row do. `inexact` says why they do not.
    `values` are the listed values, `values_share` the share of rows
    holding one of them. `pattern_share` and `format_share` are the shares of rows matching the pattern and the format.
    Each share is of every row, a null counting against it, so `completeness` is the share holding a value.

    `parsed_type` is the profile's logical type where the engine generates the column as text, as it does where a text
    source holds values that do not parse as that type. A reader of the type turns them into nulls, so the column is
    typed as a string, and `raw_type_share` is the share that parse.
    """

    name: str
    path: str | None
    logical_type: str
    physical_type: str
    nullable: bool
    completeness: Share
    exact: bool
    inexact: str = 'trimmed at k_min rows from each end'
    identifier: bool = False
    sensitivity: str | None = None
    semantic_type: str | None = None
    low: Any = None
    high: Any = None
    trimmed_share: Share | None = None
    places: int | None = None
    min_length: int | None = None
    max_length: int | None = None
    values: tuple[Any, ...] = ()
    values_share: Share | None = None
    pattern: str | None = None
    pattern_share: Share | None = None
    format: str | None = None
    format_share: Share | None = None
    mean: Moment | None = None
    std: Moment | None = None
    type_mix: tuple[tuple[str, float], ...] = ()
    raw_type_share: Share | None = None
    parsed_type: str | None = None
    missing_tokens: tuple[str, ...] = ()
    strftime: str | None = None
    presence: float | None = None
    fields: tuple['ContractColumn', ...] = ()
    element: 'ContractColumn | None' = None
    min_items: int | None = None
    max_items: int | None = None

    @property
    def required(self) -> bool:
        """Every row holds a value: no nulls, and as a struct's field, present in every row."""
        return self.completeness.whole and self.presence in (None, 1.0)

    @property
    def timezone(self) -> str | None:
        timestamp = timestamp_of(self.physical_type)
        return timestamp[1] if timestamp else None

    @property
    def decimal(self) -> tuple[int, int] | None:
        return decimal_of(self.physical_type)

    def held_range(self) -> tuple[Any, Any] | None:
        """The extremes, where they bound every value."""
        return (self.low, self.high) if self.exact and (self.low is not None or self.high is not None) else None

    def held_lengths(self) -> tuple[int | None, int | None] | None:
        """The least and greatest lengths, where they bound every value."""
        return (self.min_length, self.max_length) if self.exact and (self.min_length or self.max_length) else None

    def held_values(self) -> tuple[Any, ...]:
        """The listed values, where every value is one of them."""
        return self.values if self.values and self.values_share is not None and self.listed_share().whole else ()

    def listed_share(self) -> Share:
        """The share of the rows holding a value that hold a listed one."""
        assert self.values_share is not None
        return within_values(self.values_share, self.completeness)

    def held_pattern(self) -> str | None:
        """The pattern, where every value matches it."""
        whole = self.pattern_share is not None and within_values(self.pattern_share, self.completeness).whole
        return self.pattern if self.pattern and whole else None

    def held_format(self) -> str | None:
        """The format, where every value is of it."""
        whole = self.format_share is not None and within_values(self.format_share, self.completeness).whole
        return self.format if self.format and whole else None

    @property
    def label(self) -> str:
        """The column's dotted path where it is nested, else its name."""
        return self.path or self.name

    def nested(self) -> tuple['ContractColumn', ...]:
        return (*self.fields, *([self.element] if self.element else []))

    def holds_constraint(self) -> bool:
        """Whether a strict format would check anything of this column beyond its type and nulls."""
        return bool(self.held_range() or self.held_lengths() or self.held_values() or self.held_pattern() or self.held_format())

    def left_out(self) -> Iterator['LeftOut']:
        """What a strict format leaves out of this column and every field nested in it, and why."""
        if self.parsed_type and self.raw_type_share is not None:
            parsed = within_values(self.raw_type_share, self.completeness)
            # The record schema allows each JSON type a column's values hold, so it leaves out no 'json_types' fact.
            fact = 'json_types' if len(self.type_mix) > 1 else 'type'
            parse = f'{percent(parsed.value)} of values parse as{parsed.caveat()}'
            yield LeftOut(fact, f'{self.label}: the {self.parsed_type} type, which {parse}')
        if not self.identifier and (self.low is not None or self.high is not None) and not self.exact:
            yield LeftOut('range', f'{self.label}: the range {self.low} to {self.high}, {self.inexact}')
        if (self.min_length or self.max_length) and not self.exact:
            yield LeftOut('lengths', f'{self.label}: the lengths {self.min_length} to {self.max_length}, {self.inexact}')
        if self.values and not self.held_values():
            listed = self.listed_share()
            outside = f'{percent(1 - listed.value)} of values fall outside{listed.caveat()}'
            yield LeftOut('values', f'{self.label}: the {len(self.values)} listed values, which {outside}')
        if self.pattern and not self.held_pattern():
            matched = within_values(self.pattern_share, self.completeness) if self.pattern_share else Share(0.0, None)
            yield LeftOut(
                'pattern', f'{self.label}: the pattern {self.pattern}, which {percent(matched.value)} of values match{matched.caveat()}'
            )
        if self.format and not self.held_format():
            if self.format_share:
                formatted = within_values(self.format_share, self.completeness)
                failed = f'{percent(1 - formatted.value)} of values fail{formatted.caveat()}'
            else:
                failed = 'an unstated share of values fail'
            yield LeftOut('format', f'{self.label}: the {self.format} format, which {failed}')
        for inner in self.nested():
            yield from inner.left_out()


@dc.dataclass(frozen=True)
class LeftOut:
    """A fact an export leaves out, `fact` naming its kind, such as `range`, and `note` the column, the fact and why."""

    fact: str
    note: str


def within_values(share: Share, completeness: Share) -> Share:
    """A share of every row, as a share of the rows holding a value, measured over those rows."""
    held_rows = None if share.rows is None else round(share.rows * completeness.value)
    if completeness.value <= 0:
        return Share(0.0, held_rows, short_of=share.short_of)
    return Share(min(1.0, share.value / completeness.value), held_rows, share.rounding, share.short_of)


def percent(share: float) -> str:
    return f'{share * 100:.4g}%'


@dc.dataclass(frozen=True)
class Key:
    """Columns whose values no two rows share, in `uniqueness` of the rows. `whole` says they hold for every row, as the
    engine reads a key it refuses to draw a value of twice."""

    columns: tuple[str, ...]
    uniqueness: Share
    whole: bool


@dc.dataclass(frozen=True)
class ForeignKey:
    """`columns` of this table refer to `to_columns` of `to_table`, held in `share` of the rows holding a value."""

    columns: tuple[str, ...]
    to_table: str
    to_columns: tuple[str, ...]
    share: Share | None

    @property
    def named(self) -> str:
        return ', '.join(self.columns)

    @property
    def target(self) -> str:
        """The columns referred to, as `customers.customer_id`, or `partsupp.(ps_partkey, ps_suppkey)` for several."""
        listed = self.to_columns[0] if len(self.to_columns) == 1 else f'({", ".join(self.to_columns)})'
        return f'{self.to_table}.{listed}'


@dc.dataclass(frozen=True)
class ContractTable:
    """One table's contract: its columns, keys and partitions, and what identifies the profile it came from."""

    name: str
    columns: tuple[ContractColumn, ...]
    seed: bool
    rows: int | None
    k_min: int | None
    source_format: str | None
    csv_null: str | None
    profile_sha256: str
    primary_key: Key | None = None
    unique_keys: tuple[Key, ...] = ()
    partition_keys: tuple[str, ...] = ()
    foreign_keys: tuple[ForeignKey, ...] = ()
    profile: Profile | None = None
    sample: str | None = None
    floor: int | None = None

    @property
    def text_source(self) -> bool:
        return self.source_format in TEXT_SOURCES

    def column(self, name: str) -> ContractColumn | None:
        return next((column for column in self.columns if column.name == name), None)

    def unique_columns(self) -> set[str]:
        """Each column a whole key of one column makes unique."""
        keys = [self.primary_key, *self.unique_keys] if self.primary_key else list(self.unique_keys)
        return {key.columns[0] for key in keys if len(key.columns) == 1 and key.whole}

    def stated_keys(self) -> list[Key]:
        return [self.primary_key, *self.unique_keys] if self.primary_key else list(self.unique_keys)

    def left_out(self) -> list[LeftOut]:
        """What a strict format leaves out of this table, and why."""
        notes = [note for column in self.columns for note in column.left_out()]
        for key in self.stated_keys():
            if not key.whole:
                unique = f'{percent(key.uniqueness.value)} of rows hold uniquely{key.uniqueness.caveat()}'
                notes.append(LeftOut('key', f'{", ".join(key.columns)}: the key, which {unique}'))
        for reference in self.foreign_keys:
            if reference.share is None or not reference.share.whole:
                share = reference.share
                held = f'{percent(share.value)} of values hold{share.caveat()}' if share else 'an unstated share of values hold'
                notes.append(LeftOut('foreign_key', f'{reference.named}: the foreign key to {reference.target}, which {held}'))
        return notes


@dc.dataclass(frozen=True)
class ContractDocument:
    """What one export holds: a table's contract, or each of a bundle's, under the name and SHA-256 of the profile file."""

    name: str
    profile_sha256: str
    tables: tuple[ContractTable, ...]
    bundle: bool


def contract_document(profile: Profile | Bundle, type_mix: frozenset[str] | None = None) -> ContractDocument:
    """The contract of a table profile, or of every table of a bundle, with the foreign keys its relationships state.
    `type_mix` holds the pointers of the columns a table profile generates as text where its bundle's plan decides
    them."""
    plain = profile.to_plain()
    if isinstance(profile, Profile):
        pointers = type_mix if type_mix is not None else type_mix_pointers(profile)
        return ContractDocument(str(plain['name']), profile.sha256, (contract_table(profile, pointers),), False)
    relationships = profile.relationships()
    planned = bundle_type_mix_pointers(profile)
    tables = []
    for name, table in profile.tables.items():
        found = contract_table(table, None if planned is None else planned[name])
        keys = tuple(foreign_key(relationship, found) for relationship in relationships if relationship.child == name)
        tables.append(dc.replace(found, foreign_keys=keys))
    return ContractDocument(str(plain['name']), profile.sha256, tuple(tables), True)


def type_mix_pointers(profile: Profile) -> frozenset[str] | None:
    """The pointer of each column the engine generates as text at the profile's row count, or None where the engine
    refuses the profile, which then generates nothing."""
    document = profile.to_plain()
    rows = (document.get('dataset') or {}).get('rows') or 1
    try:
        plan = engine_plan(document, str(profile.path or 'the exported profile'), rows)
    except UngenerableProfile:
        return None
    return frozenset(pointer for pointer, _ in plan.type_mix_pointers())


def bundle_type_mix_pointers(bundle: Bundle) -> dict[str, frozenset[str]] | None:
    """Each table's pointers of the columns the engine generates as text at the bundle's own scale, which a parent's
    text decides for a key referring to it, or None where the engine refuses the bundle, which then generates
    nothing."""
    try:
        plan = bundle_arguments(bundle, 0, 1.0, {}).plan()
    except UngenerableProfile:
        return None
    return {name: frozenset(pointer for pointer, _ in table.type_mix_pointers()) for name, table in plan.tables()}


def contract_table(profile: Profile, type_mix: frozenset[str] | None) -> ContractTable:
    document = profile.to_plain()
    generated = document.get('generated') or {}
    seed = generated.get('engine') == 'import'
    rows = None if seed else described_rows(document)
    privacy = document.get('privacy') or {}
    identifiers = {entry['column'] for entry in privacy.get('suppressed') or [] if entry.get('reason') == 'identifier'}
    source = document.get('source') or {}
    k_min = privacy.get('k_min') or DEFAULT_K_MIN
    declared = generated.get('declared')
    sampled = not seed and (generated.get('sample') or {}).get('strategy', 'full') != 'full'
    facts = TableFacts(
        seed=seed,
        rows=rows,
        k_min=k_min,
        identifiers=identifiers,
        json_source=source.get('format') in JSON_SOURCES,
        text_source=source.get('format') in TEXT_SOURCES,
        declared=None if declared is None else {path: frozenset(listed) for path, listed in declared.items()},
        sample=f'measured over a sample of {rows:,} rows' if sampled and rows else None,
        floor=k_min if not seed and generated.get('engine') else None,
        type_mix_pointers=type_mix,
    )
    keys = (document.get('dataset') or {}).get('keys') or {}
    wholes = iter(_core.whole_keys(json.dumps(document, allow_nan=False)))
    primary_key = table_key(keys['primary'], facts, next(wholes)) if keys.get('primary') else None
    unique_keys = tuple(table_key(key, facts, next(wholes)) for key in keys.get('candidates') or [])
    partitioning = (source.get('layout') or {}).get('partitioning') or {}
    return ContractTable(
        name=str(document['name']),
        columns=tuple(contract_column(column, facts, f'/columns/{index}') for index, column in enumerate(document.get('columns') or [])),
        seed=seed,
        rows=rows,
        k_min=privacy.get('k_min'),
        source_format=source.get('format'),
        csv_null=((source.get('csv') or {}).get('writer') or {}).get('null'),
        profile_sha256=profile.sha256,
        primary_key=primary_key,
        unique_keys=unique_keys,
        partition_keys=tuple(key['name'] for key in partitioning.get('keys') or []),
        profile=profile,
        sample=facts.sample,
        floor=facts.floor,
    )


@dc.dataclass(frozen=True)
class TableFacts:
    """What a column's facts are read against: whether the table is a seed, the rows its shares are of, the columns the
    policy treats as identifiers, whether its source is JSON, whose raw types a record schema follows, whether it is
    text, whose raw values may not parse as a column's type, a seed's `generated.declared`, where the profile read a
    sample, the caveat each share of it carries, where the profiler wrote it, the k_min its privacy floor applied, and
    the pointer of each column the engine generates as text, None where it refuses to plan the table."""

    seed: bool
    rows: int | None
    k_min: int
    identifiers: set[str]
    json_source: bool
    text_source: bool = False
    declared: Mapping[str, frozenset[str]] | None = None
    sample: str | None = None
    floor: int | None = None
    type_mix_pointers: frozenset[str] | None = None

    def stated(self, path: str, fact: str) -> bool:
        """Whether the profile states `fact` of the column at `path`, rather than drawing a default for it. A profile
        made from data states every fact. A seed states what its schema declared, and one recording nothing declared
        states only its values and its pattern, which no default fills."""
        if not self.seed:
            return True
        if self.declared is None:
            return fact in UNDEFAULTED_FACTS
        return fact in self.declared.get(path, frozenset())


def floored_share(value: float, rows: int | None, rounding: float, floor: int | None, sample: str | None, breaking: str) -> Share:
    """A share the profiler writes as 1 where fewer than `floor`, its k_min, rows are `breaking` it, so a 1 it measured
    states none of those rows. A share no profiler wrote, with no `floor`, and a declared one, with no rows, are themselves."""
    if value < 1 or not rows or floor is None:
        return Share(value, rows, rounding, sample)
    unstated = f'short of the fewer than {floor} rows {breaking}, which the privacy floor writes as none'
    return Share(value, rows, rounding + (floor - 1) / rows, sample or unstated)


def table_key(key: Mapping[str, Any], facts: 'TableFacts', whole: bool) -> Key:
    uniqueness = key.get('uniqueness')
    if uniqueness is None:
        return Key(tuple(key['columns']), Share(1.0, None), whole)
    rounding = half_unit(1 - uniqueness, RATE_DIGITS)
    share = floored_share(uniqueness, facts.rows, rounding, facts.floor, facts.sample, 'sharing a value')
    return Key(tuple(key['columns']), share, whole)


def foreign_key(relationship: StatedRelationship, child: ContractTable) -> ForeignKey:
    """The relationship from columns of `child`. Its share is `1 − orphan_fraction`, at the rows holding a value in each.
    Without one, a seed's relationship is declared whole, and a profile's distinct-value `containment` states no share of
    rows."""
    columns = relationship.child_columns
    found = [child.column(column) for column in columns]
    orphans = relationship.document.get('orphan_fraction')
    if orphans is not None:
        held = None if child.rows is None or None in found else min(column.completeness.value for column in found if column)
        held_rows = None if held is None or child.rows is None else round(child.rows * held)
        rounding = half_unit(orphans, RATE_DIGITS)
        share: Share | None = floored_share(1 - orphans, held_rows, rounding, child.floor, child.sample, 'orphaned')
    else:
        share = Share(1.0, None) if child.seed else None
    return ForeignKey(columns, relationship.parent, relationship.parent_columns, share)


def contract_column(column: Mapping[str, Any], facts: TableFacts, pointer: str, presence: float | None = None) -> ContractColumn:
    """The contract facts of the profile column at `pointer`, or of a struct's field present in `presence` of its
    parent's rows."""
    name = str(column['name'])
    null_fraction = column.get('null_fraction') or 0.0
    rows = facts.rows
    completeness = Share(1 - null_fraction, rows, half_unit(null_fraction, RATE_DIGITS), facts.sample)
    semantic = column.get('semantic') or {}
    numeric = column.get('numeric') or {}
    temporal = column.get('temporal') or {}
    string = column.get('string') or {}
    extremes = numeric.get('extremes') or temporal.get('extremes') or {}
    identifier = name in facts.identifiers or semantic.get('sensitivity') == 'identifier' or extremes.get('method') == 'digit_bounds'
    measured_exactly = extremes.get('method') == 'exact'
    exact = facts.seed or (measured_exactly and facts.sample is None)
    inexact = facts.sample if measured_exactly and facts.sample else 'trimmed at k_min rows from each end'
    held_rows = None if rows is None else round(rows * (1 - null_fraction))
    path = str(column.get('path') or name)
    physical = str(column['physical_type'])

    def stated(fact: str) -> bool:
        return facts.stated(path, fact)

    found: dict[str, Any] = {}
    if not identifier and extremes:
        found |= {
            'low': extremes.get('low') if stated('low') else None,
            'high': extremes.get('high') if stated('high') else None,
            'trimmed_share': trimmed_share(completeness, held_rows, facts),
        }
    found['places'] = numeric.get('scale') if decimal_of(physical) or stated('scale') else None
    length = string.get('length') or {}
    found |= {
        'min_length': length.get('min') if stated('min_length') else None,
        'max_length': length.get('max') if stated('max_length') else None,
    }
    listed = listed_section(dict(column))
    if listed is not None and not identifier and stated('top_values'):
        _, section = listed
        other = section.get('other_fraction') or 0.0
        found['values'] = tuple(entry['value'] for entry in section['top_values'])
        found['values_share'] = completeness.of(Share(1 - other, held_rows, half_unit(other, RATE_DIGITS)))
    if string.get('pattern') and stated('pattern'):
        found['pattern'] = string['pattern']
        found['pattern_share'] = completeness.of(pattern_share(string['pattern'], string.get('shapes'), held_rows, facts.seed))
    semantic_format = SEMANTIC_FORMATS.get(semantic.get('type') or '')
    if semantic_format is not None and stated('format'):
        invalid = (column.get('quality') or {}).get('invalid_fraction')
        found['format'] = semantic_format
        if facts.seed:
            found['format_share'] = completeness
        elif invalid is not None:
            found['format_share'] = completeness.of(Share(1 - invalid, held_rows, half_unit(invalid, RATE_DIGITS)))
    if not identifier and held_rows:
        found |= moments(numeric, held_rows)
    type_mix = column.get('type_mix_fractions') or {}
    logical = str(column['logical_type'])
    if type_mix and facts.json_source:
        found['type_mix'] = json_type_mix(type_mix)
    parsed = None
    if type_mix and logical in RAW_TYPE_KEYS and RAW_TYPE_KEYS[logical] in type_mix:
        raw = type_mix[RAW_TYPE_KEYS[logical]]
        parsed = Share(raw, held_rows, half_unit(1 - raw, RATE_DIGITS), facts.sample)
        found['raw_type_share'] = completeness.of(parsed)
    if facts.type_mix_pointers is None:
        # The engine refuses the profile, so the contract holds only for the source, whose values a sample does not
        # prove all parse.
        as_text = facts.text_source and parsed is not None and not parsed.whole
    else:
        as_text = pointer in facts.type_mix_pointers
    if as_text:
        found['parsed_type'] = logical
        logical = physical = 'string'
        # The extremes were trimmed over the values that parse.
        if parsed is not None and found.get('trimmed_share') is not None:
            found['trimmed_share'] = found['trimmed_share'].of(parsed)
    struct = column.get('struct') or {}
    shares = struct.get('presence_fractions') or {}
    found['fields'] = tuple(
        contract_column(field, facts, f'{pointer}/struct/fields/{index}', shares.get(field['name']))
        for index, field in enumerate(struct.get('fields') or [])
    )
    listing = column.get('list') or {}
    if listing.get('element'):
        found['element'] = contract_column(listing['element'], dc.replace(facts, rows=None), f'{pointer}/list/element')
    if facts.seed and listing.get('length'):
        least, most = (listing['length'].get(level) for level in LIST_LENGTH_LEVELS)
        found |= {'min_items': least if stated('min_items') else None, 'max_items': most if stated('max_items') else None}
    return ContractColumn(
        name=name,
        path=column.get('path'),
        logical_type=logical,
        physical_type=physical,
        nullable=null_fraction > 0 or (facts.seed and bool(column.get('nullable'))),
        completeness=completeness,
        exact=exact,
        inexact=inexact,
        identifier=identifier,
        sensitivity=semantic.get('sensitivity'),
        semantic_type=semantic.get('type'),
        missing_tokens=tuple(column.get('missing_token_fractions') or {}),
        strftime=temporal.get('format'),
        presence=presence,
        **found,
    )


# The `type_mix` key a column of each logical type parses as, whose share DQDL's ColumnDataType checks of raw text.
RAW_TYPE_KEYS = {'integer': 'integer', 'number': 'decimal', 'boolean': 'boolean', 'date': 'date', 'timestamp': 'timestamp'}


def trimmed_share(completeness: Share, held_rows: int | None, facts: TableFacts) -> Share:
    """The share of rows inside trimmed extremes: all but the `k_min − 1` rows trimmed from each end of the values."""
    if facts.seed or not held_rows:
        return completeness
    inside = max(0.0, 1 - 2 * (facts.k_min - 1) / held_rows)
    return completeness.of(Share(inside, held_rows))


def pattern_share(pattern: str, shapes: Any, held_rows: int | None, seed: bool) -> Share:
    """The share of values matching `pattern`: a seed's are all drawn from it, and a profile's are those of the most common
    shape, which the profiler generalized it from, and of each other listed shape it matches. A shape too rare to list is
    counted against it."""
    if seed:
        return Share(1.0, None)
    try:
        compiled = re.compile(pattern)
    except re.error:
        return Share(0.0, held_rows)
    listed = shapes or []
    # A shape's letters stand for any letter, so only the shape it came from vouches for a pattern's literal letters.
    dominant = max(listed, key=lambda entry: entry['fraction'], default=None)
    matched = [entry['fraction'] for entry in listed if entry is dominant or compiled.fullmatch(shape_value(entry['shape']))]
    return Share(min(1.0, sum(matched)), held_rows, sum(half_unit(rate, RATE_DIGITS) for rate in matched))


def shape_value(shape: str) -> str:
    """A value of a shape: a digit for each 9, a letter for each A or a, and every other character as itself."""
    return ''.join(SHAPE_CHARACTERS.get(character, character) for character in shape)


def moments(numeric: Mapping[str, Any], rows: int) -> dict[str, Moment]:
    """The mean within `STANDARD_ERRORS` standard errors, σ/√n, and the standard deviation within as many of its own,
    σ/√(2(n − 1)), each widened by its written rounding."""
    mean, std = numeric.get('mean'), numeric.get('std')
    if not isinstance(mean, int | float) or not isinstance(std, int | float) or rows < 2:
        return {}
    return {
        'mean': Moment(mean, STANDARD_ERRORS * std / math.sqrt(rows) + half_unit(mean, MOMENT_DIGITS)),
        'std': Moment(std, STANDARD_ERRORS * std / math.sqrt(2 * (rows - 1)) + half_unit(std, MOMENT_DIGITS)),
    }


def json_type_mix(type_mix: Mapping[str, float]) -> tuple[tuple[str, float], ...]:
    """Each JSON type the raw values parse as, with its share, the largest first and ties by name."""
    shares: dict[str, float] = {}
    for key, share in type_mix.items():
        if share > 0 and key in TYPE_MIX_JSON_TYPES:
            json_type = TYPE_MIX_JSON_TYPES[key]
            shares[json_type] = shares.get(json_type, 0.0) + share
    return tuple(sorted(shares.items(), key=lambda item: (-item[1], item[0])))
