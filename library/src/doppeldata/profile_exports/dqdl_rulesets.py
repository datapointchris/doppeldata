"""An AWS Glue Data Quality ruleset, in DQDL, of each table.

A rule on a column's values runs `where` the column holds a value, so a null fails none of them, and its threshold is a
share of the values held. A fact the profile states of every value gets a rule with no threshold. A share gets one
`STANDARD_ERRORS` standard errors below it, at the rows it was measured over, and a range the privacy trim cut gets one
allowing the `k_min − 1` rows trimmed from each end. `between` excludes both ends, so a range is widened by one step of
its type.
A bundle's rulesets follow one another, each under a comment naming its table.
"""

import datetime as dt
import decimal
import math
from collections.abc import Iterator
from typing import Any

from doppeldata.profile_exports.contract_columns import STANDARD_ERRORS
from doppeldata.profile_exports.contract_columns import ContractColumn
from doppeldata.profile_exports.contract_columns import ContractDocument
from doppeldata.profile_exports.contract_columns import ContractTable
from doppeldata.profile_exports.contract_columns import Share
from doppeldata.profile_exports.contract_columns import within_values
from doppeldata.profile_exports.table_schemas import unanchored
from doppeldata.profiling.table_profile import MOMENT_DIGITS

# The DQDL type each logical type's raw text is checked as, by `ColumnDataType`, which casts the text to it. An integer
# is a LONG, as INTEGER refuses one past 32 bits.
DATA_TYPES = {'integer': 'LONG', 'number': 'DOUBLE', 'boolean': 'BOOLEAN', 'date': 'DATE', 'timestamp': 'TIMESTAMP'}
# A cast reads a date or a timestamp written only as ISO 8601 does.
ISO_DATE = '%Y-%m-%d'
RANGED_TYPES = ('integer', 'number', 'date', 'timestamp')
# Spark sums a 64-bit integer column without widening it, so a sum past this wraps and its mean is wrong.
LONG_LIMIT = 2**63


def dqdl_rulesets(document: ContractDocument) -> str:
    return '\n'.join(ruleset(table, document) for table in document.tables)


def ruleset(table: ContractTable, document: ContractDocument) -> str:
    notes: list[str] = []
    rules = [*table_rules(table, notes)]
    for column in table.columns:
        rules += column_rules(column, table, notes)
    lines = [f'# The ruleset of {table.name}, exported from its doppeldata profile {document.profile_sha256}.']
    if table.rows:
        lines.append(f'# Each threshold lies {STANDARD_ERRORS} standard errors below the share the profile states of {table.rows} rows.')
    else:
        lines.append('# A seed states no share of rows, so each rule holds for every row.')
    if notes:
        lines.append('# Left out:')
        lines.extend(f'#   {note}' for note in notes)
    if not rules:
        return '\n'.join([*lines, 'Rules = []', ''])
    return '\n'.join([*lines, 'Rules = [', ',\n'.join(f'    {rule}' for rule in rules), ']', ''])


def table_rules(table: ContractTable, notes: list[str]) -> Iterator[str]:
    """The table's keys and foreign keys."""
    primary = table.primary_key
    for key in table.stated_keys():
        named = ' '.join(quoted(column) for column in key.columns)
        if key.whole:
            yield f'IsPrimaryKey {named}' if key is primary else f'IsUnique {named}'
        elif key.uniqueness.rows:
            yield f'Uniqueness {named} >= {number(key.uniqueness.least())}'
    for reference in table.foreign_keys:
        held = [table.column(column) for column in reference.columns]
        found = [column for column in held if column is not None]
        # DQDL names a composite key's columns as one comma-joined string, and the reference's in braces.
        named = quoted(','.join(reference.columns))
        listed = reference.to_columns[0] if len(reference.to_columns) == 1 else '{' + ','.join(reference.to_columns) + '}'
        target = quoted(f'{reference.to_table}.{listed}')
        if reference.share is None or len(found) < len(held):
            notes.append(f'{reference.named}: the foreign key to {reference.target}, of no stated share')
        elif reference.share.whole and all(column.completeness.whole for column in found):
            yield f'ReferentialIntegrity {named} {target} = 1.0'
        elif len(found) == 1 and found[0].completeness.rows:
            yield f'ReferentialIntegrity {named} {target} >= {number(found[0].completeness.of(reference.share).least())}'
        else:
            notes.append(f'{reference.named}: the foreign key to {reference.target}, of columns of no stated null share together')


def column_rules(column: ContractColumn, table: ContractTable, notes: list[str]) -> list[str]:
    name = quoted(column.name)
    where = '' if column.completeness.whole else f' where {quoted(f"{sql_name(column.name)} is not null")}'
    rules = []
    if column.completeness.whole:
        rules.append(f'IsComplete {name}')
    elif column.completeness.rows:
        rules.append(f'Completeness {name} >= {number(column.completeness.least())}')
    if (column.low is not None or column.high is not None) and value_type(column) in RANGED_TYPES:
        inside = within_values(column.trimmed_share, column.completeness) if column.trimmed_share else None
        rules.append(f'ColumnValues {name} {range_condition(column)}{where}{threshold(None if column.exact else inside)}')
    if column.values and column.logical_type != 'boolean' and column.values_share is not None:
        listed = ', '.join(value_literal(value, column) for value in column.values)
        rules.append(f'ColumnValues {name} in [{listed}]{where}{threshold(column.listed_share())}')
    shortest, longest = column.min_length, column.max_length
    if column.held_lengths() is not None:
        if shortest is not None and longest is not None:
            condition = f'between {shortest - 1} and {longest + 1}'
        else:
            condition = f'<= {longest}' if longest is not None else f'>= {shortest}'
        rules.append(f'ColumnLength {name} {condition}{where}')
    elif shortest is not None or longest is not None:
        notes.append(f'{column.label}: the lengths {shortest} to {longest}, trimmed, as ColumnLength takes no threshold')
    if column.pattern and column.pattern_share is not None:
        share = within_values(column.pattern_share, column.completeness)
        rules.append(f'ColumnValues {name} matches {quoted(unanchored(column.pattern))}{where}{threshold(share)}')
    if column.format:
        notes.append(f'{column.label}: the {column.format} format, which DQDL has no rule for')
    rules += moment_rules(column, notes)
    iso = column.strftime is None or column.strftime.startswith(ISO_DATE)
    if table.text_source and column.raw_type_share is not None and value_type(column) in DATA_TYPES and iso:
        raw = within_values(column.raw_type_share, column.completeness)
        rules.append(f'ColumnDataType {name} = {quoted(DATA_TYPES[value_type(column)])}{where}{threshold(raw)}')
    nested = [inner for inner in column.nested() if inner.holds_constraint()]
    notes.extend(f'{inner.label}: its constraints, as a rule checks no nested field' for inner in nested)
    return rules


def moment_rules(column: ContractColumn, notes: list[str]) -> Iterator[str]:
    """The mean and standard deviation, each between its bounds, where Spark computes them as the profile did."""
    if column.mean is None and column.std is None:
        return
    if column.parsed_type:
        notes.append(f'{column.label}: its mean and standard deviation, as DQDL computes them only of a numeric column')
        return
    held = round((column.completeness.rows or 0) * column.completeness.value)
    for rule, moment in (('Mean', column.mean), ('StandardDeviation', column.std)):
        if moment is None:
            continue
        if rule == 'Mean' and column.logical_type == 'integer' and abs(moment.value) * held >= LONG_LIMIT:
            notes.append(f'{column.label}: its mean, as Spark sums a 64-bit integer column in 64 bits, and its {held} values overflow them')
            continue
        low, high = outward(*moment.bounds())
        yield f'{rule} {quoted(column.name)} between {low} and {high}'


def value_type(column: ContractColumn) -> str:
    """The type the column's values are compared as: the profile's, even where a strict format types the column as text."""
    return column.parsed_type or column.logical_type


def threshold(share: Share | None) -> str:
    """` with threshold >= <least>` for a share below every row, and nothing for one that is whole or declared."""
    if share is None or share.whole or not share.rows:
        return ''
    return f' with threshold >= {number(share.least())}'


def range_condition(column: ContractColumn) -> str:
    """`between` the column's ends, each widened one step, or `>=` or `<=` its one end."""
    if column.low is not None and column.high is not None:
        low, high = widened(column.low, column.high, column)
        return f'between {low} and {high}'
    temporal = value_type(column) in ('date', 'timestamp')
    end = column.low if column.low is not None else column.high
    written = quoted(str(as_day(end))) if temporal else number(end)
    return f'>= {written}' if column.low is not None else f'<= {written}'


def widened(low: Any, high: Any, column: ContractColumn) -> tuple[str, str]:
    """The range one step beyond each end, as `between` excludes both, written as DQDL of the column's type."""
    if value_type(column) in ('date', 'timestamp'):
        day = dt.timedelta(days=1)
        return quoted(str(as_day(low) - day)), quoted(str(as_day(high) + day))
    if value_type(column) == 'integer':
        return number(int(low) - 1), number(int(high) + 1)
    step = decimal.Decimal(1).scaleb(-column.places) if column.places is not None else None
    least, most = decimal.Decimal(repr(low)), decimal.Decimal(repr(high))
    return number(least - (step or last_place(least))), number(most + (step or last_place(most)))


def outward(low: float, high: float) -> tuple[str, str]:
    """`low` rounded down and `high` up, at `MOMENT_DIGITS` significant digits of the larger."""
    largest = max(abs(low), abs(high))
    place = decimal.Decimal(1).scaleb(math.floor(math.log10(largest)) - MOMENT_DIGITS + 1) if largest else decimal.Decimal(1)
    least = decimal.Decimal(repr(low)).quantize(place, rounding=decimal.ROUND_FLOOR)
    most = decimal.Decimal(repr(high)).quantize(place, rounding=decimal.ROUND_CEILING)
    return number(least), number(most)


def last_place(value: decimal.Decimal) -> decimal.Decimal:
    """One unit in the last decimal place `value` is written to, such as 0.1 for 1.5."""
    return decimal.Decimal(1).scaleb(min(int(value.as_tuple().exponent), 0))


def as_day(value: Any) -> dt.date:
    return value if isinstance(value, dt.date) else dt.date.fromisoformat(str(value)[:10])


def value_literal(value: Any, column: ContractColumn) -> str:
    if value_type(column) in ('integer', 'number') and isinstance(value, int | float):
        return number(value)
    return quoted(str(value))


def number(value: Any) -> str:
    """A number as DQDL writes one: digits, a point and digits, never an exponent."""
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    written = format(value if isinstance(value, decimal.Decimal) else decimal.Decimal(repr(float(value))), 'f')
    return written if '.' in written else f'{written}.0'


def quoted(text: str) -> str:
    """`text` as a DQDL string, which keeps a backslash as written, so a pattern's `\\d` reads as a digit."""
    return '"' + text.replace('"', '\\"') + '"'


def sql_name(name: str) -> str:
    """A column's name as Spark SQL reads it in a `where` clause."""
    return '`' + name.replace('`', '``') + '`'
