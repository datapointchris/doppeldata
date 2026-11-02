"""A module defining a `dataframely.Schema` of each table, importing only polars and dataframely.

A column checks what holds for every row, as the pandera export's fields do. A whole primary key marks its columns
`primary_key`, and every other whole key of several columns is a rule. A list's or a struct's nested columns carry
their own checks, which dataframely evaluates.
"""

from doppeldata.profile_exports.contract_columns import ContractColumn
from doppeldata.profile_exports.contract_columns import ContractDocument
from doppeldata.profile_exports.contract_columns import ContractTable
from doppeldata.profile_exports.python_sources import attribute_names
from doppeldata.profile_exports.python_sources import class_name
from doppeldata.profile_exports.python_sources import literal
from doppeldata.profile_exports.python_sources import module_source
from doppeldata.profile_exports.python_sources import snake
from doppeldata.profile_exports.type_names import POLARS_NAMES
from doppeldata.profile_exports.type_names import decimal_of
from doppeldata.profile_exports.type_names import timestamp_of
from doppeldata.profile_exports.type_names import type_name

RESERVED = (
    'cast',
    'column_names',
    'columns',
    'create_empty',
    'create_empty_if_none',
    'filter',
    'is_valid',
    'matches',
    'primary_key',
    'sample',
    'to_pydantic_model',
    'to_sqlalchemy_columns',
    'validate',
)
# dataframely's column class of each Polars type it spells differently.
DATAFRAMELY_NAMES = {'Boolean': 'Bool'}


def dataframely_module(document: ContractDocument) -> str:
    notes: list[str] = []
    body = '\n\n'.join(schema(table, notes, document.bundle) for table in document.tables)
    return module_source(document, 'The dataframely schemas', notes, ['import dataframely as dy', 'import polars as pl'], body)


def schema(table: ContractTable, notes: list[str], bundle: bool) -> str:
    """The table's schema, adding to `notes` what it leaves out."""
    prefix = f'{table.name}.' if bundle else ''
    notes.extend(prefix + note.note for note in table.left_out() if note.fact != 'foreign_key')
    lines = [f'class {class_name(table.name)}(dy.Schema):']
    primary = table.primary_key.columns if table.primary_key and table.primary_key.whole else ()
    unique = table.unique_columns() - set(primary)
    for column, (attribute, aliased) in zip(table.columns, attribute_names(table.columns, RESERVED), strict=True):
        flags = {'primary_key': column.name in primary, 'unique': column.name in unique}
        written = column_source(column, flags, column.name if aliased else None)
        if written is None:
            notes.append(f'{prefix}{column.name}: the column, whose type {column.physical_type} has no dataframely column')
            continue
        lines.append(f'    {attribute} = {written}')
        if column.held_format():
            notes.append(f'{prefix}{column.label}: the {column.held_format()} format, which dataframely has no check of')
    for key in table.stated_keys():
        if len(key.columns) > 1 and key.whole and key.columns != primary:
            lines += [
                '',
                '    @dy.rule()',
                f'    def {snake("_".join(key.columns))}_unique(cls) -> pl.Expr:',
                f'        return pl.struct({", ".join(map(repr, key.columns))}).is_unique()',
            ]
    return '\n'.join(lines) + '\n'


def column_source(column: ContractColumn, flags: dict[str, bool], alias: str | None) -> str | None:
    """The column's dataframely column, such as `dy.Int64(nullable=False, min=1)`, else None for a type it has none of."""
    arguments = [f'nullable={not column.required}']
    arguments += [f'{flag}=True' for flag, held in flags.items() if held]
    held = column.held_range()
    if held is not None:
        arguments += [f'{keyword}={literal(end, column)}' for keyword, end in zip(('min', 'max'), held, strict=True) if end is not None]
    lengths = column.held_lengths()
    if lengths is not None:
        arguments += [f'{keyword}={end}' for keyword, end in zip(('min_length', 'max_length'), lengths, strict=True) if end is not None]
    pattern = column.held_pattern()
    if pattern:
        arguments.append(f'regex={pattern!r}')
    values = column.held_values()
    if values:
        listed = ', '.join(literal(value, column) for value in values)
        arguments.append(f'is_in=[{listed}]' if column.logical_type == 'integer' else f'check=lambda column: column.is_in([{listed}])')
    if column.logical_type == 'array':
        if column.min_items is not None or column.max_items is not None:
            arguments += [
                f'{keyword}={count}'
                for keyword, count in (('min_length', column.min_items), ('max_length', column.max_items))
                if count is not None
            ]
        element = column_source(column.element, {}, None) if column.element else 'dy.String(nullable=True)'
        return f'dy.List({element}, {", ".join(with_alias(arguments, alias))})' if element else None
    if column.logical_type == 'object':
        fields = [(field.name, column_source(field, {}, None)) for field in column.fields]
        if not fields or any(written is None for _, written in fields):
            return None
        inner = '{' + ', '.join(f'{name!r}: {written}' for name, written in fields) + '}'
        return f'dy.Struct({inner}, {", ".join(with_alias(arguments, alias))})'
    kind = scalar_column(column.physical_type)
    if kind is None:
        return None
    constructor, parameters = kind
    return f'dy.{constructor}({", ".join([*parameters, *with_alias(arguments, alias)])})'


def with_alias(arguments: list[str], alias: str | None) -> list[str]:
    return [*arguments, f'alias={alias!r}'] if alias else arguments


def scalar_column(arrow_type: str) -> tuple[str, list[str]] | None:
    """The dataframely column class of a scalar Arrow type, and the parameters its type takes."""
    decimal = decimal_of(arrow_type)
    if decimal is not None:
        return 'Decimal', [f'precision={decimal[0]}', f'scale={decimal[1]}']
    timestamp = timestamp_of(arrow_type)
    if timestamp is not None:
        unit, zone = timestamp
        return 'Datetime', [*([f'time_zone={zone!r}'] if zone else []), f'time_unit={unit!r}']
    if type_name(arrow_type) == 'time64':
        return 'Time', []
    name = POLARS_NAMES.get(arrow_type)
    return (DATAFRAMELY_NAMES.get(name, name), []) if name else None
