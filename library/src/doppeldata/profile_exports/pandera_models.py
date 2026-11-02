"""A module defining a `pandera.polars.DataFrameModel` of each table, importing only polars and pandera.

A field checks what holds for every row: its dtype, its nulls, a whole key, and a range, listed values, lengths or a
pattern where the profile states them of every value. pandera checks no format and no field nested in a struct or a
list, so those are named as left out.
"""

from doppeldata.profile_exports.contract_columns import ContractColumn
from doppeldata.profile_exports.contract_columns import ContractDocument
from doppeldata.profile_exports.contract_columns import ContractTable
from doppeldata.profile_exports.python_sources import attribute_names
from doppeldata.profile_exports.python_sources import class_name
from doppeldata.profile_exports.python_sources import literal
from doppeldata.profile_exports.python_sources import module_source
from doppeldata.profile_exports.type_names import polars_instance
from doppeldata.profile_exports.type_names import polars_name

RESERVED = (
    'Config',
    'build_schema_',
    'empty',
    'example',
    'from_json',
    'from_yaml',
    'get_metadata',
    'pydantic_validate',
    'strategy',
    'to_json',
    'to_json_schema',
    'to_schema',
    'to_yaml',
    'validate',
)


def pandera_module(document: ContractDocument) -> str:
    notes: list[str] = []
    body = '\n\n'.join(model(table, notes, document.bundle) for table in document.tables)
    return module_source(document, 'The pandera models', notes, ['import pandera.polars as pa', 'import polars as pl'], body)


def model(table: ContractTable, notes: list[str], bundle: bool) -> str:
    """The table's model, adding to `notes` what it leaves out."""
    prefix = f'{table.name}.' if bundle else ''
    notes.extend(prefix + note.note for note in table.left_out() if note.fact != 'foreign_key')
    lines = [f'class {class_name(table.name)}(pa.DataFrameModel):']
    unique = table.unique_columns()
    for column, (attribute, aliased) in zip(table.columns, attribute_names(table.columns, RESERVED), strict=True):
        annotation = polars_annotation(column)
        if annotation is None:
            notes.append(f'{prefix}{column.name}: the column, whose type {column.physical_type} has no Polars type')
            continue
        arguments = [f'alias={column.name!r}'] if aliased else []
        arguments += field_arguments(column, column.name in unique)
        lines.append(f'    {attribute}: {annotation} = pa.Field({", ".join(arguments)})')
        notes.extend(prefix + note for note in unchecked(column))
    composite = [list(key.columns) for key in table.stated_keys() if len(key.columns) > 1 and key.whole]
    if composite:
        lines += ['', '    class Config:', f'        unique = {composite[0]!r}']
        notes.extend(f'{prefix}{", ".join(key)}: the key, as a model holds one key of several columns' for key in composite[1:])
    return '\n'.join(lines) + '\n'


def field_arguments(column: ContractColumn, unique: bool) -> list[str]:
    arguments = []
    if not column.completeness.whole:
        arguments.append('nullable=True')
    if unique:
        arguments.append('unique=True')
    held = column.held_range()
    if held is not None:
        arguments += [f'{keyword}={literal(end, column)}' for keyword, end in zip(('ge', 'le'), held, strict=True) if end is not None]
    values = column.held_values()
    if values:
        arguments.append(f'isin=[{", ".join(literal(value, column) for value in values)}]')
    lengths = column.held_lengths()
    if lengths is not None:
        bounds = {key: end for key, end in zip(('min_value', 'max_value'), lengths, strict=True) if end is not None}
        arguments.append(f'str_length={bounds!r}')
    pattern = column.held_pattern()
    if pattern:
        arguments.append(f'str_matches={pattern!r}')
    return arguments


def unchecked(column: ContractColumn) -> list[str]:
    """What a model of `column` cannot check: its format, and what holds of a field nested in it."""
    found = []
    if column.held_format():
        found.append(f'{column.label}: the {column.held_format()} format, which pandera has no check of')
    found += [
        f'{inner.label}: its constraints, as pandera checks a nested field by type' for inner in nested(column) if inner.holds_constraint()
    ]
    return found


def nested(column: ContractColumn) -> list[ContractColumn]:
    return [found for inner in column.nested() for found in (inner, *nested(inner))]


def polars_annotation(column: ContractColumn) -> str | None:
    """The column's Polars type as a model's annotation, such as `Annotated[pl.List, pl.String()]`."""
    if column.logical_type == 'object':
        inner = polars_type(column)
        return f'Annotated[pl.Struct, {inner[len("pl.Struct(") : -1]}]' if inner else None
    if column.logical_type == 'array':
        element = polars_type(column.element) if column.element else 'pl.String()'
        return f'Annotated[pl.List, {element}]' if element else None
    return polars_name(column.physical_type)


def polars_type(column: ContractColumn) -> str | None:
    """The column's Polars type as an instance, such as `pl.Struct({'city': pl.String()})`."""
    if column.logical_type == 'object':
        fields = [(field.name, polars_type(field)) for field in column.fields]
        if not fields or any(kind is None for _, kind in fields):
            return None
        return 'pl.Struct({' + ', '.join(f'{name!r}: {kind}' for name, kind in fields) + '})'
    if column.logical_type == 'array':
        element = polars_type(column.element) if column.element else 'pl.String()'
        return f'pl.List({element})' if element else None
    return polars_instance(column.physical_type)
