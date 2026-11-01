"""A module holding each table's `StructType`, as `<TABLE>_STRUCT_TYPE`, and a `pandera.pyspark.DataFrameModel` of it,
importing only pyspark and pandera.

The struct type reads the data with its types, and the model checks what holds for every row, as the pandera export's
fields do. pandera's PySpark backend checks no uniqueness, so a key is named as left out.
"""

from doppeldata.profile_exports.contract_columns import ContractColumn
from doppeldata.profile_exports.contract_columns import ContractDocument
from doppeldata.profile_exports.contract_columns import ContractTable
from doppeldata.profile_exports.pandera_models import RESERVED
from doppeldata.profile_exports.pandera_models import nested
from doppeldata.profile_exports.python_sources import attribute_names
from doppeldata.profile_exports.python_sources import class_name
from doppeldata.profile_exports.python_sources import constant_name
from doppeldata.profile_exports.python_sources import literal
from doppeldata.profile_exports.python_sources import module_source
from doppeldata.profile_exports.type_names import spark_name


def pyspark_module(document: ContractDocument) -> str:
    notes: list[str] = []
    body = '\n\n'.join(table_source(table, notes, document.bundle) for table in document.tables)
    return module_source(
        document,
        'The PySpark struct types and pandera models',
        notes,
        ['import pandera.pyspark as pa', 'import pyspark.sql.types as st'],
        body,
    )


def table_source(table: ContractTable, notes: list[str], bundle: bool) -> str:
    """The table's struct type and model, adding to `notes` what they leave out."""
    prefix = f'{table.name}.' if bundle else ''
    notes.extend(prefix + note.note for note in table.left_out() if note.fact != 'foreign_key')
    typed = [(column, spark_type(column)) for column in table.columns]
    for column, kind in typed:
        if kind is None:
            notes.append(f'{prefix}{column.name}: the column, whose type {column.physical_type} has no Spark type')
    kept = [(column, kind) for column, kind in typed if kind is not None]
    fields = [f'        st.StructField({column.name!r}, {kind}, nullable={not column.completeness.whole}),' for column, kind in kept]
    lines = [f'{constant_name(table.name)}_STRUCT_TYPE = st.StructType(', '    [', *fields, '    ]', ')', '', '']
    lines.append(f'class {class_name(table.name)}(pa.DataFrameModel):')
    columns = [column for column, _ in kept]
    for (column, kind), (attribute, aliased) in zip(kept, attribute_names(columns, RESERVED), strict=True):
        arguments = [f'alias={column.name!r}'] if aliased else []
        arguments += field_arguments(column)
        lines.append(f'    {attribute}: {kind} = pa.Field({", ".join(arguments)})')
        if column.held_format():
            notes.append(f'{prefix}{column.label}: the {column.held_format()} format, which pandera has no check of')
        notes.extend(
            f'{prefix}{inner.label}: its constraints, as pandera checks a nested field by type'
            for inner in nested(column)
            if inner.holds_constraint()
        )
    whole = [key for key in table.stated_keys() if key.whole]
    notes.extend(f'{prefix}{", ".join(key.columns)}: the key, as pandera checks no uniqueness on PySpark' for key in whole)
    return '\n'.join(lines) + '\n'


def field_arguments(column: ContractColumn) -> list[str]:
    arguments = []
    if not column.completeness.whole:
        arguments.append('nullable=True')
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


def spark_type(column: ContractColumn) -> str | None:
    """The column's Spark type, such as `st.ArrayType(st.StringType(), True)`, else None where a part has none."""
    if column.logical_type == 'object':
        fields = [(field, spark_type(field)) for field in column.fields]
        if not fields or any(kind is None for _, kind in fields):
            return None
        written = ', '.join(f'st.StructField({field.name!r}, {kind}, {not field.required})' for field, kind in fields)
        return f'st.StructType([{written}])'
    if column.logical_type == 'array':
        element = column.element
        kind = spark_type(element) if element else 'st.StringType()'
        return f'st.ArrayType({kind}, {element is None or not element.completeness.whole})' if kind else None
    name = spark_name(column.physical_type)
    return f'st.{name}' if name else None
