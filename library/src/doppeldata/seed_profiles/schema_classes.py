"""A seed from a schema class a project already has: a pandera `DataFrameModel` or `DataFrameSchema`, a dataframely
`Schema`, or a Spark `StructType`.

Each is read by its attributes, so the library imports none of pandera, dataframely and pyspark, and declares none.
Types and constraints come from the class. A text column it says nothing more of takes the fake its name hints.
"""

import dataclasses as dc
import re
from typing import Any

from doppeldata.profile_format import Profile
from doppeldata.seed_profiles.declared_columns import DeclaredColumn
from doppeldata.seed_profiles.declared_columns import warn_left_out
from doppeldata.seed_profiles.declared_tables import DeclaredTable
from doppeldata.seed_profiles.hive_types import hive_column
from doppeldata.short_profiles import INLINE_TABLE

LIBRARIES = ('pandera', 'dataframely', 'pyspark')
# pandas' and Polars' dtype names, as pandera prints them. A datetime is matched in its own case, keeping its zone's.
INTEGER_DTYPE = re.compile(r'(?P<unsigned>u?)int(?P<bits>8|16|32|64)')
DECIMAL_DTYPE = re.compile(r'decimal\(precision=(?P<precision>\d+|none), scale=(?P<scale>\d+)\)')
PANDAS_DATETIME = re.compile(r'datetime64\[(?P<unit>s|ms|us|ns)(?:, (?P<zone>[^\]]+))?\]', re.IGNORECASE)
POLARS_DATETIME = re.compile(r"datetime\(time_unit='(?P<unit>ms|us|ns)', time_zone=(?:'(?P<zone>[^']+)'|None)\)", re.IGNORECASE)
TEXT_DTYPES = ('str', 'string', 'utf8', 'object', 'category', 'categorical', 'string[python]', 'string[pyarrow]')
FLOAT_DTYPES = {'float32': 'float', 'float': 'float', 'float64': 'double', 'double': 'double'}
# A decimal with no stated precision takes Arrow's widest.
DECIMAL_PRECISION = 38
# Each dataframely column class read as a scalar, and its Arrow type.
DATAFRAMELY_SCALARS = {
    **{f'{sign}Int{bits}': f'{sign.lower()}int{bits}' for sign in ('', 'U') for bits in (8, 16, 32, 64)},
    'Float32': 'float',
    'Float64': 'double',
    'Float': 'double',
    'Integer': 'int64',
    'Bool': 'bool',
    'Date': 'date32[day]',
    'Time': 'time64[us]',
    'String': 'string',
    'Categorical': 'string',
    'Enum': 'string',
}
LOGICAL_OF_ARROW = {
    'bool': 'boolean',
    'date32[day]': 'date',
    'time64[us]': 'time',
    'string': 'string',
    'float': 'number',
    'double': 'number',
}


def schema_library(source: Any) -> str | None:
    """The library whose schema class `source` is, by the module that defines it or a class it derives from, else None."""
    cls = source if isinstance(source, type) else type(source)
    for base in cls.__mro__:
        root = base.__module__.split('.')[0]
        if root in LIBRARIES:
            return root
    return None


def schema_class_profile(source: Any) -> Profile | None:
    """The seed of a pandera, dataframely or Spark schema class, or None where `source` is none of them."""
    library = schema_library(source)
    if library == 'pandera':
        return pandera_table(source).profile()
    if library == 'dataframely' and isinstance(source, type) and callable(getattr(source, 'columns', None)):
        return dataframely_table(source).profile()
    if library == 'pyspark' and isinstance(getattr(source, 'fields', None), list):
        return spark_table(source).profile()
    return None


def qualified(source: Any) -> str:
    cls = source if isinstance(source, type) else type(source)
    return f'{cls.__module__}.{cls.__qualname__}'


def left_out(named: str, unread: list[str]) -> None:
    if unread:
        warn_left_out(f'{named}: columns of types the engine draws nothing of are left out: {", ".join(unread)}')


def pandera_table(source: Any) -> DeclaredTable:
    """A pandera `DataFrameModel`'s table, from the schema it builds, or a `DataFrameSchema`'s."""
    model = isinstance(source, type)
    schema = source.to_schema() if model else source
    name = source.__name__ if model else schema.name or INLINE_TABLE
    named = f'pandera:{qualified(source) if model else name}'
    columns, unread = [], []
    for column_name, column in schema.columns.items():
        declared = frame_column(str(column_name), str(column.dtype) if column.dtype is not None else 'string')
        if declared is None:
            unread.append(f'{column_name} ({column.dtype})')
            continue
        declared = dc.replace(declared, required=not column.nullable, key=bool(column.unique))
        columns.append(dc.replace(declared, **checked(column.checks)))
    left_out(named, unread)
    unique = schema.unique if isinstance(schema.unique, list) else [schema.unique] if schema.unique else []
    return DeclaredTable(name=str(name), columns=tuple(columns), imported_from=named, unique_keys=(tuple(unique),) if unique else ())


def checked(checks: Any) -> dict[str, Any]:
    """The bounds, values, pattern and lengths a pandera column's built-in checks state, by each check's name."""
    found: dict[str, Any] = {}
    for check in checks or []:
        stats = getattr(check, 'statistics', None) or {}
        name = getattr(check, 'name', None)
        if name in ('greater_than_or_equal_to', 'greater_than'):
            found |= {'low': stats.get('min_value'), 'low_exclusive': name == 'greater_than'}
        elif name in ('less_than_or_equal_to', 'less_than'):
            found |= {'high': stats.get('max_value'), 'high_exclusive': name == 'less_than'}
        elif name == 'in_range':
            found |= {'low': stats.get('min_value'), 'high': stats.get('max_value')}
            found |= {'low_exclusive': not stats.get('include_min', True), 'high_exclusive': not stats.get('include_max', True)}
        elif name in ('isin', 'unique_values_eq'):
            found['values'] = tuple(stats.get('allowed_values') or ())
        elif name == 'equal_to':
            found['values'] = (stats.get('value'),)
        elif name == 'str_matches':
            found['pattern'] = str(stats.get('pattern'))
        elif name == 'str_length':
            exact = stats.get('exact_value')
            found |= {'min_length': exact or stats.get('min_value'), 'max_length': exact or stats.get('max_value')}
    return found


def frame_column(name: str, dtype: str) -> DeclaredColumn | None:
    """The column a pandas or Polars dtype name declares, such as `int32`, `string[pyarrow]` or `Datetime(time_unit='us', ...)`."""
    text = dtype.strip().lower()
    integer = INTEGER_DTYPE.fullmatch(text)
    if integer is not None:
        return DeclaredColumn(name=name, logical_type='integer', physical_type=f'{integer["unsigned"]}int{integer["bits"]}')
    if text in FLOAT_DTYPES:
        return DeclaredColumn(name=name, logical_type='number', physical_type=FLOAT_DTYPES[text])
    decimal = DECIMAL_DTYPE.fullmatch(text)
    if decimal is not None:
        precision = DECIMAL_PRECISION if decimal['precision'] == 'none' else int(decimal['precision'])
        return DeclaredColumn(name=name, logical_type='number', physical_type=f'decimal128({precision}, {decimal["scale"]})')
    if text in TEXT_DTYPES or text.startswith('enum'):
        return DeclaredColumn(name=name, logical_type='string')
    if text in ('bool', 'boolean'):
        return DeclaredColumn(name=name, logical_type='boolean')
    if text in ('date', 'date32[day]'):
        return DeclaredColumn(name=name, logical_type='date')
    if text in ('time', 'time64[us]'):
        return DeclaredColumn(name=name, logical_type='time')
    stamp = PANDAS_DATETIME.fullmatch(dtype.strip()) or POLARS_DATETIME.fullmatch(dtype.strip())
    if stamp is not None or text == 'datetime':
        unit = stamp['unit'].lower() if stamp is not None else 'us'
        zone = stamp['zone'] if stamp is not None else None
        return timestamp_column(name, unit, zone)
    return None


def timestamp_column(name: str, unit: str, zone: str | None) -> DeclaredColumn:
    """A timestamp column in `unit`, an instant in UTC where it names a zone and a wall-clock reading where it names none."""
    if not zone:
        return DeclaredColumn(name=name, logical_type='timestamp', physical_type=f'timestamp[{unit}]')
    zone = 'UTC' if zone.lower() == 'utc' else zone
    return DeclaredColumn(name=name, logical_type='timestamp', physical_type=f'timestamp[{unit}, tz={zone}]', timezone='UTC')


def dataframely_table(source: Any) -> DeclaredTable:
    """A dataframely `Schema`'s table, each column read by its class's name and its rule attributes."""
    named = f'dataframely:{qualified(source)}'
    columns, unread = [], []
    for column_name, column in source.columns().items():
        declared = dataframely_column(str(column_name), column)
        if declared is None:
            unread.append(f'{column_name} ({type(column).__name__})')
        else:
            columns.append(declared)
    left_out(named, unread)
    primary = source.primary_key() if callable(getattr(source, 'primary_key', None)) else []
    return DeclaredTable(name=source.__name__, columns=tuple(columns), imported_from=named, primary_key=tuple(primary))


def dataframely_column(name: str, column: Any) -> DeclaredColumn | None:
    kind = type(column).__name__
    required = not getattr(column, 'nullable', False)
    if kind == 'Struct':
        fields = [dataframely_column(str(field), inner) for field, inner in (getattr(column, 'inner', None) or {}).items()]
        if not all(fields):
            return None
        return DeclaredColumn(name=name, logical_type='object', required=required, fields=tuple(field for field in fields if field))
    if kind in ('List', 'Array'):
        element = dataframely_column('element', column.inner)
        if element is None:
            return None
        return DeclaredColumn(name=name, logical_type='array', required=required, element=dc.replace(element, required=True))
    if kind == 'Decimal':
        precision = getattr(column, 'precision', None) or DECIMAL_PRECISION
        physical = f'decimal128({precision}, {getattr(column, "scale", 0) or 0})'
        declared = DeclaredColumn(name=name, logical_type='number', physical_type=physical)
    elif kind == 'Datetime':
        declared = timestamp_column(name, getattr(column, 'time_unit', None) or 'us', getattr(column, 'time_zone', None))
    elif kind in DATAFRAMELY_SCALARS:
        physical = DATAFRAMELY_SCALARS[kind]
        logical = LOGICAL_OF_ARROW.get(physical, 'integer')
        declared = DeclaredColumn(name=name, logical_type=logical, physical_type=physical)
    else:
        return None
    low, high = getattr(column, 'min', None), getattr(column, 'max', None)
    low_exclusive, high_exclusive = getattr(column, 'min_exclusive', None), getattr(column, 'max_exclusive', None)
    return dc.replace(
        declared,
        required=required,
        key=bool(getattr(column, 'primary_key', False)),
        low=low if low is not None else low_exclusive,
        high=high if high is not None else high_exclusive,
        low_exclusive=low is None and low_exclusive is not None,
        high_exclusive=high is None and high_exclusive is not None,
        pattern=getattr(column, 'regex', None),
        min_length=getattr(column, 'min_length', None),
        max_length=getattr(column, 'max_length', None),
        values=tuple(getattr(column, 'categories', None) or ()) if kind == 'Enum' else (),
    )


def spark_table(struct: Any) -> DeclaredTable:
    """A Spark `StructType`'s table, each field read by its type's Hive name."""
    columns, unread = [], []
    for field in struct.fields:
        declared = spark_column(field.name, field.dataType, field.nullable)
        if declared is None:
            unread.append(f'{field.name} ({field.dataType.simpleString()})')
        else:
            columns.append(declared)
    left_out('spark:StructType', unread)
    return DeclaredTable(name=INLINE_TABLE, columns=tuple(columns), imported_from='spark:StructType')


def spark_column(name: str, data_type: Any, nullable: bool) -> DeclaredColumn | None:
    """A Spark field's column. A `TimestampType` is an instant, so it is UTC, where `timestamp_ntz` is not."""
    kind = type(data_type).__name__
    if kind == 'StructType':
        fields = [spark_column(field.name, field.dataType, field.nullable) for field in data_type.fields]
        if not all(fields):
            return None
        return DeclaredColumn(name=name, logical_type='object', required=not nullable, fields=tuple(field for field in fields if field))
    if kind == 'ArrayType':
        element = spark_column('element', data_type.elementType, False)
        return DeclaredColumn(name=name, logical_type='array', required=not nullable, element=element) if element else None
    if kind == 'TimestampType':
        return DeclaredColumn(
            name=name, logical_type='timestamp', physical_type='timestamp[us, tz=UTC]', timezone='UTC', required=not nullable
        )
    return hive_column(name, data_type.simpleString(), required=not nullable)
