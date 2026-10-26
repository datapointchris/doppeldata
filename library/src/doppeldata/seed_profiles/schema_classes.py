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
from doppeldata.seed_profiles.declared_columns import logical_type_of
from doppeldata.seed_profiles.declared_columns import warn_left_out
from doppeldata.seed_profiles.declared_tables import DeclaredTable
from doppeldata.seed_profiles.hive_types import hive_column
from doppeldata.short_profiles import INLINE_TABLE
from doppeldata.spark_polars_arrow_types import DECIMAL_PRECISION
from doppeldata.spark_polars_arrow_types import polars_arrow_type
from doppeldata.spark_polars_arrow_types import spark_arrow_type
from doppeldata.spark_polars_arrow_types import type_name

LIBRARIES = ('pandera', 'dataframely', 'pyspark')
# The pandera built-in checks a seed reads, by the name each gives itself.
READ_CHECKS = (
    'greater_than_or_equal_to',
    'greater_than',
    'less_than_or_equal_to',
    'less_than',
    'in_range',
    'isin',
    'unique_values_eq',
    'equal_to',
    'str_matches',
    'str_length',
)
# pandas-backed pandera's dtype classes, by their names in lower case, each parameter read off the dtype.
INTEGER_DTYPE = re.compile(r'(?P<unsigned>u?)int(?P<bits>8|16|32|64)')
TEXT_DTYPES = ('string', 'object')
# The engine draws no dictionary type, so these are drawn as text.
DICTIONARY_DTYPES = ('category', 'categorical', 'enum')
FLOAT_DTYPES = {'float32': 'float', 'float64': 'double'}


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
    named = f'pandera:{qualified(source) if model else schema.name or "DataFrameSchema"}'
    columns, unread, unchecked = [], [], [f'the table ({check_name(check)})' for check in getattr(schema, 'checks', None) or []]
    as_text = []
    for column_name, column in schema.columns.items():
        declared = frame_column(str(column_name), column.dtype)
        if declared is None:
            unread.append(f'{column_name} ({column.dtype})')
            continue
        if type(column.dtype).__name__.lower() in DICTIONARY_DTYPES:
            as_text.append(str(column_name))
        declared = dc.replace(declared, required=not column.nullable, key=bool(column.unique))
        found, skipped = checked(column.checks)
        columns.append(dc.replace(declared, **found))
        unchecked += [f'{column_name} ({skipped_name})' for skipped_name in skipped]
    left_out(named, unread)
    drawn_as_text(named, as_text, 'coerce=True')
    if unchecked:
        warn_left_out(f'{named}: checks a seed cannot read are left out: {", ".join(unchecked)}')
    unique = schema.unique if isinstance(schema.unique, list) else [schema.unique] if schema.unique else []
    return DeclaredTable(name=str(name), columns=tuple(columns), imported_from=named, unique_keys=(tuple(unique),) if unique else ())


def check_name(check: Any) -> str:
    return str(getattr(check, 'name', None) or getattr(check, 'error', None) or type(check).__name__)


def checked(checks: Any) -> tuple[dict[str, Any], list[str]]:
    """The bounds, values, pattern and lengths a pandera column's built-in checks state, by each check's name, and the
    name of each check it reads nothing from, such as a custom one.
    """
    found: dict[str, Any] = {}
    skipped: list[str] = []
    for check in checks or []:
        stats = getattr(check, 'statistics', None) or {}
        name = getattr(check, 'name', None)
        if name not in READ_CHECKS:
            skipped.append(check_name(check))
        elif name in ('greater_than_or_equal_to', 'greater_than'):
            found |= {'low': stats.get('min_value'), 'low_exclusive': name == 'greater_than'}
        elif name in ('less_than_or_equal_to', 'less_than'):
            found |= {'high': stats.get('max_value'), 'high_exclusive': name == 'less_than'}
        elif name == 'in_range':
            found |= {'low': stats.get('min_value'), 'high': stats.get('max_value')}
            found |= {'low_exclusive': not stats.get('include_min', True), 'high_exclusive': not stats.get('include_max', True)}
        elif name == 'isin':
            found['values'] = tuple(stats.get('allowed_values') or ())
        elif name == 'unique_values_eq':
            found['values'] = tuple(sorted(stats.get('values') or (), key=str))
        elif name == 'equal_to':
            found['values'] = (stats.get('value'),)
        elif name == 'str_matches':
            found['pattern'] = str(stats.get('pattern'))
        elif name == 'str_length':
            exact = stats.get('exact_value')
            found |= {'min_length': exact or stats.get('min_value'), 'max_length': exact or stats.get('max_value')}
    return found, skipped


def frame_column(name: str, dtype: Any) -> DeclaredColumn | None:
    """The column a pandera dtype declares: by the Polars type it wraps, where it wraps one, else by its class, such as
    `INT32`, `STRING` or `DateTime`. A column declaring no dtype is text.
    """
    if dtype is None:
        return DeclaredColumn(name=name, logical_type='string')
    kind = type(dtype).__name__.lower()
    if kind in DICTIONARY_DTYPES:
        categories = getattr(dtype, 'categories', None)
        values = tuple(str(value) for value in categories) if categories is not None else ()
        return DeclaredColumn(name=name, logical_type='string', values=values)
    wrapped = getattr(dtype, 'type', None)
    if is_polars_type(wrapped):
        return polars_column(name, wrapped)
    integer = INTEGER_DTYPE.fullmatch(kind)
    if integer is not None:
        return DeclaredColumn(name=name, logical_type='integer', physical_type=f'{integer["unsigned"]}int{integer["bits"]}')
    if kind in FLOAT_DTYPES:
        return DeclaredColumn(name=name, logical_type='number', physical_type=FLOAT_DTYPES[kind])
    if kind == 'decimal':
        precision = getattr(dtype, 'precision', None) or DECIMAL_PRECISION
        physical = f'decimal128({precision}, {getattr(dtype, "scale", None) or 0})'
        return DeclaredColumn(name=name, logical_type='number', physical_type=physical)
    if kind in TEXT_DTYPES:
        return DeclaredColumn(name=name, logical_type='string')
    if kind in ('bool', 'boolean'):
        return DeclaredColumn(name=name, logical_type='boolean')
    if kind == 'date':
        return DeclaredColumn(name=name, logical_type='date')
    if kind == 'datetime':
        zone = getattr(dtype, 'tz', None)
        return timestamp_column(name, str(getattr(dtype, 'unit', None) or 'us'), str(zone) if zone is not None else None)
    return None


def is_polars_type(value: Any) -> bool:
    """Whether `value` is a Polars data type, a class or an instance, told apart without importing polars."""
    return value is not None and (value if isinstance(value, type) else type(value)).__module__.partition('.')[0] == 'polars'


def polars_column(name: str, dtype: Any) -> DeclaredColumn | None:
    """The column a Polars type declares, of the Arrow type the Polars profiler writes for it. A dictionary type is drawn
    as text, and a datetime naming a zone is an instant in UTC.
    """
    kind = type_name(dtype)
    if kind in ('Categorical', 'Enum'):
        return DeclaredColumn(name=name, logical_type='string')
    if kind == 'Datetime':
        return timestamp_column(name, getattr(dtype, 'time_unit', None) or 'us', getattr(dtype, 'time_zone', None))
    physical = polars_arrow_type(dtype)
    logical = logical_type_of(physical)
    return DeclaredColumn(name=name, logical_type=logical, physical_type=physical) if logical else None


def drawn_as_text(named: str, columns: list[str], validated_with: str) -> None:
    if columns:
        warn_left_out(f'{named}: columns of a dictionary type are drawn as text, so validate with {validated_with}: {", ".join(columns)}')


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
    as_text = [str(column_name) for column_name, column in source.columns().items() if type(column).__name__.lower() in DICTIONARY_DTYPES]
    drawn_as_text(named, as_text, 'cast=True')
    unchecked = [str(column_name) for column_name, column in source.columns().items() if getattr(column, 'check', None)]
    if unchecked:
        warn_left_out(f'{named}: custom checks a seed cannot read are left out: {", ".join(unchecked)}')
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
        return DeclaredColumn(
            name=name,
            logical_type='array',
            required=required,
            element=dc.replace(element, required=True),
            min_items=getattr(column, 'min_length', None),
            max_items=getattr(column, 'max_length', None),
        )
    declared = polars_column(name, getattr(column, 'dtype', None))
    if declared is None:
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
    """A Spark field's column, of the Arrow type the Spark profiler writes for it. A `TimestampType` is an instant, so it
    is UTC, where `timestamp_ntz` is not. A varchar's or a char's lengths are read from its Hive name.
    """
    kind = type_name(data_type)
    if kind == 'StructType':
        fields = [spark_column(field.name, field.dataType, field.nullable) for field in data_type.fields]
        if not all(fields):
            return None
        return DeclaredColumn(name=name, logical_type='object', required=not nullable, fields=tuple(field for field in fields if field))
    if kind == 'ArrayType':
        element = spark_column('element', data_type.elementType, False)
        return DeclaredColumn(name=name, logical_type='array', required=not nullable, element=element) if element else None
    if kind in ('VarcharType', 'CharType'):
        return hive_column(name, data_type.simpleString(), required=not nullable)
    physical = spark_arrow_type(data_type)
    logical = logical_type_of(physical)
    if logical is None:
        return None
    timezone = 'UTC' if kind == 'TimestampType' else None
    return DeclaredColumn(name=name, logical_type=logical, physical_type=physical, timezone=timezone, required=not nullable)
