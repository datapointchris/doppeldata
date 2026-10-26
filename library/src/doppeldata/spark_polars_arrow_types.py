"""The Arrow type, as pyarrow spells it, of a Spark or a Polars data type, read by its class's name and its attributes.

The Spark and Polars profilers and the seeds of Spark, pandera and dataframely schemas each ask this module, so one type
reads as one Arrow type whichever of them meets it. It imports neither pyspark nor polars, so a seed needs neither.
"""

from typing import Any

# A decimal with no stated precision takes Arrow's widest.
DECIMAL_PRECISION = 38
SPARK_SCALARS = {
    'ByteType': 'int8',
    'ShortType': 'int16',
    'IntegerType': 'int32',
    'LongType': 'int64',
    'FloatType': 'float',
    'DoubleType': 'double',
    'StringType': 'string',
    'BooleanType': 'bool',
    'DateType': 'date32[day]',
    'BinaryType': 'binary',
    'NullType': 'null',
    # Spark holds a timestamp as an instant in microseconds, read in the session's UTC.
    'TimestampType': 'timestamp[us, tz=UTC]',
    'TimestampNTZType': 'timestamp[us]',
    'DayTimeIntervalType': 'duration[us]',
}
POLARS_SCALARS = {
    **{f'{sign}Int{bits}': f'{sign.lower()}int{bits}' for sign in ('', 'U') for bits in (8, 16, 32, 64)},
    'Float32': 'float',
    'Float64': 'double',
    'String': 'string',
    'Boolean': 'bool',
    'Date': 'date32[day]',
    # Polars holds a time of day in nanoseconds.
    'Time': 'time64[ns]',
    'Binary': 'binary',
    'Null': 'null',
}


def type_name(data_type: Any) -> str:
    """The name of `data_type`'s class, or its own name where it is a class, as Polars' unparameterized types are."""
    return (data_type if isinstance(data_type, type) else type(data_type)).__name__


def spark_arrow_type(data_type: Any) -> str:
    """A Spark type's Arrow type, as Spark's own Arrow conversion gives it, and its `simpleString` where it has none."""
    kind = type_name(data_type)
    if kind == 'DecimalType':
        return f'decimal128({data_type.precision}, {data_type.scale})'
    if kind == 'ArrayType':
        return f'list<item: {spark_arrow_type(data_type.elementType)}>'
    if kind == 'StructType':
        return 'struct<' + ', '.join(f'{field.name}: {spark_arrow_type(field.dataType)}' for field in data_type.fields) + '>'
    if kind == 'MapType':
        return f'map<{spark_arrow_type(data_type.keyType)}, {spark_arrow_type(data_type.valueType)}>'
    return SPARK_SCALARS.get(kind, data_type.simpleString())


def polars_arrow_type(dtype: Any) -> str:
    """A Polars type's Arrow type, such as `decimal128(12, 2)` or `timestamp[us, tz=UTC]`, and its own name in lower
    case where it has none.
    """
    kind = type_name(dtype)
    if kind == 'Decimal':
        return f'decimal128({getattr(dtype, "precision", None) or DECIMAL_PRECISION}, {getattr(dtype, "scale", None) or 0})'
    if kind == 'Datetime':
        unit, zone = getattr(dtype, 'time_unit', None) or 'us', getattr(dtype, 'time_zone', None)
        return f'timestamp[{unit}, tz={zone}]' if zone else f'timestamp[{unit}]'
    if kind == 'Duration':
        return f'duration[{getattr(dtype, "time_unit", None) or "us"}]'
    if kind == 'List':
        return f'list<item: {polars_arrow_type(dtype.inner)}>'
    if kind == 'Array':
        return f'fixed_size_list<item: {polars_arrow_type(dtype.inner)}>[{dtype.size}]'
    if kind == 'Struct':
        return 'struct<' + ', '.join(f'{field.name}: {polars_arrow_type(field.dtype)}' for field in dtype.fields) + '>'
    return POLARS_SCALARS.get(kind, str(dtype).lower())
