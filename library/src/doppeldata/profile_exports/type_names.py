"""Each export format's name for a column's type, read from the tables the seed importers read it back through.

The importers map a format's type names onto Arrow types, as Hive's `bigint` onto `int64`. Each table here inverts one of
theirs, the first name for an Arrow type winning, so an exported type imports as the same logical type. A format with no
name for an integer width, a decimal's precision or a time zone imports as its importer's default.
"""

import re
from collections.abc import Mapping

from doppeldata.seed_profiles.declared_columns import ARROW_TYPE_NAME
from doppeldata.seed_profiles.declared_columns import DECIMAL_TYPE
from doppeldata.seed_profiles.declared_columns import FORMAT_TYPES
from doppeldata.seed_profiles.hive_types import SCALARS as HIVE_SCALARS
from doppeldata.seed_profiles.json_schemas import NUMBER_FORMATS
from doppeldata.seed_profiles.json_schemas import TEMPORAL_FORMATS
from doppeldata.seed_profiles.table_schemas import TYPES as TABLE_SCHEMA_TYPES
from doppeldata.spark_polars_arrow_types import POLARS_SCALARS
from doppeldata.spark_polars_arrow_types import SPARK_SCALARS

TIMESTAMP_TYPE = re.compile(r'^timestamp\[(?P<unit>\w+)(?:,\s*tz=(?P<zone>[^\]]+))?\]$')


def inverted(table: Mapping[str, str]) -> dict[str, str]:
    """Each value of `table` to the first key naming it."""
    found: dict[str, str] = {}
    for key, value in table.items():
        found.setdefault(value, key)
    return found


HIVE_NAMES = inverted({name: arrow for name, (arrow, _) in HIVE_SCALARS.items()})
SPARK_NAMES = inverted(SPARK_SCALARS)
POLARS_NAMES = inverted(POLARS_SCALARS)
JSON_NUMBER_FORMATS = inverted(NUMBER_FORMATS)
JSON_TEMPORAL_FORMATS = inverted(TEMPORAL_FORMATS)
TABLE_SCHEMA_NAMES = inverted(TABLE_SCHEMA_TYPES)
# A semantic type's JSON Schema format, the one the importers read the type from.
SEMANTIC_FORMATS = inverted(FORMAT_TYPES)
JSON_TYPES = {
    'string': 'string',
    'integer': 'integer',
    'number': 'number',
    'boolean': 'boolean',
    'object': 'object',
    'array': 'array',
    'date': 'string',
    'timestamp': 'string',
    'time': 'string',
}
# The JSON type a raw value parsing as each `type_mix` key is written as.
TYPE_MIX_JSON_TYPES = {
    'integer': 'integer',
    'decimal': 'number',
    'string': 'string',
    'boolean': 'boolean',
    'date': 'string',
    'timestamp': 'string',
}
# Spark has no unsigned integer, so each takes the narrowest signed type holding every value.
SPARK_UNSIGNED = {'uint8': 'ShortType', 'uint16': 'IntegerType', 'uint32': 'LongType'}


def type_name(arrow_type: str) -> str:
    """The name of an Arrow type ahead of its parameters, such as `decimal128` or `timestamp`."""
    name = ARROW_TYPE_NAME.match(arrow_type)
    return name[0] if name else arrow_type


def decimal_of(arrow_type: str) -> tuple[int, int] | None:
    """A decimal type's precision and scale, else None."""
    found = DECIMAL_TYPE.match(arrow_type)
    return (int(found['precision']), int(found['scale'])) if found else None


def timestamp_of(arrow_type: str) -> tuple[str, str | None] | None:
    """A timestamp type's unit and zone, the zone None for a wall-clock reading, else None."""
    found = TIMESTAMP_TYPE.match(arrow_type)
    return (found['unit'], found['zone']) if found else None


def hive_name(arrow_type: str) -> str | None:
    """The Hive name of a scalar Arrow type, such as `bigint` or `decimal(12,2)`, else None, as for `uint32` or `time64`."""
    decimal = decimal_of(arrow_type)
    if decimal is not None:
        return f'decimal({decimal[0]},{decimal[1]})'
    if timestamp_of(arrow_type) is not None:
        return 'timestamp'
    return HIVE_NAMES.get(arrow_type)


def spark_name(arrow_type: str) -> str | None:
    """The `pyspark.sql.types` constructor of a scalar Arrow type, such as `LongType()`, else None.

    A timestamp naming a zone is Spark's instant, and one naming none its wall-clock reading, whatever its unit.
    """
    decimal = decimal_of(arrow_type)
    if decimal is not None:
        return f'DecimalType({decimal[0]}, {decimal[1]})'
    timestamp = timestamp_of(arrow_type)
    if timestamp is not None:
        return 'TimestampType()' if timestamp[1] else 'TimestampNTZType()'
    if arrow_type == 'uint64':
        return 'DecimalType(20, 0)'
    name = SPARK_UNSIGNED.get(arrow_type) or SPARK_NAMES.get(arrow_type)
    return f'{name}()' if name else None


def polars_name(arrow_type: str) -> str | None:
    """The Polars type of a scalar Arrow type, written as an annotation pandera reads, such as `pl.Int64` or
    `Annotated[pl.Decimal, 12, 2]`, else None. pandera requires every argument of an annotated type, so a timestamp
    with no zone names `None`."""
    decimal = decimal_of(arrow_type)
    if decimal is not None:
        return f'Annotated[pl.Decimal, {decimal[0]}, {decimal[1]}]'
    timestamp = timestamp_of(arrow_type)
    if timestamp is not None:
        unit, zone = timestamp
        return f'Annotated[pl.Datetime, {unit!r}, {zone!r}]'
    if type_name(arrow_type) == 'time64':
        return 'pl.Time'
    name = POLARS_NAMES.get(arrow_type)
    return f'pl.{name}' if name else None


def polars_instance(arrow_type: str) -> str | None:
    """The Polars type of a scalar Arrow type, written as an instance, such as `pl.Decimal(12, 2)`, else None."""
    decimal = decimal_of(arrow_type)
    if decimal is not None:
        return f'pl.Decimal({decimal[0]}, {decimal[1]})'
    timestamp = timestamp_of(arrow_type)
    if timestamp is not None:
        unit, zone = timestamp
        return f'pl.Datetime({unit!r}, {zone!r})' if zone else f'pl.Datetime({unit!r})'
    if type_name(arrow_type) == 'time64':
        return 'pl.Time()'
    name = POLARS_NAMES.get(arrow_type)
    return f'pl.{name}()' if name else None
