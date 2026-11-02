"""The Spark backend. Spark computes every statistic through the DataFrame API alone, so it runs over Spark Connect too.

Most statistics are one `agg` over every column. The values at rank k_min from each end come from one `mapInArrow`
pass in which each task keeps its k_min values nearest each end, merged on the driver. The steps between neighboring
rows come from a `mapInArrow` pass over each file split, stitched in the listing's order on the driver, because Spark
has no row order.
pyspark is imported only here, and doppeldata declares it nowhere.
"""

import contextlib
import dataclasses as dc
import datetime as dt
import importlib
import io
import math
import os
import socket
import time
import warnings
import zoneinfo
from collections.abc import Callable
from collections.abc import Iterator
from typing import Any
from urllib.parse import unquote
from urllib.parse import urlparse

import pyarrow as pa
import pyspark.sql.functions as sf
import pyspark.sql.types as st
from pyspark import StorageLevel
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import Window

from doppeldata import issue_codes
from doppeldata._core import VERSION
from doppeldata.errors import IncompatibleInstall
from doppeldata.errors import ProfilingWarning
from doppeldata.errors import SourceError
from doppeldata.profiling import parquet_footers
from doppeldata.profiling.backend import HIDDEN_COLUMN_PREFIX
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import ParsedType
from doppeldata.profiling.backend import Recode
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import SourceFormat
from doppeldata.profiling.backend import SourceRead
from doppeldata.profiling.expression_rule import ExpressionCandidate
from doppeldata.profiling.expression_rule import counted_rows
from doppeldata.profiling.sources import is_spark_frame
from doppeldata.profiling.statistic_catalog import ALL_STATISTICS
from doppeldata.profiling.statistic_catalog import BOOLEAN_TEXT
from doppeldata.profiling.statistic_catalog import DECIMAL_TEXT
from doppeldata.profiling.statistic_catalog import DIGITS_TEXT
from doppeldata.profiling.statistic_catalog import INTEGER_TEXT
from doppeldata.profiling.statistic_catalog import NUMERIC
from doppeldata.profiling.statistic_catalog import PADDED_DIGITS_TEXT
from doppeldata.profiling.statistic_catalog import RAW_TYPE_FOLDS
from doppeldata.profiling.statistic_catalog import RAW_TYPE_LAYOUTS
from doppeldata.profiling.statistic_catalog import RAW_TYPES
from doppeldata.profiling.statistic_catalog import READ_AS
from doppeldata.profiling.statistic_catalog import SHAPE_FROM
from doppeldata.profiling.statistic_catalog import SHAPE_TO
from doppeldata.profiling.statistic_catalog import TIME_UNITS
from doppeldata.profiling.statistic_catalog import ColumnKind
from doppeldata.profiling.statistic_catalog import FrequencyKey
from doppeldata.spark import PYTHON_ERROR_LINE
from doppeldata.spark_polars_arrow_types import spark_arrow_type

# HyperLogLog++'s relative standard deviation. At 0.01 each column's sketch holds 2^14 registers. A count of a few hundred
# falls in linear counting and lands within a few values of exact.
DISTINCT_RSD = 0.01
# Greenwald-Khanna's accuracy, whose reciprocal is the largest rank error a quantile may carry.
QUANTILE_ACCURACY = 10_000
MICROS_PER_DAY = 86_400_000_000
# The columns one compiled aggregate covers. Too many make its generated code pass the JVM's 64 KB method limit, and Spark
# then runs the query interpreted. docs/profiling-with-spark.md gives the split that was measured.
COLUMNS_PER_QUERY = 16
# The share of the hash range a sample keeps, as a multiple of the rows asked for. Rows each placed apart fall short of
# them with odds too small to meet, and a DataFrame's repeated rows, placed together, can.
OVERSAMPLE = 1.2
# Each setting the backend's queries run under, restored afterward. In UTC a timestamp's month, hour and whole units read
# as Polars reads a UTC column. CORRECTED reads a date the newer parser cannot read as null, where EXCEPTION raises.
# Spark compiles no query whose rows or aggregate buffers hold more than maxFields fields, and its default of 100 is
# passed by the moments of a few dozen columns, which then run interpreted at several times the cost.
SESSION_SETTINGS = {
    'spark.sql.session.timeZone': 'UTC',
    'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
    'spark.sql.codegen.maxFields': '10000',
}
# A Spark Connect server's port where `SPARK_REMOTE` names none, and the tries and seconds it is given to take a
# connection before the profile is refused.
CONNECT_PORT = 15002
CONNECT_ATTEMPTS = 3
CONNECT_TIMEOUT_SECONDS = 10
# Spark 3.5 has no try_cast on a column, and a cast with ANSI off gives null where a value does not parse.
ANSI_OFF = {'spark.sql.ansi.enabled': 'false'}
FILE_COLUMN = f'{HIDDEN_COLUMN_PREFIX}_file'
BLOCK_COLUMN = f'{HIDDEN_COLUMN_PREFIX}_block'
MALFORMED_COLUMN = f'{HIDDEN_COLUMN_PREFIX}_malformed'
PLACE_COLUMN = f'{HIDDEN_COLUMN_PREFIX}_place'
SPLIT_ROW_COLUMN = f'{HIDDEN_COLUMN_PREFIX}_split_row'
OFFSET_COLUMN = f'{HIDDEN_COLUMN_PREFIX}_offset'
FILE_ROW_COLUMN = f'{HIDDEN_COLUMN_PREFIX}_file_row'
LISTED_COLUMN = f'{HIDDEN_COLUMN_PREFIX}_listed'
# A string column's value has white space at an edge where its first or last character is any Unicode white space.
WHITESPACE_EDGE = r'(?U)^\s|\s$'
EPOCH = dt.datetime(1970, 1, 1)
# Each strftime layout the catalog tries, as Spark's datetime pattern. Spark parses no day-of-week name, so a layout
# opening with one has it cut off first. A letter count is ignored in parsing unless two fields touch.
SPARK_LAYOUTS = {
    '%Y-%m-%d': 'yyyy-MM-dd',
    '%Y/%m/%d': 'yyyy/MM/dd',
    '%Y%m%d': 'yyyyMMdd',
    '%m/%d/%Y': 'MM/dd/yyyy',
    '%d/%m/%Y': 'dd/MM/yyyy',
    '%m-%d-%Y': 'MM-dd-yyyy',
    '%d-%m-%Y': 'dd-MM-yyyy',
    '%d.%m.%Y': 'dd.MM.yyyy',
    '%m/%d/%y': 'MM/dd/yy',
    '%d %b %Y': 'dd MMM yyyy',
    '%d %B %Y': 'dd MMMM yyyy',
    '%b %d, %Y': 'MMM dd, yyyy',
    '%B %d, %Y': 'MMMM dd, yyyy',
    '%Y-%m-%dT%H:%M:%S%.f': "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]",
    '%Y-%m-%dT%H:%M:%S%.f%#z': "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]XXX",
    '%Y-%m-%d %H:%M:%S%.f': 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]',
    '%Y-%m-%d %H:%M:%S%.f%#z': 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]XXX',
    '%Y-%m-%d %H:%M': 'yyyy-MM-dd HH:mm',
    '%Y/%m/%d %H:%M:%S': 'yyyy/MM/dd HH:mm:ss',
    '%m/%d/%Y %H:%M:%S': 'MM/dd/yyyy HH:mm:ss',
    '%m/%d/%Y %H:%M': 'MM/dd/yyyy HH:mm',
    '%m/%d/%Y %I:%M:%S %p': 'MM/dd/yyyy hh:mm:ss a',
    '%d/%m/%Y %H:%M:%S': 'dd/MM/yyyy HH:mm:ss',
    '%a, %d %b %Y %H:%M:%S %z': 'dd MMM yyyy HH:mm:ss Z',
    '%b %d, %Y %H:%M:%S': 'MMM dd, yyyy HH:mm:ss',
}
WEEKDAY_PREFIX = r'^[A-Za-z]{3}, '
# The encoding DuckDB's reader names, as Spark's CSV reader names it.
SPARK_ENCODINGS = {'utf-8': 'UTF-8', 'latin-1': 'ISO-8859-1', 'utf-16': 'UTF-16'}
DELIMITED_DIALECT_MODULE = 'doppeldata.profiling.delimited_dialect'
DUCKDB_CONFIG: dict[str, str | bool | int | float | list[str]] = {'autoinstall_known_extensions': False, 'autoload_known_extensions': False}

unspelled = sorted({layout for layouts in RAW_TYPE_LAYOUTS.values() for layout in layouts} - SPARK_LAYOUTS.keys())
if unspelled:
    raise RuntimeError(f'the Spark backend has no datetime pattern for {", ".join(unspelled)}')


def column(name: str) -> Column:
    """A column by its name, which may hold a dot or a space."""
    return sf.col('`' + name.replace('`', '``') + '`')


def join_keys(table: DataFrame, names: list[str]) -> list[Column]:
    """Each key column as both sides of a join hold it, text as text and any integer as a bigint, named `key0`, `key1` and
    on, so both sides join on the same names."""
    return [
        (column(name).cast('bigint') if kind_of(table.schema[name].dataType) is ColumnKind.INTEGER else column(name)).alias(f'key{at}')
        for at, name in enumerate(names)
    ]


def count_where(condition: Column) -> Column:
    return sf.count(sf.when(condition, 1))


class SparkOps:
    """The operations `expression_rule.counted_rows` counts with, over Spark columns."""

    def arithmetic(self, operator: str, left: Column, right: Column) -> Column:
        return {'+': left + right, '-': left - right, '*': left * right}[operator]

    def quotient(self, left: Column, right: Column) -> Column:
        """Null where `right` is zero, as every engine holds it. Under ANSI mode the division itself would raise."""
        return sf.when(right != 0, left / right)

    def held(self, value: Column) -> Column:
        return value.isNotNull()

    def is_zero(self, value: Column) -> Column:
        return value == 0

    def within(self, value: Column, tolerance: float) -> Column:
        return sf.coalesce(sf.abs(value) <= tolerance, sf.lit(False))

    def both(self, first: Column, second: Column) -> Column:
        return first & second

    def negated(self, condition: Column) -> Column:
        return ~condition


@dc.dataclass(frozen=True)
class Operand:
    """One column as the statistics read it.

    `value` is what a numeric statistic reads: a decimal as a double, and a float with NaN and the infinities as null.
    `ordered` is what an extreme is taken of: the value, a timestamp's microseconds or a date's days since the epoch.
    """

    raw: Column
    spec: ColumnSpec | None
    major: int

    @property
    def kind(self) -> ColumnKind | None:
        return self.spec.kind if self.spec else None

    @property
    def value(self) -> Column:
        if self.kind is ColumnKind.DECIMAL:
            return self.raw.cast('double')
        if self.kind is ColumnKind.FLOAT:
            return sf.when(~sf.isnan(self.raw) & (sf.abs(self.raw) != float('inf')), self.raw)
        return self.raw

    @property
    def ordered(self) -> Column:
        if self.kind is ColumnKind.DATETIME:
            return sf.unix_micros(self.raw.cast('timestamp'))
        if self.kind is ColumnKind.DATE:
            return sf.unix_date(self.raw)
        return self.value

    def back(self, value: Any) -> Any:
        """A value of `ordered` as the profile reads it: a datetime or a date for a temporal column."""
        if value is None:
            return None
        if self.kind is ColumnKind.DATETIME:
            moment = EPOCH + dt.timedelta(microseconds=int(value))
            return moment.replace(tzinfo=zone_of(self.spec)) if self.spec and self.spec.timezone else moment
        if self.kind is ColumnKind.DATE:
            return EPOCH.date() + dt.timedelta(days=int(value))
        if self.kind is ColumnKind.INTEGER:
            return int(value)
        return float(value)


def epoch_micros(o: Operand) -> Column:
    """A date or timestamp as microseconds since the epoch."""
    return o.ordered * MICROS_PER_DAY if o.kind is ColumnKind.DATE else o.ordered


def zone_of(spec: ColumnSpec | None) -> dt.tzinfo:
    """The time zone a timestamp column's values are read in, which is UTC under the backend's session settings."""
    return zoneinfo.ZoneInfo(spec.timezone if spec and spec.timezone else 'UTC')


@dc.dataclass(frozen=True)
class Folded:
    """The aggregate columns one request reads, and how their values become its answer."""

    columns: list[Column]
    answer: Callable[[list[Any]], Any] = lambda values: values[0]


def single(aggregate: Column) -> Folded:
    return Folded([aggregate])


def counts(conditions: list[Column]) -> Folded:
    return Folded([count_where(condition) for condition in conditions], list)


def as_bigint(c: Column, major: int) -> Column:
    return c.try_cast('bigint') if major >= 4 else c.cast('bigint')


def as_double(c: Column, major: int) -> Column:
    return c.try_cast('double') if major >= 4 else c.cast('double')


def layout_has_offset(layout: str | None) -> bool:
    return bool(layout) and 'z' in (layout or '')


def parsed_timestamp(c: Column, layout: str | None) -> Column:
    """The text read through the layout as an instant in the session's UTC, or null where it does not read."""
    if layout and layout.startswith('%a, '):
        c = sf.regexp_replace(c, WEEKDAY_PREFIX, '')
    return sf.try_to_timestamp(c, sf.lit(SPARK_LAYOUTS[layout or '']))


def is_digits(c: Column) -> Column:
    return c.rlike(DIGITS_TEXT)


# Whether each raw value reads as the raw type, given the type's layout. Each is tried only on a value no earlier type in
# `RAW_TYPES` read.
TEXT_MATCHERS: dict[str, Callable[[Column, str | None, int], Column]] = {
    'integer': lambda c, _, major: c.rlike(INTEGER_TEXT) & as_bigint(c, major).isNotNull(),
    # A run of digits too long for an integer is an identifier, never a decimal.
    'decimal': lambda c, _, __: c.rlike(DECIMAL_TEXT) & ~is_digits(c),
    'boolean': lambda c, _, __: c.rlike(BOOLEAN_TEXT),
    'date': lambda c, layout, _: parsed_timestamp(c, layout).isNotNull() & ~is_digits(c),
    'timestamp': lambda c, layout, _: parsed_timestamp(c, layout).isNotNull() & ~is_digits(c),
}
TEXT_CONVERTERS: dict[str, Callable[[Column, str | None, int], Column]] = {
    'integer': lambda c, _, major: as_bigint(c, major),
    'decimal': lambda c, _, major: as_double(c, major),
    'boolean': lambda c, _, __: sf.lower(c) == 'true',
    'date': lambda c, layout, _: sf.to_date(parsed_timestamp(c, layout)),
    # A timestamp read with no offset is wall-clock time, as Polars reads it, and one with an offset an instant in UTC.
    'timestamp': lambda c, layout, _: (
        parsed_timestamp(c, layout) if layout_has_offset(layout) else parsed_timestamp(c, layout).cast('timestamp_ntz')
    ),
}


def raw_type_code(c: Column, layouts: tuple[tuple[str, str], ...], major: int) -> Column:
    """Each non-null value's index in `READ_AS` of the first raw type it reads as."""
    chosen = dict(layouts)
    tried = [raw for raw in RAW_TYPES if raw not in RAW_TYPE_LAYOUTS or raw in chosen]
    code: Column = sf.lit(READ_AS.index('string'))
    for raw in reversed(tried):
        code = sf.when(TEXT_MATCHERS[raw](c, chosen.get(raw), major), sf.lit(READ_AS.index(raw))).otherwise(code)
    return sf.when(c.isNotNull(), code)


def reads(c: Column, parsed_type: ParsedType, major: int) -> Column:
    """Whether each value parses as its column's type, or one the type folds in."""
    if parsed_type.json_type_column:
        raise SourceError(
            issue_codes.ENGINE_DOES_NOT_READ, 'engine spark profiles no JSON source; profile it with engine polars', value='json'
        )
    raw, layout = parsed_type.raw_type, parsed_type.format
    read = TEXT_MATCHERS[raw](c, layout, major)
    for folded in RAW_TYPE_FOLDS.get(raw, ()):
        read = read | TEXT_MATCHERS[folded](c, layout, major)
    return read


def parsed(c: Column, parsed_type: ParsedType, major: int) -> Column:
    """The column read as its type, with a value its counts gave another type as null."""
    if parsed_type.missing:
        c = sf.when(~c.isin(list(parsed_type.missing)), c)
    if parsed_type.raw_type == 'string':
        return c
    return sf.when(reads(c, parsed_type, major), TEXT_CONVERTERS[parsed_type.raw_type](c, parsed_type.format, major))


def column_chunks(requests: list[Request]) -> list[list[Request]]:
    """The requests split by column into groups of `COLUMNS_PER_QUERY` columns, a table-wide request counting as one."""
    columns = list(dict.fromkeys(request.column for request in requests))
    chunks = [set(columns[start : start + COLUMNS_PER_QUERY]) for start in range(0, len(columns), COLUMNS_PER_QUERY)]
    return [[request for request in requests if request.column in chunk] for chunk in chunks]


def histogram_counts(o: Operand, edges: tuple[float, ...]) -> Folded:
    """The rows below the first edge, in each bin from one edge up to the next with the last bin closed, and above the last."""
    v = o.value.cast('double')
    bins = [(v >= low) & (v < high) for low, high in zip(edges[:-2], edges[1:-1], strict=True)]
    return counts([v < edges[0], *bins, (v >= edges[-2]) & (v <= edges[-1]), v > edges[-1]])


def unparsed_rows(columns: tuple[tuple[str, ParsedType], ...], major: int) -> Folded:
    failing = [
        column(name).isNotNull() & ~reads(column(name), parsed_type, major)
        for name, parsed_type in columns
        if parsed_type.raw_type != 'string'
    ]
    if not failing:
        return single(sf.lit(0))
    # One function of every column's flag, where a chain of ors would nest a level per column, past the 100 levels a
    # Spark Connect plan's protobuf may nest.
    return single(count_where(sf.greatest(*failing) if len(failing) > 1 else failing[0]))


def whole(nanoseconds: int) -> Callable[[Operand, Any], Folded]:
    # Spark holds microseconds, so every value is a whole number of them.
    micros = nanoseconds // 1000
    return lambda o, _: Folded([sf.min((o.ordered % micros == 0).cast('int'))], lambda values: values[0] in (None, 1))


def decimal_places(o: Operand, most: int) -> Folded:
    """The fewest places, up to `most`, that every value equals itself rounded to, or None: the most any one value needs."""
    needed: Column = sf.lit(most + 1)
    for places in reversed(range(most + 1)):
        needed = sf.when(sf.round(o.value, places) == o.value, sf.lit(places)).otherwise(needed)
    return Folded(
        [sf.max(sf.when(o.value.isNotNull(), needed))],
        lambda values: 0 if values[0] is None else values[0] if values[0] <= most else None,
    )


def quantiles(c: Column, levels: tuple[float, ...]) -> Folded:
    return Folded(
        [sf.percentile_approx(c, list(levels), QUANTILE_ACCURACY)],
        lambda values: list(values[0]) if values[0] is not None else [None] * len(levels),
    )


Translation = Callable[[Operand, Any], Folded]

# Each statistic one aggregate over every column answers.
AGGREGATED: dict[str, Translation] = {
    'rows': lambda _, __: single(sf.count(sf.lit(1))),
    'null_count': lambda o, _: single(count_where(o.value.isNull())),
    'min': lambda o, _: Folded([sf.min(o.ordered)], lambda values: o.back(values[0])),
    'max': lambda o, _: Folded([sf.max(o.ordered)], lambda values: o.back(values[0])),
    'mean': lambda o, _: single(sf.mean(o.value)),
    'std': lambda o, _: single(sf.stddev_samp(o.value)),
    'skew': lambda o, _: single(sf.skewness(o.value)),
    'kurtosis': lambda o, _: single(sf.kurtosis(o.value)),
    'zero_fraction': lambda o, _: single(sf.mean((o.value == 0).cast('int'))),
    'negative_fraction': lambda o, _: single(sf.mean((o.value < 0).cast('int'))),
    'quantiles': lambda o, levels: quantiles(o.value, levels),
    'decimal_places': decimal_places,
    'histogram_counts': histogram_counts,
    'length_mean': lambda o, _: single(sf.mean(sf.length(o.raw))),
    'length_quantiles': lambda o, levels: quantiles(sf.length(o.raw), levels),
    'listed_value_counts': lambda o, values: counts([o.raw == sf.lit(value) for value in values]),
    'whitespace_edge_fraction': lambda o, _: single(sf.mean(o.raw.rlike(WHITESPACE_EDGE).cast('int'))),
    'true_fraction': lambda o, _: single(sf.mean(o.raw.cast('int'))),
    'month_counts': lambda o, _: counts([sf.month(o.raw) == month for month in range(1, 13)]),
    # Spark's dayofweek counts from Sunday as 1, and the catalog from Monday.
    'weekday_counts': lambda o, _: counts([(sf.dayofweek(o.raw) + 5) % 7 + 1 == day for day in range(1, 8)]),
    'hour_counts': lambda o, _: counts([sf.hour(o.raw) == hour for hour in range(24)]),
    **{f'whole_{unit}': whole(nanoseconds) for unit, nanoseconds, _ in TIME_UNITS},
    'padded_digits': lambda o, _: single(count_where(o.raw.rlike(PADDED_DIGITS_TEXT))),
    'unparsed_rows': lambda o, columns: unparsed_rows(columns, o.major),
}
# Each statistic taken from a column's k_min values nearest one end, as the key they are ordered by and whether the low end.
ORDERED: dict[str, Callable[[Operand], tuple[Column, bool]]] = {
    'trimmed_low': lambda o: (o.ordered, True),
    'trimmed_high': lambda o: (o.ordered, False),
    'length_trimmed_low': lambda o: (sf.length(o.raw), True),
    'length_trimmed_high': lambda o: (sf.length(o.raw), False),
}
# The steps between neighboring rows, from the split pass.
STEPPED = frozenset({'step_min', 'step_max'})
# Counted between a column's trimmed extremes, once the ordered query has found them.
BOUNDED = frozenset({'distinct_days'})
# Counted over every text column's values as one stream, so a layout tried on many columns is one expression in the plan.
TYPED = frozenset({'raw_type_counts'})
# Estimated by one HyperLogLog++ over every column's values as text, grouped by column. A sketch per column puts 2^14
# registers per column in the aggregate's buffer, which Spark spends tens of seconds compiling for a few dozen columns.
ESTIMATED = frozenset({'distinct_count'})
# Aggregated in their own query: a Greenwald-Khanna sketch keeps Spark from compiling any query holding it.
SKETCHED = frozenset({'quantiles', 'length_quantiles'})
# JSON paths are profiled by the Polars backend alone.
REFUSED = frozenset({'json_raw_type_counts'})
# Counted from one grouping of each tuple's hash, a null told apart from every value.
GROUPED = frozenset({'distinct_rows', 'unique_rows'})
# The correlations one aggregate over the ranked sample computes, kept under the compiled method's size as the columns are.
PAIRS_PER_QUERY = 128

untranslated = sorted(
    {statistic.name for statistic in ALL_STATISTICS}
    - (AGGREGATED.keys() | ORDERED.keys() | STEPPED | BOUNDED | TYPED | ESTIMATED | REFUSED | GROUPED)
)
if untranslated:
    raise RuntimeError(f'the Spark backend has no translation for {", ".join(untranslated)}')


def kind_of(data_type: st.DataType) -> ColumnKind:
    if isinstance(data_type, st.ByteType | st.ShortType | st.IntegerType | st.LongType):
        return ColumnKind.INTEGER
    if isinstance(data_type, st.FloatType | st.DoubleType):
        return ColumnKind.FLOAT
    if isinstance(data_type, st.DecimalType):
        return ColumnKind.DECIMAL
    if isinstance(data_type, st.StringType):
        return ColumnKind.STRING
    if isinstance(data_type, st.BooleanType):
        return ColumnKind.BOOLEAN
    if isinstance(data_type, st.DateType):
        return ColumnKind.DATE
    if isinstance(data_type, st.TimestampType | st.TimestampNTZType):
        return ColumnKind.DATETIME
    return ColumnKind.OTHER


def spec_of(field: st.StructField) -> ColumnSpec:
    data_type = field.dataType
    kind = kind_of(data_type)
    arrow_type = spark_arrow_type(data_type)
    if isinstance(data_type, st.DecimalType):
        return ColumnSpec(field.name, kind, arrow_type, precision=data_type.precision, scale=data_type.scale)
    if isinstance(data_type, st.TimestampType):
        return ColumnSpec(field.name, kind, arrow_type, timezone='UTC')
    return ColumnSpec(field.name, kind, arrow_type)


@dc.dataclass(frozen=True)
class ArrowBatches:
    """What each Spark task runs to hand its rows to the driver: each batch as one Arrow IPC stream.

    Spark 3.5 has no `toArrow`, and `toPandas` turns an integer column holding a null into floats. The driver reads
    these streams back with the types Spark's own Arrow conversion gave, on every Spark release and over Connect.
    """

    def __call__(self, batches: Iterator[Any]) -> Iterator[Any]:
        for batch in batches:
            sink = io.BytesIO()
            with pa.ipc.new_stream(sink, batch.schema) as writer:
                writer.write_batch(batch)
            yield pa.record_batch([pa.array([sink.getvalue()], pa.binary())], names=['batch'])


END_VALUES_SCHEMA = 'request int, value string'


@dc.dataclass(frozen=True)
class EndValues:
    """What each Spark task runs for the trimmed extremes: each key's k non-null values nearest one end, of the rows the
    task reads. The key is the batch's column at the request's index, and each value is sent as text, so one schema holds
    every type."""

    ends: tuple[tuple[int, bool], ...]

    def __call__(self, batches: Iterator[Any]) -> Iterator[Any]:
        compute = importlib.import_module('pyarrow.compute')
        held: list[Any] = [None] * len(self.ends)
        for batch in batches:
            for index, (k, low) in enumerate(self.ends):
                values = compute.drop_null(batch.column(index))
                if held[index] is not None:
                    values = pa.concat_arrays([held[index], values])
                if len(values) > k:
                    values = values.take((compute.bottom_k_unstable if low else compute.top_k_unstable)(values, k))
                held[index] = values
        rows = [(index, str(value)) for index, values in enumerate(held) if values is not None for value in values.to_pylist()]
        if rows:
            requests, texts = zip(*rows, strict=True)
            yield pa.record_batch([pa.array(requests, pa.int32()), pa.array(texts, pa.string())], names=['request', 'value'])


@dc.dataclass(frozen=True)
class DriverBuild:
    """What one Spark task runs to check its executor imports the doppeldata the driver runs, because each pass's code is
    pickled by reference and run as the executor's."""

    version: str

    def __call__(self, batches: Iterator[Any]) -> Iterator[Any]:
        for _ in batches:
            pass
        if self.version != VERSION:
            message = f'this executor runs doppeldata {VERSION}, and the driver {self.version}; install one build on both'
            raise IncompatibleInstall(issue_codes.BUILDS_DIFFER, message, value=VERSION)
        yield from ()


SPLIT_STEPS_SCHEMA = 'file string, block long, part int, column string, first long, last long, low long, high long'


@dc.dataclass(frozen=True)
class SplitSteps:
    """What each Spark task runs for the steps: each integer column's first and last value, and its least and greatest step
    between neighboring rows, within each file split the task reads in order.

    A split is a file and the byte its block starts at, or the task's partition where the rows carry no file. A step
    beside a null is null, as Polars' `diff` gives it.
    """

    columns: tuple[str, ...]
    keyed: bool

    def __call__(self, batches: Iterator[Any]) -> Iterator[Any]:
        compute = importlib.import_module('pyarrow.compute')
        task = importlib.import_module('pyspark').TaskContext.get()
        part = task.partitionId() if task else 0
        # Each split's key, and per column its first value, its last, and its least and greatest step.
        splits: dict[tuple[str | None, int | None], dict[str, list[Any]]] = {}
        for batch in batches:
            files = batch.column(FILE_COLUMN).to_pylist() if self.keyed else [None] * batch.num_rows
            blocks = batch.column(BLOCK_COLUMN).to_pylist() if self.keyed else [None] * batch.num_rows
            start = 0
            while start < batch.num_rows:
                key = (files[start], blocks[start])
                stop = start + 1
                while stop < batch.num_rows and (files[stop], blocks[stop]) == key:
                    stop += 1
                held = splits.setdefault(key, {})
                for name in self.columns:
                    values = batch.column(name).slice(start, stop - start).cast(pa.int64())
                    first, last = values[0].as_py(), values[-1].as_py()
                    steps = compute.min_max(compute.subtract(values[1:], values[:-1])) if len(values) > 1 else None
                    low = steps['min'].as_py() if steps else None
                    high = steps['max'].as_py() if steps else None
                    state = held.get(name)
                    if state is None:
                        held[name] = [first, last, low, high]
                        continue
                    if state[1] is not None and first is not None:
                        low, high = least(low, first - state[1]), greatest(high, first - state[1])
                    state[1], state[2], state[3] = last, least(state[2], low), greatest(state[3], high)
                start = stop
        rows = [(file, block, part, name, *state) for (file, block), held in splits.items() for name, state in held.items()]
        if rows:
            fields = list(zip(*rows, strict=True))
            types = [pa.string(), pa.int64(), pa.int32(), pa.string(), pa.int64(), pa.int64(), pa.int64(), pa.int64()]
            names = ['file', 'block', 'part', 'column', 'first', 'last', 'low', 'high']
            yield pa.record_batch([pa.array(values, kind) for values, kind in zip(fields, types, strict=True)], names=names)


def least(left: int | None, right: int | None) -> int | None:
    return right if left is None else left if right is None else min(left, right)


def greatest(left: int | None, right: int | None) -> int | None:
    return right if left is None else left if right is None else max(left, right)


def listed_path(path: str) -> str:
    """A file's path as Spark names it, as the listing names it: a local path, or an s3:// URL whatever S3 scheme read it."""
    parsed_url = urlparse(path)
    if parsed_url.scheme in ('', 'file'):
        return unquote(parsed_url.path)
    if parsed_url.scheme in ('s3', 's3a', 's3n'):
        return f's3://{parsed_url.netloc}{unquote(parsed_url.path)}'
    return path


def session_for(data: Any, spark: SparkSession | None) -> SparkSession:
    """The session named, a DataFrame's own, the active one, or one the builder starts: over Connect where `SPARK_REMOTE`
    names a server, else on `local[*]`."""
    if spark is not None:
        return spark
    if is_spark_frame(data):
        frame_session: SparkSession = data.sparkSession
        return frame_session
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    remote = os.environ.get('SPARK_REMOTE')
    if remote:
        refuse_silent_server(remote)
    return SparkSession.builder.getOrCreate()


def refuse_silent_server(remote: str) -> None:
    """Refuse a Spark Connect server that takes no connection in `CONNECT_ATTEMPTS` tries with a SourceError at exit 4,
    where the client's own retries would wait about ten minutes before failing."""
    parsed_url = urlparse(remote)
    if parsed_url.scheme != 'sc' or not parsed_url.hostname:
        return
    host, port = parsed_url.hostname, parsed_url.port or CONNECT_PORT
    failure: OSError | None = None
    for attempt in range(CONNECT_ATTEMPTS):
        if attempt:
            time.sleep(attempt)
        try:
            socket.create_connection((host, port), timeout=CONNECT_TIMEOUT_SECONDS).close()
            return
        except OSError as error:
            failure = error
    message = f'no answer from the Spark Connect server at {host}:{port}, which SPARK_REMOTE names, in {CONNECT_ATTEMPTS} tries: {failure}'
    raise SourceError(issue_codes.NO_ANSWER, message, value=f'{host}:{port}')


class SparkBackend:
    name = 'spark'

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.session_version: str | None = spark.version
        self.major = int(spark.version.split('.')[0])
        # Each table's row count once counted, by the table's identity, holding the table so the identity is not reused.
        self.rows: dict[int, tuple[DataFrame, int]] = {}
        self.persisted: list[DataFrame] = []
        self.parsed: list[DataFrame] = []
        # Each file's place in its source's listing, which orders the splits whose steps are stitched.
        self.listing: dict[str, int] = {}

    @contextlib.contextmanager
    def profiling(self) -> Iterator[None]:
        """Run inside the session settings the translations assume, on executors that run this doppeldata, then restore
        the settings and release what was persisted.

        The settings are set on the session itself, so its other queries run under them until the profile ends. A setting
        or frame that cannot be restored or released is noted on the error the profile raised, and warned of where it
        raised none.
        """
        settings = SESSION_SETTINGS | (ANSI_OFF if self.major < 4 else {})
        before: dict[str, str | None] = {}
        try:
            for key, value in settings.items():
                before[key] = self.spark.conf.get(key, None)
                self.spark.conf.set(key, value)
            self.refuse_unlike_executors()
            yield
        except BaseException as error:
            for failure in self.cleaned_up(before):
                error.add_note(failure)
            raise
        for failure in self.cleaned_up(before):
            warnings.warn(failure, ProfilingWarning, stacklevel=3)

    def cleaned_up(self, before: dict[str, str | None]) -> list[str]:
        """Restore each setting and release each persisted frame, each apart, and say what could not be."""
        failures = []
        for key, value in before.items():
            try:
                if value is None:
                    self.spark.conf.unset(key)
                else:
                    self.spark.conf.set(key, value)
            except Exception as error:
                failures.append(f'the session setting {key} could not be restored to {value}: {error}')
        for frame in self.persisted:
            try:
                frame.unpersist()
            except Exception as error:
                failures.append(f'a frame persisted on disk could not be released: {error}')
        self.persisted.clear()
        self.parsed.clear()
        return failures

    def refuse_unlike_executors(self) -> None:
        """Refuse a session whose executors cannot run the passes this driver pickles, as one on another Python minor or
        another doppeldata, before any source is read."""
        try:
            self.spark.range(1, numPartitions=1).mapInArrow(DriverBuild(VERSION), 'version string').collect()
        except Exception as error:
            causes = PYTHON_ERROR_LINE.findall(str(error))
            cause = ': '.join(causes[-1]) if causes else next(iter(str(error).strip().splitlines()), type(error).__name__)
            raise SourceError(issue_codes.SPARK_PASSES_FAILED, f'the Spark executors cannot run the profiling passes: {cause}') from error

    def scan(self, source: Source) -> DataFrame:
        return self.read(source).table

    def read(self, source: Source) -> SourceRead:
        if source.table is not None:
            if not is_spark_frame(source.table):
                message = 'engine spark profiles a Spark DataFrame, files or an s3:// prefix; profile an in-memory table with engine polars'
                raise SourceError(issue_codes.ENGINE_DOES_NOT_READ, message, value=type(source.table).__name__)
            return SourceRead(source.table)
        self.listing = {listed_path(file.path): index for index, file in enumerate(source.files)}
        match source.format:
            case SourceFormat.PARQUET:
                read = self.read_parquet(source)
            case SourceFormat.CSV | SourceFormat.TSV:
                read = self.read_delimited(source)
            case _:
                format_name = source.format.value if source.format else 'a table'
                message = f'engine spark reads Parquet, CSV and TSV, and {format_name} is none of them; profile it with engine polars'
                raise SourceError(issue_codes.ENGINE_DOES_NOT_READ, message, value=format_name)
        return read

    def read_parquet(self, source: Source) -> SourceRead:
        """Parquet read as one table. Files holding different schemas, as a drifted dataset's do, are read a schema at a time
        and joined by name, which widens a column to the widest type a file stores it as and leaves it null in a file that
        lacks it, as the Polars backend reads them. Spark's `mergeSchema` refuses an int32 column widened to int64.
        """
        every = parquet_footers.read_footers(source.files)
        footers = parquet_footers.sampled_footers(source.files, every)
        if all(footer.schema.equals(every[0].schema) for footer in every):
            frame = self.with_split_columns(self.reader(source).parquet(*(file.path for file in source.files)))
            return SourceRead(self.with_partition_types(frame, source), footers=footers)
        groups: list[tuple[pa.Schema, list[str]]] = []
        for file, footer in zip(source.files, every, strict=True):
            group = next((paths for schema, paths in groups if footer.schema.equals(schema)), None)
            if group is None:
                groups.append((footer.schema, [file.path]))
            else:
                group.append(file.path)
        try:
            pa.unify_schemas([schema for schema, _ in groups], promote_options='permissive')
        except pa.ArrowException as error:
            raise SourceError(issue_codes.DATA_UNREADABLE, f'{", ".join(source.roots)} could not be read as Parquet: {error}') from error
        frames = [self.with_split_columns(self.reader(source).parquet(*paths)) for _, paths in groups]
        frame = frames[0]
        for later in frames[1:]:
            frame = frame.unionByName(later, allowMissingColumns=True)
        return SourceRead(self.with_partition_types(frame, source), footers=footers)

    def reader(self, source: Source) -> Any:
        """Spark's reader, finding Hive keys below the root that was named, as the listing does."""
        root = source.roots[0] if len(source.roots) == 1 else None
        named_file = root is not None and len(source.files) == 1 and listed_path(source.files[0].path) == listed_path(root)
        return self.spark.read.option('basePath', root) if root and not named_file else self.spark.read

    def with_split_columns(self, frame: DataFrame) -> DataFrame:
        """The scan with each row's file and the byte its split starts at, which order the steps as the listing does."""
        return frame.select('*', sf.col('_metadata.file_path').alias(FILE_COLUMN), sf.col('_metadata.file_block_start').alias(BLOCK_COLUMN))

    def with_partition_types(self, frame: DataFrame, source: Source, *, text: bool = False) -> DataFrame:
        """Each Hive key typed as the Polars backend types it: as text in a text source, and an integer as int64."""
        keys = [key for key in source.partition_keys if key in frame.columns]
        retyped = {}
        for key in keys:
            data_type = frame.schema[key].dataType
            if text and not isinstance(data_type, st.StringType):
                retyped[key] = column(key).cast('string')
            elif not text and isinstance(data_type, st.ByteType | st.ShortType | st.IntegerType):
                retyped[key] = column(key).cast('bigint')
        return frame.withColumns(retyped) if retyped else frame

    def read_delimited(self, source: Source) -> SourceRead:
        """CSV or TSV read in the detected dialect with every column as text. A row whose fields do not split into the
        header's is left out and counted, as DuckDB rejects it."""
        found = importlib.import_module(DELIMITED_DIALECT_MODULE).detected(
            source.files, '\t' if source.format is SourceFormat.TSV else None, DUCKDB_CONFIG, source.dialect
        )
        dialect = found.dialect
        schema = st.StructType([st.StructField(name, st.StringType()) for name in dialect.names])
        schema = schema.add(MALFORMED_COLUMN, st.StringType())
        options = {
            'sep': dialect.delimiter,
            'quote': dialect.quote,
            'escape': dialect.escape,
            'header': dialect.header,
            'encoding': SPARK_ENCODINGS.get(dialect.encoding, dialect.encoding),
            'mode': 'PERMISSIVE',
            'columnNameOfCorruptRecord': MALFORMED_COLUMN,
            'enforceSchema': True,
        }
        raw = self.reader(source).schema(schema).options(**options).csv([file.path for file in source.files])
        raw = self.with_partition_types(self.with_split_columns(raw), source, text=True)
        # Spark refuses a query of a raw CSV reading only its malformed column, and every pass reads the text again.
        raw = self.persist(raw)
        fields = sf.size(sf.split(column(MALFORMED_COLUMN), regex_quoted(dialect.delimiter)))
        reason = (
            sf.when(fields < len(dialect.names), 'missing_columns')
            .when(fields > len(dialect.names), 'too_many_columns')
            .otherwise('unquoted_value')
        )
        rejected = {
            row['reason']: row['count']
            for row in raw.where(column(MALFORMED_COLUMN).isNotNull()).groupBy(reason.alias('reason')).count().collect()
        }
        table = raw.where(column(MALFORMED_COLUMN).isNull()).drop(MALFORMED_COLUMN)
        by_file = {listed_path(row['file']): row['count'] for row in table.groupBy(column(FILE_COLUMN).alias('file')).count().collect()}
        file_rows = tuple(by_file.get(listed_path(file.path), 0) for file in source.files)
        return SourceRead(table, dialect=found, rejected=rejected, file_rows=file_rows)

    def persist(self, frame: DataFrame) -> DataFrame:
        # On disk, read back through the page cache. In memory, Spark unrolls a block of a wide table's batches before it
        # checks the heap, and a few tasks at once exhaust it.
        frame = frame.persist(StorageLevel.DISK_ONLY)
        self.persisted.append(frame)
        return frame

    def schema(self, table: DataFrame) -> list[ColumnSpec]:
        return [spec_of(field) for field in table.schema.fields]

    def aggregate(self, table: DataFrame, requests: list[Request]) -> dict[Request, Any]:
        if not requests:
            return {}
        specs = {spec.name: spec for spec in self.schema(table)}

        def operand(request: Request) -> Operand:
            if request.column is None:
                return Operand(sf.lit(None), None, self.major)
            return Operand(column(request.column), specs[request.column], self.major)

        answers: dict[Request, Any] = {}
        refused = [request for request in requests if request.statistic in REFUSED]
        if refused:
            raise SourceError(
                issue_codes.ENGINE_DOES_NOT_READ, 'engine spark profiles no JSON source; profile it with engine polars', value='json'
            )
        compiled = [request for request in requests if request.statistic in AGGREGATED and request.statistic not in SKETCHED]
        sketched = [request for request in requests if request.statistic in SKETCHED]
        for aggregated in [*column_chunks(compiled), sketched]:
            if not aggregated:
                continue
            folded = [AGGREGATED[request.statistic](operand(request), request.argument) for request in aggregated]
            row = table.agg(*(aggregate for fold in folded for aggregate in fold.columns)).collect()[0]
            position = 0
            for request, fold in zip(aggregated, folded, strict=True):
                answers[request] = fold.answer(list(row[position : position + len(fold.columns)]))
                position += len(fold.columns)
        if Request(None, 'rows') in answers:
            self.rows[id(table)] = (table, answers[Request(None, 'rows')])
        answers |= self.distinct_counts(table, [request for request in requests if request.statistic in ESTIMATED], operand)
        ordered = [request for request in requests if request.statistic in ORDERED]
        bounded = [request for request in requests if request.statistic in BOUNDED]
        bounds = [Request(request.column, name, request.argument) for request in bounded for name in ('trimmed_low', 'trimmed_high')]
        extremes = self.ordered(table, [*ordered, *(bound for bound in bounds if bound not in ordered)], operand)
        answers |= {request: extremes[request] for request in ordered}
        answers |= self.distinct_days(table, bounded, extremes, operand)
        stepped = [request for request in requests if request.statistic in STEPPED]
        answers |= self.steps(table, stepped)
        answers |= self.type_counts(table, [request for request in requests if request.statistic in TYPED])
        answers |= self.tuple_counts(table, [request for request in requests if request.statistic in GROUPED])
        return answers

    def tuple_counts(self, table: DataFrame, requests: list[Request]) -> dict[Request, Any]:
        """Each tuple's distinct values and the rows holding a value no other row holds, from one grouping of every tuple's
        xxhash64. Each column is hashed beside whether it is null, because the hash skips a null."""
        if not requests:
            return {}
        tuples = list(dict.fromkeys(request.argument for request in requests))
        hashes = [sf.xxhash64(*(part for name in names for part in (column(name), column(name).isNull()))) for names in tuples]
        keyed = sf.inline(sf.array(*(sf.struct(sf.lit(index).alias('n'), hashed.alias('h')) for index, hashed in enumerate(hashes))))
        groups = table.select(keyed).groupBy('n', 'h').count()
        counted = groups.groupBy('n').agg(sf.count(sf.lit(1)).alias('distinct'), count_where(sf.col('count') == 1).alias('unique'))
        found = {row['n']: (row['distinct'], row['unique']) for row in counted.collect()}
        return {
            request: found.get(tuples.index(request.argument), (0, 0))[0 if request.statistic == 'distinct_rows' else 1]
            for request in requests
        }

    def average_ranks(self, table: DataFrame, names: list[str]) -> list[Column]:
        """Each named column's values as their ranks by one window over the table, a tie at the mean of its first and
        last rank, aliased `r0`, `r1` and on. A null sorts first and is ranked too, which moves every value's rank alike
        and leaves a correlation or a share of variance as it is. Its own rank is null."""
        specs = {spec.name: spec for spec in self.schema(table)}
        rows = self.row_count(table)
        ranks = []
        for index, name in enumerate(names):
            value = Operand(column(name), specs[name], self.major).ordered
            order = Window.orderBy(value.asc_nulls_first())
            mean_rank = (sf.rank().over(order) + sf.round(sf.cume_dist().over(order) * rows)) / 2
            ranks.append(sf.when(value.isNotNull(), mean_rank).cast('double').alias(f'r{index}'))
        return ranks

    def rank_correlations(self, table: DataFrame, pairs: list[tuple[str, str]]) -> list[tuple[float | None, int]]:
        """Each pair's ρ from the count, sums, squares and product of its ranks where both are present, taken on the
        driver. Twice a rank less the middle one is a whole number within ±rows, so below about 200,000 rows every sum
        is exact in a double, and a constant column's variance is exactly zero rather than a division ANSI mode raises."""
        if not pairs:
            return []
        names = list(dict.fromkeys(name for pair in pairs for name in pair))
        twice_middle = self.row_count(table) + 1
        ranked = self.persist(table.select(*self.average_ranks(table, names)))
        centered = ranked.select(*((2 * sf.col(f'r{index}') - twice_middle).alias(f'r{index}') for index in range(len(names))))
        found: list[tuple[float | None, int]] = []
        for start in range(0, len(pairs), PAIRS_PER_QUERY):
            measured = []
            for a, b in pairs[start : start + PAIRS_PER_QUERY]:
                first, second = sf.col(f'r{names.index(a)}'), sf.col(f'r{names.index(b)}')
                both = first.isNotNull() & second.isNotNull()
                x, y = sf.when(both, first), sf.when(both, second)
                measured += [count_where(both), sf.sum(x), sf.sum(y), sf.sum(x * x), sf.sum(y * y), sf.sum(x * y)]
            row = [int(value or 0) for value in centered.agg(*measured).collect()[0]]
            for at in range(0, len(row), 6):
                held, sx, sy, sxx, syy, sxy = row[at : at + 6]
                spread_x, spread_y = held * sxx - sx * sx, held * syy - sy * sy
                value = (held * sxy - sx * sy) / (math.sqrt(spread_x) * math.sqrt(spread_y)) if spread_x and spread_y else None
                found.append((value, held))
        return found

    def cross_counts(self, table: DataFrame, pairs: list[tuple[str, str]]) -> list[list[tuple[str | None, str | None, int]]]:
        """Every pair's values as one stream of (pair, value, value), grouped once."""
        if not pairs:
            return []
        keyed = sf.inline(
            sf.array(
                *(
                    sf.struct(sf.lit(index).alias('n'), column(a).cast('string').alias('a'), column(b).cast('string').alias('b'))
                    for index, (a, b) in enumerate(pairs)
                )
            )
        )
        found: list[list[tuple[str | None, str | None, int]]] = [[] for _ in pairs]
        for row in table.select(keyed).groupBy('n', 'a', 'b').count().collect():
            found[row['n']].append((row['a'], row['b'], row['count']))
        return found

    def rank_moments(self, table: DataFrame, pairs: list[tuple[str, str]]) -> list[list[tuple[int, float, float]]]:
        """Each numeric column's `average_ranks`, then every categorical column's values as one stream of (column, value),
        each row carrying the ranks, grouped once."""
        if not pairs:
            return []
        categoricals = list(dict.fromkeys(categorical for _, categorical in pairs))
        numerics = list(dict.fromkeys(numeric for numeric, _ in pairs))
        texts = [column(name).cast('string').alias(f'c{index}') for index, name in enumerate(categoricals)]
        ranked = self.persist(table.select(*texts, *self.average_ranks(table, numerics)))
        keyed = sf.inline(
            sf.array(*(sf.struct(sf.lit(index).alias('n'), sf.col(f'c{index}').alias('v')) for index in range(len(categoricals))))
        )
        moments = [
            moment for index in range(len(numerics)) for moment in (sf.count(f'r{index}'), sf.mean(f'r{index}'), sf.var_pop(f'r{index}'))
        ]
        held = [sf.col(f'r{index}') for index in range(len(numerics))]
        by_column: dict[int, list[Any]] = {}
        for row in ranked.select(keyed, *held).where(sf.col('v').isNotNull()).groupBy('n', 'v').agg(*moments).collect():
            by_column.setdefault(row[0], []).append(row)
        found = []
        for numeric, categorical in pairs:
            at = 2 + 3 * numerics.index(numeric)
            groups = by_column.get(categoricals.index(categorical), [])
            found.append([(int(row[at]), float(row[at + 1]), float(row[at + 2])) for row in groups if row[at]])
        return found

    def dependency_counts(self, table: DataFrame, lhs: str, rhs: list[str], k_min: int) -> list[tuple[int, int, int]]:
        if not rhs:
            return []
        held = [sf.count(column(name)).alias(f'n{index}') for index, name in enumerate(rhs)]
        varied = [(sf.min(column(name)) != sf.max(column(name))).alias(f'v{index}') for index, name in enumerate(rhs)]
        groups = table.where(column(lhs).isNotNull()).groupBy(column(lhs)).agg(*held, *varied)
        floored = [sf.col(f'n{index}') >= k_min for index in range(len(rhs))]
        totals = groups.agg(
            *(sf.sum(f'n{index}') for index in range(len(rhs))),
            *(sf.sum(sf.when(floor, sf.col(f'n{index}'))) for index, floor in enumerate(floored)),
            *(sf.sum(sf.when(floor & sf.col(f'v{index}'), sf.col(f'n{index}'))) for index, floor in enumerate(floored)),
        )
        row = [int(value or 0) for value in totals.collect()[0]]
        return [(row[index], row[len(rhs) + index], row[2 * len(rhs) + index]) for index in range(len(rhs))]

    def grouped_quantiles(
        self, table: DataFrame, requests: list[tuple[str, str, list[str], tuple[float, ...]]]
    ) -> list[list[tuple[str | None, int, list[float]]]]:
        """Requests naming one given and keeping the same values share one `groupBy`, `PAIRS_PER_QUERY` at a time."""
        specs = {spec.name: spec for spec in self.schema(table)}
        batches: dict[tuple[str, tuple[str, ...]], list[int]] = {}
        for at, (_, given, listed, _) in enumerate(requests):
            batches.setdefault((given, tuple(listed)), []).append(at)
        found: list[list[tuple[str | None, int, list[float]]]] = [[] for _ in requests]
        for (given, keep), batch in batches.items():
            text = column(given).cast('string')
            for start in range(0, len(batch), PAIRS_PER_QUERY):
                indexes = batch[start : start + PAIRS_PER_QUERY]
                measured = []
                for at in indexes:
                    child, _, _, levels = requests[at]
                    value = Operand(column(child), specs[child], self.major).value
                    measured += [
                        sf.count(value).alias(f'n{at}'),
                        sf.percentile_approx(value, list(levels), QUANTILE_ACCURACY).alias(f'q{at}'),
                    ]
                groups = (
                    table.where(column(given).isNotNull()).groupBy(sf.when(text.isin(list(keep)), text).alias('g')).agg(*measured).collect()
                )
                for at in indexes:
                    found[at] = [(row['g'], int(row[f'n{at}']), [float(q) for q in row[f'q{at}']]) for row in groups if row[f'n{at}'] > 0]
        return found

    def grouped_rates(self, table: DataFrame, pairs: list[tuple[str, str]]) -> list[list[tuple[str | None, int, int, int]]]:
        specs = {spec.name: spec for spec in self.schema(table)}

        def zeros(name: str) -> Column:
            return count_where(column(name) == 0) if specs[name].kind in NUMERIC else sf.lit(0)

        by_given: dict[str, list[int]] = {}
        for at, (given, _) in enumerate(pairs):
            by_given.setdefault(given, []).append(at)
        found: list[list[tuple[str | None, int, int, int]]] = [[] for _ in pairs]
        for given, indexes in by_given.items():
            counted = (
                table.groupBy(column(given).cast('string').alias('g'))
                .agg(
                    sf.count(sf.lit(1)).alias('n'),
                    *(count_where(column(pairs[at][1]).isNull()).alias(f'u{at}') for at in indexes),
                    *(zeros(pairs[at][1]).alias(f'z{at}') for at in indexes),
                )
                .collect()
            )
            for at in indexes:
                found[at] = [(row['g'], int(row['n']), int(row[f'u{at}']), int(row[f'z{at}'] or 0)) for row in counted]
        return found

    def ordered_shares(self, table: DataFrame, pairs: list[tuple[str, str]]) -> list[tuple[int, int]]:
        specs = {spec.name: spec for spec in self.schema(table)}
        found: list[tuple[int, int]] = []
        for start in range(0, len(pairs), PAIRS_PER_QUERY):
            measured = []
            for first, second in pairs[start : start + PAIRS_PER_QUERY]:
                earlier, later = (epoch_micros(Operand(column(name), specs[name], self.major)) for name in (first, second))
                both = earlier.isNotNull() & later.isNotNull()
                measured += [count_where(both), count_where(both & (later >= earlier))]
            row = table.agg(*measured).collect()[0]
            found += [(int(held or 0), int(ordered or 0)) for held, ordered in zip(row[::2], row[1::2], strict=True)]
        return found

    def lag_quantiles(
        self, table: DataFrame, pairs: list[tuple[str, str]], levels: tuple[float, ...], k_min: int
    ) -> list[tuple[int, list[float], float | None, float | None]]:
        """The quantiles from one `agg`, and the values at rank k_min from each end from one pass of `EndValues`."""
        if not pairs:
            return []
        specs = {spec.name: spec for spec in self.schema(table)}
        lags = [
            (
                epoch_micros(Operand(column(second), specs[second], self.major))
                - epoch_micros(Operand(column(first), specs[first], self.major))
            )
            for first, second in pairs
        ]
        measured = [aggregate for lag in lags for aggregate in (sf.count(lag), sf.percentile_approx(lag, list(levels), QUANTILE_ACCURACY))]
        row = table.agg(*measured).collect()[0]
        keys = [lag.alias(f'{HIDDEN_COLUMN_PREFIX}_end_{index}') for index, lag in enumerate(lag for lag in lags for _ in range(2))]
        ends = tuple((k_min, low) for _ in lags for low in (True, False))
        nearest: dict[int, list[int]] = {}
        for end in table.select(*keys).mapInArrow(EndValues(ends), END_VALUES_SCHEMA).collect():
            nearest.setdefault(end['request'], []).append(int(end['value']))
        found = []
        for index in range(len(pairs)):
            held, at = int(row[2 * index] or 0), row[2 * index + 1]
            quantiles = [float(value) for value in at] if held and at is not None else []
            low, high = sorted(nearest.get(2 * index, [])), sorted(nearest.get(2 * index + 1, []))
            ends_held = held >= k_min and len(low) >= k_min and len(high) >= k_min
            found.append((held, quantiles, float(low[k_min - 1]) if ends_held else None, float(high[-k_min]) if ends_held else None))
        return found

    def expression_matches(self, table: DataFrame, candidates: list[ExpressionCandidate]) -> list[tuple[int, int, int]]:
        specs = {spec.name: spec for spec in self.schema(table)}

        def value(name: str) -> Column:
            return Operand(column(name), specs[name], self.major).value.cast('double')

        found: list[tuple[int, int, int]] = []
        for start in range(0, len(candidates), PAIRS_PER_QUERY):
            measured = []
            for candidate in candidates[start : start + PAIRS_PER_QUERY]:
                measured += [count_where(rows) for rows in counted_rows(candidate, value, SparkOps())]
            row = table.agg(*measured).collect()[0]
            found += [(int(row[at] or 0), int(row[at + 1] or 0), int(row[at + 2] or 0)) for at in range(0, len(row), 3)]
        return found

    def foreign_key_counts(
        self, child: DataFrame, child_columns: list[str], parent: DataFrame, parent_columns: list[str]
    ) -> list[tuple[bool, int, int]]:
        keys = [f'key{at}' for at in range(len(child_columns))]
        children = (
            child.select(*join_keys(child, child_columns)).dropna(subset=keys).groupBy(*keys).agg(sf.count(sf.lit(1)).alias('children'))
        )
        parents = parent.select(*join_keys(parent, parent_columns)).dropna(subset=keys).distinct().withColumn('parent', sf.lit(True))
        joined = parents.join(children, on=keys, how='full')
        held = sf.coalesce(sf.col('parent'), sf.lit(False)).alias('parent')
        counted = joined.groupBy(held, sf.coalesce(sf.col('children'), sf.lit(0)).alias('children')).count().collect()
        return [(bool(row['parent']), int(row['children']), int(row['count'])) for row in counted]

    def inherited_counts(
        self, child: DataFrame, child_columns: list[str], parent: DataFrame, parent_columns: list[str], pairs: list[tuple[str, str]]
    ) -> tuple[int, list[int]]:
        keys = [f'key{at}' for at in range(len(child_columns))]
        compared_child = sorted({name for name, _ in pairs})
        compared_parent = sorted({name for _, name in pairs})
        children = child.select(
            *join_keys(child, child_columns), *(column(name).alias(f'c_{at}') for at, name in enumerate(compared_child))
        )
        parents = parent.select(
            *join_keys(parent, parent_columns), *(column(name).alias(f'p_{at}') for at, name in enumerate(compared_parent))
        )
        joined = children.dropna(subset=keys).join(parents.dropna(subset=keys).dropDuplicates(keys), on=keys, how='inner')
        equal = [
            sf.sum(sf.col(f'c_{compared_child.index(a)}').eqNullSafe(sf.col(f'p_{compared_parent.index(b)}')).cast('bigint')).alias(
                f'equal_{at}'
            )
            for at, (a, b) in enumerate(pairs)
        ]
        row = joined.agg(sf.count(sf.lit(1)).alias('joined'), *equal).collect()[0]
        return int(row['joined']), [int(row[f'equal_{at}'] or 0) for at in range(len(pairs))]

    def ordinal_counts(self, child: DataFrame, child_columns: list[str], candidates: list[str]) -> tuple[int, list[int]]:
        keys = [f'key{at}' for at in range(len(child_columns))]
        per_key = (
            child.select(*join_keys(child, child_columns), *(column(name).alias(f'c_{at}') for at, name in enumerate(candidates)))
            .dropna(subset=keys)
            .groupBy(*keys)
            .agg(
                sf.count(sf.lit(1)).alias('rows'),
                *(sf.min(f'c_{at}').alias(f'least_{at}') for at in range(len(candidates))),
                *(sf.max(f'c_{at}').alias(f'greatest_{at}') for at in range(len(candidates))),
                *(sf.count_distinct(f'c_{at}').alias(f'distinct_{at}') for at in range(len(candidates))),
            )
        )
        numbered = [
            sf.sum(
                (
                    (sf.col(f'least_{at}') == 1)
                    & (sf.col(f'greatest_{at}') == sf.col('rows'))
                    & (sf.col(f'distinct_{at}') == sf.col('rows'))
                ).cast('bigint')
            ).alias(f'numbered_{at}')
            for at in range(len(candidates))
        ]
        row = per_key.agg(sf.count(sf.lit(1)).alias('keys'), *numbered).collect()[0]
        return int(row['keys']), [int(row[f'numbered_{at}'] or 0) for at in range(len(candidates))]

    def distinct_counts(self, table: DataFrame, requests: list[Request], operand: Callable[[Request], Operand]) -> dict[Request, Any]:
        """Each column's distinct non-null values, estimated over each value as text, so one stream holds every type.

        A float's NaN is a value, as Polars counts it.
        """
        if not requests:
            return {}
        texts = (operand(request).raw.cast('string') for request in requests)
        pairs = sf.array(*(sf.struct(sf.lit(index).alias('n'), text.alias('v')) for index, text in enumerate(texts)))
        counted = table.select(sf.inline(pairs)).groupBy('n').agg(sf.approx_count_distinct('v', DISTINCT_RSD).alias('distinct'))
        by_request = {row['n']: row['distinct'] for row in counted.collect()}
        return {request: by_request.get(index, 0) for index, request in enumerate(requests)}

    def type_counts(self, table: DataFrame, requests: list[Request]) -> dict[Request, Any]:
        """Each text column's values counted by the first raw type each reads as, through its layouts.

        The columns become one stream of (column, value) pairs, and each distinct pair is coded once, weighted by its rows,
        because a text column repeats its values. Each distinct set of layouts is one code in the plan, read only on the
        pairs of the columns that asked for it, which keeps the plan small enough for Spark to compile however many columns
        and layouts are tried.
        """
        if not requests:
            return {}
        names = list(dict.fromkeys(request.column for request in requests if request.column is not None))
        arguments = list(dict.fromkeys(request.argument for request in requests))
        pairs = table.select(
            sf.inline(
                sf.array(*(sf.struct(sf.lit(index).alias('n'), column(name).cast('string').alias('v')) for index, name in enumerate(names)))
            )
        )
        distinct = pairs.where(sf.col('v').isNotNull()).groupBy('n', 'v').count()
        codes = []
        for argument in arguments:
            asking = [names.index(request.column) for request in requests if request.argument == argument and request.column is not None]
            codes.append(sf.when(sf.col('n').isin(asking), raw_type_code(sf.col('v'), argument, self.major)))
        coded = distinct.select('n', 'count', *(code.alias(f'c{index}') for index, code in enumerate(codes)))
        counted = coded.groupBy('n').agg(
            *(
                sf.sum(sf.when(sf.col(f'c{index}') == value, sf.col('count')))
                for index in range(len(arguments))
                for value in range(len(READ_AS))
            )
        )
        by_column = {row['n']: [count or 0 for count in list(row)[1:]] for row in counted.collect()}
        answers = {}
        for request in requests:
            at = arguments.index(request.argument) * len(READ_AS)
            held = by_column.get(names.index(request.column)) if request.column is not None else None
            answers[request] = held[at : at + len(READ_AS)] if held else [0] * len(READ_AS)
        return answers

    def ordered(self, table: DataFrame, requests: list[Request], operand: Callable[[Request], Operand]) -> dict[Request, Any]:
        """Each column's value at rank k_min from one end, from one pass keeping each task's k_min nearest values."""
        if not requests:
            return {}
        keyed = [ORDERED[request.statistic](operand(request)) for request in requests]
        keys = [key.alias(f'{HIDDEN_COLUMN_PREFIX}_end_{index}') for index, (key, _) in enumerate(keyed)]
        ends = tuple((request.argument, low) for request, (_, low) in zip(requests, keyed, strict=True))
        found: dict[int, list[str]] = {}
        for row in table.select(*keys).mapInArrow(EndValues(ends), END_VALUES_SCHEMA).collect():
            found.setdefault(row['request'], []).append(row['value'])
        answers = {}
        for index, (request, (_, low)) in enumerate(zip(requests, keyed, strict=True)):
            o = operand(request)
            length = request.statistic.startswith('length_')
            values = sorted(
                (int(value) if length or o.kind in (ColumnKind.INTEGER, ColumnKind.DATE, ColumnKind.DATETIME) else float(value))
                for value in found.get(index, [])
            )
            rank = min(request.argument, len(values))
            nearest = (values[rank - 1] if low else values[-rank]) if values else None
            answers[request] = nearest if length else o.back(nearest)
        return answers

    def distinct_days(
        self, table: DataFrame, requests: list[Request], extremes: dict[Request, Any], operand: Callable[[Request], Operand]
    ) -> dict[Request, Any]:
        """The days holding a value between each column's trimmed extremes, counted exactly."""
        if not requests:
            return {}
        aggregates = []
        for request in requests:
            o = operand(request)
            low = extremes[Request(request.column, 'trimmed_low', request.argument)]
            high = extremes[Request(request.column, 'trimmed_high', request.argument)]
            if low is None or high is None:
                aggregates.append(sf.lit(0))
                continue
            low_key, high_key = (ordered_key(o, value) for value in (low, high))
            day = sf.to_date(o.raw) if o.kind is ColumnKind.DATETIME else o.raw
            aggregates.append(sf.size(sf.collect_set(sf.when(o.ordered.between(low_key, high_key), day))))
        row = table.agg(*aggregates).collect()[0]
        return {request: int(row[index]) for index, request in enumerate(requests)}

    def steps(self, table: DataFrame, requests: list[Request]) -> dict[Request, Any]:
        """Each integer column's least and greatest step between neighboring rows, in the listing's order of the files."""
        if not requests:
            return {}
        names = tuple(dict.fromkeys(request.column for request in requests if request.column is not None))
        keyed = FILE_COLUMN in table.columns and BLOCK_COLUMN in table.columns
        selected = [column(name) for name in names] + ([column(FILE_COLUMN), column(BLOCK_COLUMN)] if keyed else [])
        rows = table.select(*selected).mapInArrow(SplitSteps(names, keyed), SPLIT_STEPS_SCHEMA).collect()

        def order(row: Any) -> tuple[Any, ...]:
            if row['file'] is None:
                return (0, '', row['part'], 0)
            path = listed_path(row['file'])
            return (self.listing.get(path, len(self.listing)), path, 0, row['block'])

        found: dict[str, tuple[int | None, int | None]] = {}
        for name in names:
            low = high = previous = None
            for row in sorted((row for row in rows if row['column'] == name), key=order):
                low, high = least(low, row['low']), greatest(high, row['high'])
                if previous is not None and row['first'] is not None:
                    low, high = least(low, row['first'] - previous), greatest(high, row['first'] - previous)
                previous = row['last']
            found[name] = (low, high)
        return {request: found[request.column][0 if request.statistic == 'step_min' else 1] for request in requests if request.column}

    def row_count(self, table: DataFrame) -> int:
        held = self.rows.get(id(table))
        if held is not None and held[0] is table:
            return held[1]
        rows = table.count()
        self.rows[id(table)] = (table, rows)
        return rows

    def picked(self, table: DataFrame, n: int, seed: int) -> DataFrame:
        """The n rows whose seeded hash of their place is least, or the table where it holds no more than n.

        A row read from files is placed by its file's index in the listing and its own index in the file, so the rows are
        the same however Spark splits the files, and each copy of a repeated row is drawn on its own. A DataFrame's row is
        placed by its values, so its copies are kept or dropped together. Rows outside the least `OVERSAMPLE` times n's
        share of the hash range are dropped before the sort.
        """
        rows = self.row_count(table)
        if rows <= n:
            return table
        if FILE_COLUMN in table.columns and BLOCK_COLUMN in table.columns:
            # A sample keeps each row's place, because its rows of one split are no longer read together.
            table = table if FILE_ROW_COLUMN in table.columns else self.with_file_rows(table)
            place = sf.xxhash64(sf.lit(seed), column(LISTED_COLUMN), column(FILE_ROW_COLUMN))
        else:
            place = sf.xxhash64(sf.lit(seed), *(column(name) for name in table.columns if not name.startswith(HIDDEN_COLUMN_PREFIX)))
        cutoff = min(2**63 - 1, -(2**63) + int(min(1.0, OVERSAMPLE * n / rows) * 2**64))
        placed = table.withColumn(PLACE_COLUMN, place).where(sf.col(PLACE_COLUMN) <= cutoff)
        return placed.orderBy(PLACE_COLUMN).limit(n).drop(PLACE_COLUMN)

    def with_file_rows(self, table: DataFrame) -> DataFrame:
        """The table with each row's file's index in the listing, and the row's index in its file: the rows of the file's
        earlier splits, counted on the driver, and then the row's place in its own split."""
        numbered = table.withColumn(SPLIT_ROW_COLUMN, sf.monotonically_increasing_id())
        counted = numbered.groupBy(FILE_COLUMN, BLOCK_COLUMN).agg(
            sf.count(sf.lit(1)).alias('rows'), sf.min(SPLIT_ROW_COLUMN).alias('first')
        )
        offsets, held = [], dict[str, int]()
        for split in sorted(counted.collect(), key=lambda split: (split[FILE_COLUMN], split[BLOCK_COLUMN])):
            file = split[FILE_COLUMN]
            before = held.get(file, 0)
            offsets.append((file, split[BLOCK_COLUMN], self.listing.get(listed_path(file), -1), before - split['first']))
            held[file] = before + split['rows']
        schema = f'{FILE_COLUMN} string, {BLOCK_COLUMN} long, {LISTED_COLUMN} long, {OFFSET_COLUMN} long'
        joined = numbered.join(sf.broadcast(self.spark.createDataFrame(offsets, schema)), [FILE_COLUMN, BLOCK_COLUMN])
        file_row = (column(SPLIT_ROW_COLUMN) + column(OFFSET_COLUMN)).alias(FILE_ROW_COLUMN)
        return joined.select(*(column(name) for name in table.columns), column(LISTED_COLUMN), file_row)

    def top_k(self, table: DataFrame, column_name: str, k: int, of: FrequencyKey = FrequencyKey.VALUE) -> list[tuple[Any, int]]:
        key = column(column_name)
        if of is FrequencyKey.SHAPE:
            key = sf.translate(key, SHAPE_FROM, SHAPE_TO)
        elif of is FrequencyKey.ZIP_PREFIX:
            key = sf.substring(key, 1, 3)
        counted = (
            table.select(key.alias('key'))
            .where(sf.col('key').isNotNull())
            .groupBy('key')
            .count()
            .orderBy(sf.desc('count'), sf.asc('key'))
            .limit(k)
            .collect()
        )
        return [(row['key'], row['count']) for row in counted]

    def sample(self, table: DataFrame, n: int, seed: int) -> pa.Table:
        """The rows `picked` gives, or every row where the table holds no more than n."""
        picked = self.picked(table, n, seed).select(*(column(name) for name in table.columns))
        streams = picked.mapInArrow(ArrowBatches(), 'batch binary').collect()
        if not streams:
            return pa.schema([]).empty_table() if not table.columns else arrow_schema(table).empty_table()
        return pa.concat_tables(pa.ipc.open_stream(bytes(row['batch'])).read_all() for row in streams)

    def sampled(self, table: DataFrame, n: int, seed: int) -> DataFrame:
        # Persisted, because every query of it would otherwise draw it again from every row.
        return self.persist(self.picked(table, n, seed))

    def parse(self, table: DataFrame, types: dict[str, ParsedType]) -> DataFrame:
        # Persisted, because the optimizer otherwise inlines each parse into every aggregate reading the column, and a
        # kurtosis reads it five times. A table an earlier parse gave is released, as only the latest is read again.
        parsed_table = self.persist(
            table.withColumns({name: parsed(column(name), parsed_type, self.major) for name, parsed_type in types.items()})
        )
        if any(frame is table for frame in self.parsed):
            table.unpersist()
            self.persisted = [frame for frame in self.persisted if frame is not table]
        self.parsed = [*(frame for frame in self.parsed if frame is not table), parsed_table]
        return parsed_table

    def recoded(self, table: DataFrame, recodes: dict[str, Recode]) -> DataFrame:
        # Not persisted: a comparison with fewer than k_min literals costs less than reading the table back from disk.
        types = {field.name: field.dataType for field in table.schema.fields}
        recoded_table = table.withColumns(
            {
                name: sf.when(column(name).isin(list(recode.values)), sf.lit(recode.modal).cast(types[name])).otherwise(column(name))
                for name, recode in recodes.items()
            }
        )
        # Rewriting values keeps every row, so the rows counted of `table` stand for its recode too.
        held = self.rows.get(id(table))
        if held is not None and held[0] is table:
            self.rows[id(recoded_table)] = (recoded_table, held[1])
        return recoded_table


def ordered_key(o: Operand, value: Any) -> Column:
    """A value `Operand.back` gave, as `ordered` holds it."""
    if isinstance(value, dt.datetime):
        moment = value.astimezone(dt.UTC).replace(tzinfo=None) if value.tzinfo else value
        return sf.lit((moment - EPOCH) // dt.timedelta(microseconds=1))
    if isinstance(value, dt.date):
        return sf.lit((value - EPOCH.date()).days)
    return sf.lit(value)


def arrow_schema(table: DataFrame) -> pa.Schema:
    return pa.schema([pa.field(spec.name, pa.null()) for spec in map(spec_of, table.schema.fields)])


def regex_quoted(text: str) -> str:
    return ''.join('\\' + character if not character.isalnum() else character for character in text)
