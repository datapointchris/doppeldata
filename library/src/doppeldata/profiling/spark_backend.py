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

from doppeldata.errors import SourceError
from doppeldata.profiling import parquet_footers
from doppeldata.profiling.backend import HIDDEN_COLUMN_PREFIX
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import ParsedType
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import SourceFormat
from doppeldata.profiling.backend import SourceRead
from doppeldata.profiling.sources import is_spark_frame
from doppeldata.profiling.statistic_catalog import ALL_STATISTICS
from doppeldata.profiling.statistic_catalog import BOOLEAN_TEXT
from doppeldata.profiling.statistic_catalog import DECIMAL_TEXT
from doppeldata.profiling.statistic_catalog import DIGITS_TEXT
from doppeldata.profiling.statistic_catalog import INTEGER_TEXT
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

# HyperLogLog++'s relative standard deviation. At 0.01 each column's sketch holds 2^14 registers, and a count of a few
# thousand values is linear counting's, close to exact.
DISTINCT_RSD = 0.01
# Greenwald-Khanna's accuracy, whose reciprocal is the largest rank error a quantile may carry.
QUANTILE_ACCURACY = 10_000
# The columns one compiled aggregate covers. Twice as many make its generated code pass the JVM's 64 KB method limit on a
# table of numeric columns, and Spark then runs the query interpreted, several times slower.
COLUMNS_PER_QUERY = 16
# The share of the hash range a sample keeps is this many times the rows asked for, which holds them all but never.
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
# Spark 3.5 has no try_cast on a column, and a cast with ANSI off gives null where a value does not parse.
ANSI_OFF = {'spark.sql.ansi.enabled': 'false'}
FILE_COLUMN = f'{HIDDEN_COLUMN_PREFIX}_file'
BLOCK_COLUMN = f'{HIDDEN_COLUMN_PREFIX}_block'
MALFORMED_COLUMN = f'{HIDDEN_COLUMN_PREFIX}_malformed'
PLACE_COLUMN = f'{HIDDEN_COLUMN_PREFIX}_place'
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


def count_where(condition: Column) -> Column:
    return sf.count(sf.when(condition, 1))


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
        raise SourceError('engine spark profiles no JSON source; profile it with engine polars')
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
    'zero_rate': lambda o, _: single(sf.mean((o.value == 0).cast('int'))),
    'negative_rate': lambda o, _: single(sf.mean((o.value < 0).cast('int'))),
    'quantiles': lambda o, levels: quantiles(o.value, levels),
    'decimal_places': decimal_places,
    'histogram_counts': histogram_counts,
    'length_mean': lambda o, _: single(sf.mean(sf.length(o.raw))),
    'length_quantiles': lambda o, levels: quantiles(sf.length(o.raw), levels),
    'listed_value_counts': lambda o, values: counts([o.raw == sf.lit(value) for value in values]),
    'whitespace_edge_rate': lambda o, _: single(sf.mean(o.raw.rlike(WHITESPACE_EDGE).cast('int'))),
    'true_rate': lambda o, _: single(sf.mean(o.raw.cast('int'))),
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

untranslated = sorted(
    {statistic.name for statistic in ALL_STATISTICS}
    - (AGGREGATED.keys() | ORDERED.keys() | STEPPED | BOUNDED | TYPED | ESTIMATED | REFUSED)
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


SIMPLE_ARROW_TYPES: dict[type, str] = {
    st.ByteType: 'int8',
    st.ShortType: 'int16',
    st.IntegerType: 'int32',
    st.LongType: 'int64',
    st.FloatType: 'float',
    st.DoubleType: 'double',
    st.StringType: 'string',
    st.BooleanType: 'bool',
    st.DateType: 'date32[day]',
    st.BinaryType: 'binary',
    st.NullType: 'null',
    # Spark holds a timestamp as an instant in microseconds, read in the session's UTC.
    st.TimestampType: 'timestamp[us, tz=UTC]',
    st.TimestampNTZType: 'timestamp[us]',
    st.DayTimeIntervalType: 'duration[us]',
}


def arrow_type(data_type: st.DataType) -> str:
    """The column's Arrow type as pyarrow spells it, as Spark's own Arrow conversion gives it."""
    if isinstance(data_type, st.DecimalType):
        return f'decimal128({data_type.precision}, {data_type.scale})'
    if isinstance(data_type, st.ArrayType):
        return f'list<item: {arrow_type(data_type.elementType)}>'
    if isinstance(data_type, st.StructType):
        return 'struct<' + ', '.join(f'{field.name}: {arrow_type(field.dataType)}' for field in data_type.fields) + '>'
    if isinstance(data_type, st.MapType):
        return f'map<{arrow_type(data_type.keyType)}, {arrow_type(data_type.valueType)}>'
    return SIMPLE_ARROW_TYPES.get(type(data_type), data_type.simpleString())


def spec_of(field: st.StructField) -> ColumnSpec:
    data_type = field.dataType
    kind = kind_of(data_type)
    if isinstance(data_type, st.DecimalType):
        return ColumnSpec(field.name, kind, arrow_type(data_type), precision=data_type.precision, scale=data_type.scale)
    if isinstance(data_type, st.TimestampType):
        return ColumnSpec(field.name, kind, arrow_type(data_type), timezone='UTC')
    return ColumnSpec(field.name, kind, arrow_type(data_type))


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
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


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
        """Run inside the session settings the translations assume, then restore them and release what was persisted."""
        settings = SESSION_SETTINGS | (ANSI_OFF if self.major < 4 else {})
        before = {key: self.spark.conf.get(key, None) for key in settings}
        for key, value in settings.items():
            self.spark.conf.set(key, value)
        try:
            yield
        finally:
            for key, value in before.items():
                if value is None:
                    self.spark.conf.unset(key)
                else:
                    self.spark.conf.set(key, value)
            for frame in self.persisted:
                frame.unpersist()
            self.persisted.clear()
            self.parsed.clear()

    def scan(self, source: Source) -> DataFrame:
        return self.read(source).table

    def read(self, source: Source) -> SourceRead:
        if source.table is not None:
            if not is_spark_frame(source.table):
                raise SourceError(
                    'engine spark profiles a Spark DataFrame, files or an s3:// prefix; profile an in-memory table with engine polars'
                )
            return SourceRead(source.table)
        self.listing = {listed_path(file.path): index for index, file in enumerate(source.files)}
        match source.format:
            case SourceFormat.PARQUET:
                read = self.read_parquet(source)
            case SourceFormat.CSV | SourceFormat.TSV:
                read = self.read_delimited(source)
            case _:
                format_name = source.format.value if source.format else 'a table'
                raise SourceError(
                    f'engine spark reads Parquet, CSV and TSV, and {format_name} is none of them; profile it with engine polars'
                )
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
            raise SourceError(f'{", ".join(source.roots)} could not be read as Parquet: {error}', exit_code=1) from error
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
            source.files, '\t' if source.format is SourceFormat.TSV else None, DUCKDB_CONFIG
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
            raise SourceError('engine spark profiles no JSON source; profile it with engine polars')
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
        return answers

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
        """The n rows whose seeded hash of their values is least, which are the same rows however Spark splits the files.

        Rows outside the least `OVERSAMPLE` times n's share of the hash range are dropped before the sort.
        """
        rows = self.row_count(table)
        if rows <= n:
            return table
        values = [column(name) for name in table.columns if not name.startswith(HIDDEN_COLUMN_PREFIX)]
        place = sf.xxhash64(sf.lit(seed), *values)
        cutoff = min(2**63 - 1, -(2**63) + int(min(1.0, OVERSAMPLE * n / rows) * 2**64))
        placed = table.withColumn(PLACE_COLUMN, place).where(sf.col(PLACE_COLUMN) <= cutoff)
        return placed.orderBy(PLACE_COLUMN).limit(n).drop(PLACE_COLUMN)

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
