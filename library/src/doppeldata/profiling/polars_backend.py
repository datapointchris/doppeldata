"""The Polars backend. Polars computes every statistic, and DuckDB only reads CSV, TSV and JSON as text."""

from collections.abc import Callable
from collections.abc import Iterable
from pathlib import PurePosixPath
from typing import Any

import duckdb
import polars as pl
import polars.selectors as cs
import pyarrow as pa
from polars.datatypes import DataTypeClass

from doppeldata.errors import SourceError
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import ParsedType
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import SourceFormat
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

# Each translation takes the column, or its value from `ROW_STAGES` where one is named, and the request's argument.
Translation = Callable[[pl.Expr, Any], pl.Expr]

ROW_INDEX = '__doppeldata_row'
ROW_STAGE = '__doppeldata_stage_{}'
# Nothing downloads at run time, so DuckDB may use only the extensions its wheel links in.
DUCKDB_CONFIG: dict[str, str | bool | int | float | list[str]] = {'autoinstall_known_extensions': False, 'autoload_known_extensions': False}


def counts_by(part: pl.Expr, values: Iterable[Any]) -> pl.Expr:
    return pl.concat_list([(part == value).sum() for value in values])


def histogram_counts(c: pl.Expr, edges: tuple[float, ...]) -> pl.Expr:
    """The rows below the first edge, in each bin, and above the last, from one sort and a binary search per edge."""
    ordered = c.drop_nulls().cast(pl.Float64).sort()
    rows_below = ordered.search_sorted(pl.Series(edges[:-1], dtype=pl.Float64), side='left').cast(pl.Int64).implode()
    # The last bin holds its upper edge, so its cumulative count includes the rows equal to it.
    rows_through_last = ordered.search_sorted(float(edges[-1]), side='right').cast(pl.Int64)
    cumulative = pl.concat_list(pl.lit(0, pl.Int64), rows_below, rows_through_last, ordered.len().cast(pl.Int64))
    return cumulative.list.diff().list.slice(1)


def whole(nanoseconds: int) -> Translation:
    return lambda c, _: (c.dt.replace_time_zone(None).dt.epoch('ns') % nanoseconds == 0).all()


def is_digits(c: pl.Expr) -> pl.Expr:
    return c.str.contains(DIGITS_TEXT)


# Whether each raw value reads as the raw type, given the type's layout. Each is tried only on a value
# no earlier type in `RAW_TYPES` read.
TEXT_MATCHERS: dict[str, Callable[[pl.Expr, str | None], pl.Expr]] = {
    'integer': lambda c, _: c.str.contains(INTEGER_TEXT) & c.str.to_integer(strict=False).is_not_null(),
    # A run of digits too long for an integer is an identifier, never a decimal.
    'decimal': lambda c, _: c.str.contains(DECIMAL_TEXT) & ~is_digits(c),
    'boolean': lambda c, _: c.str.contains(BOOLEAN_TEXT),
    'date': lambda c, layout: c.str.to_date(layout, strict=False).is_not_null() & ~is_digits(c),
    'timestamp': lambda c, layout: c.str.to_datetime(layout, strict=False, time_unit='us').is_not_null() & ~is_digits(c),
}
TEXT_CONVERTERS: dict[str, Callable[[pl.Expr, str | None], pl.Expr]] = {
    'integer': lambda c, _: c.str.to_integer(strict=False),
    'decimal': lambda c, _: c.cast(pl.Float64, strict=False),
    'boolean': lambda c, _: c.str.to_lowercase() == 'true',
    'date': lambda c, layout: c.str.to_date(layout, strict=False),
    'timestamp': lambda c, layout: c.str.to_datetime(layout, strict=False, time_unit='us'),
}


def raw_type_code(c: pl.Expr, layouts: tuple[tuple[str, str], ...]) -> pl.Expr:
    """Each non-null value's index in `READ_AS` of the first raw type it reads as."""
    chosen = dict(layouts)
    tried = [raw for raw in RAW_TYPES if raw not in RAW_TYPE_LAYOUTS or raw in chosen]
    code: pl.Expr = pl.lit(READ_AS.index('string'), pl.UInt8)
    for raw in reversed(tried):
        code = pl.when(TEXT_MATCHERS[raw](c, chosen.get(raw))).then(pl.lit(READ_AS.index(raw), pl.UInt8)).otherwise(code)
    return pl.when(c.is_not_null()).then(code)


# The streaming engine evaluates an expression again at each reference, so a value counted several times is computed first.
ROW_STAGES: dict[str, Translation] = {
    'raw_type_counts': raw_type_code,
    'month_counts': lambda c, _: c.dt.month(),
    'weekday_counts': lambda c, _: c.dt.weekday(),
    'hour_counts': lambda c, _: c.dt.hour(),
}


POLARS_STATISTICS: dict[str, Translation] = {
    'rows': lambda _, __: pl.len(),
    'null_count': lambda c, _: c.null_count(),
    # Counted exactly, so `distinct.ratio` does not move with the engine or its version.
    'distinct_count': lambda c, _: c.drop_nulls().n_unique(),
    'min': lambda c, _: c.min(),
    'max': lambda c, _: c.max(),
    'trimmed_low': lambda c, k: c.bottom_k(k).max(),
    'trimmed_high': lambda c, k: c.top_k(k).min(),
    'mean': lambda c, _: c.mean(),
    'std': lambda c, _: c.std(),
    'skew': lambda c, _: c.skew(),
    'kurtosis': lambda c, _: c.kurtosis(),
    'zero_rate': lambda c, _: (c == 0).mean(),
    'negative_rate': lambda c, _: (c < 0).mean(),
    # Every level from one sort, where a quantile per level would sort the column once each.
    'quantiles': lambda c, levels: c.quantile(list(levels), 'nearest'),
    'histogram_counts': histogram_counts,
    'step_min': lambda c, _: c.diff().min(),
    'step_max': lambda c, _: c.diff().max(),
    'length_mean': lambda c, _: c.str.len_chars().mean(),
    'length_trimmed_low': lambda c, k: c.str.len_chars().bottom_k(k).max(),
    'length_trimmed_high': lambda c, k: c.str.len_chars().top_k(k).min(),
    'length_quantiles': lambda c, levels: c.str.len_chars().quantile(list(levels), 'nearest'),
    'listed_value_counts': lambda c, values: counts_by(c, values),
    'whitespace_edge_rate': lambda c, _: (c.str.len_chars() != c.str.strip_chars().str.len_chars()).mean(),
    'true_rate': lambda c, _: c.mean(),
    'month_counts': lambda months, _: counts_by(months, range(1, 13)),
    'weekday_counts': lambda weekdays, _: counts_by(weekdays, range(1, 8)),
    'hour_counts': lambda hours, _: counts_by(hours, range(24)),
    'distinct_days': lambda c, k: c.filter(c.is_between(c.bottom_k(k).max(), c.top_k(k).min())).dt.date().n_unique(),
    **{f'whole_{unit}': whole(nanoseconds) for unit, nanoseconds, _ in TIME_UNITS},
    'raw_type_counts': lambda codes, _: counts_by(codes, range(len(READ_AS))),
    'padded_digits': lambda c, _: c.str.contains(PADDED_DIGITS_TEXT).sum(),
}

untranslated = sorted({statistic.name for statistic in ALL_STATISTICS} - POLARS_STATISTICS.keys())
untranslated += sorted(f'the raw type {raw}' for raw in set(RAW_TYPES) - (TEXT_MATCHERS.keys() & TEXT_CONVERTERS.keys()))
if untranslated:
    raise RuntimeError(f'the Polars backend has no translation for {", ".join(untranslated)}')

SIMPLE_ARROW_TYPES: dict[Any, str] = {
    pl.Int8: 'int8',
    pl.Int16: 'int16',
    pl.Int32: 'int32',
    pl.Int64: 'int64',
    pl.UInt8: 'uint8',
    pl.UInt16: 'uint16',
    pl.UInt32: 'uint32',
    pl.UInt64: 'uint64',
    pl.Float32: 'float',
    pl.Float64: 'double',
    pl.String: 'string',
    pl.Boolean: 'bool',
    pl.Date: 'date32[day]',
    pl.Time: 'time64[ns]',
    pl.Binary: 'binary',
    pl.Null: 'null',
}


def arrow_type(dtype: pl.DataType | DataTypeClass) -> str:
    """The column's Arrow type as pyarrow spells it, such as `decimal128(12, 2)` or `timestamp[us, tz=UTC]`."""
    if isinstance(dtype, pl.Decimal):
        return f'decimal128({dtype.precision or 38}, {dtype.scale})'
    if isinstance(dtype, pl.Datetime):
        return f'timestamp[{dtype.time_unit}, tz={dtype.time_zone}]' if dtype.time_zone else f'timestamp[{dtype.time_unit}]'
    if isinstance(dtype, pl.Duration):
        return f'duration[{dtype.time_unit}]'
    if isinstance(dtype, pl.List):
        return f'list<item: {arrow_type(dtype.inner)}>'
    if isinstance(dtype, pl.Array):
        return f'fixed_size_list<item: {arrow_type(dtype.inner)}>[{dtype.size}]'
    if isinstance(dtype, pl.Struct):
        return 'struct<' + ', '.join(f'{field.name}: {arrow_type(field.dtype)}' for field in dtype.fields) + '>'
    return SIMPLE_ARROW_TYPES.get(dtype.base_type(), str(dtype).lower())


def kind_of(dtype: pl.DataType) -> ColumnKind:
    if dtype.is_integer():
        return ColumnKind.INTEGER
    if dtype.is_float():
        return ColumnKind.FLOAT
    if isinstance(dtype, pl.Decimal):
        return ColumnKind.DECIMAL
    if dtype == pl.String:
        return ColumnKind.STRING
    if dtype == pl.Boolean:
        return ColumnKind.BOOLEAN
    if dtype == pl.Date:
        return ColumnKind.DATE
    if isinstance(dtype, pl.Datetime):
        return ColumnKind.DATETIME
    return ColumnKind.OTHER


def parsed(c: pl.Expr, parsed_type: ParsedType) -> pl.Expr:
    """The column read as its type, with a value its counts gave another type as null."""
    raw, layout = parsed_type.raw_type, parsed_type.format
    if raw == 'string':
        return c
    read = TEXT_MATCHERS[raw](c, layout)
    for folded in RAW_TYPE_FOLDS.get(raw, ()):
        read = read | TEXT_MATCHERS[folded](c, layout)
    return pl.when(read).then(TEXT_CONVERTERS[raw](c, layout))


def ancestor_partition_keys(source: Source) -> list[str]:
    """Hive keys Polars reads from `key=value` directories above the root that was named, which the table does not hold."""
    every = {part.split('=', 1)[0] for file in source.files for part in PurePosixPath(file.path).parent.parts if '=' in part}
    return sorted(every - set(source.partition_keys))


class PolarsBackend:
    name = 'polars'

    def scan(self, source: Source) -> pl.LazyFrame:
        if source.table is not None:
            table = pl.DataFrame(source.table).lazy()
        else:
            table = scan_files(source)
        return table.with_columns(cs.categorical().cast(pl.String), cs.enum().cast(pl.String))

    def schema(self, table: pl.LazyFrame) -> list[ColumnSpec]:
        specs = []
        for name, dtype in table.collect_schema().items():
            spec = ColumnSpec(name, kind_of(dtype), arrow_type(dtype))
            if isinstance(dtype, pl.Decimal):
                spec = ColumnSpec(name, spec.kind, spec.physical_type, precision=dtype.precision, scale=dtype.scale)
            if isinstance(dtype, pl.Datetime) and dtype.time_zone:
                spec = ColumnSpec(name, spec.kind, spec.physical_type, timezone=dtype.time_zone)
            specs.append(spec)
        return specs

    def aggregate(self, table: pl.LazyFrame, requests: list[Request]) -> dict[Request, Any]:
        if not requests:
            return {}
        kinds = {spec.name: spec.kind for spec in self.schema(table)}
        dtypes = dict(table.collect_schema())

        def column(name: str | None) -> pl.Expr:
            if name is None:
                return pl.lit(None)
            c = pl.col(name)
            match kinds[name], dtypes[name]:
                # Decimal moments and quantiles are taken as doubles, which every statistic accepts.
                case ColumnKind.DECIMAL, _:
                    return c.cast(pl.Float64)
                # A profile cannot hold NaN or an infinity, so a float column counts them as nulls.
                case ColumnKind.FLOAT, _:
                    return pl.when(c.is_finite()).then(c)
                # Steps and comparisons with zero go through a signed type, which a uint64 overflows.
                case ColumnKind.INTEGER, pl.UInt64:
                    return c.cast(pl.Int128)
            return c

        staged = {
            ROW_STAGE.format(index): ROW_STAGES[request.statistic](column(request.column), request.argument)
            for index, request in enumerate(requests)
            if request.statistic in ROW_STAGES
        }

        def operand(index: int, request: Request) -> pl.Expr:
            name = ROW_STAGE.format(index)
            return pl.col(name) if name in staged else column(request.column)

        expressions = [
            POLARS_STATISTICS[request.statistic](operand(index, request), request.argument).alias(f'r{index}')
            for index, request in enumerate(requests)
        ]
        row = table.with_columns(**staged).select(expressions).collect(engine='streaming').row(0)
        return dict(zip(requests, row, strict=True))

    def top_k(self, table: pl.LazyFrame, column: str, k: int, of: FrequencyKey = FrequencyKey.VALUE) -> list[tuple[Any, int]]:
        key = pl.col(column)
        if of is FrequencyKey.SHAPE:
            key = key.str.replace_many(list(SHAPE_FROM), list(SHAPE_TO))
        counted = (
            table.select(key.alias('key'))
            .drop_nulls()
            .group_by('key')
            .len()
            .sort(['len', 'key'], descending=[True, False])
            .head(k)
            .collect(engine='streaming')
        )
        return list(counted.iter_rows())

    def sample(self, table: pl.LazyFrame, n: int, seed: int) -> pa.Table:
        rows = table.select(pl.len()).collect(engine='streaming').item()
        if rows > n:
            picked = pl.int_range(0, rows, eager=True).sample(n, seed=seed).sort()
            table = table.with_row_index(ROW_INDEX).filter(pl.col(ROW_INDEX).is_in(picked.implode())).drop(ROW_INDEX)
        return table.collect(engine='streaming').to_arrow()

    def parse(self, table: pl.LazyFrame, types: dict[str, ParsedType]) -> pl.LazyFrame:
        return table.with_columns(parsed(pl.col(name), parsed_type).alias(name) for name, parsed_type in types.items())


def scan_files(source: Source) -> pl.LazyFrame:
    """The source's files as one table, read far enough to find a file that is not of its format."""
    format_name = source.format.value if source.format else 'a table'
    paths = [file.path for file in source.files]
    hive = bool(source.partition_keys)
    try:
        match source.format:
            case SourceFormat.PARQUET:
                table = pl.scan_parquet(paths, hive_partitioning=hive)
            case SourceFormat.ARROW:
                table = pl.scan_ipc(paths, hive_partitioning=hive)
            case _:
                table = read_text(source)
        table = table.drop(ancestor_partition_keys(source), strict=False)
        # Counting rows reads every file's footer, so a corrupt file fails here rather than mid-profile.
        table.select(pl.len()).collect(engine='streaming')
    except (pl.exceptions.PolarsError, duckdb.Error, OSError) as error:
        raise SourceError(f'{", ".join(source.roots)} could not be read as {format_name}: {first_line(error)}', exit_code=1) from error
    return table


def first_line(error: Exception) -> str:
    return str(error).strip().splitlines()[0] if str(error).strip() else type(error).__name__


def read_text(source: Source) -> pl.LazyFrame:
    """CSV, TSV or JSON read through DuckDB with every column as text, handed to Polars."""
    paths = [file.path for file in source.files]
    with duckdb.connect(config=DUCKDB_CONFIG) as connection:
        if source.format in (SourceFormat.CSV, SourceFormat.TSV):
            dialect = source.dialect
            delimiter = '\t' if source.format is SourceFormat.TSV else dialect.delimiter
            query = (
                'select * from read_csv($paths, all_varchar = true, delim = $delimiter, quote = $quote, escape = $escape, header = $header)'
            )
            parameters = {
                'paths': paths,
                'delimiter': delimiter,
                'quote': dialect.quote,
                'escape': dialect.escape,
                'header': dialect.header,
            }
            frame = selected(connection, query, parameters).pl()
        else:
            layout = 'newline_delimited' if source.format is SourceFormat.NDJSON else 'auto'
            names = selected(connection, 'select * from read_json($paths, format = $layout)', {'paths': paths, 'layout': layout}).columns
            query = 'select * from read_json($paths, format = $layout, columns = $columns)'
            columns = {name: 'VARCHAR' for name in names}
            frame = selected(connection, query, {'paths': paths, 'layout': layout, 'columns': columns}).pl()
    return frame.lazy()


def selected(connection: duckdb.DuckDBPyConnection, query: str, parameters: dict[str, Any]) -> duckdb.DuckDBPyRelation:
    relation = connection.sql(query, params=parameters)
    if relation is None:
        raise RuntimeError(f'DuckDB returned no relation for {query!r}')
    return relation
