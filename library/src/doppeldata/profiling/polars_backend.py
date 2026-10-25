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

from doppeldata.profiling import sources
from doppeldata.profiling import table_profile
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import ParsedType
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import SourceFormat
from doppeldata.profiling.statistic_catalog import ALL_STATISTICS
from doppeldata.profiling.statistic_catalog import BOOLEAN_TEXT
from doppeldata.profiling.statistic_catalog import DECIMAL_TEXT
from doppeldata.profiling.statistic_catalog import INTEGER_TEXT
from doppeldata.profiling.statistic_catalog import QUANTILES
from doppeldata.profiling.statistic_catalog import SHAPE_FROM
from doppeldata.profiling.statistic_catalog import SHAPE_TO
from doppeldata.profiling.statistic_catalog import ColumnKind
from doppeldata.profiling.statistic_catalog import FrequencyKey

# Each translation takes the column and the request's argument.
Translation = Callable[[pl.Expr, Any], pl.Expr]

NANOSECONDS = {'microseconds': 10**3, 'milliseconds': 10**6, 'seconds': 10**9, 'minutes': 60 * 10**9, 'hours': 3600 * 10**9}
NANOSECONDS['days'] = 24 * NANOSECONDS['hours']
ROW_INDEX = '__doppeldata_row'
# Nothing downloads at run time, so DuckDB may use only the extensions its wheel links in.
DUCKDB_CONFIG: dict[str, str | bool | int | float | list[str]] = {'autoinstall_known_extensions': False, 'autoload_known_extensions': False}


def quantile(level: float) -> Translation:
    return lambda c, _: c.quantile(level, 'nearest')


def length_quantile(level: float) -> Translation:
    return lambda c, _: c.str.len_chars().quantile(level, 'nearest')


def counts_by(part: pl.Expr, values: Iterable[int]) -> pl.Expr:
    return pl.concat_list([(part == value).sum() for value in values])


def whole(unit: str) -> Translation:
    return lambda c, _: (c.dt.replace_time_zone(None).dt.epoch('ns') % NANOSECONDS[unit] == 0).all()


def is_number_or_boolean(c: pl.Expr) -> pl.Expr:
    return c.str.contains(DECIMAL_TEXT) | c.str.contains(BOOLEAN_TEXT)


POLARS_STATISTICS: dict[str, Translation] = {
    'rows': lambda _, __: pl.len(),
    'null_count': lambda c, _: c.null_count(),
    # Counted exactly: approx_n_unique strays by several percent on the streaming engine.
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
    **{name: quantile(level) for name, level in QUANTILES.items()},
    'step_min': lambda c, _: c.diff().min(),
    'step_max': lambda c, _: c.diff().max(),
    'length_mean': lambda c, _: c.str.len_chars().mean(),
    'length_trimmed_low': lambda c, k: c.str.len_chars().bottom_k(k).max(),
    'length_trimmed_high': lambda c, k: c.str.len_chars().top_k(k).min(),
    **{f'length_{name}': length_quantile(level) for name, level in QUANTILES.items()},
    'whitespace_edge_rate': lambda c, _: (c.str.len_chars() != c.str.strip_chars().str.len_chars()).mean(),
    'true_rate': lambda c, _: c.mean(),
    'month_counts': lambda c, _: counts_by(c.dt.month(), range(1, 13)),
    'weekday_counts': lambda c, _: counts_by(c.dt.weekday(), range(1, 8)),
    'hour_counts': lambda c, _: counts_by(c.dt.hour(), range(24)),
    'distinct_days': lambda c, _: c.dt.date().drop_nulls().n_unique(),
    **{f'whole_{unit}': whole(unit) for unit in NANOSECONDS},
    'raw_integer': lambda c, _: c.str.contains(INTEGER_TEXT).sum(),
    'raw_decimal': lambda c, _: (c.str.contains(DECIMAL_TEXT) & ~c.str.contains(INTEGER_TEXT)).sum(),
    'raw_boolean': lambda c, _: c.str.contains(BOOLEAN_TEXT).sum(),
    'raw_date': lambda c, layout: (c.str.to_date(layout, strict=False).is_not_null() & ~is_number_or_boolean(c)).sum(),
    'raw_timestamp': lambda c, layout: (
        c.str.to_datetime(layout, strict=False, time_unit='us').is_not_null() & ~is_number_or_boolean(c)
    ).sum(),
}

untranslated = sorted({statistic.name for statistic in ALL_STATISTICS} - POLARS_STATISTICS.keys())
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
    match parsed_type.raw_type:
        case 'integer':
            return pl.when(c.str.contains(INTEGER_TEXT)).then(c.str.to_integer(strict=False))
        case 'decimal':
            return pl.when(c.str.contains(DECIMAL_TEXT)).then(c.cast(pl.Float64, strict=False))
        case 'boolean':
            return pl.when(c.str.contains(BOOLEAN_TEXT)).then(c.str.to_lowercase() == 'true')
        case 'date':
            return c.str.to_date(parsed_type.format, strict=False)
        case 'timestamp':
            return c.str.to_datetime(parsed_type.format, strict=False, time_unit='us')
    return c


def is_hive_partitioned(paths: list[str]) -> bool:
    """Whether any file sits under a `key=value` directory, whose keys Polars then reads as columns."""
    return any('=' in part for path in paths for part in PurePosixPath(path).parent.parts)


class PolarsBackend:
    name = 'polars'

    def scan(self, source: Source) -> pl.LazyFrame:
        if source.table is not None:
            table = pl.DataFrame(source.table).lazy()
        else:
            paths = [file.path for file in source.files]
            hive = is_hive_partitioned(paths)
            match source.format:
                case SourceFormat.PARQUET:
                    table = pl.scan_parquet(paths, hive_partitioning=hive)
                case SourceFormat.ARROW:
                    table = pl.scan_ipc(paths, hive_partitioning=hive)
                case _:
                    table = read_text(source)
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

        def column(name: str | None) -> pl.Expr:
            if name is None:
                return pl.lit(None)
            # Decimal moments and quantiles are taken as doubles, which every statistic accepts.
            return pl.col(name).cast(pl.Float64) if kinds[name] is ColumnKind.DECIMAL else pl.col(name)

        expressions = [
            POLARS_STATISTICS[request.statistic](column(request.column), request.argument).alias(f'r{index}')
            for index, request in enumerate(requests)
        ]
        row = table.select(expressions).collect(engine='streaming').row(0)
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


def profile_data(data: Any, *, sample: int | None, k_min: int) -> dict[str, Any]:
    return table_profile.profile_table(PolarsBackend(), sources.resolve(data), sample=sample, k_min=k_min)
