"""The Polars backend. Polars computes every statistic, and DuckDB only reads CSV, TSV and JSON as text."""

import contextlib
import dataclasses as dc
import functools
import importlib
import json
import math
import os
import re
import tempfile
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Sequence
from pathlib import PurePosixPath
from typing import Any
from typing import TypeVar
from typing import cast

import duckdb
import polars as pl
import polars.selectors as cs
import pyarrow as pa

from doppeldata import issue_codes
from doppeldata.aws_sessions import polars_credentials
from doppeldata.errors import SourceError
from doppeldata.profiling import json_paths
from doppeldata.profiling import parquet_footers
from doppeldata.profiling.backend import HIDDEN_COLUMN_PREFIX
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import JsonNode
from doppeldata.profiling.backend import JsonRead
from doppeldata.profiling.backend import ParquetFooter
from doppeldata.profiling.backend import ParsedType
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import SourceFormat
from doppeldata.profiling.backend import SourceRead
from doppeldata.profiling.expression_rule import ExpressionCandidate
from doppeldata.profiling.expression_rule import counted_rows
from doppeldata.profiling.first_blocks import FIRST_BLOCK_BYTES
from doppeldata.profiling.first_blocks import first_block
from doppeldata.profiling.recorded_counts import Recode
from doppeldata.profiling.spearman_row_influence import InfluenceTerms
from doppeldata.profiling.spearman_row_influence import row_influence
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
from doppeldata.spark_polars_arrow_types import polars_arrow_type

# Each translation takes the column, or its value from `ROW_STAGES` where one is named, and the request's argument.
Translation = Callable[[pl.Expr, Any], pl.Expr]
Item = TypeVar('Item')
ColumnPass = TypeVar('ColumnPass', bound=Callable[..., list[Any]])

# The values one batch of a pass over every row reads, as rows times columns, and always at least one column. Polars
# holds every column of a query whose statistic it cannot stream, such as a quantile, so this bounds that query.
BATCH_VALUES = 16_000_000
# The columns read at once by a pass that goes row by row across all of them. Polars streams it, but every row group in
# flight holds each column read, whatever the table's rows.
ROW_BY_ROW_COLUMNS = 8
# The rows of one row group of a staged file, which Polars decodes whole.
STAGE_ROW_GROUP_ROWS = 20_480

ROW_INDEX = '__doppeldata_row'
ROW_STAGE = '__doppeldata_stage_{}'
FOLDED, PART = '__doppeldata_folded', '__doppeldata_part'
# Nothing downloads at run time, so DuckDB may use only the extensions its wheel links in.
DUCKDB_CONFIG: dict[str, str | bool | int | float | list[str]] = {'autoinstall_known_extensions': False, 'autoload_known_extensions': False}
# One thread, because a copy keeping the rows' order buffers batches while parallel readers outrun its writer. The flush
# returns what the allocator retains, and no memory limit could then fail the copy of a wide table.
STAGE_DUCKDB_CONFIG = DUCKDB_CONFIG | {'threads': 1, 'allocator_bulk_deallocation_flush_threshold': '16MB'}
STAGE_DIRECTORY_PREFIX = 'doppeldata-stage-'
# Usually a real disk, where the platform's own temporary directory is often a tmpfs held in memory.
LARGE_TEMP_DIRECTORY = '/var/tmp'
FILE_COLUMN = f'{HIDDEN_COLUMN_PREFIX}_file'
# Imported by name, because it imports clevercsv and charset-normalizer, which only a delimited source needs.
DELIMITED_DIALECT_MODULE = 'doppeldata.profiling.delimited_dialect'
# The directory value Hive and Spark write a null partition value under, as SQL text.
HIVE_NULL = "'__HIVE_DEFAULT_PARTITION__'"
# Each rejected row once, under its first error, counted by error.
REJECTED_ROWS = (
    'select error_type, count(*) from (select distinct on (file_id, line) error_type from reject_errors '
    'order by file_id, line, byte_position nulls last, column_idx nulls last) group by error_type'
)


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


def decimal_places(c: pl.Expr, most: int) -> pl.Expr:
    """The fewest places, up to `most`, that every value equals itself rounded to, or None.

    `round(places)` is exact where `(c * 10**places).round() / 10**places` is not: Polars rewrites that division as a
    multiplication by the reciprocal, which turns 164 / 100 into 1.6400000000000001.
    """
    places: pl.Expr = pl.lit(None, pl.Int32)
    for tried in reversed(range(most + 1)):
        places = pl.when((c.round(tried) == c).all()).then(pl.lit(tried, pl.Int32)).otherwise(places)
    return places


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


def raw_type_code(c: pl.Expr, layouts: tuple[tuple[str, str], ...], types: tuple[str, ...] = RAW_TYPES) -> pl.Expr:
    """Each non-null value's index in `READ_AS` of the first of `types` it reads as."""
    chosen = dict(layouts)
    tried = [raw for raw in types if raw not in RAW_TYPE_LAYOUTS or raw in chosen]
    code: pl.Expr = pl.lit(READ_AS.index('string'), pl.UInt8)
    for raw in reversed(tried):
        code = pl.when(TEXT_MATCHERS[raw](c, chosen.get(raw))).then(pl.lit(READ_AS.index(raw), pl.UInt8)).otherwise(code)
    return pl.when(c.is_not_null()).then(code)


def json_raw_type_code(c: pl.Expr, argument: tuple[tuple[tuple[str, str], ...], str]) -> pl.Expr:
    """Each JSON value's index in `READ_AS`: a number's or a boolean's by its JSON type, a string's as a date, a timestamp or a string."""
    layouts, type_column = argument
    json_type = pl.col(type_column)
    code: pl.Expr = pl.when(c.is_not_null()).then(pl.lit(READ_AS.index('string'), pl.UInt8))
    for named, raw in json_paths.JSON_RAW_TYPES.items():
        code = pl.when(json_type == named).then(pl.lit(READ_AS.index(raw), pl.UInt8)).otherwise(code)
    return pl.when(json_type == json_paths.JSON_STRING).then(raw_type_code(c, layouts, tuple(RAW_TYPE_LAYOUTS))).otherwise(code)


# The streaming engine evaluates an expression again at each reference, so a value counted several times is computed first.
ROW_STAGES: dict[str, Translation] = {
    'raw_type_counts': raw_type_code,
    'json_raw_type_counts': json_raw_type_code,
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
    'zero_fraction': lambda c, _: (c == 0).mean(),
    'negative_fraction': lambda c, _: (c < 0).mean(),
    # Every level from one sort, where a quantile per level would sort the column once each.
    'quantiles': lambda c, levels: c.quantile(list(levels), 'nearest'),
    'decimal_places': decimal_places,
    'histogram_counts': histogram_counts,
    'step_min': lambda c, _: c.diff().min(),
    'step_max': lambda c, _: c.diff().max(),
    'length_mean': lambda c, _: c.str.len_chars().mean(),
    'length_trimmed_low': lambda c, k: c.str.len_chars().bottom_k(k).max(),
    'length_trimmed_high': lambda c, k: c.str.len_chars().top_k(k).min(),
    'length_quantiles': lambda c, levels: c.str.len_chars().quantile(list(levels), 'nearest'),
    'listed_value_counts': lambda c, values: counts_by(c, values),
    'whitespace_edge_fraction': lambda c, _: (c.str.len_chars() != c.str.strip_chars().str.len_chars()).mean(),
    'true_fraction': lambda c, _: c.mean(),
    'month_counts': lambda months, _: counts_by(months, range(1, 13)),
    'weekday_counts': lambda weekdays, _: counts_by(weekdays, range(1, 8)),
    'hour_counts': lambda hours, _: counts_by(hours, range(24)),
    'distinct_days': lambda c, k: c.filter(c.is_between(c.bottom_k(k).max(), c.top_k(k).min())).dt.date().n_unique(),
    **{f'whole_{unit}': whole(nanoseconds) for unit, nanoseconds, _ in TIME_UNITS},
    'raw_type_counts': lambda codes, _: counts_by(codes, range(len(READ_AS))),
    'json_raw_type_counts': lambda codes, _: counts_by(codes, range(len(READ_AS))),
    'padded_digits': lambda c, _: c.str.contains(PADDED_DIGITS_TEXT).sum(),
    'unique_rows': lambda _, columns: pl.struct(list(columns)).is_unique().sum(),
}


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
    if parsed_type.missing:
        c = pl.when(~c.is_in(list(parsed_type.missing))).then(c)
    if parsed_type.raw_type == 'string':
        return c
    return pl.when(reads(c, parsed_type)).then(TEXT_CONVERTERS[parsed_type.raw_type](c, parsed_type.format))


def reads(c: pl.Expr, parsed_type: ParsedType) -> pl.Expr:
    """Whether each value parses as its column's type, or one the type folds in. A JSON value goes by its JSON type."""
    raw, layout = parsed_type.raw_type, parsed_type.format
    if parsed_type.json_type_column:
        json_type = pl.col(parsed_type.json_type_column)
        if raw in RAW_TYPE_LAYOUTS:
            return (json_type == json_paths.JSON_STRING) & TEXT_MATCHERS[raw](c, layout)
        accepted = {raw, *RAW_TYPE_FOLDS.get(raw, ())}
        return json_type.is_in([named for named, read_as in json_paths.JSON_RAW_TYPES.items() if read_as in accepted])
    read = TEXT_MATCHERS[raw](c, layout)
    for folded in RAW_TYPE_FOLDS.get(raw, ()):
        read = read | TEXT_MATCHERS[folded](c, layout)
    return read


def in_batches(items: Sequence[Item], size: int) -> list[Sequence[Item]]:
    return [items[at : at + size] for at in range(0, len(items), size)]


def rows_of(table: pl.LazyFrame) -> int:
    rows: int = table.select(pl.len()).collect(engine='streaming').item()
    return rows


def column_batches(items: list[Item], columns_of: Callable[[Item], Iterable[str]], rows: int) -> list[list[Item]]:
    """`items` in order, cut so that each batch reads no more than `BATCH_VALUES` values of `rows` rows, or one item."""
    width = max(1, BATCH_VALUES // max(rows, 1))
    batches: list[list[Item]] = []
    batch: list[Item] = []
    held: set[str] = set()
    for item in items:
        columns = set(columns_of(item))
        if batch and len(held | columns) > width:
            batches.append(batch)
            batch, held = [], set()
        batch.append(item)
        held |= columns
    return [*batches, batch] if batch else batches


def in_column_batches(method: ColumnPass) -> ColumnPass:
    """A pass over items that each name their two columns first, answered in `column_batches` and joined in their order."""

    @functools.wraps(method)
    def batched(backend: 'PolarsBackend', table: pl.LazyFrame, items: list[Any], *rest: Any) -> list[Any]:
        if not items:
            return []
        batches = column_batches(items, lambda item: item[:2], rows_of(table))
        return [answer for batch in batches for answer in method(backend, table, batch, *rest)]

    return cast(ColumnPass, batched)


def request_columns(request: Request) -> set[str]:
    """The columns a request's translation reads."""
    named = {request.column} if request.column else set()
    if request.statistic == 'unique_rows':
        return set(request.argument)
    if request.statistic == 'json_raw_type_counts':
        return named | {request.argument[1]}
    return named


def folded_by_parts(table: pl.LazyFrame, parts: list[pl.Expr], fold: pl.Expr) -> pl.Series:
    """Each expression's value at each row, folded by `fold` over `FOLDED` and `PART` into one column as each arrives, in
    the table's order. Each is collected alone, so only its own columns are read at once and only one answer waits."""

    def answer(part: pl.Expr) -> pl.Series:
        return table.select(part.alias(PART)).collect(engine='streaming').to_series()

    first, *rest = parts
    folded = answer(first).alias(FOLDED)
    for part in rest:
        folded = pl.DataFrame([folded, answer(part)]).select(fold.alias(FOLDED)).to_series()
    return folded


def distinct_rows(table: pl.LazyFrame, names: tuple[str, ...]) -> int:
    parts = [pl.struct(list(batch)).hash() for batch in in_batches(names, ROW_BY_ROW_COLUMNS)]
    return folded_by_parts(table, parts, pl.struct(FOLDED, PART).hash()).n_unique()


def unparsed_rows(table: pl.LazyFrame, columns: tuple[tuple[str, ParsedType], ...]) -> int:
    typed = [(name, parsed_type) for name, parsed_type in columns if parsed_type.raw_type != 'string']
    if not typed:
        return 0
    parts = [
        pl.any_horizontal([pl.col(name).is_not_null() & ~reads(pl.col(name), parsed_type) for name, parsed_type in batch])
        for batch in in_batches(typed, ROW_BY_ROW_COLUMNS)
    ]
    unparsed: int = folded_by_parts(table, parts, pl.any_horizontal(FOLDED, PART)).to_frame().select(pl.col(FOLDED).sum()).item()
    return unparsed


# Statistics reading many columns within each row, each answered on its own rather than as one expression of a batch.
ROW_BY_ROW_STATISTICS: dict[str, Callable[[pl.LazyFrame, Any], int]] = {'distinct_rows': distinct_rows, 'unparsed_rows': unparsed_rows}

untranslated = sorted({statistic.name for statistic in ALL_STATISTICS} - POLARS_STATISTICS.keys() - ROW_BY_ROW_STATISTICS.keys())
untranslated += sorted(f'the raw type {raw}' for raw in set(RAW_TYPES) - (TEXT_MATCHERS.keys() & TEXT_CONVERTERS.keys()))
if untranslated:
    raise RuntimeError(f'the Polars backend has no translation for {", ".join(untranslated)}')


def batch_answers(table: pl.LazyFrame, requests: list[Request], column: Callable[[str | None], pl.Expr]) -> dict[Request, Any]:
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


def ancestor_partition_keys(source: Source) -> list[str]:
    """Hive keys Polars reads from `key=value` directories above the root that was named, which the table does not hold."""
    every = {part.split('=', 1)[0] for file in source.files for part in PurePosixPath(file.path).parent.parts if '=' in part}
    return sorted(every - set(source.partition_keys))


@dc.dataclass(frozen=True)
class ColumnRewrite:
    """A table a parse or a recode gave, which holds it so its id stays its own, and what it was rewritten from and how."""

    table: pl.LazyFrame
    read_from: pl.LazyFrame
    columns: list[pl.Expr]


class PolarsBackend:
    name = 'polars'
    session_version: str | None = None

    def __init__(self) -> None:
        self.stage: ParquetStage | None = None
        self.rewrites: dict[int, ColumnRewrite] = {}

    def rewritten(self, table: pl.LazyFrame, columns: list[pl.Expr]) -> pl.LazyFrame:
        """`table` with `columns` rewritten row by row, remembered so that a sample of it can be drawn before the rewrite."""
        result = table.with_columns(columns)
        self.rewrites[id(result)] = ColumnRewrite(result, table, columns)
        return result

    def scan(self, source: Source) -> pl.LazyFrame:
        return self.read(source).table

    def read(self, source: Source) -> SourceRead:
        if self.stage is None:
            raise RuntimeError('the Polars backend reads a source inside profiling(), which owns what it stages')
        read = SourceRead(pl.DataFrame(source.table).lazy()) if source.table is not None else read_files(source, self.stage)
        return dc.replace(read, table=read.table.with_columns(cs.categorical().cast(pl.String), cs.enum().cast(pl.String)))

    def schema(self, table: pl.LazyFrame) -> list[ColumnSpec]:
        specs = []
        for name, dtype in table.collect_schema().items():
            spec = ColumnSpec(name, kind_of(dtype), polars_arrow_type(dtype))
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

        answers = {
            request: ROW_BY_ROW_STATISTICS[request.statistic](table, request.argument)
            for request in requests
            if request.statistic in ROW_BY_ROW_STATISTICS
        }
        by_column = [request for request in requests if request.statistic not in ROW_BY_ROW_STATISTICS]
        for batch in column_batches(by_column, request_columns, rows_of(table)):
            answers |= batch_answers(table, batch, column)
        return answers

    def top_k(self, table: pl.LazyFrame, column: str, k: int, of: FrequencyKey = FrequencyKey.VALUE) -> list[tuple[Any, int]]:
        key = pl.col(column)
        if of is FrequencyKey.SHAPE:
            key = key.str.replace_many(list(SHAPE_FROM), list(SHAPE_TO))
        elif of is FrequencyKey.ZIP_PREFIX:
            key = key.str.slice(0, 3)
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
        rows = rows_of(table)
        if rows > n:
            picked = pl.int_range(0, rows, eager=True).sample(n, seed=seed).sort()
            # A row index stops a filter moving below a rewrite, so each rewrite is put back above the filter by hand.
            rewrites: list[list[pl.Expr]] = []
            while rewrite := self.rewrites.get(id(table)):
                rewrites.insert(0, rewrite.columns)
                table = rewrite.read_from
            table = table.with_row_index(ROW_INDEX).filter(pl.col(ROW_INDEX).is_in(picked.implode())).drop(ROW_INDEX)
            for columns in rewrites:
                table = table.with_columns(columns)
        parts = [
            table.select(list(batch)).collect(engine='streaming').rechunk()
            for batch in in_batches(table.collect_schema().names(), ROW_BY_ROW_COLUMNS)
        ]
        return pl.concat(parts, how='horizontal').to_arrow() if parts else table.collect().to_arrow()

    def sampled(self, table: pl.LazyFrame, n: int, seed: int) -> pl.LazyFrame:
        return pl.DataFrame(self.sample(table, n, seed)).lazy()

    @contextlib.contextmanager
    def profiling(self) -> Iterator[None]:
        """Every query reading a source, and the stage holding its text, whose files go on every exit but a kill."""
        self.stage = ParquetStage()
        try:
            yield
        finally:
            self.stage.close()
            self.stage = None
            self.rewrites.clear()

    def parse(self, table: pl.LazyFrame, types: dict[str, ParsedType]) -> pl.LazyFrame:
        return self.rewritten(table, [parsed(pl.col(name), parsed_type).alias(name) for name, parsed_type in types.items()])

    def recoded(self, table: pl.LazyFrame, recodes: dict[str, Recode]) -> pl.LazyFrame:
        dtypes = dict(table.collect_schema())
        return self.rewritten(
            table,
            [
                pl.when(pl.col(name).is_in(pl.Series(recode.values, dtype=dtypes[name]).implode()))
                .then(pl.lit(recode.modal, dtype=dtypes[name]))
                .otherwise(pl.col(name))
                .alias(name)
                for name, recode in recodes.items()
            ],
        )

    def average_ranks(self, table: pl.LazyFrame, names: list[str]) -> list[pl.Expr]:
        """Each named column's values as their average ranks over its own values, a null left null, aliased `r0`, `r1`
        and on."""
        kinds = {spec.name: spec.kind for spec in self.schema(table)}
        return [numeric_value(pl.col(name), kinds[name]).rank('average').cast(pl.Float64).alias(f'r{at}') for at, name in enumerate(names)]

    def rank_correlations(self, table: pl.LazyFrame, pairs: list[tuple[str, str]]) -> list[tuple[float | None, int]]:
        if not pairs:
            return []
        names = list(dict.fromkeys(name for pair in pairs for name in pair))
        ranked = table.select(self.average_ranks(table, names)).collect(engine='streaming')
        found: list[tuple[float | None, int]] = []
        # A pair holds its two columns' ranks, filtered to the rows holding both, until its query ends.
        for batch in in_batches(pairs, max(1, BATCH_VALUES // max(2 * ranked.height, 1))):
            measured = []
            for index, (a, b) in enumerate(batch):
                first, second = pl.col(f'r{names.index(a)}'), pl.col(f'r{names.index(b)}')
                both = first.is_not_null() & second.is_not_null()
                measured += [pl.corr(first.filter(both), second.filter(both)).alias(f'c{index}'), both.sum().alias(f'n{index}')]
            row = ranked.select(measured).row(0)
            found += [
                (float(value) if value is not None and math.isfinite(value) else None, int(held))
                for value, held in zip(row[::2], row[1::2], strict=True)
            ]
        return found

    def rank_correlation_influence(self, table: pl.LazyFrame, pairs: list[tuple[str, str]]) -> list[float | None]:
        if not pairs:
            return []
        kinds = {spec.name: spec.kind for spec in self.schema(table)}
        frames = [
            spearman_influence_variance(
                table.select(numeric_value(pl.col(a), kinds[a]).alias('x'), numeric_value(pl.col(b), kinds[b]).alias('y'))
            )
            for a, b in pairs
        ]
        found = []
        for frame in pl.collect_all(frames, engine='streaming'):
            variance, spread_x, spread_y = frame.row(0)
            found.append(float(variance) if spread_x and spread_y and variance is not None and math.isfinite(variance) else None)
        return found

    @in_column_batches
    def cross_counts(self, table: pl.LazyFrame, pairs: list[tuple[str, str]]) -> list[list[tuple[str | None, str | None, int]]]:
        frames = [
            table.select(pl.col(a).cast(pl.String).alias('a'), pl.col(b).cast(pl.String).alias('b')).group_by('a', 'b').len()
            for a, b in pairs
        ]
        return [[(a, b, int(rows)) for a, b, rows in frame.iter_rows()] for frame in pl.collect_all(frames, engine='streaming')]

    def rank_moments(self, table: pl.LazyFrame, pairs: list[tuple[str, str]]) -> list[list[tuple[int, float, float]]]:
        numerics = list(dict.fromkeys(numeric for numeric, _ in pairs))
        by_category: dict[str, list[int]] = {}
        for index, (_, categorical) in enumerate(pairs):
            by_category.setdefault(categorical, []).append(index)
        categoricals = list(by_category)
        ranked = table.select(*(pl.col(name).alias(f'c{at}') for at, name in enumerate(categoricals)), *self.average_ranks(table, numerics))
        frames = []
        for at, indexes in enumerate(by_category.values()):
            moments = []
            for index in indexes:
                value = pl.col(f'r{numerics.index(pairs[index][0])}')
                moments += [value.count().alias(f'n{index}'), value.mean().alias(f'm{index}'), value.var(ddof=0).alias(f'v{index}')]
            frames.append(ranked.filter(pl.col(f'c{at}').is_not_null()).group_by(f'c{at}').agg(moments))
        answers: list[list[tuple[int, float, float]]] = [[] for _ in pairs]
        for indexes, frame in zip(by_category.values(), pl.collect_all(frames, engine='streaming'), strict=True):
            for index in indexes:
                rows = frame.select(f'n{index}', f'm{index}', f'v{index}').iter_rows()
                answers[index] = [(int(count), float(mean), float(variance)) for count, mean, variance in rows if count]
        return answers

    def dependency_counts(self, table: pl.LazyFrame, lhs: str, rhs: list[str], k_min: int) -> list[tuple[int, int, int]]:
        if not rhs:
            return []
        held = [pl.col(name).count().alias(f'n{index}') for index, name in enumerate(rhs)]
        varied = [(pl.col(name).min() != pl.col(name).max()).alias(f'v{index}') for index, name in enumerate(rhs)]
        groups = table.filter(pl.col(lhs).is_not_null()).group_by(lhs).agg(*held, *varied)
        floored = [pl.col(f'n{index}') >= k_min for index in range(len(rhs))]
        totals = groups.select(
            *(pl.col(f'n{index}').sum().alias(f'h{index}') for index in range(len(rhs))),
            *(pl.col(f'n{index}').filter(floor).sum().alias(f's{index}') for index, floor in enumerate(floored)),
            *(pl.col(f'n{index}').filter(floor & pl.col(f'v{index}')).sum().alias(f'x{index}') for index, floor in enumerate(floored)),
        )
        row = [int(value or 0) for value in totals.collect(engine='streaming').row(0)]
        return [(row[index], row[len(rhs) + index], row[2 * len(rhs) + index]) for index in range(len(rhs))]

    @in_column_batches
    def grouped_quantiles(
        self, table: pl.LazyFrame, requests: list[tuple[str, str, list[str], tuple[float, ...]]]
    ) -> list[list[tuple[str | None, int, list[float]]]]:
        kinds = {spec.name: spec.kind for spec in self.schema(table)}
        # Children grouped by one parent's same values share a scan and its grouping.
        shared: dict[tuple[str, tuple[str, ...]], list[int]] = {}
        for at, (_, given, listed, _) in enumerate(requests):
            shared.setdefault((given, tuple(listed)), []).append(at)
        frames = []
        for (given, keep), indexes in shared.items():
            text = pl.col(given).cast(pl.String)
            values = [numeric_value(pl.col(requests[at][0]), kinds[requests[at][0]]).alias(f'v{at}') for at in indexes]
            counted = []
            for at in indexes:
                value = pl.col(f'v{at}')
                counted += [
                    value.count().alias(f'n{at}'),
                    *(value.quantile(level, 'nearest').alias(f'q{at}_{index}') for index, level in enumerate(requests[at][3])),
                ]
            frames.append(
                table.filter(pl.col(given).is_not_null())
                .select(pl.when(text.is_in(list(keep))).then(text).alias('g'), *values)
                .group_by('g')
                .agg(counted)
            )
        found: list[list[tuple[str | None, int, list[float]]]] = [[] for _ in requests]
        for indexes, groups in zip(shared.values(), pl.collect_all(frames, engine='streaming'), strict=True):
            for row in groups.iter_rows(named=True):
                for at in indexes:
                    if row[f'n{at}']:
                        levels = range(len(requests[at][3]))
                        found[at].append((row['g'], int(row[f'n{at}']), [float(row[f'q{at}_{index}']) for index in levels]))
        return found

    @in_column_batches
    def grouped_rates(self, table: pl.LazyFrame, pairs: list[tuple[str, str]]) -> list[list[tuple[str | None, int, int, int]]]:
        kinds = {spec.name: spec.kind for spec in self.schema(table)}

        def zeros(name: str) -> pl.Expr:
            if kinds[name] in (ColumnKind.INTEGER, ColumnKind.FLOAT, ColumnKind.DECIMAL):
                return (pl.col(name) == 0).sum()
            return pl.lit(0, pl.UInt32)

        by_given: dict[str, list[int]] = {}
        for at, (given, _) in enumerate(pairs):
            by_given.setdefault(given, []).append(at)
        frames = [
            table.group_by(pl.col(given).cast(pl.String).alias('g')).agg(
                pl.len().alias('n'),
                *(pl.col(pairs[at][1]).is_null().sum().alias(f'u{at}') for at in indexes),
                *(zeros(pairs[at][1]).alias(f'z{at}') for at in indexes),
            )
            for given, indexes in by_given.items()
        ]
        found: list[list[tuple[str | None, int, int, int]]] = [[] for _ in pairs]
        for indexes, counted in zip(by_given.values(), pl.collect_all(frames, engine='streaming'), strict=True):
            for row in counted.iter_rows(named=True):
                for at in indexes:
                    found[at].append((row['g'], int(row['n']), int(row[f'u{at}']), int(row[f'z{at}'] or 0)))
        return found

    @in_column_batches
    def ordered_shares(self, table: pl.LazyFrame, pairs: list[tuple[str, str]]) -> list[tuple[int, int]]:
        epochs = self.epoch_micros(table, {name for pair in pairs for name in pair})
        counts = []
        for at, (first, second) in enumerate(pairs):
            both = epochs[first].is_not_null() & epochs[second].is_not_null()
            counts += [both.sum().alias(f'n{at}'), (both & (epochs[second] >= epochs[first])).sum().alias(f'o{at}')]
        row = table.select(counts).collect(engine='streaming').row(0)
        return [(int(held), int(ordered)) for held, ordered in zip(row[::2], row[1::2], strict=True)]

    @in_column_batches
    def lag_quantiles(
        self, table: pl.LazyFrame, pairs: list[tuple[str, str]], levels: tuple[float, ...], k_min: int
    ) -> list[tuple[int, list[float], float | None, float | None]]:
        epochs = self.epoch_micros(table, {name for pair in pairs for name in pair})
        measured = []
        for at, (first, second) in enumerate(pairs):
            lag = (epochs[second] - epochs[first]).cast(pl.Float64)
            measured += [
                lag.count().alias(f'n{at}'),
                *(lag.quantile(level, 'nearest').alias(f'q{at}_{index}') for index, level in enumerate(levels)),
                lag.bottom_k(k_min).max().alias(f'l{at}'),
                lag.top_k(k_min).min().alias(f'h{at}'),
            ]
        row = table.select(measured).collect(engine='streaming').row(0, named=True)
        found = []
        for at in range(len(pairs)):
            held = int(row[f'n{at}'])
            quantiles = [float(row[f'q{at}_{index}']) for index in range(len(levels))] if held else []
            low, high = (row[f'l{at}'], row[f'h{at}']) if held >= k_min else (None, None)
            found.append((held, quantiles, low, high))
        return found

    def expression_matches(self, table: pl.LazyFrame, candidates: list[ExpressionCandidate]) -> list[tuple[int, int, int]]:
        if not candidates:
            return []
        kinds = {spec.name: spec.kind for spec in self.schema(table)}

        def value(name: str) -> pl.Expr:
            return numeric_value(pl.col(name), kinds[name]).cast(pl.Float64)

        counts = []
        for at, candidate in enumerate(candidates):
            held, informative, matched = counted_rows(candidate, value, PolarsOps())
            counts += [held.sum().alias(f'n{at}'), informative.sum().alias(f'i{at}'), matched.sum().alias(f'm{at}')]
        row = table.select(counts).collect(engine='streaming').row(0)
        return [(int(row[at]), int(row[at + 1]), int(row[at + 2])) for at in range(0, len(row), 3)]

    def epoch_micros(self, table: pl.LazyFrame, names: set[str]) -> dict[str, pl.Expr]:
        """Each named date or timestamp column as microseconds since the epoch."""
        schema = table.collect_schema()
        return {name: (pl.col(name).cast(pl.Datetime('us')) if schema[name] == pl.Date else pl.col(name)).dt.epoch('us') for name in names}

    def foreign_key_counts(
        self, child: pl.LazyFrame, child_columns: list[str], parent: pl.LazyFrame, parent_columns: list[str]
    ) -> list[tuple[bool, int, int]]:
        keys = [f'key{at}' for at in range(len(child_columns))]
        children = child.select(join_keys(child, child_columns)).drop_nulls().group_by(keys).len('children')
        parents = parent.select(join_keys(parent, parent_columns)).drop_nulls().unique().with_columns(parent=pl.lit(True))
        joined = parents.join(children, on=keys, how='full', coalesce=True)
        counted = joined.group_by(pl.col('parent').fill_null(False), pl.col('children').fill_null(0)).len('values')
        return [(bool(held), int(rows), int(values)) for held, rows, values in counted.collect(engine='streaming').iter_rows()]

    def inherited_counts(
        self,
        child: pl.LazyFrame,
        child_columns: list[str],
        parent: pl.LazyFrame,
        parent_columns: list[str],
        pairs: list[tuple[str, str]],
    ) -> tuple[int, list[int]]:
        keys = [f'key{at}' for at in range(len(child_columns))]
        compared_child = sorted({name for name, _ in pairs})
        compared_parent = sorted({name for _, name in pairs})
        children = child.select(*join_keys(child, child_columns), *(pl.col(name).alias(f'c:{name}') for name in compared_child))
        parents = parent.select(*join_keys(parent, parent_columns), *(pl.col(name).alias(f'p:{name}') for name in compared_parent))
        joined = children.drop_nulls(keys).join(parents.drop_nulls(keys).unique(keys), on=keys, how='inner')
        equal = [pl.col(f'c:{a}').eq_missing(pl.col(f'p:{b}')).sum().alias(str(at)) for at, (a, b) in enumerate(pairs)]
        counted = joined.select(pl.len().alias('joined'), *equal).collect(engine='streaming').row(0)
        return int(counted[0]), [int(value or 0) for value in counted[1:]]

    def ordinal_counts(self, child: pl.LazyFrame, child_columns: list[str], candidates: list[str]) -> tuple[int, list[int]]:
        keys = [f'key{at}' for at in range(len(child_columns))]
        held = [pl.col(f'c{at}') for at in range(len(candidates))]
        per_key = (
            child.select(*join_keys(child, child_columns), *(pl.col(name).alias(f'c{at}') for at, name in enumerate(candidates)))
            .drop_nulls(keys)
            .group_by(keys)
            .agg(
                pl.len().alias('rows'),
                *(value.min().alias(f'least{at}') for at, value in enumerate(held)),
                *(value.max().alias(f'greatest{at}') for at, value in enumerate(held)),
                *(value.drop_nulls().n_unique().alias(f'distinct{at}') for at, value in enumerate(held)),
            )
        )
        numbered = [
            ((pl.col(f'least{at}') == 1) & (pl.col(f'greatest{at}') == pl.col('rows')) & (pl.col(f'distinct{at}') == pl.col('rows')))
            .sum()
            .alias(f'numbered{at}')
            for at in range(len(candidates))
        ]
        counted = per_key.select(pl.len().alias('keys'), *numbered).collect(engine='streaming').row(0)
        return int(counted[0]), [int(value or 0) for value in counted[1:]]


def numeric_value(c: pl.Expr, kind: ColumnKind) -> pl.Expr:
    """A column as a rank or a moment reads it: a decimal as a double, and a float's NaN and infinities as null."""
    if kind is ColumnKind.DECIMAL:
        return c.cast(pl.Float64)
    if kind is ColumnKind.FLOAT:
        return pl.when(c.is_finite()).then(c)
    return c


def spearman_influence_variance(pair: pl.LazyFrame) -> pl.LazyFrame:
    """One row: the mean squared `row_influence` of a row of `pair` on the Spearman correlation of its `x` and `y`, over
    the rows holding both, and the variance of each's mid-distribution function there, zero for a constant column."""
    held = pair.filter(pl.col('x').is_not_null() & pl.col('y').is_not_null())
    rows = pl.len().cast(pl.Float64)
    centered = held.with_columns(
        u=(pl.col('x').rank('average').cast(pl.Float64) - 0.5) / rows - 0.5,
        w=(pl.col('y').rank('average').cast(pl.Float64) - 0.5) / rows - 0.5,
    )

    def above(frame: pl.LazyFrame, by: str, of: str, name: str) -> pl.LazyFrame:
        """Each row's sum of `of` over the rows above it in `by`, and half of it over the rows tied with it, over the rows."""
        through = frame.sort(by).with_columns(pl.col(of).cum_sum().alias('_through'))
        tied_or_below = pl.col('_through').last().over(by)
        return through.with_columns(((pl.col(of).sum() - tied_or_below + pl.col(of).sum().over(by) / 2) / rows).alias(name)).drop(
            '_through'
        )

    by_x = above(above(centered, 'x', 'w', 'g_x'), 'x', 'u', 'h_x')
    both = above(above(by_x, 'y', 'u', 'g_y'), 'y', 'w', 'h_y')
    u, w = pl.col('u'), pl.col('w')
    covariance, spread_x, spread_y = (u * w).mean(), (u * u).mean(), (w * w).mean()
    terms = InfluenceTerms(u, w, pl.col('g_x'), pl.col('h_x'), pl.col('g_y'), pl.col('h_y'))
    influence = row_influence(terms, covariance, spread_x, spread_y)
    return both.select((influence * influence).mean().alias('variance'), spread_x.alias('spread_x'), spread_y.alias('spread_y'))


class PolarsOps:
    """The operations `expression_rule.counted_rows` counts with, over Polars expressions."""

    def arithmetic(self, operator: str, left: pl.Expr, right: pl.Expr) -> pl.Expr:
        return {'+': left + right, '-': left - right, '*': left * right}[operator]

    def quotient(self, left: pl.Expr, right: pl.Expr) -> pl.Expr:
        return pl.when(right != 0).then(left / right)

    def held(self, value: pl.Expr) -> pl.Expr:
        return value.is_not_null()

    def is_zero(self, value: pl.Expr) -> pl.Expr:
        return value == 0

    def within(self, value: pl.Expr, tolerance: float) -> pl.Expr:
        return (value.abs() <= tolerance).fill_null(False)

    def both(self, first: pl.Expr, second: pl.Expr) -> pl.Expr:
        return first & second

    def negated(self, condition: pl.Expr) -> pl.Expr:
        return ~condition


def join_key(table: pl.LazyFrame, name: str) -> pl.Expr:
    """A key column as both sides of a join hold it: text as text, and any integer as an int64."""
    return pl.col(name).cast(pl.Int64, strict=False) if table.collect_schema()[name].is_integer() else pl.col(name)


def join_keys(table: pl.LazyFrame, names: list[str]) -> list[pl.Expr]:
    """Each key column as `join_key` holds it, named `key0`, `key1` and on, so both sides join on the same names."""
    return [join_key(table, name).alias(f'key{at}') for at, name in enumerate(names)]


def large_temp_directory() -> str:
    """Where a large temporary file goes: `$TMPDIR`, else a directory usually on disk, else the platform's own."""
    for directory in (os.environ.get('TMPDIR'), LARGE_TEMP_DIRECTORY):
        if directory and os.access(directory, os.W_OK | os.X_OK):
            return directory
    return tempfile.gettempdir()


class ParquetStage:
    """The Parquet files DuckDB wrote one profile's text sources to, so their rows wait on disk for Polars to scan. The
    directory holding them is made at the first file."""

    def __init__(self) -> None:
        self.directory: tempfile.TemporaryDirectory[str] | None = None
        self.files = 0

    def connected(self) -> duckdb.DuckDBPyConnection:
        """A DuckDB session that writes a staged file, and spills anything else it must under the same directory."""
        if self.directory is None:
            self.directory = tempfile.TemporaryDirectory(prefix=STAGE_DIRECTORY_PREFIX, dir=large_temp_directory())
        return duckdb.connect(config=STAGE_DUCKDB_CONFIG | {'temp_directory': self.directory.name})

    def staged(self, connection: duckdb.DuckDBPyConnection, query: str, parameters: dict[str, Any]) -> pl.LazyFrame:
        """`query`'s rows, in its order, written to a file of their own and scanned from it."""
        if self.directory is None:
            raise RuntimeError('a stage writes through the connection connected() opened')
        self.files += 1
        path = f'{self.directory.name}/{self.files}.parquet'
        options = f'format parquet, compression zstd, compression_level 1, row_group_size {STAGE_ROW_GROUP_ROWS}'
        connection.execute(f'copy ({query}) to {sql_text(path)} ({options})', parameters)
        return pl.scan_parquet(path, glob=False, hive_partitioning=False)

    def close(self) -> None:
        if self.directory is not None:
            self.directory.cleanup()


def read_files(source: Source, stage: ParquetStage) -> SourceRead:
    """The source's files as one table, read far enough to find a file that is not of its format, and what reading them found."""
    format_name = source.format.value if source.format else 'a table'
    paths = [file.path for file in source.files]
    hive = bool(source.partition_keys)
    # Built here, on the calling thread, so the scan holds the AWS profile the call chose wherever Polars reads it.
    credentials: Any = polars_credentials() if any(path.startswith('s3://') for path in paths) else 'auto'
    try:
        match source.format:
            case SourceFormat.PARQUET:
                every = parquet_footers.read_footers(source.files)
                scan = pl.scan_parquet(paths, hive_partitioning=hive, credential_provider=credentials, **schema_union(every))
                read = SourceRead(scan, footers=parquet_footers.sampled_footers(source.files, every))
            case SourceFormat.ARROW:
                read = SourceRead(pl.scan_ipc(paths, hive_partitioning=hive, credential_provider=credentials))
            case SourceFormat.CSV | SourceFormat.TSV:
                read = read_delimited(source, stage)
            case _:
                read = read_json(source, stage)
        table = read.table.drop(ancestor_partition_keys(source), strict=False)
        # Counting rows reads every file's footer, so a corrupt file fails here rather than mid-profile.
        table.select(pl.len()).collect(engine='streaming')
    except (pl.exceptions.PolarsError, duckdb.Error, OSError, pa.ArrowException) as error:
        message = f'{", ".join(source.roots)} could not be read as {format_name}: {first_line(error)}'
        raise SourceError(issue_codes.DATA_UNREADABLE, message) from error
    return dc.replace(read, table=table)


def schema_union(footers: tuple[ParquetFooter, ...]) -> dict[str, Any]:
    """The scan options reading files whose schemas differ, as a drifted dataset's do, as one table. Each column any footer
    read holds is read at the widest type one stores it as, and is null in a file that lacks it. A column two files store as
    types neither widens to, such as an integer and a string, raises ArrowTypeError.
    """
    distinct: list[pa.Schema] = []
    for footer in footers:
        if not any(footer.schema.equals(seen) for seen in distinct):
            distinct.append(footer.schema)
    if len(distinct) < 2:
        return {}
    union = pa.unify_schemas(distinct, promote_options='permissive')
    return {
        'schema': pl.DataFrame(union.empty_table()).schema,
        'missing_columns': 'insert',
        'extra_columns': 'ignore',
        'cast_options': pl.ScanCastOptions(integer_cast='upcast', float_cast='upcast'),
    }


def first_line(error: Exception) -> str:
    return str(error).strip().splitlines()[0] if str(error).strip() else type(error).__name__


def read_delimited(source: Source, stage: ParquetStage) -> SourceRead:
    """CSV or TSV read through DuckDB in the detected dialect with every column as text, staged for Polars to scan.

    A row DuckDB rejects is left out of the table and counted by its first error. A quoted empty field is an empty
    string, where a bare one is null.
    """
    found = importlib.import_module(DELIMITED_DIALECT_MODULE).detected(
        source.files, '\t' if source.format is SourceFormat.TSV else None, DUCKDB_CONFIG, source.dialect
    )
    dialect = found.dialect
    query = (
        'select * from read_csv($paths, auto_detect = false, columns = $columns, delim = $delimiter, quote = $quote, '
        'escape = $escape, header = $header, encoding = $encoding, allow_quoted_nulls = false, strict_mode = true, store_rejects = true, '
        'filename = $file_column, hive_partitioning = $hive, hive_types_autocast = false)'
    )
    parameters = {
        'paths': [file.path for file in source.files],
        'columns': {name: 'VARCHAR' for name in dialect.names},
        'delimiter': dialect.delimiter,
        'quote': dialect.quote,
        'escape': dialect.escape,
        'header': dialect.header,
        'encoding': dialect.encoding,
        'file_column': FILE_COLUMN,
        'hive': bool(source.partition_keys),
    }
    with stage.connected() as connection:
        table = stage.staged(connection, query, parameters)
        rejected = {error.lower().replace(' ', '_'): rows for error, rows in selected(connection, REJECTED_ROWS, {}).fetchall()}
    return SourceRead(table.drop(FILE_COLUMN), dialect=found, rejected=rejected, file_rows=rows_by_file(table, source))


def rows_by_file(table: pl.LazyFrame, source: Source) -> tuple[int, ...]:
    counted = dict(table.group_by(FILE_COLUMN).len().collect(engine='streaming').iter_rows())
    return tuple(counted.get(file.path, 0) for file in source.files)


def ends_each_line(block: bytes) -> bool:
    """Whether every whole line of a file's first block holds one JSON value, as NDJSON's lines do."""
    whole = block.splitlines()[:-1] if len(block) >= FIRST_BLOCK_BYTES else block.splitlines()
    try:
        held = [json.loads(line) for line in whole if line.strip()]
    except ValueError:
        return False
    return bool(held) and not block.lstrip().startswith(b'[')


def read_json(source: Source, stage: ParquetStage) -> SourceRead:
    """JSON read through DuckDB, a flat column for each path and a table for each array's elements, staged for Polars to scan."""
    paths = [file.path for file in source.files]
    block = first_block(paths[0])
    lines = source.format is SourceFormat.NDJSON or ends_each_line(block)
    # DuckDB's `auto` reads objects concatenated with no delimiter, as Firehose writes them without its newline
    # delimiter, and pretty-printed ones.
    layout = sql_text('newline_delimited' if lines else 'array' if block.lstrip().startswith(b'[') else 'auto')
    records = f'read_json_objects($paths, format = {layout}, filename = {sql_text(FILE_COLUMN)})'
    parameters = {'paths': paths}
    with stage.connected() as connection:
        structure = selected(connection, f'select json_group_structure(json) from {records}', parameters).fetchall()[0][0]
        root = json_paths.record_tree(json.loads(structure) if structure else {})
        reading = JsonTableReading(connection, stage, parameters, {})
        table = json_table(reading, f'select * from {records}', root, hive_values(source.partition_keys, root))
    return SourceRead(
        table.drop(FILE_COLUMN),
        file_rows=rows_by_file(table, source),
        json=JsonRead(root, reading.elements, lines),
    )


@dc.dataclass(frozen=True)
class JsonTableReading:
    """What every table of one JSON source is read with, and each array's element table by the array's path."""

    connection: duckdb.DuckDBPyConnection
    stage: ParquetStage
    parameters: dict[str, Any]
    elements: dict[str, Any]


def json_table(reading: JsonTableReading, relation: str, root: JsonNode, hive: Sequence[str] = ()) -> pl.LazyFrame:
    """The table `root` heads, read from `relation`, with each array's elements read into `elements` by the array's path,
    and each of `hive`'s selections after its own columns."""
    selections = [FILE_COLUMN] if root.path == '' else []
    for node, path in json_paths_of(root):
        text = sql_text(path)
        selections.append(f'json_type(json, {text}) as {sql_name(json_paths.type_column(node.column))}')
        if node.kind == 'scalar':
            selections.append(f'json_extract_string(json, {text}) as {sql_name(node.column)}')
        elif node.kind == 'array' and node.element is not None:
            selections.append(f'json_array_length(json, {text})::bigint as {sql_name(json_paths.length_column(node.column))}')
            items = f'select unnest(json_extract(json, {sql_text(path + "[*]")})) as json from ({relation})'
            reading.elements[node.path] = json_table(reading, items, node.element)
    return reading.stage.staged(reading.connection, f'select {", ".join([*selections, *hive])} from ({relation})', reading.parameters)


def hive_values(keys: tuple[str, ...], root: JsonNode) -> list[str]:
    """The selection of each Hive key's text value in each record's file path, for a key no record holds as a field."""
    fields = {key for key, _ in root.fields}
    selections = []
    for key in keys:
        if key not in fields:
            directory = sql_text(f'/{re.escape(key)}=([^/]*)/')
            selections.append(f'nullif(url_decode(regexp_extract({FILE_COLUMN}, {directory}, 1)), {HIVE_NULL}) as {sql_name(key)}')
    return selections


def json_paths_of(root: JsonNode) -> list[tuple[JsonNode, str]]:
    """Each node of the table `root` heads, with its JSON path from the table's record or element."""
    found = [(root, '$')] if root.column else []

    def walk(node: JsonNode, path: str) -> None:
        for key, child in node.fields:
            child_path = f'{path}."{key}"'
            found.append((child, child_path))
            if child.kind == 'object':
                walk(child, child_path)

    walk(root, '$')
    return found


def sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_name(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def selected(connection: duckdb.DuckDBPyConnection, query: str, parameters: dict[str, Any]) -> duckdb.DuckDBPyRelation:
    relation = connection.sql(query, params=parameters)
    if relation is None:
        raise RuntimeError(f'DuckDB returned no relation for {query!r}')
    return relation
