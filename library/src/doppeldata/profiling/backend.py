"""What a profiling engine provides, and the values that cross between it and the profiler."""

import dataclasses as dc
import enum
import hashlib
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING
from typing import Any
from typing import Protocol

from doppeldata.profiling.expression_rule import ExpressionCandidate
from doppeldata.profiling.statistic_catalog import ColumnKind
from doppeldata.profiling.statistic_catalog import FrequencyKey
from doppeldata.published_schemas import published_enum

if TYPE_CHECKING:
    import pyarrow as pa

# A table as the backend holds it: Polars' is a LazyFrame, and Spark's a DataFrame.
Table = Any


class SourceFormat(enum.Enum):
    """A source's file format, spelled as a profile's `source.format`."""

    PARQUET = 'parquet'
    ARROW = 'arrow'
    CSV = 'csv'
    TSV = 'tsv'
    JSON = 'json'
    NDJSON = 'ndjson'


@dc.dataclass(frozen=True)
class FormatTraits:
    """The file suffixes a format is recognized by, and whether its values arrive as text to be typed."""

    suffixes: tuple[str, ...]
    text: bool


FORMAT_TRAITS = {
    SourceFormat.PARQUET: FormatTraits(('.parquet', '.pq'), text=False),
    SourceFormat.ARROW: FormatTraits(('.arrow', '.feather', '.ipc'), text=False),
    SourceFormat.CSV: FormatTraits(('.csv',), text=True),
    SourceFormat.TSV: FormatTraits(('.tsv', '.tab'), text=True),
    SourceFormat.JSON: FormatTraits(('.json',), text=True),
    SourceFormat.NDJSON: FormatTraits(('.ndjson', '.jsonl'), text=True),
}
TEXT_FORMATS = frozenset(source_format for source_format, traits in FORMAT_TRAITS.items() if traits.text)
# The codec a text file is compressed with, by the suffix after its format's. DuckDB reads each.
TEXT_CODEC_SUFFIXES = {'.gz': 'gzip', '.zst': 'zstd', '.bz2': 'bzip2'}

unspelled = sorted({source_format.value for source_format in SourceFormat} - set(published_enum('SourceFormat')))
if unspelled:
    raise RuntimeError(f'the profile format has no source.format {", ".join(unspelled)}')


@dc.dataclass(frozen=True)
class CsvDialect:
    """How a delimited text file is read. The default is RFC 4180 with a header row.

    An empty `quote` reads no field as quoted. `encoding` is spelled as DuckDB's `read_csv` takes it.
    """

    delimiter: str = ','
    quote: str = '"'
    escape: str = '"'
    header: bool = True
    encoding: str = 'utf-8'
    # The columns' names, from the header row or numbered where there is none.
    names: tuple[str, ...] = ()


@dc.dataclass(frozen=True)
class DialectFound:
    """A delimited source's dialect, how each sniffer read the first block, and what that block's bytes hold. A dialect
    the source was known to be written in is read with no sniffer, so `named` is empty and `agreed` and `chosen` None."""

    dialect: CsvDialect
    # Each sniffer's delimiter, quote and escape by its name, the one read first leading, or None where it found no
    # dialect. A quote or escape it named none of is empty.
    named: dict[str, tuple[str, str, str] | None]
    # Whether the two read the same delimiter and quote, a quote neither names being read as `"`.
    agreed: bool | None
    # The sniffer whose dialect was read, or `default` where neither named one.
    chosen: str | None
    # The share of the block's records that split into the most common number of fields under `dialect`.
    confidence: float
    # charset-normalizer's name for the block's encoding, such as `ascii` or `cp1252`.
    detected_encoding: str | None
    bom: bool
    # The block's line breaks counted by terminator, `lf` and `crlf`.
    line_terminators: dict[str, int]
    # `always`, `non_numeric` or `necessary`, as the block's quoted fields show.
    quoting: str
    trailing_delimiter: bool


@dc.dataclass(frozen=True)
class ParquetFooter:
    """What one Parquet file's footer says of how it was written. It holds no value statistic."""

    rows: int
    created_by: str | None
    format_version: str
    spark_version: str | None
    # Each row group's rows and its column chunks' compressed bytes.
    row_groups: tuple[tuple[int, int], ...]
    # The codec most column chunks use, as Parquet names it, such as `SNAPPY`.
    codec: str | None
    # Every leaf column's dotted path, and those whose chunks in every row group carry a dictionary page.
    columns: tuple[str, ...]
    dictionary_columns: tuple[str, ...]
    # Whether most column chunks record statistics, and whether most have a column and offset index.
    statistics: bool
    page_index: bool
    # The columns every row group declares itself sorted by, in order.
    sorting_columns: tuple[str, ...]
    # Each timestamp column's stored unit, `ms`, `us`, `ns` or `int96`.
    timestamp_units: dict[str, str]
    # Each decimal column's scale.
    decimal_scales: dict[str, int]
    # The file's columns as Arrow types.
    schema: 'pa.Schema'


@dc.dataclass(frozen=True)
class SourceRead:
    """A source's table, and what reading its files found beside the rows."""

    table: 'Table'
    dialect: DialectFound | None = None
    # The rows a reader rejected by DuckDB's error type, in snake case, each row counted once.
    rejected: dict[str, int] = dc.field(default_factory=dict)
    # Each file's rows, where the reader counts them per file.
    file_rows: tuple[int, ...] = ()
    footers: tuple[ParquetFooter, ...] = ()
    json: 'JsonRead | None' = None


@dc.dataclass(frozen=True)
class JsonRead:
    """A JSON source's path tree, a table of each array's elements by the array's `path`, and whether a record ends each line."""

    paths: 'JsonNode'
    elements: dict[str, 'Table']
    lines: bool


@dc.dataclass(frozen=True)
class JsonNode:
    """One path of a JSON record: an object's fields, an array's element, or a scalar.

    `kind` is `object`, `array` or `scalar`. `column` names the path's flat column in its table, the record's or an
    array's elements', and `path` is the dotted path from the record, unique across every table.
    """

    column: str
    path: str
    kind: str
    fields: tuple[tuple[str, 'JsonNode'], ...] = ()
    element: 'JsonNode | None' = None


HIDDEN_COLUMN_PREFIX = '__doppeldata'


def is_hidden(column: str) -> bool:
    """Whether a column is one the reader added, such as a JSON value's type, which no profile column describes."""
    return column.startswith(HIDDEN_COLUMN_PREFIX)


@dc.dataclass(frozen=True)
class SourceFile:
    """One file of a source: a local path or an `s3://` URL, its size, and what changes when its content does."""

    path: str
    size: int
    # The local modification time in nanoseconds, or the S3 ETag.
    revision: str
    # The `key=value` directories between the root that was named and the file, as Hive partition keys.
    partition_keys: tuple[str, ...] = ()


@dc.dataclass(frozen=True)
class Source:
    """The data one profile describes: files of one format, or a table already in memory."""

    format: SourceFormat | None
    files: tuple[SourceFile, ...] = ()
    # Any object exporting the Arrow PyCapsule stream interface, such as a pyarrow, Polars or pandas table.
    table: Any = None
    # The dialect a delimited source is known to be written in, read with no sniffer, or None to sniff it.
    dialect: CsvDialect | None = None
    # The name the profile takes when none is given: the file's stem or the directory's name.
    name: str = 'table'
    # The paths and URLs as they were named, which an error about the source quotes.
    roots: tuple[str, ...] = ()

    @property
    def partition_keys(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(key for file in self.files for key in file.partition_keys))

    @property
    def fingerprint(self) -> str | None:
        """`sha256:<hex>` over each file's path, size and revision as listed, never its content.

        It tells two listings apart, and nothing more: a file rewritten between the listing and the read
        is profiled new and fingerprinted old.
        """
        if not self.files:
            return None
        lines = sorted(f'{file.path}\t{file.size}\t{file.revision}' for file in self.files)
        return 'sha256:' + hashlib.sha256('\n'.join(lines).encode()).hexdigest()


@dc.dataclass(frozen=True)
class ColumnSpec:
    """One column as the backend reads it, with its Arrow type as pyarrow spells it."""

    name: str
    kind: ColumnKind
    physical_type: str
    precision: int | None = None
    scale: int | None = None
    timezone: str | None = None


@dc.dataclass(frozen=True)
class Request:
    """One statistic of one column, or of the table when `column` is None."""

    column: str | None
    statistic: str
    argument: Any = None


@dc.dataclass(frozen=True)
class ParsedType:
    """The type a text column's values are parsed as, and the strftime format of a date or timestamp."""

    raw_type: str
    format: str | None = None
    # Text tokens read as null, such as `NULL`.
    missing: tuple[str, ...] = ()
    # The column holding each JSON value's JSON type, which decides its raw type in place of its text.
    json_type_column: str | None = None


class Backend(Protocol):
    """A profiling engine. `aggregate` answers every request it is given, in as few queries as the engine compiles.

    A statistic of one column or of the whole table is a `statistic_catalog` entry that `aggregate` answers. One measured
    over pairs of columns, or across two tables, is a method of its own, because each engine shapes that query its own way.
    """

    name: str
    # The version of the session that reads the files, as a Spark cluster's, or None for an engine running in this process.
    session_version: str | None

    def scan(self, source: Source) -> Table: ...

    def read(self, source: Source) -> SourceRead:
        """The source's table, with what reading its files found beside the rows."""
        ...

    def schema(self, table: Table) -> list[ColumnSpec]: ...

    def aggregate(self, table: Table, requests: list[Request]) -> dict[Request, Any]: ...

    def top_k(self, table: Table, column: str, k: int, of: FrequencyKey = FrequencyKey.VALUE) -> list[tuple[Any, int]]:
        """The k most frequent non-null values, shapes or ZIP prefixes and their row counts, most frequent first, ties by value."""
        ...

    def sample(self, table: Table, n: int, seed: int) -> 'pa.Table':
        """A seeded uniform sample of n rows, or every row when the table holds no more."""
        ...

    def sampled(self, table: Table, n: int, seed: int) -> Table:
        """A seeded sample of n rows as a table the engine queries, so a cluster's sample never crosses to the driver."""
        ...

    def profiling(self) -> AbstractContextManager[None]:
        """What every query of one profile runs inside, such as a Spark session's settings."""
        ...

    def parse(self, table: Table, types: dict[str, ParsedType]) -> Table:
        """The text table with each named column parsed as its type. A value that does not parse becomes null."""
        ...

    def rank_correlations(self, table: Table, pairs: list[tuple[str, str]]) -> list[tuple[float | None, int]]:
        """Each pair's Spearman correlation over the rows holding both, and how many rows those are. Each column is
        ranked once over its own values with ties at their mean rank. The correlation is None where either column is
        constant over those rows."""
        ...

    def cross_counts(self, table: Table, pairs: list[tuple[str, str]]) -> list[list[tuple[str | None, str | None, int]]]:
        """Each pair's rows by its two values written as text, a null as None, over every row."""
        ...

    def rank_moments(self, table: Table, pairs: list[tuple[str, str]]) -> list[list[tuple[int, float, float]]]:
        """For each pair of a numeric column and a categorical one, each categorical value's rows holding a numeric value,
        and the mean and population variance of those rows' numeric ranks. Each numeric column is ranked once over its
        own values, ties at their mean rank, as `rank_correlations` ranks it."""
        ...

    def dependency_counts(self, table: Table, lhs: str, rhs: list[str], k_min: int) -> list[tuple[int, int, int]]:
        """For each column of `rhs`, the rows holding it and `lhs`, those of them whose `lhs` value at least k_min such
        rows hold, and those of these whose `lhs` value holds more than one `rhs` value."""
        ...

    def grouped_quantiles(
        self, table: Table, requests: list[tuple[str, str, list[str], tuple[float, ...]]]
    ) -> list[list[tuple[str | None, int, list[float]]]]:
        """For each `(child, given, keep, levels)`, the numeric `child` within each value of `given` that `keep` lists,
        written as text, and within every other value not null as one group keyed None: each group's rows holding a
        child value, and the child's quantile at each of `levels` over them, taken as `aggregate` takes a column's
        quantiles. A group holding no child value is left out."""
        ...

    def grouped_rates(self, table: Table, pairs: list[tuple[str, str]]) -> list[list[tuple[str | None, int, int, int]]]:
        """For each `(given, child)` pair, each value of `given` written as text, a null as None, with its rows, those
        whose child is null, and those whose child is zero. A child that is not numeric holds no zero."""
        ...

    def ordered_shares(self, table: Table, pairs: list[tuple[str, str]]) -> list[tuple[int, int]]:
        """For each pair of dates or timestamps, the rows holding both, and those whose second is at or after its first."""
        ...

    def lag_quantiles(
        self, table: Table, pairs: list[tuple[str, str]], levels: tuple[float, ...], k_min: int
    ) -> list[tuple[int, list[float], float | None, float | None]]:
        """For each pair of dates or timestamps, the rows holding both, and of the second less the first, in
        microseconds, its quantile at each of `levels` and its value at rank k_min from each end, None where fewer
        rows hold both."""
        ...

    def expression_matches(self, table: Table, candidates: list[ExpressionCandidate]) -> list[tuple[int, int, int]]:
        """For each candidate, the rows `expression_rule.counted_rows` selects: those holding its three columns, the
        informative ones among them, and those of the latter where the target is its result."""
        ...

    def foreign_key_counts(self, child: Table, child_column: str, parent: Table, parent_column: str) -> list[tuple[bool, int, int]]:
        """The values of both columns, counted as `(in_parent, child_rows, values)`: how many values the parent holds or
        not that `child_rows` child rows hold. A parent value no child row holds is counted at 0 child rows."""
        ...
