"""What a profiling engine provides, and the values that cross between it and the profiler."""

import dataclasses as dc
import enum
import hashlib
from typing import TYPE_CHECKING
from typing import Any
from typing import Protocol

from doppeldata.profiling.statistic_catalog import ColumnKind
from doppeldata.profiling.statistic_catalog import FrequencyKey
from doppeldata.published_schemas import published_enum

if TYPE_CHECKING:
    import pyarrow as pa

# A table as the backend holds it; Polars' is a LazyFrame.
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

unspelled = sorted({source_format.value for source_format in SourceFormat} - set(published_enum('SourceFormat')))
if unspelled:
    raise RuntimeError(f'the profile format has no source.format {", ".join(unspelled)}')


@dc.dataclass(frozen=True)
class CsvDialect:
    """How a delimited text file is read. The default is RFC 4180 with a header row."""

    delimiter: str = ','
    quote: str = '"'
    escape: str = '"'
    header: bool = True


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
    dialect: CsvDialect = CsvDialect()
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


class Backend(Protocol):
    """A profiling engine. `aggregate` answers every request it is given in one query."""

    name: str

    def scan(self, source: Source) -> Table: ...

    def schema(self, table: Table) -> list[ColumnSpec]: ...

    def aggregate(self, table: Table, requests: list[Request]) -> dict[Request, Any]: ...

    def top_k(self, table: Table, column: str, k: int, of: FrequencyKey = FrequencyKey.VALUE) -> list[tuple[Any, int]]:
        """The k most frequent non-null values or shapes and their row counts, most frequent first, ties by value."""
        ...

    def sample(self, table: Table, n: int, seed: int) -> 'pa.Table':
        """A seeded uniform sample of n rows, or every row when the table holds no more."""
        ...

    def parse(self, table: Table, types: dict[str, ParsedType]) -> Table:
        """The text table with each named column parsed as its type. A value that does not parse becomes null."""
        ...
