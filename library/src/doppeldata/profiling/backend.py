"""What a profiling engine provides, and the values that cross between it and the profiler."""

import dataclasses as dc
import enum
import hashlib
from typing import TYPE_CHECKING
from typing import Any
from typing import Protocol

from doppeldata.profiling.statistic_catalog import ColumnKind
from doppeldata.profiling.statistic_catalog import FrequencyKey

if TYPE_CHECKING:
    import pyarrow as pa

# A table as the backend holds it: a Polars LazyFrame, or a Spark DataFrame.
Table = Any


class SourceFormat(enum.Enum):
    """A source's file format, spelled as a profile's `source.format`."""

    PARQUET = 'parquet'
    ARROW = 'arrow'
    CSV = 'csv'
    TSV = 'tsv'
    JSON = 'json'
    NDJSON = 'ndjson'


TEXT_FORMATS = frozenset({SourceFormat.CSV, SourceFormat.TSV, SourceFormat.JSON, SourceFormat.NDJSON})


class SourceError(ValueError):
    """The data named cannot be read as one table."""


@dc.dataclass(frozen=True)
class CsvDialect:
    """How a delimited text file is read. The default is RFC 4180 with a header row."""

    delimiter: str = ','
    quote: str = '"'
    escape: str = '"'
    header: bool = True


@dc.dataclass(frozen=True)
class SourceFile:
    """One file of a source: a local path or an `s3://` URL, its size and its modification time."""

    path: str
    size: int
    modified: str


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

    @property
    def fingerprint(self) -> str | None:
        """`sha256:<hex>` over each file's path, size and modification time, never its content."""
        if not self.files:
            return None
        lines = sorted(f'{file.path}\t{file.size}\t{file.modified}' for file in self.files)
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
    """A profiling engine. Each method is one pass over the table, and `aggregate` answers every request in it."""

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
