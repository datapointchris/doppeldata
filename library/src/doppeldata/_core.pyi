"""
doppeldata's engine, compiled.
"""

from _typeshed import SupportsGetItem
from typing import Any, Final, SupportsIndex, final

BLOCK_ROWS: Final[int]
"""
The rows in each batch the engine generates, every one but the last.
"""

FORMAT_VERSION: Final[int]
"""
The profile format version the engine reads.
"""

VERSION: Final[str]
"""
The engine's release, which is the tag the wheel was built from.
"""

@final
class Plan:
    """
    A table profile resolved for generation: ``rows`` rows from ``seed``. ``source`` names the
    profile in an error.
    """
    def __new__(cls, /, profile_json: str, profile_digest: str, seed: SupportsIndex, rows: SupportsIndex, source: str) -> Plan: ...
    @property
    def block_count(self, /) -> int: ...
    def column_samplers(self, /) -> list[tuple[str, str, str, float]]:
        """
        Each top-level column's name, Arrow type, what draws its values, and the share of rows
        drawn null, in schema order.
        """
    @property
    def dataset_digest(self, /) -> str:
        """
        ``sha256:`` and the hex SHA-256 of the identity's canonical JSON.
        """
    def estimate_bytes(self, /, options: WriteOptions) -> int:
        """
        The bytes the write would store, its manifest left out, from the first four blocks
        encoded with its settings.
        """
    def generate(self, /, *, threads: SupportsIndex) -> Any:
        """
        Every row, as one table holding a batch per block, generated on ``threads`` threads,
        zero taking one per core. An interrupt stops it and is raised.
        """
    @property
    def identity_json(self, /) -> str:
        """
        The dataset identity's RFC 8785 canonical JSON: the five values its rows are a pure
        function of.
        """
    def layout_json(self, /, options: WriteOptions) -> str:
        """
        Each file the write would hold, as JSON: its name, its range and the range's rows. A
        partitioned range is written once under each partition value's directory.
        """
    def stream(self, /, *, threads: SupportsIndex) -> Any:
        """
        The blocks in order, as a reader. ``threads`` threads, zero taking one per core, each
        make the blocks ahead of the reader, and each holds at most two.
        """
    def write(self, /, options: WriteOptions, destination: str, *, threads: SupportsIndex, replace: bool, s3_store: dict |None = None) -> str:
        """
        Writes the dataset to ``destination``, a local directory or an ``s3://`` URL, and returns
        the manifest as JSON. An S3 URL's store is built from ``s3_store``: the keyword arguments
        pyo3-object_store's ``S3Store`` takes, such as ``config`` and ``credential_provider``. An
        interrupt cancels the write, which leaves a partial manifest and raises.
        """
    def write_options(self, /, choices_json: str) -> WriteOptions:
        """
        What to write: the profile's recorded settings, with ``choices_json`` laid over them. It
        is a JSON object holding any of ``format``, ``writer``, ``file_rows``, ``partition_by``,
        ``partition_style`` and ``profile_path``.
        """

@final
class WriteOptions:
    """
    The settings a write takes, from ``Plan.write_options``.
    """
    @property
    def settings_json(self, /) -> str:
        """
        The format, every setting it writes with, and the layout, as the manifest records them.
        """

def determinism_values_digest(profile_json: str, profile_digest: str, seed: SupportsIndex, rows: SupportsIndex, *, threads: SupportsIndex) -> str:
    """
    The determinism contract's SHA-256 over every value and text cell generated from a profile,
    made on ``threads`` threads the way ``Plan.generate`` makes its blocks. A wheel's build
    compares it with the hash the contract pins.
    """

def fitted_families() -> dict[str, bool]:
    """
    Whether each scipy.stats family a profile's ``fit`` can name is discrete, by its name. Any
    other family leaves the column a linear tail.
    """

def generalize_pattern(values: SupportsGetItem[int, str]) -> str |None:
    """
    The anchored regular expression grex generalizes the values to, with each digit as a digit
    class, or None when it would hold a literal or a character class only some values have.
    """

def schema_file_names() -> dict[str, str]:
    """
    The file name under ``doppeldata/schemas/`` of each JSON Schema the library ships, by name.
    """

def shape_of(value: str) -> str:
    """
    The value's shape, as a profile stores it: each ASCII upper-case letter as A, lower-case
    letter as a and digit as 9, and every other character as itself.
    """
