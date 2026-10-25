"""
doppeldata's engine, compiled.
"""

from _typeshed import SupportsGetItem
from typing import Any, Final, SupportsIndex, final

BLOCK_ROWS: Final[int]
"""
The rows in each batch the engine generates, every one but the last.
"""

DEFAULT_FILE_ROWS: Final[int]
"""
The rows in each file a write makes when neither the caller nor the profile sizes them.
"""

FORMAT_VERSION: Final[int]
"""
The profile format version the engine reads.
"""

STREAM_VERSION: Final[int]
"""
The version of the draws the engine makes, part of every dataset's identity.
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
    def claim(self, /, options: WriteOptions, destination: str, *, replace: bool, s3_store: dict |None = None) -> None:
        """
        Claims ``destination`` for the dataset ``write`` would write there, as ``write`` does
        before its first object. Any process then writes ranges with ``write_ranges``, and one
        calls ``commit``.
        """
    def column_samplers(self, /) -> list[tuple[str, str, str, float]]:
        """
        Each top-level column's name, type as a profile's ``physical_type`` spells it, what draws
        its values, and the share of rows drawn null, in schema order.
        """
    def commit(self, /, options: WriteOptions, destination: str, files_json: str, *, tasks: SupportsIndex, elapsed_ms: SupportsIndex, s3_store: dict |None = None) -> str:
        """
        Stores the manifest of a write ``claim`` claimed, from every record ``write_ranges``
        returned, given as one JSON list, and returns it as JSON. ``tasks`` is how many the
        ranges were divided among, and ``elapsed_ms`` the time from the claim. A range with no
        record leaves the partial manifest under the claim's name, and raises.
        """
    @property
    def dataset_digest(self, /) -> str:
        """
        ``sha256:`` and the hex SHA-256 of the identity's canonical JSON.
        """
    def estimate_bytes(self, /, options: WriteOptions) -> int:
        """
        The bytes the write would store, its manifest left out, from its first blocks encoded
        with its settings.
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
    def ranges_json(self, /, options: WriteOptions) -> str:
        """
        Each row range the write would make, as a JSON list of objects keyed as the manifest
        keys its files: ``path``, ``range``, ``first_row`` and ``range_rows``, with
        ``objects_at_most``. A partitioned range's path holds ``*`` for each partition value.
        """
    def stream(self, /, *, threads: SupportsIndex) -> Any:
        """
        The blocks in order, as a reader. ``threads`` threads, zero taking one per core, each
        make the blocks ahead of the reader, and each holds at most two.
        """
    def stream_blocks(self, /, start: SupportsIndex, stop: SupportsIndex, *, threads: SupportsIndex) -> Any:
        """
        Blocks ``start`` up to ``stop`` in order, as a reader made the way ``stream`` makes
        every block, so they are the batches ``stream`` yields for those blocks.
        """
    def write(self, /, options: WriteOptions, destination: str, *, threads: SupportsIndex, replace: bool, s3_store: dict |None = None) -> str:
        """
        Writes the dataset to ``destination``, a URL ``destination_scheme`` accepts, and returns
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
    def write_ranges(self, /, options: WriteOptions, destination: str, ranges: SupportsGetItem[int, SupportsIndex], *, s3_store: dict |None = None) -> str:
        """
        Writes the objects of the ranges numbered ``ranges`` in the layout ``ranges_json``
        lists, the bytes ``write`` writes for them, on one thread, and returns their manifest
        records as a JSON list. It neither claims ``destination`` nor stores a manifest.
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

def check_digits_hold(kind: str, values: SupportsGetItem[int, str]) -> list[bool]:
    """
    Whether each value holds the check digit ``kind`` names, such as ``luhn``, where generation
    writes one: its last digit, computed from the digits before it.
    """

def code_list(name: str) -> tuple[str, list[str]] |None:
    """
    The release a shipped code list's first line names, and its codes without their dots. None
    when the engine ships no list of that name.
    """

def destination_scheme(url: str) -> tuple[str, str |None]:
    """
    What ``url`` names as a write's destination: ``("local", None)``, ``("s3", bucket)`` or
    ``("memory", None)``. Any other scheme raises the library's ``WriteFailed``.
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

def pattern_is_generalized(pattern: str) -> bool |None:
    """
    Whether ``pattern`` holds no literal only some values have, as the privacy floor admits a
    stored pattern: every class is ``\d`` or whole ASCII digit and letter ranges. None when it
    does not parse.
    """

def pattern_problem(pattern: str) -> str |None:
    """
    Why the engine cannot generate from ``pattern``, as planning a profile that stores it
    refuses it, or None when it can.
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
