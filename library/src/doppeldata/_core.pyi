"""
doppeldata's engine, compiled.
"""

from _typeshed import SupportsGetItem
from os import PathLike
from typing import Any, Final, SupportsFloat, SupportsIndex, final

BLOCK_ROWS: Final[int]
"""
The rows in each batch the engine generates, every one but the last.
"""

DATASET_DIGEST_KEY: Final[str]
"""
The metadata key every Parquet and Arrow IPC file holds its dataset's digest under.
"""

DATASET_ROWS_KEY: Final[str]
"""
The metadata key every Parquet and Arrow IPC file holds its dataset's generated rows under.
"""

DEFAULT_FILE_ROWS: Final[int]
"""
The rows in each file a write makes when neither the caller nor the profile sizes them.
"""

DEFAULT_K_MIN: Final[int]
"""
The fewest rows a stored literal or cell rests on, where a profile states no `privacy.k_min`.
"""

FAULTS_FILE_NAME: Final[str]
"""
The name of the list of faults a write under `malformed_rows` leaves beside its files.
"""

FIRST_BLOCK_BYTES: Final[int]
"""
The head of a text file the profiler reads its dialect, JSON layout and encoding from.
"""

MANIFEST_FILE_NAME: Final[str]
"""
The name of the record a whole write leaves beside its files.
"""

PARTIAL_MANIFEST_FILE_NAME: Final[str]
"""
The name of the claim a write holds its destination with, and the record a stopped write leaves.
"""

PROFILE_DIGEST_KEY: Final[str]
"""
The metadata key every Parquet and Arrow IPC file holds its profile's digest under.
"""

PROFILE_KEY: Final[str]
"""
The metadata key a file holds the whole profile's canonical JSON under, where its write embeds it.
"""

SCENARIO_DIGEST_KEY: Final[str]
"""
The metadata key a Parquet or Arrow IPC file written under a scenario holds its digest under.
"""

SCENARIO_KEY: Final[str]
"""
The metadata key a file holds its scenario's canonical document under, where its write embeds the profile.
"""

SEED_KEY: Final[str]
"""
The metadata key every Parquet and Arrow IPC file holds its dataset's seed under.
"""

STREAM_VERSION: Final[int]
"""
The version of the draws the engine makes, part of every dataset's identity.
"""

STREAM_VERSION_KEY: Final[str]
"""
The metadata key every Parquet and Arrow IPC file holds the engine's stream version under.
"""

VERSION: Final[str]
"""
The engine's release, which is the tag the wheel was built from.
"""

@final
class BundlePlan:
    """
    A bundle resolved for generation: a ``Plan`` for each table, every parent before its children,
    at ``scale`` times each table's row count from ``seed``. ``bundle_json`` holds every table
    inline. ``scenarios``, where given, is each scenario's JSON with its perturbation ids
    resolved and the source an error names it by, beside the digest of the whole set.
    """
    def __new__(cls, /, bundle_json: str, bundle_digest: str, seed: SupportsIndex, scale: SupportsFloat |SupportsIndex, source: str, scenarios: tuple[SupportsGetItem[int, tuple[str, str]], str] |None = None) -> BundlePlan: ...
    @property
    def bundle_json(self, /) -> str:
        """
        The bundle as the plan draws it, as JSON: every table inline, each scenario's patch
        applied to its table and to the relationships drawing that table's columns.
        """
    def claim(self, /, destination: str, profile_json: str, scenario_json: str |None, *, replace: bool, s3_store: dict |None = None) -> str:
        """
        Claims ``destination`` for the bundle and returns the claim as JSON, which ``commit``
        takes back. ``profile_json`` and ``scenario_json`` are the manifest's ``profile`` and
        ``scenario`` records. Each table is then written to ``<destination>/<table>``.
        """
    def commit(self, /, destination: str, claim_json: str, written: SupportsGetItem[int, str], *, elapsed_ms: SupportsIndex, s3_store: dict |None = None) -> str:
        """
        Stores the bundle's manifest and returns it as JSON: whole as ``_manifest.json`` once
        every table is, else partial under the claim's name. ``claim_json`` is what ``claim``
        returned, ``written`` each table's manifest JSON as its write returned it, and
        ``elapsed_ms`` the time from the claim. The manifest takes the most threads and tasks
        any table used.
        """
    @property
    def dataset_digest(self, /) -> str: ...
    @property
    def identity_json(self, /) -> str:
        """
        The bundle's own identity as RFC 8785 canonical JSON: no table, and every table's rows summed.
        """
    @property
    def relationships_json(self, /) -> str:
        """
        Each relationship as JSON, in the bundle's order: its name, pointer, child and parent
        tables and columns, what draws its columns instead where another relationship does, each
        ``inherited`` entry and ``ordinal`` set aside with why, and the columns it copies and
        numbers where it is drawn.
        """
    def set_aside(self, /) -> list[str]:
        """
        The pointer of each relationship, ``inherited`` entry and ``ordinal`` the bundle states and
        the plan does not draw.
        """
    def tables(self, /) -> list[tuple[str, Plan]]:
        """
        Each table's name and plan, every parent before its children.
        """

@final
class Plan:
    """
    A table profile resolved for generation: ``rows`` rows from ``seed``. ``source`` names the
    profile in an error. ``scenario``, where given, is the scenario applied: its JSON with each
    perturbation's id resolved, its digest, and the name an error gives it. The profile is the
    one given, and the engine applies the scenario's patch to it. ``overrides``, where given, is
    a JSON list of each pinned column and its values, as ``column_overrides::ColumnOverride``
    reads it, and a value its column cannot hold raises ValueError.
    """
    def __new__(cls, /, profile_json: str, profile_digest: str, seed: SupportsIndex, rows: SupportsIndex, source: str, scenario: tuple[str, str, str] |None = None, overrides: str |None = None) -> Plan: ...
    @property
    def block_count(self, /) -> int: ...
    def claim(self, /, options: WriteOptions, destination: str, *, replace: bool, s3_store: dict |None = None) -> None:
        """
        Claims ``destination`` for the dataset ``write`` would write there, as ``write`` does
        before its first object. Any process then writes ranges with ``write_ranges``, and one
        calls ``commit``.
        """
    def column_traces_json(self, /) -> str:
        """
        A JSON list of how each top-level column is drawn, in schema order, as
        ``doppeldata_core::ColumnTrace`` serializes one.
        """
    def commit(self, /, options: WriteOptions, destination: str, written: SupportsGetItem[int, str], *, tasks: SupportsIndex, elapsed_ms: SupportsIndex, s3_store: dict |None = None) -> str:
        """
        Stores the manifest of a write ``claim`` claimed, and the fault list under
        ``malformed_rows``, from what each ``write_ranges`` call returned, each string as it
        returned it, and returns the manifest as JSON. ``tasks`` is how many the ranges were
        divided among, and ``elapsed_ms`` the time from the claim. A range with no record leaves
        the partial manifest under the claim's name, and raises.
        """
    def copula_groups(self, /) -> list[list[str]]:
        """
        The names of the columns each Gaussian copula draws together, each group sorted.
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
    def file_columns(self, /, partition_by: SupportsGetItem[int, str]) -> list[str]:
        """
        The names of the columns each file of the rows holds, in order: the plan's less those
        ``partition_by`` names, or under ``cdc`` the full load's record.
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
    def perturbation_writes(self, /) -> list[tuple[str, list[str]]]:
        """
        Each perturbation's id and the columns it writes as planned, in the scenario's order.
        A ``hot_keys`` on a foreign key writes every column its relationship draws or copies.
        """
    def ranges_json(self, /, options: WriteOptions) -> str:
        """
        Each row range the write would make, as a JSON list of objects keyed as the manifest
        keys its files: ``path``, ``range``, ``first_row`` and ``range_rows``, with
        ``objects_at_most``. A partitioned range's path holds ``*`` for each partition value.
        """
    @property
    def rows_written(self, /) -> int:
        """
        The rows the files hold: those generated, and every copy the scenario plans.
        """
    def set_aside(self, /) -> list[str]:
        """
        The JSON pointer of each dependence entry the profile states and the plan does not draw,
        each child's sampler naming why.
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
    def stream_json(self, /, options: WriteOptions) -> str |None:
        """
        The change stream a ``cdc`` write makes beside its full load, as a JSON object keyed
        ``style``, ``prefix``, ``expected_prefix``, ``changes``, ``change_files_at_most`` and
        ``expected_parts``, or None without a ``cdc``. Every change is drawn to count it.
        """
    def type_mix_pointers(self, /) -> list[tuple[str, str]]:
        """
        The JSON Pointer into the profile of each column, nested fields included, that the plan
        generates as text, with the ``type_mix`` type its own draws are written as, in profile
        order. A column is text where its type mix holds values of other types, or where a bundle
        copies its values from a parent's text.
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
        lists, the bytes ``write`` writes for them, on one thread. It returns their manifest
        records and the faults ``malformed_rows`` placed in them as one JSON object, ``files``
        and ``faults``. It neither claims ``destination`` nor stores a manifest.
        """
    def written_settings_json(self, /, options: WriteOptions) -> str:
        """
        The format and every setting the write's manifest records, as ``WriteOptions``'s
        ``settings_json`` spells them, with a ``cdc`` stream's timestamp unit applied.
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

def data_file_metadata(path: str |PathLike[str]) -> dict[str, str] |None:
    """
    The key-value metadata of the local Parquet or Arrow IPC file at ``path``, or None for a
    file of neither format. Raises OSError where the file cannot be opened, and ValueError where
    its footer or schema does not read.
    """

def destination_scheme(url: str) -> tuple[str, str |None]:
    """
    What ``url`` names as a write's destination: ``("local", None)``, ``("s3", bucket)`` or
    ``("memory", None)``. Any other scheme raises the library's ``WriteFailed``.
    """

def determinism_bundle_values_digest(bundle_json: str, bundle_digest: str, seed: SupportsIndex, scale: SupportsFloat |SupportsIndex, *, threads: SupportsIndex) -> str:
    """
    The determinism contract's SHA-256 over every table a bundle generates at ``scale``, each
    table's blocks made on ``threads`` threads, the tables parent first.
    """

def determinism_values_digest(profile_json: str, profile_digest: str, seed: SupportsIndex, rows: SupportsIndex, *, threads: SupportsIndex, scenario: tuple[str, str] |None = None) -> str:
    """
    The determinism contract's SHA-256 over every value and text cell generated from a profile,
    with the scenario ``scenario`` gives as its JSON and its digest, made on ``threads`` threads
    the way ``Plan.generate`` makes its blocks. Under ``cdc`` it also covers every change file
    and the expected table. A wheel's build compares it with the hash the contract pins.
    """

def expected_metric_op(metric: str) -> str:
    """
    The op whose perturbation an expectation's metric measures, such as ``hot_keys`` for
    ``hot_keys.fraction``.
    """

def expected_metric_takes_column(metric: str) -> bool:
    """
    Whether an expectation's metric measures one column, so the expectation names ``column``.
    """

def fault_kind_facts(kind: str, format: str) -> tuple[bool, bool, bool]:
    """
    Of the fault kind ``kind`` and files written as ``format``: whether the kind breaks whole
    files, whether the files can hold it, and whether ``data check`` reads them back as one table
    while they hold it.
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

def issue_codes() -> list[tuple[str, str, str, str |None, int]]:
    """
    Every code the engine and its writers refuse with, in declaration order: the name the
    library binds it by, the code, what raises it, its hint and its exit status.
    """

def one_file_faults_name(file_name: str) -> str:
    """
    The fault list a write to the one file ``file_name`` keeps beside it, such as
    ``_orders.csv.faults.jsonl``.
    """

def op_formats() -> list[tuple[str, list[str]]]:
    """
    Each op, as ``op`` names it, with the formats a write takes it in, as ``format`` names them.
    """

def patched_profile(profile_json: str, scenario_json: str) -> tuple[str, list[str]]:
    """
    ``profile_json`` with the scenario's patch applied as the engine applies it, and each column
    the patch names that the profile does not hold.
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

def pattern_within_lengths(pattern: str, min_length: SupportsIndex |None, max_length: SupportsIndex |None) -> str |None:
    """
    ``pattern`` with its repetitions bounded so every value drawn from it holds at least
    ``min_length`` and at most ``max_length`` characters, either None where unbounded. None where
    no bounds do. A pattern whose values already fit is returned as given.
    """

def perturbation_facts(scenario_json: str) -> list[tuple[str, list[str], bool, bool, bool]]:
    """
    Each of a valid scenario's perturbations, in list order: its id, the columns it writes, adds,
    drops or renames, whether it shapes the files a write makes rather than the rows, whether it
    acts across several files, so a one-file write cannot hold it, and whether it orders the whole
    table, so one process writes the dataset.
    """

def provider_lengths() -> dict[str, tuple[int, int]]:
    """
    The fewest and most characters each provider's values hold, by the provider's name, read
    from the lists and templates it draws from.
    """

def scenario_ids(scenario_json: str) -> tuple[list[str], tuple[int, str] |None]:
    """
    Each perturbation's id in ``scenario_json``, in list order. Where one repeats an earlier
    one's, the list is empty and the second item is its index and id.
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

def top_share_ranks() -> list[tuple[str, int]]:
    """
    Each ``fanout.top_fractions`` field's name and the rank, at scale 1, its share is held at,
    busiest first.
    """
