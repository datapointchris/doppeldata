"""Profiling a dataset into a profile. The library declares no engine, so a job that only generates installs none."""

import dataclasses as dc
import importlib
import importlib.metadata
import os
import warnings
from collections.abc import Mapping
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any

from doppeldata import errors
from doppeldata._core import FORMAT_VERSION
from doppeldata.errors import ColumnsNotFrozen
from doppeldata.errors import EarlierProfileNotReused
from doppeldata.errors import ProfilingEnginesMissing
from doppeldata.errors import ProfilingWarning
from doppeldata.errors import SourceError
from doppeldata.privacy_policy import APPLIED_POLICIES
from doppeldata.privacy_policy import Policy
from doppeldata.profile_format import DERIVED_FROM
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import Profile
from doppeldata.profiling import table_grouping
from doppeldata.profiling import table_profile
from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import SourceFormat
from doppeldata.profiling.detector_packs import HEALTHCARE
from doppeldata.profiling.detector_packs import EnabledPack
from doppeldata.profiling.detector_packs import SuppliedCodeList
from doppeldata.profiling.detector_packs import enabled_packs
from doppeldata.profiling.detector_packs import read_code_lists

if TYPE_CHECKING:
    import pyarrow as pa


@dc.dataclass(frozen=True)
class EnginePackage:
    """A package an engine imports, by its distribution and module names, and the formats whose files it reads or sniffs.

    One `everywhere` is imported to profile any source, and any other only to profile a source of a format it reads.
    `generated.readers` records the version of each that reads the source's format. That is the session's version for
    one `read_by_session`, because the session reads the files, and a Spark Connect client's package or one a
    launcher leaves in site-packages need not be its version. `cli_declares` says whether doppeldata-cli installs it.
    """

    distribution: str
    module: str
    reads: frozenset[SourceFormat]
    everywhere: bool = True
    cli_declares: bool = True
    read_by_session: bool = False


@dc.dataclass(frozen=True)
class Engine:
    """An engine's backend class, as its module and name, and the packages that module imports.

    A `sessioned` engine's backend is built on a session the module's `session_for` resolves, as Spark's is.
    `reads_remote_text` says whether it reads a text source on S3.
    """

    module: str
    backend: str
    packages: tuple[EnginePackage, ...]
    sessioned: bool = False
    reads_remote_text: bool = False


DELIMITED = frozenset({SourceFormat.CSV, SourceFormat.TSV})
ENGINES = {
    'polars': Engine(
        'doppeldata.profiling.polars_backend',
        'PolarsBackend',
        (
            EnginePackage('polars', 'polars', frozenset({SourceFormat.PARQUET, SourceFormat.ARROW})),
            EnginePackage('duckdb', 'duckdb', DELIMITED | {SourceFormat.JSON, SourceFormat.NDJSON}),
            EnginePackage('pyarrow', 'pyarrow', frozenset({SourceFormat.PARQUET})),
            EnginePackage('clevercsv', 'clevercsv', DELIMITED, everywhere=False),
            EnginePackage('charset-normalizer', 'charset_normalizer', DELIMITED, everywhere=False),
        ),
    ),
    # Spark reads every file. DuckDB, CleverCSV and charset-normalizer sniff a delimited source's dialect on the driver.
    # pyspark comes with the Spark it runs in, whose version is the cluster's, so the CLI declares none.
    'spark': Engine(
        'doppeldata.profiling.spark_backend',
        'SparkBackend',
        (
            EnginePackage('pyspark', 'pyspark', DELIMITED | {SourceFormat.PARQUET}, cli_declares=False, read_by_session=True),
            EnginePackage('pyarrow', 'pyarrow', frozenset({SourceFormat.PARQUET})),
            EnginePackage('duckdb', 'duckdb', DELIMITED, everywhere=False),
            EnginePackage('clevercsv', 'clevercsv', DELIMITED, everywhere=False),
            EnginePackage('charset-normalizer', 'charset_normalizer', DELIMITED, everywhere=False),
        ),
        sessioned=True,
        reads_remote_text=True,
    ),
}
# The module that lists what was named, and the packages it imports to list S3, which profiling needs whatever the engine.
SOURCES_MODULE = 'doppeldata.profiling.sources'
LISTING_PACKAGES = (EnginePackage('boto3', 'boto3', frozenset()),)


def profile(
    data: Any,
    *,
    engine: str = 'polars',
    spark: Any = None,
    sample: int | None = None,
    policy: str = Policy.DEFAULT,
    k_min: int | None = None,
    edges_from: Profile | None = None,
    packs: Sequence[str] = (),
    code_lists: Mapping[str, str | os.PathLike[str]] | None = None,
) -> Profile | Bundle:
    """Profile `data` into a valid profile holding no rows, written under the privacy policy `policy`.

    `data` is a path, an `s3://` URL, a list of them, a pyarrow, Polars or pandas table, or under `engine='spark'` a
    Spark DataFrame. One directory or prefix named is grouped into tables as an AWS Glue crawler groups one: where its
    folders' schemas are similar it is one table, and otherwise the result is a `Bundle` of one table per cluster of
    similar folders. `sample` profiles a seeded random sample of that many rows. Under `default`, `strict` and
    `healthcare`, no stored literal rests on fewer than `k_min` rows, `DEFAULT_K_MIN` unless given, and
    `privacy.audit` lists each one. `none` keeps every value, and takes and states no `k_min`. `healthcare` also
    enables the healthcare detector pack.

    `engine` is `polars` or `spark`. Spark profiles in `spark`, the session given, else a DataFrame's own, the active
    session, or one pyspark's builder starts: over Spark Connect where `SPARK_REMOTE` names a server, else on
    `local[*]`. Spark estimates distinct counts with HyperLogLog++ and quantiles with Greenwald-Khanna, where Polars
    counts both.

    Each string column, and each integer column whose name hints a type a validator confirms, is typed by
    the detector packs named in `packs` and the general pack, which every run enables. A pack the library does not ship is
    found by name in the `doppeldata.detector_packs` entry-point group, and `DetectorPackError` names one
    that is not installed or does not load.

    `code_lists` maps the name a detector reads a list by to a local file holding one code a line, such as
    `{'ndc': 'ndc.txt'}`, and the healthcare pack then types an NDC only where the list holds it.
    `generated.code_lists` records each list's sha256 and never its path. `InvalidCodeList` names a list no
    enabled detector reads, or a file that cannot be read.

    `edges_from` is an earlier profile of the same table. Where its source fingerprint matches the files
    listed now, and it was made by this doppeldata, scipy, detector packs, code lists and readers, such as DuckDB and
    CleverCSV, with the same engine, policy, `k_min` and sampling, it is returned unchanged, so a reviewer's
    edits survive a re-run. Otherwise an `EarlierProfileNotReused` names what differs, each column it holds a
    histogram or value list for is counted against those, and the rows outside them are written as the
    column's `overflow`. A column only one of the two tables holds, or whose list would move to another
    section, issues `ColumnsNotFrozen`.
    """
    if sample is not None and sample < 1:
        raise ValueError(f'sample must be a positive number of rows, not {sample}')
    chosen = available_engine(engine)
    if spark is not None and not chosen.sessioned:
        raise ValueError(f'spark names the session engine spark profiles in, and engine {engine} takes none')
    applied = applied_policy(policy, k_min)
    stated = applied.k_min(k_min)
    # `none` holds no floor, so every statistic is computed over every row.
    floor = stated or 1
    named = [*packs, HEALTHCARE] if applied is Policy.HEALTHCARE and HEALTHCARE not in packs else list(packs)
    enabled = enabled_packs(named)
    supplied = read_code_lists(code_lists or {}, enabled)
    source, reading = listed_source(data, engine)
    tables = table_grouping.grouped(source)
    if edges_from is not None and len(tables) > 1:
        grouping = f'{", ".join(source.roots)} groups into {len(tables)} tables'
        message = f"edges_from is one table's profile, and {grouping}; profile each table's folder on its own"
        raise SourceError(message)
    backend = engine_backend(engine, data, spark)
    readers = {
        package.distribution: backend.session_version
        if package.read_by_session and backend.session_version
        else importlib.metadata.version(package.distribution)
        for package in reading
    }
    if edges_from is not None:
        changed = what_changed(
            edges_from,
            source.fingerprint,
            engine=engine,
            sample=sample,
            policy=applied,
            k_min=stated,
            packs=enabled,
            code_lists=supplied,
            readers=readers,
        )
        if changed is None:
            return edges_from
        warnings.warn(EarlierProfileNotReused(changed), stacklevel=2)
    frozen = table_profile.FrozenLayout.of(edges_from.to_plain()) if edges_from is not None else None
    profiled = {}
    with backend.profiling():
        for table in tables:
            document = table_profile.profile_table(
                backend,
                table,
                sample=sample,
                k_min=floor,
                policy=applied,
                packs=enabled,
                code_lists=supplied,
                readers=readers,
                frozen=frozen,
            )
            profiled[table.name] = Profile(document)
    importlib.import_module(SOURCES_MODULE).check_unchanged(source)
    if len(tables) > 1:
        bundle = Bundle({'profile_version': FORMAT_VERSION, 'kind': 'bundle', 'name': source.name, 'tables': {}}, tables=profiled)
        bundle.document['tables'] = {name: table.document for name, table in profiled.items()}
        issues = bundle.validate()
        if issues:
            raise InvalidProfile(f'the profile of {data!r}', issues)
        return bundle
    only = next(iter(profiled.values()))
    issues = only.validate()
    if issues:
        raise InvalidProfile(f'the profile of {data!r}', issues)
    if edges_from is not None:
        earlier, now = (column_names(document.to_plain()) for document in (edges_from, only))
        undescribed, missing = [name for name in now if name not in earlier], [name for name in earlier if name not in now]
        relisted = table_profile.relisted_columns(edges_from.to_plain(), only.to_plain())
        if undescribed or missing or relisted:
            warnings.warn(ColumnsNotFrozen(undescribed, missing, relisted), stacklevel=2)
    return only


def available_engine(engine: str) -> Engine:
    """The engine named, refused with ProfilingEnginesMissing where a package every source needs is not installed."""
    if engine not in ENGINES:
        raise ValueError(f'{engine!r} is not a profiling engine; choose from {", ".join(ENGINES)}')
    chosen = ENGINES[engine]
    refuse_uninstalled(engine, [package for package in (*chosen.packages, *LISTING_PACKAGES) if package.everywhere])
    return chosen


def engine_backend(engine: str, data: Any = None, spark: Any = None) -> Backend:
    """The engine's backend. A sessioned engine's is built on the session its module's `session_for` resolves from `data`
    and `spark`."""
    chosen = available_engine(engine)
    # Imported by name, because each imports packages a library-only install lacks.
    module = importlib.import_module(chosen.module)
    backend_class = getattr(module, chosen.backend)
    backend: Backend = backend_class(module.session_for(data, spark)) if chosen.sessioned else backend_class()
    return backend


def listed_source(data: Any, engine: str = 'polars') -> tuple[Source, list[EnginePackage]]:
    """`data` listed as one source, and the engine's packages that read its format, refused where the engine reads no such
    source or one of those packages is not installed."""
    chosen = ENGINES[engine]
    sources = importlib.import_module(SOURCES_MODULE)
    source: Source = sources.resolve(data)
    if source.table is not None and sources.is_spark_frame(source.table) and not chosen.sessioned:
        raise SourceError(f"a Spark DataFrame is profiled with engine='spark', and this run chose {engine}")
    if not chosen.reads_remote_text:
        sources.refuse_remote_text(source)
    reading = [package for package in chosen.packages if source.format in package.reads]
    refuse_uninstalled(engine, reading)
    return source, reading


@dc.dataclass(frozen=True)
class SampledTable:
    """A seeded random sample of a source's rows, and the rows the source holds.

    `fingerprint` is the listing's, as a profile's `source_fingerprint` records it, and None for a table in memory.
    """

    table: 'pa.Table'
    rows: int
    fingerprint: str | None


def sampled_table(data: Any, rows: int, *, engine: str = 'polars', seed: int = table_profile.SAMPLE_SEED) -> SampledTable:
    """A seeded random sample of up to `rows` rows of `data`, anything `profile` reads, at the types `profile` reads them as.

    A file that changes, appears or goes away while it is read raises the `SourceError` profiling raises.
    """
    if rows < 1:
        raise ValueError(f'rows must be a positive number of rows, not {rows}')
    source, _ = listed_source(data, engine)
    backend = engine_backend(engine, data)
    with backend.profiling():
        table = table_profile.typed_table(backend, source)
        held = table_profile.answered(backend, table, [Request(None, 'rows')])[(None, 'rows')]
        sampled = SampledTable(backend.sample(table, rows, seed), held, source.fingerprint)
    importlib.import_module(SOURCES_MODULE).check_unchanged(source)
    return sampled


def refuse_uninstalled(engine: str, packages: Sequence[EnginePackage]) -> None:
    missing = errors.uninstalled({package.distribution: package.module for package in packages})
    if missing:
        declared = all(package.cli_declares for package in packages if package.distribution in missing)
        raise ProfilingEnginesMissing(engine, missing, cli_declares=declared)


def applied_policy(policy: str, k_min: int | None) -> Policy:
    """The policy named, refused with a `ValueError` where the profiler cannot apply it or `k_min` does not fit it."""
    choices = ', '.join(applied.value for applied in APPLIED_POLICIES)
    if policy == Policy.DP:
        raise ValueError(f'policy dp is reserved for differential privacy, which doppeldata does not apply; choose from {choices}')
    if policy not in APPLIED_POLICIES:
        raise ValueError(f'{policy!r} is not a privacy policy; choose from {choices}')
    if policy == Policy.NONE and k_min is not None:
        raise ValueError(f'policy none keeps every value, so it takes no k_min, and {k_min} was given')
    if k_min is not None and k_min < 1:
        raise ValueError(f'k_min must be at least 1, not {k_min}')
    return Policy(policy)


def column_names(document: dict[str, Any]) -> list[str]:
    return [column['name'] for column in document.get('columns') or []]


def what_changed(
    previous: Profile,
    fingerprint: str | None,
    *,
    engine: str,
    sample: int | None,
    policy: Policy,
    k_min: int | None,
    packs: list[EnabledPack],
    code_lists: Mapping[str, SuppliedCodeList],
    readers: dict[str, str],
) -> str | None:
    """What keeps `previous` from being what profiling the listed source with these settings would give, or None.

    So an earlier doppeldata, a run without scipy, or one with other detector packs or code lists, never hands back a
    profile this one would write differently.
    """
    document = previous.to_plain()
    generated = document.get('generated') or {}
    if fingerprint is None:
        return 'the data was not read from files, so nothing shows it is the data that profile was made from'
    privacy = document.get('privacy') or {}
    if privacy.get('policy') != policy.value:
        return f'it was written under policy {described(privacy.get("policy"))}, and this run applies {policy.value}'
    recorded_k_min = privacy.get('k_min')
    if recorded_k_min != k_min:
        return f'it was made with k_min {recorded_k_min}, and this run uses {k_min}'
    fitting, _ = table_profile.fitting_module()
    now = table_profile.derivation(engine, fingerprint, fitting.FITTER if fitting else None, packs, code_lists, readers)
    for field in DERIVED_FROM:
        if generated.get(field) != now.get(field):
            return f'its generated.{field} is {described(generated.get(field))}, and this run writes {described(now.get(field))}'
    # A profile of every row stands for any sample of it, and a sampled one only for a sample of that size.
    read = generated.get('sample') or {}
    if read.get('strategy') == 'full' or (sample is not None and read.get('rows_read') == sample):
        return None
    reading = 'every row' if sample is None else f'a sample of {sample} rows'
    return f'it was made from a sample of {read.get("rows_read")} rows, and this run reads {reading}'


def described(value: Any) -> str:
    if value is None:
        return 'absent'
    if isinstance(value, dict):
        return ', '.join(f'{key}: {item}' for key, item in value.items())
    return str(value)
