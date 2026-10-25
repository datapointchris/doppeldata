"""Profiling a dataset into a profile. The library declares no engine, so a job that only generates installs none."""

import dataclasses as dc
import importlib
import importlib.metadata
import importlib.util
import os
import warnings
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

from doppeldata import _core
from doppeldata.errors import ColumnsNotFrozen
from doppeldata.errors import EarlierProfileNotReused
from doppeldata.errors import ProfilingEnginesMissing
from doppeldata.errors import ProfilingWarning
from doppeldata.privacy_policy import APPLIED_POLICIES
from doppeldata.privacy_policy import Policy
from doppeldata.profile_format import DERIVED_FROM
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import Profile
from doppeldata.profiling import table_profile
from doppeldata.profiling.backend import SourceFormat
from doppeldata.profiling.detector_packs import HEALTHCARE
from doppeldata.profiling.detector_packs import EnabledPack
from doppeldata.profiling.detector_packs import SuppliedCodeList
from doppeldata.profiling.detector_packs import enabled_packs
from doppeldata.profiling.detector_packs import read_code_lists

DEFAULT_K_MIN = _core.DEFAULT_K_MIN


@dc.dataclass(frozen=True)
class EnginePackage:
    """A package an engine imports, by its distribution and module names, and the formats whose files it reads or sniffs.

    One `everywhere` is imported to profile any source, and any other only to profile a source of a format it reads.
    `generated.readers` records the version of each that reads the source's format.
    """

    distribution: str
    module: str
    reads: frozenset[SourceFormat]
    everywhere: bool = True


@dc.dataclass(frozen=True)
class Engine:
    """An engine's backend class, as its module and name, and the packages that module imports."""

    module: str
    backend: str
    packages: tuple[EnginePackage, ...]


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
    )
}
# The module that lists what was named, and the packages it imports to list S3, which profiling needs whatever the engine.
SOURCES_MODULE = 'doppeldata.profiling.sources'
LISTING_PACKAGES = (EnginePackage('boto3', 'boto3', frozenset()),)


def profile(
    data: Any,
    *,
    engine: str = 'polars',
    sample: int | None = None,
    policy: str = Policy.DEFAULT,
    k_min: int | None = None,
    edges_from: Profile | None = None,
    packs: Sequence[str] = (),
    code_lists: Mapping[str, str | os.PathLike[str]] | None = None,
) -> Profile:
    """Profile `data` into a valid table profile holding no rows, written under the privacy policy `policy`.

    `data` is a path, an `s3://` URL, a list of them, or a pyarrow, Polars or pandas table. `sample`
    profiles a seeded random sample of that many rows. Under `default`, `strict` and `healthcare`, no stored
    literal rests on fewer than `k_min` rows, `DEFAULT_K_MIN` unless given, and `privacy.audit` lists each one.
    `none` keeps every value, and takes and states no `k_min`. `healthcare` also enables the healthcare detector pack.

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
    if engine not in ENGINES:
        raise ValueError(f'{engine!r} is not a profiling engine; choose from {", ".join(ENGINES)}')
    if sample is not None and sample < 1:
        raise ValueError(f'sample must be a positive number of rows, not {sample}')
    applied = applied_policy(policy, k_min)
    stated = applied.k_min(k_min)
    # `none` holds no floor, so every statistic is computed over every row.
    floor = stated or 1
    chosen = ENGINES[engine]
    missing = uninstalled([package for package in (*chosen.packages, *LISTING_PACKAGES) if package.everywhere])
    if missing:
        raise ProfilingEnginesMissing(engine, missing)
    # Imported by name, because each imports packages a library-only install lacks.
    backend = getattr(importlib.import_module(chosen.module), chosen.backend)()
    named = [*packs, HEALTHCARE] if applied is Policy.HEALTHCARE and HEALTHCARE not in packs else list(packs)
    enabled = enabled_packs(named)
    supplied = read_code_lists(code_lists or {}, enabled)
    sources = importlib.import_module(SOURCES_MODULE)
    source = sources.resolve(data)
    reading = [package for package in chosen.packages if source.format in package.reads]
    missing = uninstalled(reading)
    if missing:
        raise ProfilingEnginesMissing(engine, missing)
    readers = {package.distribution: importlib.metadata.version(package.distribution) for package in reading}
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
    profiled = Profile(
        table_profile.profile_table(
            backend,
            source,
            sample=sample,
            k_min=floor,
            policy=applied,
            packs=enabled,
            code_lists=supplied,
            readers=readers,
            frozen=frozen,
        )
    )
    sources.check_unchanged(source)
    issues = profiled.validate()
    if issues:
        raise InvalidProfile(f'the profile of {data!r}', issues)
    if edges_from is not None:
        earlier, now = (column_names(document.to_plain()) for document in (edges_from, profiled))
        undescribed, missing = [name for name in now if name not in earlier], [name for name in earlier if name not in now]
        relisted = table_profile.relisted_columns(edges_from.to_plain(), profiled.to_plain())
        if undescribed or missing or relisted:
            warnings.warn(ColumnsNotFrozen(undescribed, missing, relisted), stacklevel=2)
    return profiled


def uninstalled(packages: Sequence[EnginePackage]) -> list[str]:
    return [package.distribution for package in packages if importlib.util.find_spec(package.module) is None]


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
