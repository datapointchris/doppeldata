"""Profiling a dataset into a profile. The library declares no engine, so a job that only generates installs none."""

import importlib
import importlib.util
import warnings
from collections.abc import Sequence
from typing import Any

from doppeldata.errors import ColumnsNotFrozen
from doppeldata.errors import EarlierProfileNotReused
from doppeldata.errors import ProfilingEnginesMissing
from doppeldata.errors import ProfilingWarning
from doppeldata.profile_format import DERIVED_FROM
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import Profile
from doppeldata.profiling import table_profile
from doppeldata.profiling.detector_packs import EnabledPack
from doppeldata.profiling.detector_packs import enabled_packs

DEFAULT_K_MIN = 11
# Each engine's backend class, as its module and name, and the packages that module imports, which install under the same names.
ENGINES = {'polars': ('doppeldata.profiling.polars_backend', 'PolarsBackend', ('polars', 'duckdb', 'pyarrow'))}
# The module that lists what was named, and what it imports to list S3, which profiling needs whatever the engine.
SOURCES_MODULE = 'doppeldata.profiling.sources'
LISTING_PACKAGES = ('boto3',)


def profile(
    data: Any,
    *,
    engine: str = 'polars',
    sample: int | None = None,
    k_min: int = DEFAULT_K_MIN,
    edges_from: Profile | None = None,
    packs: Sequence[str] = (),
) -> Profile:
    """Profile `data` into a valid table profile holding no rows.

    `data` is a path, an `s3://` URL, a list of them, or a pyarrow, Polars or pandas table. `sample`
    profiles a seeded random sample of that many rows. No stored literal rests on fewer than `k_min` rows.

    Each string column, and each integer column whose name hints a type a validator confirms, is typed by
    the detector packs named in `packs` and the general pack, which every run enables. A pack the library does not ship is
    found by name in the `doppeldata.detector_packs` entry-point group, and `DetectorPackError` names one
    that is not installed or does not load.

    `edges_from` is an earlier profile of the same table. Where its source fingerprint matches the files
    listed now, and it was made by this doppeldata, scipy and detector packs with the same engine, `k_min`
    and sampling, it is returned unchanged, so a reviewer's edits survive a re-run. Otherwise a
    `EarlierProfileNotReused` names what differs, each column it holds a histogram or value list for is counted
    against those, and the rows outside them are written as the column's `overflow`. A column only one of
    the two tables holds issues `ColumnsNotFrozen`.
    """
    if engine not in ENGINES:
        raise ValueError(f'{engine!r} is not a profiling engine; choose from {", ".join(ENGINES)}')
    if sample is not None and sample < 1:
        raise ValueError(f'sample must be a positive number of rows, not {sample}')
    if k_min < 1:
        raise ValueError(f'k_min must be at least 1, not {k_min}')
    module_name, class_name, packages = ENGINES[engine]
    missing = [package for package in (*packages, *LISTING_PACKAGES) if importlib.util.find_spec(package) is None]
    if missing:
        raise ProfilingEnginesMissing(engine, missing)
    # Imported by name, because each imports packages a library-only install lacks.
    backend = getattr(importlib.import_module(module_name), class_name)()
    enabled = enabled_packs(packs)
    sources = importlib.import_module(SOURCES_MODULE)
    source = sources.resolve(data)
    if edges_from is not None:
        changed = what_changed(edges_from, source.fingerprint, engine=engine, sample=sample, k_min=k_min, packs=enabled)
        if changed is None:
            return edges_from
        warnings.warn(EarlierProfileNotReused(changed), stacklevel=2)
    frozen = table_profile.FrozenLayout.of(edges_from.to_plain()) if edges_from is not None else None
    profiled = Profile(table_profile.profile_table(backend, source, sample=sample, k_min=k_min, packs=enabled, frozen=frozen))
    sources.check_unchanged(source)
    issues = profiled.validate()
    if issues:
        raise InvalidProfile(f'the profile of {data!r}', issues)
    if edges_from is not None:
        earlier, now = (column_names(document.to_plain()) for document in (edges_from, profiled))
        undescribed, missing = [name for name in now if name not in earlier], [name for name in earlier if name not in now]
        if undescribed or missing:
            warnings.warn(ColumnsNotFrozen(undescribed, missing), stacklevel=2)
    return profiled


def column_names(document: dict[str, Any]) -> list[str]:
    return [column['name'] for column in document.get('columns') or []]


def what_changed(
    previous: Profile, fingerprint: str | None, *, engine: str, sample: int | None, k_min: int, packs: list[EnabledPack]
) -> str | None:
    """What keeps `previous` from being what profiling the listed source with these settings would give, or None.

    So an earlier doppeldata, a run without scipy, or one with other detector packs, never hands back a profile this one
    would write differently.
    """
    document = previous.to_plain()
    generated = document.get('generated') or {}
    if fingerprint is None:
        return 'the data was not read from files, so nothing shows it is the data that profile was made from'
    recorded_k_min = (document.get('privacy') or {}).get('k_min')
    if recorded_k_min != k_min:
        return f'it was made with k_min {recorded_k_min}, and this run uses {k_min}'
    fitting, _ = table_profile.fitting_module()
    now = table_profile.derivation(engine, fingerprint, fitting.FITTER if fitting else None, packs)
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
