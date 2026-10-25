"""Profiling a dataset into a profile. The library declares no engine, so a job that only generates installs none."""

import importlib
import importlib.util
from typing import Any

from doppeldata.errors import ProfilingEnginesMissing
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import Profile
from doppeldata.profiling import table_profile

DEFAULT_K_MIN = 11
# Each engine's backend class, as its module and name, and the packages that module imports, which install under the same names.
ENGINES = {'polars': ('doppeldata.profiling.polars_backend', 'PolarsBackend', ('polars', 'duckdb', 'pyarrow'))}
# The module that lists what was named, and what it imports to list S3, which profiling needs whatever the engine.
SOURCES_MODULE = 'doppeldata.profiling.sources'
LISTING_PACKAGES = ('boto3',)


def profile(data: Any, *, engine: str = 'polars', sample: int | None = None, k_min: int = DEFAULT_K_MIN) -> Profile:
    """Profile `data` into a valid table profile holding no rows.

    `data` is a path, an `s3://` URL, a list of them, or a pyarrow, Polars or pandas table. `sample`
    profiles a seeded random sample of that many rows. No stored literal rests on fewer than `k_min` rows.
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
    sources = importlib.import_module(SOURCES_MODULE)
    source = sources.resolve(data)
    profiled = Profile(table_profile.profile_table(backend, source, sample=sample, k_min=k_min))
    sources.check_unchanged(source)
    issues = profiled.validate()
    if issues:
        raise InvalidProfile(f'the profile of {data!r}', issues)
    return profiled
