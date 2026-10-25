"""Profiling a dataset into a profile. The library declares no engine, so a job that only generates installs none."""

import importlib
import importlib.util
from typing import Any

from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import Profile
from doppeldata.profiling.backend import SourceError

DEFAULT_K_MIN = 11
# Each engine's module, and the packages it imports, which install under the same names.
ENGINES = {'polars': ('doppeldata.profiling.polars_backend', ('polars', 'duckdb', 'pyarrow', 'boto3'))}


class ProfilingEnginesMissing(ImportError):
    """Profiling was asked of an engine whose packages are not installed."""

    def __init__(self, engine: str, missing: list[str]) -> None:
        self.engine = engine
        self.missing = missing
        super().__init__(
            f'profiling with {engine} needs {", ".join(missing)}, which doppeldata does not install; '
            f'run `pip install {" ".join(missing)}`, or install doppeldata-cli, which declares them'
        )


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
    module_name, packages = ENGINES[engine]
    missing = [package for package in packages if importlib.util.find_spec(package) is None]
    if missing:
        raise ProfilingEnginesMissing(engine, missing)
    # Imported by name, because importing it imports the engine, which a library-only install lacks.
    module = importlib.import_module(module_name)
    profiled = Profile(module.profile_data(data, sample=sample, k_min=k_min))
    issues = profiled.validate()
    if issues:
        raise InvalidProfile(f'the profile of {data!r}', issues)
    return profiled
