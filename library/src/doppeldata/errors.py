"""The errors doppeldata raises for a caller to report, each with the exit status a command ends on, and the warnings it issues."""


class DoppeldataError(Exception):
    """A failure the caller can act on. `exit_code` is 1 for a failure, 2 for a usage error, and 4 when no answer came."""

    exit_code = 1


class SourceError(DoppeldataError, ValueError):
    """The data named cannot be read as one table."""

    def __init__(self, message: str, *, exit_code: int = 2) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class ProfilingEnginesMissing(DoppeldataError, ImportError):
    """Profiling was asked of an engine whose packages are not installed."""

    def __init__(self, engine: str, missing: list[str]) -> None:
        self.engine = engine
        self.missing = missing
        super().__init__(
            f'profiling with {engine} needs {", ".join(missing)}, which doppeldata does not install; '
            f'run `pip install {" ".join(missing)}`, or install doppeldata-cli, which declares them'
        )


class FitsSkipped(UserWarning):
    """Profiling finished without fitting a distribution family to any column, because scipy is not installed."""

    def __init__(self) -> None:
        super().__init__(
            'no distribution family was fitted, because scipy is not installed; '
            'run `pip install scipy`, or install doppeldata-cli, which declares it'
        )
