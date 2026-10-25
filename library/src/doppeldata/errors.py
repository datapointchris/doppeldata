"""The errors doppeldata raises for a caller to report, each with the exit status a command ends on, and the warnings it issues."""

from doppeldata.manifest import Manifest


class DoppeldataError(Exception):
    """A failure the caller can act on. `exit_code` is 1 for a failure, 2 for a usage error, and 4 when no answer came."""

    exit_code = 1


class SourceError(DoppeldataError, ValueError):
    """The data named cannot be read as one table."""

    def __init__(self, message: str, *, exit_code: int = 2) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class StoppedWrite(BaseException):
    """A write that did not finish. `code` names why, such as `destination-not-empty` or `cancelled`.

    `manifest` is the record of what a run that claimed its destination wrote, marked partial when it
    stopped early, and None for a refusal before the claim. `manifest_stored` says whether that record
    is in the store, as `_manifest.partial.json` when partial.
    """

    def __init__(
        self, code: str, message: str, *, exit_code: int = 1, manifest_json: str | None = None, manifest_stored: bool = False
    ) -> None:
        super().__init__(message)
        self.code = code
        self.exit_code = exit_code
        self.manifest = Manifest.from_json(manifest_json) if manifest_json is not None else None
        self.manifest_stored = manifest_stored


class WriteFailed(StoppedWrite, DoppeldataError):
    """A write that was refused or failed."""


class WriteInterrupted(StoppedWrite, KeyboardInterrupt):
    """A write a signal stopped, such as Ctrl-C. It is a KeyboardInterrupt, so `except Exception` never swallows it."""


class ProfilingEnginesMissing(DoppeldataError, ImportError):
    """Profiling was asked of an engine whose packages are not installed."""

    def __init__(self, engine: str, missing: list[str]) -> None:
        self.engine = engine
        self.missing = missing
        super().__init__(
            f'profiling with {engine} needs {", ".join(missing)}, which doppeldata does not install; '
            f'run `pip install {" ".join(missing)}`, or install doppeldata-cli, which declares them'
        )


class ProfilingWarning(UserWarning):
    """A profile was written, and something in it is not what the caller may have asked for."""


class FitsSkipped(ProfilingWarning):
    """Profiling finished without fitting a distribution family to any column, because scipy did not import."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(
            f'no distribution family was fitted, because scipy did not import ({reason}); '
            'run `pip install scipy`, or install doppeldata-cli, which declares it'
        )


class ColumnsNotFrozen(ProfilingWarning):
    """A re-profile's table and its earlier profile do not hold the same columns, so some were not counted against it."""

    def __init__(self, undescribed: list[str], missing: list[str]) -> None:
        self.undescribed = undescribed
        self.missing = missing
        parts = []
        if undescribed:
            parts.append(f'columns the earlier profile does not describe, binned afresh: {", ".join(undescribed)}')
        if missing:
            parts.append(f'columns the earlier profile describes that this table lacks: {", ".join(missing)}')
        super().__init__('; '.join(parts))


class PoorFits(ProfilingWarning):
    """Some numeric columns' best fit has a KS statistic above the poor-fit threshold, or no family fitted them."""

    def __init__(self, poor: dict[str, tuple[str, float]], unfitted: list[str], threshold: float) -> None:
        self.poor = poor
        self.unfitted = unfitted
        parts = []
        if poor:
            fits = ', '.join(f'{column} ({family}, KS {ks})' for column, (family, ks) in poor.items())
            parts.append(f'best fits with a KS statistic above {threshold}, written with `poor: true`: {fits}')
        if unfitted:
            parts.append(f'columns no distribution family fits: {", ".join(unfitted)}')
        super().__init__('; '.join(parts))
