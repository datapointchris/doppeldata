"""The errors doppeldata raises for a caller to report, each with the exit status a command ends on, and the warnings it issues."""

import importlib.util
from collections.abc import Mapping
from typing import TYPE_CHECKING

from doppeldata.manifest import Manifest

if TYPE_CHECKING:
    from doppeldata.privacy_policy import ProfileFileCheck
    from doppeldata.profile_comparison.reports import CheckReport
    from doppeldata.profile_comparison.reports import DiffReport


class DoppeldataError(Exception):
    """A failure the caller can act on.

    `exit_code` is 1 for a failure or a drift, 2 for a usage error, 3 for a check that found what a person must act on,
    and 4 when no answer came.
    """

    exit_code = 1


class SourceError(DoppeldataError, ValueError):
    """The data named cannot be read as one table. `role` is the input it was named as: `data`, or `against` for a check's real data."""

    def __init__(self, message: str, *, exit_code: int = 2, role: str = 'data') -> None:
        super().__init__(message)
        self.exit_code = exit_code
        self.role = role


# Each kind of failure, such as a stopped write's, and the exit status a command ends on for it.
EXIT_CODES = {'failed': 1, 'refused': 2, 'unanswered': 4}


class StoppedWrite(BaseException):
    """A write that did not finish, the base of WriteFailed and WriteInterrupted. `code` names why, such as `cancelled`.

    `kind` is `refused` when the call must change and nothing was written, `unanswered` when the store never answered, and
    `failed` otherwise. `manifest` is the record of what a run that claimed its destination wrote, marked partial when it
    stopped early, and None for a refusal before the claim. `manifest_stored` says whether that record is in the store.
    """

    def __init__(
        self, code: str, message: str, *, kind: str = 'failed', manifest_json: str | None = None, manifest_stored: bool = False
    ) -> None:
        super().__init__(message)
        self.code = code
        self.kind = kind
        self.exit_code = EXIT_CODES[kind]
        self.manifest = Manifest.from_json(manifest_json) if manifest_json is not None else None
        self.manifest_stored = manifest_stored


class WriteFailed(StoppedWrite, DoppeldataError):
    """A write that was refused or failed."""


class WriteInterrupted(StoppedWrite, KeyboardInterrupt):
    """A write a signal stopped, such as Ctrl-C. It is a KeyboardInterrupt, so `except Exception` never swallows it."""


def uninstalled(modules: Mapping[str, str]) -> list[str]:
    """Each distribution of `modules`, a map from distribution to the module it installs, whose module is not importable."""
    return [distribution for distribution, module in modules.items() if importlib.util.find_spec(module) is None]


def not_installed(doing: str, missing: list[str], installed_by: str = '') -> str:
    """The sentence refusing `doing` for want of `missing`, naming the `pip install` line and any other route."""
    return f'{doing} needs {", ".join(missing)}, which doppeldata does not install; run `pip install {" ".join(missing)}`{installed_by}'


class PackagesMissing(DoppeldataError, ImportError):
    """`feature` imports packages doppeldata does not install, and `missing` names each distribution not installed.

    `doing` says what needed them, and the message names the `pip install` line and `installed_by`'s route.
    """

    kind = 'refused'
    exit_code = EXIT_CODES['refused']

    def __init__(self, feature: str, missing: list[str], *, doing: str, installed_by: str = '') -> None:
        self.feature = feature
        self.missing = missing
        super().__init__(not_installed(doing, missing, installed_by))


class ProfilingEnginesMissing(PackagesMissing):
    """Profiling was asked of an engine whose packages are not installed."""

    def __init__(self, engine: str, missing: list[str], *, cli_declares: bool = True) -> None:
        self.engine = engine
        route = ', or install doppeldata-cli, which declares them' if cli_declares else ', or run it where Spark is installed'
        super().__init__('profiling', missing, doing=f'profiling with {engine}', installed_by=route)


class DetectorPackError(DoppeldataError):
    """A detector pack named for a run is not installed, does not load, or holds what the profile format refuses.

    `kind` is `refused` for a pack that is not installed, and `failed` for any other.
    """

    def __init__(self, message: str, *, kind: str = 'failed') -> None:
        super().__init__(message)
        self.kind = kind
        self.exit_code = EXIT_CODES[kind]


class InvalidCodeList(DoppeldataError, ValueError):
    """A code list named for a run is read by no enabled detector, or its file cannot be read or holds no code."""

    exit_code = 2


class InvalidThresholds(DoppeldataError, ValueError):
    """A thresholds file names a metric no check measures, a column the source profile lacks, or a value that is not a number."""

    exit_code = 2


class CheckFailed(DoppeldataError):
    """A check found what a person must act on. `report` is the check, naming each finding.

    It is a `CheckReport` where generated data missed its profile on a metric or a column, and a `ProfileFileCheck`
    where a profile file breaks its schema or its privacy policy.
    """

    exit_code = 3

    def __init__(self, report: 'CheckReport | ProfileFileCheck') -> None:
        self.report = report
        super().__init__(report.failure_message())


class ProfilesDrifted(DoppeldataError):
    """The later of two profiles drifted from the earlier on a metric or a field. `report` is the diff, naming each one."""

    exit_code = 1

    def __init__(self, report: 'DiffReport') -> None:
        self.report = report
        super().__init__(f'{report.b.path or report.b.name} drifted from {report.a.path or report.a.name} on {report.failures().summary()}')


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


class EarlierProfileNotReused(ProfilingWarning):
    """`edges_from` was profiled again rather than returned, because `changed` says what differs from the run that made it."""

    def __init__(self, changed: str) -> None:
        self.changed = changed
        super().__init__(
            f'the earlier profile is not returned unchanged, because {changed}; '
            'each column it holds histogram edges or a value list for is counted against them'
        )


class ColumnsNotFrozen(ProfilingWarning):
    """A re-profile's table and its earlier profile do not hold the same columns, or a column changed kind, so some
    were not counted against it.
    """

    def __init__(self, undescribed: list[str], missing: list[str], relisted: list[str]) -> None:
        self.undescribed = undescribed
        self.missing = missing
        self.relisted = relisted
        parts = []
        if undescribed:
            parts.append(f'columns the earlier profile does not describe, binned afresh: {", ".join(undescribed)}')
        if missing:
            parts.append(f'columns the earlier profile describes that this table lacks: {", ".join(missing)}')
        if relisted:
            parts.append(f'columns whose values the earlier profile lists as another kind, profiled afresh: {", ".join(relisted)}')
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
