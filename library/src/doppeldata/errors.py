"""The errors doppeldata raises for a caller to report, each carrying the issues that say why, and the warnings it issues."""

import importlib.util
from collections.abc import Mapping
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any

from doppeldata import issue_codes
from doppeldata.issue_codes import Issue
from doppeldata.manifest import Manifest

if TYPE_CHECKING:
    from doppeldata.privacy_policy import ProfileFileCheck
    from doppeldata.profile_comparison.reports import BundleCheckReport
    from doppeldata.profile_comparison.reports import CheckReport
    from doppeldata.profile_comparison.reports import DiffReport
    from doppeldata.scenario_checks import ScenarioCheck

# The accepted values a refusal's text lists before it counts the rest.
VALID_SHOWN = 10


class IssueCarrier(BaseException):
    """The base of every doppeldata error and stopped write. `issues` names each reason, at least one, and the first gives
    `code` and `exit_code`, the status a command ends on. `headline` leads the text where it lists the issues beneath it,
    and a first issue whose message is the headline is the one it lists none of.

    The text leads with the code, as `E-PROFILE-012: <message>`, then a line for each issue a headline leads, then each hint.
    """

    def __init__(self, issues: Sequence[Issue], *, headline: str | None = None) -> None:
        if not issues:
            raise ValueError('an error carries at least one issue')
        self.issues = tuple(issues)
        self.headline = headline
        super().__init__(self.message)

    @property
    def code(self) -> str:
        return self.issues[0].code

    @property
    def exit_code(self) -> int:
        return issue_codes.issue_code(self.code).exit_code

    @property
    def message(self) -> str:
        return self.headline if self.headline is not None else self.issues[0].message

    def __str__(self) -> str:
        if len(self.issues) == 1 and self.headline in (None, self.issues[0].message):
            only = self.issues[0]
            lines = [f'{self.code}: {self.message}']
            if only.closest:
                lines.append(f'  closest: {", ".join(only.closest)}')
            if only.valid:
                lines += ['  valid:', *(f'    {value}' for value in only.valid[:VALID_SHOWN])]
                if len(only.valid) > VALID_SHOWN:
                    lines.append(f'    and {len(only.valid) - VALID_SHOWN} more')
        else:
            beneath = self.issues[1:] if self.headline == self.issues[0].message else self.issues
            lines = [f'{self.code}: {self.message}:', *(f'  {issue.field or "/"}: {issue.message} [{issue.code}]' for issue in beneath)]
        lines += [f'  hint: {hint}' for hint in dict.fromkeys(issue.hint for issue in self.issues if issue.hint)]
        return '\n'.join(lines)


class DoppeldataError(IssueCarrier, Exception):
    """A failure the caller can act on, raised as one issue: its code, its message, and what it names."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        field: str | None = None,
        value: Any = None,
        valid: Sequence[str] = (),
        closest: Sequence[str] = (),
        hint: str | None = None,
    ) -> None:
        super().__init__([Issue(code, field, message, value, tuple(valid), tuple(closest), hint)])


class InvalidArgument(DoppeldataError, ValueError):
    """An argument holds a value the call does not take."""


class UnsupportedArgument(DoppeldataError, TypeError):
    """An argument is of a type the call does not take."""


class ProfileNotFound(DoppeldataError, FileNotFoundError):
    """A profile named is neither a file nor an example doppeldata ships."""


class IncompatibleInstall(DoppeldataError, RuntimeError):
    """What is installed beside doppeldata, or on Spark's executors, is not what it runs with."""


class SourceError(DoppeldataError, ValueError):
    """The data named cannot be read as one table. `role` is the input it was named as, and the issue's field: `data`,
    `against` for a check's real data, `schema` for a seed's schema, `profile` for a Parquet or Arrow IPC file named as a
    profile, `region` for the Glue catalog's region, `detected-entities` for the entities given a Glue table's columns, or
    `environment` for the AWS settings.
    """

    def __init__(self, code: str, message: str, *, role: str = 'data', value: Any = None, valid: Sequence[str] = ()) -> None:
        super().__init__(code, message, field=role, value=value, valid=valid)
        self.role = role


class StoppedWrite(IssueCarrier):
    """A write that did not finish, the base of WriteFailed and WriteInterrupted. `code` names why, as `E-WRITE-213` names
    a cancelled one, and its exit status is the code's, as docs/issue-codes.md lists them.

    `manifest` is the record of what a run that claimed its destination wrote, marked partial when it stopped early, and
    None for a refusal before the claim. `manifest_stored` says whether that record is in the store.
    """

    def __init__(self, code: str, message: str, *, manifest_json: str | None = None, manifest_stored: bool = False) -> None:
        IssueCarrier.__init__(self, [Issue(code, None, message)])
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

    def __init__(self, feature: str, missing: list[str], *, doing: str, installed_by: str = '') -> None:
        self.feature = feature
        self.missing = missing
        super().__init__(issue_codes.PACKAGES_MISSING, not_installed(doing, missing, installed_by), value=list(missing))


class ProfilingEnginesMissing(PackagesMissing):
    """Profiling was asked of an engine whose packages are not installed."""

    def __init__(self, engine: str, missing: list[str], *, cli_declares: bool = True) -> None:
        self.engine = engine
        route = ', or install doppeldata-cli, which declares them' if cli_declares else ', or run it where Spark is installed'
        super().__init__('profiling', missing, doing=f'profiling with {engine}', installed_by=route)


class DetectorPackError(DoppeldataError):
    """A detector pack named for a run is not installed, does not load, or holds what the profile format refuses."""


class InvalidCodeList(DoppeldataError, ValueError):
    """A code list named for a run is read by no enabled detector, or its file cannot be read or holds no code."""


class InvalidThresholds(DoppeldataError, ValueError):
    """A thresholds file names a metric no check measures, a column the source profile lacks, or a value that is not a
    number. Each issue's field points into the file, and `headline` names the file where several are listed."""

    def __init__(self, issues: Sequence[Issue], *, headline: str | None = None) -> None:
        IssueCarrier.__init__(self, issues, headline=headline)


class CheckFailed(DoppeldataError):
    """A check found what a person must act on. `report` is the check, naming each finding, and its `failure_code` is the
    error's code.

    It is a `CheckReport` where generated data missed its profile on a metric or a column, a `BundleCheckReport` where
    a bundle's tables missed it on a table or a relationship, a `ProfileFileCheck` where a profile file breaks its
    schema or its privacy policy, and a `ScenarioCheck` where a scenario does not hold for its profile.
    """

    def __init__(self, report: 'CheckReport | BundleCheckReport | ProfileFileCheck | ScenarioCheck') -> None:
        self.report = report
        super().__init__(report.failure_code, report.failure_message())


class ProfileFailsCheck(DoppeldataError, ValueError):
    """A write embedding a profile, or an export of one, was refused because a table made from data fails `profile check`,
    so no file carries a literal the privacy policy did not pass. `report` is that check, and each violation follows the
    refusal's first issue."""

    def __init__(self, report: 'ProfileFileCheck') -> None:
        self.report = report
        message = f'{report.failure_message()}, so no file carries its literals'
        refusal = Issue(issue_codes.PROFILE_FAILS_CHECK, 'profile', message, report.profile)
        IssueCarrier.__init__(self, [refusal, *report.violations], headline=message)


class CheckStopped(DoppeldataError):
    """A check or a diff began and could not finish. Its first issue is the check's stop code, which exits 3, and the issues
    after it say what stopped it, so a check that could not run is never read as one that found nothing."""

    def __init__(self, issues: Sequence[Issue], *, headline: str | None = None) -> None:
        IssueCarrier.__init__(self, issues, headline=headline)


class ProfilesDrifted(DoppeldataError):
    """The later of two profiles drifted from the earlier on a metric or a field. `report` is the diff, naming each one."""

    def __init__(self, report: 'DiffReport') -> None:
        self.report = report
        drifted = f'{report.b.path or report.b.name} drifted from {report.a.path or report.a.name} on {report.failures().summary()}'
        super().__init__(issue_codes.PROFILES_DRIFTED, drifted)


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
