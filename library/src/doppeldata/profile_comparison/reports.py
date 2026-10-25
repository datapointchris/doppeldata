"""The reports a check and a diff return, shaped as the `check` and `diff` schemas the engine's Rust model derives."""

import dataclasses as dc
import json
from typing import Any

from doppeldata.profile_comparison.field_changes import FieldChange
from doppeldata.profile_comparison.metrics import Bound

CHECK_VERSION = 1
DIFF_VERSION = 1
BOUND_SIGNS = {Bound.AT_MOST: '≤', Bound.AT_LEAST: '≥'}


@dc.dataclass(frozen=True)
class MetricResult:
    """One metric's value for one column, with its verdict. `value` is None where `unmeasured` says why it has none."""

    metric: str
    column: str | None
    value: float | None
    threshold: float
    bound: Bound
    passed: bool
    expected: float | None = None
    observed: float | None = None
    unmeasured: str | None = None

    def to_plain(self) -> dict[str, Any]:
        plain = dc.asdict(self) | {'bound': self.bound.value}
        return {key: value for key, value in plain.items() if value is not None or key not in ('expected', 'observed', 'unmeasured')}

    def named(self) -> str:
        """`null_rate_delta on status (0.01 to 0.05)`, or `ks on amount (unmeasured)`."""
        where = f' on {self.column}' if self.column else ''
        if self.unmeasured is not None:
            return f'{self.metric}{where} (unmeasured)'
        moved = f' ({self.expected:g} to {self.observed:g})' if self.expected is not None and self.observed is not None else ''
        return f'{self.metric}{where}{moved}'


@dc.dataclass(frozen=True)
class Failures:
    """What failed in a report, out of every metric it measured and every change it listed."""

    metrics: list[MetricResult]
    changes: list[FieldChange]
    of_metrics: int
    of_changes: int

    @classmethod
    def of(cls, metrics: list[MetricResult], changes: list[FieldChange]) -> 'Failures':
        failed = [result for result in metrics if not result.passed], [change for change in changes if not change.passed]
        return cls(*failed, len(metrics), len(changes))

    def __bool__(self) -> bool:
        return bool(self.metrics or self.changes)

    def summary(self) -> str:
        """`2 of 15 metrics and 1 of 1 field changes: ks on amount, null_rate_delta on status (0.01 to 0.05), column_added fee`."""
        counts = f'{len(self.metrics)} of {self.of_metrics} metrics and {len(self.changes)} of {self.of_changes} field changes'
        named = [result.named() for result in self.metrics] + [
            f'{change.change.value} {change.column or ""}'.rstrip() for change in self.changes
        ]
        return f'{counts}: {", ".join(named)}'


@dc.dataclass(frozen=True)
class ReportedProfile:
    name: str
    digest: str
    path: str | None


@dc.dataclass(frozen=True)
class CheckedData:
    """The data as the caller named it, the rows profiled, and the listing's fingerprint, None for an in-memory table."""

    path: str
    rows: int
    fingerprint: str | None


@dc.dataclass(frozen=True)
class CheckReport:
    """Generated data measured against the profile it was generated from.

    `passed` covers every metric and column change, and `verified` every expectation a scenario declares.
    """

    profile: ReportedProfile
    data: CheckedData
    metrics: list[MetricResult]
    changes: list[FieldChange]
    scenario: None = None
    expectations: tuple[()] = ()

    def failures(self) -> Failures:
        return Failures.of(self.metrics, self.changes)

    @property
    def passed(self) -> bool:
        return not self.failures()

    @property
    def verified(self) -> bool:
        return True

    def to_plain(self) -> dict[str, Any]:
        return {
            'check_version': CHECK_VERSION,
            'profile': dc.asdict(self.profile),
            'scenario': self.scenario,
            'data': dc.asdict(self.data),
            'passed': self.passed,
            'verified': self.verified,
            'metrics': [result.to_plain() for result in self.metrics],
            'changes': [change.to_plain() for change in self.changes],
            'expectations': list(self.expectations),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_plain(), indent=2, allow_nan=False) + '\n'

    def to_markdown(self) -> str:
        """The report for a person, failures first."""
        verdict = 'passed' if self.passed and self.verified else 'failed'
        lines = [
            f'# doppeldata check {verdict}',
            '',
            f'`{self.data.path}`, {self.data.rows:,} rows, against profile `{self.profile.name}` ({self.profile.digest}).',
            '',
        ]
        return '\n'.join(lines + findings(self.metrics, self.changes, self.failures())) + '\n'


@dc.dataclass(frozen=True)
class DiffReport:
    """Two profiles of one table, and how far `b` drifted from `a`."""

    a: ReportedProfile
    b: ReportedProfile
    metrics: list[MetricResult]
    changes: list[FieldChange]

    def failures(self) -> Failures:
        return Failures.of(self.metrics, self.changes)

    @property
    def drifted(self) -> bool:
        return bool(self.failures())

    def to_plain(self) -> dict[str, Any]:
        return {
            'diff_version': DIFF_VERSION,
            'a': dc.asdict(self.a),
            'b': dc.asdict(self.b),
            'drifted': self.drifted,
            'metrics': [result.to_plain() for result in self.metrics],
            'changes': [change.to_plain() for change in self.changes],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_plain(), indent=2, allow_nan=False) + '\n'

    def to_markdown(self) -> str:
        """The report for a person, failures first."""
        verdict = 'drifted' if self.drifted else 'agree'
        lines = [
            f'# doppeldata profiles {verdict}',
            '',
            f'`{self.b.path or self.b.name}` ({self.b.digest}) against `{self.a.path or self.a.name}` ({self.a.digest}).',
            '',
        ]
        return '\n'.join(lines + findings(self.metrics, self.changes, self.failures())) + '\n'


def findings(metrics: list[MetricResult], changes: list[FieldChange], failures: Failures) -> list[str]:
    lines = [f'{len(failures.metrics)} of {failures.of_metrics} metrics failed.', '']
    if metrics:
        lines += ['| Metric | Column | Value | Threshold | Expected | Observed | Passed |', '| --- | --- | --- | --- | --- | --- | --- |']
        for result in sorted(metrics, key=lambda result: result.passed):
            threshold = f'{BOUND_SIGNS[result.bound]} {result.threshold:g}'
            shown = f'{result.value:.6g}' if result.value is not None else f'unmeasured: {result.unmeasured}'
            expected, observed = (f'{value:.6g}' if value is not None else '' for value in (result.expected, result.observed))
            lines.append(
                f'| {result.metric} | {result.column or ""} | {shown} | {threshold} | {expected} | {observed} | {verdict(result.passed)} |'
            )
        lines.append('')
    if changes:
        lines += ['| Change | Column | Passed | Detail |', '| --- | --- | --- | --- |']
        for change in sorted(changes, key=lambda change: change.passed):
            lines.append(f'| {change.change.value} | {change.column or ""} | {verdict(change.passed)} | {change.message} |')
        lines.append('')
    return lines


def verdict(passed: bool) -> str:
    return 'yes' if passed else '**no**'
