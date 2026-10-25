"""The reports a check and a diff return, shaped as the `check` and `diff` schemas the engine's Rust model derives."""

import dataclasses as dc
import json
from typing import Any

from doppeldata.profile_comparison.field_changes import FieldChange
from doppeldata.profile_comparison.metrics import Bound

CHECK_VERSION = 1
DIFF_VERSION = 1
BOUND_SIGNS = {Bound.AT_MOST: '≤', Bound.AT_LEAST: '≥'}


OPTIONAL_METRIC_FIELDS = ('expected', 'observed', 'unmeasured', 'skipped_by')


@dc.dataclass(frozen=True)
class MetricResult:
    """One metric's value for one column, with its verdict. `value` is None where `unmeasured` says why it has none, or where
    `skipped_by` names the scenario's perturbation that moves what it compares."""

    metric: str
    column: str | None
    value: float | None
    threshold: float
    bound: Bound
    passed: bool
    expected: float | None = None
    observed: float | None = None
    unmeasured: str | None = None
    skipped_by: str | None = None

    def to_plain(self) -> dict[str, Any]:
        plain = dc.asdict(self) | {'bound': self.bound.value}
        return {key: value for key, value in plain.items() if value is not None or key not in OPTIONAL_METRIC_FIELDS}

    def named(self) -> str:
        """`null_rate_delta on status (0.01 to 0.05)`, or `ks on amount (unmeasured)`."""
        where = f' on {self.column}' if self.column else ''
        if self.unmeasured is not None:
            return f'{self.metric}{where} (unmeasured)'
        moved = f' ({self.expected:g} to {self.observed:g})' if self.expected is not None and self.observed is not None else ''
        return f'{self.metric}{where}{moved}'


@dc.dataclass(frozen=True)
class ExpectationResult:
    """One expectation a scenario declares, and the value the data holds."""

    metric: str
    column: str | None
    at_least: float | None
    at_most: float | None
    value: float
    passed: bool

    def to_plain(self) -> dict[str, Any]:
        return {key: value for key, value in dc.asdict(self).items() if value is not None or key not in ('at_least', 'at_most')}

    def bounds(self) -> str:
        """`≥ 0.55`, `≤ 0.06`, or `≥ 0.04 and ≤ 0.06`."""
        given = ((Bound.AT_LEAST, self.at_least), (Bound.AT_MOST, self.at_most))
        return ' and '.join(f'{BOUND_SIGNS[bound]} {value:g}' for bound, value in given if value is not None)

    def named(self) -> str:
        """`hot_keys.share on customer_id (0.41, expected ≥ 0.55)`."""
        where = f' on {self.column}' if self.column else ''
        return f'{self.metric}{where} ({self.value:g}, expected {self.bounds()})'


@dc.dataclass(frozen=True)
class Failures:
    """What failed in a report, out of every metric it measured, every change it listed and every expectation it measured."""

    metrics: list[MetricResult]
    changes: list[FieldChange]
    expectations: list[ExpectationResult]
    of_metrics: int
    of_changes: int
    of_expectations: int

    @classmethod
    def of(cls, metrics: list[MetricResult], changes: list[FieldChange], expectations: list[ExpectationResult] | None = None) -> 'Failures':
        expectations = expectations or []
        failed = (
            [result for result in metrics if not result.passed],
            [change for change in changes if not change.passed],
            [result for result in expectations if not result.passed],
        )
        return cls(*failed, len(metrics), len(changes), len(expectations))

    def __bool__(self) -> bool:
        return bool(self.metrics or self.changes or self.expectations)

    def summary(self) -> str:
        """`2 of 15 metrics and 1 of 1 field changes: ks on amount, null_rate_delta on status (0.01 to 0.05), column_added fee`."""
        counts = f'{len(self.metrics)} of {self.of_metrics} metrics and {len(self.changes)} of {self.of_changes} field changes'
        if self.of_expectations:
            counts += f' and {len(self.expectations)} of {self.of_expectations} expectations'
        named = (
            [result.named() for result in self.metrics]
            + [f'{change.change.value} {change.column or ""}'.rstrip() for change in self.changes]
            + [result.named() for result in self.expectations]
        )
        return f'{counts}: {", ".join(named)}'


@dc.dataclass(frozen=True)
class ReportedProfile:
    name: str
    digest: str
    path: str | None


@dc.dataclass(frozen=True)
class ReportedScenario:
    name: str
    digest: str


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
    scenario: ReportedScenario | None = None
    expectations: list[ExpectationResult] = dc.field(default_factory=list)

    def failures(self) -> Failures:
        return Failures.of(self.metrics, self.changes, self.expectations)

    def failure_message(self) -> str:
        """What `CheckFailed` says of a check the data did not pass."""
        return f'the data missed its profile on {self.failures().summary()}'

    @property
    def passed(self) -> bool:
        failures = self.failures()
        return not (failures.metrics or failures.changes)

    @property
    def verified(self) -> bool:
        return all(result.passed for result in self.expectations)

    def to_plain(self) -> dict[str, Any]:
        return {
            'check_version': CHECK_VERSION,
            'profile': dc.asdict(self.profile),
            'scenario': dc.asdict(self.scenario) if self.scenario is not None else None,
            'data': dc.asdict(self.data),
            'passed': self.passed,
            'verified': self.verified,
            'metrics': [result.to_plain() for result in self.metrics],
            'changes': [change.to_plain() for change in self.changes],
            'expectations': [result.to_plain() for result in self.expectations],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_plain(), indent=2, allow_nan=False) + '\n'

    def to_markdown(self) -> str:
        """The report for a person, failures first."""
        verdict = 'passed' if self.passed and self.verified else 'failed'
        against = f'profile `{self.profile.name}` ({self.profile.digest})'
        if self.scenario is not None:
            against += f' under scenario `{self.scenario.name}` ({self.scenario.digest})'
        lines = [f'# doppeldata check {verdict}', '', f'`{self.data.path}`, {self.data.rows:,} rows, against {against}.', '']
        return '\n'.join(lines + findings(self.metrics, self.changes, self.failures()) + expected(self.expectations)) + '\n'


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
            if result.skipped_by is not None:
                shown = f'skipped by {result.skipped_by}'
            else:
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


def expected(expectations: list[ExpectationResult]) -> list[str]:
    if not expectations:
        return []
    failed = sum(not result.passed for result in expectations)
    lines = [f'{failed} of {len(expectations)} expectations failed.', '']
    lines += ['| Expectation | Column | Value | Bounds | Passed |', '| --- | --- | --- | --- | --- |']
    for result in sorted(expectations, key=lambda result: result.passed):
        lines.append(f'| {result.metric} | {result.column or ""} | {result.value:.6g} | {result.bounds()} | {verdict(result.passed)} |')
    return [*lines, '']


def verdict(passed: bool) -> str:
    return 'yes' if passed else '**no**'
