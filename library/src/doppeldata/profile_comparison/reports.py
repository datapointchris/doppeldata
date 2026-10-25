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
    metric: str
    column: str | None
    value: float
    threshold: float
    bound: Bound
    passed: bool
    source: float | None = None
    observed: float | None = None

    def to_plain(self) -> dict[str, Any]:
        plain = dc.asdict(self) | {'bound': self.bound.value}
        return {key: value for key, value in plain.items() if value is not None or key not in ('source', 'observed')}


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

    @property
    def passed(self) -> bool:
        return all(result.passed for result in self.metrics) and all(change.passed for change in self.changes)

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
        return '\n'.join(lines + findings(self.metrics, self.changes)) + '\n'


@dc.dataclass(frozen=True)
class DiffReport:
    """Two profiles of one table, and how far `b` drifted from `a`."""

    a: ReportedProfile
    b: ReportedProfile
    metrics: list[MetricResult]
    changes: list[FieldChange]

    @property
    def drifted(self) -> bool:
        return not all(result.passed for result in self.metrics) or not all(change.passed for change in self.changes)

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
        return '\n'.join(lines + findings(self.metrics, self.changes)) + '\n'


def findings(metrics: list[MetricResult], changes: list[FieldChange]) -> list[str]:
    failed = sum(not result.passed for result in metrics)
    lines = [f'{failed} of {len(metrics)} metrics failed.', '']
    if metrics:
        lines += ['| Metric | Column | Value | Threshold | Source | Observed | Passed |', '| --- | --- | --- | --- | --- | --- | --- |']
        for result in sorted(metrics, key=lambda result: result.passed):
            threshold = f'{BOUND_SIGNS[result.bound]} {result.threshold:g}'
            source, observed = (f'{value:.6g}' if value is not None else '' for value in (result.source, result.observed))
            lines.append(
                f'| {result.metric} | {result.column or ""} | {result.value:.6g} | {threshold} | {source} | {observed} '
                f'| {verdict(result.passed)} |'
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
