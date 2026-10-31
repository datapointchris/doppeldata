"""The reports a check and a diff return, shaped as the `check` and `diff` schemas the engine's Rust model derives."""

import dataclasses as dc
import json
from typing import Any

from doppeldata.profile_comparison.field_changes import FieldChange
from doppeldata.profile_comparison.metrics import Bound

BOUND_SIGNS = {Bound.AT_MOST: '≤', Bound.AT_LEAST: '≥'}


OPTIONAL_METRIC_FIELDS = ('expected', 'observed', 'unmeasured', 'skipped_by', 'paired_with', 'worst_given_value')


@dc.dataclass(frozen=True)
class MetricResult:
    """One metric's value for one column, or a pair where `paired_with` names the second, with its verdict. A metric of
    a column given another names the value of `paired_with` it is worst within in `worst_given_value`.

    `value` is None where `unmeasured` says why it has none, or where `skipped_by` names the scenario's perturbation that
    moves what it compares. A row whose `decides` is False is one the report's verdict leaves out, such as a score against
    the real data no thresholds file sets, and it passes.
    """

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
    paired_with: str | None = None
    worst_given_value: str | None = None
    decides: bool = True

    def to_plain(self) -> dict[str, Any]:
        plain = dc.asdict(self) | {'bound': self.bound.value}
        return {key: value for key, value in plain.items() if value is not None or key not in OPTIONAL_METRIC_FIELDS}

    def meets_threshold(self) -> bool:
        """Whether `value` meets `threshold`, which for a row that decides nothing is its reference and not its verdict."""
        return self.value is not None and self.bound.holds(self.value, self.threshold)

    def where(self) -> str:
        """` on amount`, ` on fare and total`, ` on channel given region = north`, or nothing for the table."""
        if self.worst_given_value is not None:
            return f' on {self.column} given {self.paired_with} = {self.worst_given_value}'
        if self.paired_with is not None:
            return f' on {self.column} and {self.paired_with}'
        return f' on {self.column}' if self.column else ''

    def named(self) -> str:
        """`null_rate_delta on status (0.01 to 0.05)`, or `ks on amount (unmeasured)`."""
        if self.unmeasured is not None:
            return f'{self.metric}{self.where()} (unmeasured)'
        moved = f' ({self.expected:g} to {self.observed:g})' if self.expected is not None and self.observed is not None else ''
        return f'{self.metric}{self.where()}{moved}'


@dc.dataclass(frozen=True)
class ExpectationResult:
    """One expectation a scenario declares, and the value the data holds. `column` is None for one of the whole table."""

    metric: str
    column: str | None
    at_least: float | None
    at_most: float | None
    value: float | None
    passed: bool
    # Why the data holds no value, such as a column it lacks.
    unmeasured: str | None = None

    def to_plain(self) -> dict[str, Any]:
        return {
            key: value for key, value in dc.asdict(self).items() if value is not None or key not in ('at_least', 'at_most', 'unmeasured')
        }

    def bounds(self) -> str:
        """`≥ 0.55`, `≤ 0.06`, or `≥ 0.04 and ≤ 0.06`."""
        given = ((Bound.AT_LEAST, self.at_least), (Bound.AT_MOST, self.at_most))
        return ' and '.join(f'{BOUND_SIGNS[bound]} {value:g}' for bound, value in given if value is not None)

    def measured(self) -> str:
        """The value, or why there is none."""
        return f'{self.value:g}' if self.value is not None else (self.unmeasured or 'unmeasured')

    def named(self) -> str:
        """`hot_keys.share on customer_id (0.41, expected ≥ 0.55)`, or `malformed.rate (0.01, expected ≥ 0.02)`."""
        where = f' on {self.column}' if self.column is not None else ''
        return f'{self.metric}{where} ({self.measured()}, expected {self.bounds()})'


@dc.dataclass(frozen=True)
class Failures:
    """What failed in a report, out of every metric that decides it, every change it listed and every expectation it measured."""

    metrics: list[MetricResult]
    changes: list[FieldChange]
    expectations: list[ExpectationResult]
    of_metrics: int
    of_changes: int
    of_expectations: int

    @classmethod
    def of(cls, metrics: list[MetricResult], changes: list[FieldChange], expectations: list[ExpectationResult] | None = None) -> 'Failures':
        expectations = expectations or []
        deciding = [result for result in metrics if result.decides]
        failed = (
            [result for result in deciding if not result.passed],
            [change for change in changes if not change.passed],
            [result for result in expectations if not result.passed],
        )
        return cls(*failed, len(deciding), len(changes), len(expectations))

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
class RealData:
    """The real data a check scored the data against, named as the caller named it, and how it was scored.

    `rows` is the rows it holds, and `scored_rows` the rows each side's seeded sample holds. `correlation` is the
    coefficient CorrelationSimilarity compares.
    """

    path: str
    rows: int
    fingerprint: str | None
    scored_rows: int
    correlation: str


@dc.dataclass(frozen=True)
class CheckReport:
    """Generated data measured against the profile it was generated from, and scored against the real data with `against`.

    `passed` covers every metric that decides and every column change, and `verified` every expectation a scenario declares.
    """

    profile: ReportedProfile
    data: CheckedData
    metrics: list[MetricResult]
    changes: list[FieldChange]
    scenario: ReportedScenario | None = None
    expectations: list[ExpectationResult] = dc.field(default_factory=list)
    against: RealData | None = None

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
            'profile': dc.asdict(self.profile),
            'scenario': dc.asdict(self.scenario) if self.scenario is not None else None,
            'data': dc.asdict(self.data),
            'against': dc.asdict(self.against) if self.against is not None else None,
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
        if self.against is not None:
            real = self.against
            lines += [
                f'Scored against the real data at `{real.path}`, {real.rows:,} rows, '
                f'on a seeded sample of {real.scored_rows:,} rows from each side.',
                '',
            ]
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
    lines += metric_table([result for result in metrics if result.decides])
    informational = [result for result in metrics if not result.decides and result.value is not None]
    if informational:
        lines += [
            f'Scores that decide nothing, because no thresholds file sets their threshold: {len(informational)}. '
            f'Missing the threshold shown: {sum(not result.meets_threshold() for result in informational)}.',
            '',
            *metric_table(informational, against_reference=True),
        ]
    for result in metrics:
        if not result.decides and result.value is None:
            on = f' on `{result.column}`' if result.column else ''
            lines += [f'`{result.metric}`{on} decides nothing, unmeasured: {result.unmeasured}.', '']
    if changes:
        lines += ['| Change | Column | Passed | Detail |', '| --- | --- | --- | --- |']
        for change in sorted(changes, key=lambda change: change.passed):
            lines.append(f'| {change.change.value} | {change.column or ""} | {verdict(change.passed)} | {change.message} |')
        lines.append('')
    return lines


def metric_table(metrics: list[MetricResult], *, against_reference: bool = False) -> list[str]:
    """The metrics as a Markdown table, failures first, a pair's columns joined in one cell.

    With `against_reference`, the last column says whether each value meets the threshold shown, as each row passes.
    """
    if not metrics:
        return []
    judged = MetricResult.meets_threshold if against_reference else (lambda result: result.passed)
    heading = 'Meets it' if against_reference else 'Passed'
    lines = [f'| Metric | Column | Value | Threshold | Expected | Observed | {heading} |', '| --- | --- | --- | --- | --- | --- | --- |']
    for result in sorted(metrics, key=judged):
        threshold = f'{BOUND_SIGNS[result.bound]} {result.threshold:g}'
        if result.skipped_by is not None:
            shown = f'skipped by {result.skipped_by}'
        else:
            shown = f'{result.value:.6g}' if result.value is not None else f'unmeasured: {result.unmeasured}'
        expected, observed = (f'{value:.6g}' if value is not None else '' for value in (result.expected, result.observed))
        column = ', '.join(name for name in (result.column, result.paired_with) if name)
        lines.append(f'| {result.metric} | {column} | {shown} | {threshold} | {expected} | {observed} | {verdict(judged(result))} |')
    return [*lines, '']


def expected(expectations: list[ExpectationResult]) -> list[str]:
    if not expectations:
        return []
    failed = sum(not result.passed for result in expectations)
    lines = [f'{failed} of {len(expectations)} expectations failed.', '']
    lines += ['| Expectation | Column | Value | Bounds | Passed |', '| --- | --- | --- | --- | --- |']
    for result in sorted(expectations, key=lambda result: result.passed):
        lines.append(f'| {result.metric} | {result.column or ""} | {result.measured()} | {result.bounds()} | {verdict(result.passed)} |')
    return [*lines, '']


def verdict(passed: bool) -> str:
    return 'yes' if passed else '**no**'
