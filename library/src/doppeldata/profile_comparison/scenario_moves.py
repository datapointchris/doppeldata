"""What each of a scenario's perturbations moves in a check: the metrics it skips, and the column changes it makes pass."""

import dataclasses as dc
from typing import Any

from doppeldata.profile_comparison.field_changes import FieldChange
from doppeldata.profile_comparison.metrics import METRICS_BY_NAME
from doppeldata.profile_comparison.reports import MetricResult
from doppeldata.scenario_format import Scenario
from doppeldata.scenario_format import perturbation_facts


def registered(*names: str) -> frozenset[str]:
    """`names`, each looked up in the metric registry, so a renamed metric fails at import rather than silently ending a skip."""
    return frozenset(METRICS_BY_NAME[name].name for name in names)


# The metrics each op moves on every column or on the table, beside every metric on a column it writes. A partition key's
# metric is named for its column, so an op writing the key moves it.
MOVED_METRICS = {
    'duplicates': registered('distinct_ratio_delta', 'rows_per_file_delta'),
    'small_files': registered('file_count_delta', 'rows_per_file_delta'),
}
# Given `kinds`, malformed_rows replaces the rejected rows the profile records, and adds values to the columns it breaks,
# such as a status cut by an embedded line break, which moves the column's distinct values and its category shares.
NAMED_MALFORMED_METRICS = registered('malformed_rate_delta', 'distinct_ratio_delta', 'tvd')
MALFORMED_RATE_DELTA = METRICS_BY_NAME['malformed_rate_delta'].name
NOT_ASKED = 'no malformed_rows perturbation asked for malformed rows, so the malformed rates were not compared'


@dc.dataclass(frozen=True)
class Move:
    """One perturbation, by its id, and what it moves: every metric on `columns`, and each metric in `metrics` wherever it is."""

    id: str
    columns: frozenset[str]
    metrics: frozenset[str]

    def moves(self, metric: str, column: str | None) -> bool:
        return column in self.columns or metric in self.metrics


def moves(scenario: Scenario) -> list[Move]:
    plain = scenario.to_plain()
    return [
        Move(facts.id, frozenset(facts.writes), moved_metrics(entry))
        for entry, facts in zip(plain.get('perturbations') or [], perturbation_facts(plain), strict=True)
    ]


def moved_metrics(entry: dict[str, Any]) -> frozenset[str]:
    if entry['op'] == 'malformed_rows':
        return NAMED_MALFORMED_METRICS if entry.get('kinds') else frozenset()
    return MOVED_METRICS.get(entry['op'], frozenset())


def uncompared_malformed_rates(results: list[MetricResult], scenario: Scenario | None) -> list[MetricResult]:
    """Each `malformed_rate_delta` row as unmeasured and deciding nothing, where no malformed_rows asked for malformed rows."""
    entries = (scenario.to_plain().get('perturbations') or []) if scenario is not None else []
    if any(entry['op'] == 'malformed_rows' for entry in entries):
        return results
    return [
        dc.replace(result, value=None, observed=None, passed=True, unmeasured=NOT_ASKED, decides=False)
        if result.metric == MALFORMED_RATE_DELTA
        else result
        for result in results
    ]


def skipped(results: list[MetricResult], moved: list[Move]) -> list[MetricResult]:
    """Each result a perturbation moves, on either column of a pair, as skipped by the first that does, with no value, passing."""
    kept = []
    for result in results:
        columns = (result.column, result.paired_with) if result.paired_with is not None else (result.column,)
        move = next((move for move in moved if any(move.moves(result.metric, column) for column in columns)), None)
        if move is not None:
            result = dc.replace(result, value=None, passed=True, expected=None, observed=None, unmeasured=None, skipped_by=move.id)
        kept.append(result)
    return kept


def attributed(changes: list[FieldChange], moved: list[Move]) -> list[FieldChange]:
    """Each failing change on a column a perturbation writes, passing, its message naming the perturbation."""
    kept = []
    for change in changes:
        move = next((move for move in moved if change.column in move.columns), None)
        if move is not None and not change.passed:
            change = dc.replace(change, passed=True, message=f'{change.message}; perturbation {move.id} writes it, so it passes')
        kept.append(change)
    return kept
