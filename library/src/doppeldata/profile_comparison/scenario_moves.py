"""What each of a scenario's perturbations moves in a check: the metrics it skips, and the column changes it makes pass."""

import dataclasses as dc

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
        Move(facts.id, frozenset(facts.writes), MOVED_METRICS.get(entry['op'], frozenset()))
        for entry, facts in zip(plain.get('perturbations') or [], perturbation_facts(plain), strict=True)
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
