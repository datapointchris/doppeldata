"""A bundle's generated tables checked against it: each table against its own profile, under the scenarios naming it, then
each relationship by the child rows its parent keys hold."""

import dataclasses as dc
import json
import os
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

import rfc8785

from doppeldata import _core
from doppeldata import issue_codes
from doppeldata.bundle_generation import bundle_arguments
from doppeldata.bundle_generation import scenarios_document
from doppeldata.bundle_generation import scenarios_record
from doppeldata.bundle_generation import table_destination
from doppeldata.bundle_generation import table_scenarios
from doppeldata.errors import InvalidArgument
from doppeldata.errors import SourceError
from doppeldata.errors import UnsupportedArgument
from doppeldata.issue_codes import Issue
from doppeldata.manifest import BundleManifest
from doppeldata.manifest import holds_unfinished_write
from doppeldata.manifest import unfinished
from doppeldata.manifest import written_bundle_manifest
from doppeldata.manifest import written_manifest
from doppeldata.profile_comparison import checked_table
from doppeldata.profile_comparison import data_named
from doppeldata.profile_comparison import metric_row
from doppeldata.profile_comparison import written_dialect
from doppeldata.profile_comparison.metrics import Unmeasured
from doppeldata.profile_comparison.relationship_metrics import RELATIONSHIP_METRICS
from doppeldata.profile_comparison.relationship_metrics import measured
from doppeldata.profile_comparison.reports import BundleCheckReport
from doppeldata.profile_comparison.reports import CheckedData
from doppeldata.profile_comparison.reports import CheckedTable
from doppeldata.profile_comparison.reports import CheckReport
from doppeldata.profile_comparison.reports import MetricResult
from doppeldata.profile_comparison.reports import ReportedProfile
from doppeldata.profile_comparison.reports import ReportedScenario
from doppeldata.profile_comparison.scenario_moves import Move
from doppeldata.profile_comparison.scenario_moves import attributed
from doppeldata.profile_comparison.scenario_moves import moves
from doppeldata.profile_comparison.scenario_moves import registered
from doppeldata.profile_comparison.scenario_moves import skipped
from doppeldata.profile_comparison.thresholds import Thresholds
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import sha256_of
from doppeldata.profiling import relationship_counts
from doppeldata.profiling.backend import CsvDialect
from doppeldata.scenario_format import InvalidScenario
from doppeldata.scenario_format import Scenario

# The metrics comparing which values a column holds, which a relationship drawing the column decides by its parent's
# keys, dealing each count to a parent at random. Its null share and distinct values it keeps.
DRAWN_KEY_METRICS = registered('ks', 'tvd', 'zero_fraction_delta', 'boundary_adherence', 'correlation_max_abs_diff', 'conditional_kl')


def check_bundle(
    bundle: Bundle, data: Any, *, thresholds: Thresholds | None, scenario: Scenario | Sequence[Scenario] | None
) -> BundleCheckReport:
    """`check` of a bundle, which documents it."""
    scenarios = table_scenarios(scenario, bundle)
    record = scenarios_record(scenarios)
    scenarios_sha256 = sha256_of(rfc8785.dumps(scenarios_document(scenarios))) if scenarios else None
    named, held = bundle_data(bundle, data)
    if not isinstance(data, Mapping) and holds_unfinished_write(data):
        raise SourceError(issue_codes.DATA_UNFINISHED, unfinished(named), value=named)
    written = written_bundle_manifest(data) if not isinstance(data, Mapping) else None
    if written is not None:
        refuse_other_scenarios(record, scenarios_sha256, written, named)
    thresholds = thresholds or Thresholds()
    thresholds.check_columns([table.to_plain() for table in bundle.tables.values()])
    plan = bundle_arguments(bundle, 0, 1.0, scenarios).plan()
    relationships: list[dict[str, Any]] = json.loads(plan.relationships_json)
    drawn = drawn_columns(relationships)
    checked, dialects = [], {}
    for name, table_plan in plan.tables():
        profile, applied = bundle.tables[name], scenarios.get(name)
        table_written = written_manifest(held[name])
        report = checked_table(
            profile,
            held[name],
            applied,
            table_written,
            thresholds=thresholds.of_table(profile.to_plain()),
            against=None,
            against_sample=None,
            html=None,
            planned=table_plan,
        )
        widened = planned_moves(applied, table_plan)
        if widened:
            report = dc.replace(report, metrics=skipped(report.metrics, widened), changes=attributed(report.changes, widened))
        checked.append(CheckedTable(name, keys_skipped(report, drawn.get(name, {}))))
        dialect = written_dialect(table_written, profile, applied) if table_written is not None else None
        if dialect is not None:
            dialects[name] = dialect
    reported = ReportedScenario(record['name'], scenarios_sha256) if record is not None and scenarios_sha256 is not None else None
    scale = written.identity.get('scale') if written is not None else None
    return BundleCheckReport(
        profile=ReportedProfile(bundle.name, bundle.sha256, str(bundle.path) if bundle.path is not None else None),
        data=CheckedData(
            named, sum(table.report.data.rows for table in checked), None, written.dataset_sha256 if written is not None else None
        ),
        tables=checked,
        relationships=relationship_rows(plan, relationships, scenarios, held, dialects, thresholds, scale),
        scenario=reported,
    )


def bundle_data(bundle: Bundle, data: Any) -> tuple[str, dict[str, Any]]:
    """The data as the caller named it, and each table's: under `<data>/<table>/`, or as a mapping holds it."""
    if isinstance(data, Mapping):
        missing, extra = sorted(set(bundle.tables) - set(data)), sorted(set(data) - set(bundle.tables))
        if missing or extra:
            wrong = [f'lacks {", ".join(missing)}' if missing else '', f'holds {", ".join(extra)}, not tables of it' if extra else '']
            message = f'the tables given {" and ".join(each for each in wrong if each)}; give each table of {bundle.name}'
            raise InvalidArgument(issue_codes.BUNDLE_DATA_MISMATCH, message, field='data', value=sorted(data), valid=list(bundle.tables))
        return 'the tables given', dict(data)
    if not isinstance(data, str | os.PathLike):
        message = f'a bundle is checked from the directory its write left, or a mapping of each table, not {type(data).__name__}'
        raise UnsupportedArgument(issue_codes.BUNDLE_DATA_MISMATCH, message, field='data', value=type(data).__name__)
    root = os.fspath(data)
    return data_named(data), {name: table_destination(root, name) for name in bundle.tables}


def refuse_other_scenarios(record: dict[str, Any] | None, scenarios_sha256: str | None, written: BundleManifest, data: str) -> None:
    """Raise InvalidScenario where the scenarios given are not those the bundle's manifest records it was written under."""
    recorded = written.identity.get('scenario_sha256')
    if scenarios_sha256 == recorded:
        return
    held = written.document.get('scenario') or {}
    wrote = f'scenario {held.get("name")} ({recorded})' if recorded is not None else 'no scenario'
    if record is None:
        issue = Issue(issue_codes.DATA_HAS_SCENARIO, None, f'{data} was written under {wrote}; check it under that scenario', recorded)
        raise InvalidScenario(data, [issue], refusal='cannot be checked with no scenario')
    message = f'{data} was written under {wrote}, and {record["name"]} is {scenarios_sha256}; check it under the scenarios that wrote it'
    issue = Issue(issue_codes.SCENARIO_DIFFERS_FROM_DATA, None, message, scenarios_sha256)
    raise InvalidScenario(record.get('path') or record['name'], [issue], refusal=f'is not the scenario {data} was written under')


def drawn_columns(relationships: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
    """Each child column a relationship the plan draws writes, by table, beside the relationship's name."""
    drawn: dict[str, dict[str, str]] = {}
    for relationship in relationships:
        if relationship['set_aside'] is None:
            drawn.setdefault(relationship['child'], {}).update(dict.fromkeys(relationship['child_columns'], relationship['name']))
    return drawn


def planned_moves(applied: Scenario | None, plan: '_core.Plan') -> list[Move]:
    """Each perturbation the plan writes more columns for than its scenario names, as moving every one of them. A `hot_keys`
    on a foreign key gives a row a hot parent's whole key and copies that parent's inherited columns, as the relationship
    would."""
    if applied is None:
        return []
    return [
        Move(move.id, frozenset(columns), frozenset())
        for move, (_, columns) in zip(moves(applied), plan.perturbation_writes(), strict=True)
        if not set(columns) <= move.columns
    ]


def keys_skipped(report: CheckReport, drawn: dict[str, str]) -> CheckReport:
    """`report` with each of `DRAWN_KEY_METRICS` on a column a relationship draws, on either column of a pair, drawn by
    the relationship, with no value, passing."""
    if not drawn:
        return report
    kept = []
    for result in report.metrics:
        by = next((drawn[column] for column in (result.column, result.paired_with) if column in drawn), None)
        if by is not None and result.metric in DRAWN_KEY_METRICS:
            result = dc.replace(result, value=None, passed=True, expected=None, observed=None, unmeasured=None, drawn_by=by)
        kept.append(result)
    return dc.replace(report, metrics=kept)


def relationship_rows(
    plan: '_core.BundlePlan',
    planned: list[dict[str, Any]],
    scenarios: dict[str, Scenario],
    held: dict[str, Any],
    dialects: dict[str, CsvDialect],
    thresholds: Thresholds,
    scale: str | None,
) -> list[MetricResult]:
    """Each relationship metric's row on each relationship, against the bundle as the plan draws it. One the plan sets
    aside decides nothing. One whose child columns, or whose parent's key columns, a perturbation writes is skipped by it.
    Each is measured at `scale`, the one the root manifest records, or where the tables come with none, at the scale the
    parent's keys show against its stated rows."""
    drawn_bundle = json.loads(plan.bundle_json)
    stated = drawn_bundle.get('relationships') or []
    ends = {
        index: (relationship['child'], relationship['child_columns'], relationship['parent'], relationship['parent_columns'])
        for index, relationship in enumerate(planned)
        if relationship['set_aside'] is None
    }
    counted = dict(zip(ends, relationship_counts(held, dialects, list(ends.values())), strict=True))
    moved = {table: moves(applied) for table, applied in scenarios.items()}
    rows = []
    for index, relationship in enumerate(planned):
        name, set_aside = relationship['name'], relationship['set_aside']
        child_move = writing(moved.get(relationship['child'], []), relationship['child_columns'])
        move = child_move or writing(moved.get(relationship['parent'], []), relationship['parent_columns'])
        values = {}
        if set_aside is None:
            keys = sum(held_keys for in_parent, _, held_keys in counted[index] if in_parent)
            stated_rows = drawn_bundle['tables'][relationship['parent']]['dataset']['rows']
            at = float(scale) if scale is not None else keys / stated_rows if stated_rows else 1.0
            values = measured(stated[index], counted[index], at)
        for metric in RELATIONSHIP_METRICS:
            threshold = thresholds.threshold(metric, None)
            if set_aside is not None:
                aside = Unmeasured(f'the plan sets it aside, as {set_aside}')
                rows.append(metric_row(metric, None, aside, threshold, unmeasured_passes=True, relationship=name, decides=False))
                continue
            compared = values[metric.name]
            if compared is None:
                continue
            row = metric_row(metric, None, compared, threshold, unmeasured_passes=False, relationship=name)
            if move is not None:
                row = dc.replace(row, value=None, passed=True, expected=None, observed=None, unmeasured=None, skipped_by=move.id)
            rows.append(row)
    return rows


def writing(moved: list[Move], columns: list[str]) -> Move | None:
    """The first perturbation writing one of a relationship's columns, which moves every metric of it."""
    return next((move for move in moved if move.columns & set(columns)), None)
