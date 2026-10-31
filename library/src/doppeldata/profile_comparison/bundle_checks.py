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
from doppeldata.bundle_generation import bundle_arguments
from doppeldata.bundle_generation import scenarios_document
from doppeldata.bundle_generation import scenarios_record
from doppeldata.bundle_generation import table_destination
from doppeldata.bundle_generation import table_scenarios
from doppeldata.manifest import BundleManifest
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
from doppeldata.profile_format import Issue
from doppeldata.profile_format import digest_of
from doppeldata.profiling import relationship_counts
from doppeldata.profiling.backend import CsvDialect
from doppeldata.scenario_format import InvalidScenario
from doppeldata.scenario_format import Scenario

Ends = tuple[str, list[str], str, list[str]]
# The metrics comparing which values a column holds, which a relationship drawing the column decides by its parent's
# keys, dealing each count to a parent at random. Its null share and distinct values it keeps.
DRAWN_KEY_METRICS = registered('ks', 'tvd', 'zero_rate_delta', 'boundary_adherence', 'correlation_max_abs_diff', 'conditional_kl')


@dc.dataclass(frozen=True)
class DrawnRelationship:
    """A relationship the plan draws: its name, its child table, the child columns it draws, and those it copies from the
    parent row."""

    name: str
    child: str
    columns: list[str]
    copied: list[str]


def check_bundle(
    bundle: Bundle, data: Any, *, thresholds: Thresholds | None, scenario: Scenario | Sequence[Scenario] | None
) -> BundleCheckReport:
    """`check` of a bundle, which documents it."""
    scenarios = table_scenarios(scenario, bundle)
    record = scenarios_record(scenarios)
    digest = digest_of(rfc8785.dumps(scenarios_document(scenarios))) if scenarios else None
    named, held = bundle_data(bundle, data)
    written = written_bundle_manifest(data) if not isinstance(data, Mapping) else None
    if written is not None:
        refuse_other_scenarios(record, digest, written, named)
    thresholds = thresholds or Thresholds()
    thresholds.check_columns([table.to_plain() for table in bundle.tables.values()])
    plan = bundle_arguments(bundle, 0, 1.0, scenarios).plan()
    relationships = drawn_relationships(plan)
    drawn = drawn_columns(relationships)
    checked, dialects = [], {}
    for name, _ in plan.tables():
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
        )
        widened = hot_parent_moves(applied, name, relationships)
        if widened:
            report = dc.replace(report, metrics=skipped(report.metrics, widened), changes=attributed(report.changes, widened))
        checked.append(CheckedTable(name, keys_skipped(report, drawn.get(name, {}))))
        dialect = written_dialect(table_written, profile, applied) if table_written is not None else None
        if dialect is not None:
            dialects[name] = dialect
    reported = ReportedScenario(record['name'], digest) if record is not None and digest is not None else None
    return BundleCheckReport(
        profile=ReportedProfile(bundle.name, bundle.digest, str(bundle.path) if bundle.path is not None else None),
        data=CheckedData(named, sum(table.report.data.rows for table in checked), None),
        tables=checked,
        relationships=relationship_rows(plan, scenarios, held, dialects, thresholds),
        scenario=reported,
    )


def bundle_data(bundle: Bundle, data: Any) -> tuple[str, dict[str, Any]]:
    """The data as the caller named it, and each table's: under `<data>/<table>/`, or as a mapping holds it."""
    if isinstance(data, Mapping):
        missing, extra = sorted(set(bundle.tables) - set(data)), sorted(set(data) - set(bundle.tables))
        if missing or extra:
            wrong = [f'lacks {", ".join(missing)}' if missing else '', f'holds {", ".join(extra)}, not tables of it' if extra else '']
            raise ValueError(f'the tables given {" and ".join(each for each in wrong if each)}; give each table of {bundle.name}')
        return 'the tables given', dict(data)
    if not isinstance(data, str | os.PathLike):
        raise TypeError(f'a bundle is checked from the directory its write left, or a mapping of each table, not {type(data).__name__}')
    root = os.fspath(data)
    return data_named(data), {name: table_destination(root, name) for name in bundle.tables}


def refuse_other_scenarios(record: dict[str, Any] | None, digest: str | None, written: BundleManifest, data: str) -> None:
    """Raise InvalidScenario where the scenarios given are not those the bundle's manifest records it was written under."""
    recorded = written.identity.get('scenario_digest')
    if digest == recorded:
        return
    held = written.document.get('scenario') or {}
    wrote = f'scenario {held.get("name")} ({recorded})' if recorded is not None else 'no scenario'
    if record is None:
        issue = Issue('data-has-scenario', '', f'{data} was written under {wrote}; check it under that scenario')
        raise InvalidScenario(data, [issue], refusal='cannot be checked with no scenario')
    message = f'{data} was written under {wrote}, and {record["name"]} is {digest}; check it under the scenarios that wrote it'
    issue = Issue('scenario-differs-from-data', '', message)
    raise InvalidScenario(record.get('path') or record['name'], [issue], refusal=f'is not the scenario {data} was written under')


def drawn_relationships(plan: '_core.BundlePlan') -> list[DrawnRelationship]:
    """Each relationship the plan draws, leaving out those it sets aside and each `inherited` entry it sets aside."""
    stated = json.loads(plan.bundle_json).get('relationships') or []
    drawn = []
    for relationship, (name, pointer, set_aside, entries_aside) in zip(stated, plan.relationships(), strict=True):
        if set_aside is None:
            child, columns, _, _ = ends_of(relationship)
            aside = {at for at, _ in entries_aside}
            entries = enumerate(relationship.get('inherited') or [])
            copied = [entry['child'] for place, entry in entries if f'{pointer}/inherited/{place}' not in aside]
            drawn.append(DrawnRelationship(name, child, columns, copied))
    return drawn


def drawn_columns(relationships: list[DrawnRelationship]) -> dict[str, dict[str, str]]:
    """Each child column a relationship draws, by table, beside the relationship's name."""
    drawn: dict[str, dict[str, str]] = {}
    for relationship in relationships:
        drawn.setdefault(relationship.child, {}).update(dict.fromkeys(relationship.columns, relationship.name))
    return drawn


def hot_parent_moves(applied: Scenario | None, table: str, relationships: list[DrawnRelationship]) -> list[Move]:
    """Each `hot_keys` on a column a relationship draws, as moving every column the relationship draws or copies. The op
    gives a row a hot parent's whole key and copies that parent's inherited columns, as the relationship would."""
    if applied is None:
        return []
    entries = applied.to_plain().get('perturbations') or []
    widened = []
    for entry, move in zip(entries, moves(applied), strict=True):
        if entry['op'] != 'hot_keys':
            continue
        for relationship in relationships:
            if relationship.child == table and entry['column'] in relationship.columns:
                widened.append(Move(move.id, frozenset(relationship.columns) | frozenset(relationship.copied), frozenset()))
    return widened


def keys_skipped(report: CheckReport, drawn: dict[str, str]) -> CheckReport:
    """`report` with each of `DRAWN_KEY_METRICS` on a column a relationship draws, on either column of a pair, skipped by
    the relationship, with no value, passing."""
    if not drawn:
        return report
    kept = []
    for result in report.metrics:
        by = next((drawn[column] for column in (result.column, result.paired_with) if column in drawn), None)
        if by is not None and result.metric in DRAWN_KEY_METRICS:
            result = dc.replace(result, value=None, passed=True, expected=None, observed=None, unmeasured=None, skipped_by=by)
        kept.append(result)
    return dc.replace(report, metrics=kept)


def ends_of(relationship: dict[str, Any]) -> Ends:
    """The child table, its columns, the parent table and its key columns, of a relationship's `table.column` lists."""
    child = [reference.split('.', 1) for reference in relationship['from']]
    parent = [reference.split('.', 1) for reference in relationship['to']]
    return child[0][0], [column for _, column in child], parent[0][0], [column for _, column in parent]


def relationship_rows(
    plan: '_core.BundlePlan',
    scenarios: dict[str, Scenario],
    held: dict[str, Any],
    dialects: dict[str, CsvDialect],
    thresholds: Thresholds,
) -> list[MetricResult]:
    """Each relationship metric's row on each relationship, against the bundle as the plan draws it. One the plan sets
    aside decides nothing, and one whose child columns a perturbation writes is skipped by it. Each is measured at the
    scale its parent's keys were generated at, against the parent table's stated rows."""
    drawn_bundle = json.loads(plan.bundle_json)
    stated = drawn_bundle.get('relationships') or []
    planned = plan.relationships()
    drawn = [index for index, (_, _, set_aside, _) in enumerate(planned) if set_aside is None]
    counted = dict(zip(drawn, relationship_counts(held, dialects, [ends_of(stated[index]) for index in drawn]), strict=True))
    moved = {table: moves(applied) for table, applied in scenarios.items()}
    rows = []
    for index, (name, _, set_aside, _) in enumerate(planned):
        child, columns, parent, _ = ends_of(stated[index])
        move = writing(moved.get(child, []), columns)
        values = {}
        if set_aside is None:
            keys = sum(held_keys for in_parent, _, held_keys in counted[index] if in_parent)
            stated_rows = drawn_bundle['tables'][parent]['dataset']['row_count']
            values = measured(stated[index], counted[index], keys / stated_rows if stated_rows else 1.0)
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
    """The first perturbation writing one of a relationship's child columns, which moves every metric of it."""
    return next((move for move in moved if move.columns & set(columns)), None)
