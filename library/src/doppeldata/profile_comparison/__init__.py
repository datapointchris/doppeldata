"""Comparing two profiles of one table: generated data checked against its profile, and two profiles diffed for drift.

Neither reads the real rows again. A check profiles the generated data with the source profile's edges frozen,
so histograms and value lists compare bin for bin.
"""

import warnings
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from doppeldata.errors import ColumnsNotFrozen
from doppeldata.errors import EarlierProfileNotReused
from doppeldata.errors import PoorFits
from doppeldata.privacy_policy import Policy
from doppeldata.profile_comparison.field_changes import column_changes
from doppeldata.profile_comparison.field_changes import overflow_changes
from doppeldata.profile_comparison.field_changes import source_changes
from doppeldata.profile_comparison.metrics import METRICS
from doppeldata.profile_comparison.metrics import Document
from doppeldata.profile_comparison.metrics import Unmeasured
from doppeldata.profile_comparison.reports import CheckedData
from doppeldata.profile_comparison.reports import CheckReport
from doppeldata.profile_comparison.reports import DiffReport
from doppeldata.profile_comparison.reports import MetricResult
from doppeldata.profile_comparison.reports import ReportedProfile
from doppeldata.profile_comparison.reports import ReportedScenario
from doppeldata.profile_comparison.scenario_moves import attributed
from doppeldata.profile_comparison.scenario_moves import moves
from doppeldata.profile_comparison.scenario_moves import skipped
from doppeldata.profile_comparison.thresholds import Thresholds
from doppeldata.profile_format import Profile
from doppeldata.profiling import profile as profile_data
from doppeldata.profiling import sources
from doppeldata.profiling.detector_packs import EnabledPack
from doppeldata.profiling.detector_packs import enabled_packs
from doppeldata.scenario_format import Scenario
from doppeldata.scenario_format import composed
from doppeldata.scenario_format import refuse_other_scenario

# A report's values keep this many significant digits, and each verdict is taken on the value as written.
REPORT_DIGITS = 6


def check(
    profile: Profile,
    data: Any,
    *,
    thresholds: Thresholds | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
) -> CheckReport:
    """Profile `data` with `profile`'s edges and value lists frozen, and measure every metric and column change against `profile`.

    `data` is anything `doppeldata.profile` reads. The report's `passed` is False when any metric misses its threshold or
    cannot be measured, or `column_changes` finds a change that fails.

    With `scenario`, a Scenario or a list composed in order, the data is measured against the profile its patch makes. Each
    metric on what a perturbation moves is listed as skipped by it, and a change on a column one writes passes. Each
    expectation it declares is measured, and the report's `verified` is False when any misses its bounds.

    Where `data` names a directory or prefix a doppeldata write left `_manifest.json` in, InvalidScenario refuses a
    `scenario` other than the one it records, and no `scenario` where it records one.
    """
    applied = composed(scenario)
    written = sources.written_manifest(data)
    if written is not None:
        refuse_other_scenario(applied, written, data_named(data))
    effective = applied.apply(profile) if applied is not None else profile
    source = effective.to_plain()
    thresholds = thresholds or Thresholds()
    thresholds.check_columns(source)
    # The data is profiled under the profile's own policy, so each field it writes is one the profile could hold.
    privacy = source.get('privacy') or {}
    policy = Policy(privacy.get('policy') or Policy.DEFAULT)
    k_min = policy.k_min(privacy.get('k_min'))
    # The data is typed with the packs the profile was, so a type one of them declares can be confirmed.
    packs = enabled_packs((source.get('generated') or {}).get('detector_packs') or ())
    with warnings.catch_warnings():
        # The report states both in its own terms: a fit's looseness through `ks`, and a missing column as a change.
        warnings.simplefilter('ignore', PoorFits)
        warnings.simplefilter('ignore', ColumnsNotFrozen)
        # Generated data is never the data the profile was made from, so the profile is always made again.
        warnings.simplefilter('ignore', EarlierProfileNotReused)
        checked = profile_data(data, policy=policy, k_min=k_min, edges_from=effective, packs=[each.pack.name for each in packs])
    observed = checked.to_plain()
    # A check vouches for the data, so a statistic the profile states and the data cannot supply fails.
    metrics = measured(source, observed, thresholds, unmeasured_passes=False)
    changes = column_changes(source, observed, writer_changes_pass=True, declaring=declared_types(packs), listing=listed_types(packs))
    if applied is None:
        reported_scenario, expectations = None, []
    else:
        moved = moves(applied)
        metrics, changes = skipped(metrics, moved), attributed(changes, moved)
        reported_scenario, expectations = ReportedScenario(applied.name, applied.digest), applied.verify(data)
    rows = (observed.get('dataset') or {}).get('row_count') or 0
    return CheckReport(
        profile=reported(profile),
        data=CheckedData(data_named(data), rows, (observed.get('generated') or {}).get('source_fingerprint')),
        metrics=metrics,
        changes=changes,
        scenario=reported_scenario,
        expectations=expectations,
    )


def diff(a: Profile, b: Profile, *, thresholds: Thresholds | None = None) -> DiffReport:
    """Measure every metric of `b` against `a`, and list every field that changed between them.

    Overflow is measured where `b` was profiled with `a`'s edges frozen, as `profile(..., edges_from=a)` does.
    """
    earlier, later = a.to_plain(), b.to_plain()
    thresholds = thresholds or Thresholds()
    thresholds.check_columns(earlier)
    changes = column_changes(earlier, later, writer_changes_pass=False, declaring={}, listing={})
    changes += overflow_changes(earlier, later) + source_changes(earlier, later)
    # A diff reports what changed between two profiles, and B binned afresh supplies no overflow, so an unmeasured metric passes.
    return DiffReport(reported(a), reported(b), measured(earlier, later, thresholds, unmeasured_passes=True), changes)


def measured(source: Document, observed: Document, thresholds: Thresholds, *, unmeasured_passes: bool) -> list[MetricResult]:
    """Every registered metric's value for each column it applies to, with its threshold and verdict.

    A metric the source states and the other profile cannot supply has a row with no value, passing as `unmeasured_passes`.
    """
    results = []
    for metric in METRICS:
        for measurement in metric.measure(source, observed):
            threshold = thresholds.threshold(metric, measurement.column)
            outcome = measurement.compared
            if isinstance(outcome, Unmeasured):
                result = MetricResult(
                    metric.name,
                    measurement.column,
                    None,
                    threshold,
                    metric.bound,
                    unmeasured_passes,
                    outcome.expected,
                    None,
                    outcome.reason,
                )
            else:
                written = significant(outcome.value)
                passed = metric.passes(written, threshold)
                result = MetricResult(
                    metric.name, measurement.column, written, threshold, metric.bound, passed, outcome.expected, outcome.observed
                )
            results.append(result)
    return results


def declared_types(packs: list[EnabledPack]) -> dict[str, str]:
    """The pack that declares each semantic type, by type."""
    return {detector.type: each.pack.name for each in packs for detector in each.pack.detectors}


def listed_types(packs: list[EnabledPack]) -> dict[str, str]:
    """The code list each semantic type is read from, by type, for the types a detector reads one for."""
    return {detector.type: detector.code_list for each in packs for detector in each.pack.detectors if detector.code_list}


def reported(profile: Profile) -> ReportedProfile:
    return ReportedProfile(profile.document['name'], profile.digest, str(profile.path) if profile.path is not None else None)


def data_named(data: Any) -> str:
    """The data as the caller named it: its paths, or the type of an in-memory table."""
    if isinstance(data, str | Path):
        return str(data)
    if isinstance(data, list | tuple):
        return ' '.join(str(item) for item in data)
    return f'<{type(data).__module__}.{type(data).__name__}>'


def significant(value: float) -> float:
    return float(f'{value:.{REPORT_DIGITS}g}')
