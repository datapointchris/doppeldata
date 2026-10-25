"""Comparing two profiles of one table: generated data checked against its profile, and two profiles diffed for drift.

Neither reads the real rows again. A check profiles the generated data with the source profile's edges frozen,
so histograms and value lists compare bin for bin.
"""

import warnings
from pathlib import Path
from typing import Any

from doppeldata.errors import ColumnsNotFrozen
from doppeldata.errors import PoorFits
from doppeldata.profile_comparison.field_changes import column_changes
from doppeldata.profile_comparison.field_changes import overflow_changes
from doppeldata.profile_comparison.field_changes import source_changes
from doppeldata.profile_comparison.metrics import METRICS
from doppeldata.profile_comparison.metrics import Document
from doppeldata.profile_comparison.reports import CheckedData
from doppeldata.profile_comparison.reports import CheckReport
from doppeldata.profile_comparison.reports import DiffReport
from doppeldata.profile_comparison.reports import MetricResult
from doppeldata.profile_comparison.reports import ReportedProfile
from doppeldata.profile_comparison.thresholds import Thresholds
from doppeldata.profile_format import Profile
from doppeldata.profiling import DEFAULT_K_MIN
from doppeldata.profiling import profile as profile_data

# A report's values keep this many significant digits, and each verdict is taken on the value as written.
REPORT_DIGITS = 6


def check(profile: Profile, data: Any, *, thresholds: Thresholds | None = None) -> CheckReport:
    """Profile `data` with `profile`'s edges and value lists frozen, and measure every metric and column change against `profile`.

    `data` is anything `doppeldata.profile` reads. The report's `passed` is False when any metric misses its threshold,
    or the data drops, adds or changes the kind of a column.
    """
    source = profile.to_plain()
    thresholds = thresholds or Thresholds()
    thresholds.check_columns(source)
    k_min = (source.get('privacy') or {}).get('k_min') or DEFAULT_K_MIN
    with warnings.catch_warnings():
        # The report states both in its own terms: a fit's looseness through `ks`, and a missing column as a change.
        warnings.simplefilter('ignore', PoorFits)
        warnings.simplefilter('ignore', ColumnsNotFrozen)
        checked = profile_data(data, k_min=k_min, edges_from=profile)
    observed = checked.to_plain()
    return CheckReport(
        profile=reported(profile),
        data=CheckedData(
            data_named(data),
            (observed.get('dataset') or {}).get('row_count') or 0,
            (observed.get('generated') or {}).get('source_fingerprint'),
        ),
        metrics=measured(source, observed, thresholds),
        changes=column_changes(source, observed, physical_type_drifts=False),
    )


def diff(a: Profile, b: Profile, *, thresholds: Thresholds | None = None) -> DiffReport:
    """Measure every metric of `b` against `a`, and list every field that changed between them.

    Overflow is measured where `b` was profiled with `a`'s edges frozen, as `profile(..., edges_from=a)` does.
    """
    earlier, later = a.to_plain(), b.to_plain()
    thresholds = thresholds or Thresholds()
    thresholds.check_columns(earlier)
    changes = column_changes(earlier, later, physical_type_drifts=True) + overflow_changes(earlier, later) + source_changes(earlier, later)
    return DiffReport(reported(a), reported(b), measured(earlier, later, thresholds), changes)


def measured(source: Document, observed: Document, thresholds: Thresholds) -> list[MetricResult]:
    """Every registered metric's value for each column it applies to, with its threshold and verdict."""
    results = []
    for metric in METRICS:
        for measurement in metric.measure(source, observed):
            value, source_stat, observed_stat = measurement.compared
            written = significant(value)
            threshold = thresholds.threshold(metric, measurement.column)
            results.append(
                MetricResult(
                    metric.name,
                    measurement.column,
                    written,
                    threshold,
                    metric.bound,
                    metric.passes(written, threshold),
                    source_stat,
                    observed_stat,
                )
            )
    return results


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
