"""Comparing two profiles of one table: generated data checked against its profile, and two profiles diffed for drift.

Neither reads the real rows again, except a check handed them as `against`. A check profiles the generated data with
the source profile's edges frozen, so histograms and value lists compare bin for bin.
"""

import dataclasses as dc
import importlib.util
import os
import warnings
from collections.abc import Iterable
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import overload

from doppeldata import _core
from doppeldata.dataset_generation import engine_plan
from doppeldata.errors import ColumnsNotFrozen
from doppeldata.errors import EarlierProfileNotReused
from doppeldata.errors import PoorFits
from doppeldata.errors import ProfilingWarning
from doppeldata.errors import SourceError
from doppeldata.manifest import Manifest
from doppeldata.manifest import written_manifest
from doppeldata.privacy_policy import Policy
from doppeldata.profile_comparison import real_data_scores
from doppeldata.profile_comparison import side_by_side_html
from doppeldata.profile_comparison.field_changes import column_changes
from doppeldata.profile_comparison.field_changes import overflow_changes
from doppeldata.profile_comparison.field_changes import source_changes
from doppeldata.profile_comparison.metrics import METRICS
from doppeldata.profile_comparison.metrics import Compared
from doppeldata.profile_comparison.metrics import Document
from doppeldata.profile_comparison.metrics import Thresholded
from doppeldata.profile_comparison.metrics import Unmeasured
from doppeldata.profile_comparison.real_data_scores import DEFAULT_SAMPLE_ROWS
from doppeldata.profile_comparison.reports import BundleCheckReport
from doppeldata.profile_comparison.reports import CheckedData
from doppeldata.profile_comparison.reports import CheckReport
from doppeldata.profile_comparison.reports import DiffReport
from doppeldata.profile_comparison.reports import MetricResult
from doppeldata.profile_comparison.reports import RealData
from doppeldata.profile_comparison.reports import ReportedProfile
from doppeldata.profile_comparison.reports import ReportedScenario
from doppeldata.profile_comparison.scenario_moves import attributed
from doppeldata.profile_comparison.scenario_moves import moves
from doppeldata.profile_comparison.scenario_moves import skipped
from doppeldata.profile_comparison.scenario_moves import uncompared_malformed_rates
from doppeldata.profile_comparison.thresholds import Thresholds
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import Profile
from doppeldata.profiling import SampledTable
from doppeldata.profiling import profile_in_dialect
from doppeldata.profiling import sampled_table
from doppeldata.profiling.backend import CsvDialect
from doppeldata.profiling.detector_packs import EnabledPack
from doppeldata.profiling.detector_packs import enabled_packs
from doppeldata.profiling.file_traits import WRITTEN_ENCODINGS
from doppeldata.scenario_format import InvalidScenario
from doppeldata.scenario_format import Scenario
from doppeldata.scenario_format import composed
from doppeldata.scenario_format import refuse_other_scenario

if TYPE_CHECKING:
    import polars as pl

# A report's values keep this many significant digits, and each verdict is taken on the value as written.
REPORT_DIGITS = 6


@overload
def check(
    profile: Profile,
    data: Any,
    *,
    thresholds: Thresholds | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
    against: Any = None,
    against_sample: int | None = None,
    html: str | Path | None = None,
) -> CheckReport: ...


@overload
def check(
    profile: Bundle,
    data: Any,
    *,
    thresholds: Thresholds | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
    against: None = None,
    against_sample: None = None,
    html: None = None,
) -> BundleCheckReport: ...


def check(
    profile: Profile | Bundle,
    data: Any,
    *,
    thresholds: Thresholds | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
    against: Any = None,
    against_sample: int | None = None,
    html: str | Path | None = None,
) -> CheckReport | BundleCheckReport:
    """Profile `data` with `profile`'s edges and value lists frozen, and measure every metric and column change against `profile`.

    `data` is anything `doppeldata.profile` reads. The report's `passed` is False when any metric that decides misses its
    threshold or cannot be measured, or `column_changes` finds a change that fails.

    With `scenario`, a Scenario or a list composed in order, the data is measured against the profile its patch makes. Each
    metric on what a perturbation moves is listed as skipped by it, and a change on a column one writes passes. Each
    expectation it declares is measured, and the report's `verified` is False when any misses its bounds.

    Where `data` names a directory or prefix a doppeldata write left `_manifest.json` in, or one file a manifest beside it
    records alone, InvalidScenario refuses a `scenario` other than the one it records, and no `scenario` where it records one.
    InvalidScenario also refuses data holding a fault a check cannot read back in its format, before any row is read.
    `malformed_rate_delta` decides nothing on data that no malformed_rows asked for malformed rows in. Where the manifest
    records a `cdc` write, the check reads its full load alone, and a ProfilingWarning says the change files and the
    expected table went unmeasured. Delimited files a manifest records are read in the dialect it records, never one
    sniffed from them, and files with no header take their column names from the plan it records.

    `against` is the real data, read as `data` is. Each of `SCORES` then scores a seeded sample of `against_sample` rows
    from each side, `DEFAULT_SAMPLE_ROWS` unless given, as SDMetrics defines it. A score decides only where `thresholds`
    sets its threshold. `html` writes the two samples side by side, showing the real data's extremes and categories. It
    needs sweetviz, which doppeldata does not install, and `PackagesMissing` names it where it is missing. A
    `SourceError` the real data raises has `role` `against`.

    With a `Bundle`, `data` is the directory or prefix a bundle's write left, each table under `<data>/<table>/`, or a
    mapping of each table's name to its data, as `generate` returns it. Each table is checked against its own profile,
    under the scenarios naming it, and each relationship is measured by `RELATIONSHIP_METRICS`. The report is a
    `BundleCheckReport`, and a bundle takes no `against`.
    """
    if not isinstance(profile, Profile | Bundle):
        raise TypeError(f'check takes a Profile or a Bundle, as Profile.load and Bundle.load read them, not {type(profile).__name__}')
    if isinstance(profile, Bundle):
        from doppeldata.profile_comparison import bundle_checks

        if against is not None or against_sample is not None or html is not None:
            raise ValueError('against scores one table against its real data; check a table of the bundle on its own to score it')
        return bundle_checks.check_bundle(profile, data, thresholds=thresholds, scenario=scenario)
    applied = composed(scenario)
    written = written_manifest(data)
    if written is not None:
        refuse_other_scenario(applied, written, data_named(data))
    return checked_table(profile, data, applied, written, thresholds=thresholds, against=against, against_sample=against_sample, html=html)


def checked_table(
    profile: Profile,
    data: Any,
    applied: Scenario | None,
    written: Manifest | None,
    *,
    thresholds: Thresholds | None,
    against: Any,
    against_sample: int | None,
    html: str | Path | None,
) -> CheckReport:
    """`check` of one table, under the scenario that `written`, its manifest where it has one, records it was written under."""
    unreadable = unreadable_refusal(applied, written, data)
    if unreadable is not None:
        raise InvalidScenario(data_named(data), [], refusal=unreadable)
    effective = applied.apply(profile) if applied is not None else profile
    source = effective.to_plain()
    refusal = refused(source, against, against_sample, html)
    if refusal is not None:
        raise ValueError(refusal)
    if html is not None:
        side_by_side_html.missing_packages()
    thresholds = thresholds or Thresholds()
    thresholds.check_columns(source)
    thresholds.check_scores(scoring=against is not None, refusing='no real data is named to score it against')
    columns = real_data_scores.scored_columns(source)
    named = data_named(data)
    dialect = written_dialect(written, profile, applied) if written is not None else None
    if written is not None and written.cdc is not None:
        data = full_load(written, data)
    sample_rows = against_sample or DEFAULT_SAMPLE_ROWS
    real = None
    if against is not None:
        real = real_sample(against, sample_rows, columns)
        if hasattr(data, '__arrow_c_stream__'):
            # A stream is read once, and the profile and the sample each read the data.
            data = arrow_table(data)
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
        checked = profile_in_dialect(
            data, dialect, policy=policy, k_min=k_min, edges_from=effective, packs=[each.pack.name for each in packs]
        )
    observed = checked.to_plain()
    fingerprint = (observed.get('generated') or {}).get('source_fingerprint')
    # A check vouches for the data, so a statistic the profile states and the data cannot supply fails.
    metrics = uncompared_malformed_rates(measured(source, observed, thresholds, unmeasured_passes=False), applied)
    against_scored = None
    if real is not None:
        against_scored = scored(real, data, named, fingerprint, columns, thresholds, data_named(against), sample_rows)
        metrics += against_scored.rows
    changes = column_changes(source, observed, writer_changes_pass=True, declaring=declared_types(packs), listing=listed_types(packs))
    if applied is None:
        reported_scenario, expectations = None, []
    else:
        moved = moves(applied)
        metrics, changes = skipped(metrics, moved), attributed(changes, moved)
        reported_scenario, expectations = ReportedScenario(applied.name, applied.digest), applied.verify(data)
    report = CheckReport(
        profile=reported(profile),
        data=CheckedData(named, (observed.get('dataset') or {}).get('row_count') or 0, fingerprint),
        metrics=metrics,
        changes=changes,
        scenario=reported_scenario,
        expectations=expectations,
        against=against_scored.real_data if against_scored is not None else None,
    )
    if html is not None and against_scored is not None:
        side_by_side_html.write_side_by_side(against_scored.real_frame, against_scored.checked_frame, html)
    return report


# Each breaks the one dialect and header a check reads every file in.
def unreadable_refusal(scenario: Scenario | None, written: Manifest | None, data: Any) -> str | None:
    """Why a check cannot read `data` back as one table, or None. The engine says which fault kinds a check cannot read back
    in the data's format, of the kinds the manifest counts where there is one, else of those the scenario's malformed_rows
    names. A local path that does not exist is left to the profiler, which says so."""
    cannot = 'which a check cannot read back as one table; check data written without them'
    if written is not None and written.cdc is not None:
        # A change stream's faults lie in its change files, which a check does not read.
        return None
    if written is not None:
        held = unreadable(written.faults or {}, written.format)
        return f'holds {" and ".join(held)} faults, {cannot}' if held else None
    if isinstance(data, str | os.PathLike) and not os.fspath(data).startswith('s3://') and not Path(data).exists():
        return None
    entries = (scenario.to_plain().get('perturbations') or []) if scenario is not None else []
    named_kinds = {kind for entry in entries if entry['op'] == 'malformed_rows' for kind in entry.get('kinds') or []}
    data_format = format_of_data(data) if named_kinds else None
    named = unreadable(named_kinds, data_format) if data_format is not None else []
    return f'is checked under a scenario writing {" and ".join(named)} faults, {cannot}' if named else None


def full_load(written: Manifest, data: str | os.PathLike[str]) -> list[str]:
    """The full-load files of the `cdc` write at `data`, which a check reads in place of the whole write, warned of once."""
    root = os.fspath(data).rstrip('/')
    stream = written.cdc or {}
    loads = [file['path'] for file in written.files]
    named = ' to '.join(dict.fromkeys([loads[0], loads[-1]])) if loads else 'no file'
    message = (
        f'{root} holds a cdc change stream, so the check read its full load, {named}, and left its '
        f'{len(stream.get("files") or []):,} change files and {stream.get("expected_prefix")} unmeasured: they are a merge '
        "test's input and its answer"
    )
    warnings.warn(message, ProfilingWarning, stacklevel=3)
    return [f'{root}/{file["path"]}' for file in written.files]


def written_dialect(written: Manifest, profile: Profile, scenario: Scenario | None) -> CsvDialect | None:
    """The dialect a delimited write's manifest records, or None for another format. Where the files hold no header,
    the columns are named as the engine names them for the plan the manifest records."""
    if written.format not in ('csv', 'tsv'):
        return None
    writer = written.document['writer']
    names: tuple[str, ...] = ()
    if not writer['header']:
        identity = written.identity
        plan = engine_plan(profile, int(identity['dataset_rows']), int(identity['seed']), scenario)
        names = tuple(plan.file_columns(written.document['layout']['partition_by']))
    read_encodings = {spelled: read for read, spelled in WRITTEN_ENCODINGS.items()}
    return CsvDialect(
        delimiter=writer['delimiter'] or ('\t' if written.format == 'tsv' else ','),
        quote=writer['quote'],
        escape=writer['escape'] or writer['quote'],
        header=writer['header'],
        encoding=read_encodings[writer['encoding']],
        names=names,
    )


def unreadable(kinds: Iterable[str], data_format: str) -> list[str]:
    """Each of `kinds` the engine says a check cannot read back in files of `data_format`."""
    return sorted(kind for kind in kinds if not _core.fault_kind_facts(kind, data_format)[2])


def format_of_data(data: Any) -> str | None:
    """The format of the files `data` names, or None where it names none, leaving the profiler to say why."""
    # Listing the files imports boto3, and without it profiling the data refuses next, naming boto3.
    if importlib.util.find_spec('boto3') is None:
        return None
    from doppeldata.profiling import sources

    try:
        found = sources.resolve(data).format
    except SourceError:
        return None
    return found.value if found is not None else None


def refused(source: Document, against: Any, against_sample: int | None, html: str | Path | None) -> str | None:
    """Why a check cannot take these arguments, before any row is read, or None."""
    if against is None and (html is not None or against_sample is not None):
        return 'html and against_sample read the real data, so each needs against'
    if against_sample is not None and against_sample < 1:
        return f'against_sample must be a positive number of rows, not {against_sample}'
    if against is not None and not real_data_scores.scored_columns(source).ordered:
        return NOTHING_SCORED
    return side_by_side_html.unwritable(Path(html)) if html is not None else None


NOTHING_SCORED = (
    'the profile holds no column a score reads: each is an identifier, free text, a sequence, sensitive, or withheld by its privacy policy'
)


@dc.dataclass(frozen=True)
class AgainstScored:
    """The real data a check scored against, each score's row, and the two frames the scores read."""

    real_data: RealData
    rows: list[MetricResult]
    real_frame: 'pl.DataFrame'
    checked_frame: 'pl.DataFrame'


def real_sample(against: Any, rows: int, columns: real_data_scores.ScoredColumns) -> SampledTable:
    """The real data's seeded sample, refused where it holds none of the scored columns. Each refusal's role is `against`."""
    try:
        real = sampled_table(against, rows)
        if not set(columns.ordered) & set(real.table.column_names):
            raise SourceError(f'{data_named(against)} holds none of the columns a score reads: {", ".join(columns.ordered)}')
    except SourceError as error:
        error.role = 'against'
        raise
    return real


def arrow_table(data: Any) -> Any:
    import pyarrow as pa

    return pa.table(data)


def scored(
    real: SampledTable,
    data: Any,
    named: str,
    fingerprint: str | None,
    columns: real_data_scores.ScoredColumns,
    thresholds: Thresholds,
    against_named: str,
    rows: int,
) -> AgainstScored:
    """Each score's row: deciding only where `thresholds` sets it, and failing when it decides and is unscored.

    Both samples are cut to the smaller one's rows, so each side's scores read as many rows. Data whose listing changed
    since it was profiled raises the `SourceError` a changed file does.
    """
    checked = sampled_table(data, rows)
    if checked.fingerprint != fingerprint:
        raise SourceError(f'{named} changed between its profile and its sample; check it again once it stops changing', exit_code=1)
    scored_rows = min(real.table.num_rows, checked.table.num_rows)
    real_frame, checked_frame = (real_data_scores.frame(side, columns, scored_rows) for side in (real, checked))
    results = []
    for outcome in real_data_scores.scores(real_frame, checked_frame, columns):
        score, compared = outcome.score, outcome.compared
        keyed = outcome.column if outcome.paired_with is None else None
        if isinstance(compared, Compared) and compared.expected is not None and compared.observed is not None:
            compared = compared._replace(expected=significant(compared.expected), observed=significant(compared.observed))
        decides = thresholds.sets(score.name, keyed)
        threshold = thresholds.threshold(score, keyed)
        results.append(
            metric_row(
                score, outcome.column, compared, threshold, unmeasured_passes=False, paired_with=outcome.paired_with, decides=decides
            )
        )
    real_data = RealData(against_named, real.rows, real.fingerprint, scored_rows, real_data_scores.CORRELATION)
    return AgainstScored(real_data, results, real_frame, checked_frame)


def metric_row(
    measure: Thresholded,
    column: str | None,
    compared: Compared | Unmeasured,
    threshold: float,
    *,
    unmeasured_passes: bool,
    paired_with: str | None = None,
    worst_given_value: str | None = None,
    relationship: str | None = None,
    decides: bool = True,
) -> MetricResult:
    """One measure's row at `threshold`, its value written to `REPORT_DIGITS`. A row that decides nothing passes."""
    if isinstance(compared, Unmeasured):
        passed = unmeasured_passes or not decides
        return MetricResult(
            measure.name,
            column,
            None,
            threshold,
            measure.bound,
            passed,
            expected=compared.expected,
            unmeasured=compared.reason,
            paired_with=paired_with,
            worst_given_value=worst_given_value,
            relationship=relationship,
            decides=decides,
        )
    written = significant(compared.value)
    passed = measure.passes(written, threshold) or not decides
    return MetricResult(
        measure.name,
        column,
        written,
        threshold,
        measure.bound,
        passed,
        expected=compared.expected,
        observed=compared.observed,
        paired_with=paired_with,
        worst_given_value=worst_given_value,
        relationship=relationship,
        decides=decides,
    )


def diff(a: Profile, b: Profile, *, thresholds: Thresholds | None = None) -> DiffReport:
    """Measure every metric of `b` against `a`, and list every field that changed between them.

    Overflow is measured where `b` was profiled with `a`'s edges frozen, as `profile(..., edges_from=a)` does.
    """
    earlier, later = a.to_plain(), b.to_plain()
    thresholds = thresholds or Thresholds()
    thresholds.check_columns(earlier)
    thresholds.check_scores(scoring=False, refusing='a diff compares two profiles and reads no data')
    changes = column_changes(earlier, later, writer_changes_pass=False, declaring={}, listing={})
    changes += overflow_changes(earlier, later) + source_changes(earlier, later)
    # A diff reports what changed between two profiles, and B binned afresh supplies no overflow, so an unmeasured metric passes.
    return DiffReport(reported(a), reported(b), measured(earlier, later, thresholds, unmeasured_passes=True), changes)


def measured(source: Document, observed: Document, thresholds: Thresholds, *, unmeasured_passes: bool) -> list[MetricResult]:
    """Every registered metric's value for each column it applies to, with its threshold and verdict.

    A metric the source states and the other profile cannot supply has a row with no value, passing as `unmeasured_passes`.
    """
    return [
        metric_row(
            metric,
            measurement.column,
            measurement.compared,
            thresholds.threshold(metric, measurement.column),
            unmeasured_passes=unmeasured_passes,
            paired_with=measurement.paired_with,
            worst_given_value=measurement.given_value,
        )
        for metric in METRICS
        for measurement in metric.measure(source, observed)
    ]


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
