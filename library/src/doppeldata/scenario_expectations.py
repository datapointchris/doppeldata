"""The statistics a scenario's `expect` can name, each measured on generated data as a Polars LazyFrame.

`Scenario.verify` imports it by name, because it imports Polars, which doppeldata does not install.
"""

from collections.abc import Callable
from typing import Any

import polars as pl

from doppeldata.profile_comparison import significant
from doppeldata.profile_comparison.reports import ExpectationResult
from doppeldata.profiling import sources
from doppeldata.profiling.backend import HIDDEN_COLUMN_PREFIX
from doppeldata.profiling.polars_backend import PolarsBackend
from doppeldata.scenario_format import expected_metrics
from doppeldata.scenario_format import expected_perturbation

ROWS = HIDDEN_COLUMN_PREFIX + 'rows'


def hot_keys_share(table: pl.LazyFrame, column: str, perturbation: dict[str, Any]) -> int:
    """The rows holding the column's most frequent values, as many as its hot_keys perturbation's `keys`."""
    counted = table.group_by(column).agg(pl.len().alias(ROWS)).drop_nulls(column).sort(ROWS, descending=True).head(perturbation['keys'])
    held: int = counted.select(pl.col(ROWS).sum()).collect(engine='streaming').item()
    return held


def duplicates_rate(table: pl.LazyFrame, column: str, perturbation: dict[str, Any]) -> int:
    """The rows repeating an earlier row's value of `column`, the key each copy shares with its source."""
    rows, distinct = table.select(pl.len(), pl.col(column).n_unique()).collect(engine='streaming').row(0)
    return int(rows - distinct)


def late_arrival_rate(table: pl.LazyFrame, column: str, perturbation: dict[str, Any]) -> int:
    """The rows whose `column` is earlier than the ingest column its late_arrival perturbation names."""
    ingest = pl.col(perturbation['ingest']).cast(table.collect_schema()[column])
    late: int = table.select((pl.col(column) < ingest).sum()).collect(engine='streaming').item()
    return late


def missingness_rate(table: pl.LazyFrame, column: str, perturbation: dict[str, Any]) -> int:
    """The rows whose `column` is null, the profile's own nulls included."""
    nulls: int = table.select(pl.col(column).null_count()).collect(engine='streaming').item()
    return nulls


# Each statistic as the rows it counts, which the rows read divide.
MEASURES: dict[str, Callable[[pl.LazyFrame, str, dict[str, Any]], int]] = {
    'hot_keys.share': hot_keys_share,
    'duplicates.rate': duplicates_rate,
    'late_arrival.rate': late_arrival_rate,
    'missingness.rate': missingness_rate,
}
# A metric the schema names and nothing here measures would fail inside `verified`, so it fails on import instead.
if unmatched := sorted(set(expected_metrics()) ^ set(MEASURES)):
    raise ImportError(f'the scenario schema and MEASURES disagree on {", ".join(unmatched)}; measure each metric the schema names')


def verified(document: dict[str, Any], data: Any) -> list[ExpectationResult]:
    """Each expectation `document` declares, measured on `data`, which is anything `doppeldata.profile` reads. One naming a
    column the data lacks fails, with `unmeasured` saying which."""
    table = PolarsBackend().scan(sources.resolve(data))
    held = table.collect_schema().names()
    rows = table.select(pl.len()).collect(engine='streaming').item()
    perturbations = document.get('perturbations') or []
    results = []
    for expectation in document.get('expect') or []:
        metric, column = expectation['metric'], expectation['column']
        at_least, at_most = expectation.get('at_least'), expectation.get('at_most')
        index = expected_perturbation(metric, column, perturbations)
        perturbation = perturbations[index] if index is not None else {}
        missing = [name for name in (column, perturbation.get('ingest')) if name is not None and name not in held]
        if missing:
            unmeasured = f'the data holds no column {missing[0]}'
            results.append(ExpectationResult(metric, column, at_least, at_most, None, False, unmeasured))
            continue
        counted = MEASURES[metric](table, column, perturbation) if rows else 0
        value = significant(counted / rows) if rows else 0.0
        passed = (at_least is None or value >= at_least) and (at_most is None or value <= at_most)
        results.append(ExpectationResult(metric, column, at_least, at_most, value, passed))
    return results
