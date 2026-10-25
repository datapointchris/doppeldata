"""The thresholds a check and a diff hold each metric to: each metric's default, overridden for every column or for one."""

import dataclasses as dc
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from doppeldata.errors import InvalidThresholds
from doppeldata.profile_comparison.metrics import METRICS
from doppeldata.profile_comparison.metrics import Document
from doppeldata.profile_comparison.metrics import Thresholded
from doppeldata.profile_comparison.real_data_scores import SCORES
from doppeldata.profile_comparison.real_data_scores import SCORES_BY_NAME
from doppeldata.profile_format import to_plain

# Every name a thresholds file keys, the profile metrics' and the real-data scores' in one keyspace.
THRESHOLDED = {measure.name: measure for measure in (*METRICS, *SCORES)}
if len(THRESHOLDED) != len(METRICS) + len(SCORES):
    raise RuntimeError('a real-data score shares a name with a profile metric, so a thresholds file could not tell them apart')


@dc.dataclass(frozen=True)
class Thresholds:
    """`defaults` replaces a metric's threshold for every column, and `columns` replaces it for one column.

    In YAML: `{defaults: {ks: 0.03}, columns: {paid_amount: {ks: 0.08}}}`.
    """

    defaults: Mapping[str, float] = dc.field(default_factory=dict)
    columns: Mapping[str, Mapping[str, float]] = dc.field(default_factory=dict)

    @classmethod
    def load(cls, path: str | Path) -> 'Thresholds':
        """Read a YAML or JSON thresholds file, raising InvalidThresholds when it is unreadable or names a metric no check measures."""
        path = Path(path)
        try:
            text = path.read_text()
            document = json.loads(text) if path.suffix == '.json' else YAML(typ='safe').load(text)
        except (OSError, json.JSONDecodeError, YAMLError, UnicodeDecodeError) as error:
            raise InvalidThresholds(f'{path} is not a readable thresholds file: {error}') from error
        return cls.of(to_plain(document) if document is not None else {}, str(path))

    @classmethod
    def of(cls, document: Any, source: str = 'the thresholds') -> 'Thresholds':
        """Thresholds from their plain document, raising InvalidThresholds naming every fault in it."""
        if not isinstance(document, dict):
            raise InvalidThresholds(f'{source} must be a mapping holding `defaults` and `columns`')
        faults = [f'`{key}` is not a section; use `defaults` or `columns`' for key in document if key not in ('defaults', 'columns')]
        defaults = document.get('defaults') or {}
        columns = document.get('columns') or {}
        faults += metric_faults(defaults, '/defaults', for_column=False)
        if not isinstance(columns, dict):
            faults.append('`columns` must map each column name to its metrics')
            columns = {}
        for column, metrics in columns.items():
            faults += metric_faults(metrics, f'/columns/{column}', for_column=True)
        if faults:
            raise InvalidThresholds(f'{source} is not a valid thresholds file:\n' + '\n'.join(f'  {fault}' for fault in faults))
        return cls(defaults, columns)

    def threshold(self, measure: Thresholded, column: str | None) -> float:
        by_column = self.columns.get(column, {}) if column is not None else {}
        return by_column.get(measure.name, self.defaults.get(measure.name, measure.threshold))

    def sets(self, name: str, column: str | None) -> bool:
        """Whether these thresholds set `name`'s threshold for `column`, or for every column. A pair is set by `defaults` alone."""
        return name in self.defaults or (column is not None and name in self.columns.get(column, {}))

    def check_scores(self, *, scoring: bool, refusing: str) -> None:
        """Refuse a score's threshold where nothing scores the data against the real data, so it would set nothing.

        `refusing` says why nothing scores, such as `a diff compares two profiles`.
        """
        named = sorted({name for metrics in (self.defaults, *self.columns.values()) for name in metrics if name in SCORES_BY_NAME})
        if named and not scoring:
            raise InvalidThresholds(f'the thresholds set {", ".join(named)}, which score data against the real data, and {refusing}')

    def check_columns(self, source: Document) -> None:
        """Refuse a column the source profile does not hold, which a misspelled name would be. Each metric measures the source's columns."""
        held = {column['name'] for column in source.get('columns') or []}
        unknown = sorted(name for name in self.columns if name not in held)
        if unknown:
            raise InvalidThresholds(f'the thresholds name columns the source profile does not hold: {", ".join(unknown)}')


def metric_faults(metrics: Any, at: str, *, for_column: bool) -> list[str]:
    """Each name in `metrics` no check measures, each value not a number, and, under a column, each score of a pair."""
    if not isinstance(metrics, dict):
        return [f'{at} must map each metric name to its threshold']
    faults = []
    for name, value in metrics.items():
        if name not in THRESHOLDED:
            faults.append(f'{at}/{name}: {name!r} is not a metric; choose from {", ".join(THRESHOLDED)}')
        elif for_column and name in SCORES_BY_NAME and SCORES_BY_NAME[name].pairs:
            faults.append(f'{at}/{name}: {name} scores a pair of columns; set it under `defaults`')
        elif isinstance(value, bool) or not isinstance(value, int | float):
            faults.append(f'{at}/{name}: {value!r} is not a number')
    return faults
