"""The thresholds a check and a diff hold each metric to: each metric's default, overridden for every column or for one."""

import dataclasses as dc
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from doppeldata.errors import InvalidThresholds
from doppeldata.profile_comparison.metrics import METRICS_BY_NAME
from doppeldata.profile_comparison.metrics import Document
from doppeldata.profile_comparison.metrics import Metric
from doppeldata.profile_format import to_plain


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
        faults += metric_faults(defaults, '/defaults')
        if not isinstance(columns, dict):
            faults.append('`columns` must map each column name to its metrics')
            columns = {}
        for column, metrics in columns.items():
            faults += metric_faults(metrics, f'/columns/{column}')
        if faults:
            raise InvalidThresholds(f'{source} is not a valid thresholds file:\n' + '\n'.join(f'  {fault}' for fault in faults))
        return cls(defaults, columns)

    def threshold(self, metric: Metric, column: str | None) -> float:
        by_column = self.columns.get(column, {}) if column is not None else {}
        return by_column.get(metric.name, self.defaults.get(metric.name, metric.threshold))

    def check_columns(self, source: Document) -> None:
        """Refuse a column the source profile does not hold, which a misspelled name would be. Each metric measures the source's columns."""
        held = {column['name'] for column in source.get('columns') or []}
        unknown = sorted(name for name in self.columns if name not in held)
        if unknown:
            raise InvalidThresholds(f'the thresholds name columns the source profile does not hold: {", ".join(unknown)}')


def metric_faults(metrics: Any, at: str) -> list[str]:
    if not isinstance(metrics, dict):
        return [f'{at} must map each metric name to its threshold']
    faults = []
    for name, value in metrics.items():
        if name not in METRICS_BY_NAME:
            faults.append(f'{at}/{name}: {name!r} is not a metric; choose from {", ".join(METRICS_BY_NAME)}')
        elif isinstance(value, bool) or not isinstance(value, int | float):
            faults.append(f'{at}/{name}: {value!r} is not a number')
    return faults
