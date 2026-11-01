"""The thresholds a check and a diff hold each metric to: each metric's default, overridden for every column or for one."""

import dataclasses as dc
import difflib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from doppeldata import issue_codes
from doppeldata.errors import InvalidThresholds
from doppeldata.issue_codes import Issue
from doppeldata.profile_comparison.metrics import METRICS
from doppeldata.profile_comparison.metrics import Document
from doppeldata.profile_comparison.metrics import Thresholded
from doppeldata.profile_comparison.real_data_scores import SCORES
from doppeldata.profile_comparison.real_data_scores import SCORES_BY_NAME
from doppeldata.profile_comparison.relationship_metrics import RELATIONSHIP_METRICS
from doppeldata.profile_comparison.relationship_metrics import RELATIONSHIP_METRICS_BY_NAME
from doppeldata.profile_format import to_plain

# Every name a thresholds file keys, the profile metrics', the real-data scores' and the relationship metrics' in one keyspace.
THRESHOLDED = {measure.name: measure for measure in (*METRICS, *SCORES, *RELATIONSHIP_METRICS)}
if len(THRESHOLDED) != len(METRICS) + len(SCORES) + len(RELATIONSHIP_METRICS):
    raise RuntimeError('two metrics or scores share a name, so a thresholds file could not tell them apart')
SECTIONS = ('defaults', 'columns')


@dc.dataclass(frozen=True)
class Thresholds:
    """`defaults` replaces a metric's threshold for every column, and `columns` replaces it for one column. A metric
    or score of a pair of columns, and a bundle's relationship metric, is set under `defaults` alone. In a bundle check,
    `columns` names a column of any table, and holds wherever a table has it.

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
            message = f'{path} is not a readable thresholds file: {error}'
            raise InvalidThresholds([Issue(issue_codes.THRESHOLDS_UNREADABLE, None, message, str(path))]) from error
        return cls.of(to_plain(document) if document is not None else {}, str(path))

    @classmethod
    def of(cls, document: Any, source: str = 'the thresholds') -> 'Thresholds':
        """Thresholds from their plain document, raising InvalidThresholds naming every fault in it."""
        if not isinstance(document, dict):
            message = f'{source} must be a mapping holding `defaults` and `columns`'
            raise InvalidThresholds([Issue(issue_codes.INVALID_THRESHOLDS, '', message, valid=SECTIONS)])
        faults = [
            Issue(issue_codes.INVALID_THRESHOLDS, f'/{key}', f'`{key}` is not a section; use `defaults` or `columns`', key, SECTIONS)
            for key in document
            if key not in SECTIONS
        ]
        defaults = document.get('defaults') or {}
        columns = document.get('columns') or {}
        faults += metric_faults(defaults, '/defaults', for_column=False)
        if not isinstance(columns, dict):
            faults.append(Issue(issue_codes.INVALID_THRESHOLDS, '/columns', '`columns` must map each column name to its metrics'))
            columns = {}
        for column, metrics in columns.items():
            faults += metric_faults(metrics, f'/columns/{column}', for_column=True)
        if faults:
            raise InvalidThresholds(faults, headline=f'{source} is not a valid thresholds file')
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
            message = f'the thresholds set {", ".join(named)}, which score data against the real data, and {refusing}'
            raise InvalidThresholds([Issue(issue_codes.THRESHOLDS_NEED_AGAINST, None, message, named)])

    def check_columns(self, source: Document | list[Document]) -> None:
        """Refuse a column the source profile does not hold, or none of a bundle's tables do, which a misspelled name would
        be. Each metric measures the source's columns."""
        sources = source if isinstance(source, list) else [source]
        held = {column['name'] for document in sources for column in document.get('columns') or []}
        unknown = sorted(name for name in self.columns if name not in held)
        if unknown:
            known = tuple(sorted(held))
            issues = [
                Issue(
                    issue_codes.THRESHOLDS_UNKNOWN_COLUMN,
                    f'/columns/{name}',
                    f'{name} is no column the source profile holds',
                    name,
                    known,
                    tuple(difflib.get_close_matches(name, known, n=3)),
                )
                for name in unknown
            ]
            raise InvalidThresholds(issues, headline=f'the thresholds name columns the source profile does not hold: {", ".join(unknown)}')

    def of_table(self, source: Document) -> 'Thresholds':
        """These thresholds with `columns` cut to the columns one of a bundle's tables holds."""
        held = {column['name'] for column in source.get('columns') or []}
        return Thresholds(self.defaults, {name: metrics for name, metrics in self.columns.items() if name in held})


def metric_faults(metrics: Any, at: str, *, for_column: bool) -> list[Issue]:
    """Each name in `metrics` no check measures, each value not a number, and, under a column, each metric or score of
    a pair."""
    code = issue_codes.INVALID_THRESHOLDS
    if not isinstance(metrics, dict):
        return [Issue(code, at, f'{at} must map each metric name to its threshold')]
    faults = []
    names = tuple(THRESHOLDED)
    for name, value in metrics.items():
        if name not in THRESHOLDED:
            closest = tuple(difflib.get_close_matches(str(name), names, n=3))
            faults.append(Issue(code, f'{at}/{name}', f'{name!r} is not a metric', name, names, closest))
        elif for_column and THRESHOLDED[name].pairs:
            faults.append(Issue(code, f'{at}/{name}', f'{name} measures a pair of columns; set it under `defaults`', name))
        elif for_column and name in RELATIONSHIP_METRICS_BY_NAME:
            faults.append(Issue(code, f'{at}/{name}', f'{name} measures a relationship between tables; set it under `defaults`', name))
        elif isinstance(value, bool) or not isinstance(value, int | float):
            faults.append(Issue(code, f'{at}/{name}', f'{value!r} is not a number', value))
    return faults
