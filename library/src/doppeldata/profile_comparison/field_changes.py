"""The fields two profiles of one table differ in, outside what the metrics measure."""

import dataclasses as dc
import enum
from collections.abc import Iterator
from typing import Any

from doppeldata.profile_comparison.metrics import Column
from doppeldata.profile_comparison.metrics import Document
from doppeldata.profile_format import pointer
from doppeldata.profiling.statistic_catalog import ColumnKind
from doppeldata.profiling.table_profile import LOGICAL_TYPES

# A diff reports a column when more than this share of its values falls outside the edges or value list frozen from A.
OVERFLOW_SHARE = 0.05


class ChangeKind(enum.Enum):
    COLUMN_ADDED = 'column_added'
    COLUMN_DROPPED = 'column_dropped'
    KIND_CHANGED = 'kind_changed'
    PHYSICAL_TYPE_CHANGED = 'physical_type_changed'
    SEMANTIC_TYPE_CHANGED = 'semantic_type_changed'
    OVERFLOW = 'overflow'
    CSV_DIALECT_CHANGED = 'csv_dialect_changed'
    PARTITION_KEY_ADDED = 'partition_key_added'


@dc.dataclass(frozen=True)
class FieldChange:
    """One field the two profiles differ in. `field` is a JSON Pointer into the source profile for a dropped column, else into the other."""

    change: ChangeKind
    column: str | None
    field: str
    source: Any
    observed: Any
    passed: bool
    message: str

    def to_plain(self) -> dict[str, Any]:
        return dc.asdict(self) | {'change': self.change.value}


def column_kind(column: Column) -> str:
    """The profiler's `ColumnKind` the column was profiled as, read back from the logical type it wrote.

    Float and decimal share the logical type `number`, so an Arrow decimal type tells them apart.
    """
    kinds = [kind for kind, logical in LOGICAL_TYPES.items() if logical == column.get('logical_type')]
    if len(kinds) > 1:
        decimal = (column.get('physical_type') or '').startswith('decimal')
        kinds = [kind for kind in kinds if (kind is ColumnKind.DECIMAL) == decimal]
    return kinds[0].value if kinds else ColumnKind.OTHER.value


def column_changes(source: Document, observed: Document, *, physical_type_drifts: bool) -> list[FieldChange]:
    """Columns added, dropped, or holding another kind, Arrow type or semantic type.

    A check passes an Arrow type change alone, because a writer may store a column at another width or unit.
    A diff fails it, because there the source itself changed.
    """
    ours = {column['name']: index for index, column in enumerate(source.get('columns') or [])}
    theirs = {column['name']: index for index, column in enumerate(observed.get('columns') or [])}
    changes = []
    for name, index in ours.items():
        if name not in theirs:
            column = source['columns'][index]
            message = f'{name} ({column.get("physical_type")}) is in the source profile and not in the other'
            changes.append(
                FieldChange(ChangeKind.COLUMN_DROPPED, name, pointer(('columns', index)), column_kind(column), None, False, message)
            )
    for name, index in theirs.items():
        column = observed['columns'][index]
        if name not in ours:
            message = f'{name} ({column.get("physical_type")}) is not in the source profile'
            changes.append(
                FieldChange(ChangeKind.COLUMN_ADDED, name, pointer(('columns', index)), None, column_kind(column), False, message)
            )
            continue
        changes += list(retyped(source['columns'][ours[name]], column, index, physical_type_drifts=physical_type_drifts))
    return changes


def retyped(source: Column, observed: Column, index: int, *, physical_type_drifts: bool) -> Iterator[FieldChange]:
    name = observed['name']
    kinds = column_kind(source), column_kind(observed)
    if kinds[0] != kinds[1]:
        message = f'{name} is {kinds[0]} in the source profile and {kinds[1]} in the other'
        yield FieldChange(ChangeKind.KIND_CHANGED, name, pointer(('columns', index, 'logical_type')), *kinds, False, message)
    types = source.get('physical_type'), observed.get('physical_type')
    if types[0] != types[1]:
        message = f'{name} is {types[0]} in the source profile and {types[1]} in the other'
        at = pointer(('columns', index, 'physical_type'))
        yield FieldChange(ChangeKind.PHYSICAL_TYPE_CHANGED, name, at, *types, not physical_type_drifts, message)
    semantic = (source.get('semantic') or {}).get('type'), (observed.get('semantic') or {}).get('type')
    if semantic[0] != semantic[1]:
        source_type, observed_type = (f'as {kind}' if kind is not None else 'as no semantic type' for kind in semantic)
        message = f'{name} is detected {source_type} in the source profile and {observed_type} in the other'
        at = pointer(('columns', index, 'semantic', 'type'))
        yield FieldChange(ChangeKind.SEMANTIC_TYPE_CHANGED, name, at, *semantic, False, message)


def overflow_changes(source: Document, observed: Document) -> list[FieldChange]:
    """Each column with more than `OVERFLOW_SHARE` of its values outside the histogram edges or value list frozen from the source."""
    ours = {column['name']: column for column in source.get('columns') or []}
    rows = (observed.get('dataset') or {}).get('row_count')
    changes = []
    for index, column in enumerate(observed.get('columns') or []):
        name = column['name']
        if name not in ours:
            continue
        histogram = (column.get('numeric') or {}).get('histogram') or {}
        frozen = ((ours[name].get('numeric') or {}).get('histogram') or {}).get('edges')
        if 'overflow' in histogram and frozen and (histogram['edges'][0], histogram['edges'][-1]) == (frozen[0], frozen[-1]):
            outside = histogram['overflow']['below'] + histogram['overflow']['above']
            share = outside / (outside + sum(histogram['counts'])) if outside else 0.0
            at = pointer(('columns', index, 'numeric', 'histogram', 'overflow'))
            if share > OVERFLOW_SHARE:
                message = f'{share:.1%} of {name} values fall outside the histogram edges frozen from the source profile'
                changes.append(FieldChange(ChangeKind.OVERFLOW, name, at, None, round(share, 6), False, message))
        new_rows = ((column.get('string') or {}).get('overflow') or {}).get('new_value_rows')
        values = rows * (1 - (column.get('null_rate') or 0.0)) if rows else 0
        if new_rows and values and new_rows / values > OVERFLOW_SHARE:
            share = new_rows / values
            at = pointer(('columns', index, 'string', 'overflow', 'new_value_rows'))
            message = (
                f'{share:.1%} of {name} values are outside the value list frozen from the source profile, beyond the share it started with'
            )
            changes.append(FieldChange(ChangeKind.OVERFLOW, name, at, None, round(share, 6), False, message))
    return changes


def source_changes(source: Document, observed: Document) -> list[FieldChange]:
    """A CSV dialect setting that changed, and each partition key the other profile adds."""
    changes = []
    dialects = (((document.get('source') or {}).get('csv')) for document in (source, observed))
    ours, theirs = (dialect or {} for dialect in dialects)
    if ours and theirs:
        for key in sorted({*ours, *theirs}):
            if ours.get(key) != theirs.get(key):
                message = f'the CSV {key} is {ours.get(key)!r} in the source profile and {theirs.get(key)!r} in the other'
                changes.append(
                    FieldChange(
                        ChangeKind.CSV_DIALECT_CHANGED,
                        None,
                        pointer(('source', 'csv', key)),
                        ours.get(key),
                        theirs.get(key),
                        False,
                        message,
                    )
                )
    keys = [partition_keys(document) for document in (source, observed)]
    for index, key in enumerate(keys[1]):
        if key not in keys[0]:
            at = pointer(('source', 'layout', 'partitioning', 'keys', index))
            message = f"{key} partitions the other profile's files and not the source's"
            changes.append(FieldChange(ChangeKind.PARTITION_KEY_ADDED, key, at, None, key, False, message))
    return changes


def partition_keys(document: Document) -> list[str]:
    partitioning = ((document.get('source') or {}).get('layout') or {}).get('partitioning') or {}
    return [key['name'] for key in partitioning.get('keys') or []]
