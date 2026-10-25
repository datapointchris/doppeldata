"""The fields two profiles of one table differ in, outside what the metrics measure."""

import dataclasses as dc
import enum
from collections.abc import Iterator
from collections.abc import Mapping
from typing import Any

from doppeldata.profile_comparison.metrics import Column
from doppeldata.profile_comparison.metrics import Document
from doppeldata.profile_format import pointer
from doppeldata.profiling.statistic_catalog import ColumnKind
from doppeldata.profiling.table_profile import profiled_kind
from doppeldata.published_schemas import writer_fields

# Text formats hold no decimal type, so a decimal column written as CSV or JSON reads back as a float.
TEXT_READ_KINDS = {ColumnKind.DECIMAL.value, ColumnKind.FLOAT.value}
# A diff reports a column when more than this share of its values falls outside the edges or value list frozen from A.
OVERFLOW_SHARE = 0.05
# The `source.csv` keys a writer takes. The rest describe the files rather than a setting a writer takes, such as the
# malformed rows, so a diff does not compare them.
DIALECT_KEYS = frozenset(writer_fields('DelimitedSettings'))


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
    expected: Any
    observed: Any
    passed: bool
    message: str

    def to_plain(self) -> dict[str, Any]:
        return dc.asdict(self) | {'change': self.change.value}


def column_kind(column: Column) -> str:
    return profiled_kind(column).value


def packs_run(document: Document) -> frozenset[str]:
    """The detector packs a profile records its run enabled, none for a profile no run wrote."""
    return frozenset((document.get('generated') or {}).get('detector_packs') or {})


def column_changes(
    source: Document, observed: Document, *, writer_changes_pass: bool, declaring: Mapping[str, str], listing: Mapping[str, str]
) -> list[FieldChange]:
    """Columns added, dropped, or holding another kind, Arrow type or semantic type.

    With `writer_changes_pass`, as a check sets it, a change a writer alone causes is listed and passes: an Arrow type,
    because a writer may store a column at another width or unit, and a decimal read back from text as a float. A diff
    fails both, because there the source itself changed.

    A semantic type change passes where the type is declared by no detector pack both profiles ran, because only a run
    of that pack could find it. A column's `semantic.pack` names the pack, and `declaring` does where it is absent. It
    passes too where `listing` names a code list the type is read from, and the two profiles record different contents
    for it, because only a run reading that list could confirm the type.
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
        ran = packs_run(source), packs_run(observed)
        read = code_lists_read(source), code_lists_read(observed)
        typed = Typed(ran, declaring, read, listing)
        changes += list(retyped(source['columns'][ours[name]], column, index, writer_changes_pass, typed))
    return changes


@dc.dataclass(frozen=True)
class Typed:
    """What two profiles were typed with: each one's packs and code lists, the pack declaring each type, and the code
    list each type is read from.
    """

    ran: tuple[frozenset[str], frozenset[str]]
    declaring: Mapping[str, str]
    read: tuple[Mapping[str, str], Mapping[str, str]]
    listing: Mapping[str, str]

    def unconfirmed(self, blocks: tuple[dict[str, Any], dict[str, Any]], semantic: tuple[str | None, str | None]) -> str | None:
        """Why neither side's run could confirm the other's type, or None where both could."""
        packs = [block.get('pack') or self.declaring.get(kind or '') for block, kind in zip(blocks, semantic, strict=True)]
        # Each side's type is confirmed only by the other side's run of the pack that declares it.
        undeclared = [kind for kind, pack, other in zip(semantic, packs, reversed(self.ran), strict=True) if kind and pack not in other]
        if undeclared:
            return f'no detector pack both profiles ran declares {", ".join(undeclared)}'
        lists = sorted({self.listing[kind] for kind in semantic if kind is not None and kind in self.listing})
        unread = [name for name in lists if self.read[0].get(name) != self.read[1].get(name)]
        if unread:
            return f'the two profiles were not typed with the same {", ".join(unread)} code list'
        return None


def code_lists_read(document: Document) -> Mapping[str, str]:
    """Each code list a profile records its run read, by name, with the sha256 of its content."""
    lists: Mapping[str, str] = (document.get('generated') or {}).get('code_lists') or {}
    return lists


def retyped(source: Column, observed: Column, index: int, writer_changes_pass: bool, typed: Typed) -> Iterator[FieldChange]:
    name = observed['name']
    kinds = column_kind(source), column_kind(observed)
    if kinds[0] != kinds[1]:
        message = f'{name} is {kinds[0]} in the source profile and {kinds[1]} in the other'
        passed = writer_changes_pass and {*kinds} == TEXT_READ_KINDS
        yield FieldChange(ChangeKind.KIND_CHANGED, name, pointer(('columns', index, 'logical_type')), *kinds, passed, message)
    types = source.get('physical_type'), observed.get('physical_type')
    if types[0] != types[1]:
        message = f'{name} is {types[0]} in the source profile and {types[1]} in the other'
        at = pointer(('columns', index, 'physical_type'))
        yield FieldChange(ChangeKind.PHYSICAL_TYPE_CHANGED, name, at, *types, writer_changes_pass, message)
    blocks = source.get('semantic') or {}, observed.get('semantic') or {}
    semantic = blocks[0].get('type'), blocks[1].get('type')
    if semantic[0] != semantic[1]:
        source_type, observed_type = (f'as {kind}' if kind is not None else 'as no semantic type' for kind in semantic)
        message = f'{name} is detected {source_type} in the source profile and {observed_type} in the other'
        unconfirmed = typed.unconfirmed(blocks, semantic)
        if unconfirmed:
            message += f'; {unconfirmed}, so it is listed and passes'
        at = pointer(('columns', index, 'semantic', 'type'))
        yield FieldChange(ChangeKind.SEMANTIC_TYPE_CHANGED, name, at, *semantic, unconfirmed is not None, message)


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
    """A CSV dialect setting a writer takes that changed, and each partition key the other profile adds."""
    changes = []
    dialects = (((document.get('source') or {}).get('csv')) for document in (source, observed))
    ours, theirs = (dialect or {} for dialect in dialects)
    if ours and theirs:
        for key in sorted({*ours, *theirs} & DIALECT_KEYS):
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
