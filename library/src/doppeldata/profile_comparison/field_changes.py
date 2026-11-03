"""The fields two profiles of one table differ in, outside what the metrics measure."""

import dataclasses as dc
import enum
from collections.abc import Iterator
from collections.abc import Mapping
from typing import Any

from doppeldata.profile_comparison.metrics import Column
from doppeldata.profile_comparison.metrics import Document
from doppeldata.profile_format import LISTING_SECTIONS
from doppeldata.profile_format import PointerParts
from doppeldata.profile_format import described_rows
from doppeldata.profile_format import pointer
from doppeldata.profiling.json_paths import ELEMENT
from doppeldata.profiling.json_paths import joined
from doppeldata.profiling.semantic_type_hold import HeldTyping
from doppeldata.profiling.statistic_catalog import ColumnKind
from doppeldata.profiling.table_profile import profiled_kind

# Text formats hold no decimal type, so a decimal column written as CSV or JSON reads back as a float.
TEXT_READ_KINDS = {ColumnKind.DECIMAL.value, ColumnKind.FLOAT.value}
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
    """Columns added, dropped, or holding another kind, Arrow type or semantic type, a list's element and a struct's
    fields as a top-level column is.

    With `writer_changes_pass`, as a check sets it, a change a writer alone causes is listed and passes: an Arrow type,
    because a writer may store a column at another width or unit, a decimal read back from text as a float, and a string
    column whose text a read types as another kind, as CSV's 16-digit card numbers read as integers. A diff fails all
    three, because there the source itself changed.

    A semantic type change passes where the type is declared by no detector pack both profiles ran, because only a run
    of that pack could find it. A column's `semantic.pack` names the pack, and `declaring` does where it is absent. It
    passes too where `listing` names a code list the type is read from, and the two profiles record different contents
    for it, because only a run reading that list could confirm the type.
    """
    ran = packs_run(source), packs_run(observed)
    read = code_lists_read(source), code_lists_read(observed)
    typed = Typed(ran, declaring, read, listing)
    return list(changes_among(top_level_columns(source), top_level_columns(observed), writer_changes_pass, typed))


@dc.dataclass(frozen=True)
class ColumnAt:
    """One column of a profile, top-level or nested: where a JSON Pointer finds it, and its path, as a JSON source's
    columns name it, such as `lines.element.code`."""

    column: Column
    at: PointerParts
    path: str
    presence: float = 1.0

    @property
    def measured(self) -> bool:
        """Whether a profile's statistics reach the column. A nested column of a source other than JSON is stated only
        by its Arrow type, so its kind and semantic type say nothing of the data."""
        return self.path == self.column['name'] or 'logical_type' in self.column

    def nested(self) -> list['ColumnAt']:
        """The list element and each struct field directly inside the column, with no element where no list holds one,
        because its entry then measures nothing."""
        found = []
        listed = self.column.get('list') or {}
        holds_arrays = (self.column.get('null_fraction') or 0.0) < 1.0 and (listed.get('empty_fraction') or 0.0) < 1.0
        if listed.get('element') and holds_arrays:
            found.append(ColumnAt(listed['element'], (*self.at, 'list', 'element'), joined(self.path, ELEMENT)))
        struct = self.column.get('struct') or {}
        presence = struct.get('presence_fractions') or {}
        for index, field in enumerate(struct.get('fields') or []):
            at = (*self.at, 'struct', 'fields', index)
            found.append(ColumnAt(field, at, joined(self.path, field['name']), presence.get(field['name'], 1.0)))
        return found


def top_level_columns(document: Document) -> list[ColumnAt]:
    return [ColumnAt(column, ('columns', index), column['name']) for index, column in enumerate(document.get('columns') or [])]


def changes_among(ours: list[ColumnAt], theirs: list[ColumnAt], writer_changes_pass: bool, typed: 'Typed') -> Iterator[FieldChange]:
    """What differs between two sets of sibling columns, then between the columns nested in each pair both profiles state
    nested columns for. A profile may state a list or struct's type and not what is inside it, which is no change.

    With `writer_changes_pass`, a key only some of the source's objects hold passes when the data holds it nowhere,
    because a write of few rows may draw it for none of them and a JSON writer then leaves the key out.
    """
    source = {each.path: each for each in ours}
    observed = {each.path: each for each in theirs}
    for path, each in source.items():
        if path not in observed:
            message = f'{path} ({each.column.get("physical_type")}) is in the source profile and not in the other'
            drawn = writer_changes_pass and each.presence < 1.0
            if drawn:
                message += f'; {each.presence:.2%} of its objects hold the key, so it is listed and passes'
            yield FieldChange(ChangeKind.COLUMN_DROPPED, path, pointer(each.at), column_kind(each.column), None, drawn, message)
    for path, each in observed.items():
        if path not in source:
            message = f'{path} ({each.column.get("physical_type")}) is not in the source profile'
            yield FieldChange(ChangeKind.COLUMN_ADDED, path, pointer(each.at), None, column_kind(each.column), False, message)
            continue
        yield from retyped(source[path], each, writer_changes_pass, typed)
        inside = source[path].nested(), each.nested()
        if all(inside):
            yield from changes_among(*inside, writer_changes_pass, typed)


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


def retyped(ours: ColumnAt, theirs: ColumnAt, writer_changes_pass: bool, typed: Typed) -> Iterator[FieldChange]:
    source, observed, name = ours.column, theirs.column, theirs.path
    measured = ours.measured and theirs.measured
    kinds = column_kind(source), column_kind(observed)
    if measured and kinds[0] != kinds[1]:
        message = f'{name} is {kinds[0]} in the source profile and {kinds[1]} in the other'
        passed = writer_changes_pass and ({*kinds} == TEXT_READ_KINDS or kinds[0] == ColumnKind.STRING.value)
        yield FieldChange(ChangeKind.KIND_CHANGED, name, pointer((*theirs.at, 'logical_type')), *kinds, passed, message)
    types = source.get('physical_type'), observed.get('physical_type')
    if types[0] != types[1]:
        message = f'{name} is {types[0]} in the source profile and {types[1]} in the other'
        at = pointer((*theirs.at, 'physical_type'))
        yield FieldChange(ChangeKind.PHYSICAL_TYPE_CHANGED, name, at, *types, writer_changes_pass, message)
    blocks = source.get('semantic') or {}, observed.get('semantic') or {}
    semantic = blocks[0].get('type'), blocks[1].get('type')
    if measured and semantic[0] != semantic[1]:
        source_type, observed_type = (f'as {kind}' if kind is not None else 'as no semantic type' for kind in semantic)
        message = f'{name} is detected {source_type} in the source profile and {observed_type} in the other'
        unconfirmed = typed.unconfirmed(blocks, semantic)
        if unconfirmed:
            message += f'; {unconfirmed}, so it is listed and passes'
        at = pointer((*theirs.at, 'semantic', 'type'))
        yield FieldChange(ChangeKind.SEMANTIC_TYPE_CHANGED, name, at, *semantic, unconfirmed is not None, message)


def every_column_at(columns: list[ColumnAt]) -> Iterator[ColumnAt]:
    """Each of `columns`, and each column nested in one."""
    for each in columns:
        yield each
        yield from every_column_at(each.nested())


def held_typing_changes(observed: Document, held: Mapping[str, HeldTyping]) -> list[FieldChange]:
    """A listed, passing type change for each column a check's profile held to the source's typing, where its sample
    alone types it otherwise, because sampling alone could carry its share that far across the cut. A column the
    margin left with neither type differs from the source, and `column_changes` fails it."""
    pointers = {each.path: each.at for each in every_column_at(top_level_columns(observed))}
    return [
        FieldChange(
            ChangeKind.SEMANTIC_TYPE_CHANGED,
            name,
            pointer((*pointers[name], 'semantic', 'type')),
            holding.earlier,
            holding.alone,
            True,
            f'{holding.message(name)}, so it is listed and passes',
        )
        for name, holding in held.items()
        if name in pointers and holding.held == holding.earlier
    ]


def overflow_changes(source: Document, observed: Document) -> list[FieldChange]:
    """Each column with more than `OVERFLOW_SHARE` of its values outside the histogram edges or value list frozen from the source."""
    ours = {column['name']: column for column in source.get('columns') or []}
    rows = described_rows(observed)
    changes = []
    for index, column in enumerate(observed.get('columns') or []):
        name = column['name']
        if name not in ours:
            continue
        histogram = (column.get('numeric') or {}).get('histogram') or {}
        frozen = ((ours[name].get('numeric') or {}).get('histogram') or {}).get('edges')
        if 'overflow' in histogram and frozen and (histogram['edges'][0], histogram['edges'][-1]) == (frozen[0], frozen[-1]):
            outside = histogram['overflow']['rows_below'] + histogram['overflow']['rows_above']
            share = outside / (outside + sum(histogram['bin_rows'])) if outside else 0.0
            at = pointer(('columns', index, 'numeric', 'histogram', 'overflow'))
            if share > OVERFLOW_SHARE:
                message = f'{share:.1%} of {name} values fall outside the histogram edges frozen from the source profile'
                changes.append(FieldChange(ChangeKind.OVERFLOW, name, at, None, round(share, 6), False, message))
        values = rows * (1 - (column.get('null_fraction') or 0.0)) if rows else 0
        for section in LISTING_SECTIONS:
            new_rows = ((column.get(section) or {}).get('overflow') or {}).get('new_value_rows')
            if new_rows and values and new_rows / values > OVERFLOW_SHARE:
                share = new_rows / values
                at = pointer(('columns', index, section, 'overflow', 'new_value_rows'))
                message = (
                    f'{share:.1%} of {name} values are outside the value list frozen from the source profile, '
                    'beyond the share it started with'
                )
                changes.append(FieldChange(ChangeKind.OVERFLOW, name, at, None, round(share, 6), False, message))
    return changes


def source_changes(source: Document, observed: Document) -> list[FieldChange]:
    """A CSV dialect setting a writer takes that changed, and each partition key the other profile adds.

    The rest of `source.csv` describes the files rather than a setting a writer takes, such as the malformed rows, so a
    diff does not compare it.
    """
    changes = []
    sections = [(document.get('source') or {}).get('csv') or {} for document in (source, observed)]
    if all(sections):
        ours, theirs = (section.get('writer') or {} for section in sections)
        for key in sorted({*ours, *theirs}):
            if ours.get(key) != theirs.get(key):
                message = f'the CSV {key} is {ours.get(key)!r} in the source profile and {theirs.get(key)!r} in the other'
                changes.append(
                    FieldChange(
                        ChangeKind.CSV_DIALECT_CHANGED,
                        None,
                        pointer(('source', 'csv', 'writer', key)),
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
