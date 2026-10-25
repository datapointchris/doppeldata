"""Every field of a table profile that holds literals, the rule that admits them, and the rows each of its cells rests on.

A literal is a data value, or a count of rows finer than the column: a top value, a prefix, a shape, a missing-value
token, a pattern, an extreme, a quantile, a histogram's edges and bins, a calendar share, a boolean's true and false
counts, a fit's parameters, a sequence's start. An integer identifier's digit bounds hold no data value, so they are
not one. The rows are recomputed from the file alone: a rate times the rows the profile describes, less the column's
nulls, and for a struct field times its presence.
"""

import dataclasses as dc
import enum
from collections.abc import Iterator
from typing import Any

from doppeldata import _core
from doppeldata.profile_format import PointerParts
from doppeldata.profile_format import pointer
from doppeldata.profile_format import quantile_level
from doppeldata.published_schemas import published_enum
from doppeldata.published_schemas import published_fields

CALENDAR_SHARES = ('by_month', 'by_dow', 'by_hour')
TRIMMED_K = 'trimmed_k'
DIGIT_BOUNDS = 'digit_bounds'
EXACT = 'exact'
ROWS_EXPECTED = 'expected dataset.row_count, or generated.sample.rows_read'


class FieldKind(enum.Enum):
    """A kind of literal-bearing field, with the `AuditRule` that admits it and the words a finding names it by.

    Each value leads with its own key, because two kinds sharing a rule and label would otherwise be one member.
    """

    TOP_VALUES = ('top_values', 'count >= k_min', 'top values')
    PREFIXES = ('prefixes', 'count >= k_min', 'prefixes')
    BOOLEAN = ('boolean', 'cell >= k_min', 'true and false counts')
    SHAPES = ('shapes', 'count >= k_min', 'shapes')
    MISSING_TOKENS = ('missing_tokens', 'count >= k_min', 'missing-value tokens')
    HISTOGRAM = ('histogram', 'cell >= k_min', 'histogram')
    CALENDAR_SHARES = ('calendar_shares', 'cell >= k_min', 'calendar shares')
    NUMERIC_EXTREMES = ('numeric_extremes', 'trimmed_k', 'extremes')
    TEMPORAL_EXTREMES = ('temporal_extremes', 'trimmed_k', 'extremes')
    QUANTILES = ('quantiles', 'k_min rows from each end', 'quantiles')
    PATTERN = ('pattern', 'generalized', 'pattern')
    FIT = ('fit', 'fitted within the trimmed extremes', 'fitted family')
    SEQUENCE = ('sequence', 'follows the trimmed extreme', 'sequence start')

    @property
    def rule(self) -> str:
        return self.value[1]

    @property
    def label(self) -> str:
        return self.value[2]


# Every field of a column and of each statistics section in it, with the kind of literal it holds, or None where it
# holds no data value and no count finer than the column. `literal_fields` reads each field named here with a kind.
FIELD_KINDS: dict[str, dict[str, FieldKind | None]] = {
    'Column': dict.fromkeys(
        (
            'boolean',
            'distinct',
            'list',
            'logical_type',
            'name',
            'null_rate',
            'nullable',
            'numeric',
            'path',
            'physical_type',
            'quality',
            'semantic',
            'string',
            'struct',
            'temporal',
            'type_mix',
        )
    )
    | {'missing_tokens': FieldKind.MISSING_TOKENS},
    'BooleanStats': {'true_rate': FieldKind.BOOLEAN},
    'NumericStats': dict.fromkeys(
        ('integral', 'kurtosis', 'mean', 'monotonic', 'negative_rate', 'precision', 'scale', 'skew', 'std', 'zero_rate')
    )
    | {
        'extremes': FieldKind.NUMERIC_EXTREMES,
        'quantiles': FieldKind.QUANTILES,
        'histogram': FieldKind.HISTOGRAM,
        'fit': FieldKind.FIT,
        'sequence': FieldKind.SEQUENCE,
    },
    # `other_rate` and `overflow` count what a top-value list leaves out, so they are read as part of it.
    'StringStats': dict.fromkeys(('case', 'charset', 'length', 'other_rate', 'overflow', 'whitespace_edge_rate'))
    | {'top_values': FieldKind.TOP_VALUES, 'prefixes': FieldKind.PREFIXES, 'shapes': FieldKind.SHAPES, 'pattern': FieldKind.PATTERN},
    'TemporalStats': dict.fromkeys(('format', 'gap_rate', 'timezone'))
    | {
        'extremes': FieldKind.TEMPORAL_EXTREMES,
        'by_month': FieldKind.CALENDAR_SHARES,
        'by_dow': FieldKind.CALENDAR_SHARES,
        'by_hour': FieldKind.CALENDAR_SHARES,
    },
}


@dc.dataclass(frozen=True)
class Cell:
    """One entry of a field and the rows it rests on, or None where the file states no row count.

    `may_be_empty` marks a count the floor lets hold no rows, such as a histogram bin or a top-value list's remainder.
    """

    path: PointerParts
    rows: float | None
    may_be_empty: bool = False


@dc.dataclass(frozen=True)
class LiteralField:
    """One field holding literals, at `path` inside the table profile."""

    path: PointerParts
    kind: FieldKind
    values: int
    cells: tuple[Cell, ...] = ()

    @property
    def pointer(self) -> str:
        return pointer(self.path)


@dc.dataclass(frozen=True)
class ColumnLiterals:
    """One column or nested field, the rows of it holding a value, and its literal-bearing fields."""

    column: dict[str, Any]
    path: PointerParts
    held: float | None
    fields: list[LiteralField]

    @property
    def name(self) -> str:
        return str(self.column.get('path') or self.column.get('name') or pointer(self.path))

    def within(self, field: LiteralField) -> str:
        """The field's pointer inside the column, which `privacy.audit` names it by beside the column's name."""
        return pointer(field.path[len(self.path) :])


@dc.dataclass(frozen=True)
class Fault:
    """A literal that breaks its rule: where, a short code, and what was found against what was expected."""

    code: str
    path: PointerParts
    detail: str


def described_rows(table: dict[str, Any]) -> float | None:
    """The rows a profile's statistics were computed over: the sample's, or the table's."""
    sample = (table.get('generated') or {}).get('sample') or {}
    rows = sample.get('rows_read') or (table.get('dataset') or {}).get('row_count')
    return float(rows) if rows is not None else None


def column_literals(table: dict[str, Any]) -> Iterator[ColumnLiterals]:
    """Each column of the table, its nested fields after it, with the literal-bearing fields each holds."""
    described = described_rows(table)
    for index, column in enumerate(table.get('columns') or []):
        if isinstance(column, dict):
            yield from nested_literals(column, ('columns', index), described)


def nested_literals(column: dict[str, Any], at: PointerParts, rows: float | None) -> Iterator[ColumnLiterals]:
    """The column and every field nested in it. `rows` are those holding the column's key.

    A struct field's are its parent's objects times the field's presence. An array element's are not stated by the
    file, which gives an array's length only by quantile, and an element's count is of elements rather than rows.
    """
    held = rows * (1 - (column.get('null_rate') or 0.0)) if rows is not None else None
    yield ColumnLiterals(column, at, held, list(literal_fields(column, at, rows, held)))
    element = (column.get('list') or {}).get('element')
    if isinstance(element, dict):
        yield from nested_literals(element, (*at, 'list', 'element'), None)
    struct = column.get('struct') or {}
    presence = struct.get('presence') or {}
    for index, field in enumerate(struct.get('fields') or []):
        if isinstance(field, dict):
            share = presence.get(field.get('name'))
            keyed = held * share if held is not None and isinstance(share, int | float) else None
            yield from nested_literals(field, (*at, 'struct', 'fields', index), keyed)


def literal_fields(column: dict[str, Any], at: PointerParts, rows: float | None, held: float | None) -> Iterator[LiteralField]:
    """The column's literal-bearing fields. `rows` are the rows the profile describes, and `held` those holding a value."""

    def share(rate: Any, of: float | None) -> float | None:
        return rate * of if of is not None and isinstance(rate, int | float) else None

    tokens = column.get('missing_tokens') or {}
    if tokens:
        cells = tuple(Cell((*at, 'missing_tokens', token), share(rate, rows)) for token, rate in tokens.items())
        yield LiteralField((*at, 'missing_tokens'), FieldKind.MISSING_TOKENS, len(cells), cells)
    true_rate = (column.get('boolean') or {}).get('true_rate')
    if isinstance(true_rate, int | float):
        at_rate = (*at, 'boolean', 'true_rate')
        cells = (Cell(at_rate, share(true_rate, held), True), Cell(at_rate, share(1 - true_rate, held), True))
        yield LiteralField((*at, 'boolean'), FieldKind.BOOLEAN, 2, cells)
    numeric = column.get('numeric') or {}
    extremes = numeric.get('extremes') or {}
    stored = [end for end in ('low', 'high') if extremes.get(end) is not None]
    if stored and extremes.get('method') != DIGIT_BOUNDS:
        yield LiteralField((*at, 'numeric', 'extremes'), FieldKind.NUMERIC_EXTREMES, len(stored))
    if numeric.get('quantiles'):
        yield quantile_field((*at, 'numeric', 'quantiles'), numeric['quantiles'], held)
    histogram = numeric.get('histogram')
    if histogram:
        cells = tuple(Cell((*at, 'numeric', 'histogram', 'counts', index), count, True) for index, count in enumerate(histogram['counts']))
        overflow = histogram.get('overflow') or {}
        cells += tuple(
            Cell((*at, 'numeric', 'histogram', 'overflow', end), overflow[end], True) for end in ('below', 'above') if end in overflow
        )
        yield LiteralField((*at, 'numeric', 'histogram'), FieldKind.HISTOGRAM, len(histogram['counts']), cells)
    if numeric.get('fit'):
        yield LiteralField((*at, 'numeric', 'fit'), FieldKind.FIT, len(numeric['fit'].get('params') or {}))
    if numeric.get('sequence'):
        yield LiteralField((*at, 'numeric', 'sequence'), FieldKind.SEQUENCE, 1)
    string = column.get('string') or {}
    if string.get('top_values'):
        cells = tuple(
            Cell((*at, 'string', 'top_values', index), share(entry.get('rate'), held)) for index, entry in enumerate(string['top_values'])
        )
        if 'other_rate' in string:
            cells += (Cell((*at, 'string', 'other_rate'), share(string['other_rate'], held), True),)
        yield LiteralField((*at, 'string', 'top_values'), FieldKind.TOP_VALUES, len(string['top_values']), cells)
    prefixes = string.get('prefixes') or {}
    if prefixes.get('values'):
        listed = prefixes['values']
        cells = tuple(
            Cell((*at, 'string', 'prefixes', 'values', index), share(entry.get('rate'), held)) for index, entry in enumerate(listed)
        )
        if 'other_rate' in prefixes:
            cells += (Cell((*at, 'string', 'prefixes', 'other_rate'), share(prefixes['other_rate'], held), True),)
        yield LiteralField((*at, 'string', 'prefixes'), FieldKind.PREFIXES, len(listed), cells)
    if string.get('shapes'):
        cells = tuple(
            Cell((*at, 'string', 'shapes', index), share(entry.get('rate'), held)) for index, entry in enumerate(string['shapes'])
        )
        yield LiteralField((*at, 'string', 'shapes'), FieldKind.SHAPES, len(cells), cells)
    if string.get('pattern'):
        yield LiteralField((*at, 'string', 'pattern'), FieldKind.PATTERN, 1)
    temporal = column.get('temporal') or {}
    temporal_extremes = temporal.get('extremes') or {}
    stored = [end for end in ('low', 'high') if temporal_extremes.get(end) is not None]
    if stored:
        yield LiteralField((*at, 'temporal', 'extremes'), FieldKind.TEMPORAL_EXTREMES, len(stored))
    for name in CALENDAR_SHARES:
        if temporal.get(name):
            cells = tuple(Cell((*at, 'temporal', name, index), share(rate, held), True) for index, rate in enumerate(temporal[name]))
            yield LiteralField((*at, 'temporal', name), FieldKind.CALENDAR_SHARES, sum(1 for rate in temporal[name] if rate), cells)


def quantile_field(at: PointerParts, quantiles: dict[str, Any], held: float | None) -> LiteralField:
    """The quantiles, each as the rows between it and the nearer end, counting its own."""
    cells = []
    for name in quantiles:
        rows = None
        if held is not None:
            position = quantile_level(name) * (held - 1)
            rows = min(position, held - 1 - position) + 1
        cells.append(Cell((*at, name), rows))
    return LiteralField(at, FieldKind.QUANTILES, len(cells), tuple(cells))


def faults(field: LiteralField, column: ColumnLiterals, k_min: int) -> list[Fault]:
    """How the field breaks the rule its kind is admitted by, recomputed from the file. Empty when it meets it."""
    kind = field.kind
    if field.cells:
        if any(cell.rows is None for cell in field.cells):
            return [
                Fault(
                    'rows-not-stated', field.path, f'the {kind.label} of {column.name} hold rates of no stated row count; {ROWS_EXPECTED}'
                )
            ]
        found = []
        for cell in field.cells:
            rows = round(cell.rows or 0)
            if (rows == 0 and cell.may_be_empty) or rows >= k_min:
                continue
            if kind is FieldKind.QUANTILES:
                found.append(Fault('below-k-min', cell.path, f'sits {rows} rows from an end; expected at least {k_min}'))
            else:
                floor = f'0 or at least {k_min}' if cell.may_be_empty else f'at least {k_min}'
                found.append(Fault('below-k-min', cell.path, f'holds {rows} rows; expected {floor}'))
        return found
    if kind in (FieldKind.NUMERIC_EXTREMES, FieldKind.TEMPORAL_EXTREMES):
        found = []
        method = ((column.column.get('numeric') or {}).get('extremes') or {}).get('method')
        if kind is FieldKind.NUMERIC_EXTREMES and method != TRIMMED_K:
            found.append(Fault('extremes-not-trimmed', (*field.path, 'method'), f'method is {method or "absent"}; expected trimmed_k'))
        if column.held is None:
            found.append(
                Fault('rows-not-stated', field.path, f'the extremes of {column.name} rest on no stated row count; {ROWS_EXPECTED}')
            )
        elif round(column.held) < 2 * k_min - 1:
            found.append(Fault('below-k-min', field.path, f'rest on {round(column.held)} values; expected at least {2 * k_min - 1}'))
        return found
    numeric = column.column.get('numeric') or {}
    trimmed = (numeric.get('extremes') or {}).get('method') == TRIMMED_K
    if kind is FieldKind.PATTERN:
        pattern = (column.column.get('string') or {}).get('pattern')
        if not isinstance(pattern, str) or not _core.pattern_is_generalized(pattern):
            expected = 'expected one with no alternation, optional part, or class narrower than whole digit and letter ranges'
            return [Fault('pattern-not-generalized', field.path, f'pattern is {pattern}; {expected}')]
        return []
    if kind in (FieldKind.FIT, FieldKind.SEQUENCE) and not trimmed:
        method = (numeric.get('extremes') or {}).get('method') or 'absent'
        code = 'fit-without-trimmed-extremes' if kind is FieldKind.FIT else 'sequence-without-trimmed-extremes'
        return [Fault(code, field.path, f'{column.name} stores its {kind.label}, and its extremes method is {method}; expected trimmed_k')]
    if kind is FieldKind.SEQUENCE:
        sequence, extremes = numeric['sequence'], numeric['extremes']
        end = extremes.get('low') if sequence['step'] > 0 else extremes.get('high')
        derived = end - (k_min - 1) * sequence['step'] if end is not None else None
        if sequence['start'] != derived:
            return [
                Fault(
                    'sequence-start-not-derived',
                    (*field.path, 'start'),
                    f'start is {sequence["start"]}; expected {derived}, the trimmed extreme less {k_min - 1} steps',
                )
            ]
    return []


def min_count(field: LiteralField, k_min: int) -> int | None:
    """The fewest rows any literal or cell of the field rests on, as `privacy.audit` records it."""
    if field.kind in (FieldKind.NUMERIC_EXTREMES, FieldKind.TEMPORAL_EXTREMES):
        return k_min
    counts = [round(cell.rows) for cell in field.cells if cell.rows is not None and round(cell.rows) > 0]
    return min(counts) if counts else None


# A field the profile format gains is unclassified until it is named above, so the floor cannot pass it unread.
for definition, classified in FIELD_KINDS.items():
    published = set(published_fields(definition))
    if published != set(classified):
        unread, gone = sorted(published - set(classified)), sorted(set(classified) - published)
        raise RuntimeError(f'privacy_policy has not classified the {definition} fields {unread}, and classifies {gone}, which it lacks')
for definition, spelled in (('AuditRule', {kind.rule for kind in FieldKind}), ('ExtremesMethod', {TRIMMED_K, DIGIT_BOUNDS, EXACT})):
    if spelled != set(published_enum(definition)):
        raise RuntimeError(f'privacy_policy spells {definition} as {sorted(spelled)}; the profile format has {published_enum(definition)}')
