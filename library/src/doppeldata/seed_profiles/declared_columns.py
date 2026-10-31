"""A column as a schema declares it, expanded through the short form's own columns, with a default for each gap.

A schema states types and constraints and no statistics. Where it is silent, a column draws:

- a number uniformly between its bounds, a missing end `NUMBER_SPAN` beyond the given one, and 0 to `NUMBER_SPAN` with
  neither, kept inside its type;
- an integer key counting up from its least value, bounded or not;
- a date or a timestamp uniformly inside `DATE_WINDOW`, a missing end a window's length beyond the given one;
- a string from its values, else its pattern, else its format's type, else the fake its name hints, else letters and
  digits `TEXT_LENGTH` long. A pattern, a format or a fake is kept only where every value it draws holds the declared
  lengths, its repetitions bounded to them, else a shorter hint of the same meaning from `SHORTER_HINTS`;
- nulls at `NULL_RATE` where the schema allows them, and none where it requires a value.

The window is fixed rather than counted back from today, so one seed generates the same rows on any day.
"""

import dataclasses as dc
import datetime as dt
import functools
import inspect
import math
import re
import warnings
from collections.abc import Mapping
from typing import Any
from typing import TypeVar

from doppeldata import _core
from doppeldata.errors import DoppeldataError
from doppeldata.errors import ProfilingWarning
from doppeldata.errors import SourceError
from doppeldata.inline_columns import InlineColumn
from doppeldata.inline_columns import as_date
from doppeldata.inline_columns import choice
from doppeldata.inline_columns import dates
from doppeldata.inline_columns import decimals
from doppeldata.inline_columns import integers
from doppeldata.name_hinted_types import GENERAL
from doppeldata.profile_format import INTEGER_FORMAT_RANGES
from doppeldata.published_schemas import published_enum
from doppeldata.short_profiles import named_column

NULL_RATE = 0.05
NUMBER_SPAN = 1_000_000
DATE_WINDOW = (dt.date(2025, 1, 1), dt.date(2025, 12, 31))
TEXT_LENGTH = (8, 16)
# The outermost quantiles a list's declared item counts are written at, which the engine draws as its least and most.
LIST_LENGTH_LEVELS = ('p01', 'p99')
INTEGER_RANGES = {
    **INTEGER_FORMAT_RANGES,
    'int8': (-(2**7), 2**7 - 1),
    'int16': (-(2**15), 2**15 - 1),
    'uint8': (0, 2**8 - 1),
    'uint16': (0, 2**16 - 1),
}
DECIMAL_TYPE = re.compile(r'^decimal128\((?P<precision>\d+),\s*(?P<scale>\d+)\)$')
DEFAULT_PHYSICAL_TYPES = {
    'string': 'string',
    'integer': 'int64',
    'number': 'double',
    'boolean': 'bool',
    'date': 'date32[day]',
    'timestamp': 'timestamp[us]',
    'time': 'time64[us]',
}
# The logical type of each Arrow type a seed draws, by the type's name ahead of its parameters, such as `decimal128`.
LOGICAL_TYPES = {
    **{f'{sign}int{bits}': 'integer' for sign in ('', 'u') for bits in (8, 16, 32, 64)},
    'float': 'number',
    'double': 'number',
    'decimal128': 'number',
    'string': 'string',
    'bool': 'boolean',
    'date32': 'date',
    'time64': 'time',
    'timestamp': 'timestamp',
}
ARROW_TYPE_NAME = re.compile(r'\w+')
# A format JSON Schema, ODCS or Table Schema names, and the general pack's type a column in it is given.
FORMAT_TYPES = {
    'email': 'email',
    'idn-email': 'email',
    'uuid': 'uuid',
    'uri': 'url',
    'url': 'url',
    'iri': 'url',
    'ipv4': 'ipv4_address',
    'ipv6': 'ipv6_address',
}
# What a provider's hint gives way to, in order, where its values do not hold a column's declared lengths: a provider of
# the same meaning, or a pattern of its format bounded to the lengths. `.test` is reserved, as `example.com` is, and an
# address there can be as short as 8 characters, where one at `example.com` takes 13.
EMAIL_PATTERNS = ({'pattern': r'[a-z]+@example\.(?:com|net|org)'}, {'pattern': r'[a-z]+@[a-z]+\.test'})
SHORTER_HINTS: dict[str, tuple[dict[str, str], ...]] = {
    'state': ({'provider': 'state_abbr'},),
    'country': ({'provider': 'country_code'},),
    'email': EMAIL_PATTERNS,
    'free_email': EMAIL_PATTERNS,
}

# The facts `generated.declared` names, in the profile model's order: each one a schema can declare and an importer
# otherwise fills with a draw default.
DECLARED_FACTS = tuple(published_enum('DeclaredFact'))

Bound = int | float | str | dt.date
Ordered = TypeVar('Ordered', int, float, dt.date, dt.datetime)


def logical_type_of(arrow_type: str) -> str | None:
    """The logical type of an Arrow type a seed draws, such as `number` for `decimal128(12, 2)`, else None."""
    name = ARROW_TYPE_NAME.match(arrow_type)
    return LOGICAL_TYPES.get(name[0]) if name else None


@functools.cache
def provider_lengths() -> dict[str, tuple[int, int]]:
    """The fewest and most characters each provider's values hold, by the provider's name."""
    return _core.provider_lengths()


def warn_left_out(message: str) -> None:
    """Warn `message` as a ProfilingWarning at the first caller outside the library, however deep the importer runs."""
    level, frame = 2, inspect.currentframe()
    frame = frame.f_back if frame is not None else None
    while frame is not None and frame.f_back is not None and frame.f_globals.get('__name__', '').partition('.')[0] == 'doppeldata':
        level, frame = level + 1, frame.f_back
    warnings.warn(ProfilingWarning(message), stacklevel=level)


@dc.dataclass(frozen=True)
class DeclaredColumn(InlineColumn):
    """One column as a schema declares it. `logical_type` is one of the profile's, and `physical_type` an Arrow type.

    `low` and `high` bound a number, a date or a timestamp, each excluded where its `_exclusive` flag is set. `key`
    marks a column whose values a schema declares distinct, such as a sole primary key. `null_rate` is a declared share
    of nulls, where a schema states one. `semantic` is a whole `semantic` section, as Glue's findings give one.
    """

    name: str
    logical_type: str
    physical_type: str | None = None
    required: bool = False
    low: Bound | None = None
    high: Bound | None = None
    low_exclusive: bool = False
    high_exclusive: bool = False
    places: int | None = None
    min_length: int | None = None
    max_length: int | None = None
    pattern: str | None = None
    values: tuple[Any, ...] = ()
    format: str | None = None
    semantic: Mapping[str, Any] | None = None
    sensitivity: str | None = None
    null_rate: float | None = None
    key: bool = False
    timezone: str | None = None
    fields: tuple['DeclaredColumn', ...] = ()
    element: 'DeclaredColumn | None' = None
    min_items: int | None = None
    max_items: int | None = None
    # The dotted path of a field nested in a struct or a list, which the engine names its streams by.
    path: str | None = None
    # The facts of `DECLARED_FACTS` the importer set from something other than the schema's declaration, such as the
    # date window Table Schema's `year` draws from, or the partition values a catalog lists.
    undeclared: frozenset[str] = frozenset()

    def declared_facts(self) -> list[str]:
        """The facts of `DECLARED_FACTS` the schema declared, which an export of the seed carries."""
        held = {
            'low': self.low is not None,
            'high': self.high is not None,
            'scale': self.places is not None or bool(DECIMAL_TYPE.match(self.physical_type or '')),
            'min_length': self.min_length is not None,
            'max_length': self.max_length is not None,
            'min_items': self.min_items is not None,
            'max_items': self.max_items is not None,
            'format': (self.format or '').lower() in FORMAT_TYPES,
            'pattern': self.pattern is not None,
            'top_values': bool(self.values),
        }
        return [fact for fact in DECLARED_FACTS if held[fact] and fact not in self.undeclared]

    def column(self, name: str) -> dict[str, Any]:
        null_rate = 0.0 if self.required else self.null_rate if self.null_rate is not None else NULL_RATE
        column: dict[str, Any] = {'name': name}
        if self.path is not None:
            column['path'] = self.path
        column |= {'physical_type': self.arrow_type(), 'logical_type': self.logical_type, 'nullable': not self.required}
        column['null_rate'] = null_rate
        try:
            drawn = self.drawn(name)
        except DoppeldataError:
            raise
        except (TypeError, ValueError) as error:
            # Every value drawn reads is one the schema declared, such as a bound written as text.
            raise SourceError(
                f'{name}: the schema declares what no {self.logical_type} column draws from: {error}', role='schema'
            ) from error
        column |= {key: value for key, value in drawn.items() if key not in ('name', 'physical_type', 'logical_type')}
        held = dict(self.semantic or {})
        if self.logical_type == 'string' and not self.values:
            # `drawn_text` keeps the semantic's own hint only where it holds the declared lengths.
            held.pop('generator_hint', None)
        semantic = {**held, **(drawn.get('semantic') or {})}
        if self.sensitivity is not None:
            semantic['sensitivity'] = self.sensitivity
        if semantic:
            column['semantic'] = semantic
        return column

    def arrow_type(self) -> str:
        if self.physical_type is not None:
            return self.physical_type
        if self.logical_type == 'object':
            return 'struct<' + ', '.join(f'{field.name}: {field.arrow_type()}' for field in self.fields) + '>'
        if self.logical_type == 'array':
            return f'list<item: {self.element.arrow_type() if self.element else "string"}>'
        if self.logical_type == 'timestamp' and self.timezone:
            return f'timestamp[us, tz={self.timezone}]'
        return DEFAULT_PHYSICAL_TYPES[self.logical_type]

    def drawn(self, name: str) -> dict[str, Any]:
        """The sections that say what the column draws, from the short form's column of its kind where it has one."""
        if self.logical_type == 'string':
            return self.drawn_text(name)
        if self.logical_type == 'integer':
            return self.drawn_integers(name)
        if self.logical_type == 'number':
            return self.drawn_numbers(name)
        if self.logical_type == 'boolean':
            return choice([True, False]).column(name)
        if self.logical_type == 'date':
            low, high = self.date_bounds()
            return dates(low, high).column(name)
        if self.logical_type == 'timestamp':
            return self.drawn_timestamps()
        if self.logical_type == 'object':
            return {'struct': {'fields': [field.column(field.name) for field in self.fields]}}
        if self.logical_type == 'array':
            return self.drawn_list()
        return {}

    def drawn_text(self, name: str) -> dict[str, Any]:
        if self.values:
            return choice([str(value) for value in self.values]).column(name)
        if self.pattern is not None:
            problem = _core.pattern_problem(self.pattern)
            bounded = None if problem else _core.pattern_within_lengths(self.pattern, self.min_length, self.max_length)
            if bounded == self.pattern:
                return self.with_declared_lengths({'string': {'pattern': self.pattern}})
            if bounded is not None:
                # The declared pattern is what an export holds real data to, so only the hint is bounded.
                return self.with_declared_lengths(
                    {'string': {'pattern': self.pattern}, 'semantic': {'generator_hint': {'pattern': bounded}}}
                )
            problem = problem or f'it cannot be held to {self.lengths_text()}'
            warn_left_out(f'{name}: the pattern {self.pattern!r} cannot be drawn ({problem}), so it is left out')
        hinted = FORMAT_TYPES.get((self.format or '').lower())
        if hinted is not None:
            hint = self.fitted_hint(next(entry.generator_hint for entry in GENERAL if entry.type == hinted))
            if hint is not None:
                return self.with_declared_lengths({'semantic': {'type': hinted, 'generator_hint': hint}})
            warn_left_out(f'{name}: no {self.format} value can be drawn {self.lengths_text()}, so the format is left out')
        if self.semantic:
            own = self.semantic.get('generator_hint')
            hint = self.fitted_hint(own) if own else None
            if hint is not None:
                return self.with_declared_lengths({'semantic': {'generator_hint': hint}})
        else:
            named = named_column(name)
            hint = self.fitted_hint(named['semantic']['generator_hint']) if named is not None else None
            if named is not None and hint is not None:
                return self.with_declared_lengths({'semantic': {**named['semantic'], 'generator_hint': hint}})
        low, high = self.text_lengths()
        return {'string': {'length': {'min': low, 'max': high}}}

    def fitted_hint(self, hint: Mapping[str, Any]) -> dict[str, Any] | None:
        """`hint` where every value it draws holds the declared lengths, its pattern bounded to them, else the first of
        its provider's `SHORTER_HINTS` that does. None where none does."""
        if self.min_length is None and self.max_length is None:
            return dict(hint)
        for candidate in (hint, *SHORTER_HINTS.get(hint.get('provider', ''), ())):
            if 'provider' in candidate:
                shortest, longest = provider_lengths()[candidate['provider']]
                if (self.min_length or 0) <= shortest and (self.max_length is None or longest <= self.max_length):
                    return dict(candidate)
            elif 'pattern' in candidate:
                bounded = _core.pattern_within_lengths(candidate['pattern'], self.min_length, self.max_length)
                if bounded is not None:
                    return {**candidate, 'pattern': bounded}
        return None

    def with_declared_lengths(self, drawn: dict[str, Any]) -> dict[str, Any]:
        """`drawn` holding the declared lengths in `string.length`, which an export carries. A hint or a pattern draws
        without reading them."""
        declared = {end: length for end, length in (('min', self.min_length), ('max', self.max_length)) if length is not None}
        if declared:
            drawn['string'] = {**drawn.get('string', {}), 'length': declared}
        return drawn

    def lengths_text(self) -> str:
        """The declared lengths in words, such as `2 to 5 characters long`."""
        if self.min_length == self.max_length:
            return f'{self.max_length} characters long'
        if self.max_length is None:
            return f'at least {self.min_length} characters long'
        if not self.min_length:
            return f'at most {self.max_length} characters long'
        return f'{self.min_length} to {self.max_length} characters long'

    def text_lengths(self) -> tuple[int, int]:
        """`TEXT_LENGTH`, each end moved inside the declared lengths. Where that leaves one length and the declared
        lengths allow more, it spans `TEXT_LENGTH`'s width from the declared end, and at least one character.
        """
        least = self.min_length or 0
        most = self.max_length if self.max_length is not None else math.inf
        low, high = (min(max(length, least), most) for length in TEXT_LENGTH)
        if low == high and least < most:
            width = TEXT_LENGTH[1] - TEXT_LENGTH[0]
            low, high = (least, min(most, least + width)) if low == least else (max(least, most - width, 1), most)
        return int(low), int(high)

    def drawn_integers(self, name: str) -> dict[str, Any]:
        if self.values:
            return choice([int(value) for value in self.values]).column(name)
        floor, ceiling = INTEGER_RANGES.get(self.arrow_type(), INTEGER_RANGES['int64'])
        # The least and most integers inside the bounds, which may be fractional.
        low = None if self.low is None else math.floor(number(self.low)) + 1 if self.low_exclusive else math.ceil(number(self.low))
        high = None if self.high is None else math.ceil(number(self.high)) - 1 if self.high_exclusive else math.floor(number(self.high))
        if self.key:
            # A distinct key counts up from its least value, so no two rows share one. It passes `high` only once the
            # rows outnumber the values between the bounds, which no distinct draw could avoid.
            return {'numeric': {'integral': True, 'sequence': {'start': max(low if low is not None else 1, floor), 'step': 1}}}
        least, most = spanned(low, high, NUMBER_SPAN, (0, NUMBER_SPAN))
        return integers(min(max(least, floor), ceiling), max(min(most, ceiling), floor)).column(name)

    def drawn_numbers(self, name: str) -> dict[str, Any]:
        if self.values:
            return choice([float(value) for value in self.values]).column(name)
        places = self.places
        largest = math.inf
        decimal = DECIMAL_TYPE.match(self.arrow_type())
        if decimal is not None:
            places = int(decimal['scale'])
            largest = 10.0 ** (int(decimal['precision']) - places) - 10.0**-places
        low = None if self.low is None else nudged(number(self.low), up=True, places=places) if self.low_exclusive else number(self.low)
        high = (
            None if self.high is None else nudged(number(self.high), up=False, places=places) if self.high_exclusive else number(self.high)
        )
        least, most = spanned(low, high, NUMBER_SPAN, (0.0, float(NUMBER_SPAN)))
        least, most = max(least, -largest), min(most, largest)
        return decimals(least, max(least, most), places=places).column(name)

    def date_bounds(self) -> tuple[dt.date, dt.date]:
        low = None if self.low is None else day(self.low) + dt.timedelta(days=self.low_exclusive)
        high = None if self.high is None else day(self.high) - dt.timedelta(days=self.high_exclusive)
        return spanned(low, high, DATE_WINDOW[1] - DATE_WINDOW[0], DATE_WINDOW)

    def drawn_timestamps(self) -> dict[str, Any]:
        second = dt.timedelta(seconds=1)
        low = None if self.low is None else instant(self.low) + second * self.low_exclusive
        high = None if self.high is None else instant(self.high) - second * self.high_exclusive
        window = (dt.datetime.combine(DATE_WINDOW[0], dt.time()), dt.datetime.combine(DATE_WINDOW[1], dt.time(23, 59, 59)))
        least, most = spanned(low, high, window[1] - window[0], window)
        suffix = 'Z' if self.timezone else ''
        extremes = {'low': stamped(least) + suffix, 'high': stamped(most) + suffix, 'granularity': 'second'}
        temporal: dict[str, Any] = {'extremes': extremes}
        if self.timezone:
            temporal = {'timezone': self.timezone, **temporal}
        return {'temporal': temporal}

    def drawn_list(self) -> dict[str, Any]:
        listed: dict[str, Any] = {}
        if self.min_items is not None or self.max_items is not None:
            least = self.min_items or 0
            most = self.max_items if self.max_items is not None else max(least, 4)
            listed['length'] = dict(zip(LIST_LENGTH_LEVELS, (least, most), strict=True))
        if self.element is not None:
            listed['element'] = self.element.column('element')
        return {'list': listed}


def spanned(low: Ordered | None, high: Ordered | None, span: Any, neither: tuple[Ordered, Ordered]) -> tuple[Ordered, Ordered]:
    """`low` and `high`, a missing end `span` beyond the given one, and `neither` where both are missing."""
    if low is not None and high is not None:
        return low, max(low, high)
    if low is not None:
        return low, low + span
    if high is not None:
        return high - span, high
    return neither


def day(value: Bound) -> dt.date:
    """A date bound: a date, the date of a datetime, or ISO text."""
    return as_date(value if isinstance(value, dt.date) else str(value), 'a date bound')


def number(value: Bound) -> float:
    """A numeric bound as a float, refusing a date."""
    if isinstance(value, dt.date):
        raise ValueError(f'{value} bounds a number, so it must be a number')
    return float(value)


def nudged(value: float, *, up: bool, places: int | None) -> float:
    """The nearest value past an excluded bound: one unit of the last decimal place, or the next float without one."""
    if places is not None:
        return value + 10.0**-places if up else value - 10.0**-places
    return math.nextafter(value, math.inf if up else -math.inf)


def instant(value: Bound) -> dt.datetime:
    """A timestamp bound as a naive UTC datetime: a datetime, a date at midnight, or ISO text with a space or a T."""
    if isinstance(value, dt.datetime):
        parsed = value
    elif isinstance(value, dt.date):
        parsed = dt.datetime.combine(value, dt.time())
    else:
        try:
            parsed = dt.datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        except ValueError as error:
            raise ValueError(f'{value!r} is not an ISO timestamp, such as 2025-01-31T12:00:00') from error
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.UTC).replace(tzinfo=None)
    return parsed.replace(microsecond=0)


def stamped(value: dt.datetime) -> str:
    return value.isoformat(timespec='seconds')


def nested(column: DeclaredColumn, path: str) -> DeclaredColumn:
    """`column` and every field and element inside it with the dotted path the engine names its streams by."""
    fields = tuple(nested(field, f'{path}.{field.name}') for field in column.fields)
    element = nested(column.element, f'{path}.element') if column.element is not None else None
    return dc.replace(column, path=path if '.' in path else None, fields=fields, element=element)
