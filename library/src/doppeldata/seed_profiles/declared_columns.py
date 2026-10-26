"""A column as a schema declares it, expanded through the short form's own columns, with a default for each gap.

A schema states types and constraints and no statistics. Where it is silent, a column draws:

- a number uniformly between its bounds, a missing end `NUMBER_SPAN` beyond the given one, and 0 to `NUMBER_SPAN` with
  neither, kept inside its type;
- an integer key counting up from its least value, bounded or not;
- a date or a timestamp uniformly inside `DATE_WINDOW`, a missing end a window's length beyond the given one;
- a string from its values, else its pattern, else its format's type, else the fake its name hints, else letters and
  digits `TEXT_LENGTH` long. Only the letters and digits are kept inside its declared lengths;
- nulls at `NULL_RATE` where the schema allows them, and none where it requires a value.

The window is fixed rather than counted back from today, so one seed generates the same rows on any day.
"""

import dataclasses as dc
import datetime as dt
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

Bound = int | float | str | dt.date
Ordered = TypeVar('Ordered', int, float, dt.date, dt.datetime)


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
        semantic = {**(drawn.get('semantic') or {}), **(self.semantic or {})}
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
            if problem is None:
                return {'string': {'pattern': self.pattern}}
            warn_left_out(f'{name}: the pattern {self.pattern!r} cannot be drawn ({problem}), so it is left out')
        hinted = FORMAT_TYPES.get((self.format or '').lower())
        if hinted is not None:
            hint = next(entry.generator_hint for entry in GENERAL if entry.type == hinted)
            return {'semantic': {'type': hinted, 'generator_hint': dict(hint)}}
        if self.semantic and self.semantic.get('generator_hint'):
            return {}
        if not self.semantic:
            named = named_column(name)
            if named is not None:
                return named
        low, high = self.text_lengths()
        return {'string': {'length': {'min': low, 'max': high}}}

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
