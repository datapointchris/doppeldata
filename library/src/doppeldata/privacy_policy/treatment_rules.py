"""What each treatment changes in a column, the literals it refuses the column, and what else it requires of it.

The writer applies `amend` and drops each field `refused` names, and `profile check` reports what `refused` and
`treatment_faults` find, so a treatment's rules are written once.
"""

import datetime as dt
import math
import re
from typing import Any

from doppeldata.privacy_policy.column_treatment import OLDEST_AGE
from doppeldata.privacy_policy.column_treatment import Treatment
from doppeldata.privacy_policy.column_treatment import is_birth_date
from doppeldata.privacy_policy.column_treatment import restricted_zip_prefixes
from doppeldata.privacy_policy.literal_fields import DIGIT_BOUNDS
from doppeldata.privacy_policy.literal_fields import ColumnLiterals
from doppeldata.privacy_policy.literal_fields import Fault
from doppeldata.privacy_policy.literal_fields import FieldKind
from doppeldata.privacy_policy.literal_fields import LiteralField
from doppeldata.profile_format import LARGEST_EXACT_INTEGER

ZIP_PREFIX = re.compile(r'\d{3}')
YEAR_START = re.compile(r'\d{4}-01-01(?:[T ]00:00(?::00(?:\.0+)?)?)?(?:Z|[+-]\d\d:?\d\d)?')
YEAR_END = re.compile(r'\d{4}-12-31(?:[T ].*)?')
# The fields a column keeping no data value may still hold, which state its format and no value.
NO_VALUE_KEEPS = frozenset({FieldKind.SHAPES, FieldKind.PATTERN, FieldKind.MISSING_TOKENS})
REFUSED: dict[Treatment, frozenset[FieldKind]] = {
    Treatment.IDENTIFIER: frozenset(FieldKind) - NO_VALUE_KEEPS,
    Treatment.QUASI_IDENTIFIER: frozenset(FieldKind) - NO_VALUE_KEEPS,
    Treatment.FREE_TEXT: frozenset({FieldKind.TOP_VALUES, FieldKind.PREFIXES, FieldKind.SHAPES, FieldKind.PATTERN}),
    # A whole ZIP code, or a pattern's literals, can name a place a prefix does not.
    Treatment.ZIP_PREFIX: frozenset({FieldKind.TOP_VALUES, FieldKind.PATTERN}),
    # A fit's support reaches past 90, and an age held as text lists each age over 89 as a value of its own.
    Treatment.AGE: frozenset({FieldKind.FIT, FieldKind.TOP_VALUES}),
}


def refused(field: LiteralField, column: ColumnLiterals, treatment: Treatment) -> Fault | None:
    """The fault of a literal the column's treatment withholds, or None where it may hold it."""
    if field.kind not in REFUSED.get(treatment, frozenset()):
        return None
    label = field.kind.label
    if treatment.keeps_no_value:
        semantic_type = (column.column.get('semantic') or {}).get('type')
        sensitivity = 'an identifier' if treatment is Treatment.IDENTIFIER else 'a quasi-identifier under strict'
        typed = f'typed {semantic_type}, {sensitivity},' if semantic_type else f'{sensitivity},'
        return Fault('identifier-literal', field.path, f'{column.name} is {typed} and stores its {label}; expected no data value')
    if treatment is Treatment.FREE_TEXT:
        return Fault('free-text-literal', field.path, f'{column.name} is free text, and stores its {label}; expected its lengths only')
    if treatment is Treatment.ZIP_PREFIX:
        return Fault('zip-literal', field.path, f'{column.name} is a ZIP code, and stores its {label}; expected its prefixes only')
    return Fault('age-literal', field.path, f'{column.name} is an age, and stores its {label}; expected none')


def amend(column: dict[str, Any], treatment: Treatment, oldest_birth_year: int | None) -> None:
    """Rewrite what the treatment keeps in a coarser form: an identifier's digit bounds, an age capped at 90, and a
    date's extremes widened to whole years, a date of birth's raised to `oldest_birth_year`."""
    numeric, temporal = column.get('numeric') or {}, column.get('temporal') or {}
    extremes = numeric.get('extremes')
    if treatment.keeps_no_value and extremes and extremes.get('method') != DIGIT_BOUNDS:
        bounds = digit_bounds(extremes) if numeric.get('integral') else None
        if bounds:
            numeric['extremes'] = bounds
        else:
            del numeric['extremes']
    if treatment is Treatment.AGE and extremes:
        for end in ('low', 'high'):
            if isinstance(extremes.get(end), int | float) and extremes[end] > OLDEST_AGE:
                extremes[end] = OLDEST_AGE
    if treatment is Treatment.AGE:
        for name, value in (numeric.get('quantiles') or {}).items():
            if value > OLDEST_AGE:
                numeric['quantiles'][name] = OLDEST_AGE
    dates = temporal.get('extremes') or {}
    if treatment is Treatment.DATE_TO_YEAR and isinstance(dates.get('low'), str) and isinstance(dates.get('high'), str):
        floor = oldest_birth_year if is_birth_date(str(column.get('name', ''))) else None
        dates['low'], dates['high'] = (to_year(dates[end], end == 'high', floor) for end in ('low', 'high'))


def treatment_faults(column: ColumnLiterals, treatment: Treatment, oldest_birth_year: int | None) -> list[Fault]:
    """Where a column breaks a rule of its treatment beyond the literals it refuses. A date of birth's fault where the
    file states no `generated.at` points there."""
    at = column.path
    numeric = column.column.get('numeric') or {}
    string = column.column.get('string') or {}
    extremes = numeric.get('extremes') or {}
    found = []
    if treatment.keeps_no_value and extremes.get('method') == DIGIT_BOUNDS and not is_digit_bounds(extremes):
        bounds = f'extremes are {extremes.get("low")} and {extremes.get("high")}'
        detail = f'{bounds}; expected 0 or a power of ten, then one less than a power of ten'
        found.append(Fault('extremes-not-digit-bounds', (*at, 'numeric', 'extremes'), detail))
    if treatment is Treatment.ZIP_PREFIX:
        restricted = restricted_zip_prefixes()
        for index, entry in enumerate((string.get('prefixes') or {}).get('values') or []):
            value, path = str(entry.get('value')), (*at, 'string', 'prefixes', 'values', index)
            if not ZIP_PREFIX.fullmatch(value):
                found.append(Fault('zip-not-prefix', path, f'value is {value!r}; expected three digits'))
            elif value in restricted:
                found.append(Fault('restricted-zip-prefix', path, f'{value} is on the restricted prefix list; expected 000'))
    if treatment is Treatment.AGE:
        for end in ('low', 'high'):
            if isinstance(extremes.get(end), int | float) and extremes[end] > OLDEST_AGE:
                found.append(
                    Fault('age-over-89', (*at, 'numeric', 'extremes', end), f'{end} is {extremes[end]}; expected at most {OLDEST_AGE}')
                )
        for name, value in (numeric.get('quantiles') or {}).items():
            if value > OLDEST_AGE:
                found.append(Fault('age-over-89', (*at, 'numeric', 'quantiles', name), f'{name} is {value}; expected at most {OLDEST_AGE}'))
        edges = (numeric.get('histogram') or {}).get('edges') or []
        if edges and edges[-1] > OLDEST_AGE + 0.5:
            detail = f'the last edge is {edges[-1]}; expected at most {OLDEST_AGE + 0.5}'
            found.append(Fault('age-over-89', (*at, 'numeric', 'histogram', 'edges', len(edges) - 1), detail))
    if treatment is Treatment.DATE_TO_YEAR:
        dates = (column.column.get('temporal') or {}).get('extremes') or {}
        for end, shape, expected in (('low', YEAR_START, 'the first instant of a year'), ('high', YEAR_END, 'a time on December 31')):
            stated = dates.get(end)
            if stated is not None and not shape.fullmatch(str(stated)):
                found.append(Fault('date-below-year', (*at, 'temporal', 'extremes', end), f'{end} is {stated}; expected {expected}'))
        if dates and is_birth_date(column.name):
            if oldest_birth_year is None:
                found.append(
                    Fault('birth-date-unanchored', ('generated', 'at'), 'generated.at is absent; expected the time the profile was made')
                )
            for end in ('low', 'high'):
                stated = str(dates.get(end, ''))
                if stated[:4].isdigit() and oldest_birth_year is not None and int(stated[:4]) < oldest_birth_year:
                    detail = f'{end} is {stated}; expected {oldest_birth_year} or later, 90 years before generated.at'
                    found.append(Fault('age-over-89', (*at, 'temporal', 'extremes', end), detail))
    return found


def to_year(stated: str, last: bool, floor: int | None) -> str:
    """A date or timestamp widened to its year's first instant or last day, and its year raised to `floor` where it is
    earlier. One already at a year's edge, and no earlier than `floor`, is returned as written."""
    year = int(stated[:4]) if stated[:4].isdigit() else None
    widened = (YEAR_END if last else YEAR_START).fullmatch(stated)
    if widened and (floor is None or year is None or year >= floor):
        return stated
    parsed: dt.date = dt.datetime.fromisoformat(stated) if len(stated) > 10 else dt.date.fromisoformat(stated)
    target = max(parsed.year, floor) if floor is not None else parsed.year
    parsed = parsed.replace(year=target, month=12 if last else 1, day=31 if last else 1)
    if isinstance(parsed, dt.datetime):
        parsed = parsed.replace(hour=0, minute=0, second=0, microsecond=0)
    return parsed.isoformat()


def digit_bounds(extremes: dict[str, Any]) -> dict[str, Any] | None:
    """The least and greatest integers with as many digits as the extremes, or None where they are not both non-negative."""
    low, high = extremes.get('low'), extremes.get('high')
    if not isinstance(low, int | float) or not isinstance(high, int | float) or low < 0 or high < low:
        return None
    low_digits, high_digits = (len(str(int(value))) for value in (low, high))
    least, greatest = (0 if low_digits == 1 else 10 ** (low_digits - 1)), 10**high_digits - 1
    return {'low': written(least), 'high': written(greatest), 'method': DIGIT_BOUNDS}


def is_digit_bounds(extremes: dict[str, Any]) -> bool:
    """Whether the extremes are 0 or a power of ten, and one less than a power of ten, as `digit_bounds` writes them."""
    low, high = extremes.get('low'), extremes.get('high')

    def power_of_ten(value: Any) -> bool:
        return isinstance(value, int | float) and value >= 1 and math.isclose(math.log10(value), round(math.log10(value)), abs_tol=1e-12)

    return (low == 0 or power_of_ten(low)) and isinstance(high, int | float) and power_of_ten(high + 1)


def written(value: int) -> int | float:
    return value if value <= LARGEST_EXACT_INTEGER else float(value)
