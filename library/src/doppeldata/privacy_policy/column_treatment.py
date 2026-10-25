"""Which rule of a privacy policy governs each column, read from what a profile states about it.

The profiler decides from a column's kind and typing before it writes, and `profile check` decides from the written
column, so both reach the same treatment through `column_treatment`. Under `healthcare` it fails closed: every date
keeps only its years, and a name that could hold an age, a ZIP code or a place smaller than a state is taken to.
"""

import dataclasses as dc
import enum
import importlib.resources
from functools import cache
from typing import Any

from doppeldata import _core
from doppeldata.column_name_words import ZIP_HINTS
from doppeldata.column_name_words import hint_runs_in
from doppeldata.column_name_words import name_words
from doppeldata.published_schemas import published_enum

# A cell from 1 to 10 rows is what the CMS cell-size suppression policy forbids reporting. The engine reads the same
# floor from a profile stating none.
DEFAULT_K_MIN = _core.DEFAULT_K_MIN
# Safe Harbor's single category for every age over 89.
OLDEST_AGE = 90
# Words a name holds anywhere when the column holds an age in years.
AGE_HINTS = ('age', 'age_years', 'age_in_years', 'age_yrs')
# Words a name holds anywhere when the column holds a place smaller than a state, which Safe Harbor removes.
GEOGRAPHY_HINTS = (
    'address',
    'addr',
    'street',
    'city',
    'town',
    'county',
    'parish',
    'precinct',
    'geocode',
    'latitude',
    'longitude',
    'lat',
    'lon',
    'lng',
)
# The semantic types Safe Harbor names, each an identifier under `healthcare` whatever sensitivity the profile gives
# it. A ZIP code, a date and a place below a state each have their own rule.
SAFE_HARBOR_TYPES = frozenset(
    {
        'person_name',
        'email',
        'phone_number',
        'us_ssn',
        'credit_card',
        'url',
        'ipv4_address',
        'ipv6_address',
        'mac_address',
        'uuid',
        'npi',
        'mbi',
        'dea_number',
        'health_plan_beneficiary_number',
        'medical_record_number',
        'claim_number',
    }
)
ZIP_TYPE = 'us_zip'
FREE_TEXT_TYPE = 'free_text'
RESTRICTED_ZIP_PREFIXES_FILE = 'restricted_zip_prefixes.txt'


class Policy(enum.StrEnum):
    """A profile's `privacy.policy`."""

    DEFAULT = 'default'
    STRICT = 'strict'
    HEALTHCARE = 'healthcare'
    DP = 'dp'
    NONE = 'none'

    @property
    def applied(self) -> bool:
        """Whether a profiler writes a profile under it. `dp` is reserved, and none applies it."""
        return self is not Policy.DP

    @property
    def checked(self) -> bool:
        """Whether `profile check` checks a profile under it. A `none` profile withholds nothing, so it fails as such."""
        return self in (Policy.DEFAULT, Policy.STRICT, Policy.HEALTHCARE)

    def k_min(self, given: int | None) -> int | None:
        """The `k_min` a profile under it states: none under `none`, which holds no floor, and otherwise `given` or 11."""
        return None if self is Policy.NONE else given or DEFAULT_K_MIN


class Treatment(enum.Enum):
    """The rule a column's literals are held to. Each but `AT_K_MIN` withholds some, with its `privacy.suppressed` reason."""

    AT_K_MIN = None
    IDENTIFIER = 'identifier'
    QUASI_IDENTIFIER = 'quasi_identifier'
    FREE_TEXT = 'free_text'
    ZIP_PREFIX = 'zip_prefix'
    AGE = 'age_over_89'
    DATE_TO_YEAR = 'date_to_year'

    @property
    def keeps_no_value(self) -> bool:
        """Whether the column keeps no data value: only its null rate, distinct ratio, lengths, shapes and pattern."""
        return self in (Treatment.IDENTIFIER, Treatment.QUASI_IDENTIFIER)


class ValueFamily(enum.Enum):
    """The family of statistics a column's values have, which decides what a policy's rule reads."""

    NUMERIC = 'numeric'
    STRING = 'string'
    TEMPORAL = 'temporal'
    OTHER = 'other'


@dc.dataclass(frozen=True)
class ColumnFacts:
    """What a policy reads of one column: its name, kind, and the type and sensitivity a detector gave it."""

    name: str
    kind: ValueFamily
    integral: bool = False
    semantic_type: str | None = None
    sensitivity: str | None = None

    @classmethod
    def of(cls, column: dict[str, Any]) -> 'ColumnFacts':
        """The facts a written column states."""
        semantic = column.get('semantic') or {}
        logical = column.get('logical_type')
        if 'numeric' in column or logical in ('integer', 'number'):
            kind = ValueFamily.NUMERIC
        elif 'temporal' in column or logical in ('date', 'timestamp', 'time'):
            kind = ValueFamily.TEMPORAL
        elif 'string' in column or logical == 'string':
            kind = ValueFamily.STRING
        else:
            kind = ValueFamily.OTHER
        integral = bool((column.get('numeric') or {}).get('integral', logical == 'integer'))
        return cls(column.get('name', ''), kind, integral, semantic.get('type'), semantic.get('sensitivity'))


def column_treatment(facts: ColumnFacts, policy: Policy) -> Treatment:
    """The rule `policy` holds the column to."""
    if policy is Policy.NONE:
        return Treatment.AT_K_MIN
    if facts.semantic_type == FREE_TEXT_TYPE:
        return Treatment.FREE_TEXT
    if policy is Policy.HEALTHCARE:
        words = name_words(facts.name)
        # A ZIP column read as a number, or not typed, is still a ZIP column, so its name decides as its type does.
        if facts.semantic_type == ZIP_TYPE or any(hint_runs_in(words, hint, at_end=True) for hint in ZIP_HINTS):
            return Treatment.ZIP_PREFIX if facts.kind is ValueFamily.STRING else Treatment.IDENTIFIER
        if facts.semantic_type in SAFE_HARBOR_TYPES or any(hint_runs_in(words, hint) for hint in GEOGRAPHY_HINTS):
            return Treatment.IDENTIFIER
        if facts.kind is ValueFamily.TEMPORAL:
            return Treatment.DATE_TO_YEAR
        # A year, month or day of birth held as a number is an element of a date that no age cap reaches.
        if facts.kind is ValueFamily.NUMERIC and is_birth_date(facts.name):
            return Treatment.IDENTIFIER
        if any(hint_runs_in(words, hint) for hint in AGE_HINTS):
            return Treatment.AGE
    if facts.sensitivity == 'identifier':
        return Treatment.IDENTIFIER
    if policy is Policy.STRICT and facts.sensitivity == 'quasi_identifier':
        return Treatment.QUASI_IDENTIFIER
    return Treatment.AT_K_MIN


def is_birth_date(name: str) -> bool:
    """Whether a column's name says it holds a date of birth, as `dob`, `birth_date`, `BIRTHDATE` or `year_of_birth`."""
    return 'birth' in name.lower() or any(word.endswith('dob') for word in name_words(name))


def oldest_birth_year(table: dict[str, Any]) -> int | None:
    """The earliest birth year `healthcare` keeps: the year the profile was made, less 90. None where it states no date."""
    made = (table.get('generated') or {}).get('at')
    if not isinstance(made, str) or not made[:4].isdigit():
        return None
    return int(made[:4]) - OLDEST_AGE


@cache
def restricted_zip_prefixes() -> frozenset[str]:
    """The three-digit ZIP prefixes whose population is 20,000 or fewer, which `healthcare` writes as `000`."""
    text = importlib.resources.files(__package__).joinpath(RESTRICTED_ZIP_PREFIXES_FILE).read_text()
    return frozenset(line.strip() for line in text.splitlines() if line.strip() and not line.startswith('#'))


CHECKED_POLICIES = tuple(policy for policy in Policy if policy.checked)
APPLIED_POLICIES = tuple(policy for policy in Policy if policy.applied)

for definition, spelled in (
    ('PrivacyPolicy', {policy.value for policy in Policy}),
    ('SuppressionReason', {treatment.value for treatment in Treatment if treatment.value}),
):
    if spelled != set(published_enum(definition)):
        raise RuntimeError(
            f'the privacy policy spells {definition} as {sorted(spelled)}; the profile format has {published_enum(definition)}'
        )
