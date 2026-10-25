"""Which rule of a privacy policy governs each column, read from what a profile states about it.

The profiler decides from a column's kind and typing before it writes, and `profile check` decides from the written
column, so both reach the same treatment through `column_treatment`.
"""

import dataclasses as dc
import enum
import importlib.resources
from functools import cache
from typing import Any

from doppeldata.column_name_words import hint_runs_in
from doppeldata.column_name_words import name_words

# A cell from 1 to 10 rows is what the CMS cell-size suppression policy forbids reporting.
DEFAULT_K_MIN = 11
# Safe Harbor's single category for every age over 89.
OLDEST_AGE = 90
# Words a name ends in when the column holds an age in years.
AGE_HINTS = ('age', 'age_years', 'age_in_years', 'age_yrs')
# Words a name holds anywhere when the column holds a date tied to a person, as Safe Harbor names them.
PERSON_DATE_HINTS = (
    'dob',
    'birth',
    'birthdate',
    'dod',
    'death',
    'deceased',
    'service',
    'admit',
    'admission',
    'admitted',
    'discharge',
    'discharged',
    'visit',
    'encounter',
)
# The semantic types in Safe Harbor's 18 identifier categories, each an identifier under `healthcare`
# whatever its pack calls it. A ZIP code has its own rule.
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


# The policies `profile check` checks, and a profiler applies.
CHECKED_POLICIES = (Policy.DEFAULT, Policy.STRICT, Policy.HEALTHCARE)


class Treatment(enum.Enum):
    """The rule a column's literals are held to. Each but `AT_K_MIN` withholds some, with its `privacy.suppressed` reason."""

    AT_K_MIN = None
    IDENTIFIER = 'identifier'
    QUASI_IDENTIFIER = 'quasi_identifier'
    FREE_TEXT = 'free_text'
    ZIP_PREFIX = 'zip_prefix'
    AGE = 'age_over_89'
    PERSON_DATE = 'date_to_year'

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
        if facts.semantic_type == ZIP_TYPE:
            return Treatment.ZIP_PREFIX if facts.kind is ValueFamily.STRING else Treatment.IDENTIFIER
        if facts.semantic_type in SAFE_HARBOR_TYPES:
            return Treatment.IDENTIFIER
        if facts.kind is ValueFamily.NUMERIC and any(hint_runs_in(words, hint, at_end=True) for hint in AGE_HINTS):
            return Treatment.AGE
        if facts.kind is ValueFamily.TEMPORAL and any(hint_runs_in(words, hint) for hint in PERSON_DATE_HINTS):
            return Treatment.PERSON_DATE
    if facts.sensitivity == 'identifier':
        return Treatment.IDENTIFIER
    if policy is Policy.STRICT and facts.sensitivity == 'quasi_identifier':
        return Treatment.QUASI_IDENTIFIER
    return Treatment.AT_K_MIN


@cache
def restricted_zip_prefixes() -> frozenset[str]:
    """The three-digit ZIP prefixes whose population is 20,000 or fewer, which `healthcare` writes as `000`."""
    text = importlib.resources.files(__package__).joinpath(RESTRICTED_ZIP_PREFIXES_FILE).read_text()
    return frozenset(line.strip() for line in text.splitlines() if line.strip() and not line.startswith('#'))
