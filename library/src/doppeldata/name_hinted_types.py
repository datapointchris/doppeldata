"""The general pack's semantic types, each with the column names that hint it and the generator hint that draws it.

The general pack's detectors add a validator to each. Generating from column names and drawing a typed column that names no
hint read them too, where no profiling engine is installed, so nothing here imports one.
"""

import copy
import dataclasses as dc
from collections.abc import Mapping
from typing import Any

from doppeldata.column_name_words import ZIP_HINTS
from doppeldata.column_name_words import hint_runs_in
from doppeldata.column_name_words import name_words


@dc.dataclass(frozen=True)
class HintedType:
    """A semantic type, the words a column's name holds where it hints the type, and what the generator draws for it.

    `fake` is the provider drawing what a pattern hint draws, offered to a name close to the hints that matches none.
    """

    type: str
    name_hints: tuple[str, ...]
    generator_hint: Mapping[str, str]
    fake: str | None = None

    @property
    def offered_fake(self) -> str | None:
        """The provider a name close to this type's hints is offered: the hint's own, or else `fake`."""
        return self.generator_hint.get('provider') or self.fake

    def ends(self, name: str) -> bool:
        """Whether one of the hints ends `name`, as a hint must where the name alone assigns the type."""
        words = name_words(name)
        return any(hint_runs_in(words, hint, at_end=True) for hint in self.name_hints)


# Area 001-899 but 666, group 01-99, serial 0001-9999. rand_regex has no lookaround, so each exclusion is spelled out.
SSN_AREA = r'(?:00[1-9]|0[1-9]\d|[1-578]\d\d|6[0-57-9]\d|66[0-57-9])'
SSN_GROUP = r'(?:0[1-9]|[1-9]\d)'
SSN_SERIAL = r'(?:000[1-9]|00[1-9]\d|0[1-9]\d\d|[1-9]\d{3})'
SSN = f'{SSN_AREA}-{SSN_GROUP}-{SSN_SERIAL}'
SSN_UNDASHED = f'{SSN_AREA}{SSN_GROUP}{SSN_SERIAL}'
SSN_HINTS = ('ssn', 'social', 'social_security')

EMAIL = HintedType('email', ('email', 'e_mail', 'mail', 'emailaddress', 'email_address'), {'provider': 'email'})
PHONE_NUMBER = HintedType(
    'phone_number',
    ('phone', 'telephone', 'tel', 'mobile', 'cell', 'fax', 'phonenumber', 'phone_number'),
    {'pattern': r'[2-9]\d\d-[2-9]\d\d-\d{4}'},
)
CREDIT_CARD = HintedType(
    'credit_card',
    ('card', 'credit_card', 'cc', 'pan', 'creditcard', 'cardnumber'),
    {'provider': 'credit_card', 'check_digit': 'luhn'},
)
US_SSN = HintedType('us_ssn', SSN_HINTS, {'pattern': SSN})
US_SSN_UNDASHED = HintedType('us_ssn', SSN_HINTS, {'pattern': SSN_UNDASHED})
UUID = HintedType('uuid', ('uuid', 'guid'), {'provider': 'uuid'})
IPV4_ADDRESS = HintedType('ipv4_address', ('ip', 'ipv4', 'ip_address', 'ipaddress'), {'provider': 'ipv4'})
IPV6_ADDRESS = HintedType('ipv6_address', ('ip', 'ipv6', 'ip_address', 'ipaddress'), {'provider': 'ipv6'})
MAC_ADDRESS = HintedType('mac_address', ('mac', 'mac_address', 'macaddress'), {'provider': 'mac_address'})
# Reserved domains, so no generated link reaches anyone.
URL = HintedType(
    'url',
    ('url', 'uri', 'link', 'href', 'website', 'homepage'),
    {'pattern': r'https://example\.(?:com|net|org)/[a-z0-9]{1,12}(?:/[a-z0-9]{1,12}){0,2}'},
)
US_ZIP = HintedType('us_zip', ZIP_HINTS, {'pattern': r'\d{5}'}, fake='zip_code')
US_STATE = HintedType('us_state', ('state', 'st', 'state_code', 'state_abbr', 'province'), {'provider': 'state_abbr'})
# Split by the part of a name the column holds, so a first-name column draws first names. A bare `name` hints none of
# them, because a product, a company or a file has one too.
FIRST_NAME = HintedType('person_name', ('first_name', 'middle_name', 'given_name', 'firstname', 'fname'), {'provider': 'first_name'})
LAST_NAME = HintedType('person_name', ('last_name', 'family_name', 'surname', 'lastname', 'lname'), {'provider': 'last_name'})
FULL_NAME = HintedType(
    'person_name',
    ('full_name', 'fullname', 'person_name', 'patient_name', 'member_name', 'customer_name', 'contact_name', 'employee_name'),
    {'provider': 'name'},
)
FREE_TEXT = HintedType(
    'free_text',
    ('comment', 'comments', 'note', 'notes', 'description', 'remarks', 'narrative', 'message', 'memo', 'feedback', 'body'),
    {'pattern': r'[A-Z][a-z]{1,9}(?: [a-z]{1,9}){3,11}\.'},
)

# In the order a column's name is tried against them. A type's first entry is what a column of the type draws where its
# name hints none of the type's entries.
GENERAL = (
    EMAIL,
    PHONE_NUMBER,
    CREDIT_CARD,
    US_SSN,
    US_SSN_UNDASHED,
    UUID,
    IPV4_ADDRESS,
    IPV6_ADDRESS,
    MAC_ADDRESS,
    URL,
    US_ZIP,
    US_STATE,
    FULL_NAME,
    FIRST_NAME,
    LAST_NAME,
    FREE_TEXT,
)
# A column holds text where its Arrow type is one of these, or where it names only the logical type.
TEXT_TYPES = ('string', 'large_string', 'utf8', 'large_utf8')


def type_named_by(name: str) -> HintedType | None:
    """The general type whose hint ends a column's `name`, or None."""
    return next((hinted for hinted in GENERAL if hinted.ends(name)), None)


def type_hint(semantic_type: str, name: str) -> Mapping[str, str] | None:
    """What a column of `semantic_type` named `name` draws: the entry its name hints, else the type's first. None for a
    type the general pack does not declare."""
    entries = [hinted for hinted in GENERAL if hinted.type == semantic_type]
    if not entries:
        return None
    return next((hinted for hinted in entries if hinted.ends(name)), entries[0]).generator_hint


def with_type_hints(document: dict[str, Any]) -> dict[str, Any]:
    """A plain profile with each typed text column that has no format of its own given its type's generator hint.

    A column's own format is a generator hint, a stored pattern or shapes. Without one the engine draws characters. The
    profiler leaves the hint out where grex stored a pattern, so a profiled column keeps drawing in that format.
    """
    if not any(needs_type_hint(column) for column in every_column(document.get('columns') or [])):
        return document
    filled = copy.deepcopy(document)
    for column in every_column(filled.get('columns') or []):
        hint = type_hint(column['semantic']['type'], column.get('name') or '') if needs_type_hint(column) else None
        if hint is not None:
            column['semantic']['generator_hint'] = dict(hint)
    return filled


def every_column(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Each column, and each list element and struct field inside one."""
    found = []
    for column in columns:
        found.append(column)
        element = (column.get('list') or {}).get('element')
        fields = (column.get('struct') or {}).get('fields') or []
        found.extend(every_column(([element] if element else []) + fields))
    return found


def needs_type_hint(column: dict[str, Any]) -> bool:
    semantic = column.get('semantic') or {}
    string = column.get('string') or {}
    physical = column.get('physical_type')
    text = physical in TEXT_TYPES or (physical is None and column.get('logical_type') == 'string')
    own_format = semantic.get('generator_hint') or string.get('pattern') or string.get('shapes')
    return bool(semantic.get('type')) and text and not own_format
