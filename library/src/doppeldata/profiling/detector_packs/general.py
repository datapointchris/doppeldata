"""The general pack: identifiers detectable by their format and check digit, and the types a column name alone hints.

Every pattern here is fixed, and comes from no data. Each generator hint draws values its own validator accepts.
"""

import dataclasses as dc
import ipaddress
import urllib.parse
from collections.abc import Callable
from collections.abc import Mapping

import pyarrow as pa
import pyarrow.compute as pc

from doppeldata.profiling.detector_packs import Sensitivity

Validator = Callable[[pa.Array], pa.BooleanArray]


@dc.dataclass(frozen=True)
class GeneralDetector:
    type: str
    sensitivity: Sensitivity
    glue_type: str | None
    name_hints: tuple[str, ...]
    generator_hint: Mapping[str, str]
    validate: Validator | None


@dc.dataclass(frozen=True)
class GeneralPack:
    name: str
    detectors: tuple[GeneralDetector, ...]


def matching(pattern: str) -> Validator:
    """Whether each value matches the RE2 `pattern`, which must anchor both ends."""
    return lambda values: pc.match_substring_regex(values, pattern)


def refined(shape: str, accepts: Callable[[str], bool]) -> Validator:
    """Whether each value matches the RE2 `shape` and `accepts` takes it."""
    return lambda values: accepted(values, pc.match_substring_regex(values, shape), accepts)


def accepted(values: pa.Array, candidates: pa.BooleanArray, accepts: Callable[[str], bool]) -> pa.BooleanArray:
    """`candidates`, keeping each value it marks only where `accepts` takes it. Only the marked values reach Python."""
    indices = pc.indices_nonzero(candidates)
    if not len(indices):
        return candidates
    passed = candidates.to_pylist()
    for index, value in zip(indices.to_pylist(), values.take(indices).to_pylist(), strict=True):
        passed[index] = accepts(value)
    return pa.array(passed, pa.bool_())


def luhn_valid(digits: str) -> bool:
    """Whether the last of the ASCII `digits` is the Luhn check digit of the rest."""
    total = 0
    for position, character in enumerate(reversed(digits)):
        digit = ord(character) - ord('0')
        if position % 2:
            digit = digit * 2 - 9 if digit > 4 else digit * 2
        total += digit
    return total % 10 == 0


def card_number(values: pa.Array) -> pa.BooleanArray:
    """13 to 19 digits, grouped by spaces or hyphens or not at all, starting with a card network's IIN, and Luhn-valid."""
    digits = pc.replace_substring_regex(values, '[ -]', '')
    shaped = pc.and_(pc.match_substring_regex(digits, r'^\d{13,19}$'), pc.match_substring_regex(digits, CARD_IIN))
    return accepted(digits, shaped, luhn_valid)


def ipv4(value: str) -> bool:
    try:
        ipaddress.IPv4Address(value)
    except ValueError:
        return False
    return True


def ipv6(value: str) -> bool:
    try:
        ipaddress.IPv6Address(value)
    except ValueError:
        return False
    return True


def url(value: str) -> bool:
    try:
        return bool(urllib.parse.urlsplit(value).hostname)
    except ValueError:
        return False


def free_text(values: pa.Array) -> pa.BooleanArray:
    """Values of 4 or more words, in a column whose values average over 40 characters."""
    words = pc.greater_equal(pc.count_substring_regex(values, r'\S+'), 4)
    mean_length = pc.mean(pc.utf8_length(values)).as_py() or 0
    return words if mean_length > 40 else pa.repeat(False, len(values))


EMAIL = (
    r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$"
)
# E.164, or a NANP number with an optional country code. A NANP area code and exchange never start with 0 or 1, which
# keeps 10-digit identifiers such as NPIs out.
PHONE = r'^\+[1-9]\d{6,14}$|^(?:\+?1[-. ]?)?(?:\([2-9]\d\d\)|[2-9]\d\d)[-. ]?[2-9]\d\d[-. ]?\d{4}$'
# Visa; Mastercard's 51-55 and 2221-2720; American Express; Diners Club; Discover and UnionPay; JCB.
CARD_IIN = r'^(?:4|5[1-5]|2(?:22[1-9]|2[3-9]\d|[3-6]\d\d|7[01]\d|720)|3[47]|3(?:0[0-5]|[689])|6(?:011|4[4-9]|5|2)|35(?:2[89]|[3-8]\d))'
# Area 001-899 but 666, group 01-99, serial 0001-9999. rand_regex has no lookaround, so each exclusion is spelled out.
SSN = (
    r'(?:00[1-9]|0[1-9]\d|[1-578]\d\d|6[0-57-9]\d|66[0-57-9])'
    r'-(?:0[1-9]|[1-9]\d)'
    r'-(?:000[1-9]|00[1-9]\d|0[1-9]\d\d|[1-9]\d{3})'
)
# RFC 9562: versions 1 to 8, and the variant's leading bits 10.
UUID = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$'
MAC = r'^[0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2}){5}$|^[0-9A-Fa-f]{2}(?:-[0-9A-Fa-f]{2}){5}$|^[0-9A-Fa-f]{4}(?:\.[0-9A-Fa-f]{4}){2}$'
# The 50 states, DC, the five inhabited territories, and the three armed forces codes, two letters each. They are
# run together, because a spell checker reads North Dakota's on its own as a misspelled "and".
USPS_STATES = 'ALAKAZARCACOCTDEFLGAHIIDILINIAKSKYLAMEMDMAMIMNMSMOMTNENVNHNJNMNYNCNDOHOKORPARISCSDTNTXUTVTVAWAWVWIWYDCPRVIGUASMPAAAEAP'
STATE_CODES = pa.array([USPS_STATES[at : at + 2] for at in range(0, len(USPS_STATES), 2)])

PACK = GeneralPack(
    'general',
    (
        GeneralDetector(
            'email', 'identifier', 'EMAIL', ('email', 'e_mail', 'mail', 'emailaddress'), {'provider': 'email'}, matching(EMAIL)
        ),
        GeneralDetector(
            'phone_number',
            'identifier',
            'PHONE_NUMBER',
            ('phone', 'telephone', 'tel', 'mobile', 'cell', 'fax', 'phonenumber'),
            {'pattern': r'[2-9]\d\d-[2-9]\d\d-\d{4}'},
            matching(PHONE),
        ),
        GeneralDetector(
            'credit_card',
            'identifier',
            'CREDIT_CARD',
            ('card', 'credit_card', 'cc', 'pan', 'creditcard', 'cardnumber'),
            {'provider': 'credit_card', 'check_digit': 'luhn'},
            card_number,
        ),
        GeneralDetector('us_ssn', 'identifier', 'USA_SSN', ('ssn', 'social', 'social_security'), {'pattern': SSN}, matching(f'^{SSN}$')),
        GeneralDetector('uuid', 'identifier', None, ('uuid', 'guid'), {'provider': 'uuid'}, matching(UUID)),
        GeneralDetector(
            'ipv4_address',
            'identifier',
            'IP_ADDRESS',
            ('ip', 'ipv4', 'ip_address', 'ipaddress'),
            {'provider': 'ipv4'},
            refined(r'^\d{1,3}(?:\.\d{1,3}){3}$', ipv4),
        ),
        GeneralDetector(
            'ipv6_address',
            'identifier',
            'IP_ADDRESS',
            ('ip', 'ipv6', 'ip_address', 'ipaddress'),
            {'provider': 'ipv6'},
            refined(r'^[0-9A-Fa-f.]*:[0-9A-Fa-f:.]*:[0-9A-Fa-f:.]*$', ipv6),
        ),
        GeneralDetector(
            'mac_address', 'identifier', 'MAC_ADDRESS', ('mac', 'mac_address', 'macaddress'), {'provider': 'mac_address'}, matching(MAC)
        ),
        GeneralDetector(
            'url',
            'quasi_identifier',
            None,
            ('url', 'uri', 'link', 'href', 'website', 'homepage'),
            # Reserved domains, so no generated link reaches anyone.
            {'pattern': r'https://example\.(?:com|net|org)/[a-z0-9]{1,12}(?:/[a-z0-9]{1,12}){0,2}'},
            refined(r'^[A-Za-z][A-Za-z0-9+.-]*://[^\s/?#]+\S*$', url),
        ),
        GeneralDetector(
            'us_zip',
            'quasi_identifier',
            None,
            ('zip', 'zipcode', 'zip_code', 'postal', 'postal_code', 'postcode'),
            # fake's zip_code draws 3- and 4-digit codes, and its post_code half ZIP+4.
            {'pattern': r'\d{5}'},
            matching(r'^\d{5}(?:-\d{4})?$'),
        ),
        GeneralDetector(
            'us_state',
            'none',
            None,
            ('state', 'st', 'state_code', 'state_abbr', 'province'),
            {'provider': 'state_abbr'},
            lambda values: pc.is_in(values, value_set=STATE_CODES),
        ),
        # A person's name has no validator, so each of these three is assigned by its name hints alone. They split by
        # what part of the name the column holds, so a first-name column draws first names. A bare `name` hints none of
        # them, because a product, a company or a file has one too.
        GeneralDetector(
            'person_name',
            'identifier',
            'PERSON_NAME',
            ('first_name', 'middle_name', 'given_name', 'firstname', 'fname'),
            {'provider': 'first_name'},
            None,
        ),
        GeneralDetector(
            'person_name',
            'identifier',
            'PERSON_NAME',
            ('last_name', 'family_name', 'surname', 'lastname', 'lname'),
            {'provider': 'last_name'},
            None,
        ),
        GeneralDetector(
            'person_name',
            'identifier',
            'PERSON_NAME',
            ('full_name', 'fullname', 'person_name', 'patient_name', 'member_name', 'customer_name', 'contact_name', 'employee_name'),
            {'provider': 'name'},
            None,
        ),
        GeneralDetector(
            'free_text',
            'sensitive',
            None,
            ('comment', 'comments', 'note', 'notes', 'description', 'remarks', 'narrative', 'message', 'memo', 'feedback', 'body'),
            {'pattern': r'[A-Z][a-z]{1,9}(?: [a-z]{1,9}){3,11}\.'},
            free_text,
        ),
    ),
)
