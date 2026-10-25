"""The general pack: identifiers detectable by their format, and the types a column name hints and its values confirm.

Every pattern here is fixed, and comes from no data.
"""

import ipaddress
import urllib.parse
from collections.abc import Callable

import pyarrow as pa
import pyarrow.compute as pc

from doppeldata.column_name_words import ZIP_HINTS
from doppeldata.profiling.detector_packs import Detector
from doppeldata.profiling.detector_packs import DetectorPack
from doppeldata.profiling.detector_packs import Validator


def matching(pattern: str) -> Validator:
    """Whether each value matches the RE2 `pattern`, which must anchor both ends."""
    return lambda values: pc.match_substring_regex(values, pattern)


def padded_matching(width: int, pattern: str) -> Validator:
    """Whether each value, left-padded with zeros to `width`, matches the RE2 `pattern`, as an integer that lost its leading zeros does."""
    return lambda values: pc.match_substring_regex(pc.utf8_lpad(values, width, '0'), pattern)


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


def card_number(values: pa.Array) -> pa.BooleanArray:
    """13 to 19 digits, grouped by spaces or hyphens or not at all, starting with a card network's IIN."""
    digits = pc.replace_substring_regex(values, '[ -]', '')
    return pc.and_(pc.match_substring_regex(digits, r'^\d{13,19}$'), pc.match_substring_regex(digits, CARD_IIN))


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


def person_name(name_hints: tuple[str, ...], provider: str) -> Detector:
    """A person's name, or one part of it. Letters alone pass, so only a column whose name hints it takes the type."""
    return Detector(
        'person_name',
        'identifier',
        name_hints,
        {'provider': provider},
        matching(PERSON_NAME),
        'pattern',
        hint_required=True,
        glue_type='PERSON_NAME',
    )


EMAIL = (
    r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$"
)
# E.164, or a NANP number with an optional country code. A NANP area code and exchange never start with 0 or 1, which
# keeps 10-digit identifiers such as NPIs out.
PHONE = r'^\+[1-9]\d{6,14}$|^(?:\+?1[-. ]?)?(?:\([2-9]\d\d\)|[2-9]\d\d)[-. ]?[2-9]\d\d[-. ]?\d{4}$'
# Visa; Mastercard's 51-55 and 2221-2720; American Express; Diners Club; Discover and UnionPay; JCB.
CARD_IIN = r'^(?:4|5[1-5]|2(?:22[1-9]|2[3-9]\d|[3-6]\d\d|7[01]\d|720)|3[47]|3(?:0[0-5]|[689])|6(?:011|4[4-9]|5|2)|35(?:2[89]|[3-8]\d))'
# Area 001-899 but 666, group 01-99, serial 0001-9999. rand_regex has no lookaround, so each exclusion is spelled out.
SSN_AREA = r'(?:00[1-9]|0[1-9]\d|[1-578]\d\d|6[0-57-9]\d|66[0-57-9])'
SSN_GROUP = r'(?:0[1-9]|[1-9]\d)'
SSN_SERIAL = r'(?:000[1-9]|00[1-9]\d|0[1-9]\d\d|[1-9]\d{3})'
SSN = f'{SSN_AREA}-{SSN_GROUP}-{SSN_SERIAL}'
SSN_UNDASHED = f'{SSN_AREA}{SSN_GROUP}{SSN_SERIAL}'
# RFC 9562: versions 1 to 8, and the variant's leading bits 10.
UUID = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$'
MAC = r'^[0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2}){5}$|^[0-9A-Fa-f]{2}(?:-[0-9A-Fa-f]{2}){5}$|^[0-9A-Fa-f]{4}(?:\.[0-9A-Fa-f]{4}){2}$'
# Letters, and the spaces, apostrophes, periods and hyphens between them, as in Mary-Ann O'Neil Jr.
PERSON_NAME = r"^\pL[\pL' .-]*$"
# The 50 states, DC, the five inhabited territories, and the three armed forces codes, two letters each. They are
# run together, because a spell checker reads North Dakota's on its own as a misspelled "and".
USPS_STATES = 'ALAKAZARCACOCTDEFLGAHIIDILINIAKSKYLAMEMDMAMIMNMSMOMTNENVNHNJNMNYNCNDOHOKORPARISCSDTNTXUTVTVAWAWVWIWYDCPRVIGUASMPAAAEAP'
STATE_CODES = pa.array([USPS_STATES[at : at + 2] for at in range(0, len(USPS_STATES), 2)])
SSN_HINTS = ('ssn', 'social', 'social_security')

PACK = DetectorPack(
    'general',
    (
        Detector(
            'email',
            'identifier',
            ('email', 'e_mail', 'mail', 'emailaddress'),
            {'provider': 'email'},
            matching(EMAIL),
            'pattern',
            glue_type='EMAIL',
        ),
        Detector(
            'phone_number',
            'identifier',
            ('phone', 'telephone', 'tel', 'mobile', 'cell', 'fax', 'phonenumber'),
            {'pattern': r'[2-9]\d\d-[2-9]\d\d-\d{4}'},
            matching(PHONE),
            'pattern',
            glue_type='PHONE_NUMBER',
        ),
        Detector(
            'credit_card',
            'identifier',
            ('card', 'credit_card', 'cc', 'pan', 'creditcard', 'cardnumber'),
            {'provider': 'credit_card', 'check_digit': 'luhn'},
            card_number,
            'pattern',
            glue_type='CREDIT_CARD',
        ),
        Detector('us_ssn', 'identifier', SSN_HINTS, {'pattern': SSN}, matching(f'^{SSN}$'), 'pattern', glue_type='USA_SSN'),
        # Most 9-digit numbers pass an SSN's ranges, so an SSN without hyphens, or an integer one that lost its leading
        # zeros, is typed only where the column's name hints it.
        Detector(
            'us_ssn',
            'identifier',
            SSN_HINTS,
            {'pattern': SSN_UNDASHED},
            padded_matching(9, f'^{SSN_UNDASHED}$'),
            'pattern',
            hint_required=True,
            glue_type='USA_SSN',
        ),
        Detector('uuid', 'identifier', ('uuid', 'guid'), {'provider': 'uuid'}, matching(UUID), 'pattern'),
        Detector(
            'ipv4_address',
            'identifier',
            ('ip', 'ipv4', 'ip_address', 'ipaddress'),
            {'provider': 'ipv4'},
            refined(r'^\d{1,3}(?:\.\d{1,3}){3}$', ipv4),
            glue_type='IP_ADDRESS',
        ),
        Detector(
            'ipv6_address',
            'identifier',
            ('ip', 'ipv6', 'ip_address', 'ipaddress'),
            {'provider': 'ipv6'},
            refined(r'^[0-9A-Fa-f.]*:[0-9A-Fa-f:.]*:[0-9A-Fa-f:.]*$', ipv6),
            glue_type='IP_ADDRESS',
        ),
        Detector(
            'mac_address',
            'identifier',
            ('mac', 'mac_address', 'macaddress'),
            {'provider': 'mac_address'},
            matching(MAC),
            'pattern',
            glue_type='MAC_ADDRESS',
        ),
        Detector(
            'url',
            'quasi_identifier',
            ('url', 'uri', 'link', 'href', 'website', 'homepage'),
            # Reserved domains, so no generated link reaches anyone.
            {'pattern': r'https://example\.(?:com|net|org)/[a-z0-9]{1,12}(?:/[a-z0-9]{1,12}){0,2}'},
            refined(r'^[A-Za-z][A-Za-z0-9+.-]*://[^\s/?#]+\S*$', url),
        ),
        # A pattern, because the fake crate's zip_code and post_code draw codes this validator fails.
        Detector('us_zip', 'quasi_identifier', ZIP_HINTS, {'pattern': r'\d{5}'}, matching(r'^\d{5}(?:-\d{4})?$'), 'pattern'),
        # A ZIP code below 10000 stored as an integer loses its leading zeros, and the lowest, 00501, keeps three digits.
        Detector('us_zip', 'quasi_identifier', ZIP_HINTS, {'pattern': r'\d{5}'}, matching(r'^\d{3,5}$'), 'pattern', hint_required=True),
        Detector(
            'us_state',
            'none',
            ('state', 'st', 'state_code', 'state_abbr', 'province'),
            {'provider': 'state_abbr'},
            lambda values: pc.is_in(values, value_set=STATE_CODES),
        ),
        # Split by the part of a name the column holds, so a first-name column draws first names. A bare `name` hints
        # none of them, because a product, a company or a file has one too.
        person_name(('first_name', 'middle_name', 'given_name', 'firstname', 'fname'), 'first_name'),
        person_name(('last_name', 'family_name', 'surname', 'lastname', 'lname'), 'last_name'),
        person_name(
            ('full_name', 'fullname', 'person_name', 'patient_name', 'member_name', 'customer_name', 'contact_name', 'employee_name'),
            'name',
        ),
        Detector(
            'free_text',
            'sensitive',
            ('comment', 'comments', 'note', 'notes', 'description', 'remarks', 'narrative', 'message', 'memo', 'feedback', 'body'),
            {'pattern': r'[A-Z][a-z]{1,9}(?: [a-z]{1,9}){3,11}\.'},
            free_text,
        ),
    ),
)
