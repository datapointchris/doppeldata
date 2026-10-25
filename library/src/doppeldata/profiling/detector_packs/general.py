"""The general pack: identifiers detectable by their format, and the types a column name hints and its values confirm.

Every pattern here is fixed, and comes from no data. Each type's name hints and generator hint are in
`doppeldata.name_hinted_types`, which generating from column names reads with no pyarrow installed.
"""

import ipaddress
import urllib.parse
from collections.abc import Callable

import pyarrow as pa
import pyarrow.compute as pc

from doppeldata import name_hinted_types as hinted
from doppeldata.name_hinted_types import HintedType
from doppeldata.profiling.detector_packs import Detector
from doppeldata.profiling.detector_packs import DetectorPack
from doppeldata.profiling.detector_packs import ListValidator
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


def typed(
    named: HintedType,
    sensitivity: str,
    validate: Validator | ListValidator | None,
    method: str = 'validator',
    *,
    hint_required: bool = False,
    glue_type: str | None = None,
) -> Detector:
    """A detector of `named`'s type, hinted and drawn as it names."""
    return Detector(named.type, sensitivity, named.name_hints, named.generator_hint, validate, method, hint_required, glue_type)


def person_name(named: HintedType) -> Detector:
    """A person's name, or one part of it. Letters alone pass, so only a column whose name hints it takes the type."""
    return typed(named, 'identifier', matching(PERSON_NAME), 'pattern', hint_required=True, glue_type='PERSON_NAME')


EMAIL = (
    r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$"
)
# E.164, or a NANP number with an optional country code. A NANP area code and exchange never start with 0 or 1, which
# keeps 10-digit identifiers such as NPIs out.
PHONE = r'^\+[1-9]\d{6,14}$|^(?:\+?1[-. ]?)?(?:\([2-9]\d\d\)|[2-9]\d\d)[-. ]?[2-9]\d\d[-. ]?\d{4}$'
# Visa; Mastercard's 51-55 and 2221-2720; American Express; Diners Club; Discover and UnionPay; JCB.
CARD_IIN = r'^(?:4|5[1-5]|2(?:22[1-9]|2[3-9]\d|[3-6]\d\d|7[01]\d|720)|3[47]|3(?:0[0-5]|[689])|6(?:011|4[4-9]|5|2)|35(?:2[89]|[3-8]\d))'
# RFC 9562: versions 1 to 8, and the variant's leading bits 10.
UUID = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$'
MAC = r'^[0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2}){5}$|^[0-9A-Fa-f]{2}(?:-[0-9A-Fa-f]{2}){5}$|^[0-9A-Fa-f]{4}(?:\.[0-9A-Fa-f]{4}){2}$'
# Letters, and the spaces, apostrophes, periods and hyphens between them, as in Mary-Ann O'Neil Jr.
PERSON_NAME = r"^\pL[\pL' .-]*$"
# The 50 states, DC, the five inhabited territories, and the three armed forces codes, two letters each. They are
# run together, because a spell checker reads North Dakota's on its own as a misspelled "and".
USPS_STATES = 'ALAKAZARCACOCTDEFLGAHIIDILINIAKSKYLAMEMDMAMIMNMSMOMTNENVNHNJNMNYNCNDOHOKORPARISCSDTNTXUTVTVAWAWVWIWYDCPRVIGUASMPAAAEAP'
STATE_CODES = pa.array([USPS_STATES[at : at + 2] for at in range(0, len(USPS_STATES), 2)])

PACK = DetectorPack(
    'general',
    (
        typed(hinted.EMAIL, 'identifier', matching(EMAIL), 'pattern', glue_type='EMAIL'),
        typed(hinted.PHONE_NUMBER, 'identifier', matching(PHONE), 'pattern', glue_type='PHONE_NUMBER'),
        typed(hinted.CREDIT_CARD, 'identifier', card_number, 'pattern', glue_type='CREDIT_CARD'),
        typed(hinted.US_SSN, 'identifier', matching(f'^{hinted.SSN}$'), 'pattern', glue_type='USA_SSN'),
        # Most 9-digit numbers pass an SSN's ranges, so an SSN without hyphens, or an integer one that lost its leading
        # zeros, is typed only where the column's name hints it.
        typed(
            hinted.US_SSN_UNDASHED,
            'identifier',
            padded_matching(9, f'^{hinted.SSN_UNDASHED}$'),
            'pattern',
            hint_required=True,
            glue_type='USA_SSN',
        ),
        typed(hinted.UUID, 'identifier', matching(UUID), 'pattern'),
        typed(hinted.IPV4_ADDRESS, 'identifier', refined(r'^\d{1,3}(?:\.\d{1,3}){3}$', ipv4), glue_type='IP_ADDRESS'),
        typed(hinted.IPV6_ADDRESS, 'identifier', refined(r'^[0-9A-Fa-f.]*:[0-9A-Fa-f:.]*:[0-9A-Fa-f:.]*$', ipv6), glue_type='IP_ADDRESS'),
        typed(hinted.MAC_ADDRESS, 'identifier', matching(MAC), 'pattern', glue_type='MAC_ADDRESS'),
        typed(hinted.URL, 'quasi_identifier', refined(r'^[A-Za-z][A-Za-z0-9+.-]*://[^\s/?#]+\S*$', url)),
        # A pattern, because the fake crate's post_code draws ZIP+4 codes in half its values.
        typed(hinted.US_ZIP, 'quasi_identifier', matching(r'^\d{5}(?:-\d{4})?$'), 'pattern'),
        # A ZIP code below 10000 stored as an integer loses its leading zeros, and the lowest, 00501, keeps three digits.
        typed(hinted.US_ZIP, 'quasi_identifier', matching(r'^\d{3,5}$'), 'pattern', hint_required=True),
        typed(hinted.US_STATE, 'none', lambda values: pc.is_in(values, value_set=STATE_CODES)),
        person_name(hinted.FIRST_NAME),
        person_name(hinted.LAST_NAME),
        person_name(hinted.FULL_NAME),
        typed(hinted.FREE_TEXT, 'sensitive', free_text),
    ),
)
