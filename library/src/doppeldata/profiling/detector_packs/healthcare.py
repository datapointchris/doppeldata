"""The healthcare pack: the US identifiers and code sets a medical claim holds, which `policy: healthcare` enables.

An ICD-10-CM or HCPCS Level II code is checked against the code list the engine ships, and generated from it. CPT is
copyrighted and the NDC directory is large, so neither ships. A CPT code is typed by shape and name hint alone, and an
NDC by those and the `ndc` list, where the caller supplies one. Every pattern here is fixed, and comes from no data.
"""

import collections
import functools
from functools import cache

import pyarrow as pa
import pyarrow.compute as pc

from doppeldata import _core
from doppeldata.profiling.detector_packs import Detector
from doppeldata.profiling.detector_packs import DetectorPack
from doppeldata.profiling.detector_packs import ListValidator
from doppeldata.profiling.detector_packs import Validator
from doppeldata.profiling.detector_packs.general import matching

ICD10CM = 'icd10cm'
HCPCS_LEVEL2 = 'hcpcs_level2'
# The letters an MBI holds: A to Z but S, L, O, I, B and Z, which read as digits.
MBI_LETTER = '[AC-HJKMNP-RT-Y]'
MBI_EITHER = '[AC-HJKMNP-RT-Y0-9]'
MBI = f'[1-9]{MBI_LETTER}{MBI_EITHER}[0-9]{MBI_LETTER}{MBI_EITHER}[0-9]{MBI_LETTER}{{2}}[0-9]{{2}}'
MBI_DASHED = f'[1-9]{MBI_LETTER}{MBI_EITHER}[0-9]-?{MBI_LETTER}{MBI_EITHER}[0-9]-?{MBI_LETTER}{{2}}[0-9]{{2}}'
# The FDA's three 10-digit segmentations, and HIPAA's 11-digit 5-4-2.
NDC_SEGMENTED = r'^(?:\d{4}-\d{4}-\d{2}|\d{5}-\d{3}-\d{2}|\d{5}-\d{4}-\d{1,2})$'
NDC_SEGMENTS = r'^(?P<labeler>\d{4,5})-(?P<product>\d{3,4})-(?P<package>\d{1,2})$'
# Each segment's digits in HIPAA's 5-4-2.
NDC_WIDTHS = (('labeler', 5), ('product', 4), ('package', 2))
NDC_LIST = 'ndc'
# A category is a letter and two letters or digits: a category's second character may be a letter.
ICD10CM_SHAPE = r'^[A-Za-z][0-9A-Za-z][0-9A-Za-z](?:\.?[0-9A-Za-z]{1,4})?$'
HCPCS_LEVEL2_SHAPE = r'^[A-Va-v]\d{4}$'
# A registrant type, the first letter of the registrant's name or a 9, six digits, then the check digit.
DEA_NUMBER = r'^[A-Z][A-Z9]\d{7}$'
NPI_HINTS = ('npi', 'national_provider_identifier', 'national_provider_id')
NDC_HINTS = ('ndc', 'national_drug_code', 'drug_code')
CPT_HINTS = ('cpt', 'cpt_code', 'procedure_code', 'proc_code')
MEMBER_HINTS = (
    'member_id',
    'member_number',
    'subscriber_id',
    'subscriber_number',
    'beneficiary_id',
    'beneficiary_number',
    'health_plan_id',
    'insurance_id',
    'policy_number',
)
MEDICAL_RECORD_HINTS = (
    'mrn',
    'medical_record_number',
    'medical_record_id',
    'medical_record',
    'chart_number',
    'patient_id',
    'patient_number',
)
CLAIM_HINTS = ('claim', 'claim_id', 'claim_number', 'claim_no')
DIGIT_SHAPE = '9'


@cache
def code_list(name: str) -> pa.Array:
    """The codes of the list the engine ships as `name`, each without its dot, read from the extension once a process."""
    shipped = _core.code_list(name)
    if shipped is None:
        raise ValueError(f'the engine ships no code list named {name!r}')
    return pa.array(shipped[1], pa.string())


def listed(shape: str, codes: str) -> Validator:
    """Whether each value matches the RE2 `shape` and, without its dots and in capitals, is a code of the list `codes`."""

    def validate(values: pa.Array) -> pa.BooleanArray:
        bare = pc.utf8_upper(pc.replace_substring(values, '.', ''))
        return pc.and_(pc.match_substring_regex(values, shape), pc.is_in(bare, value_set=code_list(codes)))

    return validate


def ndc_matching(shape: str) -> ListValidator:
    """Whether each value matches the RE2 `shape` and, where a list is supplied, names a package code the list holds."""

    def validate(values: pa.Array, codes: pa.Array | None) -> pa.BooleanArray:
        matched = pc.match_substring_regex(values, shape)
        if codes is None or not pc.any(matched).as_py():
            return matched
        known = pa.concat_arrays(ndc_readings(codes)).unique().drop_null()
        held = [pc.is_in(reading, value_set=known) for reading in ndc_readings(values)]
        return pc.and_(matched, functools.reduce(pc.or_kleene, held).fill_null(False))

    return validate


def ndc_readings(values: pa.Array) -> list[pa.Array]:
    """Each value as HIPAA's eleven digits, 5-4-2, one array a reading, null where a value has no such reading.

    A segmented code's segments are padded with zeros, and eleven digits are kept as written. Ten digits without
    hyphens have one reading for each of the FDA's segmentations, 4-4-2, 5-3-2 and 5-4-1.
    """
    segments = pc.extract_regex(values, NDC_SEGMENTS)
    padded = [pc.utf8_lpad(pc.struct_field(segments, name), width, '0') for name, width in NDC_WIDTHS]
    segmented = pc.if_else(pc.is_valid(segments), pc.binary_join_element_wise(*padded, ''), pa.scalar(None, pa.string()))
    eleven = pc.if_else(pc.match_substring_regex(values, r'^\d{11}$'), values, pa.scalar(None, pa.string()))
    ten = pc.if_else(pc.match_substring_regex(values, r'^\d{10}$'), values, pa.scalar(None, pa.string()))
    readings = [pc.coalesce(segmented, eleven)]
    for cut in (0, 5, 9):
        readings.append(pc.binary_join_element_wise(pc.utf8_slice_codeunits(ten, 0, cut), pc.utf8_slice_codeunits(ten, cut), '0'))
    return readings


def stable_shape(values: pa.Array) -> pa.BooleanArray:
    """Whether each value has the most common shape of the values, where that shape holds a digit, as an assigned number's does."""
    shapes = [_core.shape_of(value) for value in values.to_pylist()]
    dominant = collections.Counter(shapes).most_common(1)[0][0] if shapes else ''
    if DIGIT_SHAPE not in dominant:
        return pa.repeat(False, len(values))
    return pa.array([shape == dominant for shape in shapes], pa.bool_())


def assigned_number(kind: str, name_hints: tuple[str, ...], glue_type: str | None = None) -> Detector:
    """A number a health plan or provider assigns, which has no published format, so only its name and a stable shape type it."""
    return Detector(kind, 'identifier', name_hints, {}, stable_shape, hint_required=True, glue_type=glue_type)


PACK = DetectorPack(
    'healthcare',
    (
        Detector(
            'npi',
            'identifier',
            NPI_HINTS,
            {'pattern': r'[12]\d{9}', 'check_digit': 'npi'},
            matching(r'^[12]\d{9}$'),
            'pattern',
            glue_type='USA_NATIONAL_PROVIDER_IDENTIFIER',
        ),
        Detector(
            'mbi',
            'identifier',
            ('mbi', 'medicare_beneficiary_identifier', 'medicare_beneficiary_id', 'medicare_id', 'medicare_number'),
            {'pattern': MBI},
            matching(f'^{MBI_DASHED}$'),
            'pattern',
            glue_type='USA_MEDICARE_BENEFICIARY_IDENTIFIER',
        ),
        Detector(
            'ndc',
            'sensitive',
            NDC_HINTS,
            {'pattern': r'\d{5}-\d{4}-\d{2}'},
            ndc_matching(NDC_SEGMENTED),
            'pattern',
            glue_type='USA_NATIONAL_DRUG_CODE',
            code_list=NDC_LIST,
        ),
        # Most 10- and 11-digit numbers would pass, so an NDC without hyphens is typed only where the name hints it.
        Detector(
            'ndc',
            'sensitive',
            NDC_HINTS,
            {'pattern': r'\d{11}'},
            ndc_matching(r'^\d{10,11}$'),
            'pattern',
            hint_required=True,
            glue_type='USA_NATIONAL_DRUG_CODE',
            code_list=NDC_LIST,
        ),
        Detector(
            'icd10cm',
            'sensitive',
            ('icd', 'diagnosis', 'diag', 'dx'),
            {'provider': ICD10CM},
            listed(ICD10CM_SHAPE, ICD10CM),
        ),
        Detector(
            'hcpcs_level2',
            'sensitive',
            ('hcpcs', 'procedure', 'proc', 'px'),
            {'provider': HCPCS_LEVEL2},
            listed(HCPCS_LEVEL2_SHAPE, HCPCS_LEVEL2),
            glue_type='USA_HCPCS_CODE',
        ),
        # Five digits is a ZIP code's shape too, so a CPT code is typed only where the name hints it.
        Detector(
            'cpt',
            'sensitive',
            CPT_HINTS,
            {'pattern': r'\d{5}'},
            matching(r'^\d{4}[0-9FTU]$'),
            'pattern',
            hint_required=True,
            glue_type='USA_CPT_CODE',
        ),
        Detector(
            'dea_number',
            'identifier',
            ('dea', 'dea_number', 'dea_no', 'dea_registration'),
            {'pattern': r'[ABFGM][A-Z]\d{7}', 'check_digit': 'dea'},
            matching(DEA_NUMBER),
            'pattern',
            glue_type='USA_DEA_NUMBER',
        ),
        assigned_number('health_plan_beneficiary_number', MEMBER_HINTS),
        assigned_number('medical_record_number', MEDICAL_RECORD_HINTS),
        assigned_number('claim_number', CLAIM_HINTS, 'USA_HEALTH_INSURANCE_CLAIM_NUMBER'),
    ),
)
