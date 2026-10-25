"""The healthcare pack: the US identifiers and code sets a medical claim holds, which `policy: healthcare` enables.

No code set ships. An ICD-10-CM, HCPCS Level II or CPT code is typed by its shape and a name hint, and generated in that
shape. An ICD-10-CM, HCPCS Level II or NDC code is also checked against the list of its name, where the caller supplies
one. Every pattern here is fixed, and comes from no data.
"""

import collections
import functools

import pyarrow as pa
import pyarrow.compute as pc

from doppeldata import _core
from doppeldata.profiling.detector_packs import Detector
from doppeldata.profiling.detector_packs import DetectorPack
from doppeldata.profiling.detector_packs import ListValidator
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
ICD10CM_HINTS = ('icd', 'icd_code', 'icd10', 'icd10_code', 'icd10cm', 'diagnosis', 'diagnosis_code', 'diag', 'diag_code', 'dx', 'dx_code')
HCPCS_LEVEL2_HINTS = ('hcpcs', 'hcpcs_code', 'procedure', 'procedure_code', 'proc', 'proc_code', 'px', 'px_code')
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


def coded(shape: str) -> ListValidator:
    """Whether each value matches the RE2 `shape` and, where a list is supplied, is one of its codes, both read undotted."""

    def validate(values: pa.Array, codes: pa.Array | None) -> pa.BooleanArray:
        matched = pc.match_substring_regex(values, shape)
        if codes is None:
            return matched
        return pc.and_(matched, pc.is_in(undotted(values), value_set=undotted(codes).unique()))

    return validate


def undotted(codes: pa.Array) -> pa.Array:
    """Each code without its dots, in capitals, as ICD-10-CM's order file writes it."""
    return pc.utf8_upper(pc.replace_substring(codes, '.', ''))


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
        # Most short words pass ICD-10-CM's shape, and a product code HCPCS's, so each is typed only where the name hints it.
        Detector(
            'icd10cm',
            'sensitive',
            ICD10CM_HINTS,
            {'provider': ICD10CM},
            coded(ICD10CM_SHAPE),
            'pattern',
            hint_required=True,
            code_list=ICD10CM,
        ),
        Detector(
            'hcpcs_level2',
            'sensitive',
            HCPCS_LEVEL2_HINTS,
            {'provider': HCPCS_LEVEL2},
            coded(HCPCS_LEVEL2_SHAPE),
            'pattern',
            hint_required=True,
            glue_type='USA_HCPCS_CODE',
            code_list=HCPCS_LEVEL2,
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
