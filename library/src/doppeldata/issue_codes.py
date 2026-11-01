"""Every code doppeldata refuses with, each declared once, and the `Issue` naming one reason a call or a file was refused.

A code is `E-<AREA>-<NNN>` and keeps its meaning across releases. The engine and the writers declare theirs in Rust, and
each is bound here by its static's name, so the library raises the code the engine would for the same fault. Its exit
status is the one a command ends on where it is the first issue, as `EXIT_STATUSES` names them.
"""

import dataclasses as dc
import difflib
import re
from collections.abc import Sequence
from typing import Any

from doppeldata import _core

AREAS = ('PROFILE', 'SCENARIO', 'POLICY', 'DATA', 'WRITE', 'IMPORT', 'EXPORT', 'IO', 'USAGE')
CODE_FORM = re.compile(r'E-(?P<area>[A-Z]+)-\d{3}')
# The only statuses a code ends a command on. docs/issue-codes.md says which one each kind of refusal takes.
EXIT_STATUSES = {
    1: 'the work was attempted and failed: a write stopped, the data changed while it was read, or two profiles drifted',
    2: 'the call must change, and nothing was written',
    3: 'a check found what a person must act on, or stopped before it finished',
    4: 'a store or service never answered',
}
CLOSEST_SHOWN = 3


@dc.dataclass(frozen=True)
class IssueCode:
    """One code: what raises it, the exit status a command ends on, and the next thing to try. A hint starting
    `doppeldata` is a command that runs as written."""

    code: str
    raised_when: str
    exit_code: int
    hint: str | None

    @property
    def area(self) -> str:
        return self.code.split('-')[1]

    def to_json(self) -> dict[str, Any]:
        return {'code': self.code, 'area': self.area, 'raised_when': self.raised_when, 'exit_code': self.exit_code, 'hint': self.hint}


REGISTERED: dict[str, IssueCode] = {}


def declare(code: str, raised_when: str, *, exit_code: int, hint: str | None = None) -> str:
    """Register `code` and return it. A code out of form, in no area, or declared twice raises TypeError at import."""
    form = CODE_FORM.fullmatch(code)
    if form is None or form['area'] not in AREAS:
        raise TypeError(f'{code} is not E-<AREA>-<NNN> in one of the areas {", ".join(AREAS)}')
    if code in REGISTERED:
        raise TypeError(f'{code} is declared twice: {REGISTERED[code].raised_when!r} and {raised_when!r}')
    if exit_code not in EXIT_STATUSES:
        raise TypeError(f'{code} exits {exit_code}, and a command exits {", ".join(map(str, EXIT_STATUSES))}')
    REGISTERED[code] = IssueCode(code, raised_when, exit_code, hint)
    return code


ENGINE_CODES = {
    name: declare(code, raised_when, exit_code=exit_code, hint=hint) for name, code, raised_when, hint, exit_code in _core.issue_codes()
}


def issue_code(code: str) -> IssueCode:
    return REGISTERED[code]


def registered() -> list[IssueCode]:
    """Every code, by area in `AREAS`' order, then by number."""
    return sorted(REGISTERED.values(), key=lambda declared: (AREAS.index(declared.area), declared.code))


@dc.dataclass(frozen=True)
class Issue:
    """One reason a call or a file was refused. An unregistered `code` raises ValueError, and the code's hint fills `hint`.

    `field` is an RFC 6901 JSON Pointer into the document refused, `''` for its root, or the argument or input refused,
    and None where the refusal names neither. `value` is what was given there. `valid` lists what would have been taken,
    and `closest` the nearest of it to `value`, nearest first, filled from `valid` where the raiser gives none.
    """

    code: str
    field: str | None
    message: str
    value: Any = None
    valid: tuple[str, ...] = ()
    closest: tuple[str, ...] = ()
    hint: str | None = None

    def __post_init__(self) -> None:
        declared = REGISTERED.get(self.code)
        if declared is None:
            raise ValueError(f'{self.code} is no registered issue code')
        if self.hint is None and declared.hint is not None:
            object.__setattr__(self, 'hint', declared.hint)
        object.__setattr__(self, 'valid', tuple(self.valid))
        if not self.closest and self.valid and isinstance(self.value, str):
            object.__setattr__(self, 'closest', tuple(difflib.get_close_matches(self.value, self.valid, n=CLOSEST_SHOWN)))

    def to_json(self) -> dict[str, Any]:
        """The issue as one JSON object with every key present, null or empty where it names nothing."""
        value = self.value if isinstance(self.value, str | int | float | bool | list | dict | None) else str(self.value)
        return {
            'code': self.code,
            'message': self.message,
            'field': self.field,
            'valid': list(self.valid),
            'hint': self.hint,
            'value': value,
            'closest': list(self.closest),
        }


def issue_table(issues: Sequence[Issue]) -> list[str]:
    """`issues` as the rows of a GitHub Markdown table, in the order given, each with its hint where it has one."""
    if not issues:
        return []
    lines = ['| Field | Code | Issue | Hint |', '| --- | --- | --- | --- |']
    for issue in issues:
        field = f'`{issue.field}`' if issue.field else ''
        closest = f'; the closest {"is" if len(issue.closest) == 1 else "are"} {", ".join(issue.closest)}' if issue.closest else ''
        cells = (field, issue.code, issue.message + closest, issue.hint or '')
        lines.append('| ' + ' | '.join(cell.replace('|', '\\|').replace('\n', ' ') for cell in cells) + ' |')
    return [*lines, '']


@dc.dataclass(frozen=True)
class DocumentCodes:
    """The codes one kind of document is refused with where it does not parse, or breaks its JSON Schema."""

    not_a_mapping: str
    not_parseable: str
    not_finite: str
    integer_too_large: str
    required: str
    unknown_field: str
    not_one_of: str
    wrong_type: str
    out_of_range: str
    malformed: str
    other: str

    def of_validator(self, validator: str) -> str:
        """The code of a schema error, by the JSON Schema keyword that raised it."""
        by_keyword = {
            'required': self.required,
            'dependentRequired': self.required,
            'additionalProperties': self.unknown_field,
            'unevaluatedProperties': self.unknown_field,
            'enum': self.not_one_of,
            'const': self.not_one_of,
            'type': self.wrong_type,
            'pattern': self.malformed,
            'format': self.malformed,
        }
        for keyword in ('minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum', 'multipleOf', 'minItems', 'maxItems'):
            by_keyword[keyword] = self.out_of_range
        for keyword in ('minLength', 'maxLength', 'minProperties', 'maxProperties'):
            by_keyword[keyword] = self.out_of_range
        return by_keyword.get(validator, self.other)


def engine(name: str) -> str:
    """The code the engine's or the writers' static `name` declares."""
    return ENGINE_CODES[name]


PROFILE_DOCUMENT = DocumentCodes(
    not_a_mapping=declare('E-PROFILE-001', 'a profile is not a mapping of sections', exit_code=2),
    not_parseable=declare('E-PROFILE-002', 'a profile file does not parse as YAML or JSON', exit_code=2),
    not_finite=declare('E-PROFILE-003', 'a number in a profile is not finite, which canonical JSON cannot write', exit_code=2),
    integer_too_large=declare(
        'E-PROFILE-004', 'an integer in a profile is beyond ±2^53, the largest canonical JSON writes exactly', exit_code=2
    ),
    required=declare('E-PROFILE-005', 'a profile section lacks a field it requires', exit_code=2),
    unknown_field=declare(
        'E-PROFILE-006',
        'a profile section holds a key it does not declare',
        exit_code=2,
        hint='doppeldata schemas show profile prints every field',
    ),
    not_one_of=declare('E-PROFILE-007', 'a profile field holds none of the values it takes', exit_code=2),
    wrong_type=declare('E-PROFILE-008', 'a profile field holds a value of another type than it takes', exit_code=2),
    out_of_range=declare('E-PROFILE-009', 'a profile number, list, text or section is outside the bounds its field takes', exit_code=2),
    malformed=declare('E-PROFILE-010', 'a profile text field does not match the pattern or format it takes', exit_code=2),
    other=declare('E-PROFILE-011', 'a profile breaks a rule of its schema no other code names', exit_code=2),
)
SCENARIO_DOCUMENT = DocumentCodes(
    not_a_mapping=declare('E-SCENARIO-001', 'a scenario is not a mapping of sections', exit_code=2),
    not_parseable=declare('E-SCENARIO-002', 'a scenario file does not parse as YAML or JSON', exit_code=2),
    not_finite=declare('E-SCENARIO-003', 'a number in a scenario is not finite, which canonical JSON cannot write', exit_code=2),
    integer_too_large=declare(
        'E-SCENARIO-004', 'an integer in a scenario is beyond ±2^53, the largest canonical JSON writes exactly', exit_code=2
    ),
    required=declare('E-SCENARIO-005', 'a scenario section or perturbation lacks a field it requires', exit_code=2),
    unknown_field=declare(
        'E-SCENARIO-006',
        'a scenario section or perturbation holds a key it does not declare, such as a parameter its op does not take',
        exit_code=2,
        hint='doppeldata scenario show OP lists the parameters an op takes',
    ),
    not_one_of=declare('E-SCENARIO-007', 'a scenario field holds none of the values it takes', exit_code=2),
    wrong_type=declare('E-SCENARIO-008', 'a scenario field holds a value of another type than it takes', exit_code=2),
    out_of_range=declare('E-SCENARIO-009', 'a scenario number, list, text or section is outside the bounds its field takes', exit_code=2),
    malformed=declare('E-SCENARIO-010', 'a scenario text field does not match the pattern or format it takes', exit_code=2),
    other=declare('E-SCENARIO-011', 'a scenario breaks a rule of its schema no other code names', exit_code=2),
)

BUNDLE_GIVEN = engine('BUNDLE_GIVEN')
TABLE_NOT_BUNDLE = engine('TABLE_NOT_BUNDLE')
NO_COLUMNS = engine('NO_COLUMNS')
DUPLICATE_COLUMN = engine('DUPLICATE_COLUMN')
INVALID_PATTERN = engine('INVALID_PATTERN')
INVALID_QUANTILES = engine('INVALID_QUANTILES')
NO_ROWS = engine('NO_ROWS')
TABLE_FILE_MISSING = declare('E-PROFILE-012', 'a bundle names a table file that does not exist', exit_code=2)
TABLE_NAME_MISMATCH = declare('E-PROFILE-013', "a bundle's table is named otherwise than the bundle names it", exit_code=2)
FRACTIONS_NOT_SUMMING_TO_ONE = declare('E-PROFILE-014', 'a set of fractions meant to sum to 1 does not', exit_code=2)
FRACTIONS_OVER_ONE = declare('E-PROFILE-015', "a row's fractions sum past 1", exit_code=2)
HISTOGRAM_BINS_MISMATCH = declare('E-PROFILE-016', "a histogram's bin_rows are not one fewer than its edges", exit_code=2)
EXTREMES_REVERSED = declare('E-PROFILE-017', "a column's low extreme is above its high", exit_code=2)
INVALID_SHORT_ROWS = declare('E-PROFILE-018', "a short profile's rows is not a count of rows, 0 or more", exit_code=2)
INVALID_COLUMNS = declare(
    'E-PROFILE-019', 'the columns given are not a mapping, or a list, of column names to what each holds', exit_code=2
)
INVALID_COLUMN = declare(
    'E-PROFILE-020', 'a short or inline column is not a fake, a range or a list of values a column can hold', exit_code=2
)
NO_FAKE_FOR_NAME = declare(
    'E-PROFILE-021',
    'a column given no fake has a name that hints none',
    exit_code=2,
    hint='doppeldata capabilities show lists every fake',
)
NOT_A_DATA_FILE = declare(
    'E-PROFILE-022', 'a file named as a profile is neither a profile nor a Parquet or Arrow IPC file holding one', exit_code=2
)
NO_EMBEDDED_PROFILE = declare(
    'E-PROFILE-023',
    'a Parquet or Arrow IPC file named as a profile holds none in its metadata',
    exit_code=2,
    hint='write it with doppeldata data generate --embed-profile, or embed_profile=True',
)
EMBEDDED_PROFILE_UNREADABLE = declare(
    'E-PROFILE-024', "a data file's embedded profile, or a count its write recorded beside it, does not read", exit_code=2
)
EMBEDDED_SHA256_MISMATCH = declare(
    'E-PROFILE-025', "a data file's embedded profile or scenario is not the one its write recorded the SHA-256 of", exit_code=2
)
DATASET_IDENTITY_CHANGED = declare(
    'E-PROFILE-026', "this release plans another dataset from a data file's embedded profile than the one the file names", exit_code=2
)
PROFILES_DRIFTED = declare('E-PROFILE-027', 'the later of two profiles drifted from the earlier on a metric or a field', exit_code=1)
PROFILE_NOT_FOUND = declare(
    'E-PROFILE-028',
    'a profile named is neither a file nor an example doppeldata ships',
    exit_code=2,
    hint='doppeldata profile list names every example',
)
PROFILE_CHECK_FAILED = declare('E-PROFILE-029', 'a profile file breaks its schema or its privacy policy', exit_code=3)
PROFILE_FAILS_CHECK = declare(
    'E-PROFILE-030', 'a profile whose literals a write would embed, or an export would carry, fails profile check', exit_code=2
)
PROFILE_DIFF_STOPPED = declare('E-PROFILE-031', 'profile diff stopped before it compared the two profiles', exit_code=3)

UNKNOWN_SCENARIO_COLUMN = engine('UNKNOWN_SCENARIO_COLUMN')
INVALID_PARAMETER = engine('INVALID_PARAMETER')
DUPLICATE_PERTURBATION_ID = engine('DUPLICATE_PERTURBATION_ID')
OP_NEEDS_ONE_PROCESS = engine('OP_NEEDS_ONE_PROCESS')
UNKNOWN_TABLE = engine('UNKNOWN_TABLE')
UNKNOWN_METRIC = engine('UNKNOWN_METRIC')
UNKNOWN_OP = declare(
    'E-SCENARIO-012', 'a perturbation names an op doppeldata does not apply', exit_code=2, hint='doppeldata scenario list names every op'
)
EXPECTATION_WITHOUT_PERTURBATION = declare(
    'E-SCENARIO-013', 'an expectation measures what no perturbation of its scenario changes', exit_code=2
)
OP_NEEDS_FILES = declare('E-SCENARIO-014', 'an op shapes the files a write makes, and the call writes none, or one', exit_code=2)
DATA_HAS_SCENARIO = declare(
    'E-SCENARIO-015',
    'data a scenario bent was checked with no scenario',
    exit_code=2,
    hint='check it under the scenario its manifest names',
)
SCENARIO_DIFFERS_FROM_DATA = declare(
    'E-SCENARIO-016', 'a check names another scenario than the one its data was written under', exit_code=2
)
UNCHECKABLE_FAULTS = declare(
    'E-SCENARIO-017',
    'the data holds malformed_rows faults a check cannot read back as one table',
    exit_code=2,
    hint='check data written without those fault kinds',
)
SCENARIO_NOT_FOUND = declare(
    'E-SCENARIO-018',
    'a scenario named is neither a file nor an example doppeldata ships',
    exit_code=2,
    hint='doppeldata scenario list names every example',
)
SCENARIO_CHECK_FAILED = declare(
    'E-SCENARIO-019', 'a scenario breaks its schema, or asks of its profile or its files what they cannot give', exit_code=3
)

UNAUDITED_LITERAL = declare('E-POLICY-001', 'a literal value the policy holds to k_min has no audit entry', exit_code=3)
STALE_AUDIT_ENTRY = declare('E-POLICY-002', 'an audit entry names a literal the profile does not hold', exit_code=3)
ROWS_NOT_STATED = declare('E-POLICY-003', "a literal's fractions rest on no stated row count, so k_min cannot be checked", exit_code=3)
KEYS_UNDER_K_MIN = declare('E-POLICY-004', "the dataset's key columns let a group of fewer than k_min rows be told apart", exit_code=3)
AUDIT_RULE_MISMATCH = declare('E-POLICY-005', "an audit entry names another rule than its field's", exit_code=3)
AUDIT_VALUES_MISMATCH = declare('E-POLICY-006', 'an audit entry lists other values than its field holds', exit_code=3)
BELOW_K_MIN = declare('E-POLICY-007', 'a literal stands for fewer rows than k_min', exit_code=3)
EXTREMES_NOT_TRIMMED = declare('E-POLICY-008', "a column's extremes are stored by another method than trimmed_k", exit_code=3)
PATTERN_NOT_GENERALIZED = declare('E-POLICY-009', 'a stored pattern names characters the policy generalizes', exit_code=3)
FIT_WITHOUT_TRIMMED_EXTREMES = declare('E-POLICY-010', 'a column stores a fit, and its extremes are not trimmed_k', exit_code=3)
SEQUENCE_WITHOUT_TRIMMED_EXTREMES = declare('E-POLICY-011', 'a column stores a sequence, and its extremes are not trimmed_k', exit_code=3)
SEQUENCE_START_NOT_DERIVED = declare('E-POLICY-012', "a sequence's start is not derived from its trimmed extremes", exit_code=3)
IDENTIFIER_LITERAL = declare('E-POLICY-013', 'a column treated as an identifier stores a data value', exit_code=3)
FREE_TEXT_LITERAL = declare('E-POLICY-014', 'a free text column stores a value rather than its lengths', exit_code=3)
ZIP_LITERAL = declare('E-POLICY-015', 'a ZIP code column stores a value rather than its prefixes', exit_code=3)
AGE_LITERAL = declare('E-POLICY-016', 'an age column stores a value', exit_code=3)
EXTREMES_NOT_DIGIT_BOUNDS = declare('E-POLICY-017', "a column's extremes are not the digit bounds its treatment stores", exit_code=3)
ZIP_NOT_PREFIX = declare('E-POLICY-018', 'a ZIP code value is not three digits', exit_code=3)
RESTRICTED_ZIP_PREFIX = declare('E-POLICY-019', 'a ZIP prefix is on the restricted list, which stores it as 000', exit_code=3)
AGE_OVER_89 = declare('E-POLICY-020', 'an age, or a birth date as an age, is stated above 89', exit_code=3)
DATE_BELOW_YEAR = declare('E-POLICY-021', 'a date is stated more finely than its year', exit_code=3)
BIRTH_DATE_UNANCHORED = declare('E-POLICY-022', 'a profile holding a birth date states no generated.at to measure ages from', exit_code=3)
BIRTH_DATE_LAG = declare('E-POLICY-023', 'a dependence lags a date column off a birth date', exit_code=3)
DEPENDENCE_QUASI_IDENTIFIER = declare('E-POLICY-024', 'a dependence is drawn given a quasi-identifier', exit_code=3)
DEPENDENCE_COLUMN_UNLISTED = declare('E-POLICY-025', 'a dependence is drawn given a column that lists no values', exit_code=3)
DEPENDENCE_VALUE_UNLISTED = declare('E-POLICY-026', 'a dependence names a value its given column does not list', exit_code=3)
POLICY_NONE = declare('E-POLICY-027', 'a profile states policy none, which keeps every value, so no policy is checked', exit_code=3)
POLICY_NOT_CHECKABLE = declare('E-POLICY-028', 'a profile states a policy profile check does not check', exit_code=3)
UNKNOWN_POLICY = declare(
    'E-POLICY-029', 'a policy named is not one doppeldata applies', exit_code=2, hint='doppeldata capabilities show lists the policies'
)
POLICY_DP_RESERVED = declare(
    'E-POLICY-030', 'policy dp was named, which is reserved for differential privacy, and doppeldata does not apply it', exit_code=2
)
K_MIN_WITH_POLICY_NONE = declare('E-POLICY-031', 'k_min was given with policy none, which keeps every value', exit_code=2)
INVALID_K_MIN = declare('E-POLICY-032', 'k_min is under 1', exit_code=2)

DATA_NOT_NAMED = declare('E-DATA-001', 'no data was named', exit_code=2)
DATA_MISSING = declare('E-DATA-002', 'a file or directory named as data does not exist', exit_code=2)
NO_READABLE_FILE = declare('E-DATA-003', 'the data named holds no file in a format doppeldata reads', exit_code=2)
MIXED_FORMATS = declare('E-DATA-004', 'the data named holds files of several formats', exit_code=2)
SEVERAL_TABLES = declare(
    'E-DATA-005',
    'the data named groups into several tables, where one is profiled',
    exit_code=2,
    hint='profile every table with doppeldata profile create --tables',
)
TABLES_ALIKE = declare('E-DATA-006', 'two tables of the data named share a name', exit_code=2)
ENGINE_DOES_NOT_READ = declare('E-DATA-007', 'the profiling engine chosen does not read the data named, or its format', exit_code=2)
DATA_UNFINISHED = declare(
    'E-DATA-008', 'the data holds _manifest.partial.json, so the write that made it stopped or is still running', exit_code=2
)
DATA_UNREADABLE = declare('E-DATA-009', 'a file of the data could not be read as its format', exit_code=1)
FILES_DISAGREE = declare('E-DATA-010', 'two files of one table start with other headers, or split into other field counts', exit_code=1)
DATA_CHANGED = declare('E-DATA-011', 'the data changed while it was read', exit_code=1)
SPARK_PASSES_FAILED = declare('E-DATA-012', 'the Spark executors could not run the profiling passes', exit_code=1)
NOTHING_SCORED = declare('E-DATA-013', 'neither the profile nor the real data holds a column a score reads', exit_code=2)
DATA_MISSED_PROFILE = declare('E-DATA-014', 'data missed its profile on a metric, a column or an expectation', exit_code=3)
BUNDLE_MISSED_PROFILE = declare('E-DATA-015', "a bundle's data missed its profile on a table or a relationship", exit_code=3)
THRESHOLDS_UNREADABLE = declare('E-DATA-016', 'a thresholds file does not read as YAML or JSON', exit_code=2)
INVALID_THRESHOLDS = declare(
    'E-DATA-017', 'a thresholds file is not a mapping of defaults and columns, or names a metric no check measures', exit_code=2
)
THRESHOLDS_NEED_AGAINST = declare('E-DATA-018', 'thresholds set a score, and the check reads no real data to score against', exit_code=2)
THRESHOLDS_UNKNOWN_COLUMN = declare('E-DATA-019', 'thresholds name a column the source profile does not hold', exit_code=2)
CODE_LIST_UNREAD = declare('E-DATA-020', 'a code list is named that no enabled detector reads', exit_code=2)
INVALID_CODE_LIST = declare('E-DATA-021', 'a code list file is not UTF-8 text, or holds no code', exit_code=2)
DETECTOR_PACK_MISSING = declare('E-DATA-022', 'a detector pack named is not installed', exit_code=2)
DETECTOR_PACK_BROKEN = declare(
    'E-DATA-023', 'an installed detector pack does not load, clashes with another, or holds or returns what doppeldata refuses', exit_code=1
)
DATA_CHECK_STOPPED = declare('E-DATA-024', 'data check stopped before it measured the data against its profile', exit_code=3)

DESTINATION_NOT_EMPTY = engine('DESTINATION_NOT_EMPTY')
RANGES_UNRECORDED = engine('RANGES_UNRECORDED')
UNSUPPORTED_COLUMN = engine('UNSUPPORTED_COLUMN')
URL_IN_A_PATH = declare('E-WRITE-001', "a destination is a Path, which collapses an s3:// URL's //", exit_code=2)
SUFFIX_NAMES_ANOTHER_FORMAT = declare('E-WRITE-002', "a destination file's suffix names another format than the one written", exit_code=2)
ONE_FILE_HOLDS_EVERY_ROW = declare(
    'E-WRITE-003', 'a destination names one file, and the write was asked to split or partition its rows', exit_code=2
)
DESTINATION_IS_A_DIRECTORY = declare('E-WRITE-004', 'a destination named as one file is a directory', exit_code=2)
MANIFEST_RECORDS_ANOTHER_FILE = declare(
    'E-WRITE-005', 'the manifest beside a destination file records another file as its dataset', exit_code=2
)
DESTINATION_NOT_SHARED = declare(
    'E-WRITE-006', 'a Spark write names a local directory, which the executors share only under a local master', exit_code=2
)
SPARK_JOB_FAILED = declare('E-WRITE-007', 'the Spark job writing a dataset failed', exit_code=1)
ONE_FILE_FROM_EXECUTORS = declare('E-WRITE-008', 'a Spark write names one file, and the executors write a directory', exit_code=2)
BUNDLE_WRITES_A_DIRECTORY = declare('E-WRITE-009', 'a bundle write names one file, and a bundle writes a directory of tables', exit_code=2)
BUNDLE_PARTITIONS_BY_TABLE = declare(
    'E-WRITE-010', 'a bundle write was given a partitioning, and each table is partitioned as its profile records', exit_code=2
)
BUNDLE_EMBEDS_NO_PROFILE = declare('E-WRITE-011', 'a bundle write was asked to embed its profile, which no bundle file holds', exit_code=2)
OUT_UNWRITABLE = declare(
    'E-WRITE-012', 'a file named to write a profile, seed or export to lies in no directory, or cannot be written', exit_code=2
)
UNKNOWN_FORMAT = engine('UNKNOWN_FORMAT')
UNKNOWN_PARTITION_STYLE = declare(
    'E-WRITE-013',
    'a partition style named is not one doppeldata writes',
    exit_code=2,
    hint='doppeldata capabilities show lists the partition styles',
)
REGENERATES_A_DIRECTORY = declare(
    'E-WRITE-014', 'a regeneration names one file as its destination, and a regeneration writes a directory of parts', exit_code=2
)
MANIFEST_NOT_FOUND = declare('E-WRITE-015', 'a manifest named to regenerate from is not at the path or URL given', exit_code=2)
IDENTITY_MISMATCH = engine('IDENTITY_MISMATCH')
PARTIAL_MANIFEST = engine('PARTIAL_MANIFEST')
REGENERATED_FILES_DIFFER = engine('REGENERATED_FILES_DIFFER')
MANIFEST_UNREADABLE = engine('MANIFEST_UNREADABLE')

NO_CREDENTIALS = engine('NO_CREDENTIALS')
STORE_REFUSED = engine('STORE_REFUSED')
STORE = engine('STORE')
NO_ANSWER = engine('NO_ANSWER')
LOCAL_IO = engine('LOCAL_IO')
FILE_METADATA_UNREADABLE = engine('FILE_METADATA_UNREADABLE')
AWS_SETTINGS = declare('E-IO-001', 'the AWS settings name a profile the AWS config lacks, or an endpoint that is no URL', exit_code=2)
BUCKET_MISSING = declare('E-IO-002', 'a destination names a bucket the store does not hold', exit_code=2)
FILE_UNREADABLE = declare('E-IO-003', 'a local file named could not be read', exit_code=2)

SCHEMA_UNPARSEABLE = declare('E-IMPORT-001', 'a schema file is not UTF-8 text holding JSON or YAML', exit_code=2)
NOT_THE_SCHEMA_FORMAT = declare('E-IMPORT-002', 'a schema file is not the format it was imported as', exit_code=2)
SCHEMA_TABLE_NOT_NAMED = declare(
    'E-IMPORT-003', 'a schema file holds several tables, and none was named', exit_code=2, hint='name one with --table, or table='
)
SCHEMA_TABLE_UNKNOWN = declare('E-IMPORT-004', 'a schema file holds no table of the name given', exit_code=2)
SCHEMA_REFERENCE_UNRESOLVED = declare(
    'E-IMPORT-005', 'a schema refers back to itself, to nothing in the file, or to a schema outside it', exit_code=2
)
SCHEMA_UNDRAWABLE = declare(
    'E-IMPORT-006', 'a schema declares what no column of its type draws from, such as a bound written as text', exit_code=2
)
NO_DRAWABLE_COLUMN = declare('E-IMPORT-007', 'a schema holds no column of a type the engine draws', exit_code=2)
GLUE_REGION = declare('E-IMPORT-008', "the Glue Data Catalog's region is not named, or names none AWS has", exit_code=2)
GLUE_TABLE_UNKNOWN = declare('E-IMPORT-009', 'the Glue Data Catalog holds no table of the name given', exit_code=2)
DETECTED_ENTITIES_UNMATCHED = declare('E-IMPORT-010', 'the detected entities name a column the Glue table lacks', exit_code=2)
UNKNOWN_IMPORT_KIND = declare('E-IMPORT-011', 'an import names a kind of schema doppeldata does not read', exit_code=2)
IMPORT_OPTION_UNREAD = declare('E-IMPORT-012', 'an import was given an option the kind of schema it reads takes none of', exit_code=2)
GLUE_TABLE_NAME = declare('E-IMPORT-013', 'a Glue table is not named as DATABASE.TABLE', exit_code=2)
DETECTED_ENTITIES_UNREADABLE = declare(
    'E-IMPORT-014', 'the detected entities are not a JSON object of each column to a list of entity types', exit_code=2
)

UNKNOWN_EXPORT_FORMAT = declare(
    'E-EXPORT-001',
    'an export format named is not one doppeldata writes',
    exit_code=2,
    hint='doppeldata capabilities show lists the export formats',
)
EXPORT_NEEDS_ONE_TABLE = declare('E-EXPORT-002', 'an export describing one table was asked of a bundle with no table named', exit_code=2)
EXPORT_TABLE_UNKNOWN = declare('E-EXPORT-003', 'an export names a table the profile does not hold', exit_code=2)

INVALID_OVERRIDE = engine('INVALID_OVERRIDE')
COMMAND_LINE = declare(
    'E-USAGE-001', 'a command line names a command or option doppeldata lacks, or gives one what it does not take', exit_code=2
)
INVALID_ROWS = declare('E-USAGE-002', 'rows is not a count of rows the call takes', exit_code=2)
INVALID_SEED = declare('E-USAGE-003', 'a seed is outside 0 to 2^64 - 1', exit_code=2)
ROWS_AND_SCALE = declare('E-USAGE-004', 'rows was given for a bundle, or scale for one table', exit_code=2)
NOT_A_SCENARIO = declare('E-USAGE-005', 'scenario= is given neither a Scenario nor a list of them', exit_code=2)
COMPOSE_NOTHING = declare('E-USAGE-006', 'Scenario.compose was given no scenario', exit_code=2)
SAVE_NEEDS_PATH = declare('E-USAGE-007', 'a profile with no file of its own was saved with no path', exit_code=2)
INVALID_PARTITIONS = declare('E-USAGE-008', 'partitions is under 1', exit_code=2)
MANIFEST_ON_BUNDLE = declare('E-USAGE-009', 'manifest_beside= was given for a bundle, which always writes its manifests', exit_code=2)
UNSUPPORTED_PROFILE_SOURCE = declare(
    'E-USAGE-010', 'a call was given a profile source of a type it does not read, such as a number', exit_code=2
)
UNKNOWN_ENGINE = declare(
    'E-USAGE-011',
    'a profiling engine named is not one doppeldata has',
    exit_code=2,
    hint='doppeldata capabilities show lists the engines',
)
SPARK_SESSION_UNUSED = declare('E-USAGE-012', 'spark= names a session, and the engine chosen takes none', exit_code=2)
INVALID_SAMPLE = declare('E-USAGE-013', 'a sample is not a positive number of rows', exit_code=2)
INVALID_RETURN_TYPE = declare('E-USAGE-014', 'as_ names no type the synthetic fixture returns', exit_code=2)
SPARK_FIXTURE_MISSING = declare(
    'E-USAGE-015', "synthetic(as_='spark') was asked for, and no fixture named spark gives a session", exit_code=2
)
BUNDLE_DATA_MISMATCH = declare(
    'E-USAGE-016', "a bundle's check was given tables other than its own, or data of a type it does not read", exit_code=2
)
AGAINST_NEEDED = declare('E-USAGE-017', 'a check was asked to score real data, and was given none, or a bundle', exit_code=2)
HTML_UNWRITABLE = declare('E-USAGE-018', 'the side-by-side HTML names a path it cannot be written to', exit_code=2)
PACKAGES_MISSING = declare('E-USAGE-019', 'a feature imports packages doppeldata does not install', exit_code=2)
BUILDS_DIFFER = declare('E-USAGE-020', 'a Spark executor runs another doppeldata build than the driver', exit_code=2)
PYSPARK_UNSUPPORTED = declare('E-USAGE-021', 'the PySpark installed takes other arguments to a call doppeldata makes', exit_code=2)
TOO_MANY_ROWS_FOR_DRIVER = declare(
    'E-USAGE-022',
    'to_spark was given more rows than it builds on the driver',
    exit_code=2,
    hint='generate it with doppeldata.spark.generate, whose rows the executors make',
)
UNKNOWN_SCHEMA = declare(
    'E-USAGE-023', 'a schema named is not one doppeldata ships', exit_code=2, hint='doppeldata schemas list names every schema'
)
AWS_PROFILE_WITH_SPARK = declare(
    'E-USAGE-024', "an AWS profile was named for Spark profiling, which reads S3 with its own session's credentials", exit_code=2
)
PROFILE_OR_COLUMNS = declare('E-USAGE-025', 'a call was given both a profile and column names, or neither', exit_code=2)
SETTLED_BY_MANIFEST = declare(
    'E-USAGE-026',
    'a regeneration was given a value its manifest settles: the seed, rows, scale, scenario, format, writer settings or layout',
    exit_code=2,
)
