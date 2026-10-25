"""A profile checked against its schema and its privacy policy, from the file alone, as `profile check` runs it.

Each rule is recomputed from what the file states: every literal must be in `privacy.audit` under the rule its kind is
admitted by, and must meet that rule, counted as a rate times the rows the profile describes. A column the policy
withholds from must hold nothing it withholds.
"""

import dataclasses as dc
import re
from pathlib import Path
from typing import Any

from doppeldata.privacy_policy.column_treatment import CHECKED_POLICIES
from doppeldata.privacy_policy.column_treatment import DEFAULT_K_MIN
from doppeldata.privacy_policy.column_treatment import OLDEST_AGE
from doppeldata.privacy_policy.column_treatment import ColumnFacts
from doppeldata.privacy_policy.column_treatment import Policy
from doppeldata.privacy_policy.column_treatment import Treatment
from doppeldata.privacy_policy.column_treatment import column_treatment
from doppeldata.privacy_policy.column_treatment import restricted_zip_prefixes
from doppeldata.privacy_policy.literal_fields import ColumnLiterals
from doppeldata.privacy_policy.literal_fields import FieldKind
from doppeldata.privacy_policy.literal_fields import LiteralField
from doppeldata.privacy_policy.literal_fields import column_literals
from doppeldata.privacy_policy.literal_fields import faults
from doppeldata.privacy_policy.withholding import is_digit_bounds
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import PointerParts
from doppeldata.profile_format import check
from doppeldata.profile_format import pointer
from doppeldata.profile_format import read_document
from doppeldata.profile_format import to_plain

# The fields a column keeping no data value may still hold, which state its format and no value.
NO_VALUE_KEEPS = frozenset({FieldKind.SHAPES, FieldKind.PATTERN, FieldKind.MISSING_TOKENS})
FREE_TEXT_FORBIDS = frozenset({FieldKind.TOP_VALUES, FieldKind.SHAPES, FieldKind.PATTERN})
ZIP_PREFIX = re.compile(r'\d{3}')
YEAR_START = re.compile(r'\d{4}-01-01(?:[T ]00:00(?::00(?:\.0+)?)?)?(?:Z|[+-]\d\d:?\d\d)?')
YEAR_END = re.compile(r'\d{4}-12-31(?:[T ].*)?')
CHECKABLE = ', '.join(policy.value for policy in CHECKED_POLICIES)


@dc.dataclass(frozen=True)
class Violation:
    """One place a profile breaks its schema or its policy. `field` is an RFC 6901 pointer, and `rule` names what it broke."""

    code: str
    field: str
    rule: str
    detail: str


@dc.dataclass(frozen=True)
class TableCheck:
    """One table profile's check: its name, policy and `k_min` as the file states them, the literal fields it audits, and each violation."""

    table: str
    policy: str | None
    k_min: int | None
    literal_fields: int
    violations: list[Violation]


@dc.dataclass(frozen=True)
class ProfileFileCheck:
    """A profile file checked against its schema and privacy policy: the file as named, and each table's check."""

    profile: str
    tables: list[TableCheck]

    @property
    def violations(self) -> list[Violation]:
        return [violation for check in self.tables for violation in check.violations]

    def failure_message(self) -> str:
        """What `CheckFailed` says of a profile file that breaks its schema or policy."""
        count = len(self.violations)
        return f'{self.profile} breaks its schema or its privacy policy in {count} place{"" if count == 1 else "s"}'


def file_checks(path: Path) -> list[TableCheck]:
    """The check of a table profile, or of each table of a bundle, whose violations point into the file named.

    A bundle's table held in its own file is checked as that file, and its violations point into it.
    """
    try:
        document = to_plain(read_document(path))
    except InvalidProfile as error:
        return [TableCheck(path.name, None, None, 0, schema_violations(error.issues))]
    if isinstance(document, dict) and document.get('kind') == 'bundle':
        issues = check(document)
        if issues:
            return [TableCheck(str(document.get('name')), None, None, 0, schema_violations(issues))]
        checks = []
        for name, entry in document['tables'].items():
            if not isinstance(entry, str):
                checks.append(table_check(entry, ('tables', name)))
                continue
            member = path.parent / entry
            if not member.is_file():
                detail = f'{member} is not a file; expected the table profile the bundle names'
                checks.append(TableCheck(name, None, None, 0, [Violation('table-not-found', pointer(('tables', name)), 'schema', detail)]))
                continue
            checks.extend(file_checks(member))
        return checks
    return [table_check(document, ())]


def table_check(table: Any, at: PointerParts) -> TableCheck:
    """One table profile's check. `at` is where it sits in the file, which each violation's field begins with."""
    issues = check(table)
    name = str(table.get('name')) if isinstance(table, dict) else ''
    if issues:
        return TableCheck(name, None, None, 0, schema_violations(issues, at))
    privacy = table.get('privacy') or {}
    policy = Policy(privacy.get('policy') or Policy.DEFAULT)
    k_min = privacy.get('k_min') or DEFAULT_K_MIN
    if policy is Policy.NONE:
        detail = f'policy is none; expected one of {CHECKABLE}'
        return TableCheck(name, policy.value, k_min, 0, [Violation('policy-none', pointer((*at, 'privacy', 'policy')), 'policy', detail)])
    if policy not in CHECKED_POLICIES:
        detail = f'policy is {policy.value}, which doppeldata does not check yet; expected one of {CHECKABLE}'
        violation = Violation('policy-not-checkable', pointer((*at, 'privacy', 'policy')), 'policy', detail)
        return TableCheck(name, policy.value, k_min, 0, [violation])
    audit = privacy.get('audit') or []
    audited = {entry.get('field'): index for index, entry in enumerate(audit)}
    violations: list[Violation] = []
    seen: set[str] = set()
    fields = 0
    for column in column_literals(table):
        treatment = column_treatment(ColumnFacts.of(column.column), policy)
        violations += [violation_at(at, *found) for found in treatment_faults(column, treatment)]
        for field in column.fields:
            fields += 1
            refusal = refused(field, column, treatment)
            if refusal:
                violations.append(violation_at(at, *refusal))
                continue
            index = audited.get(field.pointer)
            if index is None:
                detail = f'no privacy.audit entry for the {field.kind.label} of {column.name}; expected one with field {field.pointer}'
                violations.append(violation_at(at, 'unaudited-literal', field.path, 'audited', detail))
            else:
                seen.add(field.pointer)
                violations += [violation_at(at, *found) for found in audit_faults(audit[index], index, field, column, k_min)]
            violations += [
                Violation(fault.code, pointer((*at, *fault.path)), field.kind.rule, fault.detail) for fault in faults(field, column, k_min)
            ]
    for index, entry in enumerate(audit):
        if entry.get('field') not in seen:
            named = entry.get('field')
            found = f'{named} holds no literal field' if named else 'the entry names no field'
            detail = f'{found}; expected the pointer of one this file holds'
            violations.append(violation_at(at, 'stale-audit-entry', ('privacy', 'audit', index), 'audited', detail))
    return TableCheck(name, policy.value, k_min, fields, violations)


def violation_at(at: PointerParts, code: str, path: PointerParts, rule: str, detail: str) -> Violation:
    return Violation(code, pointer((*at, *path)), rule, detail)


def schema_violations(issues: list[Any], at: PointerParts = ()) -> list[Violation]:
    return [Violation(issue.code, pointer(at) + issue.field, 'schema', issue.message) for issue in issues]


def refused(field: LiteralField, column: ColumnLiterals, treatment: Treatment) -> tuple[str, PointerParts, str, str] | None:
    """The violation of a literal the column's treatment withholds, or None where it may hold it."""
    semantic_type = (column.column.get('semantic') or {}).get('type')
    if treatment.keeps_no_value and field.kind not in NO_VALUE_KEEPS:
        sensitivity = 'an identifier' if treatment is Treatment.IDENTIFIER else 'a quasi-identifier under strict'
        typed = f'typed {semantic_type}, {sensitivity},' if semantic_type else f'{sensitivity},'
        detail = f'{column.name} is {typed} and stores its {field.kind.label}; expected no data value'
        return 'identifier-literal', field.path, treatment.value or '', detail
    if treatment is Treatment.FREE_TEXT and field.kind in FREE_TEXT_FORBIDS:
        detail = f'{column.name} is free text, and stores its {field.kind.label}; expected its length statistics only'
        return 'free-text-literal', field.path, treatment.value or '', detail
    return None


def audit_faults(
    entry: dict[str, Any], index: int, field: LiteralField, column: ColumnLiterals, k_min: int
) -> list[tuple[str, PointerParts, str, str]]:
    """Where the audit entry for a field misstates it."""
    at: PointerParts = ('privacy', 'audit', index)
    found = []
    if entry.get('rule') != field.kind.rule:
        found.append(('audit-rule-mismatch', (*at, 'rule'), 'audited', f'rule is {entry.get("rule")}; expected {field.kind.rule}'))
    if entry.get('column') != column.name:
        found.append(('audit-column-mismatch', (*at, 'column'), 'audited', f'column is {entry.get("column")}; expected {column.name}'))
    if entry.get('values') is not None and entry['values'] != field.values:
        found.append(('audit-values-mismatch', (*at, 'values'), 'audited', f'values is {entry["values"]}; the field holds {field.values}'))
    if entry.get('min_count') is not None and entry['min_count'] < k_min:
        found.append(('below-k-min', (*at, 'min_count'), field.kind.rule, f'min_count is {entry["min_count"]}; expected at least {k_min}'))
    return found


def treatment_faults(column: ColumnLiterals, treatment: Treatment) -> list[tuple[str, PointerParts, str, str]]:
    """Where a column breaks a rule of its treatment beyond the literals it withholds."""
    at, rule = column.path, treatment.value or ''
    numeric = column.column.get('numeric') or {}
    string = column.column.get('string') or {}
    found = []
    extremes = numeric.get('extremes') or {}
    if treatment.keeps_no_value and extremes.get('method') == 'digit_bounds' and not is_digit_bounds(extremes):
        bounds = f'extremes are {extremes.get("low")} and {extremes.get("high")}'
        detail = f'{bounds}; expected 0 or a power of ten, then one less than a power of ten'
        found.append(('extremes-not-digit-bounds', (*at, 'numeric', 'extremes'), rule, detail))
    if treatment is Treatment.ZIP_PREFIX:
        restricted = restricted_zip_prefixes()
        for index, entry in enumerate(string.get('top_values') or []):
            value = str(entry.get('value'))
            if not ZIP_PREFIX.fullmatch(value):
                found.append(('zip-not-prefix', (*at, 'string', 'top_values', index), rule, f'value is {value!r}; expected three digits'))
            elif value in restricted:
                detail = f'{value} is on the restricted prefix list; expected 000'
                found.append(('restricted-zip-prefix', (*at, 'string', 'top_values', index), rule, detail))
        if string.get('top_values') and not string.get('top_value_suffix'):
            detail = 'top values are prefixes and no top_value_suffix follows them; expected one, such as \\d{2}'
            found.append(('zip-without-suffix', (*at, 'string'), rule, detail))
        if string.get('pattern'):
            found.append(('zip-pattern', (*at, 'string', 'pattern'), rule, f'pattern is {string["pattern"]}; expected none'))
    if treatment is Treatment.AGE:
        for end in ('low', 'high'):
            if isinstance(extremes.get(end), int | float) and extremes[end] > OLDEST_AGE:
                found.append(
                    ('age-over-89', (*at, 'numeric', 'extremes', end), rule, f'{end} is {extremes[end]}; expected at most {OLDEST_AGE}')
                )
        for name, value in (numeric.get('quantiles') or {}).items():
            if value > OLDEST_AGE:
                found.append(
                    ('age-over-89', (*at, 'numeric', 'quantiles', name), rule, f'{name} is {value}; expected at most {OLDEST_AGE}')
                )
        edges = (numeric.get('histogram') or {}).get('edges') or []
        if edges and edges[-1] > OLDEST_AGE + 0.5:
            last = len(edges) - 1
            found.append(
                (
                    'age-over-89',
                    (*at, 'numeric', 'histogram', 'edges', last),
                    rule,
                    f'the last edge is {edges[-1]}; expected at most {OLDEST_AGE + 0.5}',
                )
            )
        if numeric.get('fit'):
            found.append(
                (
                    'age-over-89',
                    (*at, 'numeric', 'fit'),
                    rule,
                    f'an age holds the fitted family {numeric["fit"].get("family")}; expected none',
                )
            )
    if treatment is Treatment.PERSON_DATE:
        temporal_extremes = (column.column.get('temporal') or {}).get('extremes') or {}
        for end, shape, expected in (('low', YEAR_START, 'the first instant of a year'), ('high', YEAR_END, 'a time on December 31')):
            stated = temporal_extremes.get(end)
            if stated is not None and not shape.fullmatch(str(stated)):
                found.append(('date-below-year', (*at, 'temporal', 'extremes', end), rule, f'{end} is {stated}; expected {expected}'))
    return found
