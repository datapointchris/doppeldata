"""A profile checked against its schema and its privacy policy, from the file alone, as `profile check` runs it.

Each rule is recomputed from what the file states: every literal must be in `privacy.audit` under the rule its kind is
admitted by, and must meet that rule, counted as a rate times the rows the profile describes. A column the policy
withholds from must hold nothing it withholds, and must meet each rule of its treatment.
"""

import dataclasses as dc
from pathlib import Path
from typing import Any

from doppeldata.embedded_profiles import data_file_metadata
from doppeldata.embedded_profiles import embedded_document
from doppeldata.errors import CheckFailed
from doppeldata.privacy_policy.column_treatment import CHECKED_POLICIES
from doppeldata.privacy_policy.column_treatment import ColumnFacts
from doppeldata.privacy_policy.column_treatment import Policy
from doppeldata.privacy_policy.column_treatment import Treatment
from doppeldata.privacy_policy.column_treatment import column_treatment
from doppeldata.privacy_policy.column_treatment import oldest_birth_year
from doppeldata.privacy_policy.literal_fields import ROWS_AT_LEAST_K_MIN
from doppeldata.privacy_policy.literal_fields import FieldKind
from doppeldata.privacy_policy.literal_fields import LiteralField
from doppeldata.privacy_policy.literal_fields import column_literals
from doppeldata.privacy_policy.literal_fields import dataset_literals
from doppeldata.privacy_policy.literal_fields import faults
from doppeldata.privacy_policy.treatment_rules import conditional_faults
from doppeldata.privacy_policy.treatment_rules import lag_faults
from doppeldata.privacy_policy.treatment_rules import lookup_faults
from doppeldata.privacy_policy.treatment_rules import refused
from doppeldata.privacy_policy.treatment_rules import treatment_faults
from doppeldata.profile_format import GIVEN_LISTS
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import PointerParts
from doppeldata.profile_format import Profile
from doppeldata.profile_format import check
from doppeldata.profile_format import pointer
from doppeldata.profile_format import read_document
from doppeldata.profile_format import to_plain

CHECKABLE = ', '.join(policy.value for policy in CHECKED_POLICIES)


@dc.dataclass(frozen=True)
class Violation:
    """One place a profile breaks its schema or its policy. `field` is an RFC 6901 pointer.

    `rule` is the `AuditRule` a literal broke, and `treatment` the `privacy.suppressed` reason whose rule a column broke.
    Each is null where the violation is of another kind, which `code` names.
    """

    code: str
    field: str
    rule: str | None
    treatment: str | None
    detail: str


@dc.dataclass(frozen=True)
class TableCheck:
    """One table profile's check: its name and policy as the file states them, the `k_min` its literals were held to,
    the literal fields it holds, and each violation."""

    table: str
    policy: str | None
    k_min: int | None
    literal_fields: int
    violations: list[Violation]


@dc.dataclass(frozen=True)
class ProfileFileCheck:
    """A profile file checked against its schema and privacy policy: the file as named, whether it passed, and each table's check."""

    profile: str
    passed: bool
    tables: list[TableCheck]

    @classmethod
    def of(cls, profile: str, tables: list[TableCheck]) -> 'ProfileFileCheck':
        return cls(profile, not any(table.violations for table in tables), tables)

    @property
    def violations(self) -> list[Violation]:
        return [violation for check in self.tables for violation in check.violations]

    def failure_message(self) -> str:
        """What `CheckFailed` says of a profile file that breaks its schema or policy."""
        count = len(self.violations)
        return f'{self.profile} breaks its schema or its privacy policy in {count} place{"" if count == 1 else "s"}'


def file_checks(path: Path) -> list[TableCheck]:
    """The check of a table profile, or of each table of a bundle, whose violations point into the file named.

    A bundle's table held in its own file is checked as that file, and its violations point into it. Raises OSError
    where the file named cannot be read. A member that cannot be read is a violation, and the others are still checked.
    A Parquet or Arrow IPC file's embedded profile is checked, and one holding none raises SourceError.
    """
    metadata = data_file_metadata(path)
    try:
        document = to_plain(read_document(path)) if metadata is None else embedded_document(path, metadata)
    except InvalidProfile as error:
        return [TableCheck(path.name, None, None, 0, schema_violations(error.issues))]
    if not isinstance(document, dict) or document.get('kind') != 'bundle':
        return [table_check(document, ())]
    issues = check(document)
    if issues:
        return [TableCheck(str(document.get('name')), None, None, 0, schema_violations(issues))]
    checks = []
    for name, entry in document['tables'].items():
        checks.append(table_check(entry, ('tables', name)) if not isinstance(entry, str) else member_check(path.parent / entry, name))
    return checks


def member_check(member: Path, name: str) -> TableCheck:
    """The check of a bundle's table held in its own file, read as a table alone, as `Bundle.load` reads it."""
    at = pointer(('tables', name))

    def refusal(code: str, detail: str) -> TableCheck:
        return TableCheck(name, None, None, 0, [Violation(code, at, None, None, detail)])

    if not member.is_file():
        return refusal('table-file-missing', f'{member} is not a file; expected the table profile the bundle names')
    try:
        document = to_plain(read_document(member))
    except OSError as error:
        return refusal('not-readable', f'{member} cannot be read: {error.strerror}; expected a readable table profile')
    except InvalidProfile as error:
        return TableCheck(member.name, None, None, 0, schema_violations(error.issues))
    if not isinstance(document, dict) or document.get('kind') != 'table':
        kind = document.get('kind') if isinstance(document, dict) else type(document).__name__
        return refusal('not-a-table', f'{member} is a {kind}; expected a table profile')
    if document.get('name') != name:
        return refusal('table-name-mismatch', f'{member} is named {document.get("name")!r}; expected {name!r}, as the bundle names it')
    return table_check(document, ())


def refuse_failed_check(found: Profile | Bundle) -> None:
    """Raise CheckFailed where a table made from data fails `profile check`, before its literals leave the profile. A seed
    was written under no policy."""
    tables = found.tables if isinstance(found, Bundle) else {found.document['name']: found}
    at = ('tables',) if isinstance(found, Bundle) else ()
    checks = [
        table_check(profile.to_plain(), (*at, name) if at else ())
        for name, profile in tables.items()
        if (profile.document.get('generated') or {}).get('engine') != 'import'
    ]
    if any(result.violations for result in checks):
        raise CheckFailed(ProfileFileCheck.of(str(found.document['name']), checks))


def table_check(table: Any, at: PointerParts) -> TableCheck:
    """One table profile's check. `at` is where it sits in the file, which each violation's field begins with."""
    issues = check(table)
    name = str(table.get('name')) if isinstance(table, dict) else ''
    if issues:
        return TableCheck(name, None, None, 0, schema_violations(issues, at))
    privacy = table.get('privacy') or {}
    policy = Policy(privacy.get('policy') or Policy.DEFAULT)
    if not policy.checked:
        code = 'policy-none' if policy is Policy.NONE else 'policy-not-checkable'
        found = 'policy is none' if policy is Policy.NONE else f'policy is {policy.value}, which doppeldata reserves and does not apply'
        violation = Violation(code, pointer((*at, 'privacy', 'policy')), None, None, f'{found}; expected one of {CHECKABLE}')
        return TableCheck(name, policy.value, privacy.get('k_min'), 0, [violation])
    k_min = policy.k_min(privacy.get('k_min')) or 1
    birth_floor = oldest_birth_year(table)
    audit = privacy.get('audit') or []
    audited = {(entry.get('column'), entry.get('field')): index for index, entry in enumerate(audit)}
    violations: list[Violation] = []
    seen: set[int] = set()
    fields = 0
    ages: set[str] = set()
    for column in column_literals(table):
        treatment = column_treatment(ColumnFacts.of(column.column), policy)
        if treatment is Treatment.AGE:
            ages.add(column.name)
        for fault in treatment_faults(column, treatment, birth_floor):
            violations.append(Violation(fault.code, pointer((*at, *fault.path)), None, treatment.value, fault.detail))
        for field in column.fields:
            fields += 1
            refusals = [found for found in [refused(field, column, treatment)] if found]
            if field.kind is FieldKind.LAG:
                refusals += lag_faults(table, column, policy)
            for refusal in refusals:
                violations.append(Violation(refusal.code, pointer((*at, *refusal.path)), None, treatment.value, refusal.detail))
            if refusals:
                continue
            index = audited.get((column.name, column.within(field)))
            if index is None:
                expected = f'expected one with column {column.name} and field {column.within(field)}'
                detail = f'no privacy.audit entry for the {field.kind.label} of {column.name}; {expected}'
                violations.append(Violation('unaudited-literal', pointer((*at, *field.path)), field.kind.rule, None, detail))
            else:
                seen.add(index)
                violations += audit_faults(audit[index], (*at, 'privacy', 'audit', index), field, k_min)
            for fault in faults(field, column, k_min):
                violations.append(Violation(fault.code, pointer((*at, *fault.path)), field.kind.rule, None, fault.detail))
    for index, entry in enumerate(audit):
        if index not in seen:
            named = f'{entry.get("column")} holds no literal at {entry.get("field")}'
            detail = f'{named}; expected the column and pointer of a field this file holds'
            violations.append(Violation('stale-audit-entry', pointer((*at, 'privacy', 'audit', index)), None, None, detail))
    dataset_audit = privacy.get('dataset_audit') or []
    dataset_audited = {entry.get('field'): index for index, entry in enumerate(dataset_audit)}
    dataset_seen: set[int] = set()
    for field in dataset_literals(table):
        fields += 1
        section, index = str(field.path[1]), int(field.path[2])
        if section in GIVEN_LISTS:
            keyed = conditional_faults(table, index, policy, section, frozenset(ages))
        else:
            keyed = lookup_faults(table, index, policy)
        for keyed_fault in keyed:
            violations.append(Violation(keyed_fault.code, pointer((*at, *keyed_fault.path)), field.kind.rule, None, keyed_fault.detail))
        audited_at = dataset_audited.get(field.pointer)
        if audited_at is None:
            detail = f'no privacy.dataset_audit entry for the {field.kind.label} at {field.pointer}; expected one with that field'
            violations.append(Violation('unaudited-literal', pointer((*at, *field.path)), field.kind.rule, None, detail))
            continue
        dataset_seen.add(audited_at)
        entry_at = (*at, 'privacy', 'dataset_audit', audited_at)
        violations += audit_faults(dataset_audit[audited_at], entry_at, field, k_min)
        if dataset_audit[audited_at].get('min_count') is None:
            detail = f'the entry states no min_count, which is all that holds the {field.kind.label} to k_min; expected at least {k_min}'
            violations.append(Violation('rows-not-stated', pointer(entry_at), field.kind.rule, None, detail))
    for index, entry in enumerate(dataset_audit):
        if index not in dataset_seen:
            detail = f'the file holds no dependence between columns at {entry.get("field")}; expected the pointer of one it holds'
            violations.append(Violation('stale-audit-entry', pointer((*at, 'privacy', 'dataset_audit', index)), None, None, detail))
    dataset = table.get('dataset') or {}
    rows = dataset.get('row_count') or 0
    if dataset.get('keys') and rows < k_min:
        detail = f'the table holds {rows} rows, and a key is stated only over {k_min}; expected no keys'
        violations.append(Violation('keys-under-k-min', pointer((*at, 'dataset', 'keys')), ROWS_AT_LEAST_K_MIN, None, detail))
    return TableCheck(name, policy.value, k_min, fields, violations)


def schema_violations(issues: list[Any], at: PointerParts = ()) -> list[Violation]:
    return [Violation(issue.code, pointer(at) + issue.field, None, None, issue.message) for issue in issues]


def audit_faults(entry: dict[str, Any], at: PointerParts, field: LiteralField, k_min: int) -> list[Violation]:
    """Where the audit entry for a field misstates it."""
    rule = field.kind.rule
    found = []
    if entry.get('rule') != rule:
        found.append(Violation('audit-rule-mismatch', pointer((*at, 'rule')), rule, None, f'rule is {entry.get("rule")}; expected {rule}'))
    if entry.get('values') is not None and entry['values'] != field.values:
        detail = f'values is {entry["values"]}; the field holds {field.values}'
        found.append(Violation('audit-values-mismatch', pointer((*at, 'values')), rule, None, detail))
    if entry.get('min_count') is not None and entry['min_count'] < k_min:
        detail = f'min_count is {entry["min_count"]}; expected at least {k_min}'
        found.append(Violation('below-k-min', pointer((*at, 'min_count')), rule, None, detail))
    return found
