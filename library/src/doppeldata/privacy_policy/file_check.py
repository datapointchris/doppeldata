"""A profile checked against its schema and its privacy policy, from the file alone, as `profile check` runs it.

Each rule is recomputed from what the file states: every literal must be in `privacy.audit` under the rule its kind is
admitted by, and must meet that rule, counted as a rate times the rows the profile describes. A column the policy
withholds from must hold nothing it withholds, and must meet each rule of its treatment.
"""

import dataclasses as dc
import importlib.resources
import json
import os
from collections.abc import Mapping
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from typing import ClassVar

from doppeldata import issue_codes
from doppeldata.embedded_profiles import data_file_metadata
from doppeldata.embedded_profiles import embedded_document
from doppeldata.errors import InvalidArgument
from doppeldata.errors import ProfileFailsCheck
from doppeldata.errors import ProfileNotFound
from doppeldata.example_profiles import bundle_example_names
from doppeldata.example_profiles import bundle_example_resource
from doppeldata.example_profiles import example_names
from doppeldata.example_profiles import example_resource
from doppeldata.issue_codes import PROFILE_DOCUMENT
from doppeldata.issue_codes import Issue
from doppeldata.issue_codes import issue_table
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
from doppeldata.privacy_policy.relationship_floors import floor_breaks
from doppeldata.privacy_policy.relationship_floors import relationship_k_min
from doppeldata.privacy_policy.relationship_floors import top_shares
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
class Violation(Issue):
    """One place a profile breaks its schema or its policy, at `field`, an RFC 6901 pointer.

    `rule` is the `AuditRule` a literal broke, and `treatment` the `privacy.suppressed` reason whose rule a column broke.
    Each is null where the violation is of another kind, which `code` names.
    """

    rule: str | None = None
    treatment: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {**super().to_json(), 'rule': self.rule, 'treatment': self.treatment}


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
class RelationshipsCheck:
    """A bundle's relationships held to their tables' `k_min`: how many it states, and each violation, at its pointer from
    the bundle's root."""

    stated: int
    violations: list[Violation]


@dc.dataclass(frozen=True)
class ProfileFileCheck:
    """A profile file checked against its schema and privacy policy: the file as named, whether it passed, each table's
    check, and a bundle's relationships' check."""

    failure_code: ClassVar[str] = issue_codes.PROFILE_CHECK_FAILED

    profile: str
    passed: bool
    tables: list[TableCheck]
    relationships: RelationshipsCheck | None = None

    @classmethod
    def of(cls, profile: str, tables: list[TableCheck], relationships: RelationshipsCheck | None = None) -> 'ProfileFileCheck':
        held = any(table.violations for table in tables) or (relationships is not None and bool(relationships.violations))
        return cls(profile, not held, tables, relationships)

    @property
    def violations(self) -> list[Violation]:
        found = [violation for check in self.tables for violation in check.violations]
        return found + (self.relationships.violations if self.relationships is not None else [])

    def to_json(self) -> str:
        """The check as `profile check --json` prints it: each table's name, policy, `k_min` and literal fields, and its
        issues, then for a bundle the relationships it states and their issues."""
        tables = [
            {
                'table': check.table,
                'policy': check.policy,
                'k_min': check.k_min,
                'literal_fields': check.literal_fields,
                'issues': [violation.to_json() for violation in check.violations],
            }
            for check in self.tables
        ]
        related = self.relationships
        relationships = None if related is None else {'stated': related.stated, 'issues': [issue.to_json() for issue in related.violations]}
        document = {'profile': self.profile, 'passed': self.passed, 'tables': tables, 'relationships': relationships}
        return json.dumps(document, indent=2, ensure_ascii=False) + '\n'

    def to_markdown(self) -> str:
        """The check for a person: each table's policy and literal fields, then its issues, then a bundle's relationships."""
        verdict = 'passed' if self.passed else 'failed'
        parts = [f'{len(self.tables)} table{"" if len(self.tables) == 1 else "s"}']
        related = self.relationships
        if related is not None and related.stated:
            parts.append(f'{related.stated} relationship{"" if related.stated == 1 else "s"}')
        lines = [f'# doppeldata profile check {verdict}', '', f'`{self.profile}`, {", ".join(parts)}.', '']
        for table in self.tables:
            count = len(table.violations)
            found = f'{count} issue{"" if count == 1 else "s"}'
            if table.policy is None:
                held = f'{found} against the schema.'
            else:
                floor = f', `k_min` {table.k_min}' if table.k_min is not None else ''
                literals = f'{table.literal_fields} literal field{"" if table.literal_fields == 1 else "s"}'
                held = f'{literals} under policy `{table.policy}`{floor}, and {found}.'
            lines += [f'## {table.table}', '', held, '', *issue_table(table.violations)]
        if related is not None and (related.stated or related.violations):
            count = len(related.violations)
            found = f'{count} issue{"" if count == 1 else "s"}'
            stated = '1 relationship,' if related.stated == 1 else f'{related.stated} relationships, each'
            held = f'{stated} held to the larger `k_min` of its two tables, and {found}.'
            lines += ['## relationships', '', held, '', *issue_table(related.violations)]
        return '\n'.join(lines).rstrip('\n') + '\n'

    def failure_message(self) -> str:
        """What `CheckFailed` says of a profile file that breaks its schema or policy."""
        count = len(self.violations)
        return f'{self.profile} breaks its schema or its privacy policy in {count} place{"" if count == 1 else "s"}'


def file_checks(path: Path) -> tuple[list[TableCheck], RelationshipsCheck | None]:
    """The check of a table profile, or of each table and the relationships of a bundle, whose violations point into the
    file named.

    A bundle's table held in its own file is checked as that file, and its violations point into it. Raises OSError
    where the file named cannot be read. A member that cannot be read is a violation, and the others are still checked.
    A Parquet or Arrow IPC file's embedded profile is checked, and one holding none raises SourceError.
    """
    metadata = data_file_metadata(path)
    try:
        document = to_plain(read_document(path)) if metadata is None else embedded_document(path, metadata)
    except InvalidProfile as error:
        return [TableCheck(path.name, None, None, 0, schema_violations(error.issues))], None
    if not isinstance(document, dict) or document.get('kind') != 'bundle':
        return [table_check(document, ())], None
    issues = check(document)
    if issues:
        return [TableCheck(str(document.get('name')), None, None, 0, schema_violations(issues))], None
    checks = []
    tables: dict[str, Any] = {}
    for name, entry in document['tables'].items():
        checked, read = member_check(path.parent / entry, name) if isinstance(entry, str) else (table_check(entry, ('tables', name)), entry)
        checks.append(checked)
        tables[name] = read if checked.policy is not None else None
    return checks, relationships_check(document, tables)


def member_check(member: Path, name: str) -> tuple[TableCheck, Any]:
    """The check of a bundle's table held in its own file, read as a table alone, as `Bundle.load` reads it, and the
    table's profile, None where it is not one."""
    at = pointer(('tables', name))

    def refusal(code: str, detail: str) -> tuple[TableCheck, Any]:
        return TableCheck(name, None, None, 0, [Violation(code, at, detail)]), None

    if not member.is_file():
        return refusal(issue_codes.TABLE_FILE_MISSING, f'{member} is not a file; expected the table profile the bundle names')
    try:
        document = to_plain(read_document(member))
    except OSError as error:
        return refusal(issue_codes.FILE_UNREADABLE, f'{member} cannot be read: {error.strerror}; expected a readable table profile')
    except InvalidProfile as error:
        return TableCheck(member.name, None, None, 0, schema_violations(error.issues)), None
    if not isinstance(document, dict) or document.get('kind') != 'table':
        kind = document.get('kind') if isinstance(document, dict) else type(document).__name__
        if not isinstance(document, dict):
            code = PROFILE_DOCUMENT.not_a_mapping
        else:
            code = issue_codes.BUNDLE_GIVEN if kind == 'bundle' else PROFILE_DOCUMENT.not_one_of
        return refusal(code, f'{member} is a {kind}; expected a table profile')
    if document.get('name') != name:
        return refusal(
            issue_codes.TABLE_NAME_MISMATCH, f'{member} is named {document.get("name")!r}; expected {name!r}, as the bundle names it'
        )
    return table_check(document, ()), document


def profile_file_check(source: str | os.PathLike[str]) -> ProfileFileCheck:
    """`profile check`'s check of the file `source` names, else of the example of that name, from the file alone. A name
    that is neither raises ProfileNotFound, and a file that cannot be read InvalidArgument, each at `profile`."""
    named = os.fspath(source)
    path = Path(source)
    try:
        if path.is_file():
            return ProfileFileCheck.of(named, *file_checks(path))
        if named in example_names() or named in bundle_example_names():
            resource = example_resource(named) if named in example_names() else bundle_example_resource(named)
            with importlib.resources.as_file(resource) as example:
                return ProfileFileCheck.of(named, *file_checks(example))
    except OSError as error:
        message = f'{named} cannot be read: {error.strerror}'
        raise InvalidArgument(issue_codes.FILE_UNREADABLE, message, field='profile', value=named) from error
    message = f'{named!r} is neither a profile file nor an example; name a file, or an example'
    known = [*example_names(), *bundle_example_names()]
    raise ProfileNotFound(issue_codes.PROFILE_NOT_FOUND, message, field='profile', value=named, valid=known)


def refuse_failed_check(found: Profile | Bundle) -> None:
    """Raise ProfileFailsCheck where a table made from data, or a bundle's relationship, fails `profile check`, before
    its literals leave the profile. A seed was written under no policy, and its relationships state no fan-out or orphans."""
    tables = found.tables if isinstance(found, Bundle) else {found.document['name']: found}
    at = ('tables',) if isinstance(found, Bundle) else ()
    checks = [
        table_check(profile.to_plain(), (*at, name) if at else ())
        for name, profile in tables.items()
        if (profile.document.get('generated') or {}).get('engine') != 'import'
    ]
    relationships = None
    if isinstance(found, Bundle):
        plain = found.to_plain()
        relationships = relationships_check(plain, plain['tables'])
    result = ProfileFileCheck.of(str(found.document['name']), checks, relationships)
    if not result.passed:
        raise ProfileFailsCheck(result)


def table_check(table: Any, at: PointerParts) -> TableCheck:
    """One table profile's check. `at` is where it sits in the file, which each violation's field begins with."""
    issues = check(table)
    name = str(table.get('name')) if isinstance(table, dict) else ''
    if issues:
        return TableCheck(name, None, None, 0, schema_violations(issues, at))
    privacy = table.get('privacy') or {}
    policy = Policy(privacy.get('policy') or Policy.DEFAULT)
    if not policy.checked:
        code = issue_codes.POLICY_NONE if policy is Policy.NONE else issue_codes.POLICY_NOT_CHECKABLE
        found = 'policy is none' if policy is Policy.NONE else f'policy is {policy.value}, which doppeldata reserves and does not apply'
        violation = Violation(code, pointer((*at, 'privacy', 'policy')), f'{found}; expected one of {CHECKABLE}')
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
            violations.append(Violation(fault.code, pointer((*at, *fault.path)), fault.message, treatment=treatment.value))
        for field in column.fields:
            fields += 1
            refusals = [found for found in [refused(field, column, treatment)] if found]
            if field.kind is FieldKind.LAG:
                refusals += lag_faults(table, column, policy)
            for refusal in refusals:
                violations.append(Violation(refusal.code, pointer((*at, *refusal.path)), refusal.message, treatment=treatment.value))
            if refusals:
                continue
            index = audited.get((column.name, column.within(field)))
            if index is None:
                expected = f'expected one with column {column.name} and field {column.within(field)}'
                detail = f'no privacy.audit entry for the {field.kind.label} of {column.name}; {expected}'
                violations.append(Violation(issue_codes.UNAUDITED_LITERAL, pointer((*at, *field.path)), detail, rule=field.kind.rule))
            else:
                seen.add(index)
                violations += audit_faults(audit[index], (*at, 'privacy', 'audit', index), field, k_min)
            for fault in faults(field, column, k_min):
                violations.append(Violation(fault.code, pointer((*at, *fault.path)), fault.message, rule=field.kind.rule))
    for index, entry in enumerate(audit):
        if index not in seen:
            named = f'{entry.get("column")} holds no literal at {entry.get("field")}'
            detail = f'{named}; expected the column and pointer of a field this file holds'
            violations.append(Violation(issue_codes.STALE_AUDIT_ENTRY, pointer((*at, 'privacy', 'audit', index)), detail))
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
            violations.append(Violation(keyed_fault.code, pointer((*at, *keyed_fault.path)), keyed_fault.message, rule=field.kind.rule))
        audited_at = dataset_audited.get(field.pointer)
        if audited_at is None:
            detail = f'no privacy.dataset_audit entry for the {field.kind.label} at {field.pointer}; expected one with that field'
            violations.append(Violation(issue_codes.UNAUDITED_LITERAL, pointer((*at, *field.path)), detail, rule=field.kind.rule))
            continue
        dataset_seen.add(audited_at)
        entry_at = (*at, 'privacy', 'dataset_audit', audited_at)
        violations += audit_faults(dataset_audit[audited_at], entry_at, field, k_min)
        if dataset_audit[audited_at].get('min_rows') is None:
            detail = f'the entry states no min_rows, which is all that holds the {field.kind.label} to k_min; expected at least {k_min}'
            violations.append(Violation(issue_codes.ROWS_NOT_STATED, pointer(entry_at), detail, rule=field.kind.rule))
    for index, entry in enumerate(dataset_audit):
        if index not in dataset_seen:
            detail = f'the file holds no dependence between columns at {entry.get("field")}; expected the pointer of one it holds'
            violations.append(Violation(issue_codes.STALE_AUDIT_ENTRY, pointer((*at, 'privacy', 'dataset_audit', index)), detail))
    dataset = table.get('dataset') or {}
    rows = dataset.get('rows') or 0
    if dataset.get('keys') and rows < k_min:
        detail = f'the table holds {rows} rows, and a key is stated only over {k_min}; expected no keys'
        violations.append(Violation(issue_codes.KEYS_UNDER_K_MIN, pointer((*at, 'dataset', 'keys')), detail, rule=ROWS_AT_LEAST_K_MIN))
    return TableCheck(name, policy.value, k_min, fields, violations)


def relationships_check(bundle: dict[str, Any], tables: Mapping[str, Any]) -> RelationshipsCheck:
    """The bundle's relationships held to the larger `k_min` of each one's tables. `tables` holds each table's profile by
    name, None where it cannot be read.

    What the file states of a relationship's rows is recomputed. A top share's parents are not stated, so it must have an
    entry in `privacy.relationship_audit` at its pointer, matching its rule and values, whose `min_rows` reaches `k_min`.
    """
    audit = (bundle.get('privacy') or {}).get('relationship_audit') or []
    audited = {entry.get('field'): index for index, entry in enumerate(audit)}
    seen: set[int] = set()
    violations: list[Violation] = []
    relationships = bundle.get('relationships') or []
    for index, relationship in enumerate(relationships):
        at = ('relationships', index)
        shares = top_shares(relationship, at)
        audited_at = audited.get(shares.pointer) if shares is not None else None
        if audited_at is not None:
            seen.add(audited_at)
        k_min = relationship_k_min(relationship, tables)
        if k_min is None:
            continue
        for kind, fault in floor_breaks(relationship, at, tables, k_min):
            violations.append(Violation(fault.code, pointer(fault.path), fault.message, rule=kind.rule))
        if shares is None:
            continue
        if audited_at is None:
            detail = f'no privacy.relationship_audit entry for the {shares.kind.label} at {shares.pointer}; expected one with that field'
            violations.append(Violation(issue_codes.UNAUDITED_LITERAL, pointer(shares.path), detail, rule=shares.kind.rule))
            continue
        entry_at = ('privacy', 'relationship_audit', audited_at)
        violations += audit_faults(audit[audited_at], entry_at, shares, k_min)
        if audit[audited_at].get('min_rows') is None:
            detail = f'the entry states no min_rows, which is all that holds the {shares.kind.label} to k_min; expected at least {k_min}'
            violations.append(Violation(issue_codes.ROWS_NOT_STATED, pointer(entry_at), detail, rule=shares.kind.rule))
    for index, entry in enumerate(audit):
        if index not in seen:
            detail = f'the bundle states no top shares at {entry.get("field")}; expected the pointer of a relationship stating some'
            violations.append(Violation(issue_codes.STALE_AUDIT_ENTRY, pointer(('privacy', 'relationship_audit', index)), detail))
    return RelationshipsCheck(len(relationships), violations)


def schema_violations(issues: Sequence[Issue], at: PointerParts = ()) -> list[Violation]:
    return [
        Violation(issue.code, pointer(at) + (issue.field or ''), issue.message, issue.value, issue.valid, issue.closest) for issue in issues
    ]


def audit_faults(entry: dict[str, Any], at: PointerParts, field: LiteralField, k_min: int) -> list[Violation]:
    """Where the audit entry for a field misstates it."""
    rule = field.kind.rule
    found = []
    if entry.get('rule') != rule:
        found.append(
            Violation(issue_codes.AUDIT_RULE_MISMATCH, pointer((*at, 'rule')), f'rule is {entry.get("rule")}; expected {rule}', rule=rule)
        )
    if entry.get('values') is not None and entry['values'] != field.values:
        detail = f'values is {entry["values"]}; the field holds {field.values}'
        found.append(Violation(issue_codes.AUDIT_VALUES_MISMATCH, pointer((*at, 'values')), detail, rule=rule))
    if entry.get('min_rows') is not None and entry['min_rows'] < k_min:
        detail = f'min_rows is {entry["min_rows"]}; expected at least {k_min}'
        found.append(Violation(issue_codes.BELOW_K_MIN, pointer((*at, 'min_rows')), detail, rule=rule))
    return found
