"""The privacy policy applied to a computed profile: what each column withholds, then the audit of every literal kept.

It reads the profile's own fields and no data, so every profiling backend inherits it unchanged. A rule that needs
data, such as a ZIP code's prefixes or a date's gap rate over whole years, is computed by the statistics pass the
policy asks it of. The written profile is checked as `profile check` checks it, so the writer cannot pass a profile
the check fails.
"""

from typing import Any

from doppeldata.privacy_policy.column_treatment import ColumnFacts
from doppeldata.privacy_policy.column_treatment import Policy
from doppeldata.privacy_policy.column_treatment import Treatment
from doppeldata.privacy_policy.column_treatment import column_treatment
from doppeldata.privacy_policy.column_treatment import oldest_birth_year
from doppeldata.privacy_policy.file_check import table_check
from doppeldata.privacy_policy.literal_fields import EXACT
from doppeldata.privacy_policy.literal_fields import ROWS_AT_LEAST_K_MIN
from doppeldata.privacy_policy.literal_fields import FieldKind
from doppeldata.privacy_policy.literal_fields import column_literals
from doppeldata.privacy_policy.literal_fields import dataset_literals
from doppeldata.privacy_policy.literal_fields import faults
from doppeldata.privacy_policy.literal_fields import min_count
from doppeldata.privacy_policy.treatment_rules import amend
from doppeldata.privacy_policy.treatment_rules import capped_age_quantiles
from doppeldata.privacy_policy.treatment_rules import conditional_faults
from doppeldata.privacy_policy.treatment_rules import lag_faults
from doppeldata.privacy_policy.treatment_rules import lookup_faults
from doppeldata.privacy_policy.treatment_rules import refused
from doppeldata.profile_format import GIVEN_LISTS
from doppeldata.profile_format import PointerParts


def withheld(
    document: dict[str, Any], policy: Policy, k_min: int, cell_floors: dict[tuple[str, str, str], int] | None = None
) -> dict[str, Any]:
    """The document with what `policy` withholds removed, and its `privacy` section written: the policy, `k_min`,
    each column withheld from and why, each literal field kept with the rule that admitted it, each dataset-level
    field a floor holds, and each one it left out, as the keys of a table of fewer than `k_min` rows.

    `cell_floors` holds the fewest rows a literal of each entry of the dataset's `GIVEN_LISTS` rests on, by the list's
    name and the entry's given and child column, and the fewest a lookup table's key rests on, by `dependencies` and its
    left- and right-hand column. `dataset_audit` records them because the file does not count them. An entry naming a
    column whose values the policy withholds, or a quasi-identifier the policy never joins, is dropped, and so is such a
    dependency's lookup table.

    `none` withholds nothing, audits nothing and states no `k_min`, because `profile check` reports a `none` profile
    whatever it holds. It is profiled with a `k_min` of 1, so its numeric extremes are the exact ones.

    Raises RuntimeError where the profile written breaks the policy, which is a fault in doppeldata.
    """
    if policy is Policy.NONE:
        for column in column_literals(document):
            extremes = (column.column.get('numeric') or {}).get('extremes')
            if extremes:
                extremes['method'] = EXACT
        document['privacy'] = {'policy': policy.value}
        return document
    birth_floor = oldest_birth_year(document)
    treatments: dict[PointerParts, Treatment] = {}
    suppressed = []
    for column in column_literals(document):
        treatment = treatments[column.path] = column_treatment(ColumnFacts.of(column.column), policy)
        if treatment is Treatment.AT_K_MIN:
            continue
        amend(column.column, treatment, birth_floor)
        entry = {'column': column.name, 'reason': treatment.value}
        semantic_type = (column.column.get('semantic') or {}).get('type')
        if semantic_type:
            entry['semantic'] = semantic_type
        suppressed.append(entry)
    for column in column_literals(document):
        for field in column.fields:
            if refused(field, column, treatments[column.path]) or (field.kind is FieldKind.LAG and lag_faults(document, column, policy)):
                drop(document, field.path, field.kind, set())
                continue
            found = faults(field, column, k_min)
            if found:
                drop(document, field.path, field.kind, {fault.path for fault in found})
    dataset = document.get('dataset') or {}
    ages = frozenset(column.name for column in column_literals(document) if treatments[column.path] is Treatment.AGE)
    for entry in dataset.get('conditional_quantiles') or []:
        if entry.get('column') in ages:
            capped_age_quantiles(entry)
    for name in GIVEN_LISTS:
        entries = dataset.get(name) or []
        kept = [entry for index, entry in enumerate(entries) if not conditional_faults(document, index, policy, name, ages)]
        if kept:
            dataset[name] = kept
        else:
            dataset.pop(name, None)
    for index, dependency in enumerate(dataset.get('dependencies') or []):
        if lookup_faults(document, index, policy):
            del dependency['table']
    left_out = []
    if dataset.get('keys') and (dataset.get('row_count') or 0) < k_min:
        del dataset['keys']
        left_out.append({'field': '/dataset/keys', 'rule': ROWS_AT_LEAST_K_MIN})
    audit = []
    for column in column_literals(document):
        for field in column.fields:
            entry = {'column': column.name, 'field': column.within(field), 'values': field.values, 'rule': field.kind.rule}
            least = min_count(field, k_min)
            if least is not None:
                entry['min_count'] = least
            audit.append(entry)
    floors = cell_floors or {}
    dataset_audit = []
    for field in dataset_literals(document):
        name, index = str(field.path[1]), int(field.path[2])
        entry = dataset[name][index]
        if name in GIVEN_LISTS:
            fewest = floors[(name, entry['given'], entry['column'])]
        elif entry.get('table'):
            fewest = floors[(name, entry['lhs'][0], entry['rhs'])]
        else:
            fewest = min_count(field, k_min) or k_min
        dataset_audit.append({'field': field.pointer, 'values': field.values, 'rule': field.kind.rule, 'min_count': fewest})
    privacy: dict[str, Any] = {'policy': policy.value, 'k_min': k_min}
    if suppressed:
        privacy['suppressed'] = suppressed
    if audit:
        privacy['audit'] = audit
    if dataset_audit:
        privacy['dataset_audit'] = dataset_audit
    if left_out:
        privacy['withheld'] = left_out
    document['privacy'] = privacy
    broken = table_check(document, ()).violations
    if broken:
        listed = '; '.join(f'{violation.field}: {violation.detail}' for violation in broken)
        raise RuntimeError(f'the {policy} profile of {document.get("name")} breaks its own policy: {listed}')
    return document


def drop(document: dict[str, Any], path: PointerParts, kind: FieldKind, failing: set[PointerParts]) -> None:
    """Remove a field. A quantile fails alone, so where some fail only those go; any other field goes whole, and a lag
    takes its derivation with it."""
    if kind is FieldKind.LAG:
        path = path[:-1]
    parent: Any = document
    for part in path[:-1]:
        parent = parent[part]
    if kind is FieldKind.QUANTILES and failing:
        for name in [name for name in parent[path[-1]] if (*path, name) in failing]:
            del parent[path[-1]][name]
        if parent[path[-1]]:
            return
    del parent[path[-1]]
    if kind is FieldKind.TOP_VALUES:
        parent.pop('other_rate', None)
        parent.pop('overflow', None)
