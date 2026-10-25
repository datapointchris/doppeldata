"""The privacy policy applied to a computed profile: what each column withholds, then the audit of every literal kept.

It reads the profile's own fields and no data, so every profiling backend inherits it unchanged. A rule that needs
data, such as a ZIP code's prefixes or a person's dates to the year, is computed by the statistics pass the policy
asks it of. A literal that fails its rule here is dropped, so a profile written under a policy passes `profile check`.
"""

import math
from typing import Any

from doppeldata.privacy_policy.column_treatment import OLDEST_AGE
from doppeldata.privacy_policy.column_treatment import ColumnFacts
from doppeldata.privacy_policy.column_treatment import Policy
from doppeldata.privacy_policy.column_treatment import Treatment
from doppeldata.privacy_policy.column_treatment import column_treatment
from doppeldata.privacy_policy.literal_fields import DIGIT_BOUNDS
from doppeldata.privacy_policy.literal_fields import FieldKind
from doppeldata.privacy_policy.literal_fields import column_literals
from doppeldata.privacy_policy.literal_fields import faults
from doppeldata.privacy_policy.literal_fields import min_count
from doppeldata.profile_format import LARGEST_EXACT_INTEGER

# What a column keeping no data value loses from each section; its null rate, distinct ratio, lengths, shapes and pattern stay.
NO_VALUE_DROPS = {
    'numeric': ('quantiles', 'histogram', 'fit', 'sequence'),
    'string': ('top_values', 'other_rate', 'overflow'),
    'temporal': ('extremes', 'by_month', 'by_dow', 'by_hour'),
}
FREE_TEXT_DROPS = ('top_values', 'other_rate', 'overflow', 'shapes', 'pattern')
EXACT = 'exact'


def withheld(document: dict[str, Any], policy: Policy, k_min: int) -> dict[str, Any]:
    """The document with what `policy` withholds removed, and its `privacy` section written: the policy, `k_min`,
    each column withheld from and why, and each literal field kept with the rule that admitted it.

    `none` withholds nothing and audits nothing, because `profile check` reports a `none` profile whatever it holds. It
    is profiled with a `k_min` of 1, so its numeric extremes are the exact ones.
    """
    privacy: dict[str, Any] = {'policy': policy.value, 'k_min': k_min}
    if policy is Policy.NONE:
        for column in column_literals(document):
            extremes = (column.column.get('numeric') or {}).get('extremes')
            if extremes:
                extremes['method'] = EXACT
        document['privacy'] = privacy
        return document
    suppressed = []
    for column in document.get('columns') or []:
        treatment = column_treatment(ColumnFacts.of(column), policy)
        if treatment is not Treatment.AT_K_MIN and withhold(column, treatment):
            entry = {'column': column['name'], 'reason': treatment.value}
            semantic_type = (column.get('semantic') or {}).get('type')
            if semantic_type:
                entry['semantic'] = semantic_type
            suppressed.append(entry)
    for column in column_literals(document):
        for field in column.fields:
            found = faults(field, column, k_min)
            if found:
                drop(document, field.path, field.kind, {fault.path for fault in found})
    audit = []
    for column in column_literals(document):
        for field in column.fields:
            entry = {'field': field.pointer, 'column': column.name, 'values': field.values, 'rule': field.kind.rule}
            least = min_count(field, k_min)
            if least is not None:
                entry['min_count'] = least
            audit.append(entry)
    if suppressed:
        privacy['suppressed'] = suppressed
    if audit:
        privacy['audit'] = audit
    document['privacy'] = privacy
    return document


def withhold(column: dict[str, Any], treatment: Treatment) -> bool:
    """Remove what `treatment` withholds from the column, and say whether it withheld anything a profile would hold."""
    numeric, string, temporal = (column.get(section) for section in ('numeric', 'string', 'temporal'))
    if treatment.keeps_no_value:
        for section, names in NO_VALUE_DROPS.items():
            for name in names:
                (column.get(section) or {}).pop(name, None)
        if numeric and numeric.get('extremes'):
            bounds = digit_bounds(numeric['extremes']) if numeric.get('integral') else None
            if bounds:
                numeric['extremes'] = bounds
            else:
                del numeric['extremes']
        return True
    if treatment is Treatment.FREE_TEXT:
        for name in FREE_TEXT_DROPS:
            (string or {}).pop(name, None)
        return True
    if treatment is Treatment.ZIP_PREFIX:
        (string or {}).pop('pattern', None)
        return True
    if treatment is Treatment.AGE:
        if not numeric:
            return False
        extremes = numeric.get('extremes') or {}
        for end in ('low', 'high'):
            if isinstance(extremes.get(end), int | float) and extremes[end] > OLDEST_AGE:
                extremes[end] = OLDEST_AGE
        for name, value in (numeric.get('quantiles') or {}).items():
            if value > OLDEST_AGE:
                numeric['quantiles'][name] = OLDEST_AGE
        numeric.pop('fit', None)
        return True
    return treatment is Treatment.PERSON_DATE and bool((temporal or {}).get('extremes'))


def digit_bounds(extremes: dict[str, Any]) -> dict[str, Any] | None:
    """The least and greatest integers with as many digits as the extremes, or None where they are not both non-negative."""
    low, high = extremes.get('low'), extremes.get('high')
    if not isinstance(low, int | float) or not isinstance(high, int | float) or low < 0 or high < low:
        return None
    low_digits, high_digits = (len(str(int(value))) for value in (low, high))
    least, greatest = (0 if low_digits == 1 else 10 ** (low_digits - 1)), 10**high_digits - 1
    return {'low': written(least), 'high': written(greatest), 'method': DIGIT_BOUNDS}


def is_digit_bounds(extremes: dict[str, Any]) -> bool:
    """Whether the extremes are 0 or a power of ten, and one less than a power of ten, as `digit_bounds` writes them."""
    low, high = extremes.get('low'), extremes.get('high')

    def power_of_ten(value: Any) -> bool:
        return isinstance(value, int | float) and value >= 1 and math.isclose(math.log10(value), round(math.log10(value)), abs_tol=1e-12)

    return (low == 0 or power_of_ten(low)) and isinstance(high, int | float) and power_of_ten(high + 1)


def written(value: int) -> int | float:
    return value if value <= LARGEST_EXACT_INTEGER else float(value)


def drop(document: dict[str, Any], path: tuple[Any, ...], kind: FieldKind, failing: set[tuple[Any, ...]]) -> None:
    """Remove a field that fails its rule. A quantile fails alone, so only the failing ones go; any other field goes whole."""
    parent: Any = document
    for part in path[:-1]:
        parent = parent[part]
    if kind is FieldKind.QUANTILES:
        for name in [name for name in parent[path[-1]] if (*path, name) in failing]:
            del parent[path[-1]][name]
        if parent[path[-1]]:
            return
    del parent[path[-1]]
    if kind is FieldKind.TOP_VALUES:
        parent.pop('other_rate', None)
