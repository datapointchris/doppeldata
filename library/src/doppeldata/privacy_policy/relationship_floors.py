"""The floor a bundle's relationships are held to, recomputed from the rows their tables state.

A fan-out quantile lies at least `k_min` parents from each end of the parent table's rows. `orphan_fraction` times the
child table's rows numbers 0 or at least `k_min`, and a `containment` under 1 states orphan keys, so its orphan rows
reach `k_min` too. A top share counts the busiest parents' child rows, and the file states only the share, so the
bundle's `privacy.relationship_audit` holds the fewest child rows any of those parents holds.

A relationship is held to the larger `k_min` of its two tables. One of a table whose policy `profile check` does not
check is not held, because that table's own check reports its policy.
"""

from collections.abc import Mapping
from typing import Any

from doppeldata import issue_codes
from doppeldata.privacy_policy.column_treatment import Policy
from doppeldata.privacy_policy.literal_fields import FieldKind
from doppeldata.privacy_policy.literal_fields import LiteralField
from doppeldata.privacy_policy.literal_fields import RuleBreak
from doppeldata.profile_format import PointerParts
from doppeldata.profile_format import quantile_level


def end_table(ends: list[str]) -> str:
    """The table a relationship's `from` or `to` names, the text before the first dot of its first column."""
    return ends[0].split('.', 1)[0]


def relationship_k_min(relationship: dict[str, Any], tables: Mapping[str, Any]) -> int | None:
    """The larger `k_min` of the relationship's two tables, or None where either states a policy `profile check` does
    not check. A table the bundle lacks, or that cannot be read, states none."""
    floors = []
    for side in ('from', 'to'):
        table = tables.get(end_table(relationship[side]))
        if not isinstance(table, dict):
            continue
        privacy = table.get('privacy') or {}
        policy = Policy(privacy.get('policy') or Policy.DEFAULT)
        if not policy.checked:
            return None
        floors.append(policy.k_min(privacy.get('k_min')))
    return max((floor for floor in floors if floor is not None), default=Policy.DEFAULT.k_min(None))


def top_shares(relationship: dict[str, Any], at: PointerParts) -> LiteralField | None:
    """The relationship's top shares as one field, counting each share stated, or None where it states none."""
    stated = [share for share in ((relationship.get('fanout') or {}).get('top_fractions') or {}).values() if share is not None]
    return LiteralField((*at, 'fanout', 'top_fractions'), FieldKind.TOP_SHARES, len(stated)) if stated else None


def floor_breaks(
    relationship: dict[str, Any], at: PointerParts, tables: Mapping[str, Any], k_min: int
) -> list[tuple[FieldKind, RuleBreak]]:
    """Where the relationship's fan-out quantiles, orphan fraction or containment break `k_min`, each with its kind."""
    child, parent = end_table(relationship['from']), end_table(relationship['to'])
    found: list[tuple[FieldKind, RuleBreak]] = []
    quantiles = (relationship.get('fanout') or {}).get('quantiles') or {}
    if quantiles:
        parents = stated_rows(tables, parent)
        quantiles_at = (*at, 'fanout', 'quantiles')
        if parents is None:
            detail = f'the fan-out quantiles count the parents of {parent}, which states no rows; expected dataset.rows in {parent}'
            found.append((FieldKind.QUANTILES, RuleBreak(issue_codes.ROWS_NOT_STATED, quantiles_at, detail)))
        else:
            for name in quantiles:
                position = quantile_level(name) * (parents - 1)
                nearer = round(min(position, parents - 1 - position) + 1)
                if nearer < k_min:
                    detail = f'sits {nearer} of the {parents} {parent} rows from an end; expected at least {k_min}'
                    found.append((FieldKind.QUANTILES, RuleBreak(issue_codes.BELOW_K_MIN, (*quantiles_at, name), detail)))
    orphan_fraction = relationship.get('orphan_fraction') or 0.0
    containment = relationship.get('containment')
    orphan_keys = containment is not None and containment < 1
    if not orphan_fraction and not orphan_keys:
        return found
    children = stated_rows(tables, child)
    if children is None:
        field = 'orphan_fraction' if orphan_fraction else 'containment'
        detail = f'the orphan rows are a share of the rows of {child}, which states none; expected dataset.rows in {child}'
        found.append((FieldKind.ORPHAN_ROWS, RuleBreak(issue_codes.ROWS_NOT_STATED, (*at, field), detail)))
        return found
    orphans = round(orphan_fraction * children)
    if 0 < orphans < k_min:
        detail = f'comes to {orphans} of the {children} {child} rows; expected 0 or at least {k_min}'
        found.append((FieldKind.ORPHAN_ROWS, RuleBreak(issue_codes.BELOW_K_MIN, (*at, 'orphan_fraction'), detail)))
    if orphan_keys and orphans < k_min:
        detail = f'is {containment}, stating orphan keys, and their rows come to {orphans}; expected 1 below {k_min} orphan rows'
        found.append((FieldKind.ORPHAN_ROWS, RuleBreak(issue_codes.BELOW_K_MIN, (*at, 'containment'), detail)))
    return found


def stated_rows(tables: Mapping[str, Any], name: str) -> int | None:
    """The rows the table named states, None where the bundle lacks it, it cannot be read, or it states none."""
    table = tables.get(name)
    rows = (table.get('dataset') or {}).get('rows') if isinstance(table, dict) else None
    return int(rows) if rows is not None else None
