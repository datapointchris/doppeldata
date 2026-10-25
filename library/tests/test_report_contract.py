import json

import pytest
from doppeldata.profile_comparison.field_changes import ChangeKind
from doppeldata.profile_comparison.metrics import Bound
from doppeldata.published_schemas import schema_text


def spelled(definition: dict) -> set[str]:
    """Every string a schema definition allows, from its `enum` and from each `const` under `oneOf`."""
    parts = definition.get('oneOf', [definition])
    return {value for part in parts for value in part.get('enum', [part['const']] if 'const' in part else [])}


@pytest.mark.parametrize('report', ['check', 'diff'])
def test_a_change_kind_or_bound_added_in_rust_or_python_alone_fails_before_a_report_fails_its_schema(report):
    definitions = json.loads(schema_text(report))['$defs']
    assert {kind.value for kind in ChangeKind} == spelled(definitions['ChangeKind'])
    assert {bound.value for bound in Bound} == spelled(definitions['Bound'])
