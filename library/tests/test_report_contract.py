import dataclasses as dc
import json

from doppeldata.profile_comparison.field_changes import ChangeKind
from doppeldata.profile_comparison.field_changes import FieldChange
from doppeldata.profile_comparison.metrics import Bound
from doppeldata.profile_comparison.reports import MetricResult
from doppeldata.published_schemas import schema_text


def spelled(definition: dict) -> set[str]:
    """Every string a schema definition allows, from its `enum` and from each `const` under `oneOf`."""
    parts = definition.get('oneOf', [definition])
    return {value for part in parts for value in part.get('enum', [part['const']] if 'const' in part else [])}


def test_a_diff_report_format_change_made_in_rust_or_python_alone_fails_here():
    """No test validates a diff report against its schema, as the check tests do a check report."""
    schema = json.loads(schema_text('diff'))
    definitions = schema['$defs']
    assert {kind.value for kind in ChangeKind} == spelled(definitions['ChangeKind'])
    assert {bound.value for bound in Bound} == spelled(definitions['Bound'])
    assert {field.name for field in dc.fields(MetricResult)} == set(definitions['MetricResult']['properties'])
    assert {field.name for field in dc.fields(FieldChange)} == set(definitions['FieldChange']['properties'])
