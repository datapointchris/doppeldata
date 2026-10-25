import dataclasses as dc
import json

import pytest
from doppeldata.profile_comparison.field_changes import ChangeKind
from doppeldata.profile_comparison.field_changes import FieldChange
from doppeldata.profile_comparison.metrics import Bound
from doppeldata.profile_comparison.reports import CHECK_VERSION
from doppeldata.profile_comparison.reports import DIFF_VERSION
from doppeldata.profile_comparison.reports import MetricResult
from doppeldata.published_schemas import schema_text

VERSIONS = {'check': CHECK_VERSION, 'diff': DIFF_VERSION}


def spelled(definition: dict) -> set[str]:
    """Every string a schema definition allows, from its `enum` and from each `const` under `oneOf`."""
    parts = definition.get('oneOf', [definition])
    return {value for part in parts for value in part.get('enum', [part['const']] if 'const' in part else [])}


@pytest.mark.parametrize('report', ['check', 'diff'])
def test_a_report_format_change_made_in_rust_or_python_alone_fails_before_a_report_fails_its_schema(report):
    schema = json.loads(schema_text(report))
    definitions = schema['$defs']
    assert schema['properties'][f'{report}_version']['const'] == VERSIONS[report]
    assert {kind.value for kind in ChangeKind} == spelled(definitions['ChangeKind'])
    assert {bound.value for bound in Bound} == spelled(definitions['Bound'])
    assert {field.name for field in dc.fields(MetricResult)} == set(definitions['MetricResult']['properties'])
    assert {field.name for field in dc.fields(FieldChange)} == set(definitions['FieldChange']['properties'])
