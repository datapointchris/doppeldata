import copy
import json
from pathlib import Path

import jsonschema
from doppeldata import Profile
from doppeldata import diff
from doppeldata.published_schemas import schema_text

ORDERS = Path(__file__).parent / 'fixtures' / 'orders.profile.yaml'


def test_a_reviewer_changing_the_csv_delimiter_gets_one_dialect_change_at_its_writer_key_in_a_report_its_schema_accepts():
    earlier = Profile.load(ORDERS)
    document = copy.deepcopy(earlier.to_plain())
    document['source']['csv']['writer']['delimiter'] = '|'
    report = json.loads(diff(earlier, Profile(document)).to_json())
    jsonschema.Draft202012Validator(json.loads(schema_text('diff'))).validate(report)
    assert [(change['change'], change['field']) for change in report['changes']] == [
        ('csv_dialect_changed', '/source/csv/writer/delimiter')
    ]


def test_a_two_value_column_holding_one_value_more_or_less_passes_its_distinct_ratio_and_two_more_fails_it():
    earlier = Profile.load(ORDERS)

    def region_delta(values: int) -> tuple[float, float, bool]:
        document = copy.deepcopy(earlier.to_plain())
        region = next(column for column in document['columns'] if column['name'] == 'region')
        region['distinct'] = {'approx_values': values, 'ratio': values / document['dataset']['rows']}
        report = json.loads(diff(earlier, Profile(document)).to_json())
        row = next(row for row in report['metrics'] if row['metric'] == 'distinct_ratio_delta' and row['column'] == 'region')
        return row['value'], row['threshold'], row['passed']

    assert [region_delta(values) for values in (1, 3, 4)] == [(0.5, 0.75, True), (0.5, 0.75, True), (1.0, 0.75, False)]
