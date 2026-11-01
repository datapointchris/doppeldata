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
