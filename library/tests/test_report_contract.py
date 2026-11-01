import copy
import json
from pathlib import Path

import jsonschema
from doppeldata import Profile
from doppeldata import check
from doppeldata import diff
from doppeldata import write
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


def test_a_check_of_a_write_to_one_file_with_its_manifest_leaves_the_layout_unmeasured_in_a_report_its_schema_accepts(tmp_path):
    profile = Profile.example('events-json')
    written = write(profile, tmp_path / 'events.ndjson', rows=2000, writer={'compression': 'none'}, manifest_beside=True)
    report = json.loads(check(profile, tmp_path / 'events.ndjson').to_json())
    jsonschema.Draft202012Validator(json.loads(schema_text('check'))).validate(report)
    assert (report['data']['dataset_sha256'], report['data']['one_file']) == (written.dataset_sha256, 'events.ndjson')
    layout = [result for result in report['metrics'] if result['metric'] in ('files_delta', 'rows_per_file_delta')]
    assert [(result['metric'], result.get('value'), result['decides']) for result in layout] == [
        ('files_delta', None, False),
        ('rows_per_file_delta', None, False),
    ]
    assert all('one file, events.ndjson' in result['unmeasured'] for result in layout)
