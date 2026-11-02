import copy
import json
from pathlib import Path

import jsonschema
from doppeldata import Profile
from doppeldata import diff
from doppeldata.profile_comparison.metrics import CORRELATION_THRESHOLD
from doppeldata.profile_comparison.metrics import Bound
from doppeldata.profile_comparison.metrics import Compared
from doppeldata.profile_comparison.metrics import correlation_changes
from doppeldata.published_schemas import schema_text

ORDERS = Path(__file__).parent / 'fixtures' / 'orders.profile.yaml'
EVERY_COLUMN_KIND = Path(__file__).parents[2] / 'crates' / 'core' / 'tests' / 'every-column-kind.profile.yaml'


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


def test_a_copula_pair_moved_past_the_threshold_passes_within_three_standard_errors_of_its_change():
    source = copy.deepcopy(Profile.load(EVERY_COLUMN_KIND).to_plain())
    stated = next(entry for entry in source['dataset']['correlations'] if entry['method'] == 'spearman')
    stated['sample_rows'] = 20_000

    def judged(influence_variance: float | None) -> tuple[float, bool]:
        observed = copy.deepcopy(source)
        moved = next(entry for entry in observed['dataset']['correlations'] if entry['method'] == 'spearman')
        moved |= {'value': stated['value'] - 0.08, 'sample_rows': 200_000}
        if influence_variance is not None:
            moved['influence_variance'] = influence_variance
        change = next(change for change in correlation_changes(source, observed) if change.column == stated['a'])
        assert isinstance(change.compared, Compared)
        held_to = Bound.AT_MOST.held_to(CORRELATION_THRESHOLD, change.compared)
        return round(held_to, 4), change.compared.value <= held_to

    assert [judged(variance) for variance in (None, 10.0, 20.0)] == [(0.05, False), (0.0704, False), (0.0995, True)]
