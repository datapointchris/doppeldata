"""Scenarios over generated data. They sit here because a check profiles with polars and pyarrow, which the CLI declares."""

import json

import doppeldata as sd
import jsonschema
import pyarrow as pa
import pyarrow.compute as pc
from doppeldata.published_schemas import schema_text

# Three engine blocks, so each perturbation draws across block boundaries.
ROWS = 150_000


def test_the_hot_key_example_gives_five_customers_its_share_and_a_check_verifies_it():
    orders, hot_key = sd.Profile.example('orders'), sd.Scenario.example('hot-key')
    table = pa.table(sd.generate(orders, rows=ROWS, scenario=hot_key))
    counts = sorted(pc.value_counts(table['customer_id']).field('counts').to_pylist(), reverse=True)
    assert abs(sum(counts[:5]) / table.num_rows - 0.6) <= 0.01

    report = sd.check(orders, table, scenario=hot_key)
    jsonschema.validate(report.to_plain(), json.loads(schema_text('check')))
    assert report.verified
    assert {result.skipped_by for result in report.metrics if result.column == 'customer_id'} == {'hot_keys:customer_id:0'}


def test_inserting_a_perturbation_leaves_the_rows_every_other_one_writes_unchanged():
    orders = sd.Profile.example('orders')
    others = [
        {'op': 'hot_keys', 'column': 'customer_id', 'keys': 5, 'share': 0.6},
        {'op': 'missingness', 'column': 'amount', 'mechanism': 'MAR', 'depends_on': 'quantity', 'rate': 0.2},
        {'op': 'out_of_range', 'column': 'quantity', 'rate': 0.01, 'values': [-1, 0]},
        {
            'op': 'late_arrival',
            'column': 'ordered_at',
            'rate': 0.05,
            'lag': {'family': 'lognormal', 'median': '2d'},
            'ingest': 'ingested_at',
        },
        {'op': 'categorical_shift', 'column': 'status', 'to': {'returned': 0.3}},
    ]
    inserted = {'op': 'broken_characters', 'column': 'customer_email', 'rate': 0.5}
    before = pa.table(sd.generate(orders, rows=ROWS, scenario=sd.Scenario({'scenario': 'before', 'perturbations': others})))
    after = pa.table(sd.generate(orders, rows=ROWS, scenario=sd.Scenario({'scenario': 'after', 'perturbations': [inserted, *others]})))
    assert before.column_names == after.column_names
    assert [name for name in before.column_names if not before[name].equals(after[name])] == ['customer_email']
