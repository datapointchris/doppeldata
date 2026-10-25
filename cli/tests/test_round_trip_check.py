"""A generated dataset checked against its profile. It sits here because profiling needs polars and pyarrow, which the CLI declares."""

import datetime as dt
import json
import random

import doppeldata as sd
import jsonschema
import pyarrow as pa
import pyarrow.compute as pc
from doppeldata.published_schemas import schema_text

ROWS = 20_000
# Generated at a quarter of the source's rows, as `data generate --rows` asks for a smaller sample.
GENERATED_ROWS = ROWS // 4


def source_table() -> pa.Table:
    """Columns the generator holds every metric for: continuous floats, few categories, unique codes, seconds and flags."""
    draw = random.Random(3)
    start = dt.datetime(2025, 3, 1)
    return pa.table(
        {
            'amount': [None if draw.random() < 0.04 else draw.lognormvariate(3, 0.8) for _ in range(ROWS)],
            'status': draw.choices(['shipped', 'delivered', 'returned', 'lost'], weights=[60, 30, 8, 2], k=ROWS),
            'code': [f'ORD{index:06d}' for index in draw.sample(range(10**6), ROWS)],
            'ordered_at': [start + dt.timedelta(seconds=draw.randrange(30 * 86_400)) for _ in range(ROWS)],
            'paid': [draw.random() < 0.7 for _ in range(ROWS)],
        }
    )


def failures(report: sd.CheckReport) -> list[tuple[str, str | None]]:
    return [(result.metric, result.column) for result in report.metrics if not result.passed]


def test_data_generated_at_fewer_rows_than_its_source_passes_its_check_and_a_raised_null_rate_fails_it_by_name():
    profile = sd.profile(source_table())
    generated = pa.table(sd.generate(profile, rows=GENERATED_ROWS, seed=1))
    schema = jsonschema.Draft202012Validator(json.loads(schema_text('check')))

    report = sd.check(profile, generated)
    schema.validate(report.to_plain())
    assert report.passed, failures(report)

    draw = random.Random(5)
    nulled = pa.array([draw.random() < 0.05 for _ in range(generated.num_rows)])
    amount = generated.column('amount').combine_chunks()
    patched_amount = pc.if_else(nulled, pa.scalar(None, amount.type), amount)
    patched = generated.set_column(generated.schema.get_field_index('amount'), 'amount', patched_amount)
    report = sd.check(profile, patched)
    schema.validate(report.to_plain())
    assert failures(report) == [('null_rate_delta', 'amount')]
