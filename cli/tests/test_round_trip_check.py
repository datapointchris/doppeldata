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
    """Columns the generator holds every metric for: continuous floats, few categories, unique codes, seconds, flags, and a
    numeric code whose 900 values each hold fewer than k_min rows of a quarter-size run.
    """
    draw = random.Random(3)
    start = dt.datetime(2025, 3, 1)
    return pa.table(
        {
            'amount': [None if draw.random() < 0.04 else draw.lognormvariate(3, 0.8) for _ in range(ROWS)],
            'status': draw.choices(['shipped', 'delivered', 'returned', 'lost'], weights=[60, 30, 8, 2], k=ROWS),
            'code': [f'ORD{index:06d}' for index in draw.sample(range(10**6), ROWS)],
            'ordered_at': [start + dt.timedelta(seconds=draw.randrange(30 * 86_400)) for _ in range(ROWS)],
            'paid': [draw.random() < 0.7 for _ in range(ROWS)],
            'store_id': [draw.randrange(900) for _ in range(ROWS)],
        }
    )


def failures(report: sd.CheckReport) -> list[tuple[str, str | None]]:
    return [(result.metric, result.column) for result in report.metrics if not result.passed]


def tiered_regions(express: dict[str, int]) -> pa.Table:
    """600 rows of a tier and a region: the standard tier's regions fixed, and the express tier's as `express` counts them."""
    standard = {'north': 150, 'south': 90, 'east': 60}
    rows = [('standard', region) for region, count in standard.items() for _ in range(count)]
    rows += [('express', region) for region, count in express.items() for _ in range(count)]
    return pa.table({'tier': [tier for tier, _ in rows], 'region': [region for _, region in rows]})


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
    assert failures(report) == [('null_fraction_delta', 'amount')]


def test_a_few_rows_outside_a_conditional_tables_cells_pass_conditional_kl_and_a_shifted_row_fails_it():
    """Three express rows of east lie outside every cell the table keeps. A profile under k_min would drop east from the
    standard row as well, so the check counts the table's own cells without a floor."""
    profile = sd.profile(tiered_regions({'north': 60, 'south': 90, 'west': 150}))
    assert [(entry['given'], entry['column']) for entry in profile.to_plain()['dataset']['conditionals']] == [('tier', 'region')]

    report = sd.check(profile, tiered_regions({'north': 60, 'south': 90, 'west': 147, 'east': 3}))
    assert report.passed, failures(report)

    report = sd.check(profile, tiered_regions({'north': 150, 'south': 90, 'west': 60}))
    assert ('conditional_kl', 'region') in failures(report)


def test_the_orders_example_generated_at_a_tenth_of_its_rows_keeps_every_quantity_and_passes_its_check():
    """`data generate orders --rows 10000`: `quantity` lists none of its 10 values, and a short run still draws them all."""
    profile = sd.Profile.example('orders')
    generated = pa.table(sd.generate(profile, rows=10_000, seed=1))
    assert len(pc.unique(generated.column('quantity'))) == 10
    report = sd.check(profile, generated)
    assert report.passed, failures(report)


def test_a_check_of_a_write_to_one_file_with_its_manifest_leaves_the_layout_unmeasured_in_a_report_its_schema_accepts(tmp_path):
    """`events-json` records 24 partitioned files, which one file of 2,000 rows would miss were its layout scored."""
    profile = sd.Profile.example('events-json')
    written = sd.write(profile, tmp_path / 'events.ndjson', rows=2000, writer={'compression': 'none'}, manifest_beside=True)
    report = sd.check(profile, tmp_path / 'events.ndjson')
    jsonschema.Draft202012Validator(json.loads(schema_text('check'))).validate(report.to_plain())
    assert (report.data.dataset_sha256, report.data.one_file) == (written.dataset_sha256, 'events.ndjson')
    layout = [result for result in report.metrics if result.metric in ('files_delta', 'rows_per_file_delta')]
    assert [(result.metric, result.value, result.decides) for result in layout] == [
        ('files_delta', None, False),
        ('rows_per_file_delta', None, False),
    ]
    assert all('one file, events.ndjson' in (result.unmeasured or '') for result in layout)
