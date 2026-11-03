"""A generated dataset checked against its profile. It sits here because profiling needs polars and pyarrow, which the CLI declares."""

import copy
import datetime as dt
import json
import random

import doppeldata as sd
import jsonschema
import pyarrow as pa
import pyarrow.compute as pc
import pytest
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


def test_a_copula_pair_moved_past_the_threshold_fails_it_and_passes_where_its_profile_measured_it_on_few_rows():
    """Two samples of one distribution move a pair measured on few rows further than the threshold."""
    draw = random.Random(7)
    x = [draw.gauss(0, 1) for _ in range(ROWS)]
    profile = sd.profile(pa.table({'x': x, 'y': [value + draw.gauss(0, 1) for value in x]}))
    generated = pa.table(sd.generate(profile, rows=GENERATED_ROWS, seed=1))

    def judged(sample_rows: int | None) -> tuple[bool, bool]:
        document = copy.deepcopy(profile.to_plain())
        stated = next(entry for entry in document['dataset']['correlations'] if entry['method'] == 'spearman')
        stated['value'] -= 0.08
        stated['sample_rows'] = sample_rows
        if sample_rows is None:
            del stated['sample_rows']
        row = next(
            result for result in sd.check(sd.Profile(document), generated).metrics if result.metric == 'correlation_worst_pair_change'
        )
        return row.threshold > 0.05, row.passed

    assert [judged(sample_rows) for sample_rows in (None, 200)] == [(False, False), (True, True)]


def item_codes(five_digit: int) -> pa.Table:
    """10,000 codes, `five_digit` of them five digits, a ZIP code's shape, and the rest a HCPCS Level II code's."""
    draw = random.Random(7)
    codes = [f'{draw.randrange(10**5):05d}' for _ in range(five_digit)]
    codes += [f'G{draw.randrange(10**4):04d}' for _ in range(10_000 - five_digit)]
    draw.shuffle(codes)
    return pa.table({'item_code': codes})


def retypings(report: sd.CheckReport) -> list[tuple[str | None, str | None, bool]]:
    return [
        (change.expected, change.observed, change.passed) for change in report.changes if change.change.value == 'semantic_type_changed'
    ]


def semantic_type(profile: sd.Profile) -> str | None:
    return (profile.to_plain()['columns'][0].get('semantic') or {}).get('type')


def test_a_share_sampling_carries_across_the_type_cut_keeps_the_profiles_typing_and_one_clear_of_it_fails_the_check():
    """A ZIP code's validator types at 95% of the sampled values, and two samples of 10,000 at that share lie about 0.3%
    apart, so a check holds 94.6% and 95.3% to one typing. A profile written against an earlier one types from its own
    data, so the privacy policy treats a ZIP column as one."""
    untyped, typed = sd.profile(item_codes(9_460)), sd.profile(item_codes(9_900))
    assert [semantic_type(untyped), semantic_type(typed)] == [None, 'us_zip']
    with pytest.warns(sd.EarlierProfileNotReused):
        assert semantic_type(sd.profile(item_codes(9_530), edges_from=untyped)) == 'us_zip'

    assert retypings(sd.check(untyped, item_codes(9_530))) == [(None, 'us_zip', True)]
    assert retypings(sd.check(typed, item_codes(9_460))) == [('us_zip', None, True)]
    assert retypings(sd.check(untyped, item_codes(9_900))) == [(None, 'us_zip', False)]
    assert retypings(sd.check(typed, item_codes(9_000))) == [('us_zip', None, False)]


def item_lines(tmp_path, five_digit: int) -> str:
    """`item_codes`' codes as JSON lines, each the one element of a record's `lines`."""
    path = tmp_path / f'{five_digit}.jsonl'
    codes = item_codes(five_digit).column('item_code').to_pylist()
    path.write_text(''.join(json.dumps({'lines': [{'code': code}]}) + '\n' for code in codes))
    return str(path)


def test_a_json_array_elements_type_change_fails_a_diff_and_a_check_holds_it_as_it_holds_a_columns(tmp_path):
    untyped, flipped = sd.profile(item_lines(tmp_path, 9_460)), sd.profile(item_lines(tmp_path, 9_530))

    diff = sd.diff(untyped, flipped)
    assert retypings(diff) == [(None, 'us_zip', False)]
    assert [(change.column, change.field) for change in diff.changes] == [
        ('lines.element.code', '/columns/0/list/element/struct/fields/0/semantic/type')
    ]
    assert retypings(sd.check(untyped, item_lines(tmp_path, 9_530))) == [(None, 'us_zip', True)]
    assert retypings(sd.check(untyped, item_lines(tmp_path, 9_900))) == [(None, 'us_zip', False)]


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
