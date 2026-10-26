import datetime as dt
import math
import random
import uuid

import polars as pl
import pytest
from doppeldata import Bundle
from doppeldata import Profile
from doppeldata import profile
from doppeldata_cli.main import app
from typer.testing import CliRunner

# Fields only a text source has, because its values were parsed from text.
TEXT_ONLY_FIELDS = {'type_mix', 'format'}
# Moments are sums whose order follows the reader's batches, so they may differ in their last digit.
MOMENTS = {'mean', 'std', 'skew', 'kurtosis'}


def leaves(value, at=()):
    if isinstance(value, dict):
        for key, item in value.items():
            if key not in TEXT_ONLY_FIELDS:
                yield from leaves(item, (*at, key))
    elif isinstance(value, list) and value and isinstance(value[0], dict):
        for index, item in enumerate(value):
            yield from leaves(item, (*at, index))
    else:
        yield at, value


def test_parquet_through_polars_and_csv_through_duckdb_profile_to_the_same_fields_and_counts(tmp_path):
    rows = 3000
    start = dt.datetime(2025, 1, 1)
    table = pl.DataFrame(
        {
            'order_id': range(1, rows + 1),
            'amount': [None if i % 40 == 0 else round(1.5 + (i * 7919 % 1000) / 7, 2) for i in range(rows)],
            'status': [('shipped', 'delivered', 'returned', 'lost')[i % 7 % 4] for i in range(rows)],
            'code': [f'ORD{i * 7919 % 10**8:08d}' for i in range(rows)],
            'ordered_at': [start + dt.timedelta(minutes=i * 97) for i in range(rows)],
            'paid': [i % 3 != 0 for i in range(rows)],
        }
    )
    table.write_parquet(tmp_path / 'orders.parquet')
    table.write_csv(tmp_path / 'orders.csv')

    profiles = {}
    for name in ('orders.parquet', 'orders.csv'):
        out = tmp_path / f'{name}.profile.yaml'
        result = CliRunner().invoke(app, ['profile', 'create', str(tmp_path / name), '--out', str(out)])
        assert result.exit_code == 0, result.output
        profiles[name] = dict(leaves(Profile.load(out).to_plain()['columns']))

    parquet, csv = profiles['orders.parquet'], profiles['orders.csv']
    assert parquet.keys() == csv.keys()
    assert {path: value for path, value in parquet.items() if path[-1] not in MOMENTS} == {
        path: value for path, value in csv.items() if path[-1] not in MOMENTS
    }


def luhn_completed(payload: str) -> str:
    total = sum(sum(divmod(int(digit) * (2 - position % 2), 10)) for position, digit in enumerate(reversed(payload)))
    return payload + str(-total % 10)


def test_the_general_pack_types_each_column_of_the_mixed_fixture(tmp_path):
    rows, draw = 10_000, random.Random(11)
    corrupted = set(draw.sample(range(rows), 30))
    cards = [luhn_completed(draw.choice(['4', '51', '37']) + ''.join(draw.choices('0123456789', k=13))) for _ in range(rows)]
    cards = [card[:-1] + str((int(card[-1]) + 1) % 10) if index in corrupted else card for index, card in enumerate(cards)]
    phones = [draw.randrange(2_000_000_000, 9_999_999_999) for _ in range(300)]
    table = pl.DataFrame(
        {
            'contact': [f'user{index}@mail{draw.randrange(9)}.example.com' for index in range(rows)],
            'card_number': cards,
            'ssn': [f'{draw.randint(1, 665):03d}-{draw.randint(1, 99):02d}-{draw.randint(1, 9999):04d}' for _ in range(rows)],
            'event_id': [str(uuid.UUID(int=draw.getrandbits(128), version=4)) for _ in range(rows)],
            'client_ip': ['.'.join(str(draw.randrange(256)) for _ in range(4)) for _ in range(rows)],
            'ship_zip': [f'{draw.randrange(100_000):05d}' for _ in range(rows)],
            'state': [draw.choice(['CA', 'NY', 'TX', 'WA', 'FL', 'MA', 'IL', 'OH']) for _ in range(rows)],
            'last_name_id': draw.sample(range(10**6), rows),
            'phone': [draw.choice(phones) for _ in range(rows)],
        }
    )
    table.write_parquet(tmp_path / 'mixed.parquet')
    out = tmp_path / 'mixed.profile.yaml'
    result = CliRunner().invoke(app, ['profile', 'create', str(tmp_path / 'mixed.parquet'), '--out', str(out)])
    assert result.exit_code == 0, result.output

    columns = {column['name']: column for column in Profile.load(out).to_plain()['columns']}
    assert {name: (column.get('semantic') or {}).get('type') for name, column in columns.items()} == {
        'contact': 'email',
        'card_number': 'credit_card',
        'ssn': 'us_ssn',
        'event_id': 'uuid',
        'client_ip': 'ipv4_address',
        'ship_zip': 'us_zip',
        'state': 'us_state',
        'last_name_id': None,
        'phone': 'phone_number',
    }
    assert columns['card_number']['quality']['invalid_rate'] == pytest.approx(0.003, abs=0.001)
    # An integer identifier repeated across rows, a phone number here, lists none of its values.
    assert 'top_values' not in columns['phone']['numeric']


def test_a_log_uniform_column_is_fitted_as_the_loguniform_the_generator_reads(tmp_path):
    draw = random.Random(5)
    amounts = [math.exp(draw.uniform(0, 5)) for _ in range(100_000)]
    pl.DataFrame({'amount': amounts}).write_parquet(tmp_path / 'amounts.parquet')
    out = tmp_path / 'amounts.profile.yaml'
    result = CliRunner().invoke(app, ['profile', 'create', str(tmp_path / 'amounts.parquet'), '--out', str(out)])
    assert result.exit_code == 0, result.output

    fit = Profile.load(out).to_plain()['columns'][0]['numeric']['fit']
    assert fit['family'] == 'loguniform'
    assert fit['ks'] < 0.02


def test_no_histogram_bin_less_the_listed_values_in_it_leaves_a_cell_under_k_min_rows():
    """Rare values beside listed ones in an outer bin, as 5 zeros below 1,000 ones, are not recovered by subtracting the list."""
    values = [0] * 5 + [1] * 1000 + [2] * 1000 + [3] * 11 + [9] * 6
    numeric = profile(pl.DataFrame({'v': values})).to_plain()['columns'][0]['numeric']
    listed = {entry['value']: round(entry['rate'] * len(values)) for entry in numeric['top_values']}
    histogram = numeric['histogram']
    for low, high, count in zip(histogram['edges'], histogram['edges'][1:], histogram['counts'], strict=False):
        outside = count - sum(rows for value, rows in listed.items() if low <= value <= high)
        assert not 0 < outside < 11
    assert not 0 < round(numeric['zero_rate'] * len(values)) < 11


def test_a_profile_states_correlations_a_dependency_floored_conditional_cells_and_a_gapped_id_as_a_sequence():
    draw, rows = random.Random(3), 4000
    amounts = [round(draw.lognormvariate(3, 1), 2) for _ in range(rows)]
    regions = [('north', 'south', 'east')[i % 3] for i in range(rows)]
    channels = [
        {'north': 'web', 'south': 'store', 'east': 'phone'}[region] if draw.random() < 0.8 else draw.choice(['web', 'store', 'phone'])
        for region in regions
    ]
    # Six north rows and twenty south rows on fax. The north cell is under k_min, so neither fax's rows outside the kept
    # cells nor the north row's rows outside its kept cells may come to six.
    fax = {*range(0, 18, 3), *range(1, 61, 3)}
    channels = ['fax' if index in fax else channel for index, channel in enumerate(channels)]
    frame = pl.DataFrame(
        {
            # Ids left after a fifth of them were rejected, still rising.
            'order_id': sorted(draw.sample(range(1, rows * 5 // 4 + 1), rows)),
            'amount': amounts,
            'fee': [round(amount / 20 + draw.uniform(0, 0.5), 2) for amount in amounts],
            'plan': [f'p{i % 40:02d}' for i in range(rows)],
            'tier': [('bronze', 'silver', 'gold', 'platinum')[i % 40 // 10] for i in range(rows)],
            'region': regions,
            'channel': channels,
        }
    )
    profiled = profile(frame).to_plain()
    dataset = profiled['dataset']

    assert dataset['keys']['primary']['columns'] == ['order_id']
    pairs = {(entry['a'], entry['b']): entry for entry in dataset['correlations']}
    assert pairs['amount', 'fee']['method'] == 'spearman'
    assert pairs['amount', 'fee']['value'] > 0.9
    assert {'kind': 'functional', 'lhs': ['plan'], 'rhs': 'tier', 'violation_rate': 0.0} in dataset['dependencies']
    conditionals = {(entry['given'], entry['column']): entry for entry in dataset['conditionals']}
    assert {('tier', 'plan'), ('region', 'channel')} <= conditionals.keys()
    for (given, child), entry in conditionals.items():
        counted = {(row[given], row[child]): row['len'] for row in frame.group_by(given, child).len().iter_rows(named=True)}
        given_rows = dict(frame.group_by(given).len().iter_rows())
        assert all(counted[value, written] >= 11 for value, row in entry['table'].items() for written in row)
        assert all(
            not 0 < given_rows[value] - sum(counted[value, written] for written in row) < 11 for value, row in entry['table'].items()
        )
    assert not any('fax' in row for row in conditionals['region', 'channel']['table'].values())
    # A sample of a quarter of the rows holds a quarter of the ids, and still states the whole table's sequence.
    for sample in (None, rows // 4):
        sampled = profiled if sample is None else profile(frame, sample=sample).to_plain()
        order_id = next(column for column in sampled['columns'] if column['name'] == 'order_id')
        assert order_id['numeric']['sequence'] == {'start': frame['order_id'][10] - 10, 'step': 1}


def test_two_folders_named_with_tables_are_two_tables_and_a_column_naming_the_other_s_key_is_a_foreign_key(tmp_path):
    customers = pl.DataFrame({'customer_id': range(1, 201), 'segment': [('retail', 'online')[i % 2] for i in range(200)]})
    # One order in twenty names one of seven customers the table lacks.
    orders = pl.DataFrame({'order_id': range(1, 2001), 'customer_id': [1 + i % 200 if i % 20 else 201 + i % 7 for i in range(2000)]})
    for name, table in (('customers', customers), ('orders', orders)):
        (tmp_path / name).mkdir()
        table.write_parquet(tmp_path / name / 'part.parquet')
    out = tmp_path / 'shop.yaml'
    named = [str(tmp_path / 'customers'), str(tmp_path / 'orders')]
    result = CliRunner().invoke(app, ['profile', 'create', *named, '--tables', '--out', str(out)])
    assert result.exit_code == 0, result.output

    bundle = Bundle.load(out).to_plain()
    keys = {name: table['dataset']['keys']['primary']['columns'] for name, table in bundle['tables'].items()}
    assert keys == {'customers': ['customer_id'], 'orders': ['order_id']}
    [relationship] = bundle['relationships']
    assert {field: relationship[field] for field in ('from', 'to', 'cardinality', 'orphan_rate')} == {
        'from': 'orders.customer_id',
        'to': 'customers.customer_id',
        'cardinality': 'many_to_one',
        'orphan_rate': 0.05,
    }
    assert relationship['containment'] == pytest.approx(190 / 197, rel=1e-3)


def test_drifted_hive_folders_are_one_table_and_unlike_folders_are_refused_unless_tables_writes_a_bundle(tmp_path):
    orders = pl.DataFrame({'order_id': range(60), 'amount': [i * 1.5 for i in range(60)]})
    customers = pl.DataFrame({'customer_id': range(60), 'region': [('north', 'south')[i % 2] for i in range(60)]})
    # A later partition widens a column and adds one, which leaves under 70 percent of the typed columns shared.
    drifted = {2023: orders.with_columns(pl.col('order_id').cast(pl.Int32)), 2024: orders.with_columns(channel=pl.lit('web'))}
    for year, table in drifted.items():
        (tmp_path / 'hive' / f'year={year}').mkdir(parents=True)
        table.write_parquet(tmp_path / 'hive' / f'year={year}' / 'part.parquet')
        # Hive folders of CSV, whose keys a text read must type from their text.
        (tmp_path / 'hive-csv' / f'year={year}').mkdir(parents=True)
        orders.write_csv(tmp_path / 'hive-csv' / f'year={year}' / 'part.csv')
    for name, table in (('orders_2023', orders), ('orders_2024', orders), ('customers', customers)):
        (tmp_path / 'mixed' / name).mkdir(parents=True)
        table.write_parquet(tmp_path / 'mixed' / name / 'part.parquet')

    for tree in ('hive', 'hive-csv'):
        result = CliRunner().invoke(app, ['profile', 'create', str(tmp_path / tree), '--out', str(tmp_path / f'{tree}.yaml')])
        assert result.exit_code == 0, result.output
        hive = Profile.load(tmp_path / f'{tree}.yaml').to_plain()
        assert [key['name'] for key in hive['source']['layout']['partitioning']['keys']] == ['year']
        assert hive['dataset']['row_count'] == 120

    refused = CliRunner().invoke(app, ['profile', 'create', str(tmp_path / 'mixed')])
    assert refused.exit_code == 2
    bundled = CliRunner().invoke(app, ['profile', 'create', str(tmp_path / 'mixed'), '--tables', '--out', str(tmp_path / 'mixed.yaml')])
    assert bundled.exit_code == 0, bundled.output
    tables = Bundle.load(tmp_path / 'mixed.yaml').tables
    assert {name: table.to_plain()['dataset']['row_count'] for name, table in tables.items()} == {'customers': 60, 'orders': 120}
