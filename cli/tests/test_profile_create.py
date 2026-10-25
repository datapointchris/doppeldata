import datetime as dt
import math
import random
import uuid

import polars as pl
import pytest
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
