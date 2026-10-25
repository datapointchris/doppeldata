import datetime as dt
import math
import random

import polars as pl
from doppeldata import Profile
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
