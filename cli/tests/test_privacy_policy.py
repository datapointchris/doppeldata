import datetime as dt
import json
import random

import polars as pl
from doppeldata import Profile
from doppeldata.example_profiles import example_resource
from doppeldata_cli.main import app
from typer.testing import CliRunner

MBI_LETTERS = 'ACDEFGHJKMNPQRTUVWXY'
DIGITS = '0123456789'
# Two prefixes on the restricted list, each among other prefixes common enough to keep.
ZIP_CODES = ('02139', '10001', '60614', '03601', '94110', '05901', '30301')


def npi_completed(payload: str) -> str:
    """`payload` and its NPI check digit: the Luhn digit over `80840` and the payload."""
    total = sum(sum(divmod(int(digit) * (2 - position % 2), 10)) for position, digit in enumerate(reversed('80840' + payload)))
    return payload + str(-total % 10)


def mbi(draw: random.Random) -> str:
    either = MBI_LETTERS + DIGITS
    classes = ('123456789', MBI_LETTERS, either, DIGITS, MBI_LETTERS, either, DIGITS, MBI_LETTERS, MBI_LETTERS, DIGITS, DIGITS)
    return ''.join(draw.choice(chars) for chars in classes)


def test_profile_check_passes_the_claims_example_and_points_at_a_three_row_top_value(tmp_path):
    runner = CliRunner()
    passed = runner.invoke(app, ['profile', 'check', 'claims'])
    assert passed.exit_code == 0, passed.output

    pending = '- {value: pending, fraction: 0.05}\n'
    edited = tmp_path / 'claims.profile.yaml'
    edited.write_text(
        example_resource('claims').read_text().replace(pending, pending + '        - {value: appealed, fraction: 0.000012}\n')
    )
    failed = runner.invoke(app, ['profile', 'check', str(edited), '--json'])

    assert failed.exit_code == 3, failed.output
    report = json.loads(failed.stdout)
    assert report['passed'] is False
    violations = report['tables'][0]['violations']
    assert {(violation['code'], violation['field']) for violation in violations} == {
        ('below-k-min', '/columns/10/string/top_values/3'),
        ('audit-values-mismatch', '/privacy/audit/21/values'),
    }


def test_profile_check_names_each_key_no_section_declares_by_its_pointer_in_a_bundle_and_its_table(tmp_path):
    table = Profile.example('orders').to_plain()
    table['nmae'] = 'orders'
    table['dataset']['row_cnt'] = 10
    table['columns'][0]['nul_rate'] = 0.1
    path = tmp_path / 'shop.json'
    path.write_text(json.dumps({'kind': 'bundle', 'name': 'shop', 'tables': {'orders': table}, 'relationship': []}))

    checked = CliRunner().invoke(app, ['profile', 'check', str(path), '--json'])

    assert checked.exit_code == 3, checked.output
    (refused,) = json.loads(checked.stdout)['tables']
    assert {(violation['code'], violation['field']) for violation in refused['violations']} == {
        ('unknown-field', '/relationship'),
        ('unknown-field', '/tables/orders/nmae'),
        ('unknown-field', '/tables/orders/dataset/row_cnt'),
        ('unknown-field', '/tables/orders/columns/0/nul_rate'),
    }


def test_healthcare_suppresses_identifiers_caps_ages_keeps_years_and_merges_restricted_zips(tmp_path):
    """A birth year over 89, an age held as text, a ZIP read as a float, a timestamp and a city are each withheld too."""
    rows, draw = 2000, random.Random(7)
    ages = [min(104, max(18, int(draw.gauss(60, 18)))) for _ in range(rows)]
    table = pl.DataFrame(
        {
            'dob': [dt.date(1930, 3, 1) + dt.timedelta(days=draw.randrange(365 * 69)) for _ in range(rows)],
            'member_age': ages,
            'age_at_admission': [str(age) for age in ages],
            'member_zip': [draw.choice(ZIP_CODES) for _ in range(rows)],
            'billing_zip': [float(draw.choice(ZIP_CODES)) for _ in range(rows)],
            'admittime': [dt.datetime(2025, 1, 1) + dt.timedelta(minutes=draw.randrange(60 * 24 * 300)) for _ in range(rows)],
            'patient_city': [draw.choice(('Boston', 'Chicago', 'Tiny Town')) for _ in range(rows)],
            'rendering_npi': [npi_completed(f'1{draw.randrange(10**7, 10**8)}') for _ in range(rows)],
            'mbi': [mbi(draw) for _ in range(rows)],
        }
    )
    table.write_parquet(tmp_path / 'claims.parquet')
    out = tmp_path / 'claims.profile.yaml'
    created = CliRunner().invoke(app, ['profile', 'create', str(tmp_path / 'claims.parquet'), '--policy', 'healthcare', '--out', str(out)])
    assert created.exit_code == 0, created.output

    profile = Profile.load(out).to_plain()
    columns = {column['name']: column for column in profile['columns']}
    assert {entry['column']: (entry['reason'], entry.get('semantic')) for entry in profile['privacy']['suppressed']} == {
        'dob': ('date_to_year', None),
        'member_age': ('age_over_89', None),
        'age_at_admission': ('age_over_89', None),
        'member_zip': ('zip_prefix', 'us_zip'),
        'billing_zip': ('identifier', None),
        'admittime': ('date_to_year', None),
        'patient_city': ('identifier', None),
        'rendering_npi': ('identifier', 'npi'),
        'mbi': ('identifier', 'mbi'),
    }
    dob = columns['dob']['temporal']['extremes']
    assert (dob['low'][4:], dob['high'][4:]) == ('-01-01', '-12-31')
    assert int(dob['low'][:4]) == int(profile['generated']['at'][:4]) - 90
    assert columns['admittime']['temporal']['extremes']['high'].startswith('2025-12-31')
    assert columns['member_age']['numeric']['extremes']['high'] == 90
    zip_prefixes = {entry['value'] for entry in columns['member_zip']['string']['prefixes']['values']}
    assert zip_prefixes == {'000', '021', '100', '606', '941', '303'}
    assert 'extremes' not in columns['billing_zip']['numeric']
    for withheld in ('age_at_admission', 'patient_city', 'rendering_npi', 'mbi'):
        assert 'top_values' not in columns[withheld]['string']
