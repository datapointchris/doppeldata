import csv
import json
import re

import doppeldata as sd
import pytest
from doppeldata.published_schemas import enum_values
from doppeldata.published_schemas import schema_text
from doppeldata.short_profiles import providers


def written_rows(columns, tmp_path, rows, name='generated.csv'):
    """The rows `columns` generates at seed 0, read back from one CSV, because the library's tests install no Arrow reader."""
    out = tmp_path / name
    sd.write(columns, out, rows=rows, seed=0)
    with out.open(newline='') as handle:
        return list(csv.DictReader(handle))


def test_sd_fakes_holds_every_provider_the_profile_schema_names():
    schema = json.loads(schema_text('profile'))
    assert sorted(providers()) == sorted(enum_values(schema['$defs']['Provider']))


def test_a_dict_of_fakes_and_ranges_generates_each_column_within_what_it_declares(tmp_path):
    rows = written_rows(
        {
            'name': sd.fakes.name,
            'zip': sd.fakes.zip_code,
            'age': sd.integers(18, 90),
            'price': sd.decimals(0, 500, places=2),
            'signed_up': sd.dates('2023-01-01', '2025-12-31'),
            'plan': sd.choice(['free', 'pro', 'team'], weights=[0.7, 0.25, 0.05]),
            'score': sd.decimals(0, 1, nulls=0.05),
        },
        tmp_path,
        1000,
    )
    assert all(re.fullmatch(r'\d{5}', row['zip']) for row in rows)
    ages = [int(row['age']) for row in rows]
    assert (min(ages), max(ages)) == (18, 90)
    assert all(len(row['price'].partition('.')[2]) <= 2 for row in rows)
    assert all('2023-01-01' <= row['signed_up'] <= '2025-12-31' for row in rows)
    assert {row['plan'] for row in rows} == {'free', 'pro', 'team'}
    assert 20 < sum(row['score'] == '' for row in rows) < 80


def test_a_list_of_names_takes_each_names_fake_and_a_name_hinting_none_is_refused_naming_the_closest(tmp_path):
    """`--columns free_email` and `--columns state` draw as `sd.fakes.free_email` and `sd.fakes.state` do."""
    rows = written_rows(['name', 'email', 'city', 'zip'], tmp_path, 100)
    assert all('@' in row['email'] and re.fullmatch(r'\d{5}', row['zip']) for row in rows)

    named = written_rows(['free_email', 'state'], tmp_path, 100, 'named.csv')
    assert named == written_rows({'free_email': sd.fakes.free_email, 'state': sd.fakes.state}, tmp_path, 100, 'fakes.csv')

    with pytest.raises(sd.InvalidProfile) as raised:
        sd.generate(['name', 'zipp'], rows=10)
    [issue] = raised.value.issues
    assert (issue.code, issue.closest[0]) == ('E-PROFILE-021', 'zip_code')
