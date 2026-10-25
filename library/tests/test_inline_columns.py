import datetime as dt
import json

import doppeldata as sd
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from doppeldata.published_schemas import enum_values
from doppeldata.published_schemas import schema_text
from doppeldata.short_profiles import providers

FIVE_DIGITS = r'^\d{5}$'


def test_sd_fakes_holds_every_provider_the_profile_schema_names():
    schema = json.loads(schema_text('profile'))
    assert sorted(providers()) == sorted(enum_values(schema['$defs']['Provider']))


def test_a_dict_of_fakes_and_ranges_generates_each_column_within_what_it_declares():
    table = pa.table(
        sd.generate(
            {
                'name': sd.fakes.name,
                'zip': sd.fakes.zip_code,
                'age': sd.integers(18, 90),
                'price': sd.decimals(0, 500, places=2),
                'signed_up': sd.dates('2023-01-01', '2025-12-31'),
                'plan': sd.choice(['free', 'pro', 'team'], weights=[0.7, 0.25, 0.05]),
                'score': sd.decimals(0, 1, nulls=0.05),
            },
            rows=1000,
            seed=0,
        )
    )
    assert pc.all(pc.match_substring_regex(table['zip'], FIVE_DIGITS)).as_py()
    assert pc.min_max(table['age']).as_py() == {'min': 18, 'max': 90}
    assert all(round(price, 2) == price for price in table['price'].to_pylist())
    signed_up = pc.min_max(table['signed_up']).as_py()
    assert dt.date(2023, 1, 1) <= signed_up['min'] and signed_up['max'] <= dt.date(2025, 12, 31)
    assert set(table['plan'].to_pylist()) == {'free', 'pro', 'team'}
    assert 20 < table['score'].null_count < 80


def test_a_list_of_names_takes_each_names_fake_and_a_name_hinting_none_is_refused_naming_the_closest():
    table = pa.table(sd.generate(['name', 'email', 'city', 'zip'], rows=100, seed=0))
    assert pc.all(pc.match_substring(table['email'], '@')).as_py()
    assert pc.all(pc.match_substring_regex(table['zip'], FIVE_DIGITS)).as_py()

    with pytest.raises(sd.InvalidProfile) as raised:
        sd.generate(['name', 'zipp'], rows=10)
    [issue] = raised.value.issues
    assert (issue.code, issue.closest[0]) == ('no-fake-for-name', 'zip_code')
