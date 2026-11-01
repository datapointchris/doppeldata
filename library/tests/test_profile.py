import copy
import json
import math
import re
from pathlib import Path

import doppeldata as sd
import pytest
from doppeldata import Bundle
from doppeldata import InvalidProfile
from doppeldata import Profile
from doppeldata import SourceError
from doppeldata.example_profiles import example_names

EXAMPLE = Path(__file__).parent / 'fixtures' / 'orders.profile.yaml'


@pytest.mark.parametrize('name', example_names())
def test_every_shipped_example_is_a_valid_profile_named_as_its_file(name):
    assert Profile.example(name).document['name'] == name


def write_json(path: Path, document: dict) -> Path:
    path.write_text(json.dumps(document))
    return path


def test_the_digest_ignores_format_comments_key_order_and_when_and_by_what_it_was_profiled(tmp_path):
    original = Profile.load(EXAMPLE)
    assert re.fullmatch(r'sha256:[0-9a-f]{64}', original.digest)
    reordered = dict(reversed(list(original.to_plain().items())))
    reordered['generated'] |= {'at': '2030-01-01T00:00:00Z', 'tool': 'doppeldata 9.9.9'}
    assert Profile.load(write_json(tmp_path / 'orders.json', reordered)).digest == original.digest

    reordered['dataset']['rows'] += 1
    assert Profile.load(write_json(tmp_path / 'orders.json', reordered)).digest != original.digest


def test_saving_changes_only_the_edited_value(tmp_path):
    text = EXAMPLE.read_text()
    path = tmp_path / 'orders.profile.yaml'
    path.write_text(text)
    profile = Profile.load(path)
    profile.document['dataset']['rows'] = 42
    profile.save()
    assert path.read_text() == text.replace('rows: 10000000', 'rows: 42')


@pytest.mark.parametrize(
    ('edit', 'expected'),
    [
        (lambda p: p['columns'][1].update(null_fraction=1.5), {('E-PROFILE-009', '/columns/1/null_fraction')}),
        (lambda p: p['dataset']['keys'].update(primary={'uniqueness': 1.0}), {('E-PROFILE-005', '/dataset/keys/primary')}),
        (lambda p: p['columns'][1]['numeric'].update(skew=math.nan), {('E-PROFILE-003', '/columns/1/numeric/skew')}),
        (lambda p: p['columns'][1]['numeric']['quantiles'].update(p5=1.0), {('E-PROFILE-010', '/columns/1/numeric/quantiles')}),
        (
            lambda p: (
                p['columns'][1]['numeric']['quantiles'].update(p95=50.0),
                p['columns'][3]['temporal'].update(weekday_fractions=[0.1] * 7),
                p['columns'][1]['numeric']['histogram'].update(bin_rows=[1, 2, 3]),
            ),
            {
                ('E-PROFILE-108', '/columns/1/numeric/quantiles/p95'),
                ('E-PROFILE-014', '/columns/3/temporal/weekday_fractions'),
                ('E-PROFILE-016', '/columns/1/numeric/histogram'),
            },
        ),
    ],
    ids=[
        'out-of-range rate',
        'missing required field',
        'NaN',
        'ambiguous quantile name',
        'orderings the schema cannot state',
    ],
)
def test_each_fault_is_reported_at_the_field_that_holds_it(tmp_path, edit, expected):
    document = copy.deepcopy(Profile.load(EXAMPLE).to_plain())
    edit(document)
    with pytest.raises(InvalidProfile) as raised:
        Profile.load(write_json(tmp_path / 'orders.json', document))
    assert {(issue.code, issue.field) for issue in raised.value.issues} == expected


def test_a_bundle_directory_and_its_single_file_form_share_one_digest(tmp_path):
    orders = Profile.load(EXAMPLE).to_plain()
    customers = {'kind': 'table', 'name': 'customers', 'dataset': {'rows': 5000}}
    relationships = [{'from': ['orders.customer_id'], 'to': ['customers.customer_id'], 'cardinality': 'many_to_one'}]

    directory = tmp_path / 'shop'
    directory.mkdir()
    (directory / 'orders.profile.yaml').write_text(EXAMPLE.read_text())
    write_json(directory / 'customers.profile.json', customers)
    index = {
        'kind': 'bundle',
        'name': 'shop',
        'tables': {'orders': 'orders.profile.yaml', 'customers': 'customers.profile.json'},
        'relationships': relationships,
    }
    (directory / 'bundle.yaml').write_text(json.dumps(index))
    single = write_json(tmp_path / 'shop.json', index | {'tables': {'orders': orders, 'customers': customers}})

    assert Bundle.load(directory).digest == Bundle.load(single).digest
    with pytest.raises(ValueError):
        Bundle.load(single).tables['orders'].save()


@pytest.mark.parametrize('suffix', ['parquet', 'arrow'])
def test_a_file_written_with_its_profile_embedded_gives_the_profile_back_and_a_plain_one_names_its_absence(tmp_path, suffix):
    orders = Profile.example('orders')
    embedded, plain = tmp_path / f'embedded.{suffix}', tmp_path / f'plain.{suffix}'
    sd.write(orders, embedded, rows=100, embed_profile=True)
    sd.write(orders, plain, rows=100)

    assert Profile.from_data_file(embedded).digest == orders.digest
    with pytest.raises(SourceError):
        Profile.from_data_file(plain)
