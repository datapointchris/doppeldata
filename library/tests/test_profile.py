import copy
import json
import math
import re
from pathlib import Path

import pytest
from doppeldata import Bundle
from doppeldata import InvalidProfile
from doppeldata import Profile

EXAMPLE = Path(__file__).parent / 'fixtures' / 'orders.profile.yaml'


def write_json(path: Path, document: dict) -> Path:
    path.write_text(json.dumps(document))
    return path


def test_the_digest_ignores_format_comments_key_order_and_when_and_by_what_it_was_profiled(tmp_path):
    original = Profile.load(EXAMPLE)
    assert re.fullmatch(r'sha256:[0-9a-f]{64}', original.digest)
    reordered = dict(reversed(list(original.to_plain().items())))
    reordered['generated'] |= {'at': '2030-01-01T00:00:00Z', 'tool': 'doppeldata 9.9.9'}
    assert Profile.load(write_json(tmp_path / 'orders.json', reordered)).digest == original.digest

    reordered['dataset']['row_count'] += 1
    assert Profile.load(write_json(tmp_path / 'orders.json', reordered)).digest != original.digest


def test_saving_changes_only_the_edited_value_and_keeps_fields_this_version_does_not_know(tmp_path):
    text = EXAMPLE.read_text().replace('kind: table\n', 'kind: table\nfrom_a_newer_tool: {kept: true, since: 2026-01-01}\n')
    path = tmp_path / 'orders.profile.yaml'
    path.write_text(text)
    profile = Profile.load(path)
    profile.document['dataset']['row_count'] = 42
    profile.save()
    assert path.read_text() == text.replace('row_count: 10000000', 'row_count: 42')


@pytest.mark.parametrize(
    ('edit', 'expected'),
    [
        (lambda p: p['columns'][1].update(null_rate=1.5), {('schema-maximum', '/columns/1/null_rate')}),
        (lambda p: p['dataset']['keys'].update(primary={'uniqueness': 1.0}), {('schema-required', '/dataset/keys/primary')}),
        (lambda p: p.update(profile_version=2), {('newer-profile-version', '/profile_version')}),
        (lambda p: p['columns'][1]['numeric'].update(skew=math.nan), {('not-finite', '/columns/1/numeric/skew')}),
        (lambda p: p['columns'][1]['numeric']['quantiles'].update(p5=1.0), {('schema-pattern', '/columns/1/numeric/quantiles')}),
        (
            lambda p: (
                p['columns'][1]['numeric']['quantiles'].update(p95=50.0),
                p['columns'][3]['temporal'].update(by_dow=[0.1] * 7),
                p['columns'][1]['numeric']['histogram'].update(counts=[1, 2, 3]),
            ),
            {
                ('quantiles-not-rising', '/columns/1/numeric/quantiles/p95'),
                ('rates-not-summing-to-one', '/columns/3/temporal/by_dow'),
                ('histogram-bins-mismatch', '/columns/1/numeric/histogram'),
            },
        ),
    ],
    ids=[
        'out-of-range rate',
        'missing required field',
        'newer version',
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
    assert {(issue.code, issue.pointer) for issue in raised.value.issues} == expected


def test_a_bundle_directory_and_its_single_file_form_share_one_digest(tmp_path):
    orders = Profile.load(EXAMPLE).to_plain()
    customers = {'profile_version': 1, 'kind': 'table', 'name': 'customers', 'dataset': {'row_count': 5000}}
    relationships = [{'from': 'orders.customer_id', 'to': 'customers.customer_id', 'cardinality': 'many_to_one'}]

    directory = tmp_path / 'shop'
    directory.mkdir()
    (directory / 'orders.profile.yaml').write_text(EXAMPLE.read_text())
    write_json(directory / 'customers.profile.json', customers)
    index = {
        'profile_version': 1,
        'kind': 'bundle',
        'name': 'shop',
        'tables': {'orders': 'orders.profile.yaml', 'customers': 'customers.profile.json'},
        'relationships': relationships,
    }
    (directory / 'bundle.yaml').write_text(json.dumps(index))
    single = write_json(tmp_path / 'shop.json', index | {'tables': {'orders': orders, 'customers': customers}})

    assert Bundle.load(directory).digest == Bundle.load(single).digest
    with pytest.raises(ValueError, match='save needs a path'):
        Bundle.load(single).tables['orders'].save()
