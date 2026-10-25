import json

from doppeldata_cli.main import app
from typer.testing import CliRunner


def test_data_plan_names_the_identity_and_ranges_data_generate_writes(tmp_path):
    flags = ['orders', '--rows', '250000', '--seed', '7', '--format', 'parquet', '--file-rows', '100000', '--out', str(tmp_path / 'orders')]
    runner = CliRunner()
    planned = runner.invoke(app, ['data', 'plan', *flags, '--json'])
    assert planned.exit_code == 0, planned.output
    plan = json.loads(planned.output)

    generated = runner.invoke(app, ['data', 'generate', *flags])
    assert generated.exit_code == 0, generated.output
    manifest = json.loads((tmp_path / 'orders' / '_manifest.json').read_text())

    assert (manifest['identity'], manifest['dataset_digest']) == (plan['identity'], plan['dataset_digest'])
    planned_ranges = [(planned['range'], planned['path'], planned['range_rows']) for planned in plan['ranges']]
    assert [(file['range'], file['path'], file['range_rows']) for file in manifest['files']] == planned_ranges
    assert sorted(path.name for path in (tmp_path / 'orders').iterdir()) == ['_manifest.json', *(path for _, path, _ in planned_ranges)]


def test_data_generate_to_one_file_with_manifest_then_with_replace_alone_leaves_the_file_and_no_stale_manifest(tmp_path):
    """`--out h.csv --manifest --seed 5`, then `--out h.csv --replace --seed 9`: the first manifest names rows h.csv no longer holds."""
    out = tmp_path / 'people.csv'
    flags = ['data', 'generate', '--columns', 'name,street,zip', '--rows', '1000', '--out', str(out)]
    runner = CliRunner()
    first = runner.invoke(app, [*flags, '--manifest', '--seed', '5'])
    assert first.exit_code == 0, first.output
    assert sorted(path.name for path in tmp_path.iterdir()) == ['_manifest.json', 'people.csv']

    second = runner.invoke(app, [*flags, '--replace', '--seed', '9'])
    assert second.exit_code == 0, second.output
    assert list(tmp_path.iterdir()) == [out]
    assert out.read_text().splitlines()[0] == 'name,street,zip'


def test_profile_expand_writes_the_profile_a_short_yaml_stands_for_which_generates_the_same_bytes(tmp_path):
    short = tmp_path / 'people.yaml'
    short.write_text('rows: 1000\ncolumns:\n  name:\n  zip: zip_code\n  age: 18..90\n  plan: [free, pro, team]\n')
    full = tmp_path / 'people.profile.yaml'
    runner = CliRunner()
    expanded = runner.invoke(app, ['profile', 'expand', str(short), '--out', str(full)])
    assert expanded.exit_code == 0, expanded.output

    for profile, out in ((short, 'short.csv'), (full, 'full.csv')):
        generated = runner.invoke(app, ['data', 'generate', str(profile), '--out', str(tmp_path / out)])
        assert generated.exit_code == 0, generated.output
    assert (tmp_path / 'short.csv').read_bytes() == (tmp_path / 'full.csv').read_bytes()

    checked = runner.invoke(app, ['data', 'check', str(short), str(tmp_path / 'short.csv')])
    assert checked.exit_code == 0, checked.output
