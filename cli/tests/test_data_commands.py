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
