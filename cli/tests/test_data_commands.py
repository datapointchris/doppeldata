import json

from doppeldata_cli.main import app
from typer.testing import CliRunner


def test_data_plan_names_the_identity_and_files_data_generate_writes(tmp_path):
    flags = ['orders', '--rows', '250000', '--seed', '7', '--format', 'parquet', '--file-rows', '100000', '--out', str(tmp_path / 'orders')]
    runner = CliRunner()
    planned = runner.invoke(app, ['data', 'plan', *flags, '--json'])
    assert planned.exit_code == 0, planned.output
    plan = json.loads(planned.output)

    generated = runner.invoke(app, ['data', 'generate', *flags])
    assert generated.exit_code == 0, generated.output
    manifest = json.loads((tmp_path / 'orders' / '_manifest.json').read_text())

    assert (manifest['identity'], manifest['dataset_digest']) == (plan['identity'], plan['dataset_digest'])
    planned_files = [(file['path'], file['rows']) for file in plan['files']]
    assert [(file['path'], file['rows']) for file in manifest['files']] == planned_files
    assert sorted(path.name for path in (tmp_path / 'orders').iterdir()) == ['_manifest.json', *(path for path, _ in planned_files)]
