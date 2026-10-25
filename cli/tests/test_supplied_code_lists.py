import hashlib
import random

import polars as pl
from doppeldata import Profile
from doppeldata_cli.main import app
from typer.testing import CliRunner

# Package codes as the FDA's directory writes them, in each of its three segmentations.
LISTED = ('0002-1433-80', '12345-678-90', '54321-1234-5')
ELEVEN_DIGITS = ('00002143380', '12345067890', '54321123405')


def test_an_ndc_list_types_only_listed_codes_records_its_sha256_and_leaves_a_check_of_generated_data_passing(tmp_path):
    draw = random.Random(5)
    rows = 60
    data = tmp_path / 'claims.csv'
    pl.DataFrame(
        {
            'ndc': [draw.choice(ELEVEN_DIGITS) for _ in range(rows)],
            'rx_ndc': [''.join(draw.choice('0123456789') for _ in range(11)) for _ in range(rows)],
        }
    ).write_csv(data)
    listed = tmp_path / 'ndc.txt'
    listed.write_text('\n'.join(LISTED) + '\n\n')
    out = tmp_path / 'claims.profile.yaml'
    runner = CliRunner()

    created = runner.invoke(
        app, ['profile', 'create', str(data), '--policy', 'healthcare', '--code-list', f'ndc={listed}', '--out', str(out)]
    )

    assert created.exit_code == 0, created.output
    profile = Profile.load(out).to_plain()
    columns = {column['name']: column.get('semantic') or {} for column in profile['columns']}
    assert columns['ndc']['type'] == 'ndc'
    assert 'validator' in columns['ndc']['detectors']
    assert columns['rx_ndc'].get('type') is None
    assert profile['generated']['code_lists'] == {'ndc': 'sha256:' + hashlib.sha256(listed.read_bytes()).hexdigest()}
    assert str(tmp_path) not in out.read_text()
    generated = runner.invoke(app, ['data', 'generate', str(out), '--rows', '2000', '--format', 'csv', '--out', str(tmp_path / 'out')])
    assert generated.exit_code == 0, generated.output
    checked = runner.invoke(app, ['data', 'check', str(out), str(tmp_path / 'out')])
    assert checked.exit_code == 0, checked.output

    unread = runner.invoke(app, ['profile', 'create', str(data), '--code-list', f'ndc={listed}'])
    assert unread.exit_code == 2
    assert 'the healthcare pack reads ndc' in unread.output
