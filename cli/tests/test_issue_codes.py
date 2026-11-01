import ast
import json
from pathlib import Path

from doppeldata import issue_codes
from doppeldata_cli.main import app
from typer.testing import CliRunner

REPO = Path(__file__).parents[2]
SOURCES = sorted([*(REPO / 'library' / 'src').rglob('*.py'), *(REPO / 'cli' / 'src').rglob('*.py')])
REGISTRY = REPO / 'library' / 'src' / 'doppeldata' / 'issue_codes.py'
# Every call whose first argument is the code of the refusal it builds.
REFUSALS = {
    'Issue',
    'Violation',
    'RuleBreak',
    'DoppeldataError',
    'InvalidArgument',
    'UnsupportedArgument',
    'ProfileNotFound',
    'IncompatibleInstall',
    'SourceError',
    'WriteFailed',
    'StoppedWrite',
    'DetectorPackError',
    'InvalidCodeList',
    'refused',
}


def test_every_refusal_names_its_code_through_the_registry():
    spelled, literal = [], []
    for path in SOURCES:
        if path == REGISTRY:
            continue
        for node in ast.walk(ast.parse(path.read_text(), str(path))):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and issue_codes.CODE_FORM.fullmatch(node.value):
                spelled.append(f'{path.relative_to(REPO)}:{node.lineno}')
            if isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant):
                name = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, 'attr', None)
                if name in REFUSALS:
                    literal.append(f'{path.relative_to(REPO)}:{node.lineno}')
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == 'issue_codes':
                held = getattr(issue_codes, node.attr)
                assert not isinstance(held, str) or held in issue_codes.REGISTERED, f'{path}:{node.lineno}'
    assert (spelled, literal) == ([], [])


def test_a_refusal_under_json_prints_one_line_per_issue_on_stderr_and_exits_with_its_status(tmp_path):
    runner = CliRunner()
    unknown = runner.invoke(app, ['data', 'plan', 'ordres', '--out', f'{tmp_path}/', '--json'])
    assert unknown.exit_code == 2, unknown.output
    (line,) = unknown.stderr.splitlines()
    error = json.loads(line)['error']
    assert (error['code'], error['field'], error['value'], error['closest'][0]) == ('E-PROFILE-028', 'PROFILE', 'ordres', 'orders')

    flag = runner.invoke(app, ['data', 'plan', 'orders', '--rowz', '5', '--out', f'{tmp_path}/', '--json'])
    assert flag.exit_code == 2, flag.output
    error = json.loads(flag.stderr)['error']
    assert (error['code'], error['field'], error['closest'], error['hint']) == (
        'E-USAGE-001',
        '--rowz',
        ['--rows'],
        'doppeldata data plan --help',
    )
