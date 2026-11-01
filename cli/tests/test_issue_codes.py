import ast
import inspect
import json
from pathlib import Path

from doppeldata import Issue
from doppeldata import IssueCarrier
from doppeldata import issue_codes
from doppeldata_cli.main import app
from typer.testing import CliRunner

REPO = Path(__file__).parents[2]
SOURCES = sorted([*(REPO / 'library' / 'src').rglob('*.py'), *(REPO / 'cli' / 'src').rglob('*.py')])
REGISTRY = REPO / 'library' / 'src' / 'doppeldata' / 'issue_codes.py'


def subclasses(root: type) -> set[type]:
    found: set[type] = set()
    pending = [root]
    while pending:
        children: list[type] = pending.pop().__subclasses__()
        found.update(children)
        pending.extend(children)
    return found


def test_every_refusal_names_its_code_through_the_registry():
    trees = {path: ast.parse(path.read_text(), str(path)) for path in SOURCES if path != REGISTRY}
    # Every class and function whose first parameter is the code of the refusal it builds.
    kinds = {Issue, *subclasses(Issue), *subclasses(IssueCarrier)}
    refusals = {kind.__name__ for kind in kinds if list(inspect.signature(kind).parameters)[:1] == ['code']}
    for tree in trees.values():
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and [argument.arg for argument in node.args.args[:1]] == ['code']:
                refusals.add(node.name)
            if isinstance(node, ast.ClassDef):
                fields = [each.target.id for each in node.body if isinstance(each, ast.AnnAssign) and isinstance(each.target, ast.Name)]
                if fields[:1] == ['code']:
                    refusals.add(node.name)
    spelled, literal = [], []
    for path, tree in trees.items():
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and issue_codes.CODE_FORM.fullmatch(node.value):
                spelled.append(f'{path.relative_to(REPO)}:{node.lineno}')
            if isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant):
                name = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, 'attr', None)
                if name in refusals:
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


def test_data_generate_in_the_readme_agent_loop_refuses_a_misspelled_format_as_json_and_prints_its_manifest(tmp_path):
    runner = CliRunner()
    misspelled = runner.invoke(app, ['data', 'generate', 'orders', '--out', f'{tmp_path}/a/', '--format', 'parqet', '--json'])
    assert misspelled.exit_code == 2, misspelled.output
    error = json.loads(misspelled.stderr)['error']
    assert (error['code'], error['field'], error['closest']) == ('E-USAGE-107', '--format', ['parquet'])

    written = runner.invoke(app, ['data', 'generate', 'orders', '--rows', '10', '--out', f'{tmp_path}/b/', '--json'])
    assert written.exit_code == 0, written.output
    assert json.loads(written.stdout)['dataset_digest'] == json.loads((tmp_path / 'b' / '_manifest.json').read_text())['dataset_digest']


def test_a_failing_scenario_check_prints_its_report_on_stdout_and_its_verdict_on_stderr(tmp_path):
    scenario = tmp_path / 'misnamed.scenario.yaml'
    scenario.write_text('scenario: misnamed\nperturbations:\n  - {op: noise, column: quantty, sigma: 1.0, fraction: 0.1}\n')
    checked = CliRunner().invoke(app, ['scenario', 'check', str(scenario), 'orders', '--json'])
    assert checked.exit_code == 3, checked.output
    assert json.loads(checked.stdout)['issues'][0]['closest'] == ['quantity']
    assert json.loads(checked.stderr)['error']['code'] == 'E-SCENARIO-019'
