import json
from typing import Any

import typer
from doppeldata.issue_codes import registered
from doppeldata.scenario_format import op_schemas
from doppeldata_cli.main import app
from typer.testing import CliRunner


def nodes(command: Any, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], Any]]:
    """Every group and command under `command`, with the words that reach it."""
    children = getattr(command, 'commands', {})
    return [(path, command), *(node for name, child in children.items() for node in nodes(child, (*path, name)))]


def test_every_command_answers_help_with_examples_and_every_leaf_takes_json_and_v_beside_q():
    for path, command in nodes(typer.main.get_command(app)):
        assert CliRunner().invoke(app, [*path, '--help']).exit_code == 0, path
        assert (command.epilog or '').startswith('Examples:'), path
        if hasattr(command, 'commands'):
            continue
        flags = {name for parameter in command.params for name in parameter.opts}
        assert '--json' in flags, path
        assert ('-v' in flags) == ('-q' in flags), path


def test_capabilities_show_lists_every_command_op_and_issue_code_with_the_formats_an_op_is_written_in():
    result = CliRunner().invoke(app, ['capabilities', 'show', '--json'])
    assert result.exit_code == 0, result.output
    held = json.loads(result.output)
    paths = [command['path'] for command in held['commands']]
    assert ['capabilities', 'show'] in paths and ['data', 'generate'] in paths
    plan = next(command for command in held['commands'] if command['path'] == ['data', 'plan'])
    assert next(flag for flag in plan['flags'] if flag['name'] == '--out')['required']
    assert [row['op'] for row in held['ops']] == list(op_schemas())
    assert {row['op']: row['formats'] for row in held['ops']}['cdc'] == ['parquet', 'csv']
    assert [row['code'] for row in held['issue_codes']] == [code.code for code in registered()]
