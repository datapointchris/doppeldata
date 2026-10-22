import enum
import importlib.metadata
import json
from typing import Annotated

import typer
from doppeldata.profile import schema_text

app = typer.Typer(no_args_is_help=True)
schema_app = typer.Typer(no_args_is_help=True, help='The JSON Schemas doppeldata validates its files against.')
app.add_typer(schema_app, name='schema')


class SchemaName(enum.StrEnum):
    PROFILE = 'profile'


SCHEMAS = {SchemaName.PROFILE: schema_text}


@schema_app.command('show')
def schema_show(name: Annotated[SchemaName, typer.Argument(help='Which schema to print.')]) -> None:
    """Print a JSON Schema (Draft 2020-12)."""
    typer.echo(SCHEMAS[name](), nl=False)


def installed_commit() -> str | None:
    """The commit a git install was built from, which uv records in direct_url.json."""
    direct_url = importlib.metadata.distribution('doppeldata-cli').read_text('direct_url.json')
    if not direct_url:
        return None
    return json.loads(direct_url).get('vcs_info', {}).get('commit_id')


def version_callback(asked: bool) -> None:
    if not asked:
        return
    commit = installed_commit()
    typer.echo(f'doppeldata {importlib.metadata.version("doppeldata-cli")}{f" @ {commit[:8]}" if commit else ""}')
    raise typer.Exit()


@app.callback()
def root(
    version: Annotated[
        bool | None,
        typer.Option('--version', callback=version_callback, is_eager=True, help='Show the installed version and exit.'),
    ] = None,
) -> None:
    """Profile a dataset into a reviewable file holding no rows, then generate synthetic data from it."""
