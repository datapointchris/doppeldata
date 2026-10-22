import importlib.metadata
import json
from typing import Annotated

import typer

app = typer.Typer(no_args_is_help=True)


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
