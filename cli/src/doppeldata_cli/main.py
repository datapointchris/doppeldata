import dataclasses as dc
import importlib.metadata
import json
from typing import Annotated

import typer
from doppeldata import Profile
from doppeldata.example_profiles import example_names
from doppeldata.example_profiles import example_text
from doppeldata.published_schemas import schema_names
from doppeldata.published_schemas import schema_summaries
from doppeldata.published_schemas import schema_text

app = typer.Typer(no_args_is_help=True)
profile_app = typer.Typer(no_args_is_help=True, help='Profiles: reviewable files that describe a dataset without holding its rows.')
app.add_typer(profile_app, name='profile')
schemas_app = typer.Typer(no_args_is_help=True, help='The JSON Schemas doppeldata validates its files against.')
app.add_typer(schemas_app, name='schemas')

JsonOption = Annotated[bool, typer.Option('--json', help='Print JSON instead of a table.')]


@profile_app.command('list')
def profile_list(as_json: JsonOption = False) -> None:
    """List the hand-written example profiles doppeldata ships, by the name profile show takes."""
    examples = [Profile.example(name).document for name in example_names()]
    rows = [{'name': doc['name'], 'row_count': doc['dataset']['row_count'], 'columns': len(doc['columns'])} for doc in examples]
    if as_json:
        typer.echo(json.dumps(rows, indent=2))
        return
    width = max(len(row['name']) for row in rows)
    for row in rows:
        typer.echo(f'{row["name"]:<{width}}  {row["columns"]} columns, {row["row_count"]:,} rows')


@profile_app.command('show', no_args_is_help=True)
def profile_show(name: Annotated[str, typer.Argument(metavar='NAME', help='An example name, as profile list prints it.')]) -> None:
    """Print an example profile's YAML, to start a profile from: doppeldata profile show orders > orders.profile.yaml"""
    if name not in example_names():
        raise typer.BadParameter(
            f'{name!r} is not an example profile doppeldata ships; choose from {", ".join(example_names())}', param_hint='NAME'
        )
    typer.echo(example_text(name), nl=False)


@schemas_app.command('list')
def schemas_list(as_json: JsonOption = False) -> None:
    """List every JSON Schema doppeldata ships, by the name schemas show takes."""
    summaries = schema_summaries()
    if as_json:
        typer.echo(json.dumps([dc.asdict(summary) for summary in summaries], indent=2))
        return
    width = max(len(summary.name) for summary in summaries)
    for summary in summaries:
        typer.echo(f'{summary.name:<{width}}  {summary.description}')


@schemas_app.command('show', no_args_is_help=True)
def schemas_show(name: Annotated[str, typer.Argument(metavar='NAME', help='A schema name, as schemas list prints it.')]) -> None:
    """Print a JSON Schema (Draft 2020-12)."""
    if name not in schema_names():
        raise typer.BadParameter(f'{name!r} is not a schema doppeldata ships; choose from {", ".join(schema_names())}', param_hint='NAME')
    typer.echo(schema_text(name), nl=False)


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
