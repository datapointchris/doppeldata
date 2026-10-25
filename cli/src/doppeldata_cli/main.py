import dataclasses as dc
import importlib.metadata
import json
import warnings
from pathlib import Path
from typing import Annotated

import typer
from doppeldata import DoppeldataError
from doppeldata import FitsSkipped
from doppeldata import InvalidProfile
from doppeldata import Profile
from doppeldata import SourceError
from doppeldata import profile
from doppeldata.example_profiles import example_names
from doppeldata.example_profiles import example_text
from doppeldata.profiling import DEFAULT_K_MIN
from doppeldata.published_schemas import schema_names
from doppeldata.published_schemas import schema_summaries
from doppeldata.published_schemas import schema_text

app = typer.Typer(no_args_is_help=True)
profile_app = typer.Typer(no_args_is_help=True, help='Profiles: reviewable files that describe a dataset without holding its rows.')
app.add_typer(profile_app, name='profile')
schemas_app = typer.Typer(no_args_is_help=True, help='The JSON Schemas of the files doppeldata reads and writes.')
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


@profile_app.command('create', no_args_is_help=True)
def profile_create(
    data: Annotated[
        list[str],
        typer.Argument(metavar='DATA...', help='Files, directories or s3:// prefixes holding one table, all of one format.'),
    ],
    out: Annotated[
        Path | None,
        typer.Option('--out', metavar='FILE', help='Write the profile to FILE, as JSON when it ends in .json, rather than print its YAML.'),
    ] = None,
    sample: Annotated[int | None, typer.Option('--sample', metavar='N', min=1, help='Profile a seeded random sample of N rows.')] = None,
    k_min: Annotated[
        int, typer.Option('--k-min', metavar='N', min=1, help='Store no data value that fewer than N rows hold.')
    ] = DEFAULT_K_MIN,
    edges_from: Annotated[
        Path | None,
        typer.Option(
            '--edges-from',
            metavar='FILE',
            help=(
                "Count each column against FILE's histogram edges and value list, writing the rows outside them as overflow. "
                'FILE is returned unchanged when the files and settings match the ones it was made from.'
            ),
        ),
    ] = None,
) -> None:
    """Profile a dataset into a file holding no rows: doppeldata profile create data/orders/ --out orders.profile.yaml

    Each file is read as its suffix's format. S3 is read with the AWS environment's credentials, and text formats from local files only.
    """
    if out is not None and not out.parent.is_dir():
        raise typer.BadParameter(f'{out.parent} is not a directory; create it, or name a FILE in one that exists', param_hint='--out')
    try:
        earlier = Profile.load(edges_from) if edges_from is not None else None
    except (OSError, InvalidProfile) as error:
        raise typer.BadParameter(str(error), param_hint='--edges-from') from error
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always', FitsSkipped)
            profiled = profile(data, sample=sample, k_min=k_min, edges_from=earlier)
    except DoppeldataError as error:
        if isinstance(error, SourceError) and error.exit_code == 2:
            raise typer.BadParameter(str(error), param_hint='DATA') from error
        if isinstance(error, InvalidProfile):
            typer.echo('doppeldata: the profile it wrote fails its own schema, which is a bug in doppeldata:', err=True)
        typer.echo(f'doppeldata: {error}', err=True)
        raise typer.Exit(error.exit_code) from error
    for notice in caught:
        if issubclass(notice.category, FitsSkipped):
            typer.echo(f'doppeldata: {notice.message}', err=True)
        else:
            warnings.showwarning(notice.message, notice.category, notice.filename, notice.lineno)
    if out is None:
        typer.echo(profiled.to_yaml(), nl=False)
        return
    profiled.save(out)


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
