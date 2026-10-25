import dataclasses as dc
import importlib.metadata
import json
import math
import warnings
from pathlib import Path
from typing import Annotated
from typing import Any

import doppeldata
import typer
from doppeldata import DoppeldataError
from doppeldata import InvalidProfile
from doppeldata import Profile
from doppeldata import ProfilingWarning
from doppeldata import SourceError
from doppeldata import profile
from doppeldata.errors import StoppedWrite
from doppeldata.example_profiles import example_names
from doppeldata.example_profiles import example_text
from doppeldata.profiling import DEFAULT_K_MIN
from doppeldata.published_schemas import output_formats
from doppeldata.published_schemas import schema_names
from doppeldata.published_schemas import schema_summaries
from doppeldata.published_schemas import schema_text

app = typer.Typer(no_args_is_help=True)
profile_app = typer.Typer(no_args_is_help=True, help='Profiles: reviewable files that describe a dataset without holding its rows.')
app.add_typer(profile_app, name='profile')
data_app = typer.Typer(no_args_is_help=True, help='Synthetic datasets generated from a profile, written as files with a manifest.')
app.add_typer(data_app, name='data')
schemas_app = typer.Typer(no_args_is_help=True, help='The JSON Schemas of the files doppeldata reads and writes.')
app.add_typer(schemas_app, name='schemas')

JsonOption = Annotated[bool, typer.Option('--json', help='Print JSON instead of a table.')]
ProfileArgument = Annotated[str, typer.Argument(metavar='PROFILE', help='A profile file, or an example name as profile list prints it.')]
RowsOption = Annotated[
    int | None, typer.Option('--rows', metavar='N', min=0, help='Rows to generate. Defaults to the row count the profile records.')
]
SeedOption = Annotated[
    int,
    typer.Option(
        '--seed', metavar='S', min=0, max=2**64 - 1, help='The seed. One profile, seed and row count give the same values anywhere.'
    ),
]
FormatOption = Annotated[
    str | None,
    typer.Option(
        '--format', metavar='F', help=f'One of {", ".join(output_formats())}. Defaults to the format the profile records, else parquet.'
    ),
]
OutOption = Annotated[str, typer.Option('--out', metavar='PATH', help='A local directory or an s3:// URL.')]
FileRowsOption = Annotated[
    int | None,
    typer.Option('--file-rows', metavar='N', min=1, help='Rows in each file. Defaults to the sizes the profile records, else 1,000,000.'),
]
PartitionByOption = Annotated[
    list[str] | None,
    typer.Option(
        '--partition-by',
        metavar='COLUMN',
        help="Write a directory per value of COLUMN, repeated for nested ones. Replaces the profile's keys.",
    ),
]
WriterOption = Annotated[
    str | None,
    typer.Option(
        '--writer',
        metavar='JSON',
        help='The format\'s settings to change, as a JSON object such as \'{"compression":"snappy"}\'. schemas show manifest names them.',
    ),
]


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
                'FILE is returned unchanged when the files, the settings, and the doppeldata and scipy versions '
                'match the ones it was made with.'
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
            warnings.simplefilter('always', ProfilingWarning)
            profiled = profile(data, sample=sample, k_min=k_min, edges_from=earlier)
    except DoppeldataError as error:
        if isinstance(error, SourceError) and error.exit_code == 2:
            raise typer.BadParameter(str(error), param_hint='DATA') from error
        if isinstance(error, InvalidProfile):
            typer.echo('doppeldata: the profile it wrote fails its own schema, which is a bug in doppeldata:', err=True)
        typer.echo(f'doppeldata: {error}', err=True)
        raise typer.Exit(error.exit_code) from error
    for notice in caught:
        if issubclass(notice.category, ProfilingWarning):
            typer.echo(f'doppeldata: {notice.message}', err=True)
        else:
            warnings.showwarning(notice.message, notice.category, notice.filename, notice.lineno)
    if earlier is not None and profiled is earlier:
        typer.echo(f'doppeldata: {edges_from} was made from these files with these settings, so it is written unchanged', err=True)
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


@dc.dataclass(frozen=True)
class DataChoices:
    """What data plan and data generate share: the profile and every flag that decides what is written."""

    profile: Profile
    rows: int | None
    seed: int
    file_format: str | None
    out: str
    file_rows: int | None
    partition_by: list[str] | None
    writer: dict[str, Any] | None

    def arguments(self) -> dict[str, Any]:
        return {
            'rows': self.rows,
            'seed': self.seed,
            'format': self.file_format,
            'file_rows': self.file_rows,
            'partition_by': self.partition_by,
            'writer': self.writer,
        }


def data_choices(
    profile_name: str,
    rows: int | None,
    seed: int,
    file_format: str | None,
    out: str,
    file_rows: int | None,
    partition_by: list[str] | None,
    writer: str | None,
) -> DataChoices:
    """The flags both data verbs take, each refused here with exit 2 when it names nothing doppeldata writes."""
    if file_format is not None and file_format not in output_formats():
        raise typer.BadParameter(
            f'{file_format!r} is not a format doppeldata writes; choose from {", ".join(output_formats())}', param_hint='--format'
        )
    settings = None
    if writer is not None:
        try:
            settings = json.loads(writer)
        except json.JSONDecodeError as error:
            raise typer.BadParameter(f'{writer!r} is not JSON: {error}', param_hint='--writer') from error
        if not isinstance(settings, dict):
            raise typer.BadParameter(f'{writer!r} is not a JSON object', param_hint='--writer')
    path = Path(profile_name)
    if path.is_file():
        try:
            loaded = Profile.load(path)
        except InvalidProfile as error:
            raise typer.BadParameter(str(error), param_hint='PROFILE') from error
    elif profile_name in example_names():
        loaded = Profile.example(profile_name)
    else:
        message = f'{profile_name!r} is neither a profile file nor an example; name a file, or one of {", ".join(example_names())}'
        raise typer.BadParameter(message, param_hint='PROFILE')
    return DataChoices(loaded, rows, seed, file_format, out, file_rows, partition_by or None, settings)


def refused(error: DoppeldataError) -> typer.Exit:
    """Report `error` on stderr, and the exit its code calls for."""
    typer.echo(f'doppeldata: {error}', err=True)
    return typer.Exit(error.exit_code)


def size(count: int) -> str:
    """`count` bytes in decimal units: 950 B, 41.3 MB."""
    if count < 1000:
        return f'{count} B'
    units = ('KB', 'MB', 'GB', 'TB')
    power = min(int(math.log10(count)) // 3, len(units))
    return f'{count / 1000**power:.1f} {units[power - 1]}'


def counted(count: int, noun: str) -> str:
    return f'{count:,} {noun}' + ('' if count == 1 else 's')


@data_app.command('plan', no_args_is_help=True)
def data_plan(
    profile_name: ProfileArgument,
    out: OutOption,
    rows: RowsOption = None,
    seed: SeedOption = 0,
    file_format: FormatOption = None,
    file_rows: FileRowsOption = None,
    partition_by: PartitionByOption = None,
    writer: WriterOption = None,
    as_json: JsonOption = False,
) -> None:
    """Print what data generate would write with the same flags: doppeldata data plan orders --rows 1000000000 --out s3://bucket/orders/

    It reads nothing at --out, and generates the first four blocks to estimate the size.
    """
    choices = data_choices(profile_name, rows, seed, file_format, out, file_rows, partition_by, writer)
    try:
        planned = doppeldata.plan(choices.profile, choices.out, **choices.arguments())
    except DoppeldataError as error:
        raise refused(error) from error
    if as_json:
        typer.echo(json.dumps(planned.to_json(), indent=2))
        return
    identity = planned.identity
    partitioned = planned.layout['partition_by']
    lines = [
        f'dataset    {planned.dataset_digest}',
        f'identity   {int(identity["dataset_rows"]):,} rows from seed {identity["seed"]}, profile {identity["profile_digest"]}, '
        f'stream version {identity["stream_version"]}',
        f'format     {planned.format} {json.dumps(planned.writer, separators=(",", ":"))}',
        f'files      {counted(len(planned.files), "file")} to {planned.destination}'
        + (f', each under one {planned.layout["partition_style"]} directory per value of {", ".join(partitioned)}' if partitioned else ''),
        f'estimate   {size(planned.estimated_bytes)}, from the first four blocks encoded with these settings',
        '',
    ]
    width = max(len(column.name) for column in planned.columns)
    type_width = max(len(column.arrow_type) for column in planned.columns)
    for column in planned.columns:
        nulls = f', {column.null_share:.3g} null' if column.null_share else ''
        lines.append(f'{column.name:<{width}}  {column.arrow_type:<{type_width}}  {column.sampler}{nulls}')
    lines.append('')
    path_width = max((len(file.path) for file in planned.files), default=0)
    lines.extend(f'{file.path:<{path_width}}  rows {file.first_row:,} to {file.first_row + file.rows - 1:,}' for file in planned.files)
    typer.echo('\n'.join(lines))


@data_app.command('generate', no_args_is_help=True)
def data_generate(
    profile_name: ProfileArgument,
    out: OutOption,
    rows: RowsOption = None,
    seed: SeedOption = 0,
    file_format: FormatOption = None,
    file_rows: FileRowsOption = None,
    partition_by: PartitionByOption = None,
    writer: WriterOption = None,
    replace: Annotated[
        bool,
        typer.Option(
            '--replace', help='Delete what an earlier doppeldata write left at --out first. Anything else there refuses the write.'
        ),
    ] = False,
    threads: Annotated[
        int, typer.Option('--threads', metavar='N', min=0, help='Threads that generate and encode. 0 takes one per core.')
    ] = 0,
) -> None:
    """Write a dataset and its _manifest.json, as data plan prints it: doppeldata data generate orders --rows 1000000 --out orders/

    A destination holding anything is refused unless --replace is given. A write that fails or is interrupted leaves
    _manifest.partial.json listing the files it completed, and exits 1. S3 is written with the AWS environment's credentials.
    """
    choices = data_choices(profile_name, rows, seed, file_format, out, file_rows, partition_by, writer)
    try:
        manifest = doppeldata.write(choices.profile, choices.out, **choices.arguments(), threads=threads, replace=replace)
    except StoppedWrite as error:
        typer.echo(f'doppeldata: {error}', err=True)
        if error.manifest is not None and error.manifest_stored:
            location = f'{choices.out.rstrip("/")}/_manifest.partial.json'
            typer.echo(f'doppeldata: {location} lists the {counted(len(error.manifest.files), "file")} the write completed', err=True)
        raise typer.Exit(error.exit_code) from error
    except DoppeldataError as error:
        raise refused(error) from error
    document = manifest.document
    rows_written = int(manifest.identity['dataset_rows'])
    seconds = max(document['elapsed_ms'], 1) / 1000
    stored = size(sum(file['bytes'] for file in manifest.files))
    typer.echo(
        f'wrote {counted(rows_written, "row")} as {document["format"]} in {counted(len(manifest.files), "file")}, {stored}, '
        f'in {seconds:.2f} s on {counted(document["threads"], "thread")}: {rows_written / seconds / 1e6:.2f}M rows/s'
    )
    typer.echo(f'{choices.out.rstrip("/")}/_manifest.json names dataset {manifest.dataset_digest}')


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
