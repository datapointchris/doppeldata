import contextlib
import dataclasses as dc
import importlib.metadata
import json
import math
import signal
import traceback
import warnings
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated
from typing import Any

import doppeldata
import typer
from doppeldata import CheckFailed
from doppeldata import DetectorPackError
from doppeldata import DoppeldataError
from doppeldata import InvalidProfile
from doppeldata import InvalidThresholds
from doppeldata import Profile
from doppeldata import ProfilesDrifted
from doppeldata import ProfilingWarning
from doppeldata import SourceError
from doppeldata import StoppedWrite
from doppeldata import Thresholds
from doppeldata import _core
from doppeldata import check
from doppeldata import diff
from doppeldata import profile
from doppeldata.dataset_generation import WriteChoices
from doppeldata.example_profiles import example_names
from doppeldata.example_profiles import example_text
from doppeldata.profile_comparison.metrics import METRICS
from doppeldata.profiling import DEFAULT_K_MIN
from doppeldata.published_schemas import output_formats
from doppeldata.published_schemas import partition_styles
from doppeldata.published_schemas import schema_names
from doppeldata.published_schemas import schema_summaries
from doppeldata.published_schemas import schema_text
from typer.core import TyperGroup


def beside(destination: str, name: str) -> str:
    """The object `name` at the top of `destination`: orders/_manifest.json, memory://_manifest.json."""
    return destination + name if destination.endswith('/') else f'{destination}/{name}'


def report_stopped_write(error: StoppedWrite) -> None:
    """Say why the write stopped, what it left behind, and the flag that gets past a destination holding a dataset."""
    hint = '; rerun with --replace' if error.code == 'destination-not-empty' else ''
    typer.echo(f'doppeldata: {error}{hint}', err=True)
    if error.manifest is not None and error.manifest_stored and error.manifest.partial:
        location = beside(error.manifest.document['destination'], '_manifest.partial.json')
        typer.echo(f'doppeldata: {location} lists the {counted(len(error.manifest.files), "file")} the write completed', err=True)


class DoppeldataGroup(TyperGroup):
    """The root group. Each doppeldata error a verb raises prints on stderr, and the command exits with its status."""

    def invoke(self, ctx: Any) -> Any:
        try:
            return super().invoke(ctx)
        except StoppedWrite as error:
            report_stopped_write(error)
            raise typer.Exit(error.exit_code) from error
        except DoppeldataError as error:
            typer.echo(f'doppeldata: {error}', err=True)
            raise typer.Exit(error.exit_code) from error


app = typer.Typer(no_args_is_help=True, cls=DoppeldataGroup)
profile_app = typer.Typer(no_args_is_help=True, help='Profiles: reviewable files that describe a dataset without holding its rows.')
app.add_typer(profile_app, name='profile')
data_app = typer.Typer(
    no_args_is_help=True, help='Synthetic datasets generated from a profile, written as files with a manifest, and checked against it.'
)
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
OutOption = Annotated[
    str,
    typer.Option(
        '--out', metavar='PATH', help='A local directory, an s3:// or s3a:// URL, or memory:// to time a write that keeps nothing.'
    ),
]
FileRowsOption = Annotated[
    int | None,
    typer.Option(
        '--file-rows',
        metavar='N',
        min=1,
        help=f'Rows in each file. Defaults to the sizes the profile records, else {_core.DEFAULT_FILE_ROWS:,}.',
    ),
]
PartitionByOption = Annotated[
    list[str] | None,
    typer.Option(
        '--partition-by',
        metavar='COLUMN',
        help='Write a directory per value of COLUMN, repeated for nested ones. Replaces the keys the profile records.',
    ),
]
NoPartitionsOption = Annotated[
    bool, typer.Option('--no-partitions', help='Write no partition directories, whatever keys the profile records.')
]
PartitionStyleOption = Annotated[
    str | None,
    typer.Option(
        '--partition-style',
        metavar='STYLE',
        help='hive for key=value/ directories, or directory for value/. Defaults to the style the profile records, else hive.',
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
ThresholdsOption = Annotated[
    Path | None,
    typer.Option(
        '--thresholds',
        metavar='FILE',
        help="Each metric's threshold, as YAML or JSON: {defaults: {ks: 0.03}, columns: {amount: {ks: 0.08}}}. The metrics are below.",
    ),
]
METRIC_DEFINITIONS = '\n\n'.join(
    f'{metric.name}: {metric.summary}. Passes at {metric.bound.value.removeprefix("at_")} {metric.threshold:g} by default.'
    for metric in METRICS
)


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
                'FILE is returned unchanged when nothing it was made from has changed: the files, the settings, '
                'and the versions of the tools and detector packs that wrote it.'
            ),
        ),
    ] = None,
    packs: Annotated[
        list[str] | None,
        typer.Option(
            '--pack',
            metavar='NAME',
            help=('Also type columns with the installed detector pack NAME; repeat it for more than one. The general pack is always on.'),
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
            profiled = profile(data, sample=sample, k_min=k_min, edges_from=earlier, packs=packs or ())
    except SourceError as error:
        if error.exit_code != 2:
            raise
        raise typer.BadParameter(str(error), param_hint='DATA') from error
    except DetectorPackError as error:
        if error.kind != 'refused':
            raise
        raise typer.BadParameter(str(error), param_hint='--pack') from error
    except InvalidProfile:
        typer.echo('doppeldata: the profile it wrote fails its own schema, which is a bug in doppeldata:', err=True)
        raise
    print_notices(caught)
    if earlier is not None and profiled is earlier:
        typer.echo(f'doppeldata: {edges_from} was made from these files with these settings, so it is written unchanged', err=True)
    if out is None:
        typer.echo(profiled.to_yaml(), nl=False)
        return
    profiled.save(out)


def print_notices(caught: list[warnings.WarningMessage]) -> None:
    """Print each profiling warning as one line on stderr, and any other warning as Python shows it."""
    for notice in caught:
        if issubclass(notice.category, ProfilingWarning):
            typer.echo(f'doppeldata: {notice.message}', err=True)
        else:
            warnings.showwarning(notice.message, notice.category, notice.filename, notice.lineno)


def loaded_profile(name: str, param_hint: str) -> Profile:
    """A profile file, or an example doppeldata ships, refused with exit 2 when it is neither or does not validate."""
    path = Path(name)
    if path.is_file():
        try:
            return Profile.load(path)
        except (OSError, InvalidProfile) as error:
            raise typer.BadParameter(str(error), param_hint=param_hint) from error
    if name in example_names():
        return Profile.example(name)
    message = f'{name!r} is neither a profile file nor an example; name a file, or one of {", ".join(example_names())}'
    raise typer.BadParameter(message, param_hint=param_hint)


@contextlib.contextmanager
def failure_exits_3() -> Iterator[None]:
    """End a check verb on 3 when what it wraps fails, so its 1 means drift and nothing else. A usage error keeps 2, and no answer 4."""
    try:
        yield
    except DoppeldataError as error:
        if error.exit_code == 1:
            error.exit_code = 3
        raise
    except Exception as error:
        traceback.print_exc()
        typer.echo(f'doppeldata: the comparison stopped on {type(error).__name__}, which is a bug in doppeldata', err=True)
        raise typer.Exit(3) from error


def loaded_thresholds(path: Path | None) -> Thresholds | None:
    try:
        return Thresholds.load(path) if path is not None else None
    except InvalidThresholds as error:
        raise typer.BadParameter(str(error), param_hint='--thresholds') from error


@profile_app.command('diff', no_args_is_help=True, epilog=METRIC_DEFINITIONS)
def profile_diff(
    a: Annotated[str, typer.Argument(metavar='A', help='The earlier profile, a file or an example name.')],
    b: Annotated[str, typer.Argument(metavar='B', help='The later profile, made with --edges-from A to measure overflow.')],
    thresholds: ThresholdsOption = None,
    as_json: JsonOption = False,
) -> None:
    """Report how far B drifted from A, two profiles of one table: doppeldata profile diff march.profile.yaml april.profile.yaml

    Exits 0 when the profiles agree, 1 when a metric misses its threshold or a field changed, and 3 when the diff could not finish.
    """
    earlier, later = loaded_profile(a, 'A'), loaded_profile(b, 'B')
    chosen = loaded_thresholds(thresholds)
    try:
        with failure_exits_3():
            report = diff(earlier, later, thresholds=chosen)
            shown = report.to_json() if as_json else report.to_markdown()
    except InvalidThresholds as error:
        raise typer.BadParameter(str(error), param_hint='--thresholds') from error
    typer.echo(shown, nl=False)
    if report.drifted:
        raise ProfilesDrifted(report)


@data_app.command('check', no_args_is_help=True, epilog=METRIC_DEFINITIONS)
def data_check(
    profile_name: Annotated[
        str, typer.Argument(metavar='PROFILE', help='The profile the data was generated from, a file or an example name.')
    ],
    data: Annotated[str, typer.Argument(metavar='DATA', help='A file, directory or s3:// prefix holding the generated table.')],
    thresholds: ThresholdsOption = None,
    as_json: JsonOption = False,
) -> None:
    """Check generated data against its profile, without the real data: doppeldata data check orders.profile.yaml out/orders/

    DATA is profiled with PROFILE's histogram edges and value lists, and each metric compares the two profiles.
    Exits 0 when every metric and column passes, and 3 when one does not or the check could not finish.
    """
    source = loaded_profile(profile_name, 'PROFILE')
    chosen = loaded_thresholds(thresholds)
    try:
        with failure_exits_3(), warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always', ProfilingWarning)
            report = check(source, data, thresholds=chosen)
            shown = report.to_json() if as_json else report.to_markdown()
    except InvalidThresholds as error:
        raise typer.BadParameter(str(error), param_hint='--thresholds') from error
    except SourceError as error:
        if error.exit_code != 2:
            raise
        raise typer.BadParameter(str(error), param_hint='DATA') from error
    print_notices(caught)
    typer.echo(shown, nl=False)
    if not (report.passed and report.verified):
        raise CheckFailed(report)


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
    """What data plan and data generate share: the profile, the rows and seed, --out, and what the write lays over the profile."""

    profile: Profile
    rows: int | None
    seed: int
    out: str
    write: WriteChoices

    def arguments(self) -> dict[str, Any]:
        return {'rows': self.rows, 'seed': self.seed, **dc.asdict(self.write)}


def data_choices(
    profile_name: str,
    rows: int | None,
    seed: int,
    file_format: str | None,
    out: str,
    file_rows: int | None,
    partition_by: list[str] | None,
    no_partitions: bool,
    partition_style: str | None,
    writer: str | None,
) -> DataChoices:
    """The flags both data verbs take, each refused here with exit 2 when it names nothing doppeldata writes."""
    if file_format is not None and file_format not in output_formats():
        raise typer.BadParameter(
            f'{file_format!r} is not a format doppeldata writes; choose from {", ".join(output_formats())}', param_hint='--format'
        )
    if partition_style is not None and partition_style not in partition_styles():
        message = f'{partition_style!r} is not a partition style doppeldata writes; choose from {", ".join(partition_styles())}'
        raise typer.BadParameter(message, param_hint='--partition-style')
    if no_partitions and partition_by:
        raise typer.BadParameter('--no-partitions writes no partitions, so it takes no --partition-by', param_hint='--no-partitions')
    settings = None
    if writer is not None:
        try:
            settings = json.loads(writer)
        except json.JSONDecodeError as error:
            raise typer.BadParameter(f'{writer!r} is not JSON: {error}', param_hint='--writer') from error
        if not isinstance(settings, dict):
            raise typer.BadParameter(f'{writer!r} is not a JSON object', param_hint='--writer')
    keys = [] if no_partitions else partition_by or None
    write = WriteChoices(file_format, file_rows, keys, partition_style, settings)
    return DataChoices(loaded_profile(profile_name, 'PROFILE'), rows, seed, out, write)


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
    no_partitions: NoPartitionsOption = False,
    partition_style: PartitionStyleOption = None,
    writer: WriterOption = None,
    as_json: JsonOption = False,
) -> None:
    """Print what data generate would write with the same flags: doppeldata data plan orders --rows 1000000000 --out s3://bucket/orders/

    It reads nothing at --out, and generates the first blocks to estimate the size. The --json shape may change before 1.0.
    """
    choices = data_choices(profile_name, rows, seed, file_format, out, file_rows, partition_by, no_partitions, partition_style, writer)
    planned = doppeldata.plan(choices.profile, choices.out, **choices.arguments())
    if as_json:
        typer.echo(json.dumps(planned.to_json(), indent=2))
        return
    identity = planned.identity
    partitioned = planned.layout['partition_by']
    ranges = counted(len(planned.ranges), 'range')
    if partitioned:
        directories = f'one {planned.layout["partition_style"]}-style directory per value of {", ".join(partitioned)}'
        files = f'up to {counted(planned.files_at_most, "file")} from {ranges} to {planned.destination}, under {directories}'
    else:
        files = f'{counted(planned.files_at_most, "file")} to {planned.destination}'
    lines = [
        f'dataset    {planned.dataset_digest}',
        f'identity   {int(identity["dataset_rows"]):,} rows from seed {identity["seed"]}, profile {identity["profile_digest"]}, '
        f'stream version {identity["stream_version"]}',
        f'format     {planned.format} {json.dumps(planned.writer, separators=(",", ":"))}',
        f'files      {files}',
        f'estimate   {size(planned.estimated_bytes)}, from the first blocks encoded with these settings',
        '',
    ]
    width = max(len(column.name) for column in planned.columns)
    type_width = max(len(column.physical_type) for column in planned.columns)
    for column in planned.columns:
        nulls = f', {column.null_share:.3g} null' if column.null_share else ''
        lines.append(f'{column.name:<{width}}  {column.physical_type:<{type_width}}  {column.sampler}{nulls}')
    lines.append('')
    path_width = max((len(planned_range.path) for planned_range in planned.ranges), default=0)
    for planned_range in planned.ranges:
        last_row = planned_range.first_row + planned_range.range_rows - 1
        objects = f', up to {counted(planned_range.objects_at_most, "file")}' if partitioned else ''
        lines.append(f'{planned_range.path:<{path_width}}  rows {planned_range.first_row:,} to {last_row:,}{objects}')
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
    no_partitions: NoPartitionsOption = False,
    partition_style: PartitionStyleOption = None,
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

    A destination holding anything is refused unless --replace is given. A write that stops after claiming --out, on a failure,
    Ctrl-C or SIGTERM, leaves _manifest.partial.json listing the files it completed. S3 is written with the AWS environment's
    credentials.
    """
    choices = data_choices(profile_name, rows, seed, file_format, out, file_rows, partition_by, no_partitions, partition_style, writer)
    terminate = signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        manifest = doppeldata.write(choices.profile, choices.out, **choices.arguments(), threads=threads, replace=replace)
    finally:
        signal.signal(signal.SIGTERM, terminate)
    document = manifest.document
    rows_written = int(manifest.identity['dataset_rows'])
    seconds = max(document['elapsed_ms'], 1) / 1000
    stored = size(sum(file['bytes'] for file in manifest.files))
    typer.echo(
        f'wrote {counted(rows_written, "row")} as {document["format"]} in {counted(len(manifest.files), "file")}, {stored}, '
        f'in {seconds:.2f} s on {counted(document["threads"], "thread")}: {rows_written / seconds / 1e6:.2f}M rows/s'
    )
    typer.echo(f'{beside(document["destination"], "_manifest.json")} names dataset {manifest.dataset_digest}')


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
