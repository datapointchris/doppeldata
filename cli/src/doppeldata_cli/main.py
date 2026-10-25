import contextlib
import dataclasses as dc
import importlib.metadata
import importlib.resources
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
from doppeldata import Bundle
from doppeldata import CheckFailed
from doppeldata import DetectorPackError
from doppeldata import DoppeldataError
from doppeldata import InvalidCodeList
from doppeldata import InvalidProfile
from doppeldata import InvalidScenario
from doppeldata import InvalidThresholds
from doppeldata import Profile
from doppeldata import ProfilesDrifted
from doppeldata import ProfilingWarning
from doppeldata import Scenario
from doppeldata import SourceError
from doppeldata import StoppedWrite
from doppeldata import Thresholds
from doppeldata import _core
from doppeldata import check
from doppeldata import diff
from doppeldata import example_scenarios
from doppeldata import profile
from doppeldata import profile_tables
from doppeldata.dataset_generation import WriteChoices
from doppeldata.dataset_generation import one_file_of
from doppeldata.dataset_generation import prepared_write
from doppeldata.example_profiles import example_names
from doppeldata.example_profiles import example_resource
from doppeldata.example_profiles import example_text
from doppeldata.manifest import MANIFEST_FILE_NAME
from doppeldata.manifest import PARTIAL_MANIFEST_FILE_NAME
from doppeldata.privacy_policy import APPLIED_POLICIES
from doppeldata.privacy_policy import DEFAULT_K_MIN
from doppeldata.privacy_policy import ProfileFileCheck
from doppeldata.privacy_policy import TableCheck
from doppeldata.privacy_policy import file_checks
from doppeldata.profile_comparison import NOTHING_SCORED
from doppeldata.profile_comparison.metrics import METRICS
from doppeldata.profile_comparison.real_data_scores import DEFAULT_SAMPLE_ROWS
from doppeldata.profile_comparison.real_data_scores import SCORES
from doppeldata.profile_comparison.real_data_scores import scored_columns
from doppeldata.profile_comparison.side_by_side_html import unwritable
from doppeldata.profiling import ENGINES
from doppeldata.profiling import applied_policy
from doppeldata.published_schemas import enum_values
from doppeldata.published_schemas import output_formats
from doppeldata.published_schemas import partition_styles
from doppeldata.published_schemas import schema_names
from doppeldata.published_schemas import schema_summaries
from doppeldata.published_schemas import schema_text
from doppeldata.scenario_format import op_schemas
from doppeldata.scenario_format import scenario_schema
from doppeldata.scenario_format import unknown_name
from doppeldata.short_profiles import COMMAND_LINE_REMEDY
from doppeldata.short_profiles import file_profile
from doppeldata.short_profiles import named_column
from doppeldata.short_profiles import no_fake_message
from typer.core import TyperGroup


def beside(destination: str, name: str) -> str:
    """The object `name` at the top of `destination`: orders/_manifest.json, memory://_manifest.json."""
    return destination + name if destination.endswith('/') else f'{destination}/{name}'


def report_stopped_write(error: StoppedWrite) -> None:
    """Say why the write stopped, what it left behind, and the flag that gets past a destination holding a dataset."""
    hint = '; rerun with --replace' if error.code == 'destination-not-empty' else ''
    typer.echo(f'doppeldata: {error}{hint}', err=True)
    if error.manifest is not None and error.manifest_stored and error.manifest.partial:
        location = beside(error.manifest.document['destination'], PARTIAL_MANIFEST_FILE_NAME)
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
scenario_app = typer.Typer(no_args_is_help=True, help='Scenarios: layers over a profile that bend the generated data on purpose.')
app.add_typer(scenario_app, name='scenario')
schemas_app = typer.Typer(no_args_is_help=True, help='The JSON Schemas of the files doppeldata reads and writes.')
app.add_typer(schemas_app, name='schemas')

JsonOption = Annotated[bool, typer.Option('--json', help='Print JSON instead of a table.')]
PROFILE_HELP = 'A profile file, a short YAML of rows and columns, or an example name as profile list prints it.'
ProfileArgument = Annotated[str, typer.Argument(metavar='PROFILE', help=PROFILE_HELP)]
ProfileOrColumnsArgument = Annotated[str | None, typer.Argument(metavar='[PROFILE]', help=f'{PROFILE_HELP} Or give --columns.')]
ColumnsOption = Annotated[
    str | None,
    typer.Option(
        '--columns',
        metavar='NAMES',
        help='Comma-separated column names, each generated by the fake its name hints, in place of PROFILE: --columns name,email,zip',
    ),
]
RowsOption = Annotated[
    int | None,
    typer.Option(
        '--rows',
        metavar='N',
        min=0,
        help="Rows to generate, before any copies a scenario's duplicates adds. Defaults to the row count the profile records.",
    ),
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
        '--out',
        metavar='PATH',
        help=(
            'A local directory, a local file named for its format such as people.csv, an s3:// or s3a:// URL, '
            'or memory:// to time a write that keeps nothing.'
        ),
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
ScenarioOption = Annotated[
    list[str] | None,
    typer.Option(
        '--scenario',
        metavar='SCENARIO',
        help='A scenario file, or an example name as scenario list prints it, laid over the profile. Repeat it to compose in order.',
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
SCORE_DEFINITIONS = (
    "With --against, these scores compare a sample of DATA with one of REAL_DATA, by SDMetrics' definitions. Each decides "
    'nothing until a --thresholds file sets its threshold, a pair score under defaults alone. A row that decides nothing '
    'passes, and shows whether it meets the reference.\n\n'
    + '\n\n'.join(
        f'{score.name}: {score.summary}. Reference at {score.bound.value.removeprefix("at_")} {score.threshold:g}.' for score in SCORES
    )
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
        typer.Argument(
            metavar='DATA...', help='Files, directories or s3:// prefixes all of one format, holding one table unless --tables is given.'
        ),
    ],
    out: Annotated[
        Path | None,
        typer.Option('--out', metavar='FILE', help='Write the profile to FILE, as JSON when it ends in .json, rather than print its YAML.'),
    ] = None,
    engine: Annotated[
        str,
        typer.Option(
            '--engine',
            metavar='NAME',
            help=(
                f'Compute the statistics with NAME: {" or ".join(ENGINES)}. spark profiles in the Spark Connect session '
                'SPARK_REMOTE names, else in one it starts on local[*], and estimates distinct counts and quantiles.'
            ),
        ),
    ] = 'polars',
    sample: Annotated[int | None, typer.Option('--sample', metavar='N', min=1, help='Profile a seeded random sample of N rows.')] = None,
    policy: Annotated[
        str,
        typer.Option(
            '--policy',
            metavar='NAME',
            help=(
                f'Write the profile under the privacy policy NAME: {", ".join(APPLIED_POLICIES)}. '
                'healthcare adds HIPAA Safe Harbor and the healthcare detector pack, and none keeps every value.'
            ),
        ),
    ] = 'default',
    k_min: Annotated[
        int | None,
        typer.Option(
            '--k-min',
            metavar='N',
            min=1,
            help=f'Store no data value that fewer than N rows hold. Defaults to {DEFAULT_K_MIN}; none takes no N.',
        ),
    ] = None,
    edges_from: Annotated[
        Path | None,
        typer.Option(
            '--edges-from',
            metavar='FILE',
            help=(
                "Count each column against FILE's histogram edges and value list, writing the rows outside them as overflow. "
                'FILE is returned unchanged when nothing it was made from has changed: the files, the settings, '
                'the versions of the tools, readers and detector packs that made it, and the code lists it read.'
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
    code_lists: Annotated[
        list[str] | None,
        typer.Option(
            '--code-list',
            metavar='NAME=FILE',
            help=(
                'Hand the detectors that read the code list NAME the codes in FILE, one a line; repeat it for more than one. '
                'With ndc=FILE, icd10cm=FILE or hcpcs_level2=FILE, the healthcare pack types such a code only where FILE holds it. '
                'The profile records its sha256, not its path.'
            ),
        ),
    ] = None,
    tables: Annotated[
        bool,
        typer.Option(
            '--tables',
            help=(
                'Profile every table DATA holds into a bundle. A directory or prefix is grouped as an AWS Glue crawler groups one: '
                'folders sharing most of their column names are one table, named for their common stem.'
            ),
        ),
    ] = False,
) -> None:
    """Profile a dataset into a file holding no rows: doppeldata profile create data/orders/ --out orders.profile.yaml

    Each file is read as its suffix's format. S3 is read with the AWS environment's credentials, and polars reads text formats from
    local files only. A directory or prefix whose folders hold unlike column names is refused, unless --tables writes a bundle
    of its tables.
    """
    if tables and edges_from is not None:
        raise typer.BadParameter("--edges-from is one table's profile, so it cannot be given with --tables", param_hint='--edges-from')
    if out is not None and not out.parent.is_dir():
        raise typer.BadParameter(f'{out.parent} is not a directory; create it, or name a FILE in one that exists', param_hint='--out')
    if engine not in ENGINES:
        raise typer.BadParameter(f'{engine!r} is not a profiling engine; choose from {", ".join(ENGINES)}', param_hint='--engine')
    try:
        applied_policy(policy, k_min)
    except ValueError as error:
        raise typer.BadParameter(str(error), param_hint='--policy') from error
    try:
        earlier = Profile.load(edges_from) if edges_from is not None else None
    except (OSError, InvalidProfile) as error:
        raise typer.BadParameter(str(error), param_hint='--edges-from') from error
    named_lists = named_files(code_lists or [], '--code-list')
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always', ProfilingWarning)
            profiled: Profile | Bundle = (
                profile_tables(data, engine=engine, sample=sample, policy=policy, k_min=k_min, packs=packs or (), code_lists=named_lists)
                if tables
                else profile(
                    data,
                    engine=engine,
                    sample=sample,
                    policy=policy,
                    k_min=k_min,
                    edges_from=earlier,
                    packs=packs or (),
                    code_lists=named_lists,
                )
            )
    except InvalidCodeList as error:
        raise typer.BadParameter(str(error), param_hint='--code-list') from error
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


def named_files(pairs: list[str], option: str) -> dict[str, Path]:
    """Each `NAME=FILE` an option was given, by name, refused with exit 2 where one has no `=` or a name repeats."""
    named: dict[str, Path] = {}
    for pair in pairs:
        name, equals, file = pair.partition('=')
        if not equals or not name or not file:
            raise typer.BadParameter(f'{pair!r} is not NAME=FILE', param_hint=option)
        if name in named:
            raise typer.BadParameter(f'{name} is named twice', param_hint=option)
        named[name] = Path(file)
    return named


def print_notices(caught: list[warnings.WarningMessage]) -> None:
    """Print each profiling warning as one line on stderr, and any other warning as Python shows it."""
    for notice in caught:
        if issubclass(notice.category, ProfilingWarning):
            typer.echo(f'doppeldata: {notice.message}', err=True)
        else:
            warnings.showwarning(notice.message, notice.category, notice.filename, notice.lineno)


def loaded_profile(name: str, param_hint: str) -> Profile:
    """A profile file, a short YAML, or an example doppeldata ships, refused with exit 2 when it is none or does not validate."""
    try:
        return file_profile(name)
    except (OSError, InvalidProfile) as error:
        raise typer.BadParameter(str(error), param_hint=param_hint) from error


def profile_or_columns(profile_name: str | None, columns: str | None) -> Profile:
    """The profile PROFILE names, or the one --columns expands to, refused with exit 2 unless exactly one is given."""
    if (profile_name is None) == (columns is None):
        given = 'both' if profile_name is not None else 'neither'
        raise typer.BadParameter(f'give PROFILE or --columns, not {given}', param_hint='PROFILE')
    if profile_name is not None:
        return loaded_profile(profile_name, 'PROFILE')
    names = [name.strip() for name in (columns or '').split(',') if name.strip()]
    unmatched = [no_fake_message(name, COMMAND_LINE_REMEDY)[0] for name in names if named_column(name) is None]
    if unmatched:
        raise typer.BadParameter('\n'.join(unmatched), param_hint='--columns')
    try:
        return Profile.expand(names)
    except (InvalidProfile, ValueError) as error:
        raise typer.BadParameter(str(error), param_hint='--columns') from error


def read_scenario(name: str) -> Scenario:
    """A scenario file, or an example doppeldata ships. A name that is neither raises FileNotFoundError."""
    if not Path(name).is_file() and name in example_scenarios.example_names():
        return Scenario.example(name)
    return Scenario.load(name)


def scenario_named(name: str, param_hint: str) -> Scenario:
    """The scenario file or example `name`, refused with exit 2 when it is neither or cannot be read. InvalidScenario passes."""
    try:
        return read_scenario(name)
    except FileNotFoundError as error:
        known = ', '.join(example_scenarios.example_names())
        message = f'{name!r} is neither a scenario file nor an example; name a file, or one of {known}'
        raise typer.BadParameter(message, param_hint=param_hint) from error
    except OSError as error:
        raise typer.BadParameter(f'{name!r} could not be read: {error.strerror}', param_hint=param_hint) from error


def loaded_scenario(names: list[str] | None) -> Scenario | None:
    """The scenarios named, composed in order, refused with exit 2 when one is neither a file nor an example, or does not validate."""
    scenarios = []
    for name in names or []:
        try:
            scenarios.append(scenario_named(name, '--scenario'))
        except InvalidScenario as error:
            raise typer.BadParameter(str(error), param_hint='--scenario') from error
    return Scenario.compose(*scenarios) if scenarios else None


@contextlib.contextmanager
def failure_exits_3() -> Iterator[None]:
    """End a check verb on 3 when what it wraps fails, so its 1 means drift and nothing else. A usage error keeps 2, and no answer 4."""
    try:
        yield
    except DoppeldataError as error:
        if error.exit_code == 1:
            error.exit_code = 3
        raise
    except OSError as error:
        typer.echo(f'doppeldata: the comparison stopped: {error}', err=True)
        raise typer.Exit(3) from error
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
    unmeasured = sum(result.unmeasured is not None for result in report.metrics)
    if unmeasured:
        message = f'{counted(unmeasured, "metric")} unmeasured and passed, because B holds nothing to compare them with'
        typer.echo(f'doppeldata: {message}; a B profiled with --edges-from A carries the overflow boundary_adherence reads', err=True)
    if report.drifted:
        raise ProfilesDrifted(report)


@data_app.command('check', no_args_is_help=True, epilog=f'{METRIC_DEFINITIONS}\n\n{SCORE_DEFINITIONS}')
def data_check(
    profile_name: Annotated[
        str, typer.Argument(metavar='PROFILE', help='The profile the data was generated from, a file or an example name.')
    ],
    data: Annotated[str, typer.Argument(metavar='DATA', help='A file, directory or s3:// prefix holding the generated table.')],
    thresholds: ThresholdsOption = None,
    scenario: ScenarioOption = None,
    against: Annotated[
        str | None,
        typer.Option(
            '--against',
            metavar='REAL_DATA',
            help=(
                "Also score DATA against REAL_DATA by the scores listed below, which follow SDMetrics' definitions and need no "
                'SDMetrics. The report holds the scores and no real row.'
            ),
        ),
    ] = None,
    against_sample: Annotated[
        int | None,
        typer.Option(
            '--against-sample',
            metavar='N',
            min=1,
            help=f'With --against, score a seeded random sample of N rows from each side. Defaults to {DEFAULT_SAMPLE_ROWS:,}.',
        ),
    ] = None,
    html: Annotated[
        Path | None,
        typer.Option(
            '--html',
            metavar='FILE',
            help=(
                'With --against, write the two samples side by side to FILE with sweetviz, which doppeldata does not install. '
                "It shows the real data's extremes and categories, so it is not for sharing."
            ),
        ),
    ] = None,
    as_json: JsonOption = False,
) -> None:
    """Check generated data against its profile: doppeldata data check orders.profile.yaml out/orders/

    DATA is profiled with PROFILE's histogram edges and value lists, and each metric compares the two profiles.
    With --scenario, DATA is measured against the profile the scenario's patch makes, each metric a perturbation moves is
    skipped, naming it, and each expectation the scenario declares is measured.
    Only --against reads the real data. Exits 0 when every metric, column and expectation passes, and 3 when one does not
    or the check could not finish.
    """
    source = loaded_profile(profile_name, 'PROFILE')
    chosen = loaded_thresholds(thresholds)
    applied = loaded_scenario(scenario)
    if against is None:
        for given, hint in ((html, '--html'), (against_sample, '--against-sample')):
            if given is not None:
                raise typer.BadParameter(f'{hint} reads the real data beside DATA; name it with --against', param_hint=hint)
    elif not scored_columns(source.to_plain()).ordered:
        raise typer.BadParameter(NOTHING_SCORED, param_hint='--against')
    unwritten = unwritable(html) if html is not None else None
    if unwritten is not None:
        raise typer.BadParameter(unwritten, param_hint='--html')
    try:
        with failure_exits_3(), warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always', ProfilingWarning)
            report = check(source, data, thresholds=chosen, scenario=applied, against=against, against_sample=against_sample, html=html)
            shown = report.to_json() if as_json else report.to_markdown()
    except InvalidThresholds as error:
        raise typer.BadParameter(str(error), param_hint='--thresholds') from error
    except SourceError as error:
        if error.exit_code != 2:
            raise
        raise typer.BadParameter(str(error), param_hint='--against' if error.role == 'against' else 'DATA') from error
    print_notices(caught)
    typer.echo(shown, nl=False)
    if not (report.passed and report.verified):
        raise CheckFailed(report)


@profile_app.command('expand', no_args_is_help=True)
def profile_expand(
    profile_name: ProfileOrColumnsArgument = None,
    columns: ColumnsOption = None,
    rows: Annotated[
        int | None, typer.Option('--rows', metavar='N', min=0, help='Record N as the row count, which data generate defaults to.')
    ] = None,
    out: Annotated[Path | None, typer.Option('--out', metavar='FILE', help='Write the profile to FILE rather than print its YAML.')] = None,
) -> None:
    """Print the full profile a short YAML or --columns stands for: doppeldata profile expand people.yaml > people.profile.yaml

    data generate reads the full profile unchanged, and it is the file to edit once a column needs more than a fake or a range.
    """
    if out is not None and not out.parent.is_dir():
        raise typer.BadParameter(f'{out.parent} is not a directory; create it, or name a FILE in one that exists', param_hint='--out')
    expanded = profile_or_columns(profile_name, columns)
    if rows is not None:
        expanded = Profile.expand(expanded, rows=rows)
    if out is None:
        typer.echo(expanded.to_yaml(), nl=False)
        return
    try:
        expanded.save(out)
    except OSError as error:
        raise typer.BadParameter(f'{out} could not be written: {error.strerror or error}', param_hint='--out') from error


@profile_app.command('show', no_args_is_help=True)
def profile_show(name: Annotated[str, typer.Argument(metavar='NAME', help='An example name, as profile list prints it.')]) -> None:
    """Print an example profile's YAML, to start a profile from: doppeldata profile show orders > orders.profile.yaml"""
    if name not in example_names():
        raise typer.BadParameter(
            f'{name!r} is not an example profile doppeldata ships; choose from {", ".join(example_names())}', param_hint='NAME'
        )
    typer.echo(example_text(name), nl=False)


@profile_app.command('check', no_args_is_help=True)
def profile_check(profile_name: ProfileArgument, as_json: JsonOption = False) -> None:
    """Check a profile against its schema and privacy policy, from the file alone: doppeldata profile check orders.profile.yaml

    Each violation prints as the field it is at, what was found and what was expected, and the command exits 3. Every
    literal must be listed in privacy.audit and meet its rule, counted as a rate of the rows the profile describes.
    """
    path = Path(profile_name)
    try:
        if path.is_file():
            report = ProfileFileCheck.of(profile_name, file_checks(path))
        elif profile_name in example_names():
            with importlib.resources.as_file(example_resource(profile_name)) as example:
                report = ProfileFileCheck.of(profile_name, file_checks(example))
        else:
            message = f'{profile_name!r} is neither a profile file nor an example; name a file, or one of {", ".join(example_names())}'
            raise typer.BadParameter(message, param_hint='PROFILE')
    except OSError as error:
        raise typer.BadParameter(f'{profile_name} cannot be read: {error.strerror}', param_hint='PROFILE') from error
    if as_json:
        typer.echo(json.dumps(dc.asdict(report), indent=2))
    else:
        for check in report.tables:
            typer.echo(checked_table_line(check))
            for violation in check.violations:
                typer.echo(f'  {violation.field}: {violation.detail} [{violation.code}]')
    if report.violations:
        raise CheckFailed(report)


def checked_table_line(check: TableCheck) -> str:
    """One table's line in profile check: its name, policy and literal fields, and how many violations follow."""
    if check.policy is None:
        return f'{check.table}: {counted(len(check.violations), "schema violation")}'
    floor = f', k_min {check.k_min}' if check.k_min is not None else ''
    audited = f'{counted(check.literal_fields, "literal field")} under {check.policy}{floor}'
    return f'{check.table}: {audited}, {counted(len(check.violations), "violation")}'


@dc.dataclass(frozen=True)
class DataChoices:
    """What data plan and data generate share: the profile, the rows and seed, --out, what the write lays over the profile,
    and the scenario."""

    profile: Profile
    rows: int | None
    seed: int
    out: str
    write: WriteChoices
    scenario: Scenario | None

    def arguments(self) -> dict[str, Any]:
        return {'rows': self.rows, 'seed': self.seed, **dc.asdict(self.write), 'scenario': self.scenario}


def data_choices(
    profile_name: str | None,
    columns: str | None,
    rows: int | None,
    seed: int,
    file_format: str | None,
    out: str,
    file_rows: int | None,
    partition_by: list[str] | None,
    no_partitions: bool,
    partition_style: str | None,
    writer: str | None,
    scenario: list[str] | None,
) -> DataChoices:
    """The flags both data verbs take, each refused here with exit 2 when it names nothing doppeldata writes."""
    write = write_choices(file_format, file_rows, partition_by, no_partitions, partition_style, writer)
    return DataChoices(profile_or_columns(profile_name, columns), rows, seed, out, write, loaded_scenario(scenario))


def write_choices(
    file_format: str | None,
    file_rows: int | None,
    partition_by: list[str] | None,
    no_partitions: bool,
    partition_style: str | None,
    writer: str | None,
) -> WriteChoices:
    """What the write flags lay over the profile, each refused with exit 2 when it names nothing doppeldata writes."""
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
    return WriteChoices(file_format, file_rows, keys, partition_style, settings)


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
    out: OutOption,
    profile_name: ProfileOrColumnsArgument = None,
    columns: ColumnsOption = None,
    rows: RowsOption = None,
    seed: SeedOption = 0,
    file_format: FormatOption = None,
    file_rows: FileRowsOption = None,
    partition_by: PartitionByOption = None,
    no_partitions: NoPartitionsOption = False,
    partition_style: PartitionStyleOption = None,
    writer: WriterOption = None,
    scenario: ScenarioOption = None,
    as_json: JsonOption = False,
) -> None:
    """Print what data generate would write with the same flags: doppeldata data plan orders --rows 1000000000 --out s3://bucket/orders/

    It reads nothing at --out, and generates the first blocks to estimate the size. The --json shape may change before 1.0.
    """
    choices = data_choices(
        profile_name, columns, rows, seed, file_format, out, file_rows, partition_by, no_partitions, partition_style, writer, scenario
    )
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
    elif (one_file := one_file_of(choices.out)) is not None:
        files = f'1 file, {one_file}'
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
    if choices.scenario is not None:
        lines.insert(2, f'scenario   {choices.scenario.name} {identity["scenario_digest"]}')
    if planned.rows_written != int(identity['dataset_rows']):
        lines.insert(3, f'rows       {planned.rows_written:,} written, the generated rows and the copies duplicates adds')
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
    out: OutOption,
    profile_name: ProfileOrColumnsArgument = None,
    columns: ColumnsOption = None,
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
            '--replace',
            help='Delete what an earlier doppeldata write left at a directory --out first, where anything else refuses the write. '
            'A one-file --out is written over.',
        ),
    ] = False,
    threads: Annotated[
        int, typer.Option('--threads', metavar='N', min=0, help='Threads that generate and encode. 0 takes one per core.')
    ] = 0,
    scenario: ScenarioOption = None,
    manifest_beside: Annotated[
        bool,
        typer.Option('--manifest', help='Write _manifest.json beside a one-file --out. A directory or S3 write always writes one.'),
    ] = False,
) -> None:
    """Write a dataset, as data plan prints it: doppeldata data generate --columns name,email,zip --rows 1000 --out people.csv

    A directory or S3 --out holds the files and their _manifest.json: doppeldata data generate orders --out orders/. A
    destination holding anything is refused unless --replace is given. A write that stops after claiming a directory, on a
    failure, Ctrl-C or SIGTERM, leaves _manifest.partial.json listing the files it completed. A one-file write that stops leaves
    nothing, unless it is killed outright, which leaves a staging directory beside the file for --replace to remove. S3 is
    written with the AWS environment's credentials.
    """
    choices = data_choices(
        profile_name, columns, rows, seed, file_format, out, file_rows, partition_by, no_partitions, partition_style, writer, scenario
    )
    one_file = one_file_of(choices.out)
    terminate = signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        manifest = doppeldata.write(
            choices.profile, choices.out, **choices.arguments(), threads=threads, replace=replace, manifest=manifest_beside
        )
    finally:
        signal.signal(signal.SIGTERM, terminate)
    document = manifest.document
    generated = int(manifest.identity['dataset_rows'])
    rows_written = sum(file['rows'] for file in manifest.files)
    copies = f' ({generated:,} generated, the rest copies)' if rows_written != generated else ''
    seconds = max(document['elapsed_ms'], 1) / 1000
    stored = size(sum(file['bytes'] for file in manifest.files))
    typer.echo(
        f'wrote {counted(rows_written, "row")}{copies} as {document["format"]} in {counted(len(manifest.files), "file")}, {stored}, '
        f'in {seconds:.2f} s on {counted(document["threads"], "thread")}: {rows_written / seconds / 1e6:.2f}M rows/s'
    )
    if one_file is not None and not manifest_beside:
        typer.echo(f'{one_file} holds dataset {manifest.dataset_digest}')
    else:
        typer.echo(f'{beside(document["destination"], MANIFEST_FILE_NAME)} names dataset {manifest.dataset_digest}')


ID_PARAMETER = "Keys the op's random stream. Defaults to <op>:<column, or table>:<n>, n counting earlier entries of that op and column."


def one_line(schema: dict[str, Any]) -> str:
    return ' '.join((schema.get('description') or '').split())


def parameter_type(schema: dict[str, Any] | bool) -> str:
    """A parameter's type as a person reads it: `string`, `integer, at least 1`, `one of MCAR, MAR, MNAR`, or its definition's
    description. A schema of `true` takes any value."""
    if isinstance(schema, bool):
        return 'anything'
    definitions = scenario_schema()['$defs']
    branches = [branch for branch in schema.get('anyOf', [schema]) if branch.get('type') != 'null']
    part = branches[0] if branches else schema
    if '$ref' in part:
        name = part['$ref'].rsplit('/', 1)[1]
        definition = definitions[name]
        listed = 'enum' in definition or all('const' in branch for branch in definition.get('oneOf', [{}]))
        return f'one of {", ".join(enum_values(definition))}' if listed else one_line(definition).removesuffix('.') or name
    types = part.get('type')
    kind = ' or '.join(each for each in (types if isinstance(types, list) else [types]) if each and each != 'null')
    if kind == 'array':
        return f'a list of {parameter_type(part["items"])}'
    if kind == 'object' and isinstance(part.get('additionalProperties'), dict):
        return f'a map to {parameter_type(part["additionalProperties"])}'
    return kind + (f', at least {part["minimum"]:g}' if 'minimum' in part else '')


@scenario_app.command('list')
def scenario_list(as_json: JsonOption = False) -> None:
    """List the ops a scenario applies, the metrics its expect names, and the example scenarios doppeldata ships.

    scenario show takes an op's or an example's name.
    """
    ops = [{'name': name, 'description': one_line(branch)} for name, branch in op_schemas().items()]
    metrics = [
        {'name': branch['const'], 'description': one_line(branch)} for branch in scenario_schema()['$defs']['ExpectedMetric']['oneOf']
    ]
    examples = [
        {'name': name, 'description': Scenario.example(name).document.get('description') or ''}
        for name in example_scenarios.example_names()
    ]
    sections = {'ops': ops, 'metrics': metrics, 'examples': examples}
    if as_json:
        typer.echo(json.dumps(sections, indent=2))
        return
    width = max(len(row['name']) for rows in sections.values() for row in rows)
    lines: list[str] = []
    for title, rows in sections.items():
        lines += [*([''] if lines else []), title, *(f'  {row["name"]:<{width}}  {row["description"]}' for row in rows)]
    typer.echo('\n'.join(lines))


@scenario_app.command('show', no_args_is_help=True)
def scenario_show(name: Annotated[str, typer.Argument(metavar='NAME', help='An op or an example, as scenario list prints it.')]) -> None:
    """Print an op's parameters, or an example scenario's YAML to start one from: doppeldata scenario show hot_keys"""
    if name in example_scenarios.example_names():
        typer.echo(f'doppeldata: {name} is an example scenario, written for the orders example profile', err=True)
        typer.echo(example_scenarios.example_text(name), nl=False)
        return
    if name not in op_schemas():
        known = [*op_schemas(), *example_scenarios.example_names()]
        issue = unknown_name('unknown-op', name, known, (), 'an op or an example scenario', 'both')
        raise typer.BadParameter(issue.message, param_hint='NAME')
    branch = op_schemas()[name]
    parameters = {key: value for key, value in branch['properties'].items() if key != 'op'}
    width = max(len(key) for key in parameters)
    lines = [f'op {name}: {one_line(branch)}', '']
    for key, schema in parameters.items():
        needed = 'required' if key in branch.get('required', ()) else 'optional'
        described = ID_PARAMETER if key == 'id' else one_line(schema)
        lines.append(f'  {key:<{width}}  {needed}  {parameter_type(schema)}' + (f'. {described}' if described else ''))
    typer.echo('\n'.join(lines))


@scenario_app.command('check', no_args_is_help=True)
def scenario_check(
    scenario_name: Annotated[
        str, typer.Argument(metavar='SCENARIO', help='A scenario file, or an example name as scenario list prints it.')
    ],
    profile_name: Annotated[str, typer.Argument(metavar='PROFILE', help='The profile it applies to, a file or an example name.')],
    rows: RowsOption = None,
    seed: SeedOption = 0,
    file_format: FormatOption = None,
    file_rows: FileRowsOption = None,
    partition_by: PartitionByOption = None,
    no_partitions: NoPartitionsOption = False,
    as_json: JsonOption = False,
) -> None:
    """Check a scenario against a profile without generating: doppeldata scenario check hot-key.scenario.yaml orders

    It validates the scenario, applies its patch, and plans each perturbation and the file layout as data generate would with
    the same flags. Exits 0 when the scenario holds, and 3 when it does not, naming each fault and the JSON Pointer to it.
    """
    source = loaded_profile(profile_name, 'PROFILE')
    write = write_choices(file_format, file_rows, partition_by, no_partitions, None, None)
    try:
        scenario = scenario_named(scenario_name, 'SCENARIO')
        prepared = prepared_write(source, 'memory://', rows, seed, write, scenario)
        prepared.engine.ranges_json(prepared.options)
    except InvalidScenario as error:
        if as_json:
            issues = [dc.asdict(issue) for issue in error.issues]
            typer.echo(json.dumps({'valid': False, 'source': error.source, 'issues': issues}, indent=2))
        else:
            typer.echo(f'doppeldata: {error}', err=True)
        raise typer.Exit(3) from error
    if as_json:
        typer.echo(json.dumps({'valid': True, 'source': scenario.source, 'issues': []}, indent=2))
        return
    plain = scenario.to_plain()
    held = f'{counted(len(plain.get("perturbations") or []), "perturbation")} and {counted(len(plain.get("expect") or []), "expectation")}'
    typer.echo(f'{scenario.source} holds for profile {source.document["name"]}: {held}, scenario {scenario.digest}')


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
