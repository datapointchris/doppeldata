import contextlib
import dataclasses as dc
import difflib
import importlib.metadata
import importlib.resources
import itertools
import json
import math
import signal
import traceback
import warnings
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Sequence
from pathlib import Path
from typing import Annotated
from typing import Any

import doppeldata
import typer
from doppeldata import Bundle
from doppeldata import CheckFailed
from doppeldata import DetectorPackError
from doppeldata import DoppeldataError
from doppeldata import InvalidArgument
from doppeldata import InvalidCodeList
from doppeldata import InvalidProfile
from doppeldata import InvalidScenario
from doppeldata import InvalidThresholds
from doppeldata import Issue
from doppeldata import IssueCarrier
from doppeldata import Profile
from doppeldata import ProfilesDrifted
from doppeldata import ProfilingWarning
from doppeldata import Scenario
from doppeldata import SourceError
from doppeldata import StoppedWrite
from doppeldata import Thresholds
from doppeldata import _core
from doppeldata import capabilities
from doppeldata import check
from doppeldata import diff
from doppeldata import example_scenarios
from doppeldata import issue_codes
from doppeldata import profile
from doppeldata import profile_tables
from doppeldata.bundle_generation import BundleWritePlan
from doppeldata.dataset_generation import GenerationSource
from doppeldata.dataset_generation import WriteChoices
from doppeldata.dataset_generation import generation_source
from doppeldata.dataset_generation import one_file_of
from doppeldata.dataset_generation import prepared_write
from doppeldata.example_profiles import bundle_example_names
from doppeldata.example_profiles import bundle_example_resource
from doppeldata.example_profiles import bundle_example_text
from doppeldata.example_profiles import example_names
from doppeldata.example_profiles import example_resource
from doppeldata.example_profiles import example_text
from doppeldata.issue_codes import issue_table
from doppeldata.manifest import MANIFEST_FILE_NAME
from doppeldata.manifest import PARTIAL_MANIFEST_FILE_NAME
from doppeldata.privacy_policy import APPLIED_POLICIES
from doppeldata.privacy_policy import DEFAULT_K_MIN
from doppeldata.privacy_policy import ProfileFileCheck
from doppeldata.privacy_policy import file_checks
from doppeldata.profile_comparison import NOTHING_SCORED
from doppeldata.profile_comparison.metrics import METRICS
from doppeldata.profile_comparison.real_data_scores import DEFAULT_SAMPLE_ROWS
from doppeldata.profile_comparison.real_data_scores import SCORES
from doppeldata.profile_comparison.real_data_scores import scored_columns
from doppeldata.profile_comparison.relationship_metrics import RELATIONSHIP_METRICS
from doppeldata.profile_comparison.side_by_side_html import unwritable
from doppeldata.profile_exports import EXPORT_FORMATS
from doppeldata.profile_exports import export
from doppeldata.profile_sources import bundle_from
from doppeldata.profiling import ENGINES
from doppeldata.profiling import applied_policy
from doppeldata.published_schemas import enum_values
from doppeldata.published_schemas import output_formats
from doppeldata.published_schemas import partition_styles
from doppeldata.published_schemas import schema_names
from doppeldata.published_schemas import schema_summaries
from doppeldata.published_schemas import schema_text
from doppeldata.scenario_format import composed
from doppeldata.scenario_format import op_schemas
from doppeldata.scenario_format import scenario_schema
from doppeldata.scenario_format import unknown_name
from doppeldata.short_profiles import COMMAND_LINE_REMEDY
from doppeldata.short_profiles import file_profile
from doppeldata.short_profiles import named_column
from doppeldata.short_profiles import no_fake_message
from typer._click.exceptions import NoArgsIsHelpError
from typer._click.exceptions import NoSuchOption
from typer._click.exceptions import UsageError
from typer.core import TyperGroup

from doppeldata_cli.command_surface import command_surface


def beside(destination: str, name: str) -> str:
    """The object `name` at the top of `destination`: orders/_manifest.json, memory://_manifest.json."""
    return destination + name if destination.endswith('/') else f'{destination}/{name}'


def report_refusal(error: IssueCarrier, *, as_json: bool) -> None:
    """A refusal on stderr: one `{"error": ...}` line of JSON per issue where `as_json`, else its text after `doppeldata: `."""
    if as_json:
        for issue in error.issues:
            typer.echo(json.dumps({'error': issue.to_json()}, ensure_ascii=False), err=True)
        return
    typer.echo(f'doppeldata: {error}', err=True)


def report_stopped_write(error: StoppedWrite, *, as_json: bool) -> None:
    """Say why the write stopped, and what it left behind where it stored a partial manifest."""
    report_refusal(error, as_json=as_json)
    if not as_json and error.manifest is not None and error.manifest_stored and error.manifest.partial:
        location = beside(error.manifest.document['destination'], PARTIAL_MANIFEST_FILE_NAME)
        completed = len(error.manifest.files)
        if error.manifest.cdc is not None:
            completed += len(error.manifest.cdc['files']) + len(error.manifest.cdc['expected'])
        typer.echo(f'doppeldata: {location} lists the {counted(completed, "file")} the write completed', err=True)


# Set in the root context's meta, which every context of one invocation shares, before any parameter is parsed.
JSON_REQUESTED = 'doppeldata.json'


class DoppeldataGroup(TyperGroup):
    """The root group. A refusal prints on stderr with its code, as one line of JSON per issue where the command line asks
    for JSON, and the command exits with the code's status.

    A parameter refused because the library refused what it names reports the library's issues, each naming the parameter
    where it names no place in a document. Under JSON, a command line the parser refuses prints as E-USAGE-001 and exits
    2. Without it, the parser's own text prints.
    """

    def parse_args(self, ctx: Any, args: list[str]) -> list[str]:
        ctx.meta[JSON_REQUESTED] = '--json' in itertools.takewhile(lambda word: word != '--', args)
        return super().parse_args(ctx, args)

    def invoke(self, ctx: Any) -> Any:
        as_json = bool(ctx.meta.get(JSON_REQUESTED))
        try:
            return super().invoke(ctx)
        except StoppedWrite as error:
            report_stopped_write(error, as_json=as_json)
            raise typer.Exit(error.exit_code) from error
        except IssueCarrier as error:
            report_refusal(error, as_json=as_json)
            raise typer.Exit(error.exit_code) from error
        except UsageError as error:
            if isinstance(error, NoArgsIsHelpError):
                raise
            refused = parameter_refusal(error)
            if refused is None and not as_json:
                raise
            refused = refused or command_line_refusal(error)
            report_refusal(refused, as_json=as_json)
            raise typer.Exit(refused.exit_code) from error


def refused(code: str, message: str, parameter: str, *, value: Any = None, valid: Sequence[str] = ()) -> InvalidArgument:
    """The refusal of what `parameter` was given, naming the values it takes and the nearest of them to `value`."""
    closest = difflib.get_close_matches(value, list(valid), n=3) if isinstance(value, str) else []
    return InvalidArgument(code, message, field=parameter, value=value, valid=valid, closest=closest)


def parameter_named(error: UsageError) -> str | None:
    """The option or argument a parser refusal names, as the command line spells it: --rows, PROFILE."""
    if not isinstance(error, typer.BadParameter):
        return None
    if isinstance(error.param_hint, str):
        return error.param_hint
    if error.param_hint:
        return next(iter(error.param_hint))
    parameter = error.param
    if parameter is None:
        return None
    if parameter.param_type_name == 'option':
        return max(parameter.opts, key=len)
    return parameter.human_readable_name


def parameter_refusal(error: UsageError) -> IssueCarrier | None:
    """A parameter refused for what the library refused, or could not read, as that refusal at the parameter, else None."""
    cause, parameter = error.__cause__, parameter_named(error)
    if isinstance(cause, IssueCarrier):
        issues = [issue if is_pointer(issue.field) or parameter is None else dc.replace(issue, field=parameter) for issue in cause.issues]
        located = IssueCarrier(issues, headline=cause.headline)
        located.exit_code = cause.exit_code
        return located
    if isinstance(cause, OSError):
        return IssueCarrier([Issue(issue_codes.FILE_UNREADABLE, parameter, error.format_message(), cause.filename)])
    return None


def is_pointer(field: str | None) -> bool:
    """Whether `field` is a JSON Pointer into a document, rather than an argument's name."""
    return field is not None and (field == '' or field.startswith('/'))


def command_line_refusal(error: UsageError) -> IssueCarrier:
    """The parser's refusal as E-USAGE-001, naming the parameter and the values it takes where the parser knows them."""
    context = error.ctx
    field: str | None = None
    valid: Sequence[str] = ()
    closest: Sequence[str] = ()
    if isinstance(error, typer.BadParameter):
        field = parameter_named(error)
        valid = list(getattr(error.param.type, 'choices', None) or ()) if error.param is not None else []
    elif isinstance(error, NoSuchOption):
        field = error.option_name
        closest = list(error.possibilities or ())
        if context is not None:
            valid = sorted(name for parameter in context.command.params for name in parameter.opts if name.startswith('--'))
    elif context is not None and (list_commands := getattr(context.command, 'list_commands', None)) is not None:
        field = 'command'
        valid = list_commands(context)
    hint = None if context is None else f'{context.command_path} --help'
    issue = Issue(issue_codes.COMMAND_LINE, field, error.format_message(), None, tuple(valid), tuple(closest), hint)
    return IssueCarrier([issue])


app = typer.Typer(name='doppeldata', no_args_is_help=True, cls=DoppeldataGroup)
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
capabilities_app = typer.Typer(
    no_args_is_help=True, help='What a program or an agent can name: every command, format, op, fake, semantic type, policy and issue code.'
)
app.add_typer(capabilities_app, name='capabilities')

JsonOption = Annotated[bool, typer.Option('--json', help='Print JSON instead of a table.')]
# capabilities show wraps each list at this width, and never inside an item.
LISTED_WIDTH = 120
PROFILE_HELP = (
    'A profile file, a short YAML of rows and columns, a Parquet or Arrow file written with --embed-profile, '
    'a bundle of related tables as a directory holding bundle.yaml or a kind: bundle file, or an example name as profile '
    'list prints it.'
)
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
WRITTEN_WITH = 'or to what a Parquet or Arrow PROFILE was written with'
RowsOption = Annotated[
    int | None,
    typer.Option(
        '--rows',
        metavar='N',
        min=0,
        help="Rows to generate, before any copies a scenario's duplicates adds. "
        f'Defaults to the row count the profile records, {WRITTEN_WITH}.',
    ),
]
SeedOption = Annotated[
    int | None,
    typer.Option(
        '--seed',
        metavar='S',
        min=0,
        max=2**64 - 1,
        help=f'The seed. One profile, seed and row count give the same values anywhere. Defaults to 0, {WRITTEN_WITH}.',
    ),
]
ScaleOption = Annotated[
    float | None,
    typer.Option(
        '--scale',
        metavar='X',
        help="With a bundle PROFILE, generate X times each table's row count, each relationship keeping its fan-out shape. Defaults to 1.",
    ),
]
EmbedProfileOption = Annotated[
    bool,
    typer.Option(
        '--embed-profile',
        help="Write the profile into each Parquet or Arrow file's metadata, where any PROFILE argument reads it back. "
        'Other formats refuse it.',
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
    f'{metric.name}: {metric.summary}. Passes at {metric.bound.value.removeprefix("at_")} {metric.threshold:g} by default'
    + (', set under defaults alone.' if metric.pairs else '.')
    for metric in METRICS
)
RELATIONSHIP_DEFINITIONS = 'On a bundle, each relationship is measured by these, each set under defaults alone.\n\n' + '\n\n'.join(
    f'{metric.name}: {metric.summary}. Passes at {metric.bound.value.removeprefix("at_")} {metric.threshold:g} by default.'
    for metric in RELATIONSHIP_METRICS
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
    """List the hand-written example profiles and bundles doppeldata ships, by the name profile show takes."""
    examples = [Profile.example(name).document for name in example_names()]
    rows: list[dict[str, Any]] = [
        {'name': doc['name'], 'kind': 'table', 'rows': doc['dataset']['rows'], 'columns': len(doc['columns'])} for doc in examples
    ]
    for name in bundle_example_names():
        bundle = Bundle.example(name)
        rows.append(
            {
                'name': name,
                'kind': 'bundle',
                'rows': sum(table.document['dataset']['rows'] for table in bundle.tables.values()),
                'tables': len(bundle.tables),
                'relationships': len(bundle.document.get('relationships') or []),
            }
        )
    if as_json:
        typer.echo(json.dumps(rows, indent=2))
        return
    width = max(len(row['name']) for row in rows)
    for row in rows:
        if row['kind'] == 'bundle':
            held = f'bundle of {counted(row["tables"], "table")} and {counted(row["relationships"], "relationship")}'
        else:
            held = f'{row["columns"]} columns'
        typer.echo(f'{row["name"]:<{width}}  {held}, {row["rows"]:,} rows')


@profile_app.command('create', no_args_is_help=True)
def profile_create(
    data: Annotated[
        list[str],
        typer.Argument(
            metavar='DATA...',
            help='Files, directories or s3:// prefixes holding one table, all of one format. With --tables, each names its own tables.',
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
                'Every correlation and conditional pair FILE states is stated again, however weak, where k_min allows. '
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
                'Profile every table DATA holds into a bundle, with the foreign keys between them. Each path named is grouped '
                'on its own, so two files are two tables, and naming their directory makes them one. A directory or prefix is '
                'grouped as an AWS Glue crawler groups one: folders sharing most of their column names are one table, named for '
                'their common stem.'
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
    writable_out(out)
    if engine not in ENGINES:
        raise refused(issue_codes.UNKNOWN_ENGINE, f'{engine!r} is not a profiling engine', '--engine', value=engine, valid=list(ENGINES))
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
        if error.exit_code != 2:
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
    """A profile file, a short YAML, a data file holding a profile, or an example doppeldata ships, refused with exit 2 when it
    is none or does not validate."""
    try:
        return file_profile(name)
    except (OSError, InvalidProfile, SourceError) as error:
        raise typer.BadParameter(str(error), param_hint=param_hint) from error


def profile_or_columns(profile_name: str | None, columns: str | None) -> Profile:
    """The profile PROFILE names, or the one --columns expands to, refused with exit 2 unless exactly one is given."""
    if (profile_name is None) == (columns is None):
        given = 'both' if profile_name is not None else 'neither'
        raise typer.BadParameter(f'give PROFILE or --columns, not {given}', param_hint='PROFILE')
    if profile_name is not None:
        return loaded_profile(profile_name, 'PROFILE')
    names = [name.strip() for name in (columns or '').split(',') if name.strip()]
    issues = [
        Issue(issue_codes.NO_FAKE_FOR_NAME, '--columns', message, name, (), closest)
        for name in names
        if named_column(name) is None
        for message, closest in [no_fake_message(name, COMMAND_LINE_REMEDY)]
    ]
    if issues:
        raise IssueCarrier(issues, headline=None if len(issues) == 1 else '--columns names columns no fake draws')
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
        message = f'{name!r} is neither a scenario file nor an example; name a file, or an example'
        raise refused(issue_codes.SCENARIO_NOT_FOUND, message, param_hint, value=name, valid=example_scenarios.example_names()) from error
    except OSError as error:
        message = f'{name!r} could not be read: {error.strerror}'
        raise refused(issue_codes.FILE_UNREADABLE, message, param_hint, value=name) from error


def writable_out(out: Path | None) -> None:
    """Refuse --out before anything is read where the directory it names does not exist."""
    if out is not None and not out.parent.is_dir():
        message = f'{out.parent} is not a directory; create it, or name a FILE in one that exists'
        raise refused(issue_codes.OUT_UNWRITABLE, message, '--out', value=str(out))


def unwritten_out(out: Path, error: OSError) -> InvalidArgument:
    """The refusal of --out where the write to it failed."""
    return refused(issue_codes.OUT_UNWRITABLE, f'{out} could not be written: {error.strerror or error}', '--out', value=str(out))


def loaded_scenario(names: list[str] | None) -> Scenario | None:
    """The scenarios named, composed in order, refused with exit 2 when one is neither a file nor an example, or does not validate."""
    scenarios = loaded_scenarios(names)
    return Scenario.compose(*scenarios) if scenarios else None


def loaded_scenarios(names: list[str] | None) -> list[Scenario]:
    """The scenarios named, in order, as a bundle takes them, each bending the table it names."""
    scenarios = []
    for name in names or []:
        try:
            scenarios.append(scenario_named(name, '--scenario'))
        except InvalidScenario as error:
            raise typer.BadParameter(str(error), param_hint='--scenario') from error
    return scenarios


def bundle_named(profile_name: str | None, columns: str | None, rows: int | None, scale: float | None) -> Bundle | None:
    """The bundle PROFILE names, or None where it names one table. Each is refused with exit 2 where it cannot be read, and
    with it --rows and --columns, which make one table, and --scale where PROFILE names one table."""
    try:
        bundle = bundle_from(profile_name) if profile_name is not None else None
    except (OSError, InvalidProfile, SourceError) as error:
        raise typer.BadParameter(str(error), param_hint='PROFILE') from error
    if bundle is None:
        if scale is not None:
            raise typer.BadParameter('--scale sizes a bundle, and PROFILE names one table; size it with --rows', param_hint='--scale')
        return None
    if columns is not None:
        raise typer.BadParameter('give PROFILE or --columns, not both', param_hint='PROFILE')
    if rows is not None:
        message = "--rows sizes one table, and PROFILE names a bundle; size it with --scale, a multiple of each table's row count"
        raise typer.BadParameter(message, param_hint='--rows')
    return bundle


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


@data_app.command('check', no_args_is_help=True, epilog=f'{METRIC_DEFINITIONS}\n\n{RELATIONSHIP_DEFINITIONS}\n\n{SCORE_DEFINITIONS}')
def data_check(
    profile_name: Annotated[
        str, typer.Argument(metavar='PROFILE', help='The profile or bundle the data was generated from, a file or an example name.')
    ],
    data: Annotated[
        str, typer.Argument(metavar='DATA', help="A file, directory or s3:// prefix holding the generated table, or a bundle's tables.")
    ],
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

    A bundle PROFILE checks each table under DATA/<table>/ against its own profile, then measures each relationship by the
    relationship metrics below: doppeldata data check orders-customers shop/. It takes no --against.
    """
    bundle = bundle_named(profile_name, None, None, None)
    if bundle is not None:
        if against is not None or against_sample is not None or html is not None:
            message = 'a bundle check scores no real data; check one table of the bundle against its real data on its own'
            raise refused(issue_codes.AGAINST_NEEDED, message, '--against')
        chosen = loaded_thresholds(thresholds)
        try:
            with failure_exits_3(), warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always', ProfilingWarning)
                bundle_report = check(bundle, data, thresholds=chosen, scenario=loaded_scenarios(scenario))
                shown = bundle_report.to_json() if as_json else bundle_report.to_markdown()
        except InvalidThresholds as error:
            raise typer.BadParameter(str(error), param_hint='--thresholds') from error
        except SourceError as error:
            if error.exit_code != 2:
                raise
            raise typer.BadParameter(str(error), param_hint='DATA') from error
        print_notices(caught)
        typer.echo(shown, nl=False)
        if not (bundle_report.passed and bundle_report.verified):
            raise CheckFailed(bundle_report)
        return
    source = loaded_profile(profile_name, 'PROFILE')
    chosen = loaded_thresholds(thresholds)
    applied = loaded_scenario(scenario)
    if against is None:
        for given, hint in ((html, '--html'), (against_sample, '--against-sample')):
            if given is not None:
                raise refused(issue_codes.AGAINST_NEEDED, f'{hint} reads the real data beside DATA; name it with --against', hint)
    elif not scored_columns(source.to_plain()).ordered:
        raise refused(issue_codes.NOTHING_SCORED, NOTHING_SCORED, '--against', value=against)
    unwritten = unwritable(html) if html is not None else None
    if unwritten is not None:
        raise refused(issue_codes.HTML_UNWRITABLE, unwritten, '--html', value=str(html))
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
    writable_out(out)
    expanded = profile_or_columns(profile_name, columns)
    if rows is not None:
        expanded = Profile.expand(expanded, rows=rows)
    if out is None:
        typer.echo(expanded.to_yaml(), nl=False)
        return
    try:
        expanded.save(out)
    except OSError as error:
        raise unwritten_out(out, error) from error


@dc.dataclass(frozen=True)
class ImportRequest:
    """What profile import was given, for the importer --from names."""

    source: str
    table: str | None
    region: str | None
    detected_entities: Path | None


@dc.dataclass(frozen=True)
class SchemaImporter:
    """How profile import reads SOURCE as one kind: whether --table picks one of several tables, and whether it reads AWS."""

    reads: Callable[[ImportRequest], Profile | Bundle]
    several_tables: bool = False
    aws: bool = False


def glue_seed(given: ImportRequest) -> Profile:
    """The seed of the Glue table SOURCE names as DATABASE.TABLE."""
    database, dot, name = given.source.partition('.')
    if not dot or not database or not name:
        raise typer.BadParameter(f'{given.source!r} is not DATABASE.TABLE', param_hint='SOURCE')
    entities = entity_types(given.detected_entities) if given.detected_entities is not None else None
    return Profile.from_glue(database, name, region=given.region, detected_entities=entities)


def data_package_seed(given: ImportRequest) -> Profile | Bundle:
    """The seed of the resource --table names, else a bundle of every resource."""
    if given.table is None:
        return Bundle.from_data_package(given.source)
    return Profile.from_data_package(given.source, table=given.table)


# The input a refused import names, by the SourceError's role.
IMPORT_ROLE_OPTIONS = {'schema': 'SOURCE', 'region': '--region', 'detected-entities': '--detected-entities'}
IMPORTERS = {
    'glue': SchemaImporter(glue_seed, aws=True),
    'odcs': SchemaImporter(lambda given: Profile.from_odcs(given.source, table=given.table), several_tables=True),
    'json-schema': SchemaImporter(lambda given: Profile.from_json_schema(given.source)),
    'table-schema': SchemaImporter(lambda given: Profile.from_table_schema(given.source)),
    'data-package': SchemaImporter(data_package_seed, several_tables=True),
}


@profile_app.command('import', no_args_is_help=True)
def profile_import(
    source: Annotated[
        str, typer.Argument(metavar='SOURCE', help='The schema file, or with --from glue the catalog table as DATABASE.TABLE.')
    ],
    kind: Annotated[str, typer.Option('--from', metavar='KIND', help=f'What SOURCE is: {", ".join(IMPORTERS)}.')],
    table: Annotated[
        str | None,
        typer.Option(
            '--table',
            metavar='NAME',
            help=(
                "Import the one table NAME from a source holding several: an ODCS contract's schema object, or a Data Package's "
                'resource. Without it a Data Package imports as a bundle of every resource and its foreign keys, as '
                'profile create --tables profiles every table.'
            ),
        ),
    ] = None,
    region: Annotated[
        str | None, typer.Option('--region', metavar='REGION', help="With --from glue, the catalog's region, else the AWS environment's.")
    ] = None,
    detected_entities: Annotated[
        Path | None,
        typer.Option(
            '--detected-entities',
            metavar='FILE',
            help=(
                "With --from glue, a JSON object of each column's Glue entity types, as classifyColumns returns them: "
                '{"email": ["EMAIL"]}. Each column takes the type a shipped detector pack declares for the first.'
            ),
        ),
    ] = None,
    out: Annotated[Path | None, typer.Option('--out', metavar='FILE', help='Write the seed to FILE rather than print its YAML.')] = None,
) -> None:
    """Import a seed profile from a schema a project already has: doppeldata profile import --from odcs orders.odcs.yaml

    The seed takes each column's type and constraints from the schema, and a fixed default its YAML states where the schema is silent.

    data generate reads the seed unchanged. glue reads the AWS Glue Data Catalog with the AWS environment's credentials.
    """
    importer = IMPORTERS.get(kind)
    if importer is None:
        raise refused(issue_codes.COMMAND_LINE, f'{kind!r} is not a source to import', '--from', value=kind, valid=list(IMPORTERS))
    if table is not None and not importer.several_tables:
        raise typer.BadParameter(f'a {kind} source holds one table, so --table picks nothing', param_hint='--table')
    for given, flag in ((region, '--region'), (detected_entities, '--detected-entities')):
        if given is not None and not importer.aws:
            raise typer.BadParameter(f'{flag} is read only with --from glue', param_hint=flag)
    writable_out(out)
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always', ProfilingWarning)
            seed = importer.reads(ImportRequest(source, table, region, detected_entities))
    except InvalidProfile as error:
        raise typer.BadParameter(f'the seed it describes fails the profile schema: {error}', param_hint='SOURCE') from error
    except SourceError as error:
        if error.exit_code != 2 or error.role not in IMPORT_ROLE_OPTIONS:
            raise
        raise typer.BadParameter(str(error), param_hint=IMPORT_ROLE_OPTIONS[error.role]) from error
    print_notices(caught)
    if out is None:
        typer.echo(seed.to_yaml(), nl=False)
        return
    try:
        seed.save(out)
    except OSError as error:
        raise unwritten_out(out, error) from error


def entity_types(path: Path) -> dict[str, list[str]]:
    """The --detected-entities file's map of each column to its Glue entity types, refused with exit 2 in any other shape."""
    try:
        found = json.loads(path.read_text())
    except OSError as error:
        raise typer.BadParameter(f'{path} could not be read: {error.strerror or error}', param_hint='--detected-entities') from error
    except json.JSONDecodeError as error:
        raise typer.BadParameter(f'{path} is not JSON: {error}', param_hint='--detected-entities') from error
    if not isinstance(found, dict) or not all(
        isinstance(types, list) and all(isinstance(each, str) for each in types) for types in found.values()
    ):
        message = f'{path} is not a JSON object of each column to a list of entity types, such as {{"email": ["EMAIL"]}}'
        raise typer.BadParameter(message, param_hint='--detected-entities')
    return found


@profile_app.command('export', no_args_is_help=True)
def profile_export(
    profile_name: Annotated[
        str, typer.Argument(metavar='PROFILE', help=f'{PROFILE_HELP} Or a bundle, a file or a directory holding bundle.yaml.')
    ],
    export_format: Annotated[
        str,
        typer.Option(
            '--format',
            metavar='FORMAT',
            help=(
                f'One of {", ".join(EXPORT_FORMATS)}. json-schema writes the record schema, a JSON Schema of one record. '
                "pyspark-schemas writes each table's StructType and a pandera model of it."
            ),
        ),
    ],
    table: Annotated[
        str | None,
        typer.Option(
            '--table',
            metavar='NAME',
            help="Export the one table NAME of a bundle. json-schema takes it, as the record schema describes one table's records.",
        ),
    ] = None,
    out: Annotated[Path | None, typer.Option('--out', metavar='FILE', help='Write the export to FILE rather than print it.')] = None,
) -> None:
    """Export a profile's contract for another tool to validate data with: doppeldata profile export orders.profile.yaml --format dqdl

    pandera, dataframely, Table Schema, the record schema and ODCS's logicalTypeOptions carry a range, listed values, lengths
    or a pattern only where the profile states it of every value, and name what they left out in their own description.
    dqdl, and ODCS's quality checks, carry a share as a floor some standard errors below the profile's, as many as a dqdl
    ruleset's header names.

    A profile made from data that fails profile check is refused, so no export carries a literal the privacy policy did not pass.
    """
    if export_format not in EXPORT_FORMATS:
        message = f'{export_format!r} is not an export format'
        raise refused(issue_codes.UNKNOWN_EXPORT_FORMAT, message, '--format', value=export_format, valid=EXPORT_FORMATS)
    writable_out(out)
    try:
        with refused_as_profile(profile_name):
            text = export(profile_name, export_format, table=table)
    except (OSError, InvalidProfile, SourceError) as error:
        raise typer.BadParameter(str(error), param_hint='PROFILE') from error
    except ValueError as error:
        raise typer.BadParameter(str(error), param_hint='--table') from error
    if out is None:
        typer.echo(text, nl=False)
        return
    try:
        out.write_text(text, encoding='utf-8')
    except OSError as error:
        raise unwritten_out(out, error) from error


@profile_app.command('show', no_args_is_help=True)
def profile_show(name: Annotated[str, typer.Argument(metavar='NAME', help='An example name, as profile list prints it.')]) -> None:
    """Print an example profile's or bundle's YAML, to start one from: doppeldata profile show orders > orders.profile.yaml"""
    if name in bundle_example_names():
        typer.echo(bundle_example_text(name), nl=False)
        return
    if name not in example_names():
        message = f'{name!r} is not an example profile doppeldata ships'
        raise refused(issue_codes.PROFILE_NOT_FOUND, message, 'NAME', value=name, valid=[*example_names(), *bundle_example_names()])
    typer.echo(example_text(name), nl=False)


@profile_app.command('check', no_args_is_help=True)
def profile_check(profile_name: ProfileArgument, as_json: JsonOption = False) -> None:
    """Check a profile against its schema and privacy policy, from the file alone: doppeldata profile check orders.profile.yaml

    It prints GitHub Markdown, a table of each issue at its field with what was found, and exits 3 on any. Every literal
    must be listed in privacy.audit and meet its rule, counted as a rate of the rows the profile describes.
    """
    path = Path(profile_name)
    try:
        if path.is_file():
            report = ProfileFileCheck.of(profile_name, file_checks(path))
        elif profile_name in example_names() or profile_name in bundle_example_names():
            resource = example_resource(profile_name) if profile_name in example_names() else bundle_example_resource(profile_name)
            with importlib.resources.as_file(resource) as example:
                report = ProfileFileCheck.of(profile_name, file_checks(example))
        else:
            message = f'{profile_name!r} is neither a profile file nor an example; name a file, or an example'
            known = [*example_names(), *bundle_example_names()]
            raise refused(issue_codes.PROFILE_NOT_FOUND, message, 'PROFILE', value=profile_name, valid=known)
    except OSError as error:
        message = f'{profile_name} cannot be read: {error.strerror}'
        raise refused(issue_codes.FILE_UNREADABLE, message, 'PROFILE', value=profile_name) from error
    typer.echo(report.to_json() if as_json else report.to_markdown(), nl=False)
    if report.violations:
        raise CheckFailed(report)


@dc.dataclass(frozen=True)
class DataChoices:
    """What data plan and data generate share: PROFILE or the --columns profile, the rows, seed and scenario the flags give,
    --out, what the write lays over the profile, and what the library generates from once a data file fills in the rest."""

    profile: str | Profile
    rows: int | None
    seed: int | None
    out: str
    write: WriteChoices
    scenario: Scenario | None
    generation: GenerationSource

    def arguments(self) -> dict[str, Any]:
        return {'rows': self.rows, 'seed': self.seed, **dc.asdict(self.write), 'scenario': self.scenario}

    def written_values_line(self) -> str | None:
        """The values a data file's write gave in place of a flag, as one line naming the file."""
        written = self.generation.written
        if written is None:
            return None
        taken = [f'seed {self.generation.seed}'] if self.seed is None else []
        if self.rows is None and self.generation.rows is not None:
            taken.append(counted(self.generation.rows, 'row'))
        if self.scenario is None and written.scenario is not None:
            taken.append(f'scenario {written.scenario.name}')
        return f'doppeldata: {", ".join(taken)}, as {written.path} was written' if taken else None


def data_choices(
    profile_name: str | None,
    columns: str | None,
    rows: int | None,
    seed: int | None,
    out: str,
    write: WriteChoices,
    scenario: list[str] | None,
) -> DataChoices:
    """The flags both data verbs take, each refused here with exit 2 when it names nothing doppeldata writes. A Parquet or
    Arrow PROFILE's write gives the seed, rows and scenario the flags leave out, and the library reads it from PROFILE."""
    profile = profile_or_columns(profile_name, columns)
    applied = loaded_scenario(scenario)
    source = profile_name if profile_name is not None else profile
    with refused_as_profile(profile_name):
        generation = generation_source(source, rows, seed, applied)
    return DataChoices(source, rows, seed, out, write, applied, generation)


@contextlib.contextmanager
def refused_as_profile(name: str | None) -> Iterator[None]:
    """Refuse PROFILE with exit 2 where the library refuses the data file it names, or a profile `name` that fails its check
    where its literals would leave it."""
    try:
        yield
    except SourceError as error:
        if error.role != 'profile':
            raise
        raise typer.BadParameter(str(error), param_hint='PROFILE') from error
    except CheckFailed as error:
        hint = f'doppeldata profile check {name} names each violation' if name is not None else None
        refusal = InvalidArgument(error.code, error.message, field='PROFILE', value=name, hint=hint)
        # The profile named cannot be used, so the command ends as a refused argument does, not as a failed check.
        refusal.exit_code = 2
        raise refusal from error


def write_choices(
    file_format: str | None,
    file_rows: int | None,
    partition_by: list[str] | None,
    no_partitions: bool,
    partition_style: str | None,
    writer: str | None,
    embed_profile: bool = False,
) -> WriteChoices:
    """What the write flags lay over the profile, each refused with exit 2 when it names nothing doppeldata writes."""
    if file_format is not None and file_format not in output_formats():
        message = f'{file_format!r} is not a format doppeldata writes'
        raise refused(issue_codes.COMMAND_LINE, message, '--format', value=file_format, valid=output_formats())
    if partition_style is not None and partition_style not in partition_styles():
        message = f'{partition_style!r} is not a partition style doppeldata writes'
        raise refused(issue_codes.COMMAND_LINE, message, '--partition-style', value=partition_style, valid=partition_styles())
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
    return WriteChoices(file_format, file_rows, keys, partition_style, settings, embed_profile)


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
    seed: SeedOption = None,
    file_format: FormatOption = None,
    file_rows: FileRowsOption = None,
    partition_by: PartitionByOption = None,
    no_partitions: NoPartitionsOption = False,
    partition_style: PartitionStyleOption = None,
    writer: WriterOption = None,
    scenario: ScenarioOption = None,
    embed_profile: EmbedProfileOption = False,
    scale: ScaleOption = None,
    as_json: JsonOption = False,
) -> None:
    """Print what data generate would write with the same flags: doppeldata data plan orders --rows 1000000000 --out s3://bucket/orders/

    It reads nothing at --out, and generates the first blocks to estimate the size. The --json shape may change before 1.0.
    A bundle PROFILE prints each table's plan in one line, parents first, and each relationship.
    """
    bundle = bundle_named(profile_name, columns, rows, scale)
    write = write_choices(file_format, file_rows, partition_by, no_partitions, partition_style, writer, embed_profile)
    if bundle is not None:
        planned_bundle = doppeldata.plan(bundle, out, seed=seed, scale=scale, scenario=loaded_scenarios(scenario), **dc.asdict(write))
        typer.echo(json.dumps(planned_bundle.to_json(), indent=2) if as_json else bundle_plan_text(planned_bundle))
        return
    choices = data_choices(profile_name, columns, rows, seed, out, write, scenario)
    with refused_as_profile(profile_name):
        planned = doppeldata.plan(choices.profile, choices.out, **choices.arguments())
    if (written := choices.written_values_line()) is not None:
        typer.echo(written, err=True)
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
    streams = []
    if planned.cdc is not None:
        stream = planned.cdc
        streams.append(
            f'stream     {counted(sum(stream["changes"].values()), "change")} as {stream["style"]} in up to '
            f'{counted(stream["change_files_at_most"], "file")} under {stream["prefix"]}, beside '
            f'{counted(stream["expected_parts"], "part")} under {stream["expected_prefix"]}'
        )
    lines = [
        f'dataset    {planned.dataset_digest}',
        f'identity   {int(identity["dataset_rows"]):,} rows from seed {identity["seed"]}, profile {identity["profile_digest"]}, '
        f'stream version {identity["stream_version"]}',
        f'format     {planned.format} {json.dumps(planned.writer, separators=(",", ":"))}',
        f'files      {files}',
        *streams,
        f'estimate   {size(planned.estimated_bytes)}, from the first blocks encoded with these settings',
        '',
    ]
    if (applied := composed(choices.generation.scenario)) is not None:
        lines.insert(2, f'scenario   {applied.name} {identity["scenario_digest"]}')
    if planned.rows_written != int(identity['dataset_rows']):
        lines.insert(3, f'rows       {planned.rows_written:,} written, the generated rows and the copies duplicates adds')
    width = max(len(column.name) for column in planned.columns)
    type_width = max(len(column.physical_type) for column in planned.columns)
    for column in planned.columns:
        notes = [f'{column.null_fraction:.3g} null'] if column.null_fraction else []
        if column.patched_fields:
            notes.append(f'{counted(len(column.patched_fields), "field")} patched')
        if column.ops:
            notes.append(f'written by {", ".join(column.ops)}')
        lines.append(f'{column.name:<{width}}  {column.physical_type:<{type_width}}  {", ".join([column.summary, *notes])}')
    lines.append('')
    path_width = max((len(planned_range.path) for planned_range in planned.ranges), default=0)
    for planned_range in planned.ranges:
        last_row = planned_range.first_row + planned_range.range_rows - 1
        objects = f', up to {counted(planned_range.objects_at_most, "file")}' if partitioned else ''
        lines.append(f'{planned_range.path:<{path_width}}  rows {planned_range.first_row:,} to {last_row:,}{objects}')
    typer.echo('\n'.join(lines))


def bundle_plan_text(planned: BundleWritePlan) -> str:
    """A bundle's plan: its identity, each table's rows, files and estimate, and each relationship, with what is set aside."""
    identity = planned.identity
    lines = [
        f'dataset    {planned.dataset_digest}',
        f'identity   {int(identity["dataset_rows"]):,} rows at scale {identity["scale"]} from seed {identity["seed"]}, bundle '
        f'{identity["profile_digest"]}, stream version {identity["stream_version"]}',
    ]
    if identity.get('scenario_digest') is not None:
        lines.append(f'scenario   {identity["scenario_digest"]}')
    lines.append('')
    width = max(len(name) for name in planned.tables)
    for name, table in planned.tables.items():
        lines.append(
            f'{name:<{width}}  {int(table.identity["dataset_rows"]):,} rows as {table.format}, up to '
            f'{counted(table.files_at_most, "file")} to {table.destination}, estimate {size(table.estimated_bytes)}'
        )
    lines.append('')
    for relationship in planned.relationships:
        aside = f', set aside, as {relationship["set_aside"]}' if relationship['set_aside'] is not None else ''
        lines.append(f'{relationship["name"]}{aside}')
        lines += [f'  {pointer} set aside: {why}' for pointer, why in relationship['entries_set_aside']]
    return '\n'.join(lines)


@data_app.command('generate', no_args_is_help=True)
def data_generate(
    out: OutOption,
    profile_name: ProfileOrColumnsArgument = None,
    columns: ColumnsOption = None,
    rows: RowsOption = None,
    seed: SeedOption = None,
    file_format: FormatOption = None,
    file_rows: FileRowsOption = None,
    partition_by: PartitionByOption = None,
    no_partitions: NoPartitionsOption = False,
    partition_style: PartitionStyleOption = None,
    writer: WriterOption = None,
    embed_profile: EmbedProfileOption = False,
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
    scale: ScaleOption = None,
) -> None:
    """Write a dataset, as data plan prints it: doppeldata data generate --columns name,email,zip --rows 1000 --out people.csv

    A directory or S3 --out holds the files and their _manifest.json: doppeldata data generate orders --out orders/. A
    destination holding anything is refused unless --replace is given. A write that stops after claiming a directory, on a
    failure, Ctrl-C or SIGTERM, leaves _manifest.partial.json listing the files it completed. A one-file write that stops leaves
    nothing, unless it is killed outright, which leaves a staging directory beside the file for --replace to remove. S3 is
    written with the AWS environment's credentials.

    A Parquet or Arrow file written with --embed-profile regenerates its dataset as PROFILE: doppeldata data generate
    orders/part-00000-fe89a3c3.parquet --out again/. Its seed, rows and scenario are the defaults, and with none of the
    three given, a file whose dataset this release would not make again is refused.

    A bundle PROFILE writes each table under --out/<table>/ with its own manifest, parents first, each foreign key drawn
    from its parent's keys, and a _manifest.json at --out naming each: doppeldata data generate orders-customers --scale
    0.1 --out shop/. Each --scenario bends the table it names.
    """
    bundle = bundle_named(profile_name, columns, rows, scale)
    write = write_choices(file_format, file_rows, partition_by, no_partitions, partition_style, writer, embed_profile)
    if bundle is not None:
        if manifest_beside:
            raise typer.BadParameter('a bundle writes a directory of tables, which always holds its manifests', param_hint='--manifest')
        terminate = signal.signal(signal.SIGTERM, signal.default_int_handler)
        try:
            stored_bundle = doppeldata.write(
                bundle,
                out,
                seed=seed,
                scale=scale,
                scenario=loaded_scenarios(scenario),
                threads=threads,
                replace=replace,
                **dc.asdict(write),
            )
        finally:
            signal.signal(signal.SIGTERM, terminate)
        width = max(len(name) for name in stored_bundle.tables)
        for name, table in stored_bundle.tables.items():
            stored = size(sum(file['bytes'] for file in table.files))
            rows_written = sum(file['rows'] for file in table.files)
            typer.echo(
                f'{name:<{width}}  wrote {counted(rows_written, "row")} as {table.format} in {counted(len(table.files), "file")}, {stored}'
            )
        document = stored_bundle.document
        seconds = max(document['elapsed_ms'], 1) / 1000
        typer.echo(
            f'in {seconds:.2f} s on {counted(document["threads"], "thread")}; '
            f'{beside(document["destination"], MANIFEST_FILE_NAME)} names dataset {stored_bundle.dataset_digest}'
        )
        return
    choices = data_choices(profile_name, columns, rows, seed, out, write, scenario)
    one_file = one_file_of(choices.out)
    if (written := choices.written_values_line()) is not None:
        typer.echo(written, err=True)
    terminate = signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        with refused_as_profile(profile_name):
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
    if manifest.cdc is not None:
        stream = manifest.cdc
        typer.echo(
            f'and {counted(sum(stream["changes"].values()), "change")} as {stream["style"]} in '
            f'{counted(len(stream["files"]), "file")}, {size(sum(file["bytes"] for file in stream["files"]))}, under '
            f'{stream["prefix"]}, beside {stream["expected_prefix"]}, {size(sum(file["bytes"] for file in stream["expected"]))}, '
            'the table a merge in commit order reaches'
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
        issue = unknown_name(issue_codes.UNKNOWN_OP, name, known, (), 'an op or an example scenario')
        raise InvalidArgument(issue.code, issue.message, field='NAME', value=name, valid=issue.valid, closest=issue.closest)
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
    seed: SeedOption = None,
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
        with refused_as_profile(profile_name):
            prepared = prepared_write(profile_name, 'memory://', rows, seed, write, scenario)
        prepared.engine.ranges_json(prepared.options)
    except InvalidScenario as error:
        echo_scenario_check(error.source, source.document['name'], as_json, issues=error.issues)
        raise typer.Exit(3) from error
    plain = scenario.to_plain()
    held = f'{counted(len(plain.get("perturbations") or []), "perturbation")} and {counted(len(plain.get("expect") or []), "expectation")}'
    echo_scenario_check(scenario.source, source.document['name'], as_json, held=f'{held}, scenario {scenario.digest}')


def echo_scenario_check(scenario: str, profile: str, as_json: bool, *, issues: Sequence[Issue] = (), held: str = '') -> None:
    """Print scenario check's report: JSON, or GitHub Markdown naming each issue at its JSON Pointer."""
    if as_json:
        plain = {'passed': not issues, 'scenario': scenario, 'profile': profile, 'issues': [issue.to_json() for issue in issues]}
        typer.echo(json.dumps(plain, indent=2, ensure_ascii=False))
        return
    if not issues:
        typer.echo(f'# doppeldata scenario check passed\n\n{scenario} holds for profile `{profile}`: {held}.')
        return
    lines = ['# doppeldata scenario check failed', '', f'{scenario} does not hold for profile `{profile}`.', '', *issue_table(issues)]
    typer.echo('\n'.join(lines).rstrip('\n'))


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
        raise refused(issue_codes.COMMAND_LINE, f'{name!r} is not a schema doppeldata ships', 'NAME', value=name, valid=schema_names())
    typer.echo(schema_text(name), nl=False)


@capabilities_app.command('show')
def capabilities_show(ctx: typer.Context, as_json: JsonOption = False) -> None:
    """List every command, format, op, fake, semantic type, policy and issue code this install takes.

    It is read from the installed doppeldata, so it matches this version exactly. --json adds each command's flags, each
    op's fields and each issue code's exit status and hint.
    """
    held = capabilities(command_surface(ctx))
    if as_json:
        typer.echo(json.dumps(held, indent=2))
        return
    width = max(len(' '.join(command['path'])) for command in held['commands'])
    for command in held['commands']:
        typer.echo(f'doppeldata {" ".join(command["path"]):<{width}}  {command["summary"] or ""}')

    def only_in(row: dict[str, Any], key: str) -> str:
        return row[key] if len(row['formats']) == len(held['formats']) else f'{row[key]} ({", ".join(row["formats"])})'

    sections = {
        'formats': held['formats'],
        'export formats': held['export_formats'],
        'engines': [row['engine'] for row in held['engines']],
        'policies': [*held['policies'], f'k_min {held["default_k_min"]} unless given'],
        'families': [row['family'] for row in held['families']],
        'fakes': held['fakes'],
        'semantic types': [f'{row["type"]} ({row["pack"]})' for row in held['semantic_types']],
        'ops': [only_in(row, 'op') for row in held['ops']],
        'fault kinds': [only_in(row, 'kind') for row in held['fault_kinds']],
        'schemas': held['schemas'],
    }
    label = max(map(len, sections)) + 2
    for name, values in sections.items():
        lines = [f'{name:<{label}}']
        for at, value in enumerate(values):
            item = value + (',' if at < len(values) - 1 else '')
            if len(lines[-1]) > label and len(lines[-1]) + 1 + len(item) > LISTED_WIDTH:
                lines.append(' ' * label)
            lines[-1] += item if len(lines[-1]) == label else f' {item}'
        typer.echo('\n'.join(lines))
    typer.echo(
        f'{counted(len(held["issue_codes"]), "issue code")}, each with its exit status and hint: doppeldata capabilities show --json'
    )


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
