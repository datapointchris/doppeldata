import contextlib
import dataclasses as dc
import importlib.metadata
import itertools
import json
import logging
import math
import signal
import sys
import warnings
from collections.abc import Iterator
from collections.abc import Sequence
from pathlib import Path
from typing import Annotated
from typing import Any
from typing import TypeVar

import doppeldata
import typer
from doppeldata import Bundle
from doppeldata import BundleManifest
from doppeldata import BundleWritePlan
from doppeldata import CheckFailed
from doppeldata import InvalidCodeList
from doppeldata import Issue
from doppeldata import IssueCarrier
from doppeldata import Manifest
from doppeldata import Profile
from doppeldata import ProfilesDrifted
from doppeldata import ProfilingWarning
from doppeldata import Scenario
from doppeldata import StoppedWrite
from doppeldata import WritePlan
from doppeldata import _core
from doppeldata import capabilities
from doppeldata import check
from doppeldata import diff
from doppeldata import issue_codes
from doppeldata import profile
from doppeldata import profile_tables
from doppeldata.dataset_generation import one_file_of
from doppeldata.example_profiles import shipped_example
from doppeldata.example_profiles import shipped_examples
from doppeldata.manifest import MANIFEST_FILE_NAME
from doppeldata.manifest import PARTIAL_MANIFEST_FILE_NAME
from doppeldata.privacy_policy import APPLIED_POLICIES
from doppeldata.privacy_policy import DEFAULT_K_MIN
from doppeldata.privacy_policy.file_check import profile_file_check
from doppeldata.profile_comparison.metrics import METRICS
from doppeldata.profile_comparison.real_data_scores import DEFAULT_SAMPLE_ROWS
from doppeldata.profile_comparison.real_data_scores import SCORES
from doppeldata.profile_comparison.relationship_metrics import RELATIONSHIP_METRICS
from doppeldata.profile_exports import EXPORT_FORMATS
from doppeldata.profile_exports import export
from doppeldata.profile_sources import profile_or_columns
from doppeldata.profiling import ENGINES
from doppeldata.published_schemas import output_formats
from doppeldata.published_schemas import schema_summaries
from doppeldata.published_schemas import schema_text
from doppeldata.scenario_checks import check_scenario
from doppeldata.scenario_reference import OpReference
from doppeldata.scenario_reference import scenario_listing
from doppeldata.scenario_reference import scenario_reference
from doppeldata.seed_profiles.importers import IMPORT_KINDS
from doppeldata.seed_profiles.importers import import_seed
from typer._click.exceptions import NoArgsIsHelpError
from typer._click.exceptions import NoSuchOption
from typer._click.exceptions import UsageError
from typer.core import TyperGroup

from doppeldata_cli.command_surface import command_surface
from doppeldata_cli.command_surface import flag_names

log = logging.getLogger('doppeldata')

# The command line's name for each argument a library refusal names, so the refusal points at what was typed.
FLAG_OF = {
    'a': 'A',
    'against': '--against',
    'against_sample': '--against-sample',
    'aws_profile': '--aws-profile',
    'b': 'B',
    'columns': '--columns',
    'data': 'DATA',
    'detected-entities': '--detected-entities',
    'detected_entities': '--detected-entities',
    'edges_from': '--edges-from',
    'embed_profile': '--embed-profile',
    'engine': '--engine',
    'file_rows': '--file-rows',
    'format': '--format',
    'html': '--html',
    'k_min': '--k-min',
    'kind': '--from',
    'manifest': '--manifest',
    'name': 'NAME',
    'packs': '--pack',
    'partition_by': '--partition-by',
    'partition_style': '--partition-style',
    'policy': '--policy',
    'profile': 'PROFILE',
    'region': '--region',
    'rows': '--rows',
    'sample': '--sample',
    'scale': '--scale',
    'scenario': '--scenario',
    'schema': 'SOURCE',
    'seed': '--seed',
    'source': 'SOURCE',
    'table': '--table',
    'threads': '--threads',
    'thresholds': '--thresholds',
    'writer': '--writer',
}


def examples(*rows: tuple[str, str]) -> str:
    """A help screen's examples block: each command beside what it is for.

    Typer folds an epilog's single line breaks into spaces and keeps a blank line as one break, so each row is a list
    item of its own paragraph, which prints as a line of its own.
    """
    return '\n\n'.join(['Examples:', *(f'- {command} — {purpose}' for command, purpose in rows)])


def beside(destination: str, name: str) -> str:
    """The object `name` at the top of `destination`: orders/_manifest.json, memory://_manifest.json."""
    return destination + name if destination.endswith('/') else f'{destination}/{name}'


Refusal = TypeVar('Refusal', bound=IssueCarrier)


def at_flags(error: Refusal, fields: dict[str, str]) -> Refusal:
    """`error` with each issue whose field `fields` maps named as the command line names it."""
    if any(issue.field in fields for issue in error.issues):
        error.issues = tuple(
            dc.replace(issue, field=fields[issue.field or '']) if issue.field in fields else issue for issue in error.issues
        )
    return error


@contextlib.contextmanager
def refused_at(fields: dict[str, str]) -> Iterator[None]:
    """Name each issue of a refusal inside the block as `fields` maps it, before the root names the rest by FLAG_OF. A
    command whose argument shares a library name with another command's flag says so here: SCENARIO, not --scenario."""
    try:
        yield
    except IssueCarrier as error:
        at_flags(error, fields)
        raise


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


@contextlib.contextmanager
def stderr_logging() -> Iterator[None]:
    """Write the `doppeldata` logger's records to stderr for one invocation, warnings and worse unless a command's -v or
    -q says otherwise."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter('doppeldata: %(message)s'))
    level = log.level
    log.addHandler(handler)
    log.setLevel(logging.WARNING)
    try:
        yield
    finally:
        log.removeHandler(handler)
        log.setLevel(level)


def logged_at(verbose: int, quiet: bool) -> None:
    """Set what stderr carries beside a refusal: warnings by default, each step at -v, each file and request at -vv, and
    only failures at -q. stdout is the same either way."""
    if verbose and quiet:
        raise typer.BadParameter('-q prints only failures and -v prints more, so give one of them', param_hint='--quiet')
    log.setLevel(logging.ERROR if quiet else {0: logging.WARNING, 1: logging.INFO}.get(verbose, logging.DEBUG))


def print_notices(caught: list[warnings.WarningMessage]) -> None:
    """Log each profiling warning as a warning of the `doppeldata` logger, so -q drops it, and show any other as Python does."""
    for notice in caught:
        if issubclass(notice.category, ProfilingWarning):
            log.warning('%s', notice.message)
        else:
            warnings.showwarning(notice.message, notice.category, notice.filename, notice.lineno)


# Set in the root context's meta, which every context of one invocation shares, before any parameter is parsed.
JSON_REQUESTED = 'doppeldata.json'


class DoppeldataGroup(TyperGroup):
    """The root group. A refusal prints on stderr with its code, as one line of JSON per issue where the command line asks
    for JSON, and the command exits with the code's status. Each issue naming a library argument names its flag.

    Under JSON, a command line the parser refuses prints as E-USAGE-001 and exits 2. Without it, the parser's own text
    prints. The `doppeldata` logger and every profiling warning go to stderr.
    """

    def parse_args(self, ctx: Any, args: list[str]) -> list[str]:
        ctx.meta[JSON_REQUESTED] = '--json' in itertools.takewhile(lambda word: word != '--', args)
        return super().parse_args(ctx, args)

    def invoke(self, ctx: Any) -> Any:
        as_json = bool(ctx.meta.get(JSON_REQUESTED))
        with stderr_logging():
            try:
                return self.noticed_invoke(ctx)
            except StoppedWrite as error:
                report_stopped_write(at_flags(error, FLAG_OF), as_json=as_json)
                raise typer.Exit(error.exit_code) from error
            except IssueCarrier as error:
                report_refusal(at_flags(error, FLAG_OF), as_json=as_json)
                raise typer.Exit(error.exit_code) from error
            except UsageError as error:
                if isinstance(error, NoArgsIsHelpError) or not as_json:
                    raise
                refused = command_line_refusal(error)
                report_refusal(refused, as_json=as_json)
                raise typer.Exit(refused.exit_code) from error

    def noticed_invoke(self, ctx: Any) -> Any:
        caught: list[warnings.WarningMessage] = []
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always', ProfilingWarning)
                return super().invoke(ctx)
        finally:
            print_notices(caught)


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
        return flag_names(parameter)[0]
    return parameter.human_readable_name


def command_line_refusal(error: UsageError) -> IssueCarrier:
    """The parser's refusal as E-USAGE-001, naming the parameter and the values it takes where the parser knows them."""
    context = error.ctx
    field: str | None = None
    value: str | None = None
    message = error.format_message()
    valid: Sequence[str] = ()
    closest: Sequence[str] = ()
    if isinstance(error, typer.BadParameter):
        field = parameter_named(error)
        valid = list(getattr(error.param.type, 'choices', None) or ()) if error.param is not None else []
    elif isinstance(error, NoSuchOption):
        field = value = error.option_name
        message = f'no such option: {error.option_name}'
        closest = list(error.possibilities or ())
        if context is not None:
            valid = sorted(name for parameter in context.command.params for name in parameter.opts if name.startswith('--'))
    elif context is not None and (list_commands := getattr(context.command, 'list_commands', None)) is not None:
        field = 'command'
        valid = list_commands(context)
    hint = None if context is None else f'{context.command_path} --help'
    issue = Issue(issue_codes.COMMAND_LINE, field, message, value, tuple(valid), tuple(closest), hint)
    return IssueCarrier([issue])


ROOT_EXAMPLES = examples(
    ('doppeldata profile list', 'the example profiles to generate from'),
    ('doppeldata data generate orders --rows 1000 --out orders/', 'write an example dataset with its manifest'),
    ('doppeldata profile create orders/ --out orders.profile.yaml', 'profile a dataset you hold'),
    ('doppeldata data check orders.profile.yaml orders/', 'check generated data against its profile'),
    ('doppeldata capabilities show --json', 'everything this install takes, for a program or an agent'),
)
PROFILE_EXAMPLES = examples(
    ('doppeldata profile create orders/ --out orders.profile.yaml', 'profile files you hold'),
    ('doppeldata profile check orders.profile.yaml', 'check it holds no value its privacy policy refuses'),
    ('doppeldata profile show orders > orders.profile.yaml', 'start from an example instead'),
    ('doppeldata profile diff orders.profile.yaml april.profile.yaml', 'how far a later profile drifted'),
)
DATA_EXAMPLES = examples(
    ('doppeldata data plan orders --rows 1000000 --out orders/', 'what a write would make, before it runs'),
    ('doppeldata data generate orders --rows 1000 --out orders/', 'write it'),
    ('doppeldata data check orders orders/', 'check what was written against its profile'),
)
SCENARIO_EXAMPLES = examples(
    ('doppeldata scenario list', 'each op, the metrics an expectation names, and each example'),
    ('doppeldata scenario show hot_keys', "an op's parameters"),
    ('doppeldata scenario check hot-key orders', 'check a scenario holds for a profile, without generating'),
)
SCHEMAS_EXAMPLES = examples(
    ('doppeldata schemas list', 'each schema by name'),
    ('doppeldata schemas show profile > profile.schema.json', 'a schema for an editor to validate profiles with'),
)
CAPABILITIES_EXAMPLES = examples(
    ('doppeldata capabilities show', 'everything this install takes, for a person'),
    ('doppeldata capabilities show --json', 'each list whole, for a program or an agent'),
)

app = typer.Typer(name='doppeldata', no_args_is_help=True, cls=DoppeldataGroup)
profile_app = typer.Typer(
    no_args_is_help=True, help='Profiles: reviewable files that describe a dataset without holding its rows.', epilog=PROFILE_EXAMPLES
)
app.add_typer(profile_app, name='profile', rich_help_panel='Profile and generate')
data_app = typer.Typer(
    no_args_is_help=True,
    help='Synthetic datasets generated from a profile, written as files with a manifest, and checked against it.',
    epilog=DATA_EXAMPLES,
)
app.add_typer(data_app, name='data', rich_help_panel='Profile and generate')
scenario_app = typer.Typer(
    no_args_is_help=True, help='Scenarios: layers over a profile that bend the generated data on purpose.', epilog=SCENARIO_EXAMPLES
)
app.add_typer(scenario_app, name='scenario', rich_help_panel='Profile and generate')
schemas_app = typer.Typer(no_args_is_help=True, help='The JSON Schemas of the files doppeldata reads and writes.', epilog=SCHEMAS_EXAMPLES)
app.add_typer(schemas_app, name='schemas', rich_help_panel='Look up what doppeldata takes')
capabilities_app = typer.Typer(
    no_args_is_help=True,
    help='What a program or an agent can name: everything this install takes, read from the registries that refuse a value outside them.',
    epilog=CAPABILITIES_EXAMPLES,
)
app.add_typer(capabilities_app, name='capabilities', rich_help_panel='Look up what doppeldata takes')

MAKE_PANEL = 'Make a profile'
READ_PANEL = 'Read a profile'
HAND_OFF_PANEL = 'Hand a profile to another tool'

JsonOption = Annotated[bool, typer.Option('--json', help='Print JSON instead of text, and each refusal as a line of JSON.')]
ProfileJsonOption = Annotated[
    bool, typer.Option('--json', help='Print each refusal as a line of JSON, and without --out the profile as JSON instead of YAML.')
]
ManifestJsonOption = Annotated[
    bool, typer.Option('--json', help="Print each refusal as a line of JSON, and the manifest's JSON instead of a summary.")
]
ExportJsonOption = Annotated[
    bool, typer.Option('--json', help='Print each refusal as a line of JSON. The export prints in its own format.')
]
VerboseOption = Annotated[
    int, typer.Option('--verbose', '-v', count=True, help='Print each step on stderr, and with -vv each file and request.')
]
QuietOption = Annotated[bool, typer.Option('--quiet', '-q', help='Print nothing on stderr but a refusal or a failure.')]
AwsProfileOption = Annotated[
    str | None,
    typer.Option(
        '--aws-profile',
        metavar='NAME',
        help='Reach AWS with the credentials and region of the profile NAME in the AWS config. Defaults to AWS_PROFILE, '
        "then boto3's default chain. A NAME the config lacks is refused before anything is read or written.",
    ),
]
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


def print_json(value: Any) -> None:
    """`value` on stdout as indented JSON."""
    typer.echo(json.dumps(value, indent=2, ensure_ascii=False))


def counted(count: int, noun: str) -> str:
    return f'{count:,} {noun}' + ('' if count == 1 else 's')


def size(count: int) -> str:
    """`count` bytes in decimal units: 950 B, 41.3 MB."""
    if count < 1000:
        return f'{count} B'
    units = ('KB', 'MB', 'GB', 'TB')
    power = min(int(math.log10(count)) // 3, len(units))
    return f'{count / 1000**power:.1f} {units[power - 1]}'


def profile_text(found: Profile | Bundle, *, as_json: bool) -> str:
    """A profile or bundle as the file `save` writes for a `.json` path where `as_json`, else for a YAML one."""
    return json.dumps(found.to_plain(), indent=2, allow_nan=False) + '\n' if as_json else found.to_yaml()


def writable_out(out: Path | None) -> None:
    """Refuse --out before anything is read where the directory it names does not exist."""
    if out is not None and not out.parent.is_dir():
        message = f'{out.parent} is not a directory; create it, or name a FILE in one that exists'
        raise doppeldata.InvalidArgument(issue_codes.OUT_UNWRITABLE, message, field='--out', value=str(out))


def saved(found: Profile | Bundle, out: Path | None, *, as_json: bool) -> None:
    """Print `found`, or save it to `out`, refusing an `out` the write to fails."""
    if out is None:
        typer.echo(profile_text(found, as_json=as_json), nl=False)
        return
    try:
        found.save(out)
    except OSError as error:
        message = f'{out} could not be written: {error.strerror or error}'
        raise doppeldata.InvalidArgument(issue_codes.OUT_UNWRITABLE, message, field='--out', value=str(out)) from error


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


def column_names(columns: str | None) -> list[str] | None:
    """--columns split at its commas, or None where it is not given."""
    return None if columns is None else [name.strip() for name in columns.split(',') if name.strip()]


def scenarios_named(names: list[str] | None) -> list[Scenario] | None:
    """The scenarios --scenario names, in order, or None where it names none."""
    return [Scenario.named(name) for name in names] if names else None


def write_settings(writer: str | None) -> dict[str, Any] | None:
    """--writer's JSON object, refused with exit 2 where it is not one."""
    if writer is None:
        return None
    try:
        settings = json.loads(writer)
    except json.JSONDecodeError as error:
        raise typer.BadParameter(f'{writer!r} is not JSON: {error}', param_hint='--writer') from error
    if not isinstance(settings, dict):
        raise typer.BadParameter(f'{writer!r} is not a JSON object', param_hint='--writer')
    return settings


def partition_keys(partition_by: list[str] | None, no_partitions: bool) -> list[str] | None:
    """The partition keys the flags give: none under --no-partitions, else --partition-by's, else the profile's."""
    if no_partitions and partition_by:
        raise typer.BadParameter('--no-partitions writes no partitions, so it takes no --partition-by', param_hint='--no-partitions')
    return [] if no_partitions else partition_by or None


@profile_app.command(
    'list',
    rich_help_panel=READ_PANEL,
    epilog=examples(('doppeldata profile list', 'each example by name'), ('doppeldata profile list --json', 'the same, for a program')),
)
def profile_list(as_json: JsonOption = False) -> None:
    """List the hand-written example profiles and bundles doppeldata ships, by the name every PROFILE argument takes."""
    listed = shipped_examples()
    if as_json:
        print_json(listed)
        return
    width = max((len(row['name']) for row in listed), default=0)
    for row in listed:
        if row['kind'] == 'bundle':
            held = f'bundle of {counted(row["tables"], "table")} and {counted(row["relationships"], "relationship")}'
        else:
            held = f'{row["columns"]} columns'
        typer.echo(f'{row["name"]:<{width}}  {held}, {row["rows"]:,} rows')


@profile_app.command(
    'create',
    no_args_is_help=True,
    rich_help_panel=MAKE_PANEL,
    epilog=examples(
        ('doppeldata profile create orders/ --out orders.profile.yaml', 'profile a directory of files as one table'),
        ('doppeldata profile create shop/ --tables --out shop.bundle.yaml', 'profile related tables into a bundle with their keys'),
        (
            'doppeldata profile create orders/ --edges-from orders.profile.yaml --out april.profile.yaml',
            "profile again on an earlier profile's edges, for profile diff",
        ),
        (
            'doppeldata profile create s3://bucket/orders/ --aws-profile analytics --out orders.profile.yaml',
            'read S3 with a named AWS profile',
        ),
    ),
)
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
    aws_profile: AwsProfileOption = None,
    verbose: VerboseOption = 0,
    quiet: QuietOption = False,
    as_json: ProfileJsonOption = False,
) -> None:
    """Profile a dataset into a file holding no rows, from local files or S3.

    Each file is read as its suffix's format. polars reads text formats from local files only. A directory or prefix whose
    folders hold unlike column names is refused, unless --tables writes a bundle of its tables.
    """
    logged_at(verbose, quiet)
    if tables and edges_from is not None:
        raise typer.BadParameter("--edges-from is one table's profile, so it cannot be given with --tables", param_hint='--edges-from')
    writable_out(out)
    named_lists = named_files(code_lists or [], '--code-list')
    settings: dict[str, Any] = {'engine': engine, 'sample': sample, 'policy': policy, 'k_min': k_min, 'packs': packs or ()}
    try:
        profiled: Profile | Bundle = (
            profile_tables(data, **settings, code_lists=named_lists, aws_profile=aws_profile)
            if tables
            else profile(data, **settings, edges_from=edges_from, code_lists=named_lists, aws_profile=aws_profile)
        )
    except InvalidCodeList as error:
        error.issues = tuple(dc.replace(issue, field='--code-list') for issue in error.issues)
        raise
    saved(profiled, out, as_json=as_json)


@profile_app.command(
    'diff',
    no_args_is_help=True,
    rich_help_panel=READ_PANEL,
    epilog=examples(
        ('doppeldata profile diff orders.profile.yaml april.profile.yaml', 'report how far April drifted from the profile before it'),
        ('doppeldata profile diff orders.profile.yaml april.profile.yaml --json', 'the same, for a program'),
    )
    + '\n\n'
    + METRIC_DEFINITIONS,
)
def profile_diff(
    a: Annotated[str, typer.Argument(metavar='A', help='The earlier profile, a file or an example name.')],
    b: Annotated[str, typer.Argument(metavar='B', help='The later profile, made with --edges-from A to measure overflow.')],
    thresholds: ThresholdsOption = None,
    verbose: VerboseOption = 0,
    quiet: QuietOption = False,
    as_json: JsonOption = False,
) -> None:
    """Report how far B drifted from A, two profiles of one table.

    Exits 0 when the profiles agree, 1 when a metric misses its threshold or a field changed, and 3 when the diff could not finish.
    """
    logged_at(verbose, quiet)
    report = diff(a, b, thresholds=thresholds)
    typer.echo(report.to_json() if as_json else report.to_markdown(), nl=False)
    if report.drifted:
        raise ProfilesDrifted(report)


@data_app.command(
    'check',
    no_args_is_help=True,
    epilog=examples(
        ('doppeldata data check orders orders/', 'check generated data against the profile it came from'),
        ('doppeldata data check orders.profile.yaml orders/ --against real/orders/', 'also score it against the real data'),
        ('doppeldata data check orders-customers shop/', "check a bundle's tables and relationships"),
    )
    + f'\n\n{METRIC_DEFINITIONS}\n\n{RELATIONSHIP_DEFINITIONS}\n\n{SCORE_DEFINITIONS}',
)
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
    aws_profile: AwsProfileOption = None,
    verbose: VerboseOption = 0,
    quiet: QuietOption = False,
    as_json: JsonOption = False,
) -> None:
    """Check generated data against its profile.

    DATA is profiled with PROFILE's histogram edges and value lists, and each metric compares the two profiles.
    With --scenario, DATA is measured against the profile the scenario's patch makes, each metric a perturbation moves is
    skipped, naming it, and each expectation the scenario declares is measured.
    Only --against reads the real data. Exits 0 when every metric, column and expectation passes, and 3 when one does not
    or the check could not finish.

    A bundle PROFILE checks each table under DATA/<table>/ against its own profile, then measures each relationship by the
    relationship metrics below. It takes no --against.
    """
    logged_at(verbose, quiet)
    report = check(
        profile_name,
        data,
        thresholds=thresholds,
        scenario=scenarios_named(scenario),
        against=against,
        against_sample=against_sample,
        html=html,
        aws_profile=aws_profile,
    )
    typer.echo(report.to_json() if as_json else report.to_markdown(), nl=False)
    if not (report.passed and report.verified):
        raise CheckFailed(report)


@profile_app.command(
    'expand',
    no_args_is_help=True,
    rich_help_panel=MAKE_PANEL,
    epilog=examples(
        ('doppeldata profile expand --columns name,email,zip --rows 1000', 'the full profile three column names stand for'),
        ('doppeldata profile expand people.yaml --out people.profile.yaml', "a short YAML's full profile, to edit"),
    ),
)
def profile_expand(
    profile_name: ProfileOrColumnsArgument = None,
    columns: ColumnsOption = None,
    rows: Annotated[
        int | None, typer.Option('--rows', metavar='N', min=0, help='Record N as the row count, which data generate defaults to.')
    ] = None,
    out: Annotated[Path | None, typer.Option('--out', metavar='FILE', help='Write the profile to FILE rather than print its YAML.')] = None,
    as_json: ProfileJsonOption = False,
) -> None:
    """Print the full profile a short YAML or --columns stands for.

    data generate reads the full profile unchanged, and it is the file to edit once a column needs more than a fake or a range.
    """
    writable_out(out)
    saved(Profile.expand(profile_or_columns(profile_name, column_names(columns)), rows=rows), out, as_json=as_json)


@profile_app.command(
    'import',
    no_args_is_help=True,
    rich_help_panel=MAKE_PANEL,
    epilog=examples(
        ('doppeldata profile import --from odcs orders.odcs.yaml', 'a seed from an ODCS data contract'),
        ('doppeldata profile import --from data-package datapackage.json', 'a bundle from a Data Package and its foreign keys'),
        ('doppeldata profile import --from glue sales.orders --aws-profile analytics', 'a seed from an AWS Glue catalog table'),
    ),
)
def profile_import(
    source: Annotated[
        str, typer.Argument(metavar='SOURCE', help='The schema file, or with --from glue the catalog table as DATABASE.TABLE.')
    ],
    kind: Annotated[str, typer.Option('--from', metavar='KIND', help=f'What SOURCE is: {", ".join(IMPORT_KINDS)}.')],
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
        str | None, typer.Option('--region', metavar='REGION', help="With --from glue, the catalog's region, else the AWS profile's.")
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
    aws_profile: AwsProfileOption = None,
    out: Annotated[Path | None, typer.Option('--out', metavar='FILE', help='Write the seed to FILE rather than print its YAML.')] = None,
    verbose: VerboseOption = 0,
    quiet: QuietOption = False,
    as_json: ProfileJsonOption = False,
) -> None:
    """Import a seed profile from a schema a project already has.

    The seed takes each column's type and constraints from the schema, and a fixed default its YAML states where the schema is silent.

    data generate reads the seed unchanged. glue reads the AWS Glue Data Catalog, and takes --aws-profile and --region.
    """
    logged_at(verbose, quiet)
    writable_out(out)
    seed = import_seed(kind, source, table=table, region=region, detected_entities=detected_entities, aws_profile=aws_profile)
    saved(seed, out, as_json=as_json)


@profile_app.command(
    'export',
    no_args_is_help=True,
    rich_help_panel=HAND_OFF_PANEL,
    epilog=examples(
        ('doppeldata profile export orders.profile.yaml --format dqdl', 'an AWS Glue Data Quality ruleset to validate data with'),
        ('doppeldata profile export orders --format odcs --out orders.odcs.yaml', 'an ODCS data contract'),
        ('doppeldata profile export orders-customers --format json-schema --table orders', "one bundle table's record schema"),
    ),
)
def profile_export(
    profile_name: ProfileArgument,
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
    as_json: ExportJsonOption = False,
) -> None:
    """Export a profile's contract for another tool to validate data with.

    pandera, dataframely, Table Schema, the record schema and ODCS's logicalTypeOptions carry a range, listed values, lengths
    or a pattern only where the profile states it of every value, and name what they left out in their own description.
    dqdl, and ODCS's quality checks, carry a share as a floor some standard errors below the profile's, as many as a dqdl
    ruleset's header names.

    A profile made from data that fails profile check is refused, so no export carries a literal the privacy policy did not pass.
    """
    writable_out(out)
    text = export(profile_name, export_format, table=table)
    if out is None:
        typer.echo(text, nl=False)
        return
    try:
        out.write_text(text, encoding='utf-8')
    except OSError as error:
        message = f'{out} could not be written: {error.strerror or error}'
        raise doppeldata.InvalidArgument(issue_codes.OUT_UNWRITABLE, message, field='--out', value=str(out)) from error


@profile_app.command(
    'show',
    no_args_is_help=True,
    rich_help_panel=READ_PANEL,
    epilog=examples(
        ('doppeldata profile show orders > orders.profile.yaml', 'copy an example to edit'),
        ('doppeldata profile show orders --json', 'the same profile as JSON'),
    ),
)
def profile_show(
    name: Annotated[str, typer.Argument(metavar='NAME', help='An example name, as profile list prints it.')],
    as_json: Annotated[
        bool, typer.Option('--json', help="Print the example's document as JSON instead of its YAML, and each refusal as a line of JSON.")
    ] = False,
) -> None:
    """Print an example profile's or bundle's YAML, comments included, to start one from."""
    shipped = shipped_example(name)
    if as_json:
        print_json(shipped.document)
        return
    typer.echo(shipped.text, nl=False)


@profile_app.command(
    'check',
    no_args_is_help=True,
    rich_help_panel=READ_PANEL,
    epilog=examples(
        ('doppeldata profile check orders.profile.yaml', 'check a profile file before sharing it'),
        ('doppeldata profile check orders --json', 'check an example, for a program'),
    ),
)
def profile_check(profile_name: ProfileArgument, as_json: JsonOption = False) -> None:
    """Check a profile against its schema and privacy policy, from the file alone.

    It prints GitHub Markdown, a table of each issue at its field with what was found, and exits 3 on any. Every literal
    must be listed in privacy.audit and meet its rule, counted as a rate of the rows the profile describes.
    """
    report = profile_file_check(profile_name)
    typer.echo(report.to_json() if as_json else report.to_markdown(), nl=False)
    if report.violations:
        raise CheckFailed(report)


@data_app.command(
    'plan',
    no_args_is_help=True,
    epilog=examples(
        ('doppeldata data plan orders --rows 1000000 --out orders/', 'the files, identity and size a write would make'),
        ('doppeldata data plan orders --out s3://bucket/orders/ --aws-profile analytics', 'the same for S3, reading nothing there'),
        ('doppeldata data plan orders-customers --scale 0.1 --out shop/', "each bundle table's plan, parents first"),
    ),
)
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
    aws_profile: AwsProfileOption = None,
    verbose: VerboseOption = 0,
    quiet: QuietOption = False,
    as_json: JsonOption = False,
) -> None:
    """Print what data generate would write with the same flags.

    It reads nothing at --out, and generates the first blocks to estimate the size. The --json shape may change before 1.0.
    A bundle PROFILE prints each table's plan in one line, parents first, and each relationship.
    """
    logged_at(verbose, quiet)
    planned = doppeldata.plan(
        profile_or_columns(profile_name, column_names(columns)),
        out,
        rows=rows,
        seed=seed,
        scale=scale,
        format=file_format,
        file_rows=file_rows,
        partition_by=partition_keys(partition_by, no_partitions),
        partition_style=partition_style,
        writer=write_settings(writer),
        scenario=scenarios_named(scenario),
        embed_profile=embed_profile,
        aws_profile=aws_profile,
    )
    if as_json:
        print_json(planned.to_json())
        return
    typer.echo(bundle_plan_text(planned) if isinstance(planned, BundleWritePlan) else plan_text(planned))


def plan_text(planned: WritePlan) -> str:
    """One table's plan: its identity, format, files and estimate, each column's sampler, and each range."""
    identity = planned.identity
    partitioned = planned.layout['partition_by']
    ranges = counted(len(planned.ranges), 'range')
    if partitioned:
        directories = f'one {planned.layout["partition_style"]}-style directory per value of {", ".join(partitioned)}'
        files = f'up to {counted(planned.files_at_most, "file")} from {ranges} to {planned.destination}, under {directories}'
    elif planned.one_file is not None:
        files = f'1 file, {planned.one_file}'
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
    if planned.scenario is not None:
        lines.insert(2, f'scenario   {planned.scenario} {identity["scenario_digest"]}')
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
    return '\n'.join(lines)


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


@data_app.command(
    'generate',
    no_args_is_help=True,
    epilog=examples(
        ('doppeldata data generate orders --rows 1000 --out orders/', "write an example's dataset with its manifest"),
        ('doppeldata data generate --columns name,email,zip --rows 100 --out people.csv', 'one CSV from column names alone'),
        ('doppeldata data generate orders --out s3://bucket/orders/ --aws-profile analytics', 'write to S3 with a named AWS profile'),
        ('doppeldata data generate orders-customers --scale 0.1 --out shop/', 'related tables, each foreign key drawn from its parent'),
        ('doppeldata data generate orders --scenario hot-key --out skewed/', 'bend the data with an example scenario'),
    ),
)
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
    aws_profile: AwsProfileOption = None,
    verbose: VerboseOption = 0,
    quiet: QuietOption = False,
    as_json: ManifestJsonOption = False,
) -> None:
    """Write a dataset, as data plan prints it.

    A directory or S3 --out holds the files and their _manifest.json. A destination holding anything is refused unless
    --replace is given. A write that stops after claiming a directory, on a failure, Ctrl-C or SIGTERM, leaves
    _manifest.partial.json listing the files it completed. A one-file write that stops leaves nothing, unless it is killed
    outright, which leaves a staging directory beside the file for --replace to remove.

    A Parquet or Arrow file written with --embed-profile regenerates its dataset as PROFILE. Its seed, rows and scenario
    are the defaults, and with none of the three given, a file whose dataset this release would not make again is refused.

    A bundle PROFILE writes each table under --out/<table>/ with its own manifest, parents first, each foreign key drawn
    from its parent's keys, and a _manifest.json at --out naming each. Each --scenario bends the table it names.
    """
    logged_at(verbose, quiet)
    source = profile_or_columns(profile_name, column_names(columns))
    arguments: dict[str, Any] = {
        'rows': rows,
        'seed': seed,
        'scale': scale,
        'format': file_format,
        'file_rows': file_rows,
        'partition_by': partition_keys(partition_by, no_partitions),
        'partition_style': partition_style,
        'writer': write_settings(writer),
        'scenario': scenarios_named(scenario),
        'embed_profile': embed_profile,
        'manifest': manifest_beside,
        'aws_profile': aws_profile,
    }
    terminate = signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        written = doppeldata.write(source, out, threads=threads, replace=replace, **arguments)
    finally:
        signal.signal(signal.SIGTERM, terminate)
    if as_json:
        print_json(written.document)
        return
    if isinstance(written, BundleManifest):
        print_bundle_written(written)
        return
    print_written(written, one_file_of(out) if not manifest_beside else None)


def print_written(manifest: Manifest, one_file: Path | None) -> None:
    """What one table's write wrote, at what rate, and where its identity is recorded."""
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
    if one_file is not None:
        typer.echo(f'{one_file} holds dataset {manifest.dataset_digest}')
    else:
        typer.echo(f'{beside(document["destination"], MANIFEST_FILE_NAME)} names dataset {manifest.dataset_digest}')


def print_bundle_written(written: BundleManifest) -> None:
    """What each table of a bundle's write wrote, and where the bundle's identity is recorded."""
    width = max(len(name) for name in written.tables)
    for name, table in written.tables.items():
        stored = size(sum(file['bytes'] for file in table.files))
        rows_written = sum(file['rows'] for file in table.files)
        typer.echo(
            f'{name:<{width}}  wrote {counted(rows_written, "row")} as {table.format} in {counted(len(table.files), "file")}, {stored}'
        )
    document = written.document
    seconds = max(document['elapsed_ms'], 1) / 1000
    typer.echo(
        f'in {seconds:.2f} s on {counted(document["threads"], "thread")}; '
        f'{beside(document["destination"], MANIFEST_FILE_NAME)} names dataset {written.dataset_digest}'
    )


@scenario_app.command(
    'list',
    epilog=examples(
        ('doppeldata scenario list', 'each op, metric and example by name'),
        ('doppeldata scenario list --json', 'the same, for a program'),
    ),
)
def scenario_list(as_json: JsonOption = False) -> None:
    """List the ops a scenario applies, the metrics its expect names, and the example scenarios doppeldata ships.

    scenario show takes an op's or an example's name.
    """
    sections = scenario_listing()
    if as_json:
        print_json(sections)
        return
    width = max((len(row['name']) for rows in sections.values() for row in rows), default=0)
    lines: list[str] = []
    for title, rows in sections.items():
        lines += [*([''] if lines else []), title, *(f'  {row["name"]:<{width}}  {row["description"]}' for row in rows)]
    typer.echo('\n'.join(lines))


@scenario_app.command(
    'show',
    no_args_is_help=True,
    epilog=examples(
        ('doppeldata scenario show hot_keys', "an op's parameters, each with its type and whether it is required"),
        ('doppeldata scenario show hot-key > hot-key.scenario.yaml', 'copy an example scenario to edit'),
    ),
)
def scenario_show(
    name: Annotated[str, typer.Argument(metavar='NAME', help='An op or an example, as scenario list prints it.')],
    as_json: Annotated[
        bool,
        typer.Option('--json', help="Print the op's parameters, or the example's document, as JSON, and each refusal as a line of JSON."),
    ] = False,
) -> None:
    """Print an op's parameters, or an example scenario's YAML to start one from."""
    shown = scenario_reference(name)
    if not isinstance(shown, OpReference):
        if as_json:
            print_json(shown.document)
        else:
            typer.echo(shown.text, nl=False)
        return
    if as_json:
        print_json(shown.to_json())
        return
    width = max((len(parameter.name) for parameter in shown.parameters), default=0)
    lines = [f'op {shown.name}: {shown.description}', '']
    for parameter in shown.parameters:
        needed = 'required' if parameter.required else 'optional'
        described = f'. {parameter.description}' if parameter.description else ''
        lines.append(f'  {parameter.name:<{width}}  {needed}  {parameter.type}{described}')
    typer.echo('\n'.join(lines))


@scenario_app.command(
    'check',
    no_args_is_help=True,
    epilog=examples(
        ('doppeldata scenario check hot-key orders', 'check an example scenario against an example profile'),
        ('doppeldata scenario check bad-csv orders --format parquet', "see why a layout cannot hold a scenario's faults"),
    ),
)
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
    verbose: VerboseOption = 0,
    quiet: QuietOption = False,
    as_json: JsonOption = False,
) -> None:
    """Check a scenario against a profile without generating.

    It validates the scenario, applies its patch, and plans each perturbation and the file layout as data generate would with
    the same flags. Exits 0 when the scenario holds, and 3 when it does not, naming each fault and the JSON Pointer to it.
    """
    logged_at(verbose, quiet)
    with refused_at({'scenario': 'SCENARIO'}):
        report = check_scenario(
            scenario_name,
            profile_name,
            rows=rows,
            seed=seed,
            format=file_format,
            file_rows=file_rows,
            partition_by=partition_keys(partition_by, no_partitions),
        )
    typer.echo(report.to_json() if as_json else report.to_markdown(), nl=False)
    if not report.passed:
        raise CheckFailed(report)


@schemas_app.command(
    'list',
    epilog=examples(('doppeldata schemas list', 'each schema by name'), ('doppeldata schemas list --json', 'the same, for a program')),
)
def schemas_list(as_json: JsonOption = False) -> None:
    """List every JSON Schema doppeldata ships, by the name schemas show takes."""
    summaries = schema_summaries()
    if as_json:
        print_json([dc.asdict(summary) for summary in summaries])
        return
    width = max((len(summary.name) for summary in summaries), default=0)
    for summary in summaries:
        typer.echo(f'{summary.name:<{width}}  {summary.description}')


@schemas_app.command(
    'show',
    no_args_is_help=True,
    epilog=examples(
        ('doppeldata schemas show manifest', 'the schema of _manifest.json'),
        ('doppeldata schemas show profile > profile.schema.json', 'a schema for an editor to validate profiles with'),
    ),
)
def schemas_show(
    name: Annotated[str, typer.Argument(metavar='NAME', help='A schema name, as schemas list prints it.')],
    as_json: Annotated[bool, typer.Option('--json', help='Print each refusal as a line of JSON. The schema is JSON either way.')] = False,
) -> None:
    """Print a JSON Schema (Draft 2020-12)."""
    typer.echo(schema_text(name), nl=False)


@capabilities_app.command('show', epilog=CAPABILITIES_EXAMPLES)
def capabilities_show(ctx: typer.Context, as_json: JsonOption = False) -> None:
    """List everything this install takes, read from the registries that refuse a value outside them.

    It is read from the installed doppeldata, so it matches this version exactly. --json prints each list whole, with every
    field of each row the text leaves out.
    """
    held = capabilities(command_surface(ctx))
    if as_json:
        print_json(held)
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


@app.callback(epilog=ROOT_EXAMPLES)
def root(
    version: Annotated[
        bool | None,
        typer.Option('--version', callback=version_callback, is_eager=True, help='Show the installed version and exit.'),
    ] = None,
) -> None:
    """Profile a dataset into a reviewable file holding no rows, then generate synthetic data from it.

    Every command is doppeldata NOUN VERB, as doppeldata data generate is, and every one takes --json.
    Start with doppeldata profile list for an example to generate from, or doppeldata profile create on data you hold.
    """
