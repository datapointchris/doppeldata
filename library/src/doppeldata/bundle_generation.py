"""A bundle's tables generated together, each parent before its children, every foreign key drawn from its parent's keys.

Each table's identity holds the bundle's SHA-256, the scale and the table's name, so a table regenerates alone from the bundle
and its manifest. A write puts each table under `<out>/<table>/` with a manifest of its own, and stores at `out` the bundle's
manifest naming each.
"""

import dataclasses as dc
import json
import os
import time
from collections.abc import Callable
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import rfc8785

from doppeldata import _core
from doppeldata import issue_codes
from doppeldata.dataset_generation import LARGEST_SEED
from doppeldata.dataset_generation import ArrowStreamExportable
from doppeldata.dataset_generation import WriteChoices
from doppeldata.dataset_generation import WritePlan
from doppeldata.dataset_generation import destination_of
from doppeldata.dataset_generation import one_file_of
from doppeldata.dataset_generation import s3_store_of
from doppeldata.dataset_generation import write_plan_of
from doppeldata.errors import InvalidArgument
from doppeldata.errors import StoppedWrite
from doppeldata.errors import UnsupportedArgument
from doppeldata.errors import WriteFailed
from doppeldata.issue_codes import Issue
from doppeldata.manifest import BundleManifest
from doppeldata.manifest import Manifest
from doppeldata.name_hinted_types import with_type_hints
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import sha256_of
from doppeldata.profile_sources import ProfileSource
from doppeldata.scenario_format import InvalidScenario
from doppeldata.scenario_format import Scenario
from doppeldata.scenario_format import refuse_file_shaping


@dc.dataclass(frozen=True)
class BundleRun:
    """A bundle to generate, from `seed` at `scale` times each table's row count."""

    bundle: Bundle
    seed: int
    scale: float


@dc.dataclass(frozen=True)
class TableRun:
    """One table to generate, as `generate` takes its profile, rows and seed."""

    profile: ProfileSource
    rows: int | None
    seed: int | None


def run_of(profile: ProfileSource | Bundle, rows: int | None, seed: int | None, scale: float | None) -> BundleRun | TableRun:
    """What a call generating `profile` generates. A Bundle is generated whole, from seed 0 and at scale 1 where left None,
    and refuses `rows`, which sizes one table. Anything else is one table, which refuses `scale`, which sizes a bundle."""
    if not isinstance(profile, Bundle):
        if scale is not None:
            message = "a scale sizes a bundle as a multiple of each table's row count, and this is one table; give a row count"
            raise InvalidArgument(issue_codes.ROWS_AND_SCALE, message, field='scale', value=scale)
        return TableRun(profile, rows, seed)
    if rows is not None:
        message = "a row count sizes one table, and a bundle is sized by a scale, a multiple of each table's row count"
        raise InvalidArgument(issue_codes.ROWS_AND_SCALE, message, field='rows', value=rows)
    return BundleRun(profile, 0 if seed is None else seed, 1.0 if scale is None else scale)


def table_scenarios(scenario: Scenario | Sequence[Scenario] | None, bundle: Bundle) -> dict[str, Scenario]:
    """Each table a `scenario=` argument bends and the one scenario it applies there: every scenario naming the table,
    composed in the order given. A scenario naming no table, or one the bundle lacks, raises InvalidScenario."""
    if scenario is None:
        return {}
    listed = [scenario] if isinstance(scenario, Scenario) else list(scenario) if isinstance(scenario, list | tuple) else None
    if listed is None or not all(isinstance(each, Scenario) for each in listed):
        message = f'scenario takes a Scenario, such as Scenario.load(path), or a list of them, not {type(scenario).__name__}'
        raise UnsupportedArgument(issue_codes.NOT_A_SCENARIO, message, field='scenario', value=type(scenario).__name__)
    grouped: dict[str, list[Scenario]] = {}
    for given in listed:
        for member in given.members or (given,):
            table = member.to_plain().get('table')
            if table not in bundle.tables:
                named = f'names {table}, which is not' if table is not None else 'names no table, and a bundle scenario names one'
                message = f'the scenario {named} of the bundle: {", ".join(bundle.tables)}'
                issue = Issue(issue_codes.UNKNOWN_TABLE, '/table', message, table, tuple(bundle.tables))
                raise InvalidScenario(member.source, [issue])
            grouped.setdefault(table, []).append(member)
    return {table: Scenario.compose(*members) for table, members in grouped.items()}


def scenarios_document(scenarios: dict[str, Scenario]) -> dict[str, Any]:
    """The map of each table to its scenario's canonical document, which a bundle's `scenario_sha256` is taken over."""
    return {table: json.loads(applied.canonical()) for table, applied in sorted(scenarios.items())}


def scenarios_record(scenarios: dict[str, Scenario]) -> dict[str, Any] | None:
    """The scenarios as a bundle's manifests record them: their names joined with `+`, the document `scenario_sha256` covers,
    and each scenario's name, path and SHA-256 where several apply. None where none applies."""
    if not scenarios:
        return None
    members = [member for applied in scenarios.values() for member in (applied.members or (applied,))]
    lone_path = members[0].path if len(members) == 1 else None
    record: dict[str, Any] = {
        'name': '+'.join(member.name for member in members),
        'path': str(lone_path) if lone_path is not None else None,
        'document': scenarios_document(scenarios),
    }
    if len(members) > 1:
        record['composed_of'] = [
            {'name': member.name, 'path': str(member.path) if member.path is not None else None, 'sha256': member.sha256}
            for member in members
        ]
    return record


@dc.dataclass(frozen=True)
class BundleArguments:
    """Everything the engine's bundle plan is built from, so a process holding only these, such as a Spark executor, builds
    the same plan. `bundle_json` holds every table inline, each with the hints the engine reads a typed text column with."""

    bundle_json: str
    bundle_sha256: str
    seed: int
    scale: float
    source: str
    scenarios: tuple[tuple[tuple[str, str], ...], str] | None = None

    def plan(self) -> '_core.BundlePlan':
        return _core.BundlePlan(self.bundle_json, self.bundle_sha256, self.seed, self.scale, self.source, scenarios=self.scenarios)


def bundle_arguments(bundle: Bundle, seed: int, scale: float, scenarios: dict[str, Scenario]) -> BundleArguments:
    """The engine's arguments for `bundle` at `scale` times each table's row count, from `seed`, under `scenarios`."""
    if not 0 <= seed <= LARGEST_SEED:
        raise InvalidArgument(issue_codes.INVALID_SEED, f'seed must be between 0 and 2^64 - 1, not {seed}', field='seed', value=seed)
    plain = bundle.to_plain()
    plain['tables'] = {name: with_type_hints(table) for name, table in plain['tables'].items()}
    source = str(bundle.path) if bundle.path is not None else f'bundle {bundle.name!r}'
    applied = None
    if scenarios:
        listed = tuple((json.dumps(each.to_plain(), allow_nan=False), each.source) for each in scenarios.values())
        applied = (listed, sha256_of(rfc8785.dumps(scenarios_document(scenarios))))
    return BundleArguments(json.dumps(plain, allow_nan=False), bundle.sha256, seed, float(scale), source, applied)


def generate_bundle(run: BundleRun, *, threads: int, scenario: Scenario | Sequence[Scenario] | None) -> dict[str, ArrowStreamExportable]:
    """Every table of the bundle, each one Arrow table in memory, parents first."""
    scenarios = table_scenarios(scenario, run.bundle)
    for applied in scenarios.values():
        refuse_file_shaping(applied, 'generate', write_with='doppeldata.write')
    plan = bundle_arguments(run.bundle, run.seed, run.scale, scenarios).plan()
    return {name: table.generate(threads=threads) for name, table in plan.tables()}


@dc.dataclass(frozen=True)
class BundleWritePlan:
    """What `write` would do with a bundle and the same arguments: the bundle's identity, and each table's `WritePlan`,
    parents first, each under its own directory. `relationships` lists each with what draws its columns instead where
    another does. The shape of `to_json` may change before 1.0."""

    destination: str
    identity: dict[str, Any]
    dataset_sha256: str
    tables: dict[str, WritePlan]
    relationships: list[dict[str, Any]]
    set_aside: list[str]

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True)
class PreparedBundleWrite:
    """A bundle write's destination, the engine's plan, and each table's plan, write options and destination, parents first.
    `choices_json` is what each table's options were built from."""

    destination: str
    scheme: str
    bucket: str | None
    bundle: Bundle
    arguments: BundleArguments
    engine: '_core.BundlePlan'
    scenarios: dict[str, Scenario]
    choices_json: str
    tables: list[tuple[str, '_core.Plan', '_core.WriteOptions', str]]


def table_destination(destination: str, table: str) -> str:
    """Where a bundle written to `destination` writes `table`."""
    if destination == 'memory://':
        return destination
    if '://' in destination:
        return f'{destination.rstrip("/")}/{table}'
    return os.fspath(Path(destination) / table)


def prepared_bundle_write(
    run: BundleRun, out: str | os.PathLike[str], choices: WriteChoices, scenario: Scenario | Sequence[Scenario] | None
) -> PreparedBundleWrite:
    """What a bundle's write to `out` runs on, refusing what the engine would refuse."""
    bundle = run.bundle
    if one_file_of(out) is not None:
        message = f'{os.fspath(out)} names one file, and a bundle writes a directory of tables; write it to a directory'
        raise WriteFailed(issue_codes.BUNDLE_WRITES_A_DIRECTORY, message)
    choice = (('partition keys', choices.partition_by), ('partition style', choices.partition_style))
    held = [concept for concept, value in choice if value is not None]
    if held:
        message = f'each table of a bundle is partitioned as its profile records, so a bundle write takes no {" or ".join(held)}'
        raise WriteFailed(issue_codes.BUNDLE_PARTITIONS_BY_TABLE, message)
    if choices.embed_profile:
        message = 'a bundle table regenerates from the whole bundle, which no file embeds; write it without embedding the profile'
        raise WriteFailed(issue_codes.BUNDLE_EMBEDS_NO_PROFILE, message)
    destination, scheme, bucket = destination_of(out)
    scenarios = table_scenarios(scenario, bundle)
    arguments = bundle_arguments(bundle, run.seed, run.scale, scenarios)
    engine = arguments.plan()
    record = scenarios_record(scenarios)
    chosen = {key: value for key, value in dc.asdict(choices).items() if value is not None and key != 'embed_profile'}
    if bundle.path is not None:
        chosen['profile_path'] = str(bundle.path)
    if record is not None:
        chosen['scenario'] = record
    choices_json = json.dumps(chosen)
    tables = []
    for name, table in engine.tables():
        options = table.write_options(choices_json)
        # Every range is planned here, so what a table's write would refuse is refused before the root is claimed.
        table.ranges_json(options)
        tables.append((name, table, options, table_destination(destination, name)))
    return PreparedBundleWrite(destination, scheme, bucket, bundle, arguments, engine, scenarios, choices_json, tables)


def plan_bundle(run: BundleRun, out: str | os.PathLike[str], *, choices: WriteChoices, scenario: Any) -> BundleWritePlan:
    prepared = prepared_bundle_write(run, out, choices, scenario)
    return BundleWritePlan(
        destination=prepared.destination,
        identity=json.loads(prepared.engine.identity_json),
        dataset_sha256=prepared.engine.dataset_sha256,
        tables={
            name: write_plan_of(table, options, destination, prepared.scenarios.get(name))
            for name, table, options, destination in prepared.tables
        },
        relationships=json.loads(prepared.engine.relationships_json),
        set_aside=prepared.engine.set_aside(),
    )


def write_bundle(
    run: BundleRun, out: str | os.PathLike[str], *, choices: WriteChoices, threads: int, replace: bool, scenario: Any
) -> BundleManifest:
    """Write every table of the bundle under `out`, and the bundle's manifest at `out`, returning it with each table's."""
    prepared = prepared_bundle_write(run, out, choices, scenario)
    store = s3_store_of(prepared.destination, prepared.bucket)

    def write_table(name: str, table: '_core.Plan', options: '_core.WriteOptions', destination: str) -> str:
        written: str = table.write(options, destination, threads=threads, replace=False, s3_store=store)
        return written

    return bundle_written(prepared, prepared.destination, replace, store, write_table, (StoppedWrite,))


def bundle_written(
    prepared: PreparedBundleWrite,
    root: str,
    replace: bool,
    store: dict[str, Any] | None,
    write_table: Callable[[str, '_core.Plan', '_core.WriteOptions', str], str],
    stops: tuple[type[BaseException], ...],
) -> BundleManifest:
    """Claims `root` for the bundle, writes each table to `<root>/<table>` through `write_table`, parents first, and stores
    the bundle's manifest, returning it with each table's. A table whose write raises one of `stops` leaves the root's
    partial manifest, naming each table already whole, and the error is raised."""
    options = {name: table_options for name, _, table_options, _ in prepared.tables}

    def write_with_options(name: str, table: '_core.Plan', destination: str) -> str:
        return write_table(name, table, options[name], destination)

    bundle = prepared.bundle
    profile_record = {'name': bundle.name, 'path': str(bundle.path) if bundle.path is not None else None}
    records = (profile_record, scenarios_record(prepared.scenarios))
    tables = [(name, table) for name, table, *_ in prepared.tables]
    return bundle_written_table_by_table(prepared.engine, records, tables, root, replace, store, write_with_options, stops)


def bundle_written_table_by_table(
    engine: '_core.BundlePlan',
    records: tuple[dict[str, Any], dict[str, Any] | None],
    tables: list[tuple[str, '_core.Plan']],
    root: str,
    replace: bool,
    store: dict[str, Any] | None,
    write_table: Callable[[str, '_core.Plan', str], str],
    stops: tuple[type[BaseException], ...],
) -> BundleManifest:
    """`bundle_written` from the bundle's plan, the `profile` and `scenario` records its manifest carries, and each table's
    plan, parents first, with `write_table` returning the manifest JSON of the table it writes to the destination it is given."""
    profile_record, scenario_record = records
    scenario_json = json.dumps(scenario_record) if scenario_record is not None else None
    claim = engine.claim(root, json.dumps(profile_record), scenario_json, replace=replace, s3_store=store)
    started = time.monotonic()
    written: list[str] = []

    def committed() -> str:
        elapsed_ms = round((time.monotonic() - started) * 1000)
        stored: str = engine.commit(root, claim, written, elapsed_ms=elapsed_ms, s3_store=store)
        return stored

    try:
        for name, table in tables:
            written.append(write_table(name, table, table_destination(root, name)))
    except stops:
        committed()
        raise
    manifests = {name: Manifest.from_json(each) for (name, _), each in zip(tables, written, strict=True)}
    return BundleManifest(json.loads(committed()), manifests)
