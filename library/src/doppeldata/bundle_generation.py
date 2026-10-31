"""A bundle's tables generated together, each parent before its children, every foreign key drawn from its parent's keys.

Each table's identity holds the bundle's digest, the scale and the table's name, so a table regenerates alone from the bundle
and its manifest. A write puts each table under `<out>/<table>/` with a manifest of its own, and stores at `out` the bundle's
manifest naming each.
"""

import dataclasses as dc
import json
import math
import os
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import rfc8785

from doppeldata import _core
from doppeldata.dataset_generation import LARGEST_SEED
from doppeldata.dataset_generation import ArrowStreamExportable
from doppeldata.dataset_generation import WriteChoices
from doppeldata.dataset_generation import WritePlan
from doppeldata.dataset_generation import destination_of
from doppeldata.dataset_generation import one_file_of
from doppeldata.dataset_generation import s3_store_of
from doppeldata.dataset_generation import write_plan_of
from doppeldata.errors import StoppedWrite
from doppeldata.errors import WriteFailed
from doppeldata.example_profiles import bundle_example_names
from doppeldata.manifest import BundleManifest
from doppeldata.manifest import Manifest
from doppeldata.name_hinted_types import with_type_hints
from doppeldata.profile_format import BUNDLE_INDEX
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import Issue
from doppeldata.profile_format import digest_of
from doppeldata.profile_format import read_document
from doppeldata.profile_sources import ProfileSource
from doppeldata.scenario_format import InvalidScenario
from doppeldata.scenario_format import Scenario
from doppeldata.scenario_format import refuse_file_shaping

DOCUMENT_SUFFIXES = ('.yaml', '.yml', '.json')


def bundle_from(source: Any) -> Bundle | None:
    """The bundle `source` names: a Bundle, a bundle file, a directory holding `bundle.yaml`, or an example bundle's name.
    None for any other source, which names one table."""
    if isinstance(source, Bundle):
        return source
    if not isinstance(source, str | os.PathLike):
        return None
    path = Path(source)
    if path.is_dir():
        return Bundle.load(path) if (path / BUNDLE_INDEX).is_file() else None
    if path.is_file():
        if path.suffix.lower() not in DOCUMENT_SUFFIXES:
            return None
        document = read_document(path)
        return Bundle.load(path) if isinstance(document, dict) and document.get('kind') == 'bundle' else None
    if isinstance(source, str) and source in bundle_example_names():
        return Bundle.example(source)
    return None


def table_source(profile: ProfileSource | Bundle) -> ProfileSource:
    """`profile`, which `bundle_from` found names one table."""
    if isinstance(profile, Bundle):
        raise TypeError('a Bundle holds several tables, which generate, plan and write take')
    return profile


def table_scenarios(scenario: Scenario | Sequence[Scenario] | None, bundle: Bundle) -> dict[str, Scenario]:
    """Each table a `scenario=` argument bends and the one scenario it applies there: every scenario naming the table,
    composed in the order given. A scenario naming no table, or one the bundle lacks, raises InvalidScenario."""
    if scenario is None:
        return {}
    listed = [scenario] if isinstance(scenario, Scenario) else list(scenario) if isinstance(scenario, list | tuple) else None
    if listed is None or not all(isinstance(each, Scenario) for each in listed):
        raise TypeError(f'scenario takes a Scenario, such as Scenario.load(path), or a list of them, not {type(scenario).__name__}')
    grouped: dict[str, list[Scenario]] = {}
    for given in listed:
        for member in given.members or (given,):
            table = member.to_plain().get('table')
            if table not in bundle.tables:
                named = f'names {table}, which is not' if table is not None else 'names no table, and a bundle scenario names one'
                message = f'the scenario {named} of the bundle: {", ".join(bundle.tables)}'
                raise InvalidScenario(member.source, [Issue('unknown-table', '/table', message)])
            grouped.setdefault(table, []).append(member)
    return {table: Scenario.compose(*members) for table, members in grouped.items()}


def scenarios_document(scenarios: dict[str, Scenario]) -> dict[str, Any]:
    """The map of each table to its scenario's canonical document, which a bundle's scenario digest is taken over."""
    return {table: json.loads(applied.canonical()) for table, applied in sorted(scenarios.items())}


def scenarios_record(scenarios: dict[str, Scenario]) -> dict[str, Any] | None:
    """The scenarios as a bundle's manifests record them: their names joined with `+`, the document the digest covers, and
    each scenario's name, path and digest where several apply. None where none applies."""
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
            {'name': member.name, 'path': str(member.path) if member.path is not None else None, 'digest': member.digest}
            for member in members
        ]
    return record


@dc.dataclass(frozen=True)
class BundleArguments:
    """Everything the engine's bundle plan is built from, so a process holding only these, such as a Spark executor, builds
    the same plan. `bundle_json` holds every table inline, each with the hints the engine reads a typed text column with."""

    bundle_json: str
    bundle_digest: str
    seed: int
    scale: float
    source: str
    scenarios: tuple[tuple[tuple[str, str], ...], str] | None = None

    def plan(self) -> '_core.BundlePlan':
        return _core.BundlePlan(self.bundle_json, self.bundle_digest, self.seed, self.scale, self.source, scenarios=self.scenarios)


def bundle_arguments(bundle: Bundle, seed: int, scale: float, scenarios: dict[str, Scenario]) -> BundleArguments:
    """The engine's arguments for `bundle` at `scale` times each table's row count, from `seed`, under `scenarios`."""
    if not 0 <= seed <= LARGEST_SEED:
        raise ValueError(f'seed must be between 0 and 2^64 - 1, not {seed}')
    if isinstance(scale, bool) or not isinstance(scale, int | float) or not math.isfinite(scale) or scale <= 0:
        raise ValueError(f'scale must be a number above zero, not {scale!r}')
    plain = bundle.to_plain()
    plain['tables'] = {name: with_type_hints(table) for name, table in plain['tables'].items()}
    source = str(bundle.path) if bundle.path is not None else f'bundle {bundle.name!r}'
    applied = None
    if scenarios:
        listed = tuple((json.dumps(each.to_plain(), allow_nan=False), each.source) for each in scenarios.values())
        applied = (listed, digest_of(rfc8785.dumps(scenarios_document(scenarios))))
    return BundleArguments(json.dumps(plain, allow_nan=False), bundle.digest, seed, float(scale), source, applied)


def refuse_rows(rows: int | None) -> None:
    if rows is not None:
        raise ValueError("rows sizes one table, and a bundle is sized by scale, a multiple of each table's row count")


def generate_bundle(
    bundle: Bundle, *, seed: int, scale: float, threads: int, scenario: Scenario | Sequence[Scenario] | None
) -> dict[str, ArrowStreamExportable]:
    """Every table of `bundle`, each one Arrow table in memory, parents first."""
    scenarios = table_scenarios(scenario, bundle)
    for applied in scenarios.values():
        refuse_file_shaping(applied, 'generate', write_with='doppeldata.write')
    plan = bundle_arguments(bundle, seed, scale, scenarios).plan()
    return {name: table.generate(threads=threads) for name, table in plan.tables()}


@dc.dataclass(frozen=True)
class BundleWritePlan:
    """What `write` would do with a bundle and the same arguments: the bundle's identity, and each table's `WritePlan`,
    parents first, each under its own directory. `relationships` lists each with what draws its columns instead where
    another does. The shape of `to_json` may change before 1.0."""

    destination: str
    identity: dict[str, Any]
    dataset_digest: str
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
    bundle: Bundle,
    out: str | os.PathLike[str],
    seed: int,
    scale: float,
    choices: WriteChoices,
    scenario: Scenario | Sequence[Scenario] | None,
) -> PreparedBundleWrite:
    """What a bundle's write to `out` runs on, refusing what the engine would refuse."""
    if one_file_of(out) is not None:
        message = f'{os.fspath(out)} names one file, and a bundle writes a directory of tables; write it to a directory'
        raise WriteFailed('bundle-writes-a-directory', message, kind='refused')
    choice = (('partition_by', choices.partition_by), ('partition_style', choices.partition_style))
    held = [name for name, value in choice if value is not None]
    if held:
        message = f'each table of a bundle is partitioned as its profile records, so a bundle write takes no {" or ".join(held)}'
        raise WriteFailed('bundle-partitions-by-table', message, kind='refused')
    if choices.embed_profile:
        message = 'a bundle table regenerates from the whole bundle, which no file embeds; write it without embed_profile'
        raise WriteFailed('bundle-embeds-no-profile', message, kind='refused')
    destination, scheme, bucket = destination_of(out)
    scenarios = table_scenarios(scenario, bundle)
    arguments = bundle_arguments(bundle, seed, scale, scenarios)
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
        tables.append((name, table, options, table_destination(destination, name)))
    return PreparedBundleWrite(destination, scheme, bucket, bundle, arguments, engine, scenarios, choices_json, tables)


def plan_bundle(
    bundle: Bundle, out: str | os.PathLike[str], *, seed: int, scale: float, choices: WriteChoices, scenario: Any
) -> BundleWritePlan:
    prepared = prepared_bundle_write(bundle, out, seed, scale, choices, scenario)
    relationships = [
        {'name': name, 'pointer': pointer, 'drawn_by': drawn_by, 'entries_set_aside': [list(pair) for pair in entries]}
        for name, pointer, drawn_by, entries in prepared.engine.relationships()
    ]
    return BundleWritePlan(
        destination=prepared.destination,
        identity=json.loads(prepared.engine.identity_json),
        dataset_digest=prepared.engine.dataset_digest,
        tables={name: write_plan_of(table, options, destination) for name, table, options, destination in prepared.tables},
        relationships=relationships,
        set_aside=prepared.engine.set_aside(),
    )


def write_bundle(
    bundle: Bundle,
    out: str | os.PathLike[str],
    *,
    seed: int,
    scale: float,
    choices: WriteChoices,
    threads: int,
    replace: bool,
    scenario: Any,
) -> BundleManifest:
    """Write every table of `bundle` under `out`, and the bundle's manifest at `out`, returning it with each table's."""
    prepared = prepared_bundle_write(bundle, out, seed, scale, choices, scenario)
    s3_store = s3_store_of(prepared.destination, prepared.bucket)
    profile_record = {'name': bundle.name, 'path': str(bundle.path) if bundle.path is not None else None}
    record = scenarios_record(prepared.scenarios)
    engine = prepared.engine
    claim = json.loads(
        engine.claim(
            prepared.destination,
            json.dumps(profile_record),
            json.dumps(record) if record is not None else None,
            replace=replace,
            s3_store=s3_store,
        )
    )
    started = time.monotonic()
    written: list[str] = []

    def committed() -> str:
        held = [json.loads(each) for each in written]
        claim.update(threads=held[0]['threads'] if held else 0, elapsed_ms=round((time.monotonic() - started) * 1000))
        stored: str = engine.commit(prepared.destination, json.dumps(claim), written, s3_store=s3_store)
        return stored

    try:
        for _, table, options, destination in prepared.tables:
            written.append(table.write(options, destination, threads=threads, replace=False, s3_store=s3_store))
    except StoppedWrite:
        committed()
        raise
    return BundleManifest(
        json.loads(committed()), {name: Manifest.from_json(each) for (name, *_), each in zip(prepared.tables, written, strict=True)}
    )
