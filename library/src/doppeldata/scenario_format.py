"""The scenario format: read, validate, compose and identify a scenario file, and apply its patch to a profile.

A scenario is committed as YAML and accepted as JSON, and validates against the JSON Schema derived from the engine's Rust
model. Its identity is the SHA-256 of its RFC 8785 canonical JSON, with each perturbation's id resolved, and the description
and the expectations left out. So naming a default id, rewording the description or moving a bound keeps it. The engine
applies the patch, so the stream version governs how it merges.
"""

import copy
import dataclasses as dc
import difflib
import functools
import importlib
import importlib.resources
import io
import json
from collections.abc import Iterator
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

import jsonschema
import rfc8785

from doppeldata import _core
from doppeldata import issue_codes
from doppeldata.errors import DoppeldataError
from doppeldata.errors import InvalidArgument
from doppeldata.errors import IssueCarrier
from doppeldata.errors import ProfilingEnginesMissing
from doppeldata.errors import UnsupportedArgument
from doppeldata.example_scenarios import example_resource
from doppeldata.issue_codes import SCENARIO_DOCUMENT
from doppeldata.issue_codes import Issue
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import PointerParts
from doppeldata.profile_format import Profile
from doppeldata.profile_format import digest_of
from doppeldata.profile_format import integer_format_checker
from doppeldata.profile_format import pointer
from doppeldata.profile_format import read_document
from doppeldata.profile_format import schema_error_issues
from doppeldata.profile_format import to_plain
from doppeldata.profile_format import unrepresentable_numbers
from doppeldata.profile_format import yaml
from doppeldata.published_schemas import enum_values
from doppeldata.published_schemas import schema_text

if TYPE_CHECKING:
    from doppeldata.manifest import Manifest
    from doppeldata.profile_comparison.reports import ExpectationResult

SCENARIO_EXPECTATIONS_MODULE = 'doppeldata.scenario_expectations'


class InvalidScenario(DoppeldataError, ValueError):
    """A scenario that does not validate, that the profile it applies to or the files written cannot hold, that is not
    the scenario the data checked was written under, or whose faults a check cannot read back. `refusal` follows `source`
    on the message's first line, which is the whole message where no issue is listed."""

    def __init__(self, source: str, issues: Sequence[Issue], *, refusal: str = 'is not a valid scenario') -> None:
        self.source = source
        IssueCarrier.__init__(self, issues, headline=f'{source} {refusal}')


@functools.cache
def scenario_schema() -> dict[str, Any]:
    schema: dict[str, Any] = json.loads(schema_text('scenario'))
    return schema


def op_schemas() -> dict[str, dict[str, Any]]:
    """Each op's JSON Schema by its name, in the order the scenario schema lists them."""
    return {branch['properties']['op']['const']: branch for branch in scenario_schema()['$defs']['Perturbation']['oneOf']}


def expected_metrics() -> list[str]:
    """The statistics an expectation can name."""
    return enum_values(scenario_schema()['$defs']['ExpectedMetric'])


def fault_kinds() -> list[str]:
    """The faults malformed_rows can name."""
    return enum_values(scenario_schema()['$defs']['FaultKind'])


@functools.cache
def validators() -> dict[str, jsonschema.Draft202012Validator]:
    """The whole document's validator under `scenario`, one per op under its name, and one each for any perturbation and an
    expectation, so an error inside one entry is reported at its own field rather than as the `oneOf` it fails."""
    schema = scenario_schema()
    checker = integer_format_checker()
    definitions = {'$defs': schema['$defs']}
    by_part = {
        'scenario': jsonschema.Draft202012Validator(schema, format_checker=checker),
        'perturbation': jsonschema.Draft202012Validator(definitions | {'$ref': '#/$defs/Perturbation'}, format_checker=checker),
        'expectation': jsonschema.Draft202012Validator(definitions | {'$ref': '#/$defs/Expectation'}, format_checker=checker),
    }
    for op, branch in op_schemas().items():
        by_part[op] = jsonschema.Draft202012Validator(definitions | branch, format_checker=checker)
    return by_part


def schema_issues(validator: jsonschema.Draft202012Validator, value: Any, at: PointerParts) -> list[Issue]:
    return [issue for error in validator.iter_errors(value) for issue in schema_error_issues(error, at, SCENARIO_DOCUMENT)]


def check(plain: Any) -> list[Issue]:
    """Every reason the document is not a valid scenario: numbers canonical JSON cannot write, ops and metrics doppeldata
    does not know, the schema's, then the ids and expectations the schema cannot state."""
    if not isinstance(plain, dict):
        return [Issue(SCENARIO_DOCUMENT.not_a_mapping, '', 'a scenario is a mapping of sections', type(plain).__name__)]
    issues = list(unrepresentable_numbers(plain, (), SCENARIO_DOCUMENT))
    if issues:
        return issues
    entries, expectations = plain.get('perturbations'), plain.get('expect')
    outer = plain | {name: [] for name, listed in (('perturbations', entries), ('expect', expectations)) if isinstance(listed, list)}
    issues = schema_issues(validators()['scenario'], outer, ())
    for index, entry in enumerate(entries if isinstance(entries, list) else []):
        op = entry.get('op') if isinstance(entry, dict) else None
        if isinstance(op, str) and op not in op_schemas():
            issues.append(unknown_name(issue_codes.UNKNOWN_OP, op, list(op_schemas()), ('perturbations', index), 'an op'))
        else:
            issues += schema_issues(validators()[op if isinstance(op, str) else 'perturbation'], entry, ('perturbations', index))
    for index, expectation in enumerate(expectations if isinstance(expectations, list) else []):
        metric = expectation.get('metric') if isinstance(expectation, dict) else None
        if isinstance(metric, str) and metric not in expected_metrics():
            at = ('expect', index, 'metric')
            issues.append(unknown_name(issue_codes.UNKNOWN_METRIC, metric, expected_metrics(), at, 'a metric'))
        else:
            issues += schema_issues(validators()['expectation'], expectation, ('expect', index))
    if issues:
        return issues
    _, repeated = _core.scenario_ids(json.dumps(plain, allow_nan=False))
    if repeated is not None:
        index, given = repeated
        message = f"{given!r} is an earlier perturbation's id, and each id keys its own random stream; give this one another"
        issues.append(Issue(issue_codes.DUPLICATE_PERTURBATION_ID, pointer(('perturbations', index, 'id')), message, given))
    return issues + list(expectation_issues(plain))


def unknown_name(code: str, name: str, known: list[str], at: PointerParts, noun: str) -> Issue:
    """A name doppeldata does not know, with every one it knows and the closest of them."""
    closest = tuple(difflib.get_close_matches(name, known, n=3))
    return Issue(code, pointer(at), f'{name!r} is not {noun} doppeldata knows', name, tuple(known), closest)


@dc.dataclass(frozen=True)
class Facts:
    """What the engine says of one perturbation: its id, the columns it writes, whether it shapes files, whether it acts
    across several, and whether it orders the whole table, so one process writes the dataset."""

    id: str
    writes: tuple[str, ...]
    shapes_files: bool
    spans_files: bool
    needs_one_process: bool


def perturbation_facts(plain: dict[str, Any]) -> list[Facts]:
    """Each of a valid scenario's perturbations, in list order, as the engine's catalog describes it."""
    return [
        Facts(id, tuple(writes), shapes_files, spans_files, needs_one_process)
        for id, writes, shapes_files, spans_files, needs_one_process in _core.perturbation_facts(json.dumps(plain, allow_nan=False))
    ]


def expected_perturbation(metric: str, column: str | None, perturbations: list[dict[str, Any]]) -> int | None:
    """The index of the perturbation an expectation measures: the first of the op its metric measures, on its column where the
    op takes one."""
    op = _core.expected_metric_op(metric)
    for index, entry in enumerate(perturbations):
        if entry['op'] == op and entry.get('column', column) == column:
            return index
    return None


def expectation_issues(plain: dict[str, Any]) -> Iterator[Issue]:
    """Expectations giving no bound, measuring what nothing perturbs, or measuring a column another perturbation rewrites."""
    perturbations = plain.get('perturbations') or []
    facts = perturbation_facts(plain)
    patched = ((plain.get('patch') or {}).get('columns')) or {}
    for index, expectation in enumerate(plain.get('expect') or []):
        at = pointer(('expect', index))
        metric, column = expectation['metric'], expectation.get('column')
        if expectation.get('at_least') is None and expectation.get('at_most') is None:
            yield Issue(issue_codes.INVALID_PARAMETER, at, f'{metric} gives no bound; an expectation takes at_least, at_most or both')
        takes_column = _core.expected_metric_takes_column(metric)
        if not takes_column and column is not None:
            message = f'{metric} measures the whole table, so it takes no column'
            yield Issue(issue_codes.INVALID_PARAMETER, pointer(('expect', index, 'column')), message, column)
        if takes_column and column is None:
            yield Issue(issue_codes.INVALID_PARAMETER, at, f'{metric} measures one column, so it takes column')
            continue
        measured = expected_perturbation(metric, column, perturbations)
        if metric == 'missingness.fraction' and measured is None and (patched.get(column) or {}).get('null_fraction') is not None:
            continue
        op = _core.expected_metric_op(metric)
        if measured is None:
            on = f' to {column}' if 'column' in op_schemas()[op]['properties'] else ''
            message = f'no {op} perturbation applies{on}, so the scenario changes nothing {metric} measures'
            yield Issue(issue_codes.EXPECTATION_WITHOUT_PERTURBATION, at, message, metric)
        elif metric == 'late_arrival.fraction' and perturbations[measured].get('ingest') is None:
            message = f'late_arrival.fraction is measured against the ingest column of the late_arrival on {column}, which names none'
            yield Issue(issue_codes.INVALID_PARAMETER, at, message)
        elif metric == 'duplicates.fraction':
            rewriting = next(
                (later for position, later in enumerate(facts) if column in later.writes and (position > measured or later.shapes_files)),
                None,
            )
            if rewriting is not None:
                message = (
                    f'{rewriting.id} writes {column} after {facts[measured].id} copies rows, so a copy would no longer share its '
                    f"source's {column}; measure duplicates.fraction on a key no later perturbation writes"
                )
                yield Issue(issue_codes.INVALID_PARAMETER, pointer(('expect', index, 'column')), message, column)


def composed_patch(first: dict[str, Any], then: dict[str, Any]) -> dict[str, Any]:
    """One merge patch doing what `first` then `then` do."""
    composed = dict(first)
    for key, value in then.items():
        earlier = composed.get(key)
        composed[key] = composed_patch(earlier, value) if isinstance(value, dict) and isinstance(earlier, dict) else copy.deepcopy(value)
    return composed


def patched_profile(profile: dict[str, Any], scenario: dict[str, Any]) -> tuple[dict[str, Any], list[Issue]]:
    """`profile` with the scenario's patch applied as the engine applies it, and an issue for each patched column the profile
    lacks."""
    text, unknown = _core.patched_profile(json.dumps(profile, allow_nan=False), json.dumps(scenario, allow_nan=False))
    names = [column['name'] for column in profile.get('columns') or []]
    issues = [
        Issue(
            issue_codes.UNKNOWN_SCENARIO_COLUMN,
            pointer(('patch', 'columns', name)),
            f'{name} is not a column of {profile["name"]}: {", ".join(names)}',
            name,
            tuple(names),
            tuple(difflib.get_close_matches(name, names, n=3)),
        )
        for name in unknown
    ]
    patched: dict[str, Any] = json.loads(text)
    return patched, issues


def repointed(issue: Issue, patched: dict[str, Any]) -> Issue:
    """An issue the patched profile fails, pointed into the patch: `/columns/3/null_fraction` as `/patch/columns/discount/null_fraction`."""
    field = issue.field or ''
    parts = field.split('/')
    if len(parts) > 2 and parts[1] == 'columns' and parts[2].isdigit():
        name = patched['columns'][int(parts[2])]['name']
        return dc.replace(issue, field=pointer(('patch', 'columns', name)) + ''.join('/' + part for part in parts[3:]))
    return dc.replace(issue, field='/patch' + field)


class Scenario:
    """A scenario, kept as the document it was read from, or several composed in order, which `members` lists."""

    def __init__(self, document: Any, path: Path | None = None, members: Sequence['Scenario'] = ()) -> None:
        self.document = document
        self.path = path
        self.members = tuple(members)

    @classmethod
    def load(cls, path: str | Path) -> 'Scenario':
        """Read a YAML or JSON scenario, raising InvalidScenario when it does not validate."""
        path = Path(path)
        try:
            document = read_document(path, SCENARIO_DOCUMENT)
        except InvalidProfile as error:
            raise InvalidScenario(str(path), error.issues) from error
        scenario = cls(document, path)
        issues = scenario.validate()
        if issues:
            raise InvalidScenario(str(path), issues)
        return scenario

    @classmethod
    def example(cls, name: str, **parameters: Any) -> 'Scenario':
        """An example scenario the library ships, written for the `orders` example profile. It has no file of its own.

        Each example applies one perturbation, and a keyword replaces that parameter of it, as
        `Scenario.example('hot-key', column='customer_id', share=0.6)` does. `column` moves the example's expectations with
        it. A keyword changing any value leaves the description out, and one changing a value other than `column` leaves the
        expectations out too, because each holds only for the example's values.
        """
        with importlib.resources.as_file(example_resource(name)) as path:
            shipped = cls.load(path)
        shipped.path = None
        if not parameters:
            return shipped
        document = shipped.to_plain()
        (entry,) = document['perturbations']
        taken = [key for key in op_schemas()[entry['op']]['properties'] if key != 'op']
        unknown = sorted(set(parameters) - set(taken))
        if unknown:
            message = f'the {name} example applies {entry["op"]}, which takes {", ".join(taken)}, not {", ".join(unknown)}'
            closest = tuple(difflib.get_close_matches(unknown[0], taken, n=3))
            raise UnsupportedArgument(SCENARIO_DOCUMENT.unknown_field, message, field=unknown[0], valid=taken, closest=closest)
        if any(entry.get(key) != value for key, value in parameters.items()):
            document.pop('description', None)
        if any(entry.get(key) != value for key, value in parameters.items() if key != 'column'):
            document['expect'] = []
        for expectation in document.get('expect') or []:
            if 'column' in parameters and expectation.get('column') == entry.get('column'):
                expectation['column'] = parameters['column']
        entry.update(parameters)
        scenario = cls(document)
        issues = scenario.validate()
        if issues:
            raise InvalidScenario(f'the {name} example', issues)
        return scenario

    @classmethod
    def compose(cls, *scenarios: 'Scenario') -> 'Scenario':
        """Several scenarios as one, applied in order: their patches merge left to right, and their perturbations and
        expectations concatenate. Its name joins theirs with `+`, and its digest is the composed document's."""
        members = tuple(member for scenario in scenarios for member in (scenario.members or (scenario,)))
        if not members:
            raise InvalidArgument(issue_codes.COMPOSE_NOTHING, 'compose takes one scenario or more', field='scenarios')
        if len(members) == 1:
            return members[0]
        plain = [member.to_plain() for member in members]
        document: dict[str, Any] = {'scenario': '+'.join(each['scenario'] for each in plain)}
        tables = sorted({each['table'] for each in plain if each.get('table') is not None})
        if len(tables) > 1:
            message = f'the scenarios bend {", ".join(tables)}, and one composition bends one table'
            issue = Issue(issue_codes.UNKNOWN_TABLE, '/table', message, tables[1], tuple(tables))
            raise InvalidScenario('+'.join(each['scenario'] for each in plain), [issue])
        if tables:
            document['table'] = tables[0]
        patches = [each['patch'] for each in plain if each.get('patch') is not None]
        if patches:
            document['patch'] = functools.reduce(composed_patch, patches)
        document['perturbations'] = [entry for each in plain for entry in each.get('perturbations') or []]
        document['expect'] = [expectation for each in plain for expectation in each.get('expect') or []]
        composed = cls(document, None, members)
        issues = composed.validate()
        if issues:
            raise InvalidScenario(composed.source, issues)
        return composed

    @property
    def name(self) -> str:
        name: str = self.document['scenario']
        return name

    @property
    def source(self) -> str:
        """The scenario as an error names it: its path, or its name where it has no file."""
        return str(self.path) if self.path is not None else f'scenario {self.name!r}'

    def validate(self) -> list[Issue]:
        """Every reason the scenario is invalid, short of the profile it applies to. Empty when it is valid."""
        return check(self.to_plain())

    def to_plain(self) -> dict[str, Any]:
        plain: dict[str, Any] = to_plain(self.document)
        return plain

    def to_yaml(self) -> str:
        text = io.StringIO()
        yaml().dump(self.document, text)
        return text.getvalue()

    def canonical(self) -> bytes:
        """The RFC 8785 canonical JSON the digest is taken over: each perturbation's id resolved, and no description or
        expectations, which feed no value."""
        plain = {key: value for key, value in self.to_plain().items() if key not in ('description', 'expect')}
        ids, _ = _core.scenario_ids(json.dumps(plain, allow_nan=False))
        plain['perturbations'] = [entry | {'id': id} for entry, id in zip(plain.get('perturbations') or [], ids, strict=True)]
        return rfc8785.dumps(plain)

    @property
    def digest(self) -> str:
        """`sha256:<hex>` over the canonical form."""
        return digest_of(self.canonical())

    def apply(self, profile: Profile) -> Profile:
        """The profile with the scenario's patch applied: the effective profile the engine generates from, with its own digest.

        The engine applies the same patch when it generates. A patched column the profile does not hold is refused, and so is
        a patch leaving the profile invalid, each as InvalidScenario. Without a patch, `profile` itself is returned.
        """
        if not self.to_plain().get('patch'):
            return profile
        patched, issues = patched_profile(profile.to_plain(), self.to_plain())
        if not issues:
            issues = [repointed(issue, patched) for issue in Profile(patched).validate()]
        if issues:
            raise InvalidScenario(self.source, issues)
        return Profile(patched)

    def verify(self, data: Any) -> list['ExpectationResult']:
        """Each expectation the scenario declares, measured on `data`, which is anything `doppeldata.profile` reads."""
        if not self.to_plain().get('expect'):
            return []
        try:
            measuring = importlib.import_module(SCENARIO_EXPECTATIONS_MODULE)
        except ModuleNotFoundError as error:
            raise ProfilingEnginesMissing('polars', [error.name or 'polars']) from error
        verified: list[ExpectationResult] = measuring.verified(self.to_plain(), data)
        return verified


def composed(scenario: 'Scenario | Sequence[Scenario] | None') -> Scenario | None:
    """The scenario a `scenario=` argument names: one Scenario, or a list composed in order."""
    if scenario is None or isinstance(scenario, Scenario):
        return scenario
    if isinstance(scenario, list | tuple) and all(isinstance(each, Scenario) for each in scenario):
        return Scenario.compose(*scenario) if scenario else None
    message = f'scenario takes a Scenario, such as Scenario.load(path), or a list of them, not {type(scenario).__name__}'
    raise UnsupportedArgument(issue_codes.NOT_A_SCENARIO, message, field='scenario', value=type(scenario).__name__)


def refuse_file_shaping(
    scenario: Scenario, run: str, *, cannot: str = 'writes none', write_with: str | None = None, across_files: bool = False
) -> None:
    """Raise InvalidScenario naming each op that shapes the files a write makes, which `run` cannot apply, because it `cannot`.

    `across_files` refuses only an op acting across several files, which is all a write to one file cannot apply. `write_with`
    names the call that applies the rest.
    """
    plain = scenario.to_plain()
    instead = f'; write with {write_with}' if write_with else ''
    issues = [
        Issue(
            issue_codes.OP_NEEDS_FILES,
            pointer(('perturbations', index)),
            f'{entry["op"]} shapes the files a write makes, and {run} {cannot}{instead}',
            entry['op'],
        )
        for index, (entry, facts) in enumerate(zip(plain.get('perturbations') or [], perturbation_facts(plain), strict=True))
        if (facts.spans_files if across_files else facts.shapes_files)
    ]
    if issues:
        raise InvalidScenario(scenario.source, issues)


def refuse_other_scenario(scenario: Scenario | None, written: 'Manifest', data: str) -> None:
    """Raise InvalidScenario when `scenario` is not the one the manifest of `data` records, or is None where it records one.

    A bundle's table records the bundle's map of each table to its scenario, so the table is held to its own entry there.
    """
    recorded = written.document.get('scenario') or {}
    table = written.identity.get('table')
    if table is not None:
        applied = (recorded.get('document') or {}).get(table)
        if (json.loads(scenario.canonical()) if scenario is not None else None) == applied:
            return
        digest = digest_of(rfc8785.dumps(applied)) if applied is not None else None
    else:
        digest = written.identity.get('scenario_digest')
        if (scenario.digest if scenario is not None else None) == digest:
            return
    wrote = f'scenario {recorded.get("name")} ({digest})' if digest is not None else 'no scenario'
    if scenario is None:
        issue = Issue(issue_codes.DATA_HAS_SCENARIO, '', f'{data} was written under {wrote}; check it under that scenario', digest)
        raise InvalidScenario(data, [issue], refusal='cannot be checked with no scenario')
    message = f'{data} was written under {wrote}, and {scenario.name} is {scenario.digest}; check it under the scenario that wrote it'
    issue = Issue(issue_codes.SCENARIO_DIFFERS_FROM_DATA, '', message, scenario.digest)
    raise InvalidScenario(scenario.source, [issue], refusal=f'is not the scenario {data} was written under')


def manifest_record(scenario: Scenario) -> dict[str, Any]:
    """The scenario as a manifest records it: its name, its path, the canonical document its digest covers, and each member's
    name, path and digest where it composes several."""
    path = str(scenario.path) if scenario.path is not None else None
    record: dict[str, Any] = {'name': scenario.name, 'path': path, 'document': json.loads(scenario.canonical())}
    if scenario.members:
        record['composed_of'] = [
            {'name': member.name, 'path': str(member.path) if member.path is not None else None, 'digest': member.digest}
            for member in scenario.members
        ]
    return record
