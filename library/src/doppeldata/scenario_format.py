"""Scenario format v1: read, validate, compose and identify a scenario file, and apply its patch to a profile.

A scenario is committed as YAML and accepted as JSON, and validates against the JSON Schema derived from the engine's Rust
model. Its identity is the SHA-256 of its RFC 8785 canonical JSON, with each perturbation's id resolved and the description
left out, so naming a default id or rewording the description keeps it.
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
from doppeldata.errors import DoppeldataError
from doppeldata.errors import ProfilingEnginesMissing
from doppeldata.example_scenarios import example_resource
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import Issue
from doppeldata.profile_format import PointerParts
from doppeldata.profile_format import Profile
from doppeldata.profile_format import cause_of
from doppeldata.profile_format import digest_of
from doppeldata.profile_format import integer_format_checker
from doppeldata.profile_format import pointer
from doppeldata.profile_format import read_document
from doppeldata.profile_format import to_plain
from doppeldata.profile_format import unrepresentable_numbers
from doppeldata.profile_format import yaml
from doppeldata.published_schemas import enum_values
from doppeldata.published_schemas import schema_text

if TYPE_CHECKING:
    from doppeldata.profile_comparison.reports import ExpectationResult

# The statistics an expectation may measure over every column, where the rest each measure one.
TABLE_METRICS = frozenset({'duplicates.rate'})
SCENARIO_EXPECTATIONS_MODULE = 'doppeldata.scenario_expectations'


class InvalidScenario(DoppeldataError, ValueError):
    """A scenario that does not validate, or that the profile it applies to or the files written cannot hold."""

    exit_code = 2

    def __init__(self, source: str, issues: list[Issue]) -> None:
        self.source = source
        self.issues = issues
        listed = '\n'.join(f'  {issue.field or "/"}: {issue.message} [{issue.code}]' for issue in issues)
        super().__init__(f'{source} is not a valid scenario:\n{listed}')


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
    return [
        Issue(f'schema-{cause.validator}', pointer((*at, *cause.absolute_path)), cause.message)
        for cause in map(cause_of, validator.iter_errors(value))
    ]


def check(plain: Any) -> list[Issue]:
    """Every reason the document is not a valid scenario: numbers canonical JSON cannot write, ops and metrics doppeldata
    does not know, the schema's, then the ids and expectations the schema cannot state."""
    if not isinstance(plain, dict):
        return [Issue('not-a-mapping', '', 'a scenario is a mapping of sections')]
    issues = list(unrepresentable_numbers(plain, ()))
    if issues:
        return issues
    entries, expectations = plain.get('perturbations'), plain.get('expect')
    outer = plain | {name: [] for name, listed in (('perturbations', entries), ('expect', expectations)) if isinstance(listed, list)}
    issues = schema_issues(validators()['scenario'], outer, ())
    for index, entry in enumerate(entries if isinstance(entries, list) else []):
        op = entry.get('op') if isinstance(entry, dict) else None
        if isinstance(op, str) and op not in op_schemas():
            issues.append(unknown_name('unknown-op', op, list(op_schemas()), ('perturbations', index), 'an op', 'every op'))
        else:
            issues += schema_issues(validators()[op if isinstance(op, str) else 'perturbation'], entry, ('perturbations', index))
    for index, expectation in enumerate(expectations if isinstance(expectations, list) else []):
        metric = expectation.get('metric') if isinstance(expectation, dict) else None
        if isinstance(metric, str) and metric not in expected_metrics():
            at = ('expect', index, 'metric')
            issues.append(unknown_name('unknown-metric', metric, expected_metrics(), at, 'a metric', 'every metric'))
        else:
            issues += schema_issues(validators()['expectation'], expectation, ('expect', index))
    if issues:
        return issues
    _, repeated = _core.scenario_ids(json.dumps(plain, allow_nan=False))
    if repeated is not None:
        index, given = repeated
        message = f"{given!r} is an earlier perturbation's id, and each id keys its own random stream; give this one another"
        issues.append(Issue('duplicate-perturbation-id', pointer(('perturbations', index, 'id')), message))
    return issues + list(expectation_issues(plain))


def unknown_name(code: str, name: str, known: list[str], at: PointerParts, noun: str, listing: str) -> Issue:
    """An op or metric doppeldata does not know, naming the closest it does and the command that lists them."""
    closest = difflib.get_close_matches(name, known, n=1)
    hint = f'the closest is {closest[0]}, and ' if closest else ''
    return Issue(code, pointer(at), f'{name!r} is not {noun} doppeldata knows; {hint}`doppeldata scenario list` lists {listing}')


def expected_perturbation(metric: str, column: str | None, perturbations: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The perturbation an expectation measures: the first of the op its metric is named for, on its column where it names one."""
    op = metric.split('.', 1)[0]
    for entry in perturbations:
        if entry['op'] == op and (metric in TABLE_METRICS or entry.get('column') == column):
            return entry
    return None


def expectation_issues(plain: dict[str, Any]) -> Iterator[Issue]:
    """Expectations giving no bound, naming no column where their metric measures one, or measuring what nothing perturbs."""
    perturbations = plain.get('perturbations') or []
    patched = ((plain.get('patch') or {}).get('columns')) or {}
    for index, expectation in enumerate(plain.get('expect') or []):
        at = pointer(('expect', index))
        metric, column = expectation['metric'], expectation.get('column')
        if expectation.get('at_least') is None and expectation.get('at_most') is None:
            yield Issue('invalid-parameter', at, f'{metric} gives no bound; an expectation takes at_least, at_most or both')
        if column is None and metric not in TABLE_METRICS:
            yield Issue('invalid-parameter', at, f'{metric} measures one column, so the expectation names it in `column`')
            continue
        entry = expected_perturbation(metric, column, perturbations)
        if metric == 'missingness.rate' and entry is None and (patched.get(column) or {}).get('null_rate') is not None:
            continue
        op, on = metric.split('.', 1)[0], f' to {column}' if column and metric not in TABLE_METRICS else ''
        if entry is None:
            message = f'no {op} perturbation applies{on}, so the scenario changes nothing {metric} measures'
            yield Issue('expectation-without-perturbation', at, message)
        elif metric == 'late_arrival.rate' and entry.get('ingest') is None:
            message = f'late_arrival.rate is measured against the ingest column of the late_arrival on {column}, which names none'
            yield Issue('invalid-parameter', at, message)


def merge_patch(target: Any, patch: Any) -> Any:
    """`patch` applied to `target` as RFC 7396 does: a mapping merges key by key, null removes a key, and anything else replaces."""
    if not isinstance(patch, dict):
        return copy.deepcopy(patch)
    merged = dict(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            merged.pop(key, None)
        else:
            merged[key] = merge_patch(merged.get(key), value)
    return merged


def composed_patch(first: dict[str, Any], then: dict[str, Any]) -> dict[str, Any]:
    """One merge patch doing what `first` then `then` do."""
    composed = dict(first)
    for key, value in then.items():
        earlier = composed.get(key)
        composed[key] = composed_patch(earlier, value) if isinstance(value, dict) and isinstance(earlier, dict) else copy.deepcopy(value)
    return composed


def patched_profile(profile: dict[str, Any], patch: dict[str, Any]) -> tuple[dict[str, Any], list[Issue]]:
    """`profile` with `patch` applied, its `columns` matched by name, and an issue for each patched column the profile lacks."""
    patched: dict[str, Any] = merge_patch(profile, {key: value for key, value in patch.items() if key != 'columns'})
    columns = patch.get('columns') or {}
    held = [column['name'] for column in profile.get('columns') or []]
    issues = [
        Issue('unknown-column', pointer(('patch', 'columns', name)), f'{name} is not a column of {profile["name"]}: {", ".join(held)}')
        for name in columns
        if name not in held
    ]
    patched['columns'] = [
        merge_patch(column, columns.get(column['name']) or {})
        for column in profile.get('columns') or []
        if column['name'] not in columns or columns[column['name']] is not None
    ]
    return patched, issues


def repointed(issue: Issue, patched: dict[str, Any]) -> Issue:
    """An issue the patched profile fails, pointed into the patch: `/columns/3/null_rate` as `/patch/columns/discount/null_rate`."""
    parts = issue.field.split('/')
    if len(parts) > 2 and parts[1] == 'columns' and parts[2].isdigit():
        name = patched['columns'][int(parts[2])]['name']
        return dc.replace(issue, field=pointer(('patch', 'columns', name)) + ''.join('/' + part for part in parts[3:]))
    return dc.replace(issue, field='/patch' + issue.field)


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
            document = read_document(path)
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
        it. A keyword changing any other value leaves the expectations and the description out, because each holds only for the
        example's values.
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
            raise TypeError(f'the {name} example applies {entry["op"]}, which takes {", ".join(taken)}, not {", ".join(unknown)}')
        if any(entry.get(key) != value for key, value in parameters.items() if key != 'column'):
            document['expect'] = []
            document.pop('description', None)
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
            raise ValueError('compose takes one scenario or more')
        if len(members) == 1:
            return members[0]
        plain = [member.to_plain() for member in members]
        document: dict[str, Any] = {'scenario': '+'.join(each['scenario'] for each in plain)}
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
        """The RFC 8785 canonical JSON the digest is taken over: each perturbation's id resolved, and no description."""
        plain = {key: value for key, value in self.to_plain().items() if key != 'description'}
        ids, _ = _core.scenario_ids(json.dumps(plain, allow_nan=False))
        plain['perturbations'] = [entry | {'id': id} for entry, id in zip(plain.get('perturbations') or [], ids, strict=True)]
        plain.setdefault('expect', [])
        return rfc8785.dumps(plain)

    @property
    def digest(self) -> str:
        """`sha256:<hex>` over the canonical form."""
        return digest_of(self.canonical())

    def apply(self, profile: Profile) -> Profile:
        """The profile with the scenario's patch applied: the effective profile the engine generates from, with its own digest.

        A patched column the profile does not hold is refused, and so is a patch leaving the profile invalid, each as
        InvalidScenario. Without a patch, `profile` itself is returned.
        """
        patch = self.to_plain().get('patch')
        if not patch:
            return profile
        patched, issues = patched_profile(profile.to_plain(), patch)
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
    raise TypeError(f'scenario takes a Scenario, such as Scenario.load(path), or a list of them, not {type(scenario).__name__}')


def refuse_file_shaping(scenario: Scenario, run: str) -> None:
    """Raise InvalidScenario naming each op that shapes the files a write makes, which `run`, writing none, cannot apply."""
    shaping = set(_core.file_shaping_ops())
    issues = [
        Issue('op-needs-files', pointer(('perturbations', index)), f'{entry["op"]} shapes the files a write makes, and {run} writes none')
        for index, entry in enumerate(scenario.to_plain().get('perturbations') or [])
        if entry['op'] in shaping
    ]
    if issues:
        raise InvalidScenario(scenario.source, issues)


def manifest_record(scenario: Scenario) -> dict[str, Any]:
    """The scenario as a manifest records it: its name and path, and each member's of a composition."""
    record: dict[str, Any] = {'name': scenario.name, 'path': str(scenario.path) if scenario.path is not None else None}
    if scenario.members:
        record['composed_of'] = [manifest_record(member) for member in scenario.members]
    return record
