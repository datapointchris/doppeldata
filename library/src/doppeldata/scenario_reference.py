"""What the scenario format offers, read from its schema: each op a scenario applies, each metric its `expect` names, each
example the library ships, and one op's parameters."""

import dataclasses as dc
from typing import Any

from doppeldata import example_scenarios
from doppeldata import issue_codes
from doppeldata.errors import InvalidArgument
from doppeldata.published_schemas import enum_values
from doppeldata.scenario_format import Scenario
from doppeldata.scenario_format import op_schemas
from doppeldata.scenario_format import scenario_schema

ID_PARAMETER = "Keys the op's random stream. Defaults to <op>:<column, or table>:<n>, n counting earlier entries of that op and column."


def one_line(schema: dict[str, Any]) -> str:
    """A schema's description on one line."""
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


def scenario_listing() -> dict[str, list[dict[str, str]]]:
    """Each op, each metric an expectation names, and each example scenario, by name with its description."""
    ops = [{'name': name, 'description': one_line(branch)} for name, branch in op_schemas().items()]
    metrics = [
        {'name': branch['const'], 'description': one_line(branch)} for branch in scenario_schema()['$defs']['ExpectedMetric']['oneOf']
    ]
    examples = [
        {'name': name, 'description': Scenario.example(name).document.get('description') or ''}
        for name in example_scenarios.example_names()
    ]
    return {'ops': ops, 'metrics': metrics, 'examples': examples}


@dc.dataclass(frozen=True)
class OpParameter:
    """One parameter an op takes: whether a perturbation must give it, its type as a person reads it, and what it does."""

    name: str
    required: bool
    type: str
    description: str


@dc.dataclass(frozen=True)
class OpReference:
    """An op's description and each parameter it takes, in the order the scenario schema lists them."""

    name: str
    description: str
    parameters: list[OpParameter]

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True)
class ShippedScenario:
    """An example scenario as shipped: its YAML with comments, and its document."""

    name: str
    text: str
    document: dict[str, Any]


def scenario_reference(name: str) -> OpReference | ShippedScenario:
    """The example scenario `name`, else the op of that name, refused at `name` with every op and example where it is neither."""
    if name in example_scenarios.example_names():
        return ShippedScenario(name, example_scenarios.example_text(name), Scenario.example(name).document)
    branch = op_schemas().get(name)
    if branch is None:
        known = [*op_schemas(), *example_scenarios.example_names()]
        message = f'{name!r} is not an op or an example scenario doppeldata knows'
        raise InvalidArgument(issue_codes.UNKNOWN_OP, message, field='name', value=name, valid=known)
    required = branch.get('required', ())
    parameters = [
        OpParameter(key, key in required, parameter_type(schema), ID_PARAMETER if key == 'id' else one_line(schema))
        for key, schema in branch['properties'].items()
        if key != 'op'
    ]
    return OpReference(name, one_line(branch), parameters)
