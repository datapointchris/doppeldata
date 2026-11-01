"""The JSON Schemas the library ships, as the engine's `published_schemas::all()` lists them."""

import dataclasses as dc
import importlib.resources
import json
from typing import Any

from doppeldata import _core
from doppeldata.errors import InvalidArgument
from doppeldata.issue_codes import UNKNOWN_SCHEMA


@dc.dataclass(frozen=True)
class SchemaSummary:
    """One JSON Schema the library ships: the name `doppeldata schemas show` takes, and the schema's own title and description."""

    name: str
    title: str
    description: str


def schema_names() -> list[str]:
    """The names of the JSON Schemas the library ships, as `doppeldata schemas show` takes them."""
    return list(_core.schema_file_names())


def schema_text(name: str) -> str:
    """A JSON Schema the library ships, as committed in the package, refused with InvalidArgument where `name` names none."""
    files = _core.schema_file_names()
    if name not in files:
        raise InvalidArgument(UNKNOWN_SCHEMA, f'{name!r} is not a schema doppeldata ships', field='name', value=name, valid=list(files))
    return importlib.resources.files('doppeldata').joinpath('schemas', files[name]).read_text()


def published_enum(definition: str) -> list[str]:
    """The values the profile schema allows for one of its string enums, such as `SourceFormat`."""
    return enum_values(profile_definitions()[definition])


def published_fields(definition: str) -> dict[str, list[str] | None]:
    """Each field of one of the profile schema's objects, such as `GeneratorHint`, with the values it allows where it is an enum."""
    definitions = profile_definitions()
    fields: dict[str, list[str] | None] = {}
    for field, schema in definitions[definition]['properties'].items():
        refs = [branch['$ref'] for branch in (schema, *schema.get('anyOf', ())) if '$ref' in branch]
        target = definitions[refs[0].rsplit('/', 1)[1]] if refs else {}
        fields[field] = enum_values(target) if is_enum(target) else None
    return fields


def is_enum(schema: dict[str, Any]) -> bool:
    return 'enum' in schema or all('const' in branch or 'enum' in branch for branch in schema.get('oneOf', [{}]))


def profile_definitions() -> dict[str, Any]:
    definitions: dict[str, Any] = json.loads(schema_text('profile'))['$defs']
    return definitions


def enum_values(schema: dict[str, Any]) -> list[str]:
    """An enum's values. One whose values carry descriptions is published as a `oneOf`, of constants and of the undescribed rest."""
    branches = schema.get('oneOf', [schema])
    return [value for branch in branches for value in (branch['enum'] if 'enum' in branch else [branch['const']])]


def output_formats() -> list[str]:
    """The formats a write takes, as the manifest schema's `format` names them."""
    return [branch['properties']['format']['const'] for branch in json.loads(schema_text('manifest'))['oneOf']]


def partition_styles() -> list[str]:
    """The partition directory styles a write takes, as the manifest schema names them."""
    return [branch['const'] for branch in json.loads(schema_text('manifest'))['$defs']['PartitionStyle']['oneOf']]


def schema_summaries() -> list[SchemaSummary]:
    """Every JSON Schema the library ships, by name, with the title and description each document carries."""
    documents = {name: json.loads(schema_text(name)) for name in schema_names()}
    return [SchemaSummary(name, document['title'], document['description']) for name, document in documents.items()]
