"""The JSON Schemas the library ships, as the engine's `published_schemas::all()` lists them."""

import dataclasses as dc
import importlib.resources
import json

from doppeldata import _core


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
    """A JSON Schema the library ships, as committed in the package."""
    return importlib.resources.files('doppeldata').joinpath('schemas', _core.schema_file_names()[name]).read_text()


def published_enum(definition: str) -> list[str]:
    """The values the profile schema allows for one of its string enums, such as `SourceFormat`."""
    return list(json.loads(schema_text('profile'))['$defs'][definition]['enum'])


def schema_summaries() -> list[SchemaSummary]:
    """Every JSON Schema the library ships, by name, with the title and description each document carries."""
    documents = {name: json.loads(schema_text(name)) for name in schema_names()}
    return [SchemaSummary(name, document['title'], document['description']) for name, document in documents.items()]
