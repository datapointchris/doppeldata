"""A seed from a Frictionless Table Schema v2, and a bundle of seeds from a Data Package's resources.

One Table Schema records no relationship, because a foreign key names a resource only a package declares. In a package,
each single-field foreign key becomes a bundle relationship with no fan-out.
"""

import dataclasses as dc
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from doppeldata import issue_codes
from doppeldata.errors import SourceError
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import Profile
from doppeldata.seed_profiles.declared_columns import DATE_WINDOW
from doppeldata.seed_profiles.declared_columns import DeclaredColumn
from doppeldata.seed_profiles.declared_columns import warn_left_out
from doppeldata.seed_profiles.declared_tables import DeclaredTable
from doppeldata.seed_profiles.declared_tables import imported_generated
from doppeldata.seed_profiles.json_schemas import bounds
from doppeldata.seed_profiles.schema_files import schema_document
from doppeldata.seed_profiles.schema_files import table_name

# Each field type a column draws, and its logical type. `any` reads as text.
TYPES = {
    'string': 'string',
    'integer': 'integer',
    'number': 'number',
    'boolean': 'boolean',
    'date': 'date',
    'datetime': 'timestamp',
    'time': 'time',
    'year': 'integer',
    'list': 'array',
    'any': 'string',
}
# What Table Schema reads as null where a schema names no `missingValues`.
DEFAULT_MISSING_VALUES = ('',)


def table_schema_profile(path: str | os.PathLike[str]) -> Profile:
    """The seed of the table a Table Schema file describes, named for the file."""
    found, document = schema_document(path)
    return schema_table(document, table_name(found), f'table-schema:{found.name}').profile()


def data_package_profile(path: str | os.PathLike[str], *, table: str | None = None) -> Profile:
    """The seed of the resource `table` names in a Data Package, or of its only one. `Profile.from_data_package` documents it."""
    found, _, resources = package_resources(path)
    if table is None and len(resources) != 1:
        message = f'{found} holds the resources {", ".join(resources)}; name one with table=, or --table'
        raise SourceError(issue_codes.SCHEMA_TABLE_NOT_NAMED, message, role='schema', valid=list(resources))
    name = table if table is not None else next(iter(resources))
    if name not in resources:
        message = f'{found} holds no resource named {name}; it holds {", ".join(resources)}'
        raise SourceError(issue_codes.SCHEMA_TABLE_UNKNOWN, message, role='schema', value=name, valid=list(resources))
    return schema_table(resources[name], name, f'data-package:{found.name}#{name}').profile()


def data_package_bundle(path: str | os.PathLike[str]) -> Bundle:
    """A bundle of every resource's seed in a Data Package, and a relationship for each single-field foreign key."""
    found, package_name, resources = package_resources(path)
    tables = {name: schema_table(schema, name, f'data-package:{found.name}#{name}').profile() for name, schema in resources.items()}
    bundle = Bundle.of(package_name, tables)
    bundle.document['generated'] = imported_generated(f'data-package:{found.name}')
    relationships = foreign_keys(resources, found)
    if relationships:
        bundle.document['relationships'] = relationships
    issues = bundle.validate()
    if issues:
        raise InvalidProfile(str(found), issues)
    return bundle


def package_resources(path: str | os.PathLike[str]) -> tuple[Path, str, dict[str, Mapping[str, Any]]]:
    """The package's name, or its file's where it gives none, and each resource holding a Table Schema, by name, with the
    schema read from beside the package where the resource names a file.
    """
    found, package = schema_document(path)
    if not isinstance(package, Mapping) or not isinstance(package.get('resources'), list):
        raise SourceError(issue_codes.NOT_THE_SCHEMA_FORMAT, f'{found} is not a Data Package, which lists its resources', role='schema')
    resources: dict[str, Mapping[str, Any]] = {}
    for resource in package['resources']:
        if not isinstance(resource, Mapping) or 'schema' not in resource:
            continue
        schema = resource['schema']
        if isinstance(schema, str):
            if '://' in schema:
                message = f'{found}: {resource.get("name")} names its schema at {schema}; save it beside the package'
                raise SourceError(issue_codes.SCHEMA_REFERENCE_UNRESOLVED, message, role='schema', value=schema)
            _, schema = schema_document(found.parent / schema)
        resources[str(resource.get('name'))] = schema
    if not resources:
        raise SourceError(issue_codes.NOT_THE_SCHEMA_FORMAT, f'{found} holds no resource with a schema', role='schema')
    return found, str(package.get('name') or table_name(found)), resources


def schema_table(schema: Any, name: str, named: str) -> DeclaredTable:
    """The table a Table Schema describes, its first missing value written as a CSV's null."""
    if not isinstance(schema, Mapping) or not isinstance(schema.get('fields'), list):
        raise SourceError(issue_codes.NOT_THE_SCHEMA_FORMAT, f'{named} is not a Table Schema, which lists its fields', role='schema')
    columns, unread = [], []
    for field in schema['fields']:
        column = schema_column(field)
        if column is None:
            unread.append(f'{field.get("name")} ({field.get("type")})' if isinstance(field, Mapping) else repr(field))
        else:
            columns.append(column)
    if unread:
        warn_left_out(f'{named}: fields of types the engine draws nothing of are left out: {", ".join(unread)}')
    missing = [entry.get('value') if isinstance(entry, Mapping) else entry for entry in schema.get('missingValues', DEFAULT_MISSING_VALUES)]
    source = {'csv': {'writer': {'null': str(missing[0])}}} if missing and missing[0] != '' else {}
    return DeclaredTable(
        name=name,
        columns=tuple(columns),
        imported_from=named,
        primary_key=tuple(as_list(schema.get('primaryKey'))),
        unique_keys=tuple(tuple(as_list(key)) for key in schema.get('uniqueKeys') or []),
        source=source,
    )


def schema_column(field: Any, name: str | None = None) -> DeclaredColumn | None:
    """The column a field declares, or None for a type the engine draws nothing of, such as `geopoint` or `duration`."""
    if not isinstance(field, Mapping):
        return None
    logical = TYPES.get(field.get('type', 'any'))
    if logical is None:
        return None
    constraints = field.get('constraints') or {}
    categories = [entry.get('value') if isinstance(entry, Mapping) else entry for entry in field.get('categories') or []]
    column = DeclaredColumn(
        name=name or str(field.get('name')),
        logical_type=logical,
        required=bool(constraints.get('required')),
        key=bool(constraints.get('unique')),
        values=tuple(constraints.get('enum') or categories),
    )
    if field.get('type') == 'year':
        column = dc.replace(
            column,
            physical_type='int16',
            low=DATE_WINDOW[0].year,
            high=DATE_WINDOW[1].year,
            undeclared=frozenset(end for end, keyword in (('low', 'minimum'), ('high', 'maximum')) if keyword not in constraints),
        )
    if logical == 'string':
        return dc.replace(
            column,
            pattern=constraints.get('pattern'),
            min_length=constraints.get('minLength'),
            max_length=constraints.get('maxLength'),
            format=field.get('format'),
        )
    if logical in ('integer', 'number', 'date', 'timestamp'):
        found = {key: value for key, value in bounds(constraints).items() if value is not None}
        return dc.replace(column, **found)
    if logical == 'array':
        element = schema_column({'type': field.get('itemType', 'string')}, 'element')
        if element is None:
            return None
        return dc.replace(
            column,
            element=dc.replace(element, required=True),
            min_items=constraints.get('minLength'),
            max_items=constraints.get('maxLength'),
        )
    return column


def as_list(value: Any) -> list[str]:
    """A key's fields, written as a list or, as Table Schema v1 allows, as one field's name."""
    if value is None:
        return []
    return [value] if isinstance(value, str) else [str(each) for each in value]


def foreign_keys(resources: Mapping[str, Mapping[str, Any]], found: Path) -> list[dict[str, list[str]]]:
    """Each foreign key in the package as a relationship from each `table.column` to the `table.column` it refers to."""
    relationships, unread = [], []
    for name, schema in resources.items():
        for key in schema.get('foreignKeys') or []:
            reference = key.get('reference') or {}
            fields, referred = as_list(key.get('fields')), as_list(reference.get('fields'))
            target = reference.get('resource') or name
            if not fields or len(fields) != len(referred) or target not in resources or target == name:
                unread.append(f'{name}({", ".join(fields)})')
                continue
            relationships.append({'from': [f'{name}.{field}' for field in fields], 'to': [f'{target}.{field}' for field in referred]})
    if unread:
        warn_left_out(
            f'{found}: foreign keys referring to the same resource, to no resource the package holds, or naming fields '
            f'unevenly are left out: {", ".join(unread)}'
        )
    return relationships
