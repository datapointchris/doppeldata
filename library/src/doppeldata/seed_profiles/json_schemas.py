"""A seed from a JSON Schema (Draft 2020-12) of one record, each of its `properties` a column.

A nested object is a struct column and an array a list column, so `properties` and `items` nest as deep as the record
does. A `$ref` within the file is followed. A property the record does not require, or whose type allows `null`, draws
nulls.
"""

import dataclasses as dc
import math
import os
from collections.abc import Mapping
from typing import Any

from doppeldata import issue_codes
from doppeldata.errors import SourceError
from doppeldata.profile_format import Profile
from doppeldata.seed_profiles.declared_columns import DeclaredColumn
from doppeldata.seed_profiles.declared_columns import warn_left_out
from doppeldata.seed_profiles.declared_tables import DeclaredTable
from doppeldata.seed_profiles.schema_files import schema_document
from doppeldata.seed_profiles.schema_files import table_name

# A string `format` that makes the column temporal, and the logical type it takes.
TEMPORAL_FORMATS = {'date': 'date', 'date-time': 'timestamp', 'time': 'time'}
TYPES = ('string', 'integer', 'number', 'boolean', 'object', 'array')
# OpenAPI's integer and number formats, and the Arrow type each names.
NUMBER_FORMATS = {'int32': 'int32', 'int64': 'int64', 'float': 'float', 'double': 'double'}


def json_schema_profile(path: str | os.PathLike[str]) -> Profile:
    """The seed of the record a JSON Schema file describes, named for the file."""
    found, document = schema_document(path)
    named = f'json-schema:{found.name}'
    record = resolved(document, document)
    if not isinstance(record, Mapping) or not isinstance(record.get('properties'), Mapping):
        message = f'{found} describes no object with properties, which a seed takes its columns from'
        raise SourceError(issue_codes.NOT_THE_SCHEMA_FORMAT, message, role='schema')
    required = set(record.get('required') or [])
    columns, unread = [], []
    for name, schema in record['properties'].items():
        column = json_column(name, schema, name in required, document)
        if column is None:
            unread.append(name)
        else:
            columns.append(column)
    if unread:
        warn_left_out(f'{named}: properties of no type a column holds are left out: {", ".join(unread)}')
    return DeclaredTable(name=table_name(found), columns=tuple(columns), imported_from=named).profile()


def resolved(schema: Any, document: Any) -> Any:
    """`schema`, following a `$ref` to a pointer within the file, such as `#/$defs/address`, until it holds none."""
    seen = set()
    while isinstance(schema, Mapping) and isinstance(schema.get('$ref'), str) and schema['$ref'].startswith('#'):
        reference = schema['$ref']
        if reference in seen:
            raise SourceError(
                issue_codes.SCHEMA_REFERENCE_UNRESOLVED,
                f'{reference} refers back to itself, so it describes no column',
                role='schema',
                value=reference,
            )
        seen.add(reference)
        target = document
        try:
            for part in reference.lstrip('#').strip('/').split('/') if reference != '#' else []:
                step = part.replace('~1', '/').replace('~0', '~')
                target = target[int(step)] if isinstance(target, list) else target[step]
        except (KeyError, IndexError, TypeError, ValueError) as error:
            message = f'{reference} points at nothing in the file'
            raise SourceError(issue_codes.SCHEMA_REFERENCE_UNRESOLVED, message, role='schema', value=reference) from error
        if not isinstance(target, Mapping):
            message = f'{reference} points at {target!r}, which is no schema'
            raise SourceError(issue_codes.SCHEMA_REFERENCE_UNRESOLVED, message, role='schema', value=reference)
        schema = {**target, **{key: value for key, value in schema.items() if key != '$ref'}}
    return schema


def json_column(name: str, schema: Any, required: bool, document: Any) -> DeclaredColumn | None:
    """The column a property's schema declares, or None for one that is only `null` or names no type a column holds."""
    schema = resolved(schema, document)
    if not isinstance(schema, Mapping):
        return None
    schema, nullable = without_null_branch(schema, document)
    types = schema.get('type')
    listed = types if isinstance(types, list) else [types]
    nullable = nullable or 'null' in listed
    kind = next((each for each in listed if each in TYPES), None)
    values = schema.get('enum') or ([schema['const']] if 'const' in schema else [])
    nullable = nullable or None in values
    values = [value for value in values if value is not None]
    if kind is None and values:
        kind = 'boolean' if all(isinstance(value, bool) for value in values) else 'string'
    if kind is None:
        return None
    column = DeclaredColumn(name=name, logical_type=kind, required=required and not nullable)
    if kind == 'string':
        logical = TEMPORAL_FORMATS.get(schema.get('format', ''))
        if logical is not None:
            return dc.replace(column, logical_type=logical, timezone='UTC' if logical == 'timestamp' else None)
        return dc.replace(
            column,
            values=tuple(values),
            pattern=schema.get('pattern'),
            min_length=schema.get('minLength'),
            max_length=schema.get('maxLength'),
            format=schema.get('format'),
        )
    if kind in ('integer', 'number'):
        return dc.replace(column, physical_type=NUMBER_FORMATS.get(schema.get('format', '')), values=tuple(values), **bounds(schema))
    if kind == 'object':
        inner = set(schema.get('required') or [])
        fields = [
            json_column(field, inner_schema, field in inner, document) for field, inner_schema in (schema.get('properties') or {}).items()
        ]
        return dc.replace(column, fields=tuple(field for field in fields if field is not None)) if any(fields) else None
    if kind == 'array':
        element = json_column('element', schema.get('items') or {'type': 'string'}, True, document)
        return dc.replace(column, element=element, min_items=schema.get('minItems'), max_items=schema.get('maxItems'))
    return column


def without_null_branch(schema: Mapping[str, Any], document: Any) -> tuple[Mapping[str, Any], bool]:
    """A schema of `anyOf` or `oneOf` one type and `null`, as its one type and whether `null` was a branch."""
    for keyword in ('anyOf', 'oneOf'):
        branches = [resolved(branch, document) for branch in schema.get(keyword) or []]
        others = [branch for branch in branches if not (isinstance(branch, Mapping) and branch.get('type') == 'null')]
        if branches and len(others) == 1 and isinstance(others[0], Mapping):
            merged = {**{key: value for key, value in schema.items() if key != keyword}, **others[0]}
            return merged, len(others) < len(branches)
    return schema, False


def bounds(schema: Mapping[str, Any]) -> dict[str, Any]:
    """A number's bounds, as Draft 2020-12 writes them and as Draft 4's boolean `exclusiveMinimum` does, and its places."""
    found: dict[str, Any] = {'low': schema.get('minimum'), 'high': schema.get('maximum')}
    for end, keyword in (('low', 'exclusiveMinimum'), ('high', 'exclusiveMaximum')):
        excluded = schema.get(keyword)
        if isinstance(excluded, bool):
            found[f'{end}_exclusive'] = excluded
        elif isinstance(excluded, int | float):
            found |= {end: excluded, f'{end}_exclusive': True}
    step = schema.get('multipleOf')
    if isinstance(step, int | float) and 0 < step < 1 and math.isclose(-math.log10(step), round(-math.log10(step))):
        found['places'] = round(-math.log10(step))
    return found
