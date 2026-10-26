"""The record schema: a JSON Schema 2020-12 of one record of a table, each column a property.

It mirrors `seed_profiles.json_schemas`, so each property imports as the column it was exported from. A struct nests
`properties` and a list `items`. A column of a JSON source whose raw values parse as several JSON types allows each,
never only the most common.
"""

import json
from typing import Any

from doppeldata.profile_exports.contract_columns import ContractColumn
from doppeldata.profile_exports.contract_columns import ContractTable
from doppeldata.profile_exports.type_names import JSON_NUMBER_FORMATS
from doppeldata.profile_exports.type_names import JSON_TEMPORAL_FORMATS
from doppeldata.profile_exports.type_names import JSON_TYPES

DIALECT = 'https://json-schema.org/draft/2020-12/schema'
LEFT_OUT = 'Left out, because the profile does not state it holds for every record'


def record_schema(table: ContractTable) -> str:
    notes = [note.note for note in table.left_out() if note.fact not in ('key', 'foreign_key', 'json_types')]
    properties = {column.name: column_schema(column, notes) for column in table.columns}
    schema: dict[str, Any] = {
        '$schema': DIALECT,
        'title': table.name,
        'description': f'One record of {table.name}, exported from its doppeldata profile sha256:{table.digest}.',
        'type': 'object',
        'properties': properties,
    }
    # A writer may leave a null field out, as doppeldata's does, so only a column never null is required.
    required = [column.name for column in table.columns if column.required]
    if required:
        schema['required'] = required
    if notes:
        schema['description'] += f' {LEFT_OUT}: ' + '; '.join(notes) + '.'
    return json.dumps(schema, indent=2, ensure_ascii=False) + '\n'


def column_schema(column: ContractColumn, notes: list[str]) -> dict[str, Any]:
    """The column's schema, adding to `notes` each fact JSON Schema has no keyword for."""
    kind = JSON_TYPES.get(column.logical_type, 'string')
    types = [json_type for json_type, _ in column.type_mix] if len(column.type_mix) > 1 else [kind]
    if not column.completeness.whole:
        types.append('null')
    schema: dict[str, Any] = {'type': types[0] if len(types) == 1 else types}
    form = JSON_TEMPORAL_FORMATS.get(column.logical_type) or JSON_NUMBER_FORMATS.get(column.physical_type) or column.held_format()
    if form:
        schema['format'] = form
    held = column.held_range()
    if held is not None:
        if kind in ('integer', 'number'):
            schema |= {keyword: end for keyword, end in zip(('minimum', 'maximum'), held, strict=True) if end is not None}
        else:
            notes.append(f'{column.label}: the range {held[0]} to {held[1]}, as JSON Schema bounds only a number')
    if kind == 'number' and column.places == 0:
        schema['multipleOf'] = 1
    elif kind == 'number' and column.places is not None:
        notes.append(f'{column.label}: its {column.places} decimal places, as a validator tests multipleOf in binary floating point')
    lengths = column.held_lengths()
    if lengths is not None:
        schema |= {keyword: end for keyword, end in zip(('minLength', 'maxLength'), lengths, strict=True) if end is not None}
    values = column.held_values()
    if values:
        schema['enum'] = [*values, *([None] if 'null' in types else [])]
    pattern = column.held_pattern()
    if pattern:
        schema['pattern'] = pattern
    if column.logical_type == 'object':
        schema['properties'] = {field.name: column_schema(field, notes) for field in column.fields}
        schema['required'] = [field.name for field in column.fields if field.presence in (None, 1.0)]
    if column.logical_type == 'array':
        schema['items'] = column_schema(column.element, notes) if column.element else {'type': 'string'}
        schema |= {
            keyword: count for keyword, count in (('minItems', column.min_items), ('maxItems', column.max_items)) if count is not None
        }
    return schema
