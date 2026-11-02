"""A Frictionless Table Schema v2 of a table, and a Data Package v2 of a bundle, each table a resource.

It mirrors `seed_profiles.table_schemas`, so each field imports as the same logical type, nullability, listed values and
bounds. Table Schema has no integer width, decimal precision or time zone, so those import as the importer's default.
Constraints hold for every row, and `missingValues` holds the CSV's null and every token the profile read as null. A
package's foreign key is a relationship the profile states every value holds.
"""

import json
import re
from typing import Any

from doppeldata.profile_exports.contract_columns import ContractColumn
from doppeldata.profile_exports.contract_columns import ContractDocument
from doppeldata.profile_exports.contract_columns import ContractTable
from doppeldata.profile_exports.type_names import TABLE_SCHEMA_NAMES

TABLE_SCHEMA_PROFILE = 'https://datapackage.org/profiles/2.0/tableschema.json'
DATA_PACKAGE_PROFILE = 'https://datapackage.org/profiles/2.0/datapackage.json'
# The string formats Table Schema v2 names.
STRING_FORMATS = ('email', 'uri', 'uuid')
RANGED_TYPES = ('integer', 'number', 'date', 'datetime')
# A profile's temporal format is chrono's, as Polars reads it, and a field's is Python's strptime. A format using only
# the directives both share is written. Another starting as ISO 8601 does is left to the field's default, which reads
# ISO 8601.
DIRECTIVE = re.compile(r'%(.)')
PYTHON_DIRECTIVES = frozenset('aAbBdHIjmMpSyYzZf%')
ISO_DATE = '%Y-%m-%d'
LEFT_OUT = 'Left out, because the profile does not state it holds for every row'


def table_schema(document: ContractDocument) -> str:
    """The Table Schema of the document's one table, or the Data Package of a bundle's, as JSON."""
    if not document.bundle:
        return dumped({'$schema': TABLE_SCHEMA_PROFILE, **schema_of(document.tables[0])})
    resources = []
    for table in document.tables:
        schema = schema_of(table)
        keys = [
            {'fields': list(key.columns), 'reference': {'resource': key.to_table, 'fields': list(key.to_columns)}}
            for key in table.foreign_keys
            if key.share is not None and key.share.whole
        ]
        if keys:
            schema['foreignKeys'] = keys
        resource = {'name': table.name, 'path': f'{table.name}.{table.source_format or "csv"}', 'schema': schema}
        resources.append(resource)
    package = {
        '$schema': DATA_PACKAGE_PROFILE,
        'name': document.name,
        'description': f'The tables of {document.name}, exported from their doppeldata profile, which holds no rows.',
        'resources': resources,
    }
    return dumped(package)


def schema_of(table: ContractTable) -> dict[str, Any]:
    notes: list[str] = [note.note for note in table.left_out()]
    fields = [field(column, table, notes) for column in table.columns]
    schema: dict[str, Any] = {'fields': fields}
    missing = missing_values(table)
    if missing != ['']:
        schema['missingValues'] = missing
    primary = table.primary_key
    if primary is not None and primary.whole:
        schema['primaryKey'] = list(primary.columns)
    composite = [list(key.columns) for key in table.unique_keys if len(key.columns) > 1 and key.whole]
    if composite:
        schema['uniqueKeys'] = composite
    if notes:
        schema['description'] = f'{LEFT_OUT}: ' + '; '.join(notes) + '.'
    return schema


def field(column: ContractColumn, table: ContractTable, notes: list[str]) -> dict[str, Any]:
    """The column's field, adding to `notes` each fact Table Schema has no place for."""
    kind = TABLE_SCHEMA_NAMES.get(column.logical_type, 'object')
    entry: dict[str, Any] = {'name': column.name, 'type': kind}
    if column.logical_type == 'array':
        element = TABLE_SCHEMA_NAMES.get(column.element.logical_type) if column.element else 'string'
        if element in (None, 'list'):
            entry['type'] = 'array'
        else:
            entry['itemType'] = element
    if kind in ('date', 'datetime') and column.strftime:
        if all(directive in PYTHON_DIRECTIVES for directive in DIRECTIVE.findall(column.strftime)):
            entry['format'] = column.strftime
        elif not column.strftime.startswith(ISO_DATE):
            notes.append(f'{column.label}: the format {column.strftime}, which has no Python strptime directives')
    constraints: dict[str, Any] = {}
    if column.completeness.whole:
        constraints['required'] = True
    if column.name in table.unique_columns():
        constraints['unique'] = True
    held = column.held_range()
    if held is not None and kind in RANGED_TYPES:
        constraints |= {keyword: end for keyword, end in zip(('minimum', 'maximum'), held, strict=True) if end is not None}
    lengths = column.held_lengths()
    if lengths is not None:
        constraints |= {keyword: end for keyword, end in zip(('minLength', 'maxLength'), lengths, strict=True) if end is not None}
    if column.logical_type == 'array' and entry['type'] == 'list':
        constraints |= {
            keyword: count for keyword, count in (('minLength', column.min_items), ('maxLength', column.max_items)) if count is not None
        }
    values = column.held_values()
    if values:
        constraints['enum'] = list(values)
    pattern = column.held_pattern()
    if pattern:
        constraints['pattern'] = unanchored(pattern)
    form = column.held_format()
    if form in STRING_FORMATS:
        entry['format'] = form
    elif form:
        notes.append(f'{column.label}: the {form} format, which Table Schema does not name')
    if constraints:
        entry['constraints'] = constraints
    if entry['type'] in ('object', 'array'):
        notes.append(f'{column.label}: its fields, as a Table Schema field of type {entry["type"]} names none')
    elif column.nested():
        notes.extend(
            f'{inner.label}: its constraints, as a Table Schema field holds no nested field'
            for inner in column.nested()
            if inner.holds_constraint()
        )
    return entry


def missing_values(table: ContractTable) -> list[str]:
    """The CSV's null first, as the importer reads it, then every token the profile read as null, sorted."""
    first = table.csv_null or ''
    tokens = sorted({token for column in table.columns for token in column.missing_tokens} - {first})
    return [first, *tokens]


def unanchored(pattern: str) -> str:
    """`pattern` without a leading `^` and a trailing `$`, as Table Schema matches a pattern against the whole value."""
    trimmed = pattern.removeprefix('^')
    return trimmed[:-1] if trimmed.endswith('$') and not trimmed.endswith('\\$') else trimmed


def dumped(document: dict[str, Any]) -> str:
    return json.dumps(document, indent=2, ensure_ascii=False) + '\n'
