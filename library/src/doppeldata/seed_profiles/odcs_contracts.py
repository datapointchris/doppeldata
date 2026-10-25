"""A seed from one schema object of an Open Data Contract Standard v3 contract, each of its properties a column.

A property's `physicalType` gives its Arrow type where it is a Hive type, such as `varchar(18)` or `bigint`, and its
`logicalType` and `logicalTypeOptions` give the rest. A column takes its `physicalName`, as the data store holds it.
A `measure`, an aggregate held only as an expression, is no column.
"""

import dataclasses as dc
import os
from collections.abc import Mapping
from typing import Any

from doppeldata.errors import SourceError
from doppeldata.profile_format import Profile
from doppeldata.seed_profiles.declared_columns import DeclaredColumn
from doppeldata.seed_profiles.declared_columns import warn_left_out
from doppeldata.seed_profiles.declared_tables import DeclaredTable
from doppeldata.seed_profiles.declared_tables import PartitionKey
from doppeldata.seed_profiles.hive_types import hive_column
from doppeldata.seed_profiles.json_schemas import bounds
from doppeldata.seed_profiles.schema_files import schema_document

LOGICAL_TYPES = ('string', 'date', 'timestamp', 'time', 'number', 'integer', 'object', 'array', 'boolean')
# The classifications that mark a column sensitive, so the privacy policy treats it so once a real profile replaces the seed.
SENSITIVE_CLASSIFICATIONS = ('restricted', 'confidential')
# Where a property gives no position, it sorts after every one that does.
UNPOSITIONED = 1 << 31


def odcs_profile(path: str | os.PathLike[str], *, table: str | None = None) -> Profile:
    """The seed of the schema object `table` names in an ODCS v3 contract, or of its only one. `Profile.from_odcs` documents it."""
    found, document = schema_document(path)
    if not isinstance(document, Mapping) or not str(document.get('apiVersion', 'v3')).startswith('v3'):
        raise SourceError(f'{found} is not an ODCS v3 contract, whose apiVersion starts v3', role='schema')
    objects = [entry for entry in document.get('schema') or [] if isinstance(entry, Mapping)]
    names = [entry.get('name') for entry in objects]
    if table is None and len(objects) != 1:
        held = (
            f'holds the schema objects {", ".join(map(str, names))}; name one with table=, or --table'
            if objects
            else 'holds no schema object'
        )
        raise SourceError(f'{found} {held}', role='schema')
    chosen = (
        objects[0] if table is None else next((entry for entry in objects if table in (entry.get('name'), entry.get('physicalName'))), None)
    )
    if chosen is None:
        raise SourceError(f'{found} holds no schema object named {table}; it holds {", ".join(map(str, names))}', role='schema')
    named = f'odcs:{found.name}#{chosen.get("name")}'
    return contract_table(chosen, named).profile()


def contract_table(entry: Mapping[str, Any], named: str) -> DeclaredTable:
    properties = [prop for prop in entry.get('properties') or [] if isinstance(prop, Mapping) and prop.get('semanticType') != 'measure']
    declared = [(prop, contract_column(prop)) for prop in properties]
    kept = [(prop, column) for prop, column in declared if column is not None]
    unread = [f'{prop.get("name")} ({prop.get("logicalType") or prop.get("physicalType")})' for prop, column in declared if column is None]
    if unread:
        warn_left_out(f'{named}: properties of types the engine draws nothing of are left out: {", ".join(unread)}')
    primary = sorted((pair for pair in kept if pair[0].get('primaryKey')), key=lambda pair: position(pair[0], 'primaryKeyPosition'))
    partitioned = sorted((pair for pair in kept if pair[0].get('partitioned')), key=lambda pair: position(pair[0], 'partitionKeyPosition'))
    unique = [(column.name,) for prop, column in kept if prop.get('unique') and not prop.get('primaryKey')]
    quality = metrics(entry.get('quality'))
    return DeclaredTable(
        name=str(entry.get('physicalName') or entry.get('name')),
        columns=tuple(column for _, column in kept),
        imported_from=named,
        primary_key=tuple(column.name for _, column in primary),
        unique_keys=tuple(unique),
        partition_keys=tuple(PartitionKey(column.name, column.arrow_type()) for _, column in partitioned),
        row_count=row_count(quality.get('rowCount')),
        duplicate_row_rate=share(quality.get('duplicateValues')),
    )


def position(prop: Mapping[str, Any], key: str) -> int:
    value = prop.get(key)
    return value if isinstance(value, int) and value >= 0 else UNPOSITIONED


def contract_column(prop: Mapping[str, Any], name: str | None = None) -> DeclaredColumn | None:
    """The column a property declares, or None for a type the engine draws nothing of, such as `map` or `vector`."""
    name = name or str(prop.get('physicalName') or prop.get('name'))
    logical = prop.get('logicalType')
    physical = prop.get('physicalType')
    hive = hive_column(name, physical) if isinstance(physical, str) else None
    if hive is not None and logical in (None, hive.logical_type):
        column = hive
    elif logical is None or logical in LOGICAL_TYPES:
        column = DeclaredColumn(name=name, logical_type=str(logical or 'string'))
    else:
        return None
    options = prop.get('logicalTypeOptions') or {}
    column = dc.replace(column, required=bool(prop.get('required')), key=bool(prop.get('unique')))
    if prop.get('classification') in SENSITIVE_CLASSIFICATIONS:
        column = dc.replace(column, sensitivity='sensitive')
    values = tuple(entry.get('value') if isinstance(entry, Mapping) else entry for entry in prop.get('enum') or [])
    nulls = share(metrics(prop.get('quality')).get('nullValues'))
    if nulls is not None:
        column = dc.replace(column, null_rate=nulls)
    if column.logical_type == 'string':
        return dc.replace(
            column,
            values=values,
            pattern=options.get('pattern'),
            min_length=options.get('minLength', column.min_length),
            max_length=options.get('maxLength', column.max_length),
            format=options.get('format'),
        )
    if column.logical_type in ('integer', 'number', 'date', 'timestamp'):
        found = bounds(options)
        if column.logical_type in ('integer', 'number'):
            found['values'] = values
        if column.logical_type == 'timestamp' and options.get('timezone'):
            found['timezone'] = 'UTC'
            found['physical_type'] = 'timestamp[us, tz=UTC]'
        return dc.replace(column, **found)
    if column.logical_type == 'object':
        fields = [contract_column(field) for field in prop.get('properties') or [] if isinstance(field, Mapping)]
        return dc.replace(column, physical_type=None, fields=tuple(field for field in fields if field is not None))
    if column.logical_type == 'array':
        items = prop.get('items')
        element = contract_column(items, 'element') if isinstance(items, Mapping) else None
        return dc.replace(
            column,
            physical_type=None,
            element=dc.replace(element, required=True) if element is not None else None,
            min_items=options.get('minItems'),
            max_items=options.get('maxItems'),
        )
    return column


def metrics(checks: Any) -> dict[str, Mapping[str, Any]]:
    """Each library quality check by its metric, such as `nullValues`."""
    return {check['metric']: check for check in checks or [] if isinstance(check, Mapping) and isinstance(check.get('metric'), str)}


def share(check: Mapping[str, Any] | None) -> float | None:
    """The share of rows a `nullValues` or `duplicateValues` check allows: 0 where it must be 0, a percent it must be, or
    half a percent it must stay under, so generated data stays inside it.
    """
    if check is None:
        return None
    if check.get('mustBe') == 0:
        return 0.0
    if check.get('unit') != 'percent':
        return None
    for keyword, part in (('mustBe', 1.0), ('mustBeLessOrEqualTo', 0.5), ('mustBeLessThan', 0.5)):
        if isinstance(check.get(keyword), int | float):
            return min(1.0, check[keyword] / 100 * part)
    return None


def row_count(check: Mapping[str, Any] | None) -> int | None:
    """The row count a `rowCount` check names: the count it must be, or the least it allows."""
    if check is None:
        return None
    for keyword, added in (('mustBe', 0), ('mustBeGreaterOrEqualTo', 0), ('mustBeGreaterThan', 1)):
        if isinstance(check.get(keyword), int):
            return check[keyword] + added
    between = check.get('mustBeBetween')
    if isinstance(between, list) and between and isinstance(between[0], int):
        return between[0]
    return None
