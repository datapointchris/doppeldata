"""An Open Data Contract Standard v3.2.0 contract of a profile, each table a schema object and each column a property.

It mirrors `seed_profiles.odcs_contracts`: a `physicalType` is the column's Hive type, `logicalTypeOptions` hold what
holds for every row, and a share below every row becomes a `quality` check in percent. A bundle's tables are one
contract, each foreign key a relationship from its property.
"""

import io
from typing import Any

from doppeldata.profile_exports.contract_columns import ContractColumn
from doppeldata.profile_exports.contract_columns import ContractDocument
from doppeldata.profile_exports.contract_columns import ContractTable
from doppeldata.profile_exports.contract_columns import Share
from doppeldata.profile_exports.contract_columns import ceiled
from doppeldata.profile_exports.contract_columns import within_values
from doppeldata.profile_exports.type_names import hive_name
from doppeldata.profile_format import yaml

API_VERSION = 'v3.2.0'
CONTRACT_VERSION = '1.0.0'
# The facts a quality check carries at their share, so only the rest are named as left out.
QUALITY_FACTS = ('values', 'pattern', 'key')
# The classification a sensitivity is written as, the importer reading `restricted` and `confidential` as sensitive.
CLASSIFICATIONS = {'identifier': 'restricted', 'sensitive': 'confidential', 'quasi_identifier': 'confidential'}
NUMERIC_TYPES = ('integer', 'number')
TEMPORAL_TYPES = ('date', 'timestamp')


def odcs_contract(document: ContractDocument, examples: dict[str, dict[str, list[Any]]]) -> str:
    """The contract as YAML. `examples` holds each table's generated example values by column."""
    left_out = [note.note for table in document.tables for note in table.left_out() if note.fact not in QUALITY_FACTS]
    description = {'purpose': f'The contract of {document.name}, exported from its doppeldata profile, which holds no rows.'}
    if left_out:
        description['limitations'] = 'Left out, because the profile does not state it holds for every row: ' + '; '.join(left_out) + '.'
    contract: dict[str, Any] = {
        'apiVersion': API_VERSION,
        'kind': 'DataContract',
        'id': document.name,
        'name': document.name,
        'version': CONTRACT_VERSION,
        'status': 'draft',
        'description': description,
        'schema': [schema_object(table, examples.get(table.name, {})) for table in document.tables],
        'customProperties': [{'property': 'doppeldata.profile_sha256', 'value': document.profile_sha256}],
    }
    text = io.StringIO()
    yaml().dump(contract, text)
    return text.getvalue()


def schema_object(table: ContractTable, examples: dict[str, list[Any]]) -> dict[str, Any]:
    entry: dict[str, Any] = {'name': table.name, 'physicalName': table.name, 'logicalType': 'object', 'physicalType': 'table'}
    entry['properties'] = [table_property(table, column, examples.get(column.name)) for column in table.columns]
    # A foreign key of several columns is stated on the schema object, each side a list of its columns.
    references = [
        {
            'type': 'foreignKey',
            'from': [f'{table.name}.{column}' for column in key.columns],
            'to': [f'{key.to_table}.{column}' for column in key.to_columns],
        }
        for key in table.foreign_keys
        if len(key.columns) > 1
    ]
    if references:
        entry['relationships'] = references
    composite = [key for key in table.stated_keys() if len(key.columns) > 1 and not key.uniqueness.whole]
    quality = [duplicates(key.uniqueness, list(key.columns)) for key in composite]
    if quality:
        entry['quality'] = quality
    return entry


def table_property(table: ContractTable, column: ContractColumn, examples: list[Any] | None) -> dict[str, Any]:
    """A top-level column's property, with its key, partition, foreign key and examples."""
    prop = column_property(column)
    primary = table.primary_key
    if primary is not None and primary.uniqueness.whole and column.name in primary.columns:
        prop |= {'primaryKey': True, 'primaryKeyPosition': primary.columns.index(column.name) + 1}
    elif column.name in table.unique_columns():
        prop['unique'] = True
    if column.name in table.partition_keys:
        prop |= {'partitioned': True, 'partitionKeyPosition': table.partition_keys.index(column.name) + 1}
    references = [
        {'type': 'foreignKey', 'to': f'{key.to_table}.{key.to_columns[0]}'} for key in table.foreign_keys if key.columns == (column.name,)
    ]
    if references:
        prop['relationships'] = references
    quality = list(prop.pop('quality', []))
    for key in table.stated_keys():
        if key.columns == (column.name,) and not key.uniqueness.whole:
            quality.append(duplicates(key.uniqueness))
    if quality:
        prop['quality'] = quality
    if examples:
        prop['examples'] = examples
    return prop


def column_property(column: ContractColumn) -> dict[str, Any]:
    """A column's property: its types, what holds for every row, and a quality check of each share below every row."""
    prop: dict[str, Any] = {'name': column.name, 'logicalType': column.logical_type}
    physical = hive_type(column)
    if physical is not None:
        prop['physicalType'] = physical
    if column.required:
        prop['required'] = True
    options = type_options(column)
    if options:
        prop['logicalTypeOptions'] = options
    values = column.held_values()
    if values:
        prop['enum'] = [{'value': value} for value in values]
    if column.sensitivity in CLASSIFICATIONS:
        prop['classification'] = CLASSIFICATIONS[column.sensitivity]
    if column.semantic_type:
        prop['tags'] = [column.semantic_type]
    if column.logical_type == 'object':
        prop['properties'] = [column_property(field) for field in column.fields]
    if column.logical_type == 'array' and column.element is not None:
        prop['items'] = column_property(column.element)
    quality = quality_checks(column)
    if quality:
        prop['quality'] = quality
    return prop


def hive_type(column: ContractColumn) -> str | None:
    """The column's Hive type, nesting a struct's and a list's, else None where a part has no Hive name."""
    if column.logical_type == 'object':
        fields = [(field.name, hive_type(field)) for field in column.fields]
        if not fields or any(kind is None for _, kind in fields):
            return None
        return 'struct<' + ','.join(f'{name}:{kind}' for name, kind in fields) + '>'
    if column.logical_type == 'array':
        element = hive_type(column.element) if column.element else 'string'
        return f'array<{element}>' if element else None
    return hive_name(column.physical_type)


def type_options(column: ContractColumn) -> dict[str, Any]:
    options: dict[str, Any] = {}
    held = column.held_range()
    if held is not None and column.logical_type in (*NUMERIC_TYPES, *TEMPORAL_TYPES):
        low, high = held
        options |= {key: value for key, value in (('minimum', low), ('maximum', high)) if value is not None}
    if column.logical_type == 'number' and column.places is not None:
        options['multipleOf'] = float(f'1e-{column.places}')
    if column.logical_type == 'timestamp' and column.timezone:
        options |= {'timezone': True, 'defaultTimezone': column.timezone}
    lengths = column.held_lengths()
    if lengths is not None and column.logical_type == 'string':
        options |= {key: value for key, value in (('minLength', lengths[0]), ('maxLength', lengths[1])) if value is not None}
    if column.logical_type == 'array':
        options |= {key: value for key, value in (('minItems', column.min_items), ('maxItems', column.max_items)) if value is not None}
    if column.logical_type == 'string':
        pattern, form = column.held_pattern(), column.held_format()
        if pattern:
            options['pattern'] = pattern
        if form:
            options['format'] = form
    return options


def quality_checks(column: ContractColumn) -> list[dict[str, Any]]:
    """A check of each share below every row that the profile measured: the nulls, the values outside the listed ones,
    and the values not matching the pattern, each in percent of the values held."""
    checks = []
    if not column.completeness.whole and column.completeness.rows:
        checks.append(at_most({'metric': 'nullValues'}, column.completeness))
    if column.values and not column.held_values() and column.values_share and column.values_share.rows:
        outside = within_values(column.values_share, column.completeness)
        checks.append(at_most({'metric': 'invalidValues', 'arguments': {'validValues': list(column.values)}}, outside))
    if column.pattern and not column.held_pattern() and column.pattern_share and column.pattern_share.rows:
        unmatched = within_values(column.pattern_share, column.completeness)
        checks.append(at_most({'metric': 'invalidValues', 'arguments': {'pattern': column.pattern}}, unmatched))
    return checks


def at_most(check: dict[str, Any], share: Share) -> dict[str, Any]:
    """`check` allowing the rows outside `share`, in percent, up to its tolerance."""
    return check | {'mustBeLessOrEqualTo': percent_of(1 - share.least()), 'unit': 'percent'}


def duplicates(uniqueness: Share, columns: list[str] | None = None) -> dict[str, Any]:
    check: dict[str, Any] = {'metric': 'duplicateValues'}
    if columns:
        check['arguments'] = {'properties': columns}
    return at_most(check, uniqueness)


def percent_of(share: float) -> float:
    return ceiled(share * 100)
