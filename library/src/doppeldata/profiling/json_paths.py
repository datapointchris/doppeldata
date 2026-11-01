"""A JSON source's paths, and the profile's nested columns assembled from the flat columns each path is read into.

DuckDB's `json_group_structure` merges every record's structure into one tree. Each scalar path is read into a
text column beside a column of its JSON type, and each object or array path into its type alone. An array's
elements are a table of their own. So every scalar profiles as a top-level column would, and the assembly gives
each object its fields and their presence, and each array its length and element. Nothing here imports an engine.
"""

import dataclasses as dc
from typing import Any

from doppeldata import issue_codes
from doppeldata.errors import SourceError
from doppeldata.profiling.backend import HIDDEN_COLUMN_PREFIX
from doppeldata.profiling.backend import JsonNode
from doppeldata.profiling.recorded_counts import rate

ELEMENT = 'element'
# DuckDB's `json_type` for each JSON number and boolean, by the raw type it counts as.
JSON_RAW_TYPES = {
    'UBIGINT': 'integer',
    'BIGINT': 'integer',
    'HUGEINT': 'integer',
    'UHUGEINT': 'integer',
    'DOUBLE': 'decimal',
    'BOOLEAN': 'boolean',
}
JSON_STRING = 'VARCHAR'
JSON_NULL = 'NULL'
CONTAINER_TYPES = {'object': 'OBJECT', 'array': 'ARRAY'}
# The values a type column is counted against: a JSON null and each container type.
COUNTED_TYPES = (JSON_NULL, *CONTAINER_TYPES.values())


def type_column(column: str) -> str:
    return f'{HIDDEN_COLUMN_PREFIX}_json_type:{column}'


def length_column(column: str) -> str:
    return f'{HIDDEN_COLUMN_PREFIX}_json_length:{column}'


def tree_of(structure: Any, column: str = '', path: str = '') -> JsonNode:
    """The path tree of DuckDB's merged structure. A path whose type varies between records is a scalar of type `JSON`."""
    if isinstance(structure, dict):
        fields = tuple((key, tree_of(value, joined(column, key), joined(path, key))) for key, value in structure.items())
        return JsonNode(column, path, 'object', fields)
    if isinstance(structure, list):
        element = tree_of(structure[0] if structure else JSON_NULL, ELEMENT, joined(path, ELEMENT))
        return JsonNode(column, path, 'array', element=element)
    return JsonNode(column, path, 'scalar')


def record_tree(structure: Any) -> JsonNode:
    tree = tree_of(structure)
    if tree.kind != 'object':
        raise SourceError(issue_codes.DATA_UNREADABLE, 'every JSON record must be an object, whose keys are the columns')
    return tree


def joined(prefix: str, key: str) -> str:
    return f'{prefix}.{key}' if prefix else key


def depth(node: JsonNode) -> int:
    """The deepest nesting of objects and arrays under the node, counting the node as 1."""
    if node.kind == 'object':
        return 1 + max((depth(child) for _, child in node.fields), default=0)
    if node.kind == 'array' and node.element is not None:
        return 1 + depth(node.element)
    return 0


def nodes(root: JsonNode) -> list[JsonNode]:
    """Every node in one table, the root's descendants, stopping at each array, whose element is another table."""
    found = []
    for _, child in root.fields:
        found.append(child)
        if child.kind == 'object':
            found += nodes(child)
    return found


def table_nodes(root: JsonNode) -> list[JsonNode]:
    """Every node read into the table `root` heads: an element root itself, and every record or object field below it."""
    return ([root] if root.column else []) + nodes(root)


def arrays(root: JsonNode) -> list[JsonNode]:
    return [node for node in table_nodes(root) if node.kind == 'array']


@dc.dataclass(frozen=True)
class TypeCounts:
    """One path's rows in its table by its JSON type: missing, an explicit null, an object or an array."""

    missing: int
    null: int
    object: int
    array: int


@dc.dataclass(frozen=True)
class ArrayLengths:
    """One array path's empty arrays, and its length quantiles by name, each clear of the ends by k_min arrays."""

    empty: int
    quantiles: dict[str, int]


@dc.dataclass(frozen=True)
class ProfiledTable:
    """One table of a JSON source profiled: each scalar's entry by its column, each path's type counts, each array's
    lengths by its column, and each array's elements by its path.
    """

    rows: int
    entries: dict[str, dict[str, Any]]
    counts: dict[str, TypeCounts]
    lengths: dict[str, ArrayLengths]
    elements: dict[str, 'ProfiledTable']


def assembled(root: JsonNode, profiled: ProfiledTable) -> list[dict[str, Any]]:
    """The profile's columns, one per record key, each object with its fields and each array with its element."""
    return [entry_of(key, node, profiled, nested=False) for key, node in root.fields]


def entry_of(key: str, node: JsonNode, profiled: ProfiledTable, *, nested: bool) -> dict[str, Any]:
    """One path's column. A nested field's null rate is its share of the rows holding its key, and its presence is
    the share of its parent's object rows holding the key, as the generator reads both.
    """
    counts = profiled.counts[node.column]
    present = profiled.rows - counts.missing if nested else profiled.rows
    entry: dict[str, Any] = {'name': key}
    if node.path != key:
        entry['path'] = node.path
    if node.kind == 'scalar':
        scalar = profiled.entries[node.column]
        entry |= {name: value for name, value in scalar.items() if name != 'name'}
        if nested:
            # A field missing from an object is null in Arrow, so a field any object lacks is nullable.
            entry |= {'nullable': counts.null + counts.missing > 0, 'null_fraction': rate(counts.null, present)}
        return entry
    held = counts.object if node.kind == 'object' else counts.array
    nullable = held < present or (nested and counts.missing > 0)
    entry |= {'physical_type': '', 'logical_type': node.kind, 'nullable': nullable, 'null_fraction': rate(present - held, present)}
    if node.kind == 'object':
        fields = [entry_of(field, child, profiled, nested=True) for field, child in node.fields]
        presence = {field: rate(profiled.rows - profiled.counts[child.column].missing, counts.object) for field, child in node.fields}
        entry['physical_type'] = 'struct<' + ', '.join(f'{field["name"]}: {field["physical_type"]}' for field in fields) + '>'
        entry['struct'] = {'fields': fields, 'presence_fractions': presence}
    elif node.element is not None:
        elements = profiled.elements[node.path]
        element = entry_of(ELEMENT, node.element, elements, nested=False)
        lengths = profiled.lengths[node.column]
        listed: dict[str, Any] = {'length': lengths.quantiles} if lengths.quantiles else {}
        entry['physical_type'] = f'list<item: {element["physical_type"]}>'
        entry['list'] = listed | {'empty_fraction': rate(lengths.empty, counts.array), 'element': element}
    return entry
