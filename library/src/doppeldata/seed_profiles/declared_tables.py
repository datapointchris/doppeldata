"""A table as a schema declares it, written as a seed: a profile marked `generated.engine: import`, holding no statistics."""

import dataclasses as dc
import datetime as dt
from collections.abc import Mapping
from typing import Any

from doppeldata.profile_format import FORMAT_VERSION
from doppeldata.profile_format import Profile
from doppeldata.profiling.table_profile import TOOL
from doppeldata.seed_profiles.declared_columns import DeclaredColumn
from doppeldata.seed_profiles.declared_columns import nested
from doppeldata.short_profiles import expanded
from doppeldata.short_profiles import validated


@dc.dataclass(frozen=True)
class PartitionKey:
    """A column the source's files are partitioned by, and how many values the catalog lists for it, where it lists them."""

    name: str
    physical_type: str
    count: int | None = None


@dc.dataclass(frozen=True)
class DeclaredTable:
    """One table's declared columns and keys, and what the schema says of its files, from the schema `imported_from` names.

    `source` holds the profile's `source` fields a catalog records, such as `format` and `csv`. A sole primary key
    column, or a column a unique key names alone, draws distinct values, and every primary key column draws no nulls.
    """

    name: str
    columns: tuple[DeclaredColumn, ...]
    imported_from: str
    primary_key: tuple[str, ...] = ()
    unique_keys: tuple[tuple[str, ...], ...] = ()
    partition_keys: tuple[PartitionKey, ...] = ()
    source: Mapping[str, Any] = dc.field(default_factory=dict)
    row_count: int | None = None
    duplicate_row_rate: float | None = None

    def profile(self) -> Profile:
        """The seed: the short form's expansion of the columns, under the table's `generated`, `source` and `dataset`."""
        distinct = {key[0] for key in (self.primary_key, *self.unique_keys) if len(key) == 1}
        columns = {
            column.name: nested(
                dc.replace(column, key=column.key or column.name in distinct, required=column.required or column.name in self.primary_key),
                column.name,
            )
            for column in self.columns
        }
        columns_only = expanded(dict(columns), None, self.name, self.imported_from)
        generated = imported_generated(self.imported_from)
        document: dict[str, Any] = {'profile_version': FORMAT_VERSION, 'kind': 'table', 'name': self.name, 'generated': generated}
        source = dict(self.source)
        if self.partition_keys:
            keys = [
                {'name': key.name, 'type': key.physical_type, **({'values': {'count': key.count}} if key.count is not None else {})}
                for key in self.partition_keys
            ]
            source['layout'] = {**(source.get('layout') or {}), 'partitioning': {'style': 'hive', 'keys': keys}}
        if source:
            document['source'] = source
        dataset: dict[str, Any] = {}
        if self.row_count is not None:
            dataset['row_count'] = self.row_count
        if self.duplicate_row_rate is not None:
            dataset['duplicate_row_rate'] = self.duplicate_row_rate
        keys_section: dict[str, Any] = {}
        if self.primary_key:
            keys_section['primary'] = {'columns': list(self.primary_key)}
        if self.unique_keys:
            keys_section['candidates'] = [{'columns': list(key)} for key in self.unique_keys]
        if keys_section:
            dataset['keys'] = keys_section
        if dataset:
            document['dataset'] = dataset
        document['columns'] = columns_only['columns']
        return validated(document, self.imported_from)


def imported_generated(imported_from: str) -> dict[str, Any]:
    """A seed's `generated` section: the tool, the time, `engine: import`, and the schema it was read from."""
    return {
        'tool': TOOL,
        'at': dt.datetime.now(dt.UTC).isoformat(timespec='seconds').replace('+00:00', 'Z'),
        'engine': 'import',
        'imported_from': imported_from,
    }
