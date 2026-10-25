"""A directory or an S3 prefix grouped into tables the way an AWS Glue crawler groups one, before any engine reads it.

Each folder directly under the root that was named is a candidate table, and the files directly under it are one more,
named after the root. Its `key=value` folders are Hive partitions of one candidate, whatever their schemas. Folders
sharing most of their column names are one table, and each cluster of such folders is a table of its own.
"""

import contextlib
import dataclasses as dc
import importlib
import json
import os
import random
import warnings
from collections.abc import Iterator
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

from doppeldata.errors import ProfilingWarning
from doppeldata.errors import SourceError
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import SourceFile
from doppeldata.profiling.backend import SourceFormat

# Folders are one table where each shares more than this share of the column names in its and the first folder's
# schemas together, a Glue crawler's 70 percent. Names alone, because both readers widen a column's type across files.
SIMILAR = 0.7
# Each folder is a table of its own, as a crawler makes one, where the folders fall into more clusters than this.
MOST_CLUSTERS = 5
# The files of each folder whose schemas are read, a seeded sample beyond that, because each one on S3 is a remote read.
FILES_READ = 16
FILES_SEED = 0
# A crawler reads a text file's first thousand records.
RECORDS_READ = 1000
# Imported by name, because each imports pyarrow, which a library-only install lacks.
DELIMITED_DIALECT_MODULE = 'doppeldata.profiling.delimited_dialect'
FIRST_BLOCKS_MODULE = 'doppeldata.profiling.first_blocks'
PARQUET_FOOTERS_MODULE = 'doppeldata.profiling.parquet_footers'
# The candidate of the files directly under the root, and the one of every `key=value` folder, both named after the root.
ROOT_FOLDER = ''
PARTITIONS = '='
# What a cluster's name drops from the end of its folders' common prefix, such as the year in `orders_2024`.
NAME_SUFFIX = '0123456789_-. '

# The column names a refusal lists for each table before it counts the rest.
NAMES_SHOWN = 8

Schema = frozenset[str]


@dc.dataclass(frozen=True)
class GroupedTable:
    """One table a source groups into: its source, the candidate folders it holds, and their column names where read."""

    source: Source
    folders: tuple[str, ...] = ()
    columns: Schema | None = None


def only_table(source: Source) -> Source:
    """`source` where it groups into one table, refused with a SourceError naming each table where it groups into more."""
    tables = grouped(source)
    if len(tables) == 1:
        return tables[0].source
    described = '; '.join(f'{table.source.name} ({described_folders(table)}: {described_columns(table)})' for table in tables)
    raise SourceError(
        f'{source.roots[0]} holds {len(tables)} tables: {described}. Profile every table with profile_tables() or '
        "`profile create --tables`, or name one table's folder"
    )


def described_folders(table: GroupedTable) -> str:
    labels = {ROOT_FOLDER: 'the files at the root', PARTITIONS: 'the key=value folders'}
    return ', '.join(labels.get(folder, f'{folder}/') for folder in table.folders)


def described_columns(table: GroupedTable) -> str:
    if table.columns is None:
        return 'columns unread'
    names = sorted(table.columns)
    more = f' and {len(names) - NAMES_SHOWN} more' if len(names) > NAMES_SHOWN else ''
    return ', '.join(names[:NAMES_SHOWN]) + more


def grouped(source: Source) -> list[GroupedTable]:
    """The tables `source` holds: itself where its folders are similar, or one table per cluster of similar folders.

    Only one directory or prefix named is grouped. Several paths named are one table, as a crawler's include path is.
    """
    if len(source.roots) != 1 or len(source.files) < 2 or source.format is None:
        return [GroupedTable(source)]
    root = source.roots[0]
    folders: dict[str, list[SourceFile]] = {}
    for file in source.files:
        parts = relative_parts(root, file.path)
        folder = ROOT_FOLDER if len(parts) < 2 else PARTITIONS if '=' in parts[0] else parts[0]
        folders.setdefault(folder, []).append(file)
    if len(folders) < 2:
        return [GroupedTable(source)]
    schemas = {folder: folder_schema(files, source.format) for folder, files in folders.items()}
    unread = sorted(folder for folder, schema in schemas.items() if schema is None)
    if len(unread) == len(schemas):
        return [GroupedTable(source)]
    if unread:
        named = ', '.join(source.name if folder in (ROOT_FOLDER, PARTITIONS) else folder for folder in unread)
        warnings.warn(f'{root}: no column names could be read in {named}, so each is a table of its own', ProfilingWarning, stacklevel=2)
    clusters: list[list[str]] = []
    for folder in sorted(folders):
        home = next((cluster for cluster in clusters if similarity(schemas[cluster[0]], schemas[folder]) > SIMILAR), None)
        if home is None:
            clusters.append([folder])
        else:
            home.append(folder)
    if len(clusters) == 1:
        return [GroupedTable(source)]
    if len(clusters) > MOST_CLUSTERS:
        clusters = [[folder] for folder in sorted(folders)]
    tables = []
    for cluster in clusters:
        files = tuple(sorted((file for folder in cluster for file in folders[folder]), key=lambda file: file.path))
        # A table of one named folder is read from that folder, so a reader finds its Hive keys below it.
        alone = len(cluster) == 1 and cluster[0] not in (ROOT_FOLDER, PARTITIONS)
        roots = (joined(root, cluster[0]),) if alone else source.roots
        table = dc.replace(source, files=files, name=cluster_name(cluster, source.name), roots=roots)
        read = [schema for schema in (schemas[folder] for folder in cluster) if schema is not None]
        tables.append(GroupedTable(table, tuple(cluster), frozenset().union(*read) if read else None))
    names = [table.source.name for table in tables]
    if len(set(names)) < len(names):
        raise SourceError(f'{root} groups into tables named {", ".join(names)}, two of them alike; name each folder as its own source')
    return tables


def cluster_name(cluster: list[str], root_name: str) -> str:
    """The folders' common prefix without a trailing number or separator, so a new `orders_2023/` keeps `orders`. The
    root's name where the cluster holds the root's own files or its Hive partitions, and the first folder's where the
    prefix leaves nothing."""
    if ROOT_FOLDER in cluster or PARTITIONS in cluster:
        return root_name
    return os.path.commonprefix(cluster).rstrip(NAME_SUFFIX) or cluster[0]


def similarity(first: Schema | None, second: Schema | None) -> float:
    """The share of the two schemas' column names both hold. A schema that could not be read is like no other."""
    if first is None or second is None:
        return 0.0
    union = first | second
    return len(first & second) / len(union) if union else 1.0


def relative_parts(root: str, path: str) -> tuple[str, ...]:
    if root.startswith('s3://'):
        prefix = urlparse(root).path.strip('/')
        key = urlparse(path).path.lstrip('/')
        return PurePosixPath(key.removeprefix(prefix).lstrip('/')).parts
    try:
        return Path(path).relative_to(Path(root).resolve()).parts
    except ValueError:
        # The lister resolves links, so a linked file can lie outside the root. It joins the root's own files.
        return (Path(path).name,)


def joined(root: str, folder: str) -> str:
    return f'{root.rstrip("/")}/{folder}' if root.startswith('s3://') else str(Path(root) / folder)


def folder_schema(files: list[SourceFile], source_format: SourceFormat) -> Schema | None:
    """The union of the sampled files' schemas, or None where no file's could be read."""
    chosen = files if len(files) <= FILES_READ else sorted(random.Random(FILES_SEED).sample(files, FILES_READ), key=lambda file: file.path)
    if source_format is SourceFormat.PARQUET:
        footers = importlib.import_module(PARQUET_FOOTERS_MODULE).read_footers(tuple(chosen))
        return frozenset().union(*(fields_of(footer.schema) for footer in footers))
    read = [schema for schema in (file_schema(file, source_format) for file in chosen) if schema is not None]
    return frozenset().union(*read) if read else None


def file_schema(file: SourceFile, source_format: SourceFormat) -> Schema | None:
    """A file's column names, or None where none could be read."""
    match source_format:
        case SourceFormat.ARROW:
            return arrow_schema(file)
        case SourceFormat.CSV | SourceFormat.TSV:
            return delimited_schema(file, '\t' if source_format is SourceFormat.TSV else None)
        case _:
            return json_schema(file)


def fields_of(schema: Any) -> Schema:
    return frozenset(field.name for field in schema)


def arrow_schema(file: SourceFile) -> Schema | None:
    """An Arrow IPC file's schema. One on S3 is not read, because its schema sits behind its first batch."""
    if file.path.startswith('s3://'):
        return None
    ipc = importlib.import_module('pyarrow.ipc')
    try:
        schema = ipc.open_file(file.path).schema
    except OSError:
        schema = ipc.open_stream(file.path).schema
    return fields_of(schema)


def delimited_schema(file: SourceFile, delimiter: str | None) -> Schema | None:
    """A delimited file's column names, from its header as Glue decides one, or numbered as Glue numbers them."""
    dialect = importlib.import_module(DELIMITED_DIALECT_MODULE)
    raw = importlib.import_module(FIRST_BLOCKS_MODULE).first_block(file.path)
    if not raw.strip():
        return None
    text = dialect.block_text(raw, 'utf-8')
    first_line = text.splitlines()[0] if text else ''
    separator = delimiter or max(dialect.GLUE_DELIMITERS, key=first_line.count)
    records = dialect.split_records(text, separator, '"', '"', limit=RECORDS_READ)
    if not records:
        return None
    header = dialect.glue_header(records)
    names = [field.text for field in records[0]] if header is not False else [f'col{index}' for index in range(len(records[0]))]
    return frozenset(names)


def json_schema(file: SourceFile) -> Schema | None:
    """The keys of the first records a JSON file's first block holds."""
    raw = importlib.import_module(FIRST_BLOCKS_MODULE).first_block(file.path).decode('utf-8', errors='replace')
    keys: set[str] = set()
    for record in json_records(raw):
        if isinstance(record, dict):
            keys.update(record)
    return frozenset(keys) or None


def json_records(text: str) -> Iterator[Any]:
    """The first records of a JSON array or of one value a line. An array is read a record at a time, because the block
    usually ends inside it."""
    stripped = text.lstrip()
    if stripped.startswith('['):
        decoder, at = json.JSONDecoder(), 1
        for _ in range(RECORDS_READ):
            while at < len(stripped) and stripped[at] in ', \t\r\n':
                at += 1
            try:
                record, at = decoder.raw_decode(stripped, at)
            except ValueError:
                return
            yield record
        return
    for line in text.splitlines()[:RECORDS_READ]:
        with contextlib.suppress(ValueError):
            yield json.loads(line)
