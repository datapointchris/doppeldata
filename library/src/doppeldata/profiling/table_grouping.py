"""A directory or an S3 prefix grouped into tables the way an AWS Glue crawler groups one, before any engine reads it.

Each folder directly under the root that was named is a candidate table, and the files directly under it are one more,
named after the root. Folders whose schemas are similar are one table, its `key=value` folders Hive partitions.
Otherwise each cluster of similar folders is a table of its own, and the profile is a bundle.
"""

import contextlib
import dataclasses as dc
import importlib
import json
import random
from collections.abc import Iterator
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

from doppeldata.errors import SourceError
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import SourceFile
from doppeldata.profiling.backend import SourceFormat

# Folders are one table where each shares more than this share of the (name, type) pairs in its and the first folder's
# schemas together, a Glue crawler's 70 percent.
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
# The folder of the files directly under the root.
ROOT_FOLDER = ''

Schema = frozenset[tuple[str, str]]


def grouped(source: Source) -> list[Source]:
    """The tables `source` holds: itself where its folders are similar, or one source per cluster of similar folders.

    Only one directory or prefix named is grouped. Several paths named are one table, as a crawler's include path is.
    """
    if len(source.roots) != 1 or len(source.files) < 2 or source.format is None:
        return [source]
    root = source.roots[0]
    folders: dict[str, list[SourceFile]] = {}
    for file in source.files:
        parts = relative_parts(root, file.path)
        folders.setdefault(parts[0] if len(parts) > 1 else ROOT_FOLDER, []).append(file)
    if len(folders) < 2:
        return [source]
    schemas = {folder: folder_schema(files, source.format) for folder, files in folders.items()}
    clusters: list[list[str]] = []
    for folder in sorted(folders):
        home = next((cluster for cluster in clusters if similarity(schemas[cluster[0]], schemas[folder]) > SIMILAR), None)
        if home is None:
            clusters.append([folder])
        else:
            home.append(folder)
    if len(clusters) == 1:
        return [source]
    if len(clusters) > MOST_CLUSTERS:
        clusters = [[folder] for folder in sorted(folders)]
    tables = []
    for cluster in clusters:
        named = cluster[0] or source.name
        files = tuple(sorted((file for folder in cluster for file in folders[folder]), key=lambda file: file.path))
        # A table of one folder is read from that folder, so a reader finds its Hive keys below it.
        roots = (joined(root, cluster[0]),) if len(cluster) == 1 and cluster[0] else source.roots
        tables.append(dc.replace(source, files=files, name=named, roots=roots))
    names = [table.name for table in tables]
    if len(set(names)) < len(names):
        raise SourceError(f'{root} groups into tables named {", ".join(names)}, two of them alike; name each folder as its own source')
    return tables


def similarity(first: Schema | None, second: Schema | None) -> float:
    """The share of the two schemas' (name, type) pairs both hold. A schema that could not be read is like any other."""
    if first is None or second is None:
        return 1.0
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
        footers = importlib.import_module(PARQUET_FOOTERS_MODULE).read_footers(tuple(chosen), every=True)
        return frozenset().union(*(fields_of(footer.schema) for footer in footers))
    read = [schema for schema in (file_schema(file, source_format) for file in chosen) if schema is not None]
    return frozenset().union(*read) if read else None


def file_schema(file: SourceFile, source_format: SourceFormat) -> Schema | None:
    """A file's columns with their types where the format declares them, and their names alone in a text format."""
    match source_format:
        case SourceFormat.ARROW:
            return arrow_schema(file)
        case SourceFormat.CSV | SourceFormat.TSV:
            return delimited_schema(file, '\t' if source_format is SourceFormat.TSV else None)
        case _:
            return json_schema(file)


def fields_of(schema: Any) -> Schema:
    """An Arrow schema's (name, type) pairs."""
    return frozenset((field.name, str(field.type)) for field in schema)


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
    return frozenset((name, 'text') for name in names)


def json_schema(file: SourceFile) -> Schema | None:
    """The keys of the first records a JSON file's first block holds."""
    raw = importlib.import_module(FIRST_BLOCKS_MODULE).first_block(file.path).decode('utf-8', errors='replace')
    keys: set[str] = set()
    for record in json_records(raw):
        if isinstance(record, dict):
            keys.update(record)
    return frozenset((key, 'json') for key in keys) or None


def json_records(text: str) -> Iterator[Any]:
    stripped = text.lstrip()
    if stripped.startswith('['):
        with contextlib.suppress(ValueError):
            yield from json.loads(stripped)[:RECORDS_READ]
        return
    for line in text.splitlines()[:RECORDS_READ]:
        with contextlib.suppress(ValueError):
            yield json.loads(line)
