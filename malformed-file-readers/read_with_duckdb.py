"""Read each dataset write_every_fault_kind.py wrote with DuckDB, and print what DuckDB made of each fault kind, one line each.

    read_with_duckdb.py OUT

Each CSV file is read on its own with the columns declared, strictly, storing its rejects. A fault on a line is caught where
DuckDB rejects that line, and a rejected line no fault names is counted as unplaced. NDJSON is read with the columns
declared, and again ignoring errors where the first read raises. The Parquet files are read as one table, then by name.
"""

import bisect
import collections
import json
import re
import sys
from pathlib import Path
from typing import Any

import duckdb
from doppeldata.manifest import FAULTS_FILE_NAME

DUCKDB_TYPES = {'int64': 'BIGINT', 'int32': 'INTEGER', 'string': 'VARCHAR', 'timestamp[us, tz=UTC]': 'TIMESTAMPTZ'}
READER = f'duckdb {duckdb.__version__}'


def duckdb_type(physical: str) -> str:
    return 'DECIMAL' + physical.removeprefix('decimal128') if physical.startswith('decimal128(') else DUCKDB_TYPES[physical]


def first_line(error: Exception) -> str:
    return str(error).strip().splitlines()[0][:160]


def data_files(dataset: Path, extension: str) -> list[Path]:
    """The dataset's files, leaving out `_manifest.json` and anything else named as a sidecar is."""
    return sorted(path for path in dataset.glob(f'*.{extension}') if not path.name.startswith(('_', '.')))


def faults_of(dataset: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in (dataset / FAULTS_FILE_NAME).read_text().splitlines()]


def rejected_lines(path: Path, columns: dict[str, str]) -> dict[int, str]:
    """Each line DuckDB rejects in one CSV file, by its first error, in snake case."""
    with duckdb.connect() as connection:
        # Held in a table rather than fetched, because a TIMESTAMPTZ fetched into Python needs pytz.
        connection.execute(
            "create table rows as select * from read_csv($path, columns = $columns, header = true, delim = ',', quote = '\"', "
            "escape = '\"', auto_detect = false, strict_mode = true, store_rejects = true, encoding = 'utf-8')",
            {'path': str(path), 'columns': columns},
        )
        rows = connection.execute('select min(line_byte_position), min(error_type) from reject_errors group by line').fetchall()
    # DuckDB's `line` counts records, where the fault list counts physical lines, so each reject is placed by its byte.
    breaks = [match.start() for match in re.finditer(b'\n', path.read_bytes())]
    return {bisect.bisect_left(breaks, int(at)) + 1: str(error).lower().replace(' ', '_') for at, error in rows}


def read_csv(dataset: Path, columns: dict[str, str]) -> list[str]:
    faults = faults_of(dataset)
    rejects: dict[str, dict[int, str]] = {}
    raised: dict[str, str] = {}
    for path in data_files(dataset, 'csv'):
        try:
            rejects[path.name] = rejected_lines(path, columns)
        except duckdb.Error as error:
            raised[path.name] = first_line(error)
    lines = []
    by_kind: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for fault in faults:
        by_kind[fault['kind']].append(fault)
    placed_lines = {(fault['file'], fault['line']) for fault in faults if fault['line'] is not None}
    for kind, placed in by_kind.items():
        located = [fault for fault in placed if fault['line'] is not None]
        files = sorted({fault['file'] for fault in placed})
        if located:
            caught = [rejects.get(fault['file'], {}).get(fault['line']) for fault in located]
            types = collections.Counter(error for error in caught if error is not None)
            lines.append(f'caught {sum(types.values())} of {len(located)} lines placed ({described(types)})')
        else:
            lines.append(f'placed in {len(files)} files, {file_outcome(files, rejects, raised, placed_lines)}')
        lines[-1] = f'{kind}: {lines[-1]}'
    unplaced = sum(1 for file, held in rejects.items() for line in held if (file, line) not in placed_lines)
    raised_unfaulted = sorted(set(raised) - {fault['file'] for fault in faults})
    return [*lines, f'rejected {unplaced} lines no fault names; raised in {len(raised_unfaulted)} files no fault names']


def described(types: collections.Counter[str]) -> str:
    return ', '.join(f'{error} {count}' for error, count in types.most_common()) or 'none rejected'


def file_outcome(files: list[str], rejects: dict[str, dict[int, str]], raised: dict[str, str], placed: set[tuple[str, int]]) -> str:
    """What reading the files a file fault was placed in gave: the error any raised, and the lines the rest rejected that
    no row fault names."""
    errors = sorted({raised[file] for file in files if file in raised})
    rejected = collections.Counter(error for file in files for line, error in rejects.get(file, {}).items() if (file, line) not in placed)
    outcome = f'rejected {sum(rejected.values())} lines no row fault names ({described(rejected)})'
    return f'raised in {sum(file in raised for file in files)}: {"; ".join(errors)}; {outcome}' if errors else outcome


def read_ndjson(dataset: Path, columns: dict[str, str]) -> list[str]:
    faults = faults_of(dataset)
    written = sum(file['rows'] for file in json.loads((dataset / '_manifest.json').read_text())['files'])
    kinds = collections.Counter(fault['kind'] for fault in faults)
    paths = [str(path) for path in data_files(dataset, 'ndjson')]
    # Held in a table, so every column is cast, where a count alone would read none of them.
    query = (
        'create or replace table rows as select * from '
        "read_json($paths, columns = $columns, format = 'newline_delimited', ignore_errors = $ignore)"
    )
    with duckdb.connect() as connection:

        def rows_read() -> int:
            read = connection.execute('select count(*) from rows').fetchone()
            return read[0] if read else 0

        try:
            connection.execute(query, {'paths': paths, 'columns': columns, 'ignore': False})
            outcome = f'read {rows_read()} of {written} rows'
        except duckdb.Error as error:
            connection.execute(query, {'paths': paths, 'columns': columns, 'ignore': True})
            outcome = f'raised: {first_line(error)}; ignoring errors, read {rows_read()} of {written} rows'
    return [f'{kind}: {count} placed, {outcome}' for kind, count in kinds.items()]


def read_parquet(dataset: Path) -> list[str]:
    faults = faults_of(dataset)
    paths = [str(path) for path in data_files(dataset, 'parquet')]
    drifted = sorted({column for fault in faults for column in drifted_columns(fault)})
    outcomes = []
    for by_name in (False, True):
        with duckdb.connect() as connection:
            try:
                relation = connection.execute(f'describe select * from read_parquet($paths, union_by_name = {by_name})', {'paths': paths})
                types = {name: kind for name, kind, *_ in relation.fetchall()}
                count = connection.execute(
                    f'select count(*) from read_parquet($paths, union_by_name = {by_name})', {'paths': paths}
                ).fetchone()
                held = ', '.join(f'{column} {types.get(column)}' for column in drifted)
                outcomes.append(f'{"by name" if by_name else "as one"}: read {count[0] if count else 0} rows, {held}')
            except duckdb.Error as error:
                outcomes.append(f'{"by name" if by_name else "as one"}: raised: {first_line(error)}')
    kinds = collections.Counter(fault['kind'] for fault in faults)
    return [f'{kind}: placed in {count} files; {"; ".join(outcomes)}' for kind, count in kinds.items()]


def drifted_columns(fault: dict[str, Any]) -> list[str]:
    columns = fault['detail'].get('columns') or []
    return list(columns) if isinstance(columns, dict) else columns


def main(out: str) -> int:
    root = Path(out)
    columns = {column['name']: duckdb_type(column['physical_type']) for column in json.loads((root / 'columns.json').read_text())}
    for dataset in sorted(path for path in root.glob('*/*') if path.is_dir()):
        format = dataset.parent.name
        found = (
            read_csv(dataset, columns)
            if format == 'csv'
            else read_ndjson(dataset, columns)
            if format == 'ndjson'
            else read_parquet(dataset)
        )
        for line in found:
            print(f'{READER} {format}/{dataset.name} {line}', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
