"""Read each dataset write_every_fault_kind.py wrote with Spark, and print what Spark made of each fault kind, one line each.

    spark-submit read_with_spark.py OUT

CSV and NDJSON are read with the columns declared beside `_corrupt_record`, under PERMISSIVE. Spark names no line, so a
fault on a line is caught where that line's text comes back as a corrupt record, and a corrupt record no fault's line holds
is counted as unplaced. A dataset AGAIN_WITH names is read again with the option Spark offers against its fault, and a
file holding a byte order mark is read with its header inferred. The Parquet files are read as one table, then with
mergeSchema. It needs only PySpark, so it runs on any image's own Python.
"""

import collections
import json
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

SPARK_TYPES = {'int64': 'BIGINT', 'int32': 'INT', 'string': 'STRING', 'timestamp[us, tz=UTC]': 'TIMESTAMP'}
CORRUPT = '_corrupt_record'
# The option a dataset is read again with, beside the defaults: the one Spark offers against its fault.
AGAIN_WITH: dict[str, dict[str, Any]] = {
    'embedded_newline': {'multiLine': True},
    'unescaped_quote': {'unescapedQuoteHandling': 'RAISE_ERROR'},
    'header_drift': {'enforceSchema': False},
}


def spark_type(physical: str) -> str:
    return 'DECIMAL' + physical[len('decimal128') :] if physical.startswith('decimal128(') else SPARK_TYPES[physical]


def first_line(error: Exception) -> str:
    """The first line of the error, or of the deepest Java exception behind it, which py4j's own message leaves out."""
    cause = getattr(error, 'java_exception', None)
    while cause is not None and cause.getCause() is not None:
        cause = cause.getCause()
    text = (cause.toString() if cause is not None else str(error)).strip()
    return text.splitlines()[0][:200] if text else type(error).__name__


def data_files(dataset: Path, extension: str) -> list[Path]:
    return sorted(path for path in dataset.glob(f'*.{extension}') if not path.name.startswith(('_', '.')))


def faults_of(dataset: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in (dataset / '_faults.ndjson').read_text().splitlines()]


def physical_lines(path: Path) -> list[str]:
    """The file's lines as Spark decodes them, each without its line break."""
    return [line.rstrip(b'\r').decode('utf-8', errors='replace') for line in path.read_bytes().split(b'\n')]


def read_text(spark: SparkSession, format: str, paths: list[Path], schema: str, options: dict[str, Any]) -> Any:
    reader = spark.read.schema(f'{schema}, {CORRUPT} STRING').option('mode', 'PERMISSIVE').option('columnNameOfCorruptRecord', CORRUPT)
    reader = reader.options(**options)
    if format == 'csv':
        reader = reader.option('header', True).option('quote', '"').option('escape', '"').option('encoding', 'UTF-8')
        frame = reader.csv([str(path) for path in paths])
    else:
        frame = reader.json([str(path) for path in paths])
    # Spark refuses a query of raw text that reads only the corrupt record, unless the rows are cached first.
    return frame.withColumn('file', sf.element_at(sf.split(sf.input_file_name(), '/'), -1)).cache()


def text_outcome(spark: SparkSession, dataset: Path, format: str, schema: str, options: dict[str, Any] | None = None) -> list[str]:
    faults = faults_of(dataset)
    paths = data_files(dataset, format)
    written = sum(file['rows'] for file in json.loads((dataset / '_manifest.json').read_text())['files'])
    try:
        frame = read_text(spark, format, paths, schema, options or {})
        rows = frame.count()
        flagged = frame.where(sf.col(CORRUPT).isNotNull()).select('file', CORRUPT).collect()
    except Exception as error:
        return [f'{kind}: raised: {first_line(error)}' for kind in sorted({fault['kind'] for fault in faults})]
    corrupt: dict[str, collections.Counter[str]] = collections.defaultdict(collections.Counter)
    for record in flagged:
        corrupt[record['file']][record[CORRUPT]] += 1
    lines = {path.name: physical_lines(path) for path in paths}
    by_kind: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for fault in faults:
        by_kind[fault['kind']].append(fault)
    found = []
    for kind, placed in sorted(by_kind.items()):
        located = [fault for fault in placed if fault['line'] is not None]
        if not located:
            files = sorted({fault['file'] for fault in placed})
            in_files = sum(sum(corrupt[file].values()) for file in files)
            found.append(f'{kind}: placed in {len(files)} files, which hold {in_files} corrupt records')
            continue
        caught, missed = 0, collections.Counter[str]()
        for fault in located:
            text = lines[fault['file']][fault['line'] - 1]
            if corrupt[fault['file']][text] > 0:
                corrupt[fault['file']][text] -= 1
                caught += 1
            elif fault['column'] is not None:
                missed[fault['column']] += 1
        by_column = f' (missed in {", ".join(f"{column} {count}" for column, count in missed.most_common())})' if caught and missed else ''
        found.append(f'{kind}: caught {caught} of {len(located)} lines placed{by_column}')
    unplaced = sum(count for held in corrupt.values() for count in held.values())
    found.append(f'read {rows} rows of {written} written; {unplaced} corrupt records no fault line holds')
    return found


def header_outcome(spark: SparkSession, dataset: Path) -> list[str]:
    """The first column's name in each file a fault names, read with the header taken from the file and no schema."""
    files = sorted({fault['file'] for fault in faults_of(dataset)})
    names = [spark.read.option('header', True).csv(str(dataset / file)).columns[0] for file in files]
    return [f'with the header read, the first column of each file it is in is named {", ".join(map(repr, sorted(set(names))))}']


def parquet_outcome(spark: SparkSession, dataset: Path) -> list[str]:
    faults = faults_of(dataset)
    paths = [str(path) for path in data_files(dataset, 'parquet')]
    drifted = sorted({column for fault in faults for column in (fault['detail'].get('columns') or [])})
    outcomes = []
    for merge in (False, True):
        try:
            frame = spark.read.option('mergeSchema', merge).parquet(*paths)
            rows = len(frame.select(*drifted).collect())
            types = dict(frame.dtypes)
            held = ', '.join(f'{column} {types.get(column)}' for column in drifted)
            outcomes.append(f'{"with mergeSchema" if merge else "as one"}: read {rows} rows, {held}')
        except Exception as error:
            outcomes.append(f'{"with mergeSchema" if merge else "as one"}: raised: {first_line(error)}')
    kinds = collections.Counter(fault['kind'] for fault in faults)
    return [f'{kind}: placed in {count} files; {"; ".join(outcomes)}' for kind, count in kinds.items()]


def main(out: str) -> int:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    root = Path(out)
    schema = ', '.join(
        f'{column["name"]} {spark_type(column["physical_type"])}' for column in json.loads((root / 'columns.json').read_text())
    )
    reader = f'spark {spark.version}'
    for dataset in sorted(path for path in root.glob('*/*') if path.is_dir()):
        format = dataset.parent.name
        if format == 'parquet':
            found = parquet_outcome(spark, dataset)
        else:
            found = text_outcome(spark, dataset, format, schema)
            if dataset.name == 'bom':
                found += header_outcome(spark, dataset)
            for option, value in AGAIN_WITH.get(dataset.name, {}).items():
                found += [f'with {option} {value}, {line}' for line in text_outcome(spark, dataset, format, schema, {option: value})]
        for line in found:
            print(f'{reader} {format}/{dataset.name} {line}', flush=True)
        spark.catalog.clearCache()
    spark.stop()
    return 0


if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
