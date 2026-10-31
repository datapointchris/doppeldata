"""Generate the eight-columns example through doppeldata.spark with 7 and with 40 partitions, and compare it with doppeldata run locally.

`check-spark-parity.sh` runs this through spark-submit inside each Spark image. `generate`'s DataFrame is collected as Arrow and
compared value for value with `doppeldata.stream`, each float by its bits. `write`'s files are compared by SHA-256 with the
manifest of a `doppeldata data generate` with the same arguments, and its `_faults.jsonl` byte for byte with the one beside
that manifest. Each column of the every-column-kind fixture then goes through
`mapInArrow` alone, and whether Spark gave every value back is compared with `spark_holds`. The orders-customers bundle then
goes through `generate` and `write`, each table compared as the example is, with a local bundle write's table manifests. Last,
`to_spark` builds DataFrames on the driver from the example's local table and from the fixture's columns Spark holds, the latter
also as Polars hands them over, each collected and compared by bits, and must refuse a table past `DRIVER_ROWS_AT_MOST` rows.
It prints one JSON object per check,
and exits 1 when any differs.

Given a scenario, `write` and the local run apply all of it, and `generate` and the stream its ops that write rows, because
`generate` writes no files. Each op draws from its own stream, so the rest generate the same values without them.
"""

import json
import pathlib
import sys
import time
from typing import Any

import doppeldata as sd
import pyarrow as pa
from doppeldata.manifest import FAULTS_FILE_NAME
from doppeldata.scenario_format import perturbation_facts
from doppeldata.spark import spark_holds
from pyspark.sql import SparkSession
from pyspark.sql.pandas.types import from_arrow_schema

ROWS = 10_000_000
SEED = 42
FILE_ROWS = 1_000_000
PARTITIONS = (7, 40)
# The bundle generated and written beside the eight-columns example, and the partitions each of its tables takes.
BUNDLE = 'orders-customers'
BUNDLE_PARTITIONS = 3
BUNDLE_FILE_ROWS = 100_000
# The rows of each table to_spark builds on the driver.
TO_SPARK_ROWS = 100_000
# A float compares by its bits, so a NaN equals itself and -0.0 differs from 0.0.
FLOAT_BITS = {pa.float32(): pa.int32(), pa.float64(): pa.int64()}


def arrow_of(frame: Any) -> pa.Table:
    """The DataFrame collected as Arrow: `toArrow` from Spark 4.0, and the private `_collect_as_arrow` before it."""
    if hasattr(frame, 'toArrow'):
        return frame.toArrow()
    return pa.Table.from_batches(frame._collect_as_arrow())


def same_values(got: pa.Array, want: pa.Array) -> bool:
    """Whether two arrays hold one type and the same values, each float compared by its bits."""
    if got.type != want.type:
        return False
    if want.type in FLOAT_BITS:
        got, want = got.view(FLOAT_BITS[want.type]), want.view(FLOAT_BITS[want.type])
    return bool(got.equals(want))


def first_difference(collected: pa.Table, expected: pa.RecordBatchReader) -> str | None:
    """Where the collected table first differs from the stream doppeldata generates, or None when every value is identical."""
    offset = 0
    for batch in expected:
        piece = collected.slice(offset, batch.num_rows)
        for name in batch.schema.names:
            got = piece.column(name).combine_chunks()
            want = batch.column(name)
            if got.type != want.type:
                return f'column {name} is {got.type} where doppeldata generates {want.type}'
            if not same_values(got, want):
                return f'column {name} differs in rows {offset} to {offset + batch.num_rows}'
        offset += batch.num_rows
    if offset != collected.num_rows:
        return f'Spark returned {collected.num_rows} rows where doppeldata generates {offset}'
    return None


def carried(spark: SparkSession, table: pa.Table, name: str) -> bool:
    """Whether column `name` of `table` comes back from `mapInArrow` with every value, in a type that casts back to its own."""
    column = table.select([name]).replace_schema_metadata(None)
    want = column.column(0).combine_chunks()
    try:
        schema = from_arrow_schema(column.schema, prefer_timestamp_ntz=True)
        batches = column.to_batches()
        frame = spark.range(1, numPartitions=1).mapInArrow(lambda _, held=batches: iter(held), schema)
        return same_values(arrow_of(frame).column(0).combine_chunks().cast(want.type), want)
    except Exception:
        return False


def type_disagreements(spark: SparkSession, fixture: str) -> list[str]:
    """Each fixture column whose type `spark_holds` judges otherwise than this Spark carries it."""
    table = pa.table(sd.generate(sd.Profile.load(fixture), rows=3000, seed=1))
    return [
        f'{field.name} ({field.type}): spark_holds says {spark_holds(field.type)}'
        for field in table.schema
        if carried(spark, table, field.name) != spark_holds(field.type)
    ]


def same_faults(local_manifest_path: str, written: str) -> bool:
    """Whether the write's `_faults.jsonl` holds the local run's bytes, or neither run wrote one."""
    ours, theirs = pathlib.Path(local_manifest_path).with_name(FAULTS_FILE_NAME), pathlib.Path(written) / FAULTS_FILE_NAME
    if not ours.exists() or not theirs.exists():
        return ours.exists() == theirs.exists()
    return ours.read_bytes() == theirs.read_bytes()


def report(check: dict[str, Any]) -> bool:
    print(json.dumps(check), flush=True)
    return check['identical']


def row_ops(scenario: sd.Scenario | None) -> sd.Scenario | None:
    """The scenario without the ops that shape files, which `generate` refuses."""
    if scenario is None:
        return None
    plain = scenario.to_plain()
    entries = zip(plain.get('perturbations') or [], perturbation_facts(plain), strict=True)
    return sd.Scenario(plain | {'perturbations': [entry for entry, facts in entries if not facts.shapes_files]})


def bundle_identical(spark: SparkSession, bundle_manifest_path: str, out: str) -> bool:
    """Whether the orders-customers bundle, through `generate` and `write` at `BUNDLE_PARTITIONS`, holds a local run's values
    in each table, and writes each table's files with the SHA-256 the local write beside `bundle_manifest_path` recorded."""
    bundle = sd.Bundle.example(BUNDLE)
    identical = True
    expected = sd.generate(bundle, seed=SEED)
    for name, frame in sd.spark.generate(spark, bundle, seed=SEED, partitions=BUNDLE_PARTITIONS).items():
        difference = first_difference(arrow_of(frame), pa.RecordBatchReader.from_stream(expected[name]))
        check = {'check': 'bundle generate', 'table': name, 'partitions': BUNDLE_PARTITIONS}
        identical &= report(check | {'identical': difference is None, 'difference': difference})
    manifest = sd.spark.write(spark, bundle, f'{out}/bundle', seed=SEED, file_rows=BUNDLE_FILE_ROWS, partitions=BUNDLE_PARTITIONS)
    root = pathlib.Path(bundle_manifest_path).parent
    for name, table in manifest.tables.items():
        local = {file['path']: file['sha256'] for file in json.loads((root / name / '_manifest.json').read_text())['files']}
        written = {file['path']: file['sha256'] for file in table.files}
        differ = sorted(path for path in local.keys() | written.keys() if local.get(path) != written.get(path))
        check = {'check': 'bundle write', 'table': name, 'files': len(written)}
        identical &= report(check | {'identical': not differ, 'differ': differ})
    return identical


def as_views(data_type: pa.DataType) -> pa.DataType:
    """`data_type` as Polars hands it over: each string a string_view and each list a large_list, inside a list or struct too."""
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return pa.string_view()
    if pa.types.is_list(data_type):
        return pa.large_list(data_type.value_field.with_type(as_views(data_type.value_type)))
    if pa.types.is_struct(data_type):
        return pa.struct([data_type.field(index).with_type(as_views(data_type.field(index).type)) for index in range(data_type.num_fields)])
    return data_type


def to_spark_identical(spark: SparkSession, fixture: str) -> bool:
    """Whether `to_spark` gives back every value of `TO_SPARK_ROWS` rows of the eight-columns example, under its own schema and
    under `struct_type`'s, and of each fixture column `spark_holds` admits, as the engine types it and as Polars hands it over.
    Each is collected as Arrow and compared by bits, a large_string as the string Spark returns. A table one row past
    `DRIVER_ROWS_AT_MOST` must be refused."""
    example = pa.table(sd.generate('eight-columns', rows=TO_SPARK_ROWS, seed=SEED))
    generated = pa.table(sd.generate(sd.Profile.load(fixture), rows=TO_SPARK_ROWS, seed=SEED))
    held = generated.select([field.name for field in generated.schema if spark_holds(field.type)])
    view_types = [as_views(field.type) for field in held.schema]
    try:
        views = held.cast(pa.schema([field.with_type(view) for field, view in zip(held.schema, view_types, strict=True)]))
    except pa.ArrowNotImplementedError:
        # pyarrow before 18 casts nothing to string_view, so the views are built from Python values.
        views = pa.table(
            [pa.array(held.column(index).to_pylist(), type=view) for index, view in enumerate(view_types)], names=held.column_names
        )
    cases = [
        ('eight-columns', example, example, None),
        ('eight-columns', example, example, sd.spark.struct_type('eight-columns')),
        (pathlib.Path(fixture).name, held, held, None),
        (f'{pathlib.Path(fixture).name} as views', views, held, None),
    ]
    identical = True
    for name, table, expected, schema in cases:
        collected = arrow_of(sd.spark.to_spark(spark, table, schema=schema)).cast(expected.schema)
        difference = first_difference(collected, expected.to_batches())
        check = {'check': 'to_spark', 'table': name, 'schema': 'struct_type' if schema else 'own', 'columns': table.num_columns}
        identical &= report(check | {'rows': collected.num_rows, 'identical': difference is None, 'difference': difference})
    oversized = sd.generate({'flag': sd.choice([True, False])}, rows=sd.spark.DRIVER_ROWS_AT_MOST + 1)
    try:
        sd.spark.to_spark(spark, oversized)
        refused = False
    except ValueError:
        refused = True
    return identical & report({'check': 'to_spark refuses', 'rows': sd.spark.DRIVER_ROWS_AT_MOST + 1, 'identical': refused})


def main(local_manifest_path: str, out: str, fixture: str, bundle_manifest_path: str, scenario_path: str = '') -> int:
    spark = SparkSession.builder.getOrCreate()
    profile = sd.Profile.example('eight-columns')
    scenario = sd.Scenario.load(scenario_path) if scenario_path else None
    rows_scenario = row_ops(scenario)
    local = {file['path']: file['sha256'] for file in json.loads(pathlib.Path(local_manifest_path).read_text())['files']}
    identical = True
    for partitions in PARTITIONS:
        started = time.monotonic()
        frame = sd.spark.generate(spark, profile, rows=ROWS, seed=SEED, partitions=partitions, scenario=rows_scenario)
        collected = arrow_of(frame)
        seconds = time.monotonic() - started
        expected = sd.stream(profile, rows=ROWS, seed=SEED, scenario=rows_scenario)
        difference = first_difference(collected, pa.RecordBatchReader.from_stream(expected))
        rows = collected.num_rows
        del collected
        check = {'check': 'generate', 'partitions': partitions, 'rows': rows, 'seconds': round(seconds, 1)}
        if scenario is not None:
            check['scenario'] = scenario.name
        identical &= report(check | {'identical': difference is None, 'difference': difference})
    for partitions in PARTITIONS:
        started = time.monotonic()
        manifest = sd.spark.write(
            spark,
            profile,
            f'{out}/direct-{partitions}',
            rows=ROWS,
            seed=SEED,
            file_rows=FILE_ROWS,
            partitions=partitions,
            scenario=scenario,
        )
        seconds = time.monotonic() - started
        written = {file['path']: file['sha256'] for file in manifest.files}
        differ = sorted(path for path in local.keys() | written.keys() if local.get(path) != written.get(path))
        if not same_faults(local_manifest_path, f'{out}/direct-{partitions}'):
            differ.append(FAULTS_FILE_NAME)
        check = {'check': 'write', 'partitions': partitions, 'files': len(written), 'seconds': round(seconds, 1)}
        identical &= report(check | {'identical': not differ, 'differ': differ})
    disagree = type_disagreements(spark, fixture)
    identical &= report({'check': 'types', 'fixture': pathlib.Path(fixture).name, 'identical': not disagree, 'disagree': disagree})
    identical &= bundle_identical(spark, bundle_manifest_path, out)
    identical &= to_spark_identical(spark, fixture)
    spark.stop()
    return 0 if identical else 1


if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
