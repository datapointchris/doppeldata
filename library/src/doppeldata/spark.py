"""Synthetic datasets generated inside Spark executors: as a DataFrame, or as the files `doppeldata.write` writes.

Each executor imports the same doppeldata the driver runs and builds the engine's plan from the same arguments, so every block it
makes is the block a local run makes, whatever the partitioning. A bundle's table is planned from the whole bundle, so each
of its blocks is a pure function of its number too. This module imports pyspark and pyarrow, and doppeldata declares neither,
so a job that never touches Spark carries neither.
"""

import dataclasses as dc
import functools
import json
import os
import re
import time
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Sequence
from typing import Any
from typing import overload

from doppeldata.errors import PackagesMissing

try:
    import pyarrow as pa
    import pyspark
    import pyspark.sql.types as st
    from pyspark.sql import DataFrame
    from pyspark.sql import SparkSession
    from pyspark.sql.pandas.types import from_arrow_schema
except ImportError as error:
    missing = [(error.name or 'pyspark').split('.')[0]]
    raise PackagesMissing(
        'spark', missing, doing='doppeldata.spark', installed_by=', or run it inside PySpark 3.5 or later with pyarrow 15 or later'
    ) from error

from doppeldata import _core
from doppeldata import issue_codes
from doppeldata.bundle_generation import BundleArguments
from doppeldata.bundle_generation import BundleRun
from doppeldata.bundle_generation import bundle_arguments
from doppeldata.bundle_generation import bundle_written
from doppeldata.bundle_generation import prepared_bundle_write
from doppeldata.bundle_generation import run_of
from doppeldata.bundle_generation import table_scenarios
from doppeldata.dataset_generation import EngineArguments
from doppeldata.dataset_generation import WriteChoices
from doppeldata.dataset_generation import engine_arguments
from doppeldata.dataset_generation import generation_source
from doppeldata.dataset_generation import prepared_write
from doppeldata.dataset_generation import rows_only
from doppeldata.dataset_generation import s3_store_of
from doppeldata.errors import IncompatibleInstall
from doppeldata.errors import InvalidArgument
from doppeldata.errors import StoppedWrite
from doppeldata.errors import UnsupportedArgument
from doppeldata.errors import WriteFailed
from doppeldata.issue_codes import Issue
from doppeldata.manifest import BundleManifest
from doppeldata.manifest import Manifest
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import pointer
from doppeldata.profile_sources import ProfileSource
from doppeldata.scenario_format import InvalidScenario
from doppeldata.scenario_format import Scenario
from doppeldata.scenario_format import composed
from doppeldata.scenario_format import perturbation_facts

# The engine blocks each Spark task generates in turn. The chunk is a unit of work and never of values, so its size moves no value.
CHUNK_BLOCKS = 16

# The most rows to_spark builds a DataFrame of on the driver. Past it, generate makes the rows on the executors.
DRIVER_ROWS_AT_MOST = 10_000_000

# Arrow types whose arrays Spark reads through mapInArrow with every value kept, besides a timestamp in microseconds, a list and a
# struct of them, on the Spark releases docs/generating-inside-spark.md names. A large_string comes back as a string. The parity
# check fails in an image that carries any column of every-column-kind.profile.yaml otherwise than this says.
SPARK_HOLDS = (
    pa.types.is_boolean,
    pa.types.is_signed_integer,
    pa.types.is_float32,
    pa.types.is_float64,
    pa.types.is_string,
    pa.types.is_large_string,
    pa.types.is_date32,
    pa.types.is_decimal128,
)
# pyarrow 16 added string_view, so an older pyarrow holds none.
STRING_VIEW = pa.string_view() if hasattr(pa, 'string_view') else None

# A write task's StoppedWrite reaches the driver inside the error Spark raises there, as this marker, its code and kind as JSON,
# and its message.
TASK_STOPPED = 'doppeldata write task stopped'
TASK_STOPPED_LINE = re.compile(re.escape(TASK_STOPPED) + r' (\{[^}\n]*\}) ([^\n]*)')
# The last line of a Python traceback inside a Spark error, such as `RuntimeError: ...`, skipping the JVM's own exceptions.
PYTHON_ERROR_LINE = re.compile(r'^(?!org\.|java\.|scala\.|py4j\.)([\w.]+(?:Error|Exception|Interrupt)): (.+)$', re.MULTILINE)


@dc.dataclass(frozen=True)
class BundleTableArguments:
    """A bundle's engine arguments and the name of one of its tables, which an executor plans that table from."""

    bundle: BundleArguments
    table: str

    def plan(self) -> '_core.Plan':
        return dict(built_bundle_plan(self.bundle).tables())[self.table]


@functools.lru_cache(maxsize=4)
def built_bundle_plan(arguments: BundleArguments) -> '_core.BundlePlan':
    """The engine's bundle plan, built once per executor process for every table of it its tasks generate."""
    return arguments.plan()


@dc.dataclass(frozen=True)
class ExecutorPlan:
    """The engine's arguments as the driver resolved them, and its release and stream version, which an executor's must match."""

    arguments: EngineArguments | BundleTableArguments
    engine_version: str
    stream_version: int

    def plan(self) -> '_core.Plan':
        if (self.engine_version, self.stream_version) != (_core.VERSION, _core.STREAM_VERSION):
            message = (
                f'this executor runs doppeldata {_core.VERSION} at stream version {_core.STREAM_VERSION}, and the driver '
                f'{self.engine_version} at stream version {self.stream_version}; install one build on both, because a dataset is '
                'generated by the build that plans it'
            )
            raise IncompatibleInstall(issue_codes.BUILDS_DIFFER, message, value=_core.VERSION, valid=[self.engine_version])
        return built_plan(self.arguments)


@functools.lru_cache(maxsize=4)
def built_plan(arguments: EngineArguments | BundleTableArguments) -> '_core.Plan':
    """The engine's plan, built once per executor process for each dataset its tasks generate."""
    return arguments.plan()


@dc.dataclass(frozen=True)
class ChunkBatches:
    """What each Spark task runs for `generate`: the batches of every chunk whose number its partition holds, in order."""

    executor_plan: ExecutorPlan

    def __call__(self, batches: Iterator[Any]) -> Iterator[Any]:
        engine = self.executor_plan.plan()
        for batch in batches:
            for chunk in batch.column(0).to_pylist():
                start = chunk * CHUNK_BLOCKS
                stop = min(start + CHUNK_BLOCKS, engine.block_count)
                yield from pa.RecordBatchReader.from_stream(engine.stream_blocks(start, stop, threads=1))


@dc.dataclass(frozen=True)
class RangeWrites:
    """What each Spark task runs for `write`: the objects of every range whose number its partition holds, yielding one row that
    holds what `write_ranges` returned, untouched, for `commit` to read."""

    executor_plan: ExecutorPlan
    choices_json: str
    destination: str
    bucket: str | None

    def __call__(self, batches: Iterator[Any]) -> Iterator[Any]:
        ranges = [number for batch in batches for number in batch.column(0).to_pylist()]
        if not ranges:
            return
        engine = self.executor_plan.plan()
        options = engine.write_options(self.choices_json)
        try:
            written = engine.write_ranges(options, self.destination, ranges, s3_store=s3_store_of(self.destination, self.bucket))
        except StoppedWrite as stopped:
            raise RuntimeError(f'{TASK_STOPPED} {json.dumps({"code": stopped.code})} {stopped.message}') from stopped
        yield pa.record_batch([pa.array([written], pa.string())], names=['written'])


def executor_plan(profile: ProfileSource, rows: int | None, seed: int, scenario: Scenario | None) -> tuple[ExecutorPlan, '_core.Plan']:
    """What executors build the plan from, and the plan built on the driver, which refuses an ungenerable profile before a task runs."""
    planned = ExecutorPlan(engine_arguments(profile, rows, seed, scenario), _core.VERSION, _core.STREAM_VERSION)
    return planned, planned.plan()


def executors_destination(spark: SparkSession, scheme: str, destination: str) -> str:
    """The destination as every task writes to it, which must be the driver's. A local directory is refused unless this process's
    Spark master is local, and reaches the tasks as an absolute path."""
    if scheme != 'local':
        return destination
    connect = type(spark).__module__.startswith('pyspark.sql.connect')
    if connect or not spark.sparkContext.master.startswith('local'):
        message = (
            f"{destination} is a directory on the driver, which Spark's executors share only under a local master in this "
            'process; write to an s3:// URL, such as "s3://bucket/prefix/"'
        )
        raise WriteFailed(issue_codes.DESTINATION_NOT_SHARED, message)
    return os.path.abspath(destination)


def job_failure(error: Exception, destination: str, commit: Callable[[list[str]], Manifest]) -> WriteFailed:
    """What a failed Spark job raises once the partial manifest is stored: the code of the task's StoppedWrite that ended it,
    or SPARK_JOB_FAILED's. A failure to store the partial manifest is raised in its place."""
    recorded: WriteFailed | None = None
    try:
        commit([])
    except WriteFailed as stored:
        if stored.code != issue_codes.RANGES_UNRECORDED:
            return stored
        recorded = stored
    text = str(error)
    code = issue_codes.SPARK_JOB_FAILED
    if stopped := TASK_STOPPED_LINE.search(text):
        code, cause = json.loads(stopped.group(1))['code'], stopped.group(2)
    elif python_errors := PYTHON_ERROR_LINE.findall(text):
        cause = ': '.join(python_errors[-1])
    else:
        cause = next((line.strip() for line in text.splitlines() if line.strip()), type(error).__name__)
    message = (
        f'the Spark job writing {destination} failed: {cause}. The partial manifest records no object, because the driver learns '
        "of a task's objects only when the whole job returns. Rerun with replace=True, which deletes every part this run stored"
    )
    return WriteFailed(
        code,
        message,
        manifest_json=json.dumps(recorded.manifest.document) if recorded and recorded.manifest else None,
        manifest_stored=bool(recorded and recorded.manifest_stored),
    )


def checked_partitions(partitions: int | None, units: int) -> int:
    if partitions is not None and partitions < 1:
        message = f'partitions must be 1 or more, not {partitions}'
        raise InvalidArgument(issue_codes.INVALID_PARTITIONS, message, field='partitions', value=partitions)
    return partitions or max(units, 1)


def engine_schema(engine: '_core.Plan') -> pa.Schema:
    """The Arrow schema of the engine's batches."""
    return pa.RecordBatchReader.from_stream(engine.stream_blocks(0, 0, threads=1)).schema


def arrow_struct_type(schema: pa.Schema) -> Any:
    """The Spark schema of batches holding `schema`. A column whose values Spark would not hold exactly is refused, naming it."""
    for field in schema:
        if not spark_holds(field.type):
            message = (
                f'column {field.name!r} is {field.type}, which Spark cannot hold exactly; set its physical_type in the profile to a '
                'type Spark holds, such as int64 for an unsigned integer, timestamp[us] for a timestamp, date32 for a date64, or '
                'list for a large_list'
            )
            raise InvalidArgument(issue_codes.UNSUPPORTED_COLUMN, message, field=field.name, value=str(field.type))
    return from_arrow_schema(schema, prefer_timestamp_ntz=True)


def spark_readable(data_type: Any) -> Any:
    """`data_type` with each type Spark reads no Arrow of replaced by one holding the same values that it reads: a string_view
    by a large_string, and a large_list by a list, inside a list or struct too. Polars hands a table over in both."""
    if STRING_VIEW is not None and data_type == STRING_VIEW:
        return pa.large_string()
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return pa.list_(data_type.value_field.with_type(spark_readable(data_type.value_type)))
    if pa.types.is_struct(data_type):
        return pa.struct(
            [data_type.field(index).with_type(spark_readable(data_type.field(index).type)) for index in range(data_type.num_fields)]
        )
    return data_type


def readable_table(arrow: pa.Table) -> pa.Table:
    """`arrow` with each column `spark_readable` retypes cast to that type. pyarrow before 18 casts no string_view, so there a
    column holding one is rebuilt from its Python values."""
    readable = pa.schema([field.with_type(spark_readable(field.type)) for field in arrow.schema], metadata=arrow.schema.metadata)
    if readable == arrow.schema:
        return arrow
    columns = []
    for column, field in zip(arrow.columns, readable, strict=True):
        try:
            columns.append(column.cast(field.type))
        except pa.ArrowNotImplementedError:
            columns.append(pa.chunked_array([pa.array(chunk.to_pylist(), type=field.type) for chunk in column.chunks], type=field.type))
    return pa.Table.from_arrays(columns, schema=readable)


def spark_holds(data_type: Any) -> bool:
    if pa.types.is_timestamp(data_type):
        return bool(data_type.unit == 'us')
    if pa.types.is_list(data_type):
        return spark_holds(data_type.value_type)
    if pa.types.is_struct(data_type):
        return all(spark_holds(data_type.field(index).type) for index in range(data_type.num_fields))
    return any(holds(data_type) for holds in SPARK_HOLDS)


def one_process_scenario(scenario: Scenario | Sequence[Scenario] | None) -> Scenario | None:
    """The scenario `scenario=` names, refused where it holds an op ordering the whole table, such as `cdc`, which Spark's
    executors cannot write."""
    applied = composed(scenario)
    if applied is None:
        return None
    plain = applied.to_plain()
    issues = [
        Issue(
            issue_codes.OP_NEEDS_ONE_PROCESS,
            pointer(('perturbations', index)),
            f'{entry["op"]} orders the whole table, so one process writes the dataset; write it with doppeldata.write',
        )
        for index, (entry, facts) in enumerate(zip(plain.get('perturbations') or [], perturbation_facts(plain), strict=True))
        if facts.needs_one_process
    ]
    if issues:
        raise InvalidScenario(applied.source, issues)
    return applied


def struct_type(profile: ProfileSource, *, scenario: Scenario | Sequence[Scenario] | None = None) -> 'st.StructType':
    """The Spark schema of the DataFrame `generate` returns for `profile` under `scenario`.

    A timestamp with no time zone is a TimestampNTZType, and one with a zone a TimestampType. A column of a type `spark_holds`
    refuses, which Spark cannot carry with every value kept, raises InvalidArgument naming it.
    """
    if isinstance(profile, Bundle):
        message = 'a schema describes one table, and a bundle holds several; generate the bundle, whose DataFrames each hold one'
        raise UnsupportedArgument(issue_codes.BUNDLE_GIVEN, message, field='profile')
    applied = one_process_scenario(scenario)
    _, engine = executor_plan(profile, 0, 0, rows_only(applied, 'spark.generate', 'doppeldata.spark.write'))
    return arrow_struct_type(engine_schema(engine))


def to_spark(spark: SparkSession, table: Any, *, schema: 'st.StructType | None' = None) -> DataFrame:
    """`table` as a DataFrame built on the driver: any Arrow table, such as `doppeldata.sample` or `doppeldata.generate` returns.

    `schema` is the DataFrame's Spark schema, such as `struct_type(profile)` gives. Left None, it is the table's own Arrow schema
    as `struct_type` maps it. So no type is inferred, and a column Spark cannot hold exactly raises InvalidArgument naming it. A
    string_view column is read as a large_string, and a large_list as a list, which hold the same values, so a Polars
    DataFrame goes through.
    PySpark 4, and a Spark Connect session, build the DataFrame from the Arrow table. A classic PySpark 3.5 session takes no
    Arrow table, so its batches go to the JVM the way PySpark 4's do, and both give the same DataFrame.

    A table over `DRIVER_ROWS_AT_MOST` rows raises InvalidArgument naming `doppeldata.spark.generate`, which makes the rows on
    the executors, because a DataFrame that large should never pass through the driver.
    """
    arrow = pa.table(table)
    if arrow.num_rows > DRIVER_ROWS_AT_MOST:
        message = (
            f'the table holds {arrow.num_rows:,} rows, past the {DRIVER_ROWS_AT_MOST:,} to_spark builds on the driver; '
            "generate it with doppeldata.spark.generate, whose rows Spark's executors make"
        )
        raise InvalidArgument(issue_codes.TOO_MANY_ROWS_FOR_DRIVER, message, field='table', value=arrow.num_rows)
    arrow = readable_table(arrow)
    if schema is None:
        schema = arrow_struct_type(arrow.schema)
    connect = type(spark).__module__.startswith('pyspark.sql.connect')
    if connect or int(pyspark.__version__.split('.')[0]) >= 4:
        return spark.createDataFrame(arrow, schema=schema)
    return classic_arrow_frame(spark, arrow, schema)


def classic_arrow_frame(spark: SparkSession, arrow: pa.Table, schema: 'st.StructType') -> DataFrame:
    """`arrow` as a DataFrame of `schema` on a classic PySpark 3.5 session, by the calls PySpark 4's
    `createDataFrame(pyarrow.Table)` makes. Each batch streams to the JVM, which reads it into rows of `schema`.

    PySpark 3.5 makes the same calls for a pandas DataFrame, after building its Arrow batches from the pandas columns. Built
    from Arrow-backed columns, those batches hold chunked arrays, which 3.5's serializer refuses. So the table's own batches
    are sent instead, cast to the Arrow schema Spark reads `schema` as. A build lacking one of these calls, or taking other
    arguments to one, is refused with IncompatibleInstall naming the PySpark version, rather than sent through pandas, which turns a
    nullable integer into a float and a NaN into a null. A JVM error, or a lost connection to the JVM, is raised as it came.
    """
    from py4j.protocol import Py4JError
    from py4j.protocol import Py4JJavaError
    from py4j.protocol import Py4JNetworkError
    from pyspark.sql.pandas.serializers import ArrowStreamSerializer
    from pyspark.sql.pandas.types import to_arrow_schema

    def lacking(call: str) -> IncompatibleInstall:
        message = f'PySpark {pyspark.__version__} has no {call}, which to_spark builds a classic 3.5 DataFrame from Arrow with'
        return IncompatibleInstall(issue_codes.PYSPARK_UNSUPPORTED, message, value=pyspark.__version__)

    for name in ('_sc', '_jvm', '_jsparkSession', '_jconf'):
        if getattr(spark, name, None) is None:
            raise lacking(f'SparkSession.{name}')
    if not hasattr(spark._sc, '_serialize_to_jvm'):
        raise lacking('SparkContext._serialize_to_jvm')
    jvm, session = spark._jvm, spark._jsparkSession
    try:
        batches = arrow.cast(to_arrow_schema(schema)).to_batches(max_chunksize=spark._jconf.arrowMaxRecordsPerBatch())
        rows = spark._sc._serialize_to_jvm(
            batches,
            ArrowStreamSerializer(),
            lambda path: jvm.PythonSQLUtils.readArrowStreamFromFile(path),
            lambda: jvm.ArrowIteratorServer(),
        )
        frame = jvm.PythonSQLUtils.toDataFrame(rows, schema.json(), session)
    except (Py4JJavaError, Py4JNetworkError, pa.ArrowException):
        raise
    except TypeError as error:
        message = (
            f'PySpark {pyspark.__version__} takes other arguments to a call to_spark builds a classic 3.5 DataFrame from Arrow '
            f'with: {error}'
        )
        raise IncompatibleInstall(issue_codes.PYSPARK_UNSUPPORTED, message, value=pyspark.__version__) from error
    except Py4JError as error:
        raise lacking(f'JVM call it needs: {str(error).splitlines()[0]}') from error
    return DataFrame(frame, spark)


def chunk_frame(spark: SparkSession, planned: ExecutorPlan, engine: '_core.Plan', partitions: int | None) -> DataFrame:
    """The DataFrame whose tasks generate `engine`'s blocks, `CHUNK_BLOCKS` at a time, from the arguments `planned` holds."""
    schema = arrow_struct_type(engine_schema(engine))
    chunks = -(-engine.block_count // CHUNK_BLOCKS)
    numbers = spark.range(chunks, numPartitions=checked_partitions(partitions, chunks))
    return numbers.mapInArrow(ChunkBatches(planned), schema)


@overload
def generate(
    spark: SparkSession,
    profile: Bundle,
    *,
    rows: None = None,
    seed: int | None = None,
    partitions: int | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
    scale: float | None = None,
) -> dict[str, DataFrame]: ...


@overload
def generate(
    spark: SparkSession,
    profile: ProfileSource,
    *,
    rows: int | None = None,
    seed: int | None = None,
    partitions: int | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
    scale: None = None,
) -> DataFrame: ...


def generate(
    spark: SparkSession,
    profile: ProfileSource | Bundle,
    *,
    rows: int | None = None,
    seed: int | None = None,
    partitions: int | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
    scale: float | None = None,
) -> DataFrame | dict[str, DataFrame]:
    """The dataset as a DataFrame whose rows Spark's executors generate, with the values `doppeldata.generate` returns, in its order.

    Each task generates whole chunks of `CHUNK_BLOCKS` engine blocks through `mapInArrow`, so `partitions`, one per chunk when
    None, changes which task makes a row and never its value. Write it with any Spark writer, partitioned as the run needs.
    Every executor must have the driver's doppeldata release installed. `profile`, `rows`, `seed` and `scenario` are taken as
    `doppeldata.generate` takes them.

    A bundle returns a dict of each table's name to its DataFrame, parents first, at `scale` as `doppeldata.generate` takes it.
    Each table's tasks plan the whole bundle once per executor, and draw each foreign key from its parent's keys by formula,
    so no table reads another.
    """
    run = run_of(profile, rows, seed, scale)
    if isinstance(run, BundleRun):
        scenarios = table_scenarios(scenario, run.bundle)
        for each in scenarios.values():
            rows_only(one_process_scenario(each), 'spark.generate', 'doppeldata.spark.write')
        arguments = bundle_arguments(run.bundle, run.seed, run.scale, scenarios)
        return {
            name: chunk_frame(
                spark, ExecutorPlan(BundleTableArguments(arguments, name), _core.VERSION, _core.STREAM_VERSION), engine, partitions
            )
            for name, engine in arguments.plan().tables()
        }
    generation = generation_source(run.profile, run.rows, run.seed, scenario)
    applied = rows_only(one_process_scenario(generation.scenario), 'spark.generate', 'doppeldata.spark.write')
    planned, engine = executor_plan(generation.profile, generation.rows, generation.seed, applied)
    generation.checked(engine)
    return chunk_frame(spark, planned, engine, partitions)


@overload
def write(
    spark: SparkSession,
    profile: Bundle,
    out: str | os.PathLike[str],
    *,
    rows: None = None,
    seed: int | None = None,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: None = None,
    partition_style: None = None,
    writer: dict[str, Any] | None = None,
    replace: bool = False,
    partitions: int | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
    scale: float | None = None,
) -> BundleManifest: ...


@overload
def write(
    spark: SparkSession,
    profile: ProfileSource,
    out: str | os.PathLike[str],
    *,
    rows: int | None = None,
    seed: int | None = None,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: list[str] | None = None,
    partition_style: str | None = None,
    writer: dict[str, Any] | None = None,
    replace: bool = False,
    partitions: int | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
    scale: None = None,
) -> Manifest: ...


def write(
    spark: SparkSession,
    profile: ProfileSource | Bundle,
    out: str | os.PathLike[str],
    *,
    rows: int | None = None,
    seed: int | None = None,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: list[str] | None = None,
    partition_style: str | None = None,
    writer: dict[str, Any] | None = None,
    replace: bool = False,
    partitions: int | None = None,
    scenario: Scenario | Sequence[Scenario] | None = None,
    scale: float | None = None,
) -> Manifest | BundleManifest:
    """Write from Spark's executors the files `doppeldata.write` writes with the same arguments, byte for byte, and `_manifest.json`.

    The driver claims `out` as `doppeldata.write` does. Each task writes whole planned files, one per partition value for a
    partitioned range, and returns their records. The driver then stores the manifest from them. `partitions`, at most one per
    planned range, is one per range when None. `out` is an S3 URL or `memory://`, or a local directory under a local master.
    Every executor needs the driver's doppeldata build, and boto3 for an S3 destination, whose credentials each executor
    resolves through its own boto3 chain.

    A failed Spark job stores `_manifest.partial.json` recording no object, then raises WriteFailed. Its code is that of the
    StoppedWrite a task raised, or `spark-job-failed`. `replace` then deletes every part the run stored. `scenario` is taken as
    `doppeldata.write` takes it, and the files are the ones it writes.

    A bundle writes each table under `out/<table>/` as one Spark job, parents first, and the bundle's manifest at `out` once
    every table is whole, returning the BundleManifest `doppeldata.write` returns. A failed job stores its table's partial
    manifest and the bundle's, naming each table already whole.
    """
    choices = WriteChoices(format, file_rows, partition_by, partition_style, writer)
    run = run_of(profile, rows, seed, scale)
    if isinstance(run, BundleRun):
        return bundle_from_executors(spark, run, out, choices, replace, partitions, scenario)
    started = time.monotonic()
    prepared = prepared_write(run.profile, out, run.rows, run.seed, choices, one_process_scenario(scenario))
    if prepared.one_file is not None:
        message = f'{os.fspath(out)} names one file, which doppeldata.write writes; the executors write a directory, so end the path with /'
        raise WriteFailed(issue_codes.ONE_FILE_FROM_EXECUTORS, message)
    destination = executors_destination(spark, prepared.scheme, prepared.destination)
    store = s3_store_of(destination, prepared.bucket)
    prepared.engine.claim(prepared.options, destination, replace=replace, s3_store=store)
    planned = ExecutorPlan(prepared.arguments, _core.VERSION, _core.STREAM_VERSION)
    return Manifest.from_json(
        ranges_from_executors(
            spark, planned, prepared.engine, prepared.options, prepared.choices_json, destination, prepared.bucket, partitions, started
        )
    )


def ranges_from_executors(
    spark: SparkSession,
    planned: ExecutorPlan,
    engine: '_core.Plan',
    options: '_core.WriteOptions',
    choices_json: str,
    destination: str,
    bucket: str | None,
    partitions: int | None,
    started: float,
) -> str:
    """Write every range of `engine`'s claimed destination from the executors, and store and return its manifest's JSON. A
    failed job stores the partial manifest and raises WriteFailed."""
    ranges = len(json.loads(engine.ranges_json(options)))
    tasks = min(checked_partitions(partitions, ranges), max(ranges, 1))
    store = s3_store_of(destination, bucket)
    writes = RangeWrites(planned, choices_json, destination, bucket)

    def commit(written: list[str]) -> str:
        elapsed_ms = round((time.monotonic() - started) * 1000)
        stored: str = engine.commit(options, destination, written, tasks=tasks, elapsed_ms=elapsed_ms, s3_store=store)
        return stored

    try:
        collected = spark.range(ranges, numPartitions=tasks).mapInArrow(writes, 'written string').collect()
    except Exception as error:
        raise job_failure(error, destination, lambda written: Manifest.from_json(commit(written))) from error
    return commit([row.written for row in collected])


def bundle_from_executors(
    spark: SparkSession,
    run: BundleRun,
    out: str | os.PathLike[str],
    choices: WriteChoices,
    replace: bool,
    partitions: int | None,
    scenario: Scenario | Sequence[Scenario] | None,
) -> BundleManifest:
    """`write` of a bundle: the root claimed, each table written by its own Spark job, and the bundle's manifest stored."""
    for applied in table_scenarios(scenario, run.bundle).values():
        one_process_scenario(applied)
    prepared = prepared_bundle_write(run, out, choices, scenario)
    root = executors_destination(spark, prepared.scheme, prepared.destination)
    store = s3_store_of(root, prepared.bucket)

    def write_table(name: str, table: _core.Plan, options: _core.WriteOptions, destination: str) -> str:
        table.claim(options, destination, replace=False, s3_store=store)
        planned = ExecutorPlan(BundleTableArguments(prepared.arguments, name), _core.VERSION, _core.STREAM_VERSION)
        return ranges_from_executors(
            spark, planned, table, options, prepared.choices_json, destination, prepared.bucket, partitions, time.monotonic()
        )

    return bundle_written(prepared, root, replace, store, write_table, (WriteFailed,))
