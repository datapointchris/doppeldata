# doppeldata

Profile a dataset into a reviewable file holding no rows, then generate synthetic data from it at any
scale.

This is the library. It installs the compiled generator and the packages that read and validate a
profile's YAML, and no profiling engine. The `doppeldata` command is the separate `doppeldata-cli`
distribution, so a project importing the library installs none of the CLI's dependencies.

```bash
uv add --index https://pypi.ichrisbirch.com/simple/ doppeldata
```

```python
import doppeldata as sd

table = sd.generate(['name', 'email', 'city', 'zip'], rows=1000)   # each column takes the fake its name hints
table = sd.generate({'age': sd.integers(18, 90), 'plan': sd.choice(['free', 'pro'])}, rows=1000)
table = sd.generate(sd.Profile.load('orders.profile.yaml'), rows=1_000_000, seed=7)
table = sd.generate(OrdersSchema, rows=1000)   # a pandera, dataframely or Spark schema, read as a seed profile
```

The table crosses through the Arrow PyCapsule interface, so Polars, DuckDB and pyarrow 14 or later
read it with no copy, and none of them is needed to generate. `sd.stream()` returns a
RecordBatchReader for data larger than memory. `sd.write()` writes a directory of files with a
manifest, or one file such as `people.csv`, and
`sd.plan()` says what it would write. A write that stops raises a `StoppedWrite`.
`sd.regenerate(profile, manifest, out)` writes the dataset a manifest records again, and raises
`WriteFailed` naming each file whose bytes differ from its record.
`sd.spark.generate()` and `sd.spark.write()` do the same inside Spark's executors, with the values
and files a local run gives. They need PySpark 3.5 or later, with pyarrow 15 or later and the pandas
PySpark asks for, and doppeldata installed on every executor. A write to S3 needs boto3 on every
executor too.
`sd.spark.to_spark(spark, table)` builds a DataFrame from a table on the driver, of up to
`sd.spark.DRIVER_ROWS_AT_MOST` rows.

`sd.sample(profile, rows=50, overrides={'status': 'refunded'})` pins a column to given values, and
every column drawn from it reads them. Under pytest, the `synthetic` fixture generates a table for
each test from a seed of its own. A failed test's report names the seed that reruns it.

A schema class is read by its attributes, so none of pandera, dataframely and pyspark is needed
beside it. `sd.Profile.from_odcs()`, `from_json_schema()`, `from_table_schema()` and
`from_data_package()` import a seed from a schema file with nothing more installed.
`sd.Profile.from_glue()` reads a Glue Data Catalog table, and needs boto3, with pyarrow too where
it is given `detected_entities`.

`sd.export(profile, 'odcs')` writes the profile's contract in each format its docstring lists, with
none of the tools that read them installed.
`sd.write(..., embed_profile=True)` puts the profile in each Parquet and Arrow file, and
`sd.Profile.from_data_file()` reads it back.

The library installs no profiling engine, so a job that only generates carries none.
`doppeldata.profile()` needs polars, duckdb and pyarrow installed beside it, and boto3 to list an
`s3://` prefix, and names any that are missing. `profile(data, engine='spark')` computes the profile
in Spark instead: of a Spark DataFrame in its own session, or of files in the session `spark=`
names, else the active one. It needs pyspark and pyarrow, boto3 for an `s3://` prefix, and duckdb,
clevercsv and charset-normalizer to sniff a CSV or TSV. It estimates distinct counts with a 1% standard error, and quantiles within a rank error of
1/10,000. `profile()` refuses a directory whose folders hold unlike column names, naming each
table it holds, and `profile_tables()` profiles it to a `Bundle` of one table per kind of folder,
as an AWS Glue crawler splits one. A list given to `profile_tables()` groups each path on its own,
and the bundle records the foreign keys between its tables. With scipy installed too, it fits a distribution family to each
numeric column with enough distinct values. Without it, each such fit is written as null, and
`profile()` issues a `FitsSkipped` warning. Every warning `profile()` issues is a `ProfilingWarning`. Each step
a call takes, such as the files it listed, goes to the `doppeldata` logger, which writes nothing until a caller adds a
handler. A write to S3 needs boto3. Every call reaching S3 or the Glue catalog through boto3 takes `aws_profile=` to
name the AWS profile for that call, and `sd.spark` takes none, because Spark reads S3 with its session's own credentials.

`profile(..., packs=['acme'])` types columns with a third party's detector pack beside the general
one. A pack registers under the `doppeldata.detector_packs` entry-point group, and is a
`DetectorPack` from `doppeldata.profiling.detector_packs`. A pack is code the profiler runs on the
sampled values, so enable only one trusted with the data.

Every release is published to that package index, and nothing else is. PyPI does not carry
doppeldata, so a package of that name there is someone else's. The library needs Python 3.11 or
later, so it installs into an AWS Glue job:
<https://github.com/datapointchris/doppeldata#run-the-library-inside-an-aws-glue-job>.

The README walks up from column names to a profile of real data, and links a page for every task:
<https://github.com/datapointchris/doppeldata#readme>. Source and issues live there too.
