# Profiling with Spark

`engine='spark'` computes a profile in Spark, so a table on a cluster is profiled where it sits, and
only the statistics reach the driver. The profile is the one Polars writes: the same fields, under
the same privacy policy, with exact counts and extremes, and estimated distinct counts and
quantiles.

## Profile a table on a Glue 5.1 job

The job's script profiles a prefix and stores the YAML beside the data:

```python
# profile_table.py
import sys

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

import doppeldata as sd

args = getResolvedOptions(sys.argv, ['source', 'out_bucket', 'out_key'])
spark = SparkSession.builder.getOrCreate()
profile = sd.profile(args['source'], engine='spark', spark=spark)
boto3.client('s3').put_object(Bucket=args['out_bucket'], Key=args['out_key'], Body=profile.to_yaml().encode())
```

Upload the script and the library's wheel, then create and run the job. The README's Glue section
gives the wheel's download command.

```bash
aws s3 cp profile_table.py s3://your-bucket/glue/
aws glue create-job --name profile-claims --role your-glue-role --glue-version 5.1 \
  --worker-type G.1X --number-of-workers 2 \
  --command Name=glueetl,ScriptLocation=s3://your-bucket/glue/profile_table.py,PythonVersion=3 \
  --default-arguments '{"--additional-python-modules": "s3://your-bucket/glue/doppeldata-<version>-cp311-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl,duckdb==2.0.0,clevercsv==0.8.5,charset-normalizer"}'
aws glue start-job-run --job-name profile-claims \
  --arguments '{"--source": "s3://your-bucket/claims/", "--out_bucket": "your-bucket", "--out_key": "profiles/claims.profile.yaml"}'
```

A DataFrame the job already holds is profiled in its own session, with `sd.profile(frame,
engine='spark')`.

## Profile over a Spark Connect session

Over Spark Connect, the client runs the profiler and the session computes every statistic. The
client lists the source, sniffs a CSV's dialect and builds the profile, so it holds the library, its
CSV packages and `pyspark[connect]` at the server's Spark version:

```bash
uv pip install --index https://pypi.ichrisbirch.com/simple/ doppeldata 'pyspark[connect]==3.5.6' boto3 duckdb clevercsv charset-normalizer
SPARK_REMOTE=sc://spark-host:15002 doppeldata profile create s3://your-bucket/claims/ --engine spark --out claims.profile.yaml
```

From Python, name the session:

```python
spark = SparkSession.builder.remote('sc://spark-host:15002').getOrCreate()
profile = sd.profile('s3://your-bucket/claims/', engine='spark', spark=spark)
profile.save('claims.profile.yaml')
```

A Glue 5.1 session is `create_session` with `SessionType='SPARK_CONNECT'` and
`--additional-python-modules` naming the wheel alone, because the sniff runs on the client.
[generating-inside-spark.md](generating-inside-spark.md) gives the endpoint's URL and the client's
pins. The client reads S3 with its own credentials to list and sniff, and the session reads the
rows with its role.

## Profile locally with `--engine spark`

With no `SPARK_REMOTE`, `--engine spark` starts a session on `local[*]`. That needs Java and pyspark
beside the CLI:

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ doppeldata-cli --with pyspark==3.5.6
doppeldata profile create data/claims/ --engine spark --out claims.profile.yaml
```

## What `--additional-python-modules` names

| Source | Glue job | Glue Spark Connect session |
| --- | --- | --- |
| Parquet | the wheel | the wheel |
| CSV or TSV | the wheel, `duckdb==2.0.0`, `clevercsv==0.8.5`, `charset-normalizer` | the wheel |

Every supported Glue preinstalls pyarrow, pandas, boto3 and scipy, which the profiler also reads.
Without scipy no column would be fitted, and a profile says so. The executors import the library
for the `mapInArrow` passes, so the wheel is named in every case.

## Each statistic comes from one of seven passes, so no query is larger than Spark compiles

`SparkBackend.aggregate` splits the requests it is given by the pass that answers them. Each pass is
one query over every column it covers, except the compiled aggregate, which is one per
`COLUMNS_PER_QUERY` columns:

| Pass | Answers | How |
| --- | --- | --- |
| compiled aggregate | rows, nulls, extremes, moments, rates, value, month, weekday, hour and histogram counts | one `agg` of declarative aggregates per 16 columns |
| sketched aggregate | `quantiles` and `length_quantiles` | `percentile_approx` at accuracy 10,000 |
| distinct stream | `distinct_count` | one HyperLogLog++ over `(column, value as text)` pairs, grouped by column |
| type stream | `raw_type_counts` | one code per distinct set of layouts over `(column, value)` pairs, grouped by column |
| end values | the values at rank `k_min` from each end | `mapInArrow`: each task keeps its `k_min` values nearest each end, merged on the driver |
| split steps | `step_min` and `step_max` | `mapInArrow` over each file split in read order, stitched in the listing's order |
| distinct days | `distinct_days` | `collect_set` of the days between the trimmed extremes |

The split follows what Spark compiles:

- **Spark compiles no query whose rows or buffers hold more than `spark.sql.codegen.maxFields`
  fields.** Its default of 100 is passed by the moments of a few dozen columns, which then run
  interpreted. The backend raises it to 10,000 for the profile.
- **A compiled query's generated `processNext()` must stay under the JVM's 64 KB method limit.**
  The carrier claims' 142 numeric columns in one aggregate passed it, and Spark ran the query
  interpreted in 140.6 s. Split by columns, 16 a query took 19.1 s, 32 took 21.7 s, and 64 passed
  the limit again at 56.4 s. Each query scans only its own columns of the persisted table, so the
  split reads nothing twice.
- **One imperative aggregate, such as `percentile_approx`, keeps Spark from compiling the whole
  query**, so the quantiles have a query of their own.
- **A HyperLogLog++ sketch per column puts 1,639 buffer words per column in one aggregate**, which
  Spark spends tens of seconds handling for a few dozen columns, whatever the row count. One sketch
  grouped by column holds one sketch's buffer per group.
- **A layout tried on every text column is one expression in the type stream.** Tried per column,
  the layout trial's 25 layouts by six codes made a plan too large to compile.

*Rejected:* one `agg` holding every statistic. Spark runs it interpreted, and it profiled the
116,352-row beneficiary file in 236 s against Polars' 5 s.

*Rejected:* unioned `orderBy().limit()` queries for the trimmed extremes. Each branch is a full scan
with its own shuffle, so a table of 32 columns ran 128 of them.

*Rejected:* datasketches' KLL and frequent-items sketches. `percentile_approx` gives every quantile
and bin edge within a rank error of 1/10,000. KLL at its default `k` of 200 carries about 1.3%,
outside the 1% the profile is held to. Top values come from an exact `groupBy`, and no profile
field reads a sketch.

## The tables the passes read are persisted on disk, never in memory

A CSV is read once into a persisted frame of text columns, because each typing pass reads the text
again. The parsed table is persisted too. Otherwise the optimizer inlines each column's parse into
every aggregate that reads it, and a kurtosis reads its column five times.

Both are persisted `DISK_ONLY` and read back through the page cache. `MEMORY_AND_DISK` unrolls a
block of cached batches in memory and checks the heap only every 16 batches. Each batch is 10,000
rows of the table, so a few tasks caching a wide table at once exhaust the heap before Spark spills.
Every frame the backend persisted is released when the profile ends.

## A sample is the rows with the least seeded hash, so it does not move with the cluster

`picked` places each row by `xxhash64` of the seed and the row's values. It keeps the n rows whose
place is least, after dropping those outside the least `OVERSAMPLE` times n's share of the hash
range. Spark's own `sample` seeds each partition apart, so its rows change with how Spark splits
the files. The value sample, the fit sample and the layout trial are all drawn this way, so a
profile's fits and patterns do not depend on the cluster's size.

A row's file and split are left out of its place, so the same rows read from a local path and from
S3 are placed alike. Two identical rows share a place, and are both kept or both dropped.

## The backend runs under four session settings, and restores each one

| Setting | Value | Why |
| --- | --- | --- |
| `spark.sql.session.timeZone` | `UTC` | a timestamp's month, hour and whole units read as Polars reads a UTC column |
| `spark.sql.legacy.timeParserPolicy` | `CORRECTED` | a date the parser cannot read is null, where `EXCEPTION` raises |
| `spark.sql.codegen.maxFields` | `10000` | the compiled aggregate above |
| `spark.sql.ansi.enabled` | `false`, on Spark 3.5 only | a cast of text that does not parse is null |

Spark 4 casts through `Column.try_cast`, which gives null under ANSI mode too, so ANSI is left as
the session sets it. Spark 3.5 has no `try_cast` on a column, so the backend turns ANSI off and
casts.

## Spark reads Parquet, CSV and TSV, and refuses JSON and Arrow

- **Parquet is read with `basePath` at the root named**, so Spark finds the Hive keys the listing
  found. A key Spark types as a small integer is widened to `bigint`, as Polars types it.
- **CSV and TSV are read with every column as text**, in the dialect DuckDB and CleverCSV sniff
  from the first block. On S3 that block comes from one ranged read through boto3, decompressed
  where the file is `.gz`, `.zst` or `.bz2`. A malformed row is classified by its field count, as
  [profiler-statistics-and-readers.md](profiler-statistics-and-readers.md) says.
- **JSON and Arrow IPC are refused** with a `SourceError` naming engine polars. JSON is profiled path
  by path through Polars' nested types, which the Spark backend does not translate.
- **The backend uses the DataFrame API alone**, never `_jdf` or the JVM, so it runs over Spark
  Connect unchanged.
