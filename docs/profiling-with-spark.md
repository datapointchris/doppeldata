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
  --default-arguments '{"--additional-python-modules": "s3://your-bucket/glue/doppeldata-<version>-cp311-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl,scipy,duckdb==2.0.0,clevercsv==0.8.5,charset-normalizer"}'
aws glue start-job-run --job-name profile-claims \
  --arguments '{"--source": "s3://your-bucket/claims/", "--out_bucket": "your-bucket", "--out_key": "profiles/claims.profile.yaml"}'
```

A DataFrame the job already holds is profiled in its own session, with `sd.profile(frame,
engine='spark')`.

The `==` versions are the ones `uv.lock` resolves, which the parity check below measured.
`generated.readers` records each reader's version, so a profile read with another version is never
reused as one read with these.

## Profile over a Spark Connect session

Over Spark Connect, the client runs the profiler and the session computes every statistic. The
client lists the source, sniffs a CSV's dialect and builds the profile. The client reads S3 with its
own credentials to list and sniff, and the session reads the rows with its role.

- **The executors run each pass's code as their own.** The client pickles the `mapInArrow` passes
  by reference, so the executors need the client's Python minor version and the client's
  doppeldata. The profile runs one task first and is refused before any read where they differ.
- **A server `SPARK_REMOTE` names that takes no connection is refused with exit 4**, after
  `CONNECT_ATTEMPTS` tries of `CONNECT_TIMEOUT_SECONDS` each. The client's own retries would wait
  about ten minutes before failing.

From Python, the client holds the library at the session wheel's version, its CSV packages and
`pyspark[connect]` at the server's Spark version. `pyspark[connect]` 3.5 pins a numpy below 2,
which pyarrow 26 refuses, so the client holds pyarrow 22:

```bash
uv pip install --index https://pypi.ichrisbirch.com/simple/ 'doppeldata==<version>' 'pyspark[connect]==3.5.6' pyarrow==22.0.0 boto3 scipy duckdb clevercsv charset-normalizer
```

```python
spark = SparkSession.builder.remote('sc://spark-host:15002').getOrCreate()
profile = sd.profile('s3://your-bucket/claims/', engine='spark', spark=spark)
profile.save('claims.profile.yaml')
```

The CLI needs Python 3.13 or later, so it profiles over a server whose executors run that Python.
Glue 5.x executors run Python 3.11, so a Glue 5.1 session is profiled from Python, as below.

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ 'doppeldata-cli==<version>' --with 'pyspark[connect]==<server Spark version>'
SPARK_REMOTE=sc://spark-host:15002 doppeldata profile create s3://your-bucket/claims/ --engine spark --out claims.profile.yaml
```

## Profile over a Glue 5.1 Spark Connect session

A Glue 5.1 session names the wheel alone, because the client sniffs and fits. It also sets a
10-second sender stream, and the client runs its first query with reattachable execution off:

```python
import time
import urllib.parse

import boto3
from pyspark.sql import SparkSession

import doppeldata as sd

glue = boto3.client('glue')
glue.create_session(
    Id='profile-claims', Role='your-glue-role', GlueVersion='5.1', SessionType='SPARK_CONNECT',
    Command={'Name': 'glueetl', 'PythonVersion': '3'}, WorkerType='G.1X', NumberOfWorkers=2, IdleTimeout=15,
    DefaultArguments={
        '--additional-python-modules': 's3://your-bucket/glue/doppeldata-<version>-cp311-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl',
        '--conf': 'spark.connect.execute.reattachable.senderMaxStreamDuration=10s',
    },
)
while glue.get_session(Id='profile-claims')['Session']['Status'] == 'PROVISIONING':
    time.sleep(5)
try:
    endpoint = glue.get_session_endpoint(SessionId='profile-claims')['SparkConnect']
    token = urllib.parse.quote(endpoint['AuthToken'], safe='')
    spark = SparkSession.builder.remote(f"{endpoint['Url']}:443/;use_ssl=true;x-aws-proxy-auth={token}").getOrCreate()
    spark.client.disable_reattachable_execute()
    spark.range(1).collect()
    spark.client.enable_reattachable_execute()
    sd.profile('s3://your-bucket/claims/', engine='spark', spark=spark).save('claims.profile.yaml')
finally:
    glue.stop_session(Id='profile-claims')
```

- **The endpoint ends a result stream that sends nothing for between 20 and 45 seconds**, with
  `Stream removed (Data frame with END_STREAM flag received)`. An aggregate sends nothing until it
  finishes, and one of the claims' takes a minute. gRPC keepalive pings do not hold the stream
  open.
- **`senderMaxStreamDuration=10s` has the session end each stream after 10 seconds**, and the client
  reattaches to the running query. So no stream stays silent past the endpoint's limit, and queries
  silent for 45 and 150 seconds returned.
- **A session's first query fails with `INVALID_HANDLE.SESSION_NOT_FOUND` when reattachable
  execution is on**, so it runs with execution off. It returns 3 to 5.5 minutes after Glue reports
  the session `READY`.
- **The endpoint's `AuthToken` lives 1,800 seconds.** The client reattaches every 10 seconds with
  the token fetched before the first query in its URL. So a profile expected to run past about 25
  minutes goes to the job recipe, which holds no token. The claims' profile ran 1,158 seconds on one
  token.
- [generating-inside-spark.md](generating-inside-spark.md) holds what else a Glue 5.1 Connect
  session showed.

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
| Parquet | the wheel, `scipy` | the wheel |
| CSV or TSV | the wheel, `scipy`, `duckdb`, `clevercsv`, `charset-normalizer` | the wheel |

- **The executors import the library for the `mapInArrow` passes**, so the wheel is named in every
  case.
- **Every supported Glue preinstalls pyarrow, pandas and boto3**, which the profiler also reads.
- **No Glue preinstalls scipy**, which fits each numeric column's family on the driver. A job
  without it fits no column, writes each `fit` as null and warns `FitsSkipped`.
- **Over Spark Connect the client builds the profile**, so the client holds scipy and the CSV
  packages, and the session holds the wheel alone.

## Spark's profile matches Polars' within its bars, and is the same on Spark 3.5, Spark 4 and Glue

`spark-parity/check-profiler-parity.sh` profiles a file through Polars on the host. It then
profiles it through Spark in each image named, one container at a time, on `local[8]` with a 4 GB
driver. Both profiles must pass `profile check`, and the Spark one is held to the Polars one:

- row and null counts, types, nullability and extremes exactly;
- each quantile within a rank error of `RANK_ERROR`, measured against the file's own values;
- each distinct count within `DISTINCT_ERROR`, three of HyperLogLog++'s 1% standard errors;
- the repeated rows, keys, dependencies, lookup tables and relationships exactly, and each cell of
  a conditional table and each conditional rate both profiles state;
- each quantile given a category within `RANK_ERROR`, measured against that category's rows;
- any other field that differs is printed. So are the correlations, the correlation ratios, any
  conditional table, quantiles or rates one profile states alone, and any composite key short of
  unique one profile nominates alone, because each engine measures them on its own sample.

A dataset or relationship field the published schema gains stops the check until it compares the
field, so no field the engines might write differently goes unread.

Build the wheel, then run the check over a file. `KEEP` names a directory to keep each profile in.

```bash
cd library && uv run --with ziglang maturin build --release --zig --compatibility manylinux2014 --out ../dist && cd ..
POLICY=healthcare spark-parity/check-profiler-parity.sh \
  dist/doppeldata-*-cp311-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl \
  DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.csv
```

`TABLES=1` profiles a directory holding a folder per table into one bundle, through `--tables` on
the host and `profile_tables()` in the image. Each table is compared, and so is each relationship:

```bash
TABLES=1 spark-parity/check-profiler-parity.sh \
  dist/doppeldata-*-cp311-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl \
  shop public.ecr.aws/glue/aws-glue-libs:5
```

It ran on CMS DE-SynPUF sample 1 under `--policy healthcare`, in `apache/spark:3.5.9-python3`,
`apache/spark:4.1.3-python3` and `public.ecr.aws/glue/aws-glue-libs:5`:

| File | Rows | Columns | Worst quantile rank error | Worst distinct count error | `profile check` |
| --- | --- | --- | --- | --- | --- |
| 2008–2010 carrier claims 1A | 2,370,667 | 142 | 0.00005, `CLM_ID` p99.9 | 1.3%, `LINE_ICD9_DGNS_CD_7` 2,565 against 2,599 | 476 literal fields, 0 violations |

- **Every image wrote the same profile.** Outside `generated` and `source`, every field matches
  across them. Spark 4 reads through `try_cast`, and Spark 3.5 with ANSI off.
- **Every exact field matches Polars'.** That covers each value list, other rate and decimal scale.
  The fields that differ are the estimates: distinct counts, tail quantiles, and the histogram edges
  and counts set by those quantiles. The fits differ too, because each engine fits its own sample.
- **`DESYNPUF_ID`, the beneficiary's identifier, holds no literal.** Its profile keeps a length, a
  whitespace rate and shapes, each shape a run of `9` and `A` covering at least 11 rows.

The same check passed on 10,104,443 rows of the New York yellow-taxi trips, February to April 2024,
as Parquet in the Glue image. Its worst quantile rank error was 0.00005, at `fare_amount` p1. Its
worst distinct count error was 1.7%, at `total_amount`'s 27,315 against 26,847. Every decimal scale
and value list matched, such as two places on the fares and 249 pickup location codes. Only the
timestamps' `physical_type` differed, for the reason below.

## On Glue 5.1, a job and a Spark Connect session wrote the local Glue image's profile

The carrier claims were profiled from S3 under the default policy, on two G.1X workers: one executor
of 4 vCPUs and a 10 GB heap beside the driver.

| Run | Profile | Billed |
| --- | --- | --- |
| job, the recipe above | 1,103 s of a 1,172 s run | 2,345 DPU-seconds, 0.65 DPU-hours |
| Spark Connect session, the recipe above | 1,158 s | 3,386 DPU-seconds, 0.94 DPU-hours, over 1,693 s |

- **The two profiles match each other in all 10,945 fields.** Each matches the profile from the
  local Glue image but for `name`. A prefix names its table for the last folder, `claims`, and one
  file for its stem.
- **Both record `readers.pyspark` as `3.5.6-amzn-1`**, the session's Spark.
- **The session's 1,693 seconds include its first query's wait** and two queries that tested the
  stream limit.

## On one machine Polars profiles several times faster than Spark

Polars ran on the host's 16 cores, timed with `/usr/bin/time -v doppeldata profile create FILE`,
its elapsed time and maximum resident set. Spark ran in the Glue image on `local[8]` with a 4 GB
driver, timed by the seconds `check-profiler-parity.sh` prints for the image. Both ran under the
default policy, on a host other work shared, so each time is an upper bound.

| Data | Rows | Columns | Polars | Spark |
| --- | --- | --- | --- | --- |
| taxi trips, three Parquet files | 10,104,443 | 19 | 23 s, 3.1 GB peak | 209 s |
| carrier claims, one CSV | 2,370,667 | 142 | 71 s, 12 GB peak | 937 s |

Polars computes each pass as one streaming query over the files. Spark runs a query per pass over a
table persisted to disk, and a CSV adds the passes that type its text. On the claims no query
dominates. Reading and persisting the text, each typing pass, the parse, the quantiles and the
distinct counts each take a like share. Each aggregate of `COLUMNS_PER_QUERY` columns after the
first takes a small part of one.

So Spark is the engine for a table that already sits on a cluster, or that one machine's memory
cannot hold.

## Each statistic comes from one of the passes below, so no query is larger than Spark compiles

`SparkBackend.aggregate` splits the requests it is given by the pass that answers them. Each pass is
one query over every column it covers, except the compiled aggregate, which is one per
`COLUMNS_PER_QUERY` columns:

| Pass | Answers | How |
| --- | --- | --- |
| compiled aggregate | rows, nulls, extremes, moments, rates, value, month, weekday, hour and histogram counts | one `agg` of declarative aggregates per `COLUMNS_PER_QUERY` columns |
| sketched aggregate | `quantiles` and `length_quantiles` | `percentile_approx` at `QUANTILE_ACCURACY` |
| distinct stream | `distinct_count` | one HyperLogLog++ over `(column, value as text)` pairs, grouped by column |
| type stream | `raw_type_counts` | one code per distinct set of layouts over `(column, value)` pairs, grouped by column |
| end values | the values at rank `k_min` from each end | `mapInArrow`: each task keeps its `k_min` values nearest each end, merged on the driver |
| split steps | `step_min` and `step_max` | `mapInArrow` over each file split in read order, stitched in the listing's order |
| distinct days | `distinct_days` | `collect_set` of the days between the trimmed extremes |
| tuple counts | `distinct_rows` and `unique_rows`, for repeated rows and keys | one `xxhash64` per tuple over each value and whether it is null, every tuple in one stream grouped by `(tuple, hash)` |
| rank correlations | Spearman's ρ | each sampled column's average rank from `rank` and `cume_dist` over one window, persisted, then each pair's count, sums, squares and product where both ranks are present, over up to `PAIRS_PER_QUERY` pairs a query, with ρ taken from them on the driver |
| cross counts | Cramér's V and conditional cells | every pair's values in one stream, grouped by `(pair, value, value)` |
| rank moments | η | each sampled numeric column's average rank as for Spearman's ρ, persisted beside the categorical columns, whose values form one stream grouped by `(column, value)`, with each rank's count, mean and `var_pop` |
| dependency counts | functional dependencies | one `groupBy` on each `lhs`, with every `rhs`'s count and whether its least and greatest values differ |
| grouped quantiles | a numeric column's quantiles given a category | one `groupBy` on the category, its unlisted values as one group, with `percentile_approx` at `QUANTILE_ACCURACY` |
| grouped rates | a column's zero and null rates given a category | one `groupBy` on the category, counting each child's rows, nulls and zeros |
| ordered shares | which dates and timestamps are at or after another | one `agg` per `PAIRS_PER_QUERY` pairs, counting the rows holding both and those in order |
| lag quantiles | a lag's quantiles, `min` and `max` | one `agg` of `percentile_approx` over each lag, then one pass of `EndValues` for the values at rank `k_min` from each end |
| expression matches | which expression of two columns gives a third | one `agg` per `PAIRS_PER_QUERY` candidates, counting the rows holding all three columns, those of them not zero on both sides, and those of the latter within tolerance; a division by zero is null, because ANSI mode would raise |
| foreign key counts | a bundle's relationships | a full join of the parent's distinct keys and the child's rows per key, grouped by `(in parent, child rows)` |

The split follows what Spark compiles:

- **Spark compiles no query whose rows or buffers hold more than `spark.sql.codegen.maxFields`
  fields.** Its default of 100 is passed by the moments of a few dozen columns, which then run
  interpreted. The backend raises it for the profile, as `SESSION_SETTINGS` sets.
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
  every layout the catalog holds by every raw type's code made a plan too large to compile.

*Rejected:* one `agg` holding every statistic. Spark runs it interpreted, and it profiled the
116,352-row beneficiary file in 236 s against Polars' 5 s.

*Rejected:* unioned `orderBy().limit()` queries for the trimmed extremes. Each branch is a full scan
with its own shuffle, so a table of 32 columns ran 128 of them.

*Rejected:* `corr` for Spearman's ρ. It divides by a constant column's zero variance, which raises
under Spark 4's ANSI mode. `try_divide` over `covar_pop` and two masked `var_pop` passed the 64 KB
limit at 128 pairs a query, where the sums compile up to 160 on Spark 3.5.

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

`picked` places each row by `xxhash64` of the seed, its file's index in the listing, and its own
index in that file. It keeps the n rows whose place is least, after dropping those outside the
least `OVERSAMPLE` times n's share of the hash range. Spark's own `sample` seeds each partition
apart, so its rows change with how Spark splits the files. The value sample, the fit sample and the
layout trial are all drawn this way, so a profile's fits and patterns do not depend on the
cluster's size.

A row's index in its file is the rows of the file's earlier splits plus its place in its own split.
One `groupBy` over the split columns counts each split, and the driver adds them up in block order.
So the place does not move with the split size, and each copy of a repeated row is drawn on its own.
A row's values would place copies together, and a table of a few distinct rows would then sample
some of them whole and miss the rest.

The file's path is left out of its place, so the same files read from a local path and from S3 are
placed alike. A Spark DataFrame has no files, so its rows are placed by their values, and its
identical rows are kept or dropped together.

## The backend runs under the session settings below, and restores each one

| Setting | Value | Why |
| --- | --- | --- |
| `spark.sql.session.timeZone` | `UTC` | a timestamp's month, hour and whole units read as Polars reads a UTC column |
| `spark.sql.legacy.timeParserPolicy` | `CORRECTED` | a date the parser cannot read is null, where `EXCEPTION` raises |
| `spark.sql.codegen.maxFields` | `10000` | the compiled aggregate above |
| `spark.sql.ansi.enabled` | `false`, on Spark 3.5 only | a cast of text that does not parse is null |

Spark 4 casts through `Column.try_cast`, which gives null under ANSI mode too, so ANSI is left as
the session sets it. Spark 3.5 has no `try_cast` on a column, so the backend turns ANSI off and
casts.

- **The settings are set on the session itself.** Another thread's queries on that session run
  under them until the profile ends. A Spark Connect session is its client's own, so the other
  clients of one server are untouched. `newSession()` would keep them apart, but Spark Connect has
  none, and a DataFrame's queries run under its own session's settings.
- **Each setting is restored, and each persisted frame released, on its own.** One that fails is
  noted on the profile's error, or warned of as a `ProfilingWarning` where the profile succeeded.
  So a session ended mid-profile shows the profile's own error, not the cleanup's.

## Spark reads Parquet, CSV and TSV, and refuses JSON and Arrow

- **Parquet is read with `basePath` at the root named**, so Spark finds the Hive keys the listing
  found. A key Spark types as a small integer is widened to `bigint`, as Polars types it.
- **Parquet files holding different schemas are read a schema at a time and joined by name**, so a
  drifted dataset is one table, as under Polars. A column takes the widest type a file stores it as,
  and is null in a file that lacks it. Spark's `mergeSchema` refuses an int32 column a later file
  widens to int64, on Spark 3.5.9 and 4.1.3 alike. An explicit union schema reads it on 4.1.3 but
  not on 3.5.9, whose Parquet reader does not widen. A column stored as an integer in one file and
  a string in another fails the read, as it does under Polars.
- **Spark holds every timestamp in microseconds**, so a timestamp column's `physical_type` is
  `timestamp[us]` whatever the file stores. `source.parquet.timestamp_units` keeps the stored unit,
  which the writers cast to. Polars takes a unit from a file's stored Arrow schema where one is
  written. The taxi files store microseconds under an Arrow schema naming nanoseconds, so Polars
  writes `timestamp[ns]` for them.
- **CSV and TSV are read with every column as text**, in the dialect DuckDB and CleverCSV sniff
  from the first block. On S3 that block comes from one ranged read through boto3, decompressed
  where the file is `.gz`, `.zst` or `.bz2`. A malformed row is classified by its field count, as
  [profiler-statistics-and-readers.md](profiler-statistics-and-readers.md) says.
- **JSON and Arrow IPC are refused** with a `SourceError` naming engine polars. JSON is profiled path
  by path through Polars' nested types, which the Spark backend does not translate.
- **The backend uses the DataFrame API alone**, never `_jdf` or the JVM, so it runs over Spark
  Connect unchanged.
- **A Spark Connect plan's protobuf nests at most 100 levels.** So a condition over every column is
  one `greatest`, where a chain of `|` would nest a level per column. The claims' 142 columns
  chained by `|` failed with `Exceeded upb_DecodeOptions_MaxDepth`.
