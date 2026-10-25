# Generating inside Spark executors

`doppeldata.spark` runs the same wheel inside Spark's executors, so a billion rows are made where the
cluster reads them and never cross the driver. Both of its calls give the values a local run gives:

```python
import doppeldata as sd

profile = sd.Profile.load('orders.profile.yaml')
frame = sd.spark.generate(spark, profile, rows=10**9, seed=7)                  # a DataFrame, for any Spark writer
manifest = sd.spark.write(spark, profile, 's3://bucket/orders/', rows=10**9, seed=7)   # the files sd.write writes
```

## Each task generates whole chunks of engine blocks, so the partition count moves no value

`generate` numbers the dataset's chunks with `spark.range` and maps each partition's numbers to
batches through `mapInArrow`. A chunk is `CHUNK_BLOCKS` whole engine blocks, and chunk *n* starts
at block *n* × `CHUNK_BLOCKS`. The block is the one
[determinism-contract.md](determinism-contract.md) fixes. A task streams them from the engine's `Plan.stream_blocks`,
which makes each block the pure function `generate_block` that every local run calls. So the rows
a task yields are the rows `sd.generate` yields for those blocks, and `partitions` changes only
which task makes them.

- **Each executor builds the engine's plan from what the driver resolved**: the profile's JSON, its
  digest, the scenario, the seed and the row count. It builds it once per Python worker process.
- **A scenario's perturbations apply inside each block, so `scenario=` moves no value with the
  partition count either.** `generate` refuses `schema_drift` and `small_files` as
  `op-needs-files`, because they shape files and a DataFrame has none. `write` applies them.
- **An executor running another doppeldata release or stream version than the driver refuses the
  task.** A dataset's bytes are the build's that writes them, so a mixed cluster would write a
  manifest that names one build over files from two. Every unstamped build reports version 0.0.0,
  so two of them differ to this check only by stream version.
- **The driver builds the plan first**, so an ungenerable profile is refused before a task runs.
- **PySpark's `mapInArrow` needs pandas and pyarrow on the driver and every executor**, at the
  versions PySpark itself requires. The library declares neither, and `doppeldata.spark` imports
  pyarrow and pyspark only when it is first used.
- *Rejected:* Spark's own `rand(seed)`. Its values are deterministic only for a fixed partitioning,
  so a changed partition count changes the dataset.

## `sd.spark.write` writes the files `sd.write` writes, because each task writes whole planned files

The write is split three ways. The driver claims the destination with `_manifest.partial.json`,
exactly as a local write does. Each task writes whole planned ranges through the engine's
`Plan.write_ranges`, which runs the range writer a local write runs, and yields one record per
object it stored. The driver then stores `_manifest.json` from every record and removes the claim.
So object *i* holds the bytes a local `doppeldata data generate` writes for range *i*.

```text
driver   claim ─── _manifest.partial.json
           │
tasks    write_ranges(0) write_ranges(1) ... write_ranges(n)   one record per object
           │
driver   commit ─── _manifest.json, claim removed
```

- **`partitions` is at most one per planned range, and one per range unless given.** A partition
  holding several ranges writes them in turn on one thread, because Spark already gives each task
  a core. So the manifest records the task count in `tasks`, and `threads` is 1.
- **The destination must be one store the driver and every executor see**: an S3 URL, or
  `memory://`, which keeps nothing. A local directory is refused with code `destination-not-shared`
  unless the session is classic with a `local` master. That refuses a cluster whose executors share
  a POSIX mount too, and a Spark Connect client, whose disk is never the cluster's. Write to
  `s3://` there.
- **A path naming one file, such as `people.csv`, is refused with `one-file-from-executors`.** A
  local write stages that file and moves it into place, and no task can move what another wrote.
  `people.csv/`, with the trailing slash, names a directory.
- **The profile may be a list of column names, a dict of fakes and ranges, or a short YAML**, as
  for `sd.write`. The driver expands it before it plans.
- **A retried or speculative task writes the same names with the same bytes.** An object is
  either whole or absent, because an unfinished upload is aborted.
- **A failed Spark job stores a partial manifest recording no object.** The driver learns of a
  task's objects only when the whole job returns. `replace` then deletes every part the run stored,
  because each part's name carries the dataset's `identity8`.
- **A failed job raises `WriteFailed` with the code and kind of the task's own `StoppedWrite`**,
  such as `no-credentials`. A task's error reaches the driver inside Spark's, as a marked line the
  driver reads. Any other failure, such as an executor on another build, is `spark-job-failed`. The
  message names the cause and ends by saying to rerun with `replace=True`.
- *Rejected:* one `write(mode=...)` for both ways. Writing the DataFrame takes Spark's writer
  options, such as Delta, Iceberg or a `partitionBy`. Writing the files takes doppeldata's format and
  layout settings. One call would take two disjoint sets of arguments, and Spark names its own
  files, so its way has no manifest to return.

## Spark refuses unsigned integers, `date64`, times, timestamps not in microseconds, and `large_list`

`struct_type(profile)` gives the DataFrame's schema, and refuses a column Spark cannot carry with
every value kept. `spark_holds` decides which. A timestamp with no zone becomes a
`TimestampNTZType`. The parity check below sends each column of the determinism fixture,
`crates/core/tests/every-column-kind.profile.yaml`, through `mapInArrow` alone and collects it
back. It fails in any image that carries a column otherwise than `spark_holds` says. In Spark 3.5.9
and 4.2.0, each refused type fails this way:

| Arrow type | What Spark does |
| --- | --- |
| `uint8` to `uint64` | pyspark has no Spark type for it |
| `date64` | pyspark has no Spark type for it |
| `time32` | pyspark has no Spark type for it |
| `time64` | 3.5 has no Spark type; 4.2 maps it to `time(6)`, and its JVM then refuses the array |
| `timestamp[s]`, `timestamp[ms]` | the JVM refuses any timestamp not in microseconds |
| `large_list` | 3.5 has no Spark type; 4.2's JVM refuses the array |
| `large_string` | carried, and collected back as `string` |

Every other column came back with its type and every value. A nanosecond timestamp was not in the
fixture, and is refused with the others, because Spark holds microseconds. A refused column's
`physical_type` can be set in the profile to a type Spark holds, such as `int64` for an unsigned
integer.

## The parity check generates 10M rows in each Spark image, and every value and file matches

`spark-parity/check-spark-parity.sh` runs one container at a time. In each, it generates 10M rows of
the `eight-columns` example through `generate` with 7 and with 40 partitions, collects them as
Arrow, and compares every value with `sd.stream`, each float by its bits. It then writes them through
`write` with each partition count, and compares every file's SHA-256 with a `doppeldata data
generate` of the same arguments run on the host. Last, it checks `spark_holds` against how the image
carries each fixture column. Build the wheel, then run it over the images it names by default:

```bash
cd library && uv run --with ziglang maturin build --release --zig --compatibility manylinux2014 --out ../dist && cd ..
spark-parity/check-spark-parity.sh dist/doppeldata-*-cp311-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
```

| Image | Spark | Python, pyarrow | `generate`, 7 and 40 partitions | `write`, 7 and 40 partitions | Fixture columns |
| --- | --- | --- | --- | --- | --- |
| `apache/spark:3.5.9-python3` | 3.5.9 | 3.11.17, 26.0.0 | identical | 10 of 10 files match | as `spark_holds` says |
| `apache/spark:4.1.3-python3` | 4.1.3 | 3.11.17, 26.0.0 | identical | 10 of 10 files match | as `spark_holds` says |
| `apache/spark:4.2.0-python3` | 4.2.0 | 3.11.17, 26.0.0 | identical | 10 of 10 files match | as `spark_holds` says |
| `public.ecr.aws/glue/aws-glue-libs:5` | 3.5.4 | 3.11.15, 17.0.0 | identical | 10 of 10 files match | as `spark_holds` says |

- **The Apache images ship Python 3.10 with no pyarrow**, below the library's floor. So the check
  mounts uv into each container, which installs CPython 3.11 there, and the pyarrow and pandas
  `inside-spark-image.sh` pins.
- **The Glue image is Glue 5.0's runtime**: Spark 3.5.4 and pyarrow 17.0.0. The check keeps its
  own Python and pyarrow, and installs the wheel beside them.
- **Each container runs as the caller's uid, with that uid added to the image's `/etc/passwd`.**
  Hadoop's login refuses a uid with no name.
- **`--scenario FILE` applies a scenario to every run.** `spark-parity/eight-columns.scenario.yaml`
  holds every op that writes rows, and a schema drift. `write` and the host's run apply all of it.
  `generate` and `sd.stream` apply its ops that write rows. `spark-parity/check-spark-parity.sh
  --scenario spark-parity/eight-columns.scenario.yaml WHEEL apache/spark:3.5.9-python3` gave
  identical values at 7 and 40 partitions, and 10 of 10 files matched, the drifted half included.

## On Glue 5.1, a billion rows written from the executors match a local run file for file

One run of a Glue 5.1 job wrote 1B rows of `eight-columns`, seed 42, 1M rows a file, through
`write`. [performance.md](performance.md) shows the job's body. It ran 1,000 tasks on nine
executors. Its manifest records the same path, row count, size and SHA-256 for
all 1,000 files as a local `doppeldata data generate` with the same arguments. Both record the same
`dataset_digest`. Ten of its objects, sampled from S3 and hashed, hash as the manifest records.
[performance.md](performance.md) holds the run's throughput and the job's configuration.

## Over a Glue 5.1 Spark Connect session, both calls match a local run once reattachable execution is off

A Glue 5.1 Spark Connect session of two G.1X workers ran the 10M-row case from a client outside AWS.
`generate` with 7 and with 40 partitions was collected one Spark partition per query, and every
value matched `sd.stream`, each float by its bits. `write` stored 10 files, and all 10 hash as the
local `doppeldata data generate` records.

- **The session is `create_session` with `SessionType` `SPARK_CONNECT`**, and
  `--additional-python-modules` naming the wheel. `get_session_endpoint` returns its `Url` and an
  `AuthToken` that lives 1,800 s. The client connects to
  `<Url>:443/;use_ssl=true;x-aws-proxy-auth=<AuthToken>`, the token URL-quoted.
- **The client runs `pyspark[connect]` 3.5.6, the session's Spark.** pyarrow 26 refuses the numpy
  below 2 that it pins, so the client ran pyarrow 22.0.0, Glue 5.1's own.
- **A result stream can end in `Stream removed (Data frame with END_STREAM flag received)` while
  the client's reattachable execution, its default, is on.** A long stream, such as all 10M rows in
  one query, can end that way. With `spark.client.disable_reattachable_execute()` before the first
  query, every query completed. Why reattaching fails through the session's endpoint is not
  established.
- **Over Connect, `write` claims and commits from the client process, with the client's
  credentials.** The tasks still write with the session's role.
- **A session answered its first query 3 to 4.5 minutes after Glue reported it `READY`.**

## Executors write to S3 with their own credentials, which Glue 5.1 resolves from the job's role

Each write task builds its own S3 store through boto3 on its executor. The driver's store cannot
travel to a task. Its credential provider holds a boto3 session, which cannot be pickled.

On Glue 5.1, boto3 resolved `iam-role` in the driver and in a task on every executor core. That is
the instance metadata of the job's IAM role. The executors' environment holds no
credential. Its only AWS variables are `AWS_DEFAULT_REGION`, `AWS_METADATA_SERVICE_NUM_ATTEMPTS` and
`AWS_STS_REGIONAL_ENDPOINTS`. So the job's role is what needs the bucket's permissions, and no
credential travels with a task.

The role held `AWSGlueServiceRole` and these on the bucket, which sufficed:

- `s3:ListBucket`, `s3:GetBucketLocation` and `s3:ListBucketMultipartUploads` on the bucket;
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:AbortMultipartUpload` and
  `s3:ListMultipartUploadParts` on its objects.

## A task's batches reach pyarrow with no copy, and Spark copies them once

A task imports the engine's batches through the Arrow C stream. On Glue 5.1, importing one
72,613,888-byte chunk grew pyarrow's memory pool by 0 bytes, in a task on every executor core.
The worker's resident set grew 9 to 72 MiB, at most the chunk itself. That growth is the engine's
own buffers, which pyarrow wraps. Spark copies each batch once after that, because `mapInArrow`
sends it to the JVM as an Arrow stream over a socket.

- **Each task read `pyarrow.total_allocated_bytes()` and `/proc/self/statm` before and after
  holding the chunk**, one task per executor core, in each of the two runs.
- **A Python worker's peak resident set after a run was 359 to 503 MiB writing directly**, and 185
  to 236 MiB handing batches to Spark's writer. Writing directly, the worker also encodes and
  uploads each file.
