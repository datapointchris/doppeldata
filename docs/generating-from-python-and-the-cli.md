# Generating from Python and the command line

## Tables cross into Python through the Arrow PyCapsule interface, so no Arrow library is a dependency

`doppeldata.generate()` returns a table and `doppeldata.stream()` a RecordBatchReader. Both are
pyo3-arrow's own classes, and both carry `__arrow_c_stream__`. A reader takes them with no copy:

```python
import doppeldata as sd

profile = sd.Profile.example('orders')
table = sd.generate(profile, rows=1_000_000, seed=7)
pl.DataFrame(table)                          # Polars
duckdb.sql('select count(*) from table')     # DuckDB, by variable name
pa.table(table)                              # pyarrow 14 or later
pa.RecordBatchReader.from_stream(sd.stream(profile, rows=10**9, seed=7))   # pyarrow 15 or later
```

- **Polars and DuckDB read both in a Python 3.11 environment with no pyarrow and no arro3.** The
  schema's metadata carries the `doppeldata.*` keys a Parquet file carries, the dataset digest and
  the seed among them.
- **The stream holds at most two batches per thread.** Each worker owns a channel one batch deep,
  and the reader takes the workers' blocks in turn. So a billion-row stream runs in the memory of a
  few blocks, and its batches arrive in row order.
- **The values are the Arrow values every format writes.** A `type_mix` error or a missing-value
  token exists only as text, so the table holds those rows as null. CSV and JSON print them.
- *Rejected:* returning a pyarrow table. pyarrow would become a dependency of every job that only
  generates, and a Polars or DuckDB job reads the capsule without it.
- *Rejected:* arro3-core. pyo3-arrow's classes already carry the capsule interface, so arro3 would
  add a package and no reader.

## `generate` and `write` run with the GIL released, and Ctrl-C stops them between blocks

`generate`, `write` and the size estimate leave Python with `Python::detach`, and run the engine on
threads they spawn. `stream` makes its blocks on threads it spawns, and hands them over on its
consumer's thread. [performance.md](performance.md) records how fast `generate` and `data generate`
run on one thread and on every core.

Python runs a signal handler only on the main thread, and only when that thread returns to the
interpreter. Native code that never returns never sees Ctrl-C. So the calling thread waits in steps
of `SIGNAL_CHECK`. After each one it re-attaches and calls `PyErr_CheckSignals`. A raised exception
sets the writers' cancel flag, and the workers stop at the next block.

- **`generate` raises the handler's exception, `KeyboardInterrupt` for Ctrl-C.** A stream's threads
  check for no signal. So Ctrl-C reaches a stream when its consumer returns to Python between
  batches. For Polars that is after the last batch.
- **`write` raises `WriteInterrupted`, a `KeyboardInterrupt`**, so `except Exception` never
  swallows it. It and `WriteFailed` are both a `StoppedWrite`, which carries the partial manifest
  and whether it was stored.
- **A stopped write's partial manifest lists every object it completed, and leaves no open
  multipart upload.** A sink dropped unfinished aborts its upload. `doppeldata.spark.write` is the
  exception: a failed Spark job's partial manifest lists no object, as
  [generating-inside-spark.md](generating-inside-spark.md) says.
- **`data generate` treats SIGTERM as Ctrl-C.** A container stop or a `kill` sends SIGTERM, which
  would otherwise end the process mid-upload.
- **A process ended outright leaves its open multipart uploads in the bucket.** SIGKILL does that,
  and so does SIGTERM in a Python host with no handler for it. An `AbortIncompleteMultipartUpload`
  lifecycle rule on the bucket deletes them. The rerun is refused until `--replace`, because
  `_manifest.partial.json` stays.
- *Rejected:* a Rust signal handler, such as the `ctrlc` crate. It replaces Python's handler for the
  whole process, which breaks a host application's own.

## S3 credentials come from boto3, handed to object_store as a callable

object_store reads credentials from environment keys, web identity, ECS and the instance metadata
service. It cannot read an SSO profile or a `credential_process`. Given only those, it falls
through to the instance metadata service.

So `write()` to an `s3://` or `s3a://` URL imports boto3, resolves its session, and builds
pyo3-object_store's `S3Store` with a `credential_provider` callable over it. The region comes from
the bucket's `x-amz-bucket-region` header. The endpoint is the one boto3 resolves for that region.
So an `AWS_ENDPOINT_URL_S3`, a profile's `endpoint_url` or a VPC endpoint serves boto3's checks and
the uploads alike.

- **Refreshable credentials are fetched again every `REFRESH_EVERY`**, which `s3_access.py` keeps
  short of botocore's refresh window. `Boto3Credentials.refresh_threshold` is zero, so the store
  asks once per interval rather than on every request.
- **Credentials boto3 cannot refresh are handed over once**, such as keys in the environment. The
  store uses them for the whole write.
- **The uploads retry as boto3 does.** They make `S3_ATTEMPTS` attempts, and give up connecting
  after `S3_CONNECT_TIMEOUT`.
- **A write through boto3's chain stores the same bytes as a local write with the same flags.**
- **boto3 is imported only for an S3 destination**, and declared nowhere. Without it, `plan` and
  `write` both refuse with `boto3-missing`, exit 2.
- *Rejected:* obstore. pyo3-object_store builds the same `S3Store` inside doppeldata's own extension,
  from the object_store the writers already link. obstore would be a second compiled copy in every
  job, and it takes credentials through the same callable.

## `data plan` prints what `data generate` writes with the same flags

```bash
doppeldata data plan orders --rows 250000 --seed 7 --file-rows 100000 --out orders/
doppeldata data generate orders --rows 250000 --seed 7 --file-rows 100000 --out orders/
```

`PROFILE` is a profile file or a bundled example's name. `--out` is a local directory, an `s3://`
or `s3a://` URL, or `memory://`, which keeps nothing and times a write. `data plan` reads nothing at
`--out`. It prints the dataset digest and identity, the format with every writer setting, each
column's sampler and null share, each row range, and an estimate of the bytes. A test runs both
commands and requires the manifest's identity and ranges to equal the plan's.

- **A partitioned range becomes one file per partition value it holds.** So the plan gives each
  range a path with `*` for each partition directory, such as `status=*/part-00000-1f2e3d4c.parquet`,
  and the most files it can become.
- **`--json` prints the plan as `doppeldata.plan()` returns it, and its shape may change before
  1.0.** It has no published schema. The schemas derive from the Rust model, and the plan is
  assembled in Python.
- **Every choice is laid over the profile's recorded settings in one place.** The CLI and the SDK
  pass a `WriteChoices` to `WriteOptions::chosen` as one JSON object, which refuses an unknown key.
  `--no-partitions` writes no partitions whatever the profile records, as `partition_by=[]` does.
- **A destination is parsed once, in the writers.** `plan` and `write` both ask it first, so `plan`
  refuses what `write` would: an unknown scheme, a `Path` holding a URL, or a missing boto3.
- **`data generate` refuses a destination holding anything, exit 2, before writing a byte.**
  `--replace` deletes only what an earlier doppeldata write recorded.
- *Rejected:* `data generate --dry-run`. A flag that turns a write into a read makes one command do
  two things. A rehearsal is its own verb, which cannot write.

## A scenario is one more input to every verb that generates or checks

```bash
doppeldata scenario check hot-key orders
doppeldata data generate orders --scenario hot-key --scenario late-data --out orders/
doppeldata data check orders orders/ --scenario hot-key --scenario late-data
```

`--scenario` takes a scenario file or a bundled example's name, and repeats to compose several in
order. The SDK's `scenario=` takes a `Scenario` or a list of them, on `generate`, `stream`, `plan`
and `write`. [scenario-overlays.md](scenario-overlays.md) holds the format and what it changes.

- **`data plan` prints the scenario's name and digest under the identity.** Under a `duplicates`
  op it also prints the rows written, and `--json` holds them as `rows_written`.
- **`--rows` counts the rows generated.** A `duplicates` op writes copies beside them, so
  `data generate` then names both counts.
- **`data check` refuses a `--scenario` other than the one DATA was written under, exit 2.** It
  reads the scenario's digest from the `_manifest.json` the write left.
- **`scenario list` prints every op, every metric an expectation names, and every bundled example.**
  `scenario show` prints an op's parameters with their types and meanings, or an example's YAML.
- **`scenario check SCENARIO PROFILE` plans a write without generating a row.** It takes `data
  plan`'s `--rows`, `--seed`, `--format`, `--file-rows`, `--partition-by` and `--no-partitions`,
  so it refuses what `data generate` would with the same flags. A refusal exits 3, and `--json`
  prints each issue's `code`, `field`, message and `closest` names.

## A stopped write's kind decides its exit status

The engine names each failure with a code, such as `destination-not-empty`, and a kind.
`errors.py` alone turns the kind into the exit status:

| Kind | Exit | Meaning |
| --- | --- | --- |
| `refused` | 2 | The call must change. Nothing was written. |
| `failed` | 1 | The write was attempted and failed. |
| `unanswered` | 4 | The store stopped answering, before the write or during it. |

- **A 5xx or 429 on every retry during the write reads as `failed`.** object_store keeps the status
  inside an error type it does not export. Before the write, boto3's bucket check reads it as
  `unanswered`.
- **The CLI reports every doppeldata error in its root group.** So each verb exits on the same
  table, and a new verb needs no handler of its own.

## The size estimate fits fixed and per-row bytes from the first encoded blocks

`estimate_bytes` in the writers encodes the first block as one object, and the first
`SAMPLE_BLOCKS` as another. The difference is what the added blocks' rows bring. The rest of the
first object is fixed, such as a header, a footer and a dictionary. Nothing reaches a store.

Estimated over written bytes, for the `orders` and `eight-columns` examples at 5M rows in 1M-row
files and at 1M rows in 100,000-row files:

| Format | Estimate over written |
| --- | --- |
| Arrow | 1.000 |
| CSV, TSV, JSON, NDJSON | 0.988 to 1.000 |
| Parquet | 0.957 to 1.069 |
| Parquet, partitioned by `eight-columns`' `category` into 50 objects | 1.170 |
| CSV, partitioned the same way | 0.993 |

Each ratio is the plan's `estimated_bytes` over the bytes the manifest records:

```bash
doppeldata data plan eight-columns --rows 5000000 --format F --out out/ --json | jq .estimated_bytes
doppeldata data generate eight-columns --rows 5000000 --format F --out out/ --replace
jq '[.files[].bytes] | add' out/_manifest.json
```

Parquet compresses a whole file's row group better than a few blocks of it. A partitioned estimate
counts one object per value in every range, and a skewed key makes some of those small.
