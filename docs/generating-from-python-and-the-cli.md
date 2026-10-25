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

## The engine runs with the GIL released, and Ctrl-C stops it between blocks

Every call leaves Python with `Python::detach` and runs the engine on threads it spawns. A
pure-Python counting thread kept 98% of its idle rate while a 20M-row `generate` ran beside it.

Python runs a signal handler only on the main thread, and only when that thread returns to the
interpreter. Native code that never returns never sees Ctrl-C. So the calling thread waits in
100 ms steps. After each one it re-attaches and calls `PyErr_CheckSignals`. A raised exception sets
the writers' cancel flag, and the workers stop at the next block.

- **`generate` and `stream` raise the handler's exception, `KeyboardInterrupt` for Ctrl-C.**
- **`write` raises `WriteInterrupted`, a `KeyboardInterrupt`**, so `except Exception` never
  swallows it. Like `WriteFailed`, it carries the partial manifest and whether it was stored.
- **`data generate` exits 1 and names `_manifest.partial.json`.** An interrupted S3 write left a
  partial manifest listing every object it completed, and no open multipart upload.
- *Rejected:* a Rust signal handler, such as the `ctrlc` crate. It replaces Python's handler for the
  whole process, which breaks a host application's own.

## S3 credentials come from boto3, handed to object_store as a callable

object_store reads credentials from environment keys, web identity, ECS and the instance metadata
service. It cannot read an SSO profile or a `credential_process`. Given only those, it fell through
to the metadata service and stalled in retries.

So `write()` to an `s3://` URL imports boto3, resolves its session, and builds pyo3-object_store's
`S3Store` with a `credential_provider` callable over it. Refreshable credentials are fetched again
every five minutes. The region comes from the bucket's `x-amz-bucket-region` header, and the
endpoint from boto3's, such as `AWS_ENDPOINT_URL`.

- **Against a moto server and a `credential_process` profile, the process ran once.** Every
  object's path, rows, bytes and SHA-256 equaled a local write of the same flags.
- **boto3 is imported only for an `s3://` destination**, and declared nowhere. A missing boto3 fails
  the write with `boto3-missing`, exit 2.
- *Rejected:* obstore. pyo3-object_store builds the same `S3Store` inside doppeldata's own extension,
  from the object_store the writers already link. obstore would be a second compiled copy in every
  job, and it takes credentials through the same callable.

## `data plan` prints what `data generate` writes with the same flags

```bash
doppeldata data plan orders --rows 250000 --seed 7 --file-rows 100000 --out orders/
doppeldata data generate orders --rows 250000 --seed 7 --file-rows 100000 --out orders/
```

`PROFILE` is a profile file or a bundled example's name. `data plan` reads nothing at `--out`. It
prints the dataset digest and identity, the format with every writer setting, each file's path and
rows, each column's sampler and null share, and an estimate of the bytes. `--json` prints all of it
as `doppeldata.plan()` returns it. A test runs both commands and requires the manifest's identity
and file list to equal the plan's.

- **Every choice is laid over the profile's recorded settings in one place.** The CLI and the SDK
  pass their flags to `WriteOptions::chosen` as one JSON object, which refuses an unknown key.
- **`data generate` refuses a destination holding anything, exit 2, before writing a byte.**
  `--replace` deletes only what an earlier doppeldata write recorded.
- **A failure after the claim exits 1.** An S3 bucket check that never gets an answer exits 4.
- *Rejected:* `data generate --dry-run`. A flag that turns a write into a read makes one command do
  two things. A rehearsal is its own verb, which cannot write.

## The size estimate fits fixed and per-row bytes from four encoded blocks

`estimate_bytes` in the writers encodes the first block as one object, and the first four as
another. The difference is what three blocks of rows add. The rest of the first object is fixed,
such as a header, a footer and a dictionary. Nothing reaches a store, and a plan took 30 to 250 ms.

Estimated over written bytes, for both bundled examples at 5M rows in 1M-row files and at 1M rows
in 100,000-row files:

| Format | Estimate over written |
| --- | --- |
| Arrow | 1.000 |
| CSV, TSV, JSON, NDJSON | 0.988 to 1.000 |
| Parquet | 0.957 to 1.069 |
| Parquet, partitioned by `eight-columns`' `category` into 50 objects | 1.170 |
| CSV, partitioned the same way | 0.993 |

Parquet compresses a whole file's row group better than four blocks of it. A partitioned estimate
counts one object per value in every range, and a skewed key makes some of those small.

## A 10M-row table generates at about 60M rows a second on sixteen threads

The population is 10M rows of the `eight-columns` example, seed 42. Each row is five runs on a
16-thread Ryzen 9 6900HX with other work running:

| Call | Threads | Rows/s |
| --- | --- | --- |
| `sd.generate(profile, rows=10_000_000, seed=42, threads=N)` | 16 | 44M to 72M |
| the same | 1 | 7.6M to 9.7M |
| `doppeldata data generate eight-columns --rows 10000000 --seed 42 --file-rows 1000000 --threads N` to local disk | 16 | 16.3M to 18.9M |
| the same | 1 | 3.50M to 3.55M |

The write is ZSTD Parquet, which the encoder dominates. Its rates match the writers' own into
memory, so the CLI and the SDK add nothing measurable to a write.
