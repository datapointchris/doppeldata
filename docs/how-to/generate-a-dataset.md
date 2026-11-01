# Generate a dataset from a profile

`data plan` says what a write will make, and `data generate` makes it: files and a
`_manifest.json`, in a local directory, one local file or S3. In Python, `sd.generate` and
`sd.stream` hand the rows over in memory instead.

```bash
doppeldata data plan orders --rows 250000 --seed 7 --file-rows 100000 --out orders/
doppeldata data generate orders --rows 250000 --seed 7 --file-rows 100000 --out orders/
doppeldata data generate orders.profile.yaml --rows 100000 --out s3://your-bucket/orders/ --aws-profile analytics
```

```python
import polars as pl
import pyarrow as pa

import doppeldata as sd

profile = sd.Profile.load('orders.profile.yaml')
plan = sd.plan(profile, 'orders/', rows=250_000, seed=7, file_rows=100_000)
manifest = sd.write(profile, 'orders/', rows=250_000, seed=7, file_rows=100_000)
sd.write(profile, 's3://your-bucket/orders/', rows=100_000, seed=7, aws_profile='analytics')

table = sd.generate(profile, rows=1_000_000, seed=7)        # an Arrow table, read with no copy
pl.DataFrame(table)
reader = pa.RecordBatchReader.from_stream(sd.stream(profile, rows=10**9, seed=7))   # larger than memory
```

PROFILE is a profile file, a short YAML, a bundle, a Parquet or Arrow file written with
`--embed-profile`, or an example's name. `doppeldata profile list` names the examples, so `orders`
above runs with no file at hand.

## `data plan` prints what `data generate` writes with the same flags

```text
dataset    sha256:…
identity   250,000 rows from seed 7, profile sha256:…, stream version <n>
format     parquet {"compression":"zstd","compression_level":null,"dictionary_columns":null,"row_group_rows":1048576,"statistics":"page","timestamp_unit":null}
files      3 files to orders/
estimate   6.6 MB, from the first blocks encoded with these settings

order_id        int64                  sequence from 1 by 1
customer_id     int64                  uniform between the extremes, at the profiled distinct ratio
amount          decimal128(12, 2)      quantiles with fitted lognorm tails
quantity        int32                  quantiles with linear tails
status          string                 5 top values
ordered_at      timestamp[us, tz=UTC]  between the extremes, weighted by month, weekday
customer_email  string                 the email provider, at the profiled distinct ratio, 0.02 null
ship_zip        string                 a pattern, at the profiled distinct ratio

part-00000-722fe1c2.parquet  rows 0 to 99,999
part-00001-722fe1c2.parquet  rows 100,000 to 199,999
part-00002-722fe1c2.parquet  rows 200,000 to 249,999
```

`data plan` reads nothing at `--out`. `data generate` with the same flags then prints the same
dataset SHA-256:

```text
wrote 250,000 rows as parquet in 3 files, 6.8 MB, in 0.18 s on 16 threads: 1.39M rows/s
orders/_manifest.json names dataset sha256:…
```

`sd.plan` returns that plan as a `WritePlan`, and `sd.write` returns the `Manifest` it stored.
`data plan --json` traces each column to the profile fields it reads, as
[Drive doppeldata from a script](drive-doppeldata-from-a-script.md) shows.

## The flags choose the format, the files and the layout, each from the profile where left out

`doppeldata data generate --help` prints each flag with its default, and `help(sd.write)` each
argument. A flag left out takes what the profile records of its source, else a default.

`doppeldata schemas show manifest` lists every format's writer settings. A CSV with a pipe and a
null token:

```bash
doppeldata data generate orders --rows 100000 --format csv --writer '{"delimiter":"|","null":"NULL"}' --out orders-csv/
```

```text
order_id|customer_id|amount|quantity|status|ordered_at|customer_email|ship_zip
1|49892|35.38|2|delivered|2025-01-07T20:08:14Z|madeline.mckenzie@example.net|16711
2|70442|309.13|4|delivered|2025-07-10T23:36:32Z|alberta.spinka@example.net|52023
```

A write partitioned by `status` writes a Hive directory per value:

```bash
doppeldata data generate orders --rows 100000 --partition-by status --out orders-by-status/
```

```text
_manifest.json
status=cancelled
status=delivered
status=processing
status=returned
status=shipped
```

`--out memory://` keeps nothing, so it times a write:

```text
$ doppeldata data generate orders --rows 10000000 --out memory://
wrote 10,000,000 rows as parquet in 10 files, 232.8 MB, in 1.76 s on 16 threads: 5.67M rows/s
```

## A destination holding anything is refused, unless `--replace` is given

```text
$ doppeldata data generate orders --rows 250000 --seed 7 --out orders/
doppeldata: E-WRITE-204: orders/ already holds 4 objects, such as _manifest.json; a write with replace set deletes the objects an earlier doppeldata write recorded
  hint: rerun with --replace, or replace=True, to replace what an earlier doppeldata write left there
```

`--replace` deletes only what an earlier doppeldata write recorded. Anything else at the
destination, such as Spark's `_SUCCESS`, still refuses the write, with nothing deleted.

## S3 is reached with the AWS profile `--aws-profile` names

`--aws-profile NAME`, or `aws_profile=` in Python, names the profile in the AWS config whose
credentials, region and endpoint reach S3. Left out, `AWS_PROFILE` names it, then boto3's default
chain. A name the config lacks is refused, exit 2, before anything is written. A write to S3 needs
boto3, which the library does not install.

## A write that stops leaves `_manifest.partial.json`, naming the files it finished

Ctrl-C and SIGTERM stop a write between blocks:

```text
doppeldata: E-WRITE-213: interrupted, so the write stopped before every file was written
doppeldata: big/_manifest.partial.json lists the 19 files the write completed
```

- **The exit status is the code's.** A refused call exits 2 with nothing written, a write that
  failed or stopped exits 1, and a store that never answered exits 4.
  [Issue codes](../issue-codes.md) gives the whole table.
- **A rerun is refused until `--replace`**, because the partial manifest stays. `data check` and
  `profile create` refuse a directory holding one too.
- **In Python, a write that stops raises a `StoppedWrite`**: `WriteInterrupted` for Ctrl-C, which
  is a `KeyboardInterrupt`, or `WriteFailed`. Each carries the partial manifest.
- **A process killed outright leaves its multipart uploads open in S3.** A bucket lifecycle rule
  for `AbortIncompleteMultipartUpload` deletes them.

## Why

[Generating from Python and the command line](../generating-from-python-and-the-cli.md) says why a
table crosses into Python with no Arrow dependency, how Ctrl-C reaches native code, and how S3
credentials reach the writers. [File writers and the manifest](../file-writers-and-manifest.md)
says how a run claims its destination, and [Determinism contract](../determinism-contract.md) why
the values never move with the thread count or the file split.
