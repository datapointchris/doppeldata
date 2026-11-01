# Generating from Python and the command line

Rows come first, from a line of column names. Each section after the first adds one thing the one
before it lacks, and none is needed to generate.

## A list of column names generates rows, each column taking the fake its name hints

```python
import doppeldata as sd

table = sd.generate(['name', 'email', 'city', 'zip'], rows=1000)
```

```bash
doppeldata data generate --columns name,email,city,zip --rows 1000 --out people.csv
```

- **A name takes the general pack's type whose hint ends it, else a provider.** `customer_zip` is a
  `us_zip` and `email_address` an `email`. A provider matches its whole name, such as `city`, or an
  alias in `PROVIDER_HINTS` ending the name, such as `street` for `street_address`. So
  `billing_city` draws cities, and `city_count` matches nothing.
- **A provider's own name draws that provider where its type draws another.** So `free_email` and
  `state` draw as `sd.fakes.free_email` and `sd.fakes.state` do. A type drawn by a pattern keeps
  it: `phone_number` takes the type's pattern over fake's numbers with extensions.
- **A name matching nothing is refused, naming the closest fakes**, as `InvalidProfile` in Python
  and exit 2 on the command line. A column of random text would pass for data until someone read it.
  Each says what to give instead where the name was written: a dict in Python, a short YAML on the
  command line.

  ```text
  /columns/zipp: 'zipp' names no fake; give it one from sd.fakes, sd.integers, sd.decimals, sd.dates or sd.choice in a dict; the closest are zip_code [E-PROFILE-021]
  doppeldata: E-PROFILE-021: 'age' names no fake; give it a fake, a low..high range such as 18..90, or a list of values, in a short YAML
  ```

- **Expanding the names imports no profiling engine.** The table of name hints sits in
  `name_hinted_types.py`, apart from the general pack, which imports pyarrow. So a job that only
  generates installs no pyarrow, Polars or DuckDB.

## A dict gives each column a fake, a range or a list of values

```python
table = sd.generate({
    'name': sd.fakes.name,
    'street': sd.fakes.street_address,
    'zip': sd.fakes.zip_code,
    'age': sd.integers(18, 90),
    'price': sd.decimals(0, 500, places=2),
    'signed_up': sd.dates('2023-01-01', '2025-12-31'),
    'plan': sd.choice(['free', 'pro', 'team'], weights=[0.7, 0.25, 0.05]),
    'score': sd.decimals(0, 1, nulls=0.05),
    'email': None,
}, rows=1000, seed=0)
```

- **`sd.fakes` holds one fake for each provider the engine draws**, so tab completion lists them.
  A test fails when the profile schema names a provider `sd.fakes` lacks.
- **`sd.integers`, `sd.decimals` and `sd.dates` draw between two ends, both included.**
  `places=` rounds a decimal. `sd.choice` draws each value at its weight, or each equally.
- **Each takes `nulls=`, the share of rows that are null.**
- **`None` takes the column's fake from its name**, as the list does.
- **Every call that generates takes the dict and the list**, Spark's included.

## A short YAML gives each column a fake, a range or a list, and every command reading a profile reads it

```yaml
rows: 1000
columns:
  name:
  street:
  zip: zip_code
  age: 18..90
  plan: [free, pro, team]
```

```bash
doppeldata data generate people.yaml --out people.csv
doppeldata data check people.yaml people.csv
```

- **A column's value is a provider, a `low..high` range, a list, or empty.** A range of integers,
  numbers or ISO dates is `sd.integers`, `sd.decimals` or `sd.dates`. A list is `sd.choice`. Empty
  takes the name's fake.
- **`0..1` is a range of integers.** Write `0.0..1` for decimals.
- **Null shares, weights and decimal places need the dict or the expanded profile.** A short YAML
  holds none of them.
- **`rows` is the count `--rows` defaults to.** The table is named for the file, so this one is
  `people`.
- **A file holding `kind` is read as a full profile.**

## A write to one local file writes that file alone

`--out people.csv` writes `people.csv` and nothing beside it. The suffix is the name of a format
`doppeldata schemas show manifest` lists, such as `.csv` or `.parquet`. `--manifest`, or
`manifest=True` on `sd.write`, writes `_manifest.json` beside the file. A directory or S3
destination always holds one.

- **The path alone decides.** A suffixed path with no trailing `/` is one file, and
  `--out events.parquet/` is a directory. A path with no format suffix, such as `--out people`, is
  a directory too. An existing directory at a one-file path is refused as
  `E-WRITE-004`, exit 2, and `--replace` does not remove it.
- **The file is staged in `.people.csv.doppeldata-staging` beside it, then moved into place
  whole.** A write that fails, or that Ctrl-C or SIGTERM stops, removes the staging directory. A
  write killed outright leaves it. The next write to the file then refuses, and `--replace`
  removes it.
- **`--replace` writes over the file**, whether or not doppeldata made it.
- **A manifest recording only the file goes when the file is replaced**, with or without
  `--manifest`. So no manifest is left naming rows the file no longer holds. It goes before the
  file moves.
- **A directory whose manifest records other files refuses the write, exit 2, even with
  `--replace`.** That directory is their dataset, and a reader of it would count the file's rows
  among theirs. Write the file to another directory.
- **`data check` finds the manifest beside the file** where it records that file alone, as it finds
  one in a directory.
- **One file refuses what would split it, exit 2.** That is `--file-rows`, `--partition-by`, a
  `--format` its suffix does not name, and a scenario op that shapes the files. Writer settings
  that change the file's extension are refused too, such as gzip, which writes `.csv.gz`.
- **`sd.spark.write` refuses one file**, with `E-WRITE-008`. Its executors write their
  own objects, and none of them can move a staged file into place.
- **No rows write no file**, as they write none in a directory.

## `profile expand` writes the full profile, to edit once a column needs more

```bash
doppeldata profile expand people.yaml --out people.profile.yaml
doppeldata profile expand --columns name,email,city,zip --rows 1000
```

```python
sd.Profile.expand({'name': None, 'age': sd.integers(18, 90)}, rows=1000).save('people.profile.yaml')
```

The list, the dict and the short YAML each expand to an ordinary profile, which is what generates.
So `data generate people.profile.yaml` writes the same bytes as `data generate people.yaml`. The
manifest, determinism, `data check`, scenarios and Spark read it as any profile. From there, a
column takes any source [column-samplers.md](column-samplers.md) lists, and `profile create`
profiles real data into the same format.

- **Every column gets a `physical_type` and a `logical_type`.** `data check` reads a column's kind
  from its logical type, so a column without one fails `kind_changed`.
- **A named type writes its `semantic.type` beside the hint.** A column the general pack types
  then passes `data check`'s semantic type comparison.
- **`sd.integers` writes one histogram bin, half a unit beyond each end.** Extremes alone draw a
  number between them, which rounds to each end at half the weight of the values between.
- *Rejected:* `sd.expand`. A name on `Profile` says what it returns.
- *Rejected:* `sd.profile` taking a dict. It profiles real data, where a dict reads as rows.

## A schema a project already has generates rows, as a seed profile

```python
sd.generate(OrdersSchema, rows=1000)
```

```bash
doppeldata profile import --from odcs orders.odcs.yaml --out orders.profile.yaml
```

A pandera, dataframely or Spark schema goes wherever a profile is taken. `profile import` writes a
seed from a schema file or a Glue table, and `--help` lists the kinds it reads.
[seed-profiles-from-schemas.md](seed-profiles-from-schemas.md) says what each source's declarations
become and what a column the schema is silent on draws.

## A profile exports as the contract other tools validate with, and a write can carry it

```bash
doppeldata profile export orders --format pandera --out orders_schemas.py
doppeldata data generate orders --format parquet --embed-profile --out orders/
```

`profile export` writes the contract in each format `--format` takes, and `sd.export` returns the
same text. `--embed-profile`, or `embed_profile=True` on
`sd.write`, puts the profile in each Parquet and Arrow file, and every PROFILE argument reads it
back. Neither happens unless asked.
[profile-exports-and-embedded-profiles.md](profile-exports-and-embedded-profiles.md) shows each
format and what each leaves out.

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
- **The values are the Arrow values every format writes.** A missing-value token exists only as
  text, so the table holds its rows as null. CSV and JSON print it.
- *Rejected:* returning a pyarrow table. pyarrow would become a dependency of every job that only
  generates, and a Polars or DuckDB job reads the capsule without it.
- *Rejected:* arro3-core. pyo3-arrow's classes already carry the capsule interface, so arro3 would
  add a package and no reader.

## `sample` pins columns to given values, and every column drawn from a pinned one reads them

```python
import datetime as dt

import doppeldata as sd

profile = sd.Profile.load('crates/core/tests/every-column-kind.profile.yaml')
rows = sd.sample(profile, rows=50, seed=1, overrides={'channel': 'store', 'order_date': dt.date(2025, 3, 1)})
```

```text
channel  paid_amount  order_date  ship_date
store    13.295413    2025-03-01  2025-03-01
store    4.389998     2025-03-01  2025-03-02
store    0.0          2025-03-01  2025-03-02
store    0.0          2025-03-01  2025-03-05
```

`paid_amount` draws through its quantiles given `channel`, so all 50 rows are store-shaped: their median is 3.74 where
`generate`'s is 9.32. `ship_date` is `order_date` plus its lag, so it counts from the pinned date.

- **A value pins every row. Any other iterable except a str or bytes holds one value per row.** It must hold exactly `rows`
  values.
- **Each value is the column's own type**: a str, int, float, Decimal, bool, date, datetime, time, or None for a null. A
  float column takes an int, and a decimal column an int or a float with no more places than its scale. A Python type
  outside this list raises TypeError. A value whose kind the column does not take, or that the column cannot hold, raises
  `UngenerableProfile`, a ValueError whose issue is `E-USAGE-104` at the column's pointer. So does a list, struct or
  type-mix column, which no value pins:

  ```text
  column "quantity" holds int64 values, and the override gives it a string "five"
  the override of column "quantity" holds 3 values, and the sample is 50 rows
  column "tags" holds list<element: string> values, which no override pins
  ```

- **A pinned column takes its place in the draw order.** The engine lays it in where its draw would be, so a conditional
  table, quantiles, zero and null rates, a lookup, a lag and an expression each read the pinned values. A column reading
  no pinned column holds exactly the values `generate` gives.
- **A pinned copula member leaves its group's other members as drawn.** The group draws every member's level together, and
  the pinned member's goes unused. So its correlation with the group is not held.
- **A scenario's ops apply after the pins**, so an op writing a pinned column moves it.
- **Without overrides, `sample` returns what `generate` returns.** A Bundle is refused, because overrides name one table's
  columns.
- **The identity names the pins by their digest**, so the table's `doppeldata.dataset_digest` is its own. Its schema
  carries that digest under `doppeldata.overrides_digest`, and the pinned columns under `doppeldata.overridden_columns`.
  The digest is taken over the overrides sorted by column, so listing them in another order moves nothing.
- *Rejected:* `overrides=` on `generate`. Every table `generate` returns is a dataset its manifest regenerates, and a
  manifest records no overrides, only their digest.
- *Rejected:* replacing a column after the engine returns. A conditional child would keep the values it drew from the
  parent's own draws.
- *Rejected:* drawing a copula group's other members given the pinned value. They would move although they read no pinned
  column, and a list of values has no one level to condition on.

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
- **The uploads retry as boto3 does.** They make `ATTEMPTS` attempts, and give up connecting
  after `CONNECT_TIMEOUT`. `aws_access.py` holds both, beside the client config every boto3 client
  of doppeldata's takes.
- **A write through boto3's chain stores the same bytes as a local write with the same flags.**
- **boto3 is imported only for an S3 destination**, and declared nowhere. Without it, `plan` and
  `write` both refuse with `E-USAGE-019`, exit 2.
- *Rejected:* obstore. pyo3-object_store builds the same `S3Store` inside doppeldata's own extension,
  from the object_store the writers already link. obstore would be a second compiled copy in every
  job, and it takes credentials through the same callable.

## `data plan` prints what `data generate` writes with the same flags

```bash
doppeldata data plan orders --rows 250000 --seed 7 --file-rows 100000 --out orders/
doppeldata data generate orders --rows 250000 --seed 7 --file-rows 100000 --out orders/
```

`PROFILE` is a profile file, a short YAML or a bundled example's name, and `--columns` stands in for
it. `--out` is a local directory, a local file named for its format, an `s3://` or `s3a://` URL, or
`memory://`, which keeps nothing and times a write. `data plan` reads nothing at
`--out`. It prints the dataset digest and identity, the format with every writer setting, each
column's sampler and null fraction, each row range, and an estimate of the bytes. A test runs both
commands and requires the manifest's identity and ranges to equal the plan's.

- **`--json` traces each column as the engine planned it.** Each holds its sampler's kind, a
  summary, and every profile field the sampler reads as `{name, field, value}`. `field` is the
  field's JSON pointer, and `value` is read from the profile with the scenario's patch applied. A
  derived, lookup or conditional column adds the dependence entries it reads. `ops` names the id
  of each op writing the column, and `patched_fields` each field the patch sets.
- **`origin` is `pinned`, `overlaid`, `derived` or `profiled`, the first that holds.** A pinned
  column takes the values a caller gave. An overlaid one is changed by the scenario's patch or ops.
  A derived one is computed from the columns `reads` names. So a derived column the scenario
  patches is `overlaid`, and `reads` still names what it follows.

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

## A bundle generates every table parents first, sized by `--scale`

```bash
doppeldata data plan orders-customers --scale 0.1 --out shop/
```

```text
dataset    sha256:…
identity   59,000 rows at scale 0.1 from seed 0, bundle sha256:…, stream version …

customers  4,000 rows as parquet, up to 1 file to shop/customers, estimate 48.1 KB
orders     55,000 rows as parquet, up to 1 file to shop/orders, estimate 896.4 KB

orders.customer_id → customers.customer_id
```

```python
import doppeldata as sd

tables = sd.generate(sd.Bundle.example('orders-customers'), scale=0.1, seed=7)
tables['orders']  # an Arrow table of 55,000 rows, each customer_id one of tables['customers']'
```

`PROFILE` may be a bundle file, a bundle directory or a bundled example's name, on `data plan`,
`data generate` and `data check`. `profile list` lists the example bundles beside the profiles,
and `profile show` prints one.

**Python takes a bundle only as a `Bundle`**, which `Bundle.load` reads from a file or directory
and `Bundle.example` from an example's name. So a string or path given to `generate`, `plan` or
`write` always names one table, and each call's return type follows from its argument. `check`
takes a `Profile` or a `Bundle`.

- **`--scale` multiplies every table's row count**, 1 where left out. Each table generates
  `round(rows × scale)` rows, so the fan-out's shape holds at any scale. Each `top_fractions`
  holds over the same fraction of the parents, so `top10` of 40,000 is the busiest one of 4,000
  at scale 0.1.
- **`--rows`, `--columns`, `--manifest` and any partition flag are refused with a bundle.** `--rows`
  sizes one table, and each table is partitioned as its own profile records.
- **`generate()` returns a dict of each table's name to its table, parents first.** `write()`
  returns a `BundleManifest`, and `plan()` a `BundleWritePlan`. `stream()` refuses a bundle,
  because a stream holds one table.
- **A plan lists each relationship, and what draws it instead where another does.** A composite
  key's single-column part is drawn by the composite, and the plan names it set aside.

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
- **Under `cdc`, `data plan` prints a `stream` line, and `--json` holds it as `cdc`.** It draws
  every change to count them by operation, and counts the change files and `_expected` parts the
  write makes at most. The ranges are the `LOAD` files, the writer shows the stream's
  `timestamp_unit`, and the file count and the estimate take in the stream. A stopped write's
  report counts the change files and `_expected` parts its partial manifest lists.
- **`--rows` counts the rows generated.** A `duplicates` op writes copies beside them, so
  `data generate` then names both counts.
- **`data check` refuses a `--scenario` other than the one DATA was written under, exit 2.** It
  reads the scenario's digest from the `_manifest.json` the write left.
- **`scenario list` prints every op, every metric an expectation names, and every bundled example.**
  `scenario show` prints an op's parameters with their types and meanings, or an example's YAML.
- **`scenario check SCENARIO PROFILE` plans a write without generating a row.** It takes `data
  plan`'s `--rows`, `--seed`, `--format`, `--file-rows`, `--partition-by` and `--no-partitions`,
  so it refuses what `data generate` would with the same flags. A refusal exits 3 as
  `E-SCENARIO-019`, and `--json` prints `passed` and each issue as
  [issue-codes.md](issue-codes.md) describes one.

## A stopped write's code decides its exit status

The engine names each failure with a code, such as `E-WRITE-204`. The registry declares each code
with its exit status, so `exit_code` is read from the code and nothing else:

| Exit | Meaning | Such as |
| --- | --- | --- |
| 2 | The call must change. Nothing was written. | `E-WRITE-204`, the destination holds objects |
| 1 | The write was attempted and failed. | `E-IO-203`, the store answered with an error |
| 4 | The store stopped answering, before the write or during it. | `E-IO-204` |

- **A 5xx or 429 on every retry during the write reads as `E-IO-203`, exit 1.** object_store keeps
  the status inside an error type it does not export. Before the write, boto3's bucket check reads
  it as `E-IO-204`, exit 4.
- **The CLI reports every doppeldata error in its root group.** So each verb exits on the same
  table, and a new verb needs no handler of its own. [issue-codes.md](issue-codes.md) gives the
  text and JSON it prints.

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
