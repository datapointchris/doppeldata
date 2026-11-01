# doppeldata

Profile a dataset once, commit the profile, and generate synthetic data from it whenever a test or a
benchmark needs some.

A profile is a YAML file holding a dataset's types, distributions, skew, null and error rates, the
dependence between its columns, its file layout and its table relationships. It holds no rows.
doppeldata reads CSV, TSV, JSON, Parquet, Arrow and Spark DataFrames. It writes any of them back at
any size: in memory, to disk, to S3, or inside Spark executors.

A scenario layered on a profile bends the data on purpose: a hot join key, late rows, a flood of
small files, or broken rows a pipeline must quarantine. The same profile, scenario and seed always
produce the same data.

doppeldata is experimental. Each release's notes list what it does. Its commands and its profile
format change without notice until 1.0.

## Each step up takes one change

### Column names alone generate rows, each taking the fake its name hints

```python
import doppeldata as sd

table = sd.generate(['name', 'email', 'city', 'zip'], rows=1000)   # an Arrow table
```

```text
$ doppeldata data generate --columns name,email,city,zip --rows 1000 --out people.csv
wrote 1,000 rows as csv in 1 file, 61.3 KB, in 0.00 s on 16 threads: 1.00M rows/s
people.csv holds dataset sha256:…
$ head -4 people.csv
name,email,city,zip
Arlie Larson,heidi.reichert@example.net,Rempelton,65947
Cooper Bechtelar,brett.bernhard@example.com,Casimirville,46712
Roselyn Torphy,mitchel.dickens@example.net,Kuvaliston,01433
```

### A dict or a short YAML says more about each column inline

```python
import polars as pl

import doppeldata as sd

table = sd.generate({
    'name': None,
    'email': None,
    'age': sd.integers(18, 90),
    'plan': sd.choice(['free', 'pro', 'team'], weights=[0.7, 0.25, 0.05]),
    'monthly_spend': sd.decimals(0, 500, places=2, nulls=0.1),
    'signed_up': sd.dates('2023-01-01', '2025-12-31'),
}, rows=1000, seed=7)
print(pl.DataFrame(table).head(3))
```

```text
shape: (3, 6)
┌───────────────┬─────────────────────────────────┬─────┬──────┬───────────────┬────────────┐
│ name          ┆ email                           ┆ age ┆ plan ┆ monthly_spend ┆ signed_up  │
│ ---           ┆ ---                             ┆ --- ┆ ---  ┆ ---           ┆ ---        │
│ str           ┆ str                             ┆ i64 ┆ str  ┆ f64           ┆ date       │
╞═══════════════╪═════════════════════════════════╪═════╪══════╪═══════════════╪════════════╡
│ Bria Kihn     ┆ damaris.stoltenberg@example.or… ┆ 35  ┆ pro  ┆ 454.35        ┆ 2024-05-15 │
│ Wiley Steuber ┆ jaylon.maggio@example.com       ┆ 60  ┆ pro  ┆ 249.79        ┆ 2025-01-12 │
│ Claude Larkin ┆ samir.auer@example.net          ┆ 18  ┆ free ┆ 423.07        ┆ 2023-10-21 │
└───────────────┴─────────────────────────────────┴─────┴──────┴───────────────┴────────────┘
```

A short YAML gives each column a fake, a `low..high` range or a list of values:

```yaml
# people.yaml
rows: 1000
columns:
  name:
  email:
  city:
  zip:
  age: 18..90
  plan: [free, pro, team]
```

```text
$ doppeldata data generate people.yaml --out people.csv
wrote 1,000 rows as csv in 1 file, 69.3 KB, in 0.00 s on 16 threads: 0.25M rows/s
people.csv holds dataset sha256:…
$ head -4 people.csv
name,email,city,zip,age,plan
Kelsie Senger,annabel.mueller@example.com,South Golden Hyattshire,77030,55,pro
Stephan Borer,miles.carroll@example.com,Fisherport,18553,39,team
Floyd Mayert,jane.feil@example.com,Hicklemouth,50574,32,team
```

### A profile of real data on S3 replaces the dict

```python
profile = sd.profile('s3://your-bucket/orders/', aws_profile='analytics')
```

```text
$ doppeldata profile create s3://your-bucket/orders/ --aws-profile analytics --out orders.profile.yaml
$ head -17 orders.profile.yaml
kind: table
name: orders
generated:
  tool: doppeldata <version>
  at: '2026-11-01T08:43:10Z'
  engine: polars
  fitter: scipy 1.18.1
  detector_packs: {general: doppeldata <version>}
  readers: {polars: 2.0.0, pyarrow: 26.0.0}
  source_fingerprint: sha256:…
  sample: {strategy: full}
privacy:
  policy: default
  k_min: 11
  suppressed:
    - {column: customer_email, reason: identifier, semantic: email}
  audit:
```

The emails were typed as identifiers, so the profile keeps their format and no address. Without
`--out`, the profile prints as YAML.

### Rows generated from the profile take the real data's shape

```python
table = sd.generate(profile, rows=100_000, seed=7)
```

```text
$ doppeldata data generate orders.profile.yaml --rows 100000 --seed 7 --out synthetic/orders/
wrote 100,000 rows as parquet in 20 files, 3.4 MB, in 0.07 s on 16 threads: 1.35M rows/s
synthetic/orders/_manifest.json names dataset sha256:…
```

The Python table is the same dataset, with the SHA-256 the command printed. A dataset is a pure function of its
profile, row count and seed.

### The saved YAML is reviewed, committed, and generates the same rows anywhere

```python
profile.save('orders.profile.yaml')
again = sd.generate(sd.Profile.load('orders.profile.yaml'), rows=100_000, seed=7)   # the same dataset
```

```text
$ doppeldata profile check orders.profile.yaml
# doppeldata profile check passed

`orders.profile.yaml`, 1 table.

## orders

25 literal fields under policy `default`, `k_min` 11, and 0 issues.
```

`profile check` reads the file alone and exits 3 on a privacy or schema violation, so CI can run it
on every committed profile. [Review and commit a profile](docs/how-to/review-and-commit-a-profile.md)
reads each section.

### Spark, scenarios, checks and exports each take one more argument or command

| Change | Does | How-to |
| --- | --- | --- |
| `sd.spark.write(spark, profile, 's3://your-bucket/spark/orders/', rows=1_000_000, seed=7)` | writes the files `data generate` writes, from Spark's executors | [Generate inside Spark or AWS Glue](docs/how-to/generate-inside-spark-or-glue.md) |
| `--scenario hot-key` | five customers place 60% of the orders | [Bend data with a scenario](docs/how-to/bend-data-with-a-scenario.md) |
| `doppeldata data check orders.profile.yaml synthetic/orders/` | measures written data against its profile | [Check generated data](docs/how-to/check-generated-data.md) |
| `doppeldata profile export orders.profile.yaml --format pandera --out orders_schemas.py` | the profile as pandera models | [Export a profile as a data contract](docs/how-to/export-a-profile-as-a-contract.md) |

```text
$ doppeldata data generate orders.profile.yaml --rows 100000 --seed 7 --scenario hot-key --out hot/
wrote 100,000 rows as parquet in 20 files, 3.2 MB, in 0.09 s on 16 threads: 1.08M rows/s
hot/_manifest.json names dataset sha256:…
$ doppeldata data check orders.profile.yaml hot/ --scenario hot-key
# doppeldata check passed
...
0 of 1 expectations failed.

| Expectation | Column | Value | Bounds | Passed |
| --- | --- | --- | --- | --- |
| hot_keys.fraction | customer_id | 0.59937 | ≥ 0.55 | yes |
```

## Install

The command line:

```text
$ uv tool install --index https://pypi.ichrisbirch.com/simple/ doppeldata-cli
$ doppeldata --version
doppeldata <version>
```

The command ships in the `doppeldata-cli` distribution, so a one-off run names both:

```bash
uvx --index https://pypi.ichrisbirch.com/simple/ --from doppeldata-cli doppeldata --version
```

Every release is published to that package index, and nothing else is. Name it on every install.
PyPI does not carry doppeldata, so a package of that name there is someone else's.

- **`data check --html` also needs sweetviz**, which the command line does not declare:
  `uv tool install --index https://pypi.ichrisbirch.com/simple/ --with sweetviz doppeldata-cli`.
- **`--engine spark` needs pyspark beside the command line.**
  [Profile with Spark or AWS Glue](docs/how-to/profile-with-spark-or-glue.md) installs it.

The library, for code that profiles or generates data itself:

```bash
uv add --index https://pypi.ichrisbirch.com/simple/ doppeldata
```

The library installs no profiling engine, so a job that only generates carries none. Code that calls
`doppeldata.profile()` installs them beside it:

```bash
uv add polars duckdb pyarrow
uv add boto3                          # to profile an s3:// prefix, or write to S3
uv add clevercsv charset-normalizer   # to profile CSV or TSV files
uv add scipy                          # to fit each numeric column to a distribution family
uv add sweetviz                       # for check(..., html=...), the side-by-side HTML
```

Each is refused by name when a call needs it and it is missing. `profile(engine='spark')` needs
pyspark and pyarrow in place of polars, and duckdb only to sniff a CSV or TSV.

The library runs on Python 3.11 and later, and the command line on 3.13 and later. The library's
generator is compiled Rust. A wheel is built for each platform the `wheels` job in
`.github/workflows/build-library.yml` lists. Anywhere else, the install builds the sdist, which
needs a Rust toolchain.

### Run the library inside an AWS Glue job

Every Glue version that runs Python 3.11 or later installs the library, and
[aws-glue-support.md](docs/aws-glue-support.md) lists them. Glue compiles nothing, so download the
Linux x86_64 wheel from the index alone, and upload it to S3:

```text
$ pip download --no-deps --index-url https://pypi.ichrisbirch.com/simple/ \
    --platform manylinux2014_x86_64 --python-version 311 --only-binary=:all: \
    --dest wheels doppeldata
$ ls wheels/
doppeldata-<version>-cp311-abi3-manylinux2014_x86_64.manylinux_2_17_x86_64.whl
$ aws s3 cp --recursive wheels/ s3://your-bucket/glue/
```

Then pass its path in the job's parameters, with the file name pip downloaded. Glue installs the
wheel, and its dependencies from PyPI:

```text
--additional-python-modules  s3://your-bucket/glue/doppeldata-<version>-cp311-abi3-manylinux2014_x86_64.manylinux_2_17_x86_64.whl
```

[Profile with Spark or AWS Glue](docs/how-to/profile-with-spark-or-glue.md) and [Generate inside
Spark or AWS Glue](docs/how-to/generate-inside-spark-or-glue.md) run whole jobs, and list the
modules each names after the wheel.

Never give pip the package index as an extra index beside PyPI. pip installs the highest version it
finds on either, so anyone uploading a `doppeldata` to PyPI would run their code in the job.

A job with no internet access installs from a zip of wheels instead. Name the downloaded wheel by
its path, so pip never asks PyPI for doppeldata:

```bash
pip download --dest wheels --platform manylinux2014_x86_64 --python-version 311 \
  --only-binary=:all: wheels/doppeldata-<version>-cp311-abi3-manylinux2014_x86_64.manylinux_2_17_x86_64.whl
echo doppeldata > wheels/requirements.txt
python -m zipfile -c doppeldata-<version>.gluewheels.zip wheels/
```

Upload the zip, pass its `s3://` path to `--additional-python-modules`, and pass `--no-index` to
`--python-modules-installer-option`. Glue then resolves every dependency from the zip.

## First run

Profile a table you hold, read the profile, then generate from it and check what was written. With
no data at hand, `doppeldata data generate orders --rows 20000 --seed 1 --file-rows 5000 --out
data/orders/` writes the table these commands ran on, from the `orders` example.

```bash
doppeldata profile create data/orders/ --out orders.profile.yaml
doppeldata profile check orders.profile.yaml
doppeldata data plan orders.profile.yaml --rows 1000000 --seed 7 --out synthetic/orders/
doppeldata data generate orders.profile.yaml --rows 1000000 --seed 7 --out synthetic/orders/
doppeldata data check orders.profile.yaml synthetic/orders/
```

Each column's section says what the generator draws. A category keeps its top values and their
shares, each resting on at least `k_min` rows:

```text
$ yq '.columns[] | select(.name == "status") | .string.top_values' orders.profile.yaml
- {value: delivered, fraction: 0.462}
- {value: shipped, fraction: 0.2175}
- {value: processing, fraction: 0.14}
- {value: cancelled, fraction: 0.1013}
- {value: returned, fraction: 0.0793}
```

`privacy.audit` lists every literal a column keeps and the rule that admits it:

```text
$ rg 'column: (status|ship_zip)' orders.profile.yaml
    - {column: status, field: /string/top_values, values: 5, rule: count >= k_min, min_rows: 1586}
    - {column: status, field: /string/shapes, values: 4, rule: count >= k_min, min_rows: 1586}
    - {column: ship_zip, field: /string/shapes, values: 1, rule: count >= k_min, min_rows: 20000}
    - {column: ship_zip, field: /string/pattern, values: 1, rule: generalized}
```

`data plan` prints what `data generate` writes with the same flags, and reads nothing at `--out`:

```text
$ doppeldata data plan orders.profile.yaml --rows 1000000 --seed 7 --out synthetic/orders/
dataset    sha256:…
identity   1,000,000 rows from seed 7, profile sha256:…, stream version <n>
format     parquet {"compression":"zstd","compression_level":null,"dictionary_columns":null,"row_group_rows":5000,"statistics":"page","timestamp_unit":"us"}
files      200 files to synthetic/orders/
estimate   34.7 MB, from the first blocks encoded with these settings

order_id        int64                  sequence from 1 by 1
customer_id     int64                  quantiles with fitted randint tails
amount          decimal128(12, 2)      quantiles with fitted lognorm tails
quantity        int32                  10 top values
status          string                 5 top values
ordered_at      timestamp[us, tz=UTC]  between the extremes, weighted by month, weekday, hour
customer_email  string                 the email provider, at the profiled distinct ratio, 0.019 null
ship_zip        string                 a pattern
...
$ doppeldata data generate orders.profile.yaml --rows 1000000 --seed 7 --out synthetic/orders/
wrote 1,000,000 rows as parquet in 200 files, 34.5 MB, in 0.54 s on 16 threads: 1.87M rows/s
synthetic/orders/_manifest.json names dataset sha256:…
$ doppeldata data check orders.profile.yaml synthetic/orders/
# doppeldata check passed

`synthetic/orders/`, 1,000,000 rows, against profile `orders` (sha256:…).

0 of 31 metrics failed.
```

- **The plan and the write name the same dataset.** `_manifest.json` records it with the profile's
  SHA-256, the seed, the row count and the stream version. `data generate PROFILE --manifest FILE`
  writes it again from them, and exits 1 naming any file whose bytes moved.
- **The profile's `source` section records how the files were written**, and `data generate` writes
  it back: a CSV's dialect and encoding, a Parquet footer's codec and row groups, the rows per file.
- **`profile check` and `data check` exit 0 when clean and 3 on a finding**, so CI can run either.
  Each prints a Markdown report a pull request takes as it is.
- **Every command's `--help` ends in examples, and every command takes `--json`.**

## Every task has a page

| Task | Page |
| --- | --- |
| Generate rows from column names, a dict or a short YAML | [generate-rows-from-column-names.md](docs/how-to/generate-rows-from-column-names.md) |
| Profile a dataset, local or on S3 | [profile-a-dataset.md](docs/how-to/profile-a-dataset.md) |
| Review a profile's privacy audit and commit it | [review-and-commit-a-profile.md](docs/how-to/review-and-commit-a-profile.md) |
| Profile claims and other healthcare data under HIPAA Safe Harbor | [profile-healthcare-data.md](docs/how-to/profile-healthcare-data.md) |
| Type columns with your own detector pack | [write-a-detector-pack.md](docs/how-to/write-a-detector-pack.md) |
| Import a seed profile from a schema or a Glue table | [import-a-seed-from-a-schema.md](docs/how-to/import-a-seed-from-a-schema.md) |
| Generate a dataset in any format, locally or to S3 | [generate-a-dataset.md](docs/how-to/generate-a-dataset.md) |
| Reproduce a dataset from its manifest or its files | [reproduce-a-dataset.md](docs/how-to/reproduce-a-dataset.md) |
| Profile and generate related tables with their foreign keys | [generate-related-tables.md](docs/how-to/generate-related-tables.md) |
| Read and edit how columns depend on each other | [edit-dependent-columns.md](docs/how-to/edit-dependent-columns.md) |
| Bend data with a scenario | [bend-data-with-a-scenario.md](docs/how-to/bend-data-with-a-scenario.md) |
| Write broken files a pipeline must quarantine | [write-broken-files.md](docs/how-to/write-broken-files.md) |
| Write an AWS DMS change stream and the table it must merge to | [write-a-cdc-stream.md](docs/how-to/write-a-cdc-stream.md) |
| Check generated data against its profile, or the real data | [check-generated-data.md](docs/how-to/check-generated-data.md) |
| Measure drift between two profiles | [measure-drift-between-profiles.md](docs/how-to/measure-drift-between-profiles.md) |
| Export a profile as a data contract | [export-a-profile-as-a-contract.md](docs/how-to/export-a-profile-as-a-contract.md) |
| Test with synthetic data in pytest | [test-with-synthetic-data.md](docs/how-to/test-with-synthetic-data.md) |
| Profile with Spark, over Spark Connect, or in a Glue job | [profile-with-spark-or-glue.md](docs/how-to/profile-with-spark-or-glue.md) |
| Generate inside Spark's executors or a Glue job | [generate-inside-spark-or-glue.md](docs/how-to/generate-inside-spark-or-glue.md) |
| Drive doppeldata from a script: capabilities, refusals, the plan trace and logging | [drive-doppeldata-from-a-script.md](docs/how-to/drive-doppeldata-from-a-script.md) |

[docs/README.md](docs/README.md) indexes the design pages, which say why each part is built as it
is.

## Driving doppeldata from an agent

A program or a coding agent drives doppeldata through `--json` and the exit status alone. This
loop profiles a table, bends it with a scenario, and verifies the result. A table generated from
the `orders` example stands in for real data:

```bash
doppeldata capabilities show --json   # everything this install takes, read from the registries that refuse a value outside them
doppeldata data generate orders --rows 20000 --seed 1 --out real/ --json                  # the manifest
doppeldata profile create real/ --out orders.profile.yaml --json
doppeldata profile check orders.profile.yaml --json                       # "passed": true, or each issue at its field
doppeldata scenario check hot-key orders.profile.yaml --json              # "passed": true
doppeldata data plan orders.profile.yaml --scenario hot-key --seed 7 --out out/ --json
doppeldata data generate orders.profile.yaml --scenario hot-key --seed 7 --out out/ --json  # the manifest
doppeldata data check orders.profile.yaml out/ --scenario hot-key --json  # "passed": true, "verified": true
```

- **`capabilities show --json` is read from the installed doppeldata**, so it matches that version
  exactly. Each op lists the formats a write takes it in, and each issue code its exit status and
  hint.
- **Every refusal names a registered `E-<AREA>-<NNN>` code.** Under `--json` each issue prints on
  stderr as one line, with the field refused, the value given, every valid value, the closest of
  them and a hint. [Issue codes](docs/issue-codes.md) says what each exit status means.
- **Every write verb takes `--json` too.** `data generate` prints the manifest it wrote. `profile
  create`, `import` and `expand` print the profile as JSON when no `--out` is given.
- **`data plan --json` traces each column.** It names the sampler's kind, each profile field the
  sampler reads with its pointer and value, and the id of each op writing the column. `origin` says
  whether the column is profiled, derived from others, overlaid by the scenario or pinned.
- **Without `--json`, `profile check`, `scenario check`, `data check` and `profile diff` print
  GitHub Markdown**, so a report pastes into a pull request as it is.

[Drive doppeldata from a script](docs/how-to/drive-doppeldata-from-a-script.md) reads each of these
from a program.

## Layout

| Directory | Holds |
| --- | --- |
| `crates/` | the Rust workspace: `core`, the generation engine; `writers`, which writes its blocks as files to any object store; and `python`, the extension module over the engine |
| `library/` | the `doppeldata` distribution: the compiled extension and the Python code over it |
| `cli/` | the `doppeldata-cli` distribution: the `doppeldata` command over the library |
| `docs/` | the how-to pages under `docs/how-to/`, and the design pages saying why the design is what it is |
| `benchmarks/` | the scripts `task bench` runs to time the engine against numpy, Faker and Mimesis |
| `spark-parity/` | the checks that `doppeldata.spark` gives a local run's values and files, and that `profile(engine='spark')` gives Polars' profile, inside each Spark image named |
| `malformed-file-readers/` | what DuckDB and each Spark image named make of every fault kind `malformed_rows` writes |

## Develop

```bash
task setup    # builds the extension and installs both distributions into .venv
task test     # cargo test, then pytest
task lint     # ruff, mypy, rustfmt and clippy, writing nothing
```

A release is cut from `main` by the commit messages: `feat:` cuts a minor and `fix:` a patch. The tag
is the version of both distributions.

## License

MIT
