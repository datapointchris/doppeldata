# doppeldata

Profile a dataset once, commit the profile, and generate synthetic data from it whenever a test or a
benchmark needs some.

A profile is a YAML file holding a dataset's types, distributions, skew, null and error rates, file
layout and table relationships. It holds no rows. doppeldata is built to read CSV, TSV, JSON,
Parquet, Arrow and Spark DataFrames, and to write any of them back at any size: in memory, to disk,
to S3, or inside Spark executors.

A scenario file layered on a profile bends the data on purpose: a hot join key, late rows, a flood
of small files. The same profile, scenario and seed always produce the same data.

Rows need no profile to start. Each column takes the fake its name hints:

```python
import doppeldata as sd

sd.generate(['name', 'email', 'city', 'zip'], rows=1000)   # an Arrow table of 1,000 people
```

```bash
doppeldata data generate --columns name,email,city,zip --rows 1000 --out people.csv
```

A dict sets ranges and categories, such as `{'age': sd.integers(18, 90), 'plan': sd.choice(['free', 'pro'])}`.
A short YAML gives each column a fake, a `low..high` range or a list of values. Null shares, weights
and decimal places need the dict. `doppeldata profile expand` writes the full profile any of them
stands for. [Generating from Python and the command line](docs/generating-from-python-and-the-cli.md)
climbs from there to a profile of real data.

doppeldata is experimental. So far it profiles a table's columns and its files, generates a table from a profile
under any scenarios, and ships the profile and scenario formats with hand-written examples of each:

```bash
doppeldata profile create data/orders/ --out orders.profile.yaml   # profile a directory of one table
doppeldata profile create s3://bucket/orders/ --sample 1000000     # a seeded sample, printed as YAML
doppeldata profile create data/orders/ --edges-from orders.profile.yaml --out orders.profile.yaml  # keep its bin edges
doppeldata profile create data/claims/ --policy healthcare --out claims.profile.yaml  # add HIPAA Safe Harbor
doppeldata profile create s3://bucket/claims/ --engine spark --out claims.profile.yaml  # compute it in Spark
doppeldata profile check orders.profile.yaml   # the file against its schema and privacy policy, from the file alone
doppeldata profile list                # the example profiles
doppeldata profile show orders         # one of them, as YAML to start a profile from
doppeldata profile expand people.yaml  # the full profile a short YAML of rows and columns stands for
doppeldata profile diff march.profile.yaml april.profile.yaml  # how far April drifted from March
doppeldata data check orders.profile.yaml out/orders/        # generated data against its profile
doppeldata data check orders.profile.yaml out/orders/ --against data/orders/ --html side-by-side.html  # and the real data
doppeldata schemas show profile        # the JSON Schema every profile validates against

doppeldata data plan orders --rows 1000000 --seed 7 --out orders/       # the files a write makes, and their size
doppeldata data generate orders --rows 1000000 --seed 7 --out orders/   # write them, and _manifest.json
doppeldata data generate orders.profile.yaml --rows 100000000 --format csv --out s3://bucket/orders/

doppeldata scenario list               # the ops a scenario applies, its metrics, and the example scenarios
doppeldata scenario check hot-key orders                            # a scenario against a profile, generating nothing
doppeldata data generate orders --scenario hot-key --out orders/    # five customers place 60% of the orders
doppeldata data check orders orders/ --scenario hot-key             # and the check verifies that share
```

`data generate` writes every format `doppeldata schemas show manifest` lists, to a local directory, a
local file or S3. A directory or S3 write records `_manifest.json` beside its files, and a one-file
write does so under `--manifest`. It holds the profile's digest, the scenario's, the seed, the row
count and the engine's stream version. Those regenerate the same values in any format. From Python, a table arrives through the Arrow PyCapsule interface, so
Polars, DuckDB and pyarrow 14 or later read it with no copy:

```python
import doppeldata as sd

profile = sd.Profile.load('orders.profile.yaml')
table = sd.generate(profile, rows=1_000_000, seed=7)
polars.DataFrame(table)
sd.write(profile, 's3://bucket/orders/', rows=100_000_000, seed=7)
```

Inside Spark, the executors generate the rows, with the values a local run gives at any partition
count. `sd.spark.write` writes the files `sd.write` writes, byte for byte, from the executors:

```python
frame = sd.spark.generate(spark, profile, rows=1_000_000_000, seed=7)   # a DataFrame, for any Spark writer
sd.spark.write(spark, profile, 's3://bucket/orders/', rows=1_000_000_000, seed=7)
```

`profile create` reads Parquet and Arrow locally or from S3, with the credentials and endpoint the
AWS environment names, such as `AWS_PROFILE`. It reads CSV, TSV and JSON from local files.

`--engine spark` computes the profile in Spark instead: in the Spark Connect session `SPARK_REMOTE`
names, or in one it starts on `local[*]`. Spark reads Parquet, CSV and TSV, from S3 too. It
estimates distinct counts with a 1% standard error, and quantiles within a rank error of 1/10,000.
From Python, `sd.profile(frame,
engine='spark')` profiles a DataFrame in its own session, and `spark=` names a session for files.
`docs/profiling-with-spark.md` gives the commands for a Glue job and a Glue Spark Connect session.

A directory whose folders hold unlike schemas is profiled as a bundle, one table per kind of folder,
as an AWS Glue crawler splits a prefix. Folders that match are one table, their `key=value` names
Hive partitions.

A profile is written under a privacy policy, `default` unless `--policy` names another. No data value
it stores, such as a top value or an extreme, rests on fewer than `--k-min` rows. A
column typed as an identifier keeps its format and no value, free text keeps its lengths, and
`privacy.audit` lists every literal the file keeps with the rule that admits it. `strict` treats
quasi-identifiers as identifiers. `healthcare` adds HIPAA Safe Harbor and the healthcare detector
pack: ZIP codes keep three-digit prefixes, ages over 89 share one bin, places below a state keep no
value, and every date keeps only its years. `none` keeps every value, for data that is already synthetic or public. `profile check` reads a
profile file alone, recomputes each rule from the rates and row counts it states, and exits 3 on a
violation, so CI can run it on every committed profile.

A profile's `source` section records how the files were written. For a CSV, that is its dialect,
encoding and line endings, and its malformed rows by kind. For Parquet, it is the footer's writer,
Spark version, codec and row groups, and never a footer's min or max. For JSON, it is the nesting
depth and whether nulls are written, and each path's type mix and presence sit on its column. Every
format gets the file sizes, the rows per file, and each Hive key's values. `data generate` writes the dialect and the Parquet settings back.

A string column is given a semantic type where enough of its values pass a type's validator. Fewer
need to pass where its name hints the type. An integer column is typed only where its name hints
the type and a validator agrees. The general pack covers common identifiers, such as emails, card
numbers and US SSNs, and `doppeldata.profiling.detector_packs.general` lists its types. The type
names what the generator draws for it, and the failing share becomes `quality.invalid_rate`.
`--pack NAME` adds an installed detector pack's types to these. The healthcare pack ships with the
library, and types NPIs, MBIs, NDC, ICD-10-CM, HCPCS and CPT codes, DEA numbers, and member, medical
record and claim numbers. It ships no code set, so an ICD-10-CM or HCPCS code is typed and generated
by its format, and is not guaranteed real. `--code-list ndc=FILE`, `icd10cm=FILE` or
`hcpcs_level2=FILE` types such a code only where FILE lists it, and the profile records the file's
sha256.

Each numeric column gets an equal-depth histogram, and each with enough distinct values gets the
distribution family that fits it best. `--edges-from` counts a new profile against an earlier one's
bin edges and value lists, so the two compare bin for bin, and the rows outside them are written as
overflow. Where the files, the run's settings, the doppeldata, scipy and reader versions, the
detector packs and the code lists match the ones the earlier profile was made with, it is kept as it is, with any edits a
reviewer made. Otherwise `profile create` says what differs.

`data check` profiles generated data with its profile's edges frozen, and reads the real data only
with `--against`. It measures each column against the profile by the metrics `data check --help`
defines, each with a threshold and a verdict, and exits 3 when any fails or the check cannot finish.
`profile diff` measures the same metrics between two profiles of one table, and lists every field
that changed between them. It exits 1 on drift.
`--thresholds FILE` sets each metric's threshold, and `--json` prints the report that `schemas show
check` or `schemas show diff` describes.

`--against REAL_DATA` also scores a seeded sample of each side by the scores `data check --help`
lists. Each follows SDMetrics' definition, and doppeldata computes it itself. The report holds the
scores and no real row.
They decide nothing until `--thresholds` sets them. `--html FILE` writes the two samples side by
side with sweetviz. It shows the real data's extremes and category names, so it is not for sharing.

Each release's notes list what it does. Its commands and its profile format change without notice
until 1.0.

## Install

The command line:

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ doppeldata-cli
doppeldata --version     # prints the installed release
```

The command ships in the `doppeldata-cli` distribution, so a one-off run names both:

```bash
uvx --index https://pypi.ichrisbirch.com/simple/ --from doppeldata-cli doppeldata --version
```

Every release is published to that package index, and nothing else is. Name it on every install.

`data check --html` also needs sweetviz, which the command line does not declare:

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ --with sweetviz doppeldata-cli
```

The library, for code that profiles or generates data itself:

```bash
uv add --index https://pypi.ichrisbirch.com/simple/ doppeldata
```

The library installs no profiling engine, so a job that only generates carries none. Code that calls
`doppeldata.profile()` installs them beside it:

```bash
uv add polars duckdb pyarrow boto3
uv add clevercsv charset-normalizer   # to profile CSV or TSV files
uv add scipy     # optional: without it, no column is fitted to a distribution family
uv add sweetviz  # for check(..., html=...), the side-by-side HTML
```

`profile(engine='spark')` needs pyspark, pyarrow and boto3 instead of polars, with duckdb, clevercsv
and charset-normalizer for CSV or TSV. A Spark install brings pyspark, and the CLI declares none.

Code that writes to S3 installs boto3, which resolves the AWS environment's credentials, SSO
profiles included.

The library runs on Python 3.11 and later, and the command line on 3.13 and later. The command line
is a separate distribution, so a project importing the library installs no CLI dependencies. The
library's generator is compiled Rust. A wheel is built for each platform the `wheels` job in
`.github/workflows/build-library.yml` lists. Anywhere else, the install builds the sdist, which
needs a Rust toolchain.

## Run the library inside an AWS Glue job

Every Glue version that runs Python 3.11 or later installs the library, and
`docs/aws-glue-support.md` lists them. Glue compiles nothing, so download the Linux x86_64 wheel from
the index alone, and upload it to S3:

```bash
pip download --no-deps --index-url https://pypi.ichrisbirch.com/simple/ \
  --platform manylinux2014_x86_64 --python-version 311 --only-binary=:all: \
  --dest wheels doppeldata
aws s3 cp --recursive wheels/ s3://your-bucket/glue/
```

Then pass its path in the job's parameters, with the file name pip downloaded. Glue installs the
wheel, and its dependencies from PyPI:

```text
--additional-python-modules  s3://your-bucket/glue/doppeldata-<version>-cp311-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
```

A job that profiles names more modules after the wheel. It names `scipy`, which no Glue
preinstalls and which fits the numeric columns. For CSV or TSV it also names `duckdb==2.0.0`,
`clevercsv==0.8.5` and `charset-normalizer`. `docs/profiling-with-spark.md` gives the whole job.

Never give pip the package index as an extra index beside PyPI. pip installs the highest version it
finds on either, so anyone uploading a `doppeldata` to PyPI would run their code in the job.

A job with no internet access installs from a zip of wheels instead. Name the downloaded wheel by
its path, so pip never asks PyPI for doppeldata:

```bash
pip download --dest wheels --platform manylinux2014_x86_64 --python-version 311 \
  --only-binary=:all: wheels/doppeldata-<version>-cp311-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
echo doppeldata > wheels/requirements.txt
zip -r doppeldata-<version>.gluewheels.zip wheels/
```

Upload the zip, pass its `s3://` path to `--additional-python-modules`, and pass `--no-index` to
`--python-modules-installer-option`. Glue then resolves every dependency from the zip.

## Layout

| Directory | Holds |
| --- | --- |
| `crates/` | the Rust workspace: `core`, the generation engine; `writers`, which writes its blocks as files to any object store; and `python`, the extension module over the engine |
| `library/` | the `doppeldata` distribution: the compiled extension and the Python code over it |
| `cli/` | the `doppeldata-cli` distribution: the `doppeldata` command over the library |
| `docs/` | why the design is what it is |
| `benchmarks/` | the scripts `task bench` runs to time the engine against numpy, Faker and Mimesis |
| `spark-parity/` | the checks that `doppeldata.spark` gives a local run's values and files, and that `profile(engine='spark')` gives Polars' profile, inside each Spark image named |

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
