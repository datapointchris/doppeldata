# doppeldata

Profile a dataset once, commit the profile, and generate synthetic data from it whenever a test or a
benchmark needs some.

A profile is a YAML file holding a dataset's types, distributions, skew, null and error rates, file
layout and table relationships. It holds no rows. doppeldata is built to read CSV, TSV, JSON,
Parquet, Arrow and Spark DataFrames, and to write any of them back at any size: in memory, to disk,
to S3, or inside Spark executors.

A scenario file layered on a profile is meant to bend the data on purpose: a hot join key, a burst of
malformed rows, a flood of small files. The same profile, scenario and seed must always produce the
same data.

doppeldata is experimental. So far it profiles a table's columns and its files, generates a table from a profile,
and ships the profile format and its hand-written example profiles:

```bash
doppeldata profile create data/orders/ --out orders.profile.yaml   # profile a directory of one table
doppeldata profile create s3://bucket/orders/ --sample 1000000     # a seeded sample, printed as YAML
doppeldata profile create data/orders/ --edges-from orders.profile.yaml --out orders.profile.yaml  # keep its bin edges
doppeldata profile list                # the example profiles
doppeldata profile show orders         # one of them, as YAML to start a profile from
doppeldata profile diff march.profile.yaml april.profile.yaml  # how far April drifted from March
doppeldata data check orders.profile.yaml out/orders/        # generated data against its profile
doppeldata schemas show profile        # the JSON Schema every profile validates against

doppeldata data plan orders --rows 1000000 --seed 7 --out orders/       # the files a write makes, and their size
doppeldata data generate orders --rows 1000000 --seed 7 --out orders/   # write them, and _manifest.json
doppeldata data generate orders.profile.yaml --rows 100000000 --format csv --out s3://bucket/orders/
```

`data generate` writes every format `doppeldata schemas show manifest` lists, to a local directory or to S3.
`_manifest.json` records the profile's digest, the seed, the row count and the engine's stream
version. Those regenerate the same values in any format. From Python, a table arrives through the Arrow PyCapsule interface, so
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
AWS environment names, such as `AWS_PROFILE`. It reads CSV, TSV and JSON from local files. No data
value it stores, such as a top value or an extreme, rests on fewer than `--k-min` rows. Typing an
identifier column does not remove its values from the profile, so profile only data that is already
synthetic or public.

A profile's `source` section records how the files were written. For a CSV, that is its dialect,
encoding and line endings, and its malformed rows by kind. For Parquet, it is the footer's writer,
Spark version, codec and row groups, and never a footer's min or max. For JSON, it is each path's
type mix and presence. Every format gets the file sizes, the rows per file, and each Hive key's
values. `data generate` writes the dialect and the Parquet settings back.

A string column is given a semantic type where enough of its values pass a type's validator. Fewer
need to pass where its name hints the type. An integer column is typed only where its name hints
the type and a validator agrees. The general pack covers common identifiers, such as emails, card
numbers and US SSNs, and `doppeldata.profiling.detector_packs.general` lists its types. The type
names what the generator draws for it, and the failing share becomes `quality.invalid_rate`.
`--pack NAME` adds an installed detector pack's types to these.

Each numeric column gets an equal-depth histogram, and each with enough distinct values gets the
distribution family that fits it best. `--edges-from` counts a new profile against an earlier one's
bin edges and value lists, so the two compare bin for bin, and the rows outside them are written as
overflow. Where the files, the run's settings, the doppeldata, scipy and reader versions and the
detector packs match the ones the earlier profile was made with, it is kept as it is, with any edits a
reviewer made. Otherwise `profile create` says what differs.

`data check` profiles generated data with its profile's edges frozen, and never reads the real data.
It measures each column against the profile by the metrics `data check --help` defines, each with a
threshold and a verdict, and exits 3 when any fails or the check cannot finish. `profile diff`
measures the same metrics between two profiles of one table, and lists every field that changed
between them. It exits 1 on drift.
`--thresholds FILE` sets each metric's threshold, and `--json` prints the report that `schemas show
check` or `schemas show diff` describes.

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
```

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
| `spark-parity/` | the check that `doppeldata.spark` gives a local run's values and files inside each Spark image named |

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
