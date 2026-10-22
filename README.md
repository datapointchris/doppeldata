# doppeldata

Profile a dataset once, commit the profile, and generate synthetic data from it whenever a test or a
benchmark needs some.

A profile is a YAML file holding a dataset's types, distributions, skew, null and error rates, file
layout and table relationships. It holds no rows. doppeldata reads CSV, TSV, JSON, Parquet, Arrow,
ORC, Avro and Spark DataFrames. It writes any of them back at any size: in memory, to disk, to S3, or
inside Spark executors.

A scenario file layered on a profile bends the data on purpose: a hot join key, a burst of malformed
rows, a flood of small files. The same profile, scenario and seed always produce the same data.

doppeldata is experimental. Its commands and its profile format change without notice until 1.0.

## Install

The command line:

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ doppeldata-cli
doppeldata --version     # prints the installed release
```

Every release is published to that package index, and nothing else is. Name it on every install.

The library, for code that profiles or generates data itself:

```bash
uv add --index https://pypi.ichrisbirch.com/simple/ doppeldata
```

The library runs on Python 3.11 and later, and the command line on 3.13 and later. The command line
is a separate distribution, so a project importing the library installs no CLI dependencies. The
library's generator is compiled Rust, shipped as one wheel per platform: Linux on x86_64 and
aarch64 (manylinux2014), and macOS on Apple silicon.

## Run the library inside an AWS Glue job

Glue 5.0, 5.1 and 6.0 install the library. Glue 4.0 runs Python 3.10 and cannot. Glue compiles
nothing, so download the Linux x86_64 wheel from the index alone, and upload it to S3:

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
| `crates/` | the Rust workspace: `core`, the generation engine, and `python`, the extension module over it |
| `library/` | the `doppeldata` distribution: the extension plus the Python SDK and profiler |
| `cli/` | the `doppeldata-cli` distribution: the `doppeldata` command over the library |
| `docs/` | why the design is what it is |

## Develop

```bash
task setup    # builds the extension and installs both distributions into .venv
task test     # cargo test, then pytest
task lint     # ruff, mypy, rustfmt and clippy, writing nothing
```

A release is cut from `main` by the commit messages: `feat:` cuts a minor and `fix:` a patch. The tag
is the version of both distributions. `crates/Cargo.toml` carries `0.0.0`, and the release job sets
the tagged version before building.

## License

MIT
