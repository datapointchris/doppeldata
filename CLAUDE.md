# doppeldata

Profiles a dataset into a reviewable file holding no rows, then generates synthetic data from it.
`docs/README.md` indexes why the design is what it is. Read it before changing the profile format,
the privacy policy, the generator's streams or what the library installs.

## A Rust workspace and two Python distributions share the repo

```text
pyproject.toml  the uv workspace, the release config, and every Python tool's config for both members
crates/         the cargo workspace: core (the engine, no Python) and python (the pyo3 extension)
library/        doppeldata: imported as `doppeldata`, Python >=3.11, built by maturin, no scripts
cli/            doppeldata-cli: imported as `doppeldata_cli`, Python >=3.13, the `doppeldata` command
docs/           the design pages
```

- **The dependency runs one way: the CLI imports the library, never the reverse.** Typer and rich
  are imported only under `cli/`. Code that imports the library, including inside AWS Glue jobs,
  must never pull them in.
- **`crates/core` knows nothing of Python.** `crates/python` is the only crate that imports pyo3,
  and it builds as `doppeldata._core`. Everything the extension exposes is typed in
  `library/src/doppeldata/_core.pyi`, which mypy reads in place of the compiled module.
- **A library dependency lands on every consumer's Glue job.** Add one only if the library cannot do
  without it. It must install on CPython 3.11 from a wheel. It arrives in the change that first
  imports it. A heavy engine used by one feature, such as pyspark or scipy, is imported lazily and
  declared nowhere, and its missing-import error names what to install.
- **A change to a sampler's draw order raises the engine's stream version.** The stream version is
  part of a generated dataset's identity. `docs/determinism-contract.md` says what else must never
  change.

## The tag is the version of everything

`crates/Cargo.toml` carries `version = "0.0.0"`, and the release job sets the tagged version before
building, so the wheel and `doppeldata._core.__version__` both name the tag. The CLI's version comes
from the tag through uv-dynamic-versioning, and it requires the library at exactly that version. A
release is cut from `main` by conventional commits, and nothing is committed back.

## Commands

```bash
task setup      # uv sync --all-packages: builds the extension, installs both members
task test       # cargo test in crates/, then pytest
task lint       # ruff, mypy, rustfmt and clippy, writing nothing
task build      # wheels and sdists into dist/
```

`uv sync` rebuilds the extension when `crates/` changes. A `cargo` command runs from `crates/`,
where the workspace's `Cargo.toml` is. The root `rust-toolchain.toml` pins the compiler for every
cargo call in the repo, maturin's included.
