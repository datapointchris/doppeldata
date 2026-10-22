# doppeldata

Profiles a dataset into a reviewable file holding no rows, then generates synthetic data from it.
`docs/README.md` indexes why the design is what it is. Read it before changing the profile format,
the privacy policy, the generator's streams or what the library installs.

## A Rust workspace and two Python distributions share the repo

```text
pyproject.toml  the uv workspace, the release config, and every Python tool's config for both members
crates/         the cargo workspace: core (the engine, no Python) and python (the pyo3 extension)
library/        doppeldata: imported as `doppeldata`, built by maturin, no scripts
cli/            doppeldata-cli: imported as `doppeldata_cli`, the `doppeldata` command
docs/           the design pages
```

- **The dependency runs one way: the CLI imports the library, never the reverse.** Typer and rich
  are imported only under `cli/`. Code that imports the library, including inside AWS Glue jobs,
  must never pull them in.
- **`crates/core` knows nothing of Python.** `crates/python` is the only crate that imports pyo3,
  and it builds as `doppeldata._core`. `task generate` writes its stub,
  `library/src/doppeldata/_core.pyi`, from the crate's pyo3 introspection. mypy reads the stub in
  place of the compiled module. The extension is an inline `#[pymodule] mod`, the only form maturin
  generates a stub from.
- **The JSON Schemas are derived from the Rust model, never edited.** `published_schemas::all()` in
  `crates/core` lists each one with its file name, and `task generate` writes them into
  `library/src/doppeldata/schemas/`. `task check-generated` fails when a schema or the stub differs
  from what the crates generate, and runs in the hooks.
- **A library dependency lands on every consumer's Glue job.** Add one only if the library cannot do
  without it. It must install on CPython 3.11 from a wheel. It arrives in the change that first
  imports it. A heavy engine used by one feature, such as pyspark or scipy, is imported lazily and
  declared nowhere, and its missing-import error names what to install.
- **A change to a sampler's draw order raises the engine's stream version.** The stream version is
  part of a generated dataset's identity. `docs/determinism-contract.md` says what else must never
  change.

## Library code must run on Python 3.11

The library's floor is the Python of the oldest Glue in `docs/aws-glue-support.md`, while `uv sync`
resolves the workspace at the CLI's later floor. ruff checks `library/` against 3.11 at commit
time. `library-on-python-3.11.yml` builds the library on 3.11, imports the extension, type-checks it
as 3.11 and runs its tests there.

## The tag is the version of everything

`crates/Cargo.toml` carries `version = "0.0.0"`. `build-library.yml` runs `crates/set-version.sh`
with the tagged version before maturin builds, so the wheel and `doppeldata._core.VERSION` both
name the tag. The CLI's version comes from the tag through uv-dynamic-versioning, and it requires the
library at exactly that version. A release is cut from `main` by conventional commits, and nothing
is committed back.

Every pull request runs `build-library.yml` unstamped, so each wheel in its matrix has built before a
merge can tag. The release builds the commit again before it tags.

## Commands

```bash
task setup      # uv sync --all-packages: builds the extension, installs both members
task test       # cargo test in crates/, then pytest
task lint       # ruff, mypy, rustfmt and clippy, writing nothing
task generate   # rewrite the JSON Schemas and the extension's stub from the crates
task check-generated   # fail when a committed schema or the stub is not what the crates generate
```

`uv sync` rebuilds the extension when `crates/` changes. A `cargo` command runs from `crates/`,
where the workspace's `Cargo.toml` is. The root `rust-toolchain.toml` pins the compiler for every
cargo call in the repo, maturin's included.
