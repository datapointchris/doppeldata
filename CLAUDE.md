# doppeldata

Profiles a dataset into a reviewable file holding no rows, then generates synthetic data from it.
`docs/README.md` indexes why the design is what it is. Read it before changing the profile format,
the privacy policy, the generator's streams or what the library installs.

## A Rust workspace and two Python distributions share the repo

```text
pyproject.toml  the uv workspace, the release config, and every Python tool's config for both members
crates/         the cargo workspace: core (the engine, no Python), writers (files, object stores and
                the manifest) and python (the pyo3 extension)
library/        doppeldata: imported as `doppeldata`, built by maturin, no scripts
cli/            doppeldata-cli: imported as `doppeldata_cli`, the `doppeldata` command
docs/           the design pages
benchmarks/     PEP 723 scripts timing the engine against numpy, Faker and Mimesis, run by `task bench`
spark-parity/   the checks that doppeldata.spark matches a local run, and the Spark profiler Polars, inside each
                Spark image named, run by hand
malformed-file-readers/
                what DuckDB and each Spark image named make of every fault kind malformed_rows writes,
                run by hand
```

- **The dependency runs one way: the CLI imports the library, never the reverse.** Typer and rich
  are imported only under `cli/`. Code that imports the library, including inside AWS Glue jobs,
  must never pull them in.
- **`crates/core` knows nothing of Python.** `crates/python` is the only crate that imports pyo3,
  and it builds as `doppeldata._core`. `task generate` writes its stub,
  `library/src/doppeldata/_core.pyi`, from the crate's pyo3 introspection. mypy reads the stub in
  place of the compiled module. The extension is an inline `#[pymodule] mod`, the only form maturin
  generates a stub from.
- **`crates/writers` holds every I/O dependency, and blocks on tokio only from threads it spawns.**
  Its callers include Python threads with no runtime and async code that already runs one. So
  nothing in it may assume an ambient runtime or `block_on` from the caller's thread.
- **The JSON Schemas are derived from the Rust model, never edited.** `published_schemas::all()` in
  `crates/core` lists each one with its file name, and `task generate` writes them into
  `library/src/doppeldata/schemas/`. `task check-generated` fails when a schema or the stub differs
  from what the crates generate, and runs in the hooks.
- **A library dependency lands on every consumer's Glue job.** Add one only if the library cannot do
  without it. It must install on CPython 3.11 from a wheel. It arrives in the change that first
  imports it. A heavy engine used by one feature, such as pyspark or scipy, is imported lazily and
  declared nowhere, and its missing-import error names what to install.
- **A change to a sampler's or a perturbation's draw order raises the engine's stream version**, and
  so does a change to how a scenario's patch merges, or the engine starting to read a profile field
  it ignored before. So does a change to the hint `name_hinted_types.py` fills for a typed column.
  The engine's tests read the profile unfilled, so `library/tests/test_typed_column_hints.py` pins
  a hash over the fill beside the stream version. The stream version is part
  of a generated dataset's identity. `docs/determinism-contract.md` says what else must never change.
  The determinism contract test pins the stream version beside each `PINNED_*VALUES` hash.
  `PINNED_VALUES` covers what `crates/core/tests/every-column-kind.profile.yaml` generates, and
  `PINNED_SCENARIO_VALUES` covers it under `every-column-kind.scenario.yaml` beside it. A column
  kind, family, provider or profile field the engine gains adds a column there, and a perturbation
  op that writes rows adds an entry. The fixture's source is CSV, so a column given a `type_mix`
  share under 1 there generates as text. `PINNED_CDC_VALUES` covers it under
  `every-column-kind.cdc.scenario.yaml`: the full load, every change file and the expected table. A
  change to how `cdc` draws moves it.
- **Nothing that feeds a generated value calls std's `exp`, `ln`, `powf`, `powi` or any other
  transcendental.** Call the `libm` crate's function instead. std's may round differently by
  platform, and a dataset must regenerate to the same bits anywhere. `crates/clippy.toml` refuses
  them.

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
task bench      # time the engine against numpy, Faker and Mimesis; never in CI, see docs/performance.md
```

`uv sync` rebuilds the extension when `crates/` changes. A `cargo` command runs from `crates/`,
where the workspace's `Cargo.toml` is. The root `rust-toolchain.toml` pins the compiler for every
cargo call in the repo, maturin's included.
