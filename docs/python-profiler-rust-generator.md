# The profiler is Python, and only generation is Rust

## Profiling already runs in native code, so a Rust profiler would win nothing

Profiling reads a dataset once and reduces it to statistics. Polars and DuckDB already do that work
in native code, and together they profiled ten million rows in seconds. Writing the profiler in Rust
would add native code to maintain and buy no speed.

## Generation is where Rust pays, through threads and per-row work

Generation produces every row, so its cost scales with the output. The comparison generated an
8-column table in million-row chunks, one ZSTD Parquet file per chunk, from Rust and from numpy with
pyarrow, on one thread and on sixteen:

- **On one thread the arms tie once Parquet is written.** The encoder and ZSTD dominate both.
- **On sixteen, Rust scales and Python's pool does not.** Converting each array with `pa.array`
  holds the GIL, so the thread pool barely moved Python's generation rate. Rust wrote several times
  as many rows a second.

The columns were a sequential id, a lognormal amount, a Poisson quantity, a Zipf-weighted category
from 50 values, a uniform timestamp over a year, a Bernoulli flag, a normal score with 5% nulls and a
random 10-character code.

## Per-row work widens the gap further

Semantic values, copula rows, conditional tables and byte-level malformation cannot be vectorized in
numpy, and they are the generator's real workload. On one thread the `fake` crate produced person
names about ten times faster than Mimesis, and Mimesis more than thirty times faster than Faker.

## So the engine ships inside the library's wheel

`crates/core` is the engine, with no Python in it. `crates/python` wraps it as the extension module
`doppeldata._core`. The profiler, the SDK and the CLI stay Python and call into it.
