# The profiler is Python, and only generation is Rust

## Profiling already runs in native code, so a Rust profiler would win nothing

Profiling reads a dataset once and reduces it to statistics. Polars and DuckDB already do that work
in native code. Together they profiled 10 million rows of 6 columns in about 2.5 seconds. Writing the
profiler in Rust would add native code to maintain and buy no speed.

## Generation is where Rust pays, through threads and per-row work

Generation produces every row, so its cost scales with the output. Measured on a 16-thread Ryzen 9
6900HX, generating 10 million rows of an 8-column table in 1-million-row chunks, one ZSTD-1 Parquet
file per chunk:

| Arm | Generate only | Generate and write Parquet |
| --- | --- | --- |
| Rust, 1 thread | 5.4–5.7M rows/s | 2.3–2.4M rows/s |
| numpy and pyarrow, 1 thread, vectorized | 3.9M rows/s | 2.2–2.3M rows/s |
| Rust, 16 threads (rayon) | 25.8–27.5M rows/s | 10.9M rows/s |
| numpy and pyarrow, 16-thread pool | 4.2M rows/s | 3.4M rows/s |

The columns were a sequential id, a lognormal amount, a Poisson quantity, a Zipf-weighted category
from 50 values, a uniform timestamp over a year, a Bernoulli flag, a normal score with 5% nulls and a
random 10-character code.

- **One thread, the arms nearly tie once Parquet is written.** The encoder and ZSTD dominate both.
- **Python's thread pool barely scales.** Converting each array with `pa.array` holds the GIL, so 16
  threads moved generation from 3.9M to 4.2M rows/s.
- **Rust scales 4.7× on the full write.**

## Per-row work widens the gap further

Semantic values, copula rows, conditional tables and byte-level malformation cannot be vectorized in
numpy, and they are the generator's real workload. The `fake` crate produced about 6 million person
names a second on one thread. Mimesis 22.2 produced 557,000, and Faker 40.43 produced 16,000.

## So the engine ships inside the library's wheel

`crates/core` is the engine, with no Python in it. `crates/python` wraps it as the extension module
`doppeldata._core`. The profiler, the SDK and the CLI stay Python and call into it.
