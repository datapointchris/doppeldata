# The determinism contract

## A dataset is identified by five values

A generated dataset is a pure function of:

1. the engine's stream version;
2. the canonical SHA-256 of the profile;
3. the canonical SHA-256 of the scenario;
4. the seed;
5. the row count.

The manifest written beside the output must record all five, plus a SHA-256 over them. Three committed
files — profile, scenario and seed — reproduce a dataset anywhere.

## The identity is RFC 8785 JSON, with the seed and the row count as decimal strings

The SHA-256 is taken over the RFC 8785 canonical JSON of one object with five keys:

```json
{"profile_digest":"sha256:…","rows":"10000000","scenario_digest":null,"seed":"18446744073709551615","stream_version":1}
```

- **The seed and the row count are decimal strings.** Both are 64-bit unsigned integers. RFC 8785
  writes a number as an IEEE 754 double, which holds every integer only up to 2^53, so a seed
  written as a number would lose digits. A string keeps the seed's whole range with no cap.
- **Each digest is `sha256:` and 64 lowercase hex digits**, the form the library's profile digest
  takes. The identity's own digest is written the same way.
- **`scenario_digest` is `null` when no scenario applies.** No scenario is the absence of one, so it
  hashes as an absence rather than as the digest of an empty file.

The engine takes the profile and scenario digests as arguments and never computes them, because the
library owns the canonical form. A test pins the digest of one fixed identity, which the library's
own RFC 8785 implementation reproduces.

## Each column draws from a stream named for it, never numbered

Every (seed, table, column path, perturbation) derives its own ChaCha8 stream from a hash of its
name. So adding, removing or reordering a column leaves every other column's values unchanged.

Within a column, each kind of draw has its own stream: the values, the null mask, the `type_mix`
errors, the missing-value tokens, the wrong check digits and a list's lengths. The values stream
draws a value for every row, a null row's included. So a column's values do not move when its null
rate, its error rate or its tokens change. A nested field's streams are named by its dotted path, so
it draws the same whatever its siblings are.

A copula group, the set of numeric columns correlated through one matrix, draws from one stream keyed
by its sorted column names. Adding an unrelated column changes nothing. Adding a column to the group
changes that group's values.

## A fixed block of 65,536 rows picks the stream position

The block size is internal and fixed. It never follows batch size, file size or thread count. So
changing the number of rows per file, the number of threads or the output format changes the files
and never the values.

## The block is the unit of parallelism, because draws are not fixed-width

Rejection-sampling distributions (Gamma, Poisson, ziggurat normals) consume a variable count of random
words per value. That rules out skipping ahead to an arbitrary row. A block starts its stream fresh,
so any thread can produce any block.

## Values match across every writer, and Parquet bytes only within one engine version

The guarantees, strongest first:

1. Arrow values are identical.
2. CSV and JSON bytes are identical.
3. Parquet bytes are identical for one engine version and one set of writer settings.

An upgrade to the Arrow or Parquet writer that changes encoder output, or any change to a sampler's
draw order, raises the stream version. The stream version is part of a dataset's identity, so a
dataset generated before the change is still named exactly.

A test pins a SHA-256 over the values the bundled example profiles generate, beside the stream
version, as one pair. A change that moves a value fails it until both are updated together.

## Every exp, log and pow that feeds a value is libm's, so values match on every platform

Rust's standard library allows its transcendental functions (`exp`, `ln`, `powf`, `powi` and the
rest) to round differently by platform and by Rust version. A value computed through one of them
could differ in its last bit on another machine, and "reproduce anywhere" would fail there alone.

- **Arithmetic, `sqrt` and rounding to an integer are exact under IEEE 754**, and need nothing.
- **Every other function goes through the `libm` crate**, called directly. That covers the
  inverse CDFs a fitted tail is tabulated from at plan time.
- **A fitted family is used only where its CDF and inverse are closed forms over libm.**
  `FittedFamily::from_fit` in `crates/core/src/samplers/fitted_family.rs` names them. `norm` uses
  Wichura's AS 241, which needs only `log` and `sqrt`. A family with no closed form, such as `gamma`
  or `poisson`, leaves the column with a linear tail.
- *Rejected:* statrs's inverse CDFs. They call std's functions, and a dependency's `num-traits`
  feature cannot force libm, because features unify across the build.

CI runs the pinned-value test on Linux x86_64, macOS x86_64 and macOS arm64. A failure on one runner
is a platform difference, and never a reason to loosen the pin.
