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

## The engine owns the identity: RFC 8785 JSON keyed like the file metadata

The SHA-256 is taken over the RFC 8785 canonical JSON of one object with five keys:

```json
{"dataset_rows":"10000000","profile_digest":"sha256:…","scenario_digest":null,"seed":"18446744073709551615","stream_version":1}
```

- **The engine builds the canonical JSON and its digest, in `DatasetIdentity`.** The library and its
  manifest take both from the engine and compute neither. The library owns the profile's canonical
  form, so it computes the profile and scenario digests and passes them in.
- **Each key is a file metadata key without its `doppeldata.` prefix.** Both derive from the
  constants in `profile::provenance`. So a reader holding a file's metadata rebuilds the identity
  with no mapping, and a sixth value needs no hand ordering, because the map sorts its keys.
- **The seed and the row count are decimal strings.** Both are 64-bit unsigned integers. RFC 8785
  writes a number as an IEEE 754 double, which holds every integer only up to 2^53, so a seed
  written as a number would lose digits. A string keeps the seed's whole range with no cap.
- **Each digest is `sha256:` and 64 lowercase hex digits**, the form the library's profile digest
  takes. The identity's own digest is written the same way.
- **An absent scenario is `null` in the identity and a missing key in the metadata.** No scenario is
  the absence of one, so it hashes as an absence rather than as the digest of an empty file.
- **The profile's digest is the profile's as given, before a scenario patches it.** The scenario's
  digest covers the patch. A scenario's digest resolves every perturbation's id and leaves out its
  `description` and `expect`. So rewording it or moving a bound keeps the dataset's identity.

A test pins the digest of one fixed identity, which the library's `rfc8785` package reproduces.

## Each column draws from a stream named for it, never numbered

Every (seed, table, column path, perturbation) derives its own ChaCha8 stream from a hash of its
name. So adding, removing or reordering a column leaves every other column's values unchanged.

Within a column, each kind of draw has its own stream: the values, the null mask, the missing-value
tokens, the wrong check digits, a list's lengths and a drawn value's index. The values stream draws
a value for every row, a null row's included. So a column's values do not move when its null rate,
its error rate or its tokens change. A nested
field's streams are named by its dotted path, so it draws the same whatever its siblings are.

A column drawn by index takes each value from its `indexed_values` stream, positioned at the index
rather than the block. A string column draws the string there, and a numeric column its level's
offset inside the index's band. So the value at an index is the same in every block that draws it.

A copula group, the set of numeric columns correlated through one matrix, draws from one stream keyed
by its sorted column names. Adding an unrelated column changes nothing. Adding a column to the group
changes that group's values.

A scenario's perturbation draws from streams keyed by its id, never its place in the list. A value it
takes from a column's sampler comes from that sampler under a table name holding the id. So
inserting, removing or reordering a perturbation leaves every other one's rows unchanged. A
scenario changes no stream a profile alone draws from, so a dataset without one keeps its values.

## A fixed block of 65,536 rows picks the stream position

The block size is internal and fixed. It never follows batch size, file size or thread count. So
changing the number of rows per file, the number of threads or the output format changes the files
and never the values.

## The block is the unit of parallelism, because draws are not fixed-width

Rejection-sampling distributions (Gamma, Poisson, ziggurat normals) consume a variable count of random
words per value. That rules out skipping ahead to an arbitrary row. A block starts its stream fresh,
so any thread can produce any block.

## The stream version names the values, and the tool version names the file bytes

The guarantees, strongest first:

1. Arrow values are identical for one stream version, under any split, thread count or format.
2. File bytes, and the split drawn from `rows_per_file`, are identical for one tool version and one
   set of writer settings.
3. The faults `malformed_rows` breaks those bytes with, and the `_faults.jsonl` naming them, hold
   as the bytes do, under any thread count or split across processes. The op moves no value.

Any change to a sampler's draw order raises the stream version. So does the engine starting to read
a profile field it ignored before, because a profile that carries the field then generates
different values. The library fills a typed column's generator hint where the column has none, and
the profile's digest is taken before the fill. So a change to the hint a type fills, or to which
columns take one, raises the stream version too. The fill reads the type alone, never the column's
name, so a name hint added for detection moves no value. The engine never sees the fill, so a library test
pins a hash over the hints it writes, beside the stream version. The stream version is part of a dataset's identity, so a dataset generated
before the change is still named exactly. An encoder upgrade or a change to the file-size draw
moves bytes and leaves every value alone. The manifest's `tool` names the version that wrote the
bytes. `regenerate()` compares every file it writes with the
manifest, and names any whose bytes moved.

A test pins SHA-256 hashes beside the stream version, each named `PINNED_*VALUES`:

- `PINNED_VALUES` covers the values and text cells of a fixture holding one column of every kind,
  fitted family and provider, and of every profile field the engine reads.
- `PINNED_SCENARIO_VALUES` covers the same fixture under a scenario fixture holding a patch and
  every op that writes rows.
- `PINNED_CDC_VALUES` covers it under `every-column-kind.cdc.scenario.yaml`, a `cdc` beside a row
  op and a `schema_drift`. It covers the full load, every change file and the expected table.

A change that moves a value fails a pin until it is updated with the others. The fixtures are the
test's own, so editing a bundled example moves no pin. Both pin scripts refuse a `PINNED_*VALUES`
hash they name no fixture for.

An op whose output the scenario fixture cannot hold, as `cdc`'s cannot, takes a pin of its own. A
pin no release has frozen may be set without raising the stream version, because no released
dataset holds its bytes. Once released, it is frozen like the others.

## Every exp, log and pow that feeds a value is libm's, so values match on every platform

Rust's standard library allows its transcendental functions (`exp`, `ln`, `powf`, `powi` and the
rest) to round differently by platform and by Rust version. A value computed through one of them
could differ in its last bit on another machine, and "reproduce anywhere" would fail there alone.

- **Arithmetic, `sqrt` and rounding to an integer are exact under IEEE 754**, and need nothing.
- **Every other function goes through the `libm` crate**, called directly. That covers the
  inverse CDFs a fitted tail is tabulated from at plan time. Clippy refuses std's transcendentals
  in the workspace's own code.
- **A fitted family is used only where its CDF and inverse are closed forms over libm.**
  `FITTED_FAMILIES` in `crates/core/src/samplers/fitted_family.rs` names them. `norm` uses
  Wichura's AS 241, which needs only `log` and `sqrt`. A family with no closed form, such as `gamma`
  or `poisson`, leaves the column with a linear tail.
- *Rejected:* statrs's inverse CDFs. They call std's functions, and a dependency's `num-traits`
  feature cannot force libm, because features unify across the build.

CI runs the pinned-value test on every platform with a wheel, in the library's wheel build, and on
the platforms `determinism-contract.yml` adds. The wheel build then installs the wheel it built into
Python 3.11 and generates the fixture through it. `crates/check-pinned-values-through-wheel.py`
reads the pinned hash and the test's inputs from the test's source, so the pin has one home. A
failure on one runner is a platform difference, and never a reason to loosen the pin. The same
workflow refuses a pinned hash that changed since the last release while the pinned stream version
stayed level. It compares only the pins that release held, so a new pin starts at the next one.
Where a fixture changed since the release at a level stream version, its hash cannot be compared.
The workflow then generates the release's own fixtures with this build, which must reach the
release's hash.
