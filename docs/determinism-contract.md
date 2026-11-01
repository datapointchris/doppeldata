# The determinism contract

## A dataset is identified by the values below

A generated dataset is a pure function of:

1. the engine's stream version;
2. the canonical SHA-256 of the profile, or of the bundle;
3. the canonical SHA-256 of the scenario;
4. the seed;
5. the row count;
6. the scale a bundle is generated at;
7. the bundle table the rows are of;
8. the SHA-256 of the columns `sample` pins and their values.

Six and seven are null outside a bundle. The manifest written beside the output must record every
one, plus a SHA-256 over them. The profile or bundle, the scenario, the seed and a bundle's scale
reproduce a dataset anywhere, and the manifest records each.

The eighth exists only for a sample with a column pinned. An unpinned identity leaves its key out
rather than writing it as null, so an unpinned dataset's digest is the first seven values'. A pinned
sample's metadata carries the digest under `doppeldata.overrides_digest`, and the pinned columns,
sorted, under `doppeldata.overridden_columns`. The digest is the SHA-256 of the overrides' JSON
sorted by column. Each column reading no pinned column keeps the values the unpinned dataset holds,
because each draws from its own stream. No call writes a pinned sample to files, so no manifest
records one.

## The engine owns the identity: RFC 8785 JSON keyed like the file metadata

The SHA-256 is taken over the RFC 8785 canonical JSON of one object with a key for each value:

```json
{"dataset_rows":"10000000","profile_digest":"sha256:…","scale":null,"scenario_digest":null,"seed":"18446744073709551615","stream_version":…,"table":null}
```

- **The engine builds the canonical JSON and its digest, in `DatasetIdentity`.** The library and its
  manifest take both from the engine and compute neither. The library owns the profile's canonical
  form, so it computes the profile and scenario digests and passes them in.
- **Each key is a file metadata key without its `doppeldata.` prefix.** Both derive from the
  constants in `profile::provenance`. So a reader holding a file's metadata rebuilds the identity
  with no mapping, and a new value needs no hand ordering, because the map sorts its keys.
- **The seed, the row count and the scale are decimal strings.** The first two are 64-bit unsigned
  integers. RFC 8785 writes a number as an IEEE 754 double, which holds every integer only up to
  2^53, so a seed written as a number would lose digits. A string keeps the seed's whole range with
  no cap. The scale is Rust's shortest decimal reading back as the same double, so `0.1` is `"0.1"`.
- **Each digest is `sha256:` and 64 lowercase hex digits**, the form the library's profile digest
  takes. The identity's own digest is written the same way.
- **An absent scenario is `null` in the identity and a missing key in the metadata.** No scenario is
  the absence of one, so it hashes as an absence rather than as the digest of an empty file.
- **The profile's digest is the profile's as given, before a scenario patches it.** The scenario's
  digest covers the patch. A scenario's digest resolves every perturbation's id and leaves out its
  `description` and `expect`. So rewording it or moving a bound keeps the dataset's identity.
- **A profile or scenario key renamed moves the identity, and the values stay.** Key names are
  part of the canonical JSON each digest hashes. So a rename changes the profile digest, the
  dataset digest and every part name's `identity8`. Each stream is keyed by its table, its column's
  path and its draw's own name, never by a profile key. So the same profile under the new names
  draws the same bits.

A test pins the digest of one fixed identity, which the library's `rfc8785` package reproduces.

## Every table of a bundle is named by the bundle, so its own manifest regenerates it alone

A bundle's tables share its profile digest, seed, scale and scenario digest. Each table's identity
adds its own name and its own rows, `round(rows × scale)`. The bundle's own identity names no
table, and its rows are the sum of its tables'.

- **A bundle's scenario digest is taken over the map of each table to its scenario.** Each entry is
  the scenario's canonical document, sorted by table. So one table's scenario changing renames
  every table, because a child's foreign keys and inherited columns read its parent's values.
- **A table's own manifest regenerates that table, with its foreign keys.** `regenerate()` takes
  the table's plan from `plan_bundle` and writes that table's files alone. A foreign key is drawn
  from its parent's keys by formula, never by reading the parent's rows.
- **A child row's parent comes from its row index alone.** A keyed bijective permutation takes the
  row to a slot. The slot's rank comes from the closed-form cumulative fan-out, and a second keyed
  permutation takes the rank to a parent row. So the parent a row holds is the same at any thread
  count and in any block order, and every block mixes parents from across the whole table.
- **A rank is the first whose cumulative count passes the slot.** Up to `TABULATED_PARENTS`
  parents, the counts are tabulated at plan time and a rank is a search over them. Past that, the
  curve is inverted and the rank settled against the same rounded counts. Both give the same rank.
- **A child copying a parent column with listed values takes its busy parents by that value.** The
  ranks are dealt busiest first. Each goes to the value whose matched rows fall furthest short of
  the child's stated share, per parent row it has left. Each value's ranks then go to its parent
  rows in the parent permutation's order. So the parent column's generated values decide every
  placement, and a change to how that column draws moves every child row's parent. Past
  `PLACED_PARENTS` parents, or for a sorted draw, the parent permutation alone places them.
- **Each `top_fractions` pin sits at its rank times the scale.** At scale 0.1, `top10` pins the busiest
  parent. A pin under one parent, or on every parent, is left out. So the
  counts a parent holds move with the scale, which the identity carries.
- **A bridge's second draw takes its positions from the first's.** A bridge is a child whose
  primary key is exactly two relationships' columns. The first's position times a multiplier,
  modulo the child's rows, is the second's. The multiplier is coprime with the rows, so each draw
  still holds its own counts. It is the first of `MULTIPLIER_TRIES` tried from the rows' golden
  fraction, then as many from the second's longest run, that puts one key's rows that far apart. So
  a change to either draw's counts can change it, and with it every row's pair.
- **An ordinal is its row's place in its key's run, from 1.** It is read from the position the
  foreign key's slot is, so it draws from no stream of its own.
- **A parent key is addressed at its row, never read back.** A `sequence` key is its formula, and a
  uuid key seeks its stream to the row. A key that is itself a foreign key takes its own
  relationship's draw. Any other integer key is a sequence across its profiled extremes, and any
  other string key draws from its `indexed_values` stream at the row. Any other key is refused as
  `E-PROFILE-120`.
- **An inherited column copies its parent row's value.** The child block generates the parent
  blocks its rows touch, cached per thread, so the copy is the value the parent's table writes.

*Rejected:* dealing each block a contiguous run of parents. Each file's key range would be narrow,
so Parquet's row-group statistics would skip more than real data lets them. It would also pair a
child with fewer distinct parents across two foreign keys than real data does.

## Each column draws from a stream named for it, never numbered

Every (seed, table, column path, perturbation) derives its own ChaCha8 stream from a hash of its
name. So adding, removing or reordering a column leaves every other column's values unchanged.

Within a column, each kind of draw has its own stream: the values, the null mask, the missing-value
tokens, the wrong check digits, a list's lengths and a drawn value's index. The values stream draws
a value for every row, a null row's included. So a column's values do not move when its null rate,
its error rate or its tokens change. A nested
field's streams are named by its dotted path, so it draws the same whatever its siblings are.

A column generating as text from its `type_mix_fractions` draws each row's share from its `type_mix` stream,
one word a row, and each value of another type from `type_mix_text`. Its own share renders what its
sampler draws typed, so the text changes no other stream. A stated key's values of other types are
derived from the row, and its own draws at a row are those the typed key holds there.

A column drawn by index takes each value from its `indexed_values` stream, positioned at the index
rather than the block. A string column draws the string there, and a numeric column its level's
offset inside the index's band. So the value at an index is the same in every block that draws it.

A copula group, the set of numeric columns correlated through one matrix, draws from one stream keyed
by its sorted column names. Adding an unrelated column changes nothing. Adding a column to the group
changes that group's values. So does a curve or zero rate on a member, because the group's Pearsons
are calibrated through it. That calibration weighs each parent value by its share in a draw at seed
0, so the matrix is the same for every seed.

A column drawn given another reads the parent's generated values and draws only from its own
streams. Its conditional table's cell, and whether a row breaks its lookup or expression, come from
its `given` stream. Its lag and its quantile curve's level come from its `values` stream. So stating
a dependence moves the child's values and never the parent's. A row that breaks a lookup or an
expression takes the value the column's own sampler draws at that row.

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
  fitted family and provider, and of every profile field the engine reads. It holds a column of
  each type generating as text, a nested one and a candidate key among them.
- `PINNED_SCENARIO_VALUES` covers the same fixture under a scenario fixture holding a patch and
  every op that writes rows.
- `PINNED_CDC_VALUES` covers it under `every-column-kind.cdc.scenario.yaml`, a `cdc` beside a row
  op and a `schema_drift`. It covers the full load, every change file and the expected table.
- `PINNED_BUNDLE_VALUES` covers `every-column-kind.bundle.yaml`, a bundle holding a composite key,
  an inherited column, a key that is itself a foreign key, a bridge, an ordinal and a
  relationship the plan sets aside. A key generating as text has a foreign key, an ordinal and a
  copied column beside it.
  It is generated at 1 and 16 threads and in reverse block order, which must all match.

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

## Every JSON object the engine writes or hands Python holds its keys sorted

schemars' `preserve_order` writes each published schema in the model's field order. It also turns
on serde_json's insertion order for the whole workspace. So a `Value` or `Map` keeps its keys in
the order they were inserted, and an unrelated change to the code building it can move them.

- **A `Value` or `Map` that reaches written bytes, a SHA-256 or Python is held as `SortedKeys`.**
  Its `Serialize` sorts every object's keys at any depth. So the field writes the same bytes
  however it was built. `rg -n 'SortedKeys<' crates` lists the fields.
- **Every binding returning a `Value` it built returns it through `json_text`** in
  `crates/python`, which serializes through `SortedKeys`.
- **A struct needs neither.** Its fields serialize in declaration order.
- **A scenario's `patch` is sorted as it is read.** Its key order decides the order each column,
  field and relationship is applied in. A sort when it is written cannot reach that.
