# Profile exports and the embedded profile

A profile already states what its data holds. `profile export` writes that contract in a format
another tool validates data with, and `--embed-profile` writes the profile into the data files it
generated. Both are opt-in: a write carries neither unless asked, and the library imports none of
the tools that read them.

[Export a profile as a data contract](how-to/export-a-profile-as-a-contract.md) runs each format
and validates data with it. [Reproduce a dataset](how-to/reproduce-a-dataset.md) runs
`--embed-profile`.

## Each export states what its profile holds, in the format's own types

- **Each format's type names invert its importer's table**, so an export imports back as the same
  logical type, nullability, listed values and bounds. Table Schema has no integer width, decimal
  precision or time zone, and the record schema has no decimal. Those import back as the importer's
  default.
- **Data generated from a profile passes its exports.** A text source's column holding values of
  other types, such as `events-json`'s `user_id`, generates as text, and its exports type it as a
  string. The exports read which columns those are from the engine's plan, so the two never
  disagree. [column-samplers.md](column-samplers.md) says which columns generate as text.
- **A sampled profile's types are the sample's.** A column whose sampled values all parse as its
  type is exported as that type, though a row outside the sample may hold text. Profile every row
  where a strict export must accept the whole source.
- **Data a profile was made from passes its exports.** A test checks each export with the tool
  that reads it, against rows generated from a profile and against rows a profile was made from.
- **The same profile exports the same bytes.** An export names the profile's digest and holds no
  time. ODCS `examples` are generated from the profile with a fixed seed, never read from data.
- **A profile made from data is checked first.** One that fails `profile check` is refused, exit 2,
  as `ProfileFailsCheck` in Python. An export never carries a literal the privacy policy did not pass.
- **No validator is a dependency.** The modules import pandera, dataframely or pyspark when they
  run, and doppeldata imports none of them to write one.

## A strict format carries a fact only where the profile states it of every row

pandera, dataframely, Table Schema, the record schema and ODCS's `logicalTypeOptions` fail a frame
on one row that breaks a rule. So each carries a range, listed values, lengths, a pattern or a
format only where the profile states it holds for every row. The rest are named in the export's own
description or comment, with the share that holds:

```python
# Left out:
# - order_id: the range 11 to 99990, trimmed at k_min rows from each end
# - customer_email: the email format, which pandera has no check of
```

- **A profile's extremes are trimmed at `k_min` rows from each end**, so no strict format bounds a
  profiled column. A seed's declared bounds hold for every row, so its exports carry them.
- **A profile writes a share short of every row below 1.** Its rates keep four significant digits,
  so two breaking rows in 100,000 are written 0.9999, never 1.
- **A profile made from data never holds a key or a relationship of every row.** The privacy floor
  writes fewer than `k_min` rows sharing a key's value as none, and fewer than `k_min` orphans as
  none. So the strict formats leave each key and foreign key out and name it. DQDL carries
  `Uniqueness` and `ReferentialIntegrity` with a threshold. A seed's declared keys and
  relationships hold for every row.
- **A profile made from a sample holds nothing of every row.** Its shares are measured over the
  rows it read, so each strict format leaves out what it would carry, naming the sample.
- **A pattern is stored where its shape holds half the values.** In CMS's DE-SynPUF carrier claims
  file, `ICD9_DGNS_CD_3` matches `^\d{4}$` in about half its values. A strict format leaves it out.
  DQDL writes it with a threshold, and ODCS as an `invalidValues` quality check with a percent
  ceiling.
- **A pattern holds the share of the shape it was generalized from.** The `claims` example's
  `claim_id` is `^CLM\d{9}$`, from a shape of three letters and nine digits holding every value. A
  shape cannot say which letters, so only the dominant shape vouches for a literal.
- **A text column whose values do not all parse as its type is typed as a string.** In the carrier
  claims file, most of `HCPCS_CD_10`'s values parse as integers. A reader typed integer would turn
  the rest into nulls, so the strict formats type it string and name the integer type as left out.
  DQDL keeps its range over the values that parse.
- **The record schema of a JSON source allows each JSON type its values hold.** The `events-json`
  example's `user_id` is `{"type": ["integer", "string"]}`, and its importer reads the first.
- **The record schema requires only a column never null.** doppeldata's NDJSON writer leaves a
  null field out, as many writers do.
- **The record schema writes `multipleOf` only as 1.** A validator tests it in binary floating
  point, so `multipleOf: 0.01` rejects 68.18.

## DQDL carries each share as a threshold three standard errors below it

A ruleset is a statistical check, so it carries what a strict format leaves out. A share takes
three standard errors, `STANDARD_ERRORS`, at the profile's row count, and at least one row's worth. A mean takes 3σ/√n
and a standard deviation 3σ/√(2(n−1)). A range between trimmed extremes holds the share inside
them, 1 − 2(`k_min` − 1)/n. So `orders`' `amount` range, over 100,000 rows, takes
`with threshold >= 0.999665`.

- **DQDL's `between` excludes both ends**, so a range is widened by one step: a unit, a decimal
  place or a day.
- **A rule DQDL cannot weigh is left out and named.** `ColumnLength` takes no threshold, and DQDL
  has no rule for an email format.
- **A mean that would overflow is left out.** Spark sums a 64-bit integer column in 64 bits, so
  the mean of a 15-digit claim id wraps.
- **Deequ ignores `with threshold` on a numeric or string `ColumnValues` rule.** It asserts
  every row and reports the share in its failure reason. The DQDL reference documents the
  threshold as the share of rows that must pass. The carrier claims export passes Deequ on the file
  it was profiled from, once each reported share is compared with its threshold.

## A seed exports only what its schema declared

A seed's `generated.declared` names each fact its schema stated, as
[seed-profiles-from-schemas.md](seed-profiles-from-schemas.md) lists. An export of a seed carries
those facts as exact, and no draw default. So a date the contract bounds nowhere exports
unbounded, though the seed draws it inside `DATE_WINDOW`. A seed is exported as it is, without
`profile check`, which fails every seed.

## A bundle exports as a Data Package and as DQDL with `ReferentialIntegrity`

- **Each table is a resource or a ruleset.**
- **A seed bundle's relationships are foreign keys.** One imported from a Data Package holding
  `foreignKeys` exports them again, and DQDL as `ReferentialIntegrity ... = 1.0`.
- **A profiled bundle's Data Package names each relationship it left out.** The privacy floor
  writes fewer than `k_min` orphans as none, so no relationship of it holds for every row. DQDL
  takes a threshold below the share the profile states, as for any share, less the rows the floor
  may hide.
- **The record schema describes one record, so a bundle takes `--table`**, or `table=` in Python.
  ODCS writes each table as a schema object, and the modules a class each.

## `--embed-profile` writes the profile into every Parquet and Arrow file it generates

A file read alone then holds the profile and scenario that made it, beside the seed and rows its
provenance keys already name. Every PROFILE argument takes such a file.

- **The key is `doppeldata.profile`, beside the provenance keys.** It holds the whole profile as
  canonical JSON. Its digest, taken without the generated fields a digest leaves out, is
  `doppeldata.profile_digest`. It runs to kilobytes.
- **`doppeldata.scenario` holds the scenario's canonical document**, as the manifest's
  `scenario.document` does. A write under no scenario writes no such key.
- **A file whose profile or scenario is not the one its digest names is refused**, exit 2, as
  `SourceError` in Python.
- **Parquet holds it once, as a plain key-value pair.** The other `doppeldata.*` keys sit in
  `ARROW:schema` as well, and a copy of the profile there would double its bytes. Arrow IPC holds
  it in the schema metadata.
- **Only Parquet and Arrow IPC hold it.** A write in another format refuses `--embed-profile`
  before writing a byte, exit 2, and Python raises `WriteFailed`.
- **A profile made from data that fails `profile check` is refused**, before writing a byte, exit
  2, and Python raises `ProfileFailsCheck`. The file carries every literal the profile holds, so it must
  pass the check an export passes.
- **`data generate FILE` defaults `--seed`, `--rows` and `--scenario` to what the file's write
  recorded**, and prints each value it took from the file. So `data generate FILE` alone writes
  the dataset FILE came from. `data plan` takes the same defaults, and `scenario check` takes the
  seed and rows. Python's `generate`, `stream`, `plan`, `write` and `spark` verbs take a data file
  the same way, each value left None. A key is read only where its value is left unset, so a
  malformed seed is refused only when the seed is not given.
- **Given none of the three, a file whose dataset this release would not make again is refused**,
  exit 2, naming the key that differs, such as `doppeldata.stream_version`. Given any of them, the
  write generates the dataset they make.
- **It is the profile as given, before a scenario's patch.** The scenario applies again from
  `doppeldata.scenario`.
- **A `cdc` write embeds both in the `LOAD` files and the `_expected` parts, never in a change
  file.** A stream writes thousands of change files, and a copy of the profile can outweigh the
  rows in each one.
- **The manifest's `profile.embedded` is true.** `regenerate()` refuses such a manifest, because a
  plan holds the profile's digest but never its JSON. `data generate FILE --embed-profile` writes
  the same dataset digest again, and the same bytes where the write used the profile's own format
  and layout.
- **`Profile.from_data_file` reads the footer through the extension**, so it needs no pyarrow. A
  file holding no profile raises `SourceError`, naming `embed_profile=True`.

## Rejected

- *Rejected:* frictionless, pandera, dataframely or datacontract-cli as a library dependency. Each lands on every consumer's Glue job, and only the test suite runs them.
- *Rejected:* one module holding a model of each pandera backend, each defined under `try`. A class
  defined conditionally breaks a type checker, so `pyspark-schemas` is its own format.
- *Rejected:* a strict format bounding a column at its trimmed extremes. It fails real data the
  profile describes, on the rows the trim left out.
- *Rejected:* ODCS `examples` read from the data. A profile holds no rows, and an export holds no
  value its profile does not.
- *Rejected:* `profile import --from data-file`. `profile expand FILE --out` writes the embedded
  profile as YAML.
- *Rejected:* embedding the profile by default. Out of the box a write generates data alone.
