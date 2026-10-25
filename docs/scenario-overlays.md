# Scenario overlays

## A scenario is a merge patch, ordered perturbations and declared expectations

A profile describes data as it is. A scenario bends it on purpose, for a test that needs a hot key,
late rows or a schema change. It is a file of three parts, validated by the JSON Schema derived from
the core crate's `scenario` model, which `doppeldata schemas show scenario` prints.

- **`patch` changes what a profile can express as a value.** It is an RFC 7396 merge patch with one
  extension: `columns` is a map keyed by column name, so a patch names only the columns it changes.
  RFC 7396 replaces an array whole, so a patch on the profile's list would restate every column. A
  column patched to null is removed.
- **`perturbations` apply faults a profile cannot express**, in list order. `doppeldata scenario
  list` prints each op, and `scenario show OP` its parameters.
- **`expect` declares what the scenario promises**, such as the share the hot keys hold. A check
  certifies those and nothing undeclared.

A scenario is applied with `scenario=` in Python, or `--scenario` on `data plan`, `data generate`
and `data check`. Several compose in order: their patches merge left to right, their perturbations
and expectations concatenate, and the composition's name joins theirs with `+`.

The manifest records the document the scenario's digest covers, and each composed member's digest.
So a dataset regenerates with no scenario file, and an edited member shows which one moved.

*Rejected:* a patched column the profile does not hold being added, as RFC 7396 would add it. A
misspelled column would then generate a new column from a fragment. It is refused as
`unknown-column`.

## The engine applies the patch and the perturbations, inside the block path

1. The engine applies the patch, giving the effective profile.
2. The samplers generate each block from it.
3. Each perturbation that writes rows applies to the block, in list order.
4. The writers apply the ops that shape files to the files they make.

- **The stream version governs the patch as it does the samplers.** A change to how a patch merges
  moves generated values, so it raises the stream version. The determinism test pins values
  generated under a scenario fixture that holds a patch.
- **The identity's `profile_digest` is the profile's as given, not the patched one's.** The
  scenario's digest covers the patch. So the manifest's profile path names the file whose digest
  the identity holds.
- **A scenario changes no draw a profile alone makes.** A dataset with no scenario draws only from
  the profile's streams, so no scenario code moves its values.
- **`Scenario.verify` and the expectation metrics are Python over a Polars LazyFrame.** The library
  reads data through the profiler's readers already, and a metric there is a few lines.

## Each perturbation draws from a stream keyed by its id, never its position

Each op's draws come from `stream_key(table, id, "perturbation.<draw>")`, and a value an op draws
from a column's sampler comes from that sampler on the table `"<table>\0perturbation <id>"`. So
inserting a perturbation leaves every other one's rows unchanged, compared by Arrow value.

- **The id defaults to `<op>:<column, or table>:<n>`.** n counts earlier entries of that op and
  column. So inserting a `noise` moves no `missingness`, while a second `hot_keys` on one column
  numbers after the first. An entry that must stay put under any edit names its own `id`.
- **Two entries sharing an id are refused** as `duplicate-perturbation-id`, because they would
  share a stream.
- **A scenario's digest is the SHA-256 of its RFC 8785 canonical JSON, with every id resolved and
  no `description` or `expect`.** So naming a default id, rewording the description or moving a
  bound keeps the identity. A bound is a claim about the data, not an input to it.

## Duplicates change the row count, so each block's count is planned up front

`rate` is the share of the rows written that are copies. So a block of n generated rows carries
`n × rate / (1 − rate)` copies, its fraction drawn on a stream keyed by the block. The copies follow
the block's generated rows, so block boundaries never move, and a Spark chunk generates what a
local run does.

- **`rows` and the identity's `dataset_rows` count the rows generated.** The copies come on top.
  `data plan` prints `rows_written`, the rows the files will hold.
- **Each row of a block is filed with a generated row of it, its home.** A range takes the rows
  homed in it, so a block cut between two files sends each copy with its home.
- **A `within_file` copy is homed to its source**, so it is written to its source's file at any
  file size.
- **An `across_files` copy comes from the block half the dataset away**, and is homed to a row of
  its own block. So with two files, the copy lands in the file its source does not.
- **A write is refused when a file holds more rows than the half-dataset reach**, or the dataset is
  one block. A copy could then share its source's file, so `invalid-parameter` names the op's
  `scope`.
- **`exact: false` varies one column of each copy**, taking that column's value from another row.
  A primary key column is never the one varied.

## The ops that shape files apply only where files are written

`schema_drift` and `small_files` change the files rather than the rows. `generate`, `stream` and
`doppeldata.spark.generate` write no files, so each refuses them as `op-needs-files`.

- **A file drifts when its first row lies at or past `after_fraction` of the rows.** The writers
  apply the drift to the whole block before a range takes its rows. Its steps run as drop, widen,
  rename, add, then reorder.
- **A drift no file reaches is refused**, naming `after_fraction`. A one-file dataset drifts
  nothing, which would leave the scenario silently unapplied.
- **A partition key the drift drops or renames is refused**, because a drifted file could not be
  filed under it.
- **A drift written as CSV, TSV or Arrow is refused** as `op-not-for-format`. Their files must
  share one header or schema to read as one table. Parquet, JSON and NDJSON read back as the union.
- **An added column is drawn per written row**, so a copy and its source differ in it.
- **`small_files` sizes the files from a lognormal**, tabulated as the layout's `rows_per_file`
  quantiles. Its sizes lay over the profile's, and a `file_rows` given to the write lays over both.

## The ops that need a choice make it this way

- **`hot_keys` takes the column's most frequent values, by stated rate.** A column listing fewer
  than `keys` values takes the rest from its own sampler, on the perturbation's stream. One that
  draws too few distinct values is refused at `keys`.
- **`missingness` under MAR nulls a row with chance `rate + (2r − 1) × min(rate, 1 − rate)`.** r is
  the row's `depends_on` value as an average-rank share of its block, and a null there counts as
  the middle. So the rate averages `rate`, and rises with the other column. MNAR ranks the column's
  own values, and MCAR is uniform.
- **`late_arrival` shifts the event time earlier by at least one tick.** `ingest` names a column
  holding each row's unshifted time. A profile lacking it gains it, at the event column's type.
  `late_arrival.rate` measures against it, so an expectation of it needs `ingest`.
- **`categorical_shift` moves as few rows as reach the new mix.** Each value it names takes its
  share, and every other value keeps its proportion of what is left.
- **`out_of_range` writes a sentinel cast to the column's type, or a value past an extreme.** The
  extremes come from the profile's `extremes`, else the outer quantiles.
- **`partition_skew` gives each value named its share**, so a write partitioned by the column fills
  its directories in those shares.

## Each refusal names its JSON Pointer before anything is generated

A refusal is an `InvalidScenario` whose issues each carry a `code` and a `field`. The field is an
RFC 6901 pointer into the scenario, such as `/perturbations/2/keys`. `doppeldata scenario check
--json` prints them.

- **A code names what it refuses**, such as `unknown-column` or `op-not-for-format`.
  `invalid-parameter` says least: a parameter out of range, or one the files cannot hold, such as
  a drift no file reaches. The format's own faults carry the profile's codes.
- **An unknown op or metric lists every one doppeldata knows**, and its issue's `closest` holds the
  nearest names.
- **`doppeldata scenario check FILE PROFILE` exits 3 on a refusal.** It validates the scenario,
  applies its patch, and plans each perturbation and the file layout as `data generate` would with
  the same layout flags.
- **A data verb exits 2 on one**, as it does on an invalid profile.

## `data check --scenario` skips what a perturbation moves, and `verified` covers the expectations

The data is measured against the effective profile, so a patched null rate is the one expected.

- **`data check` refuses a scenario other than the one the data was written under.** Where DATA
  holds the `_manifest.json` a write left, a `--scenario` of another digest is refused as
  `scenario-differs-from-data`. No `--scenario` on data whose manifest records one is refused as
  `data-has-scenario`. Both exit 2. A scenario's skips would otherwise pass data it never touched.
- **A metric a perturbation moves is a row with no value and `skipped_by` naming it.** It passes. A
  perturbation moves every metric on a column it writes, as the engine reports its columns.
- **An op that changes the row count or the file sizes also moves the table metrics those set.**
  `MOVED_METRICS` in `profile_comparison/scenario_moves.py` lists them. A partition key's
  `partition_share_delta` is named for its column, so `partition_skew` moves it.
- **A column change on a column a perturbation writes passes**, its message naming it, as the
  columns a drift adds do.
- **Each expectation is measured by `Scenario.verify`.** `hot_keys.share` is the share of rows
  holding the column's `keys` most frequent values. `late_arrival.rate` is the share whose column
  is earlier than its `ingest`. `missingness.rate` is the share that is null, the profile's own
  nulls included.
- **`duplicates.rate` is the share whose `column` repeats an earlier row's**, a key each copy
  shares with its source. A later perturbation writing that column is refused, because a copy's
  key would then differ from its source's.
- **An expectation on a column the data lacks fails**, with `unmeasured` naming the column. A
  drift that drops it reaches this.
- **`verified` is false when an expectation misses its bounds**, and `data check` then exits 3.

*Rejected:* certifying a statistic the scenario did not declare. `verified` would then mean
something a reader of the scenario could not see.

## The examples are written for the `orders` profile

The example scenarios ship as package data beside the example profiles, and `doppeldata scenario
list` names them. `--scenario hot-key` takes one by name.

- **`Scenario.example(name, **parameters)` replaces parameters of the example's one perturbation.**
  `column=` moves its expectations with it.
- **A keyword changing any other value drops the expectations.** Each was written for the
  example's own values.
- **Any keyword drops the description**, which names those values.

## A drifted Parquet dataset reads as the union of its files' schemas

So `data check` profiles a written `schema_drift` as one table.
[profiler-statistics-and-readers.md](profiler-statistics-and-readers.md) says how the union is read.
