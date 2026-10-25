# The profile format and its privacy floor

## A profile is YAML, because a reviewer reads and diffs it

The profile is the file between a real dataset and every synthetic copy, and it is committed. YAML is
the committed form, because a reviewer reads it, diffs it and comments on it in a pull request. JSON
is accepted on input. One JSON Schema validates both.

- **`profile_version` changes only on a breaking format change.** An added field keeps it. The tool
  records its own version separately, under `generated.tool`. A breaking change must ship with
  `profile upgrade`, which rewrites an older file in place. A profile newer than the installed
  library refuses to load and names its version.
- **Every section is optional.** A profile imported from a data catalog or a data contract has types
  and no statistics, and the generator falls back to defaults for what is missing.
- **Unknown fields survive a read and a rewrite.** An older tool never strips what a newer one wrote.
  The file is written back through ruamel.yaml, which also keeps a reviewer's comments and key order.

## The schema is derived from the Rust model, so Python and Rust cannot disagree

The generator deserializes a profile into Rust structs with serde, and schemars derives the JSON
Schema (Draft 2020-12) from the same structs. The schema is committed at
`library/src/doppeldata/schemas/profile.v1.json`, and a cargo test fails when it differs from what
the model derives. The Python profiler validates against that committed file.

JSON Schema cannot express ordering, such as quantiles rising or rates summing to 1. A second Python
pass checks those at the fields the model gives that meaning, and reports them in the same shape as
a schema error. Each problem carries a code, a JSON Pointer to the field and a message, so a
reviewer is pointed at the line. A schema error's code names the keyword that failed, such as
`schema-maximum`.

The schema's integer widths, such as `uint32`, are checked too. A number canonical JSON cannot
write exactly is refused before the schema runs: NaN, an infinity, or an integer beyond ±2^53.
Polars returns NaN for the skew of a constant column, so a profiler must write such a statistic as
null.

## A profile's identity ignores formatting

The determinism contract keys on the profile's SHA-256, so formatting, comments and key order must
not change it. The hash is taken over the RFC 8785 canonical JSON form of the profile.
`generated.at` and `generated.tool` are left out, so re-profiling unchanged data gives the same
identity.

The library alone computes the digest, and the generator takes it as an argument. Every way into
the generator passes through the library, so one implementation leaves nothing to disagree with.
YAML is parsed only in Python, because serde_yaml is deprecated. A YAML timestamp is read as the
text written, so `low: 2020-01-03` and `low: "2020-01-03"` are one value.

## A bundle is one file or a directory, and both forms share one identity

A bundle holds several table profiles and the relationships between them. It is written as one file
with `kind: bundle` and every table inline, or as a directory whose `bundle.yaml` names one file per
table. The directory form lets each table be reviewed and regenerated alone. The digest is taken
with every table inline, so moving between the forms keeps the identity. A table file's `name` must
match the key the index gives it.

A relationship stores its fan-out as shares and quantiles, never key values. So join skew survives
into synthetic data while no key leaves the source.

## Generated files name the profile they came from

Every Parquet and Arrow file the generator writes must carry schema-level key-value metadata, under
the `doppeldata.*` constants in the core crate's `profile::provenance`. So the next profiler can
tell synthetic data from real, and any one file names the whole dataset it belongs to.

| Key | Holds |
| --- | --- |
| `doppeldata.profile_name` | the profile's `name` |
| `doppeldata.profile_version` | the format version the profile was read as |
| `doppeldata.profile_digest` | the profile's digest |
| `doppeldata.scenario_name` | the scenario's name, left out when no scenario applies |
| `doppeldata.scenario_digest` | the scenario's digest, left out when no scenario applies |
| `doppeldata.seed` | the seed, in decimal |
| `doppeldata.stream_version` | the engine's stream version |
| `doppeldata.dataset_rows` | the dataset's row count, in decimal |
| `doppeldata.dataset_digest` | the digest of the dataset's identity |

The stream version, profile digest, scenario digest, seed and row count are the dataset's whole
identity, and the dataset digest sits beside them as a quick equality check. The row key names the
dataset, because every file of it carries the same schema: a file of 65,536 rows reading
`rows: 10000000` would mislead.

## Sketches are an optional sidecar, never written for a sensitive column

Mergeable sketches (KLL, HLL, theta, frequent items) live in a binary file beside the profile, in
Apache DataSketches serialization. The generator never requires it.

The sidecar is never written for a column the privacy policy marks sensitive. KLL, REQ and t-digest
keep real values, frequent items keeps raw strings, and theta's 64-bit hashes of a low-entropy
identifier, such as a nine-digit SSN, can be brute-forced.

## No stored statistic rests on fewer than 11 rows

A profile holds aggregates rather than records. What leaks from an aggregate is a cell derived from
one person or a few: a top-k entry, a histogram bin, a minimum or a maximum. The US CMS cell-size
suppression policy forbids reporting any cell from 1 to 10, and any percentage from which such a cell
can be derived. That gives a domain-neutral default, `k_min: 11`, configurable per run.

The `default` policy:

1. A literal is stored only when at least `k_min` rows hold it, and only in a column not typed
   `identifier`. The "other" remainder of a top-k list obeys the same floor.
2. An `identifier` column stores no literals, no extremes and no top-k. It keeps its null rate,
   distinct ratio, length distribution, character-shape histogram and a generalized pattern.
3. Exact extremes are replaced by the values at rank `k_min` from each end, or omitted.
4. Free text stores length and token statistics only.
5. Every histogram bin, frequency entry, conditional-table cell and partition share meets `k_min`.
   A smaller cell merges or folds into `other`.
6. Parquet footer minimums and maximums are never copied for an `identifier` or `sensitive` column.
7. A pattern is stored only when it holds no alternation of data literals.

`strict` also treats quasi-identifiers as identifiers, and drops every lookup and conditional table
involving one. `none` writes everything. It exists for data that is already synthetic or public, and
`profile check` must always report a `none` profile.

## Every literal in the file is listed with the rule that admitted it

The profile's `privacy.audit` list names each literal it keeps and the rule that allowed it, so a
review is one read. `profile check` must read the file alone, with no data, and recompute each rule
from what the file states.
