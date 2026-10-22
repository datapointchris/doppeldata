# The profile format and its privacy floor

## A profile is YAML, because a reviewer reads and diffs it

The profile is the file between a real dataset and every synthetic copy, and it is committed. YAML is
the committed form, because a reviewer reads it, diffs it and comments on it in a pull request. JSON
is accepted on input. One JSON Schema validates both.

- **`profile_version` changes only on a breaking format change.** An added field keeps it. The tool
  records its own version separately, under `generated.tool`.
- **Every section is optional.** A profile imported from a data catalog or a data contract has types
  and no statistics, and the generator falls back to defaults for what is missing.
- **Unknown fields survive a read and a rewrite.** An older tool never strips what a newer one wrote.
  The file is written back through ruamel.yaml, which also keeps a reviewer's comments and key order.

## The schema is derived from the Rust model, so Python and Rust cannot disagree

The generator deserializes a profile into Rust structs with serde, and schemars derives the JSON
Schema (Draft 2020-12) from the same structs. The schema must be committed in the library, and CI
must regenerate it and fail on a difference. The Python profiler validates against that committed
file.

JSON Schema cannot express ordering, such as quantiles rising or rates summing to 1. A second Python
pass checks those and reports them in the same shape as a schema error.

## A profile's identity ignores formatting

The determinism contract keys on the profile's SHA-256, so formatting, comments and key order must
not change it. The hash is taken over the RFC 8785 canonical JSON form of the profile.
`generated.at` and `generated.tool` are left out, so re-profiling unchanged data gives the same
identity.

YAML is parsed only in Python, because serde_yaml is deprecated. Python sends the canonical bytes to
Rust, and both hash the same bytes.

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
