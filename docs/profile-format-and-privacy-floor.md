# The profile format and its privacy floor

## A profile is YAML, because a reviewer reads and diffs it

The profile is the file between a real dataset and every synthetic copy, and it is committed. YAML is
the committed form, because a reviewer reads it, diffs it and comments on it in a pull request. JSON
is accepted on input. One JSON Schema validates both.

- **`profile_version` changes only when an older engine would misread a file.** A field an older
  engine ignores must leave that engine's reading of the file valid, and keeps the version. A field
  that changes how an existing field reads raises `profile_version`. So a ZIP column's three-digit
  prefixes are a field of their own, `string.prefixes`: written as `top_values`, they would be read
  as whole values. The tool records its own version separately, under `generated.tool`. A breaking
  change must ship with `profile upgrade`, which rewrites an older file in place. A profile newer
  than the installed library refuses to load and names its version.
- **Every section is optional.** A profile imported from a data catalog or a data contract has types
  and no statistics, and the generator falls back to defaults for what is missing.
- **Every section of a profile accepts a key it does not know.** A job pinned to an earlier library
  still loads a profile a later release wrote. That is what lets an added field keep
  `profile_version`. The settings a caller hands a writer are not a profile, and refuse an unknown
  key.
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
Each `generated` field `GENERATED_FIELDS` in `doppeldata.profile_format` marks unhashed, such as
`at`, `tool` and `source_fingerprint`, is left out. So re-profiling unchanged data gives the same
identity, wherever the copy that was read sits.

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
the `doppeldata.*` constants in the core crate's `profile::provenance`, where each key says what it
holds. So the next profiler can tell synthetic data from real, and any one file names the whole
dataset it belongs to.

The keys carry the profile's name and format version, and the dataset's whole identity: the five
values `determinism-contract.md` names, and their digest as a quick equality check. The row key
names the dataset, because every file of it carries the same schema. One part file reading
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
   distinct ratio, length distribution, character-shape histogram and a generalized pattern. An
   integer identifier's extremes become `method: digit_bounds`, the least and greatest integers
   with as many digits as its trimmed extremes, so the generator draws values of the right length.
3. Exact extremes are replaced by the values at rank `k_min` from each end, or omitted.
4. Free text stores length and token statistics only.
5. Every histogram bin, frequency entry, conditional-table cell and partition share meets `k_min`.
   A smaller cell merges or folds into `other`.
6. Parquet footer minimums and maximums are never copied for an `identifier` or `sensitive` column.
7. A pattern is stored only when it holds no alternation of data literals.

`strict` also treats quasi-identifiers as identifiers, and drops every lookup and conditional table
involving one. `healthcare` adds HIPAA Safe Harbor, as `healthcare-policy.md` describes. `none`
writes everything, with exact extremes and no `k_min`. It exists for data that is already synthetic
or public, and `profile check` always reports a `none` profile. `dp` is reserved in the format for
a differential-privacy policy, which doppeldata does not apply. `profile create --policy dp` is a
usage error, and `profile check` reports a `dp` profile as a policy it cannot check.

## The policy is a pass over the computed profile, so every backend inherits it

A backend computes statistics and knows nothing of the policy. The pass reads the profile's own
fields, decides each column's rule from its kind, its semantic type and sensitivity, and its name,
then removes what that rule withholds. It drops any literal left that fails its floor, then lists
every literal kept. A Spark backend therefore needs no policy code of its own.

The pass then holds what it wrote to `profile check`'s rules, and raises where one fails. Both read
each rule from `privacy_policy/treatment_rules.py`, so the writer cannot write a file the check
refuses.

Three healthcare rules need data, so the statistics pass computes them where the policy asks: a ZIP
column's prefix counts, a date's gap rate over whole years, and an age's histogram binned to its
cap. Typing runs before binning for that reason.

## Every literal in the file is listed with the rule that admitted it

The profile's `privacy.audit` list names each literal field it keeps and the rule that allowed it,
so a review is one read:

```yaml
privacy:
  policy: default
  k_min: 11
  suppressed:
    - {column: customer_email, reason: identifier, semantic: email}
  audit:
    - {column: status, field: /string/top_values, values: 5, rule: "count >= k_min", min_count: 7910}
```

`field` is the field's RFC 6901 pointer within its column, so reordering the columns or adding one
leaves every other entry true. `values` counts the literals it holds, and `min_count` is the fewest
rows any of them or any cell of it rests on. A suppression's `reason` is one of the
`SuppressionReason` values `doppeldata schemas show profile` prints.

Each kind of field is admitted by one rule:

| Field | Rule | What `profile check` recomputes |
| --- | --- | --- |
| top values, ZIP prefixes, shapes, missing-value tokens | `count >= k_min` | each rate times the rows holding a value, the remainder allowed to be 0 |
| histogram bins and overflow, calendar shares, a boolean's true and false counts | `cell >= k_min` | each cell, which may be 0 |
| numeric and temporal extremes | `trimmed_k` | `method: trimmed_k`, over at least `2 · k_min − 1` values |
| quantiles | `k_min rows from each end` | the rows between each quantile and the nearer end, its own counted |
| a pattern | `generalized` | no alternation, optional part, or class narrower than whole digit and letter ranges |
| a fitted family | `fitted within the trimmed extremes` | trimmed extremes beside it |
| a sequence start | `follows the trimmed extreme` | the trimmed extreme at its end, less `k_min − 1` steps |

An integer identifier's digit bounds hold no data value, so they are not a literal.

## profile check reads the file alone

`profile check PROFILE` takes a file or an example name, and reads no data. It validates the schema
first, then the policy. The rows a rate is taken of are `generated.sample.rows_read`, or else
`dataset.row_count`, less the column's null rate. Every literal field must have an audit entry
whose column, pointer, rule and value count match it, and must meet that rule. A column the policy
withholds from must hold nothing withheld, and an audit entry naming no literal field is reported
too. A bundle is checked table by table, each table held in its own file read as that file. A
member file that is a bundle, names itself other than its key, or cannot be read is a violation at
`/tables/<name>`, and the other tables are still checked.

It exits 0 when clean and 3 on a violation, because a violation needs a person. A file it cannot
read is a usage error, exit 2. Each violation prints as the field it is at, what was found and what
was expected. `--json` prints `passed` and each violation as `{code, field, rule, treatment,
detail}`. `field` is an RFC 6901 pointer, `rule` is the audit rule the field is held to, and
`treatment` is the column's suppression reason, each null where none applies. So a CI job can run
it on every committed profile.
