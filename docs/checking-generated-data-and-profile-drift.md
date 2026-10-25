# How generated data is checked against its profile, and two profiles diffed

## A check compares two profiles, so the real rows are never read again

`data check PROFILE DATA` profiles DATA with `edges_from=PROFILE` and PROFILE's privacy policy and
`k_min`, then measures the new profile against PROFILE. Under the same policy, each field the new
profile writes is one PROFILE could hold, such as a ZIP column's prefixes under `healthcare`. The histograms and value lists then compare bin for bin.
Generated data has its own `source_fingerprint`, so the stats cache never hands PROFILE back
unchanged. DATA is typed with the detector packs PROFILE records, so a type one of them declares can
be confirmed.

`profile diff A B` measures B against A with the same metrics. B profiled with `--edges-from A`
carries overflow counts, so its boundary adherence and overflow can be measured. A B binned
afresh leaves `boundary_adherence` unmeasured, and every other metric measures it.

## A metric is a function from two profiles to values, and the registry is data

`doppeldata.profile_comparison.metrics.METRICS` lists each metric with its name, its summary, its
bound, its default threshold and its measurement. A measurement takes both profiles and yields a
value per column, or one for the table. The reports, `--thresholds` and the help of `data check`
and `profile diff` read the registry. So a metric added there reaches all three with no other
change, and that help is where each metric's summary and default threshold are listed. The
sections below give the reason behind each measurement that needs one.

The names follow SDMetrics where one exists. Its `KSComplement` is one minus the KS distance,
which `ks` bounds from below, and its `TVComplement` is one minus a categorical column's `tvd`.

## `ks` is the largest gap at the points the source profile states, a lower bound on the KS distance

A profile states its CDF at a few points and nothing between them. So `ks` reads each side only
there, and interpolates nothing.

- **The points are the source's quantiles, interior histogram edges and zero.** An edge pins the
  share below it, from the counts. Zero pins the share below it at `negative_rate`, and through it
  at `negative_rate + zero_rate`. A p-quantile q pins less: the share below q is at most p, and the
  share through q at least p. So a value several quantiles share is one jump they all sit on.
- **Each side's share at a point is a range.** It is every share a CDF holding all of that side's
  stated points can have there. The gap at the point is how far apart the two ranges are, and zero
  where they overlap. `ks` is the largest gap over the source's points, read below each point and
  through it.
- **So `ks` is a lower bound on the KS distance between the two datasets.** How the generator
  fills the span between two stated points is its own choice. Values spread across a span the
  source left empty pass `ks`. Where the source's values are few, `distinct_ratio_delta` fails them.
- **The outer edges are left out.** A profile made without overflow folds the rows beyond its
  trimmed extremes into its outer bins. `boundary_adherence` measures those rows instead.
- **An interior edge is exact on the other side when that side was counted against the source's
  edges.** A check always counts it that way, and a diff does when B was profiled with
  `--edges-from A`. Otherwise the other side's share at the edge is the range its own points allow.
  Every rate and count is a share of the non-null values.

A sequence column draws nothing, so it holds its start and step at any row count and has no `ks`.
A date or timestamp column states no point on its CDF, so `tvd` measures its shares instead.

*Rejected:* reading both profiles through the generator's own inverse CDF. That reference proves
only that the sampler matches the CDF it built. A builder that drops a column's zero run draws data
matching what it built, so a check through it passes data the profile never described.

## `tvd` measures over the source's categories, or a date's months, weekdays and hours

A string or numeric column's categories are its `top_values`, and a boolean's are `true` and
`false`. A numeric column with a list is measured by `ks` too, because its quantiles still stand.
Every value the source does not list counts as one more category, so a value the other profile
lists and the source does not adds to that category's share.

- **A source value missing from the other's list may sit in its `other` share.** The other profile
  folds each value under its `k_min` rows into `other`. So a missing value is taken to hold its
  source share there, up to `k_min − 1` of the other's rows, while the `other` share has room. A
  list of hundreds of values checked at a few thousand rows then measures no fold as distance.
- **The value is the distance beyond what sampling puts between two profiles of one
  distribution.** A category of share `p`, counted over `n_a` and `n_b` values, lies about
  `sqrt(2/π · p(1 − p) · (1/n_a + 1/n_b))` apart by chance, and never more than `2p`. Half the sum
  of those over every category, `other` included, is subtracted, and the value is never below 0.
  So a correct draw of a list of hundreds of values passes at ordinary row counts, and a draw moving
  a tenth of its rows onto one value still fails.

A date or timestamp column's shares are `by_month`, `by_dow` and `by_hour`. Its `tvd` is the
largest of their three distances, over those both profiles hold. It cannot see a shift inside one
hour or one month. `gap_rate` is not measured, because which days are empty is a seeded draw no
profile records.

## `boundary_adherence` is read from the overflow counts

The profiler sets a histogram's outer edges at the trimmed extremes, or half a unit beyond them in
an integer column. The `k_min` floor merges only interior bins, so a frozen profile's outer edges
are still the source's. The adherence is `1 − (below + above) / (below + Σ counts + above)` of the
other profile's histogram.

- An overflow of 1 to `k_min − 1` rows is written as 0, so adherence overstates by at most
  `2 · (k_min − 1)` rows.
- A histogram not counted against the source's outer edges carries no `overflow` that
  measures them, so the column's `boundary_adherence` is unmeasured.

## `distinct_ratio_delta` measures a column against the range both readings of its profile allow

The ratio is a column's distinct count over the rows it was counted over. A ZIP column written as
`string.prefixes` counts its distinct prefixes, and the generated data is profiled under the
source's policy, so both readings count prefixes. One profile cannot show
whether a column keeps that ratio at another size or keeps its count. So the generator holds the
ratio, and the check accepts either. With N source rows holding D distinct values, and n rows
checked:

- **Below N**, the ratio may run from the source's, held, up to what a random sample of n source
  rows holds where each value fills an equal share: `D · (1 − (1 − n/N)^(N/D)) / n`.
- **Above N**, it may run from `D / n`, the count saturated, up to the source's.
- **The value is how far the observed ratio falls outside that range**, relative to its nearer end,
  and 0 inside it. At n = N both ends are the source's ratio, so the value is the relative change
  in ratio.

So the range widens as n moves from N, the same lower-bound stance `ks` takes. A column that drifts
toward either reading passes, and only a count outside both fails. A few-valued column's lower end
falls near zero at a small n: a 5-value column in 100,000 rows, checked at 2,000, passes holding
anywhere from 0.1 to 5 values. `tvd` is what checks its values there.

Every kind of column is measured. A float column whose source holds cents, generated as continuous
values, shows nowhere else: its quantiles and histogram shares match. A date range collapsed to a
few days shows here too.

## Three metrics measure the files against the source's layout, scaled to the rows checked

A source profile with a `source.layout` gets three metrics, one statistic each. Each value is a
relative change, as `distinct_ratio_delta`'s is, and each defaults to at most 0.10.

- **`file_count_delta` is the file count's change.** The count expected is the source's scaled to
  the rows checked. So a check at the profile's row count holds the source's file count, and one at
  a tenth of it holds a tenth.
- **The file count expected is never under one file for each partition the rows can fill.** A
  partitioned writer writes a file for every combination of the keys' values. So the count is at
  least the product of each key's value count, or the rows checked where those are fewer.
- **`rows_per_file_delta` is the median rows per file's change**, against the source's, or the
  rows checked over the files expected where that is fewer.
- **`partition_share_delta` has a row per Hive key the source lists value shares of.** Its value is
  the largest change in one value's share of rows. A value the data lacks has a share of 0, so its
  change is 1.
- **Data whose profile records no layout leaves each of the three unmeasured**, as a table in
  memory does. A source profile with no layout, as a hand-written one may be, gets none of them.

## A metric the other profile cannot supply is a row with no value

Where the source states what a metric compares and the other profile holds nothing to compare it
with, the report holds the metric with a null `value` and the reason in `unmeasured`. A column
under `k_min` values holds no quantiles, so a check of 10 rows leaves `ks` unmeasured. A check of
an empty file leaves nearly every metric unmeasured.

- **A check fails the row.** A check vouches for the data, so a statistic the data cannot supply
  is one it cannot vouch for. Zero rows and 10 rows both exit 3.
- **A diff lists the row and passes it.** A diff reports what changed between two profiles. A B
  binned afresh carries no overflow, so its `boundary_adherence` is unmeasured, and `profile diff`
  prints one notice naming `--edges-from A`.
- **A check under a scenario passes a metric a perturbation moves.** Its row has a null `value`
  and `skipped_by` naming the perturbation's id. [scenario-overlays.md](scenario-overlays.md) lists
  which metrics each op moves.

*Rejected:* a separate list of skipped metrics. A reader would join two lists to see one column.

## A check fails on a column the data adds, drops or retypes, and a diff on any changed field

A column's kind is the profiler's `ColumnKind`, read back from the `logical_type` it wrote, with an
Arrow decimal type telling a decimal from a float.

- **A check fails** on a column the data adds or drops, or holds as another kind or semantic type.
- **A check lists and passes a change a writer alone causes.** A Parquet write given
  `timestamp_unit` casts every timestamp column to that unit, and no unit reproduces a source's
  INT96. Data written as CSV or JSON reads back at the text reader's types, so an `int32` column
  profiles as `int64`, and a decimal as a float.
- **A semantic type change passes where no detector pack both profiles ran declares the type.** A
  hand-written profile may carry any type, and one no pack declares is found in no data. A
  column's `semantic.pack` names the pack, and in a check the packs it ran name it for a column
  that does not.
- **A check passes a type change on a type a code list decides**, where the two profiles record
  different contents for that list. A check profiles generated data without the list, and
  generated codes are drawn from the type's hint rather than the list.
- **A check under a scenario passes a change on a column a perturbation writes.** Its message
  names the perturbation. That covers a column a schema drift adds or renames, and the `ingest`
  column a late arrival adds.
- **A diff fails on every other changed field.** That covers a changed Arrow type or kind, which
  there means the source itself changed. It also covers a column with more than `OVERFLOW_SHARE`
  of its values outside the edges or value list frozen from A, a CSV dialect setting a writer takes
  that changed, and a partition key B adds. The rest of `source.csv`, such as the malformed rows
  and the sniffers' confidence, describes the files rather than a setting a writer takes, so a diff
  does not compare it. The tool versions and packs a profile records are how it was made, so a diff
  compares none of them.

## A failed check exits 3, drift exits 1, and a verb that could not finish exits 3

Nothing in doppeldata changes the data it checked, so a failed check needs a person, and exits 3.
Drift between two months of one dataset is expected, so `profile diff` exits 1, as `diff` does.

- **A check whose scenario's expectation misses its bounds exits 3**, as a failed metric does.
- **A check or a diff that could not finish exits 3**, from a refused S3 listing to a crash. It
  needs a person too, and 1 then means drift and nothing else.
- **No answer from S3 exits 4**, because the next run can succeed with nobody acting.
- **A missing file, an unreadable profile, an invalid scenario or a bad thresholds file exits
  2.** So does DATA holding
  `_manifest.partial.json`, because the write there stopped or is still running.

## The report's shape does not change when a scenario applies

The check report holds `scenario`, null without one, and `expectations`, empty without one.
`verified` covers the expectations and is true when none are declared, while `passed` covers every
metric and column change. A scenario fills those fields, so a script reading the report reads the
same shape either way.

- **`scenario` holds the scenario's name and digest.** Several composed are named together, joined
  by `+`.
- **`profile` still names the profile as given.** The data is measured against it patched, so a
  null rate the scenario's patch sets is the one expected.
- **Each expectation is a row of its metric, column, bounds, measured value and `passed`.**

The JSON Schemas are derived from the core crate's `profile_comparison`
model, and `doppeldata schemas show check` and `schemas show diff` print them.

*Rejected:* one report model for both verbs. A diff has two profiles and no data, so `profile` and
`data` would each mean something else there.
