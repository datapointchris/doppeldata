# How generated data is checked against its profile, and two profiles diffed

## A check compares two profiles, so the real rows are never read again

`data check PROFILE DATA` profiles DATA with `edges_from=PROFILE` and PROFILE's `k_min`, then
measures the new profile against PROFILE. The histograms and value lists then compare bin for bin.
Generated data has its own `source_fingerprint`, so the stats cache never hands PROFILE back
unchanged.

`profile diff A B` measures B against A with the same metrics. B profiled with `--edges-from A`
carries overflow counts, so its boundary adherence and overflow can be measured. A B binned
afresh is measured by every other metric.

## A metric is a function from two profiles to values, and the registry is data

`doppeldata.profile_comparison.metrics.METRICS` lists each metric with its name, its summary, its
bound, its default threshold and its measurement. A measurement takes both profiles and yields a
value per column, or one for the table. The reports, `--thresholds` and the help of `data check`
and `profile diff` read the registry. So a metric added there reaches all three with no other
change, and that help is where each metric's summary and default threshold are listed. The
sections below give the reason behind each measurement that needs one.

The names follow SDMetrics where one exists. Its `KSComplement` is 1 − `ks`, and its `TVComplement`
is 1 − `tvd`.

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
- **Every interior edge is exact on both sides.** The other profile is counted against the source's
  edges, and every rate and count is a share of the non-null values.

A sequence column draws nothing, so it holds its start and step at any row count and has no `ks`.
A date or timestamp column states no point on its CDF, so `tvd` measures its shares instead.

*Rejected:* reading both profiles through the generator's own inverse CDF. That reference proves
only that the sampler matches the CDF it built. A builder that drops a column's zero run draws data
matching what it built, so a check through it passes data the profile never described.

## `tvd` measures over the source's categories, or a date's months, weekdays and hours

A string column's categories are its `top_values`, and a boolean's are `true` and `false`. Every
value the source does not list counts as one more category, so a value the other profile lists
and the source does not adds to that category's share. A source value missing from the other's
list counts as 0 there. A value under `k_min` rows drops off a list, so the error is under
`k_min` rows.

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
  measures them, so the column has no `boundary_adherence`.

## `distinct_ratio_delta` measures every column

The ratio is the distinct count over the rows, so it compares profiles of different sizes. Every
kind of column is measured. A float column whose source holds cents, generated as continuous
values, shows nowhere else: its quantiles and histogram shares match. A date range collapsed to a
few days shows here too.

## A check fails on a column the data adds, drops or retypes, and a diff on any changed field

A column's kind is the profiler's `ColumnKind`, read back from the `logical_type` it wrote, with an
Arrow decimal type telling a decimal from a float.

- **A check fails** on a column the data adds or drops, or holds as another kind or semantic type.
- **A check lists and passes an Arrow type change alone.** The writers cast every timestamp to one
  unit, because Parquet's Arrow writer cannot write INT96. Data written as CSV reads back at the
  text reader's widths, so an `int32` source column profiles as `int64`.
- **A diff fails on every changed field.** That covers a changed Arrow type, which there means the
  source itself changed. It also covers a column with more than `OVERFLOW_SHARE` of its values
  outside the edges or value list frozen from A, a CSV dialect setting that changed, and a
  partition key B adds.

## A failed check exits 3, drift exits 1, and a verb that could not finish exits 3

Nothing in doppeldata changes the data it checked, so a failed check needs a person, and exits 3.
Drift between two months of one dataset is expected, so `profile diff` exits 1, as `diff` does.

- **A check or a diff that could not finish exits 3**, from a refused S3 listing to a crash. It
  needs a person too, and 1 then means drift and nothing else.
- **No answer from S3 exits 4**, because the next run can succeed with nobody acting.
- **A missing file, an unreadable profile or a bad thresholds file exits 2.** So does DATA holding
  `_manifest.partial.json`, because the write there stopped or is still running.

## The report's shape does not change when a scenario applies

The check report holds `scenario`, null without one, and `expectations`, empty without one.
`verified` covers the expectations and is true when none are declared, while `passed` covers every
metric and column change. A scenario fills those fields, so a script reading the report reads the
same shape either way. The JSON Schemas are derived from the core crate's `profile_comparison`
model, and `doppeldata schemas show check` and `schemas show diff` print them.

*Rejected:* one report model for both verbs. A diff has two profiles and no data, so `profile` and
`data` would each mean something else there.
