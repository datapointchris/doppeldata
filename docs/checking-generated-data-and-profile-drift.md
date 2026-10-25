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

`doppeldata.profile_comparison.metrics.METRICS` lists each metric with its name, its bound, its
default threshold and its measurement. A measurement takes both profiles and yields a value per
column, or one for the table. The reports and `--thresholds` read the registry, so a metric added
there reaches both with no other change.

| Metric | Value | Passes |
| --- | --- | --- |
| `ks` | sup of the difference between the two CDFs the generator draws a numeric, date or timestamp column from | at most 0.05 |
| `tvd` | half the summed difference in share over the source's listed values and one share for every other value | at most 0.05 |
| `null_rate_delta` | the absolute difference in null rate | at most 0.01 |
| `distinct_ratio_delta` | the difference in distinct ratio, relative to the source's | at most 0.10 |
| `boundary_adherence` | the share of values inside the source's outer histogram edges | at least 0.99 |

The names follow SDMetrics where one exists. Its `KSComplement` is 1 − `ks`, and its `TVComplement`
is 1 − `tvd`.

## `ks` reads the CDF the generator draws from

The extension's `numeric_cdf_knots` and `temporal_cdf_knots` return the knots the generator's own
samplers build from a profile. So the check and the generator read one distribution, and a
statistic the generator ignores cannot pass the check by itself.

- **A numeric column's CDF** is the inverse CDF's knots: the quantiles, the extremes, the
  histogram's edges at their shares, the flat run at `zero_rate`, and the fitted tail.
- **A date or timestamp column's CDF** weights each day by `by_month` and `by_dow` and each hour by
  `by_hour`, with the extremes cutting the first and last day. A value is uniform within its hour,
  so the CDF is linear across one. A whole-day column puts each day's share at its start.
- **`gap_rate` is left out.** Which days are empty is a seeded draw that no profile records, and
  emptying a share of days at random leaves the expected CDF where it was.
- **The supremum is exact.** Both CDFs are linear between knots, so their difference is largest at
  a knot's value or just below one. Each side is read at every knot value of either, both at the
  value and just below it, where a jump would hide.

A sequence column draws nothing, so it holds its start and step at any row count and has no `ks`.

*Rejected:* building the CDF again in Python. A second reading of the profile could drift from
the generator's, and the check would then pass data the generator did not draw.

## `tvd` measures over the source's categories

A string column's categories are its `top_values`, and a boolean's are `true` and `false`. Every
value the source does not list counts as one more category, so a value the other profile lists
and the source does not adds to that category's share. A source value missing from the other's
list counts as 0 there. A value under `k_min` rows drops off a list, so the error is under
`k_min` rows.

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

## A check fails on a changed column kind, and a diff on any changed field

A column's kind is the profiler's: integer, float, decimal, string, boolean, date, datetime or
other, read from `logical_type` and an Arrow decimal type.

- **A check fails** on a column the data adds or drops, or holds as another kind or semantic type.
- **A check lists and passes an Arrow type change alone.** The writers cast every timestamp to one
  unit, because Parquet's Arrow writer cannot write INT96. Data written as CSV reads back at the
  text reader's widths, so an `int32` source column profiles as `int64`.
- **A diff fails on every changed field.** That covers a changed Arrow type, which there means the
  source itself changed. It also covers a column with more than 5% of its values outside the edges
  or value list frozen from A, a CSV dialect setting that changed, and a partition key B adds.

## A failed check exits 3, and drift exits 1

Nothing in doppeldata changes the data it checked, so a failed check needs a person, and exits 3.
Drift between two months of one dataset is expected, so `profile diff` exits 1, as `diff` does. A
missing file, an unreadable profile or a bad thresholds file exits 2.

## The report's shape does not change when a scenario applies

The check report holds `scenario`, null without one, and `expectations`, empty without one.
`verified` covers the expectations and is true when none are declared, while `passed` covers every
metric and column change. A scenario fills those fields, so a script reading the report reads the
same shape either way. The JSON Schemas are derived from the core crate's `profile_comparison`
model, and `doppeldata schemas show check` and `schemas show diff` print them.

*Rejected:* one report model for both verbs. A diff has two profiles and no data, so `profile` and
`data` would each mean something else there.
