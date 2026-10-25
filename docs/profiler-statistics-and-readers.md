# How the profiler computes a table's statistics

## One catalog names every statistic, and each backend translates it

`doppeldata.profiling.statistic_catalog` lists each statistic a profile records: its name and the
column kinds it applies to. It imports no engine. A backend holds one translation per name, and
refuses to load when a name has none. So a statistic added to the catalog must be translated by
every backend before any of them runs, and two backends' profiles of one table hold the same fields.
The raw text types with their order, and the time units with their lengths, are tables in the
catalog too, so a backend and the profile builder read one list.

The `Backend` protocol splits a profile into the queries an engine answers natively. One `aggregate`
answers every request it is given in one query. Counting the most frequent values, sampling and
parsing text each need a query of their own. `doppeldata.profiling.table_profile` drives the calls,
so a backend holds no profiling logic.

A profile is one aggregate over every column, then a second that counts rows against the histogram
edges and value lists the first one chose, then the frequency counts, then samples. Values are
counted only in string columns holding at most `TOP_VALUES_THRESHOLD` distinct values, the
`DEFAULT_CARDINALITY_THRESHOLD` of Deequ's column profiler. Every string column's shapes are counted.
One sample feeds grex and the date-layout trial, and another the distribution fits.

## A column's quantiles and bin edges come from one sort

`quantiles` takes a tuple of levels and answers every one, so a numeric column's named quantiles
and its 31 interior bin edges are one request. Polars sorts once for a list of levels, and once per
level for separate requests. On a 10M-row Parquet column of doubles, nine separate quantiles took
0.47 s, and forty levels as one list took 0.07 s.

`histogram_counts` takes the edges and answers with the rows below the first, in each bin, and
above the last. Polars sorts the column once and binary-searches each edge into it, in 0.12 s on the
same column. `bin_intervals` with `value_counts` took 0.31 s, and one comparison per bin took 0.6 s.

*Rejected:* Polars' `hist`. It took 0.09 s, but it is marked unstable, runs in memory only, and
closes each bin on the right where Spark's `Bucketizer` and numpy close it on the left.

## A histogram is equal-depth, and counted over every row

Equal-depth bins carry a skewed column's shape in fewer bins than equal-width ones. A column has
`HISTOGRAM_BINS` of them before the floor merges any. The outer edges are the trimmed extremes, and
each interior edge is the quantile at a multiple of one over `HISTOGRAM_BINS`. A repeated edge is
dropped, so a run of tied values sits in one bin.

Each bin holds the rows from its lower edge up to the next, and the last one holds its upper edge
too. The counts come from every row the profile describes, never from the fitting sample, because
the `k_min` floor is a promise about real rows. Rows below or above the trimmed extremes number fewer
than `k_min`, so they join the outermost bins.

## Each numeric column is fitted to the families the generator draws a tail from

The candidates are the scipy.stats names `FittedFamily::from_fit` in the core crate reads: `norm`,
`lognorm`, `expon`, `weibull_min`, `pareto`, `uniform`, `logistic`, `loguniform` and `triang`, and on
an integral column `geom` and `randint` too. Any other family leaves the column a linear tail, so
`gamma`, `beta`, `poisson` and the rest are not candidates.

- **A fit is made on a seeded sample of `FIT_SAMPLE_ROWS`.** scipy fitted seven families to 200k
  values in 11 s, and its time grows with the values.
- **A column is fitted only with `FIT_MIN_DISTINCT` distinct values or more.** Fewer are better
  described by their frequencies. A counting sequence is never fitted, because the generator draws
  nothing for it.
- **The lowest AIC wins, and its KS statistic is stored beside it.** A fit with KS above 0.1 is kept
  and written `poor: true`, so a scenario reshapes its tail only when told `allow_poor_fit`.
- **An integral column ranks every family by the mass it gives each integer.** A discrete family's
  mass is its own. A continuous family's is its probability over the unit interval centered on the
  integer, which is what the generator's rounding gives it. So AIC compares masses with masses, and
  the KS statistic compares the step functions at each integer.
- **A zero-inflated column is fitted on the rest.** Where `zero_rate` is at least 0.05, the zeros are
  left out of the fit, and `zero_inflation` records their share of the fitting sample. `zero_rate`
  stays the share over every row, and the generator's run at zero comes from it.
- **loguniform's `loc` is searched alone, with `scale` fixed at 1.** `scale` only rescales `a` and
  `b`. For a given `loc`, the likeliest `a` and `b` are the least and greatest values less `loc`.
  scipy's generic fit searches all three at once. On 20k draws of `exp(uniform(0, 5))` it stopped at
  a KS statistic of 0.082, and the search on `loc` alone reached 0.0075.

On a 10M-row Parquet column of `exp(uniform(0, 5))`, on 16 threads, fitting added 0.87 s to the
profile: 1.69 s with it and 0.82 s without, each the median of five runs. scipy's fits took 0.35 s, drawing the sample
0.03 s, and importing scipy the rest. The histogram's count took 0.13 s in both.

scipy is imported only when a column is fitted. Without it, each column fitting applies to is
written `fit: null`, and `profile()` issues one `FitsSkipped` warning, which `profile create` prints
as one line. scipy 1.18 needs Python 3.12, so a Glue 5 job on 3.11 resolves 1.17. It fits the same
families, and a fit is profile content the determinism contract does not pin.

## An earlier profile freezes the edges, or stands unchanged

`profile(..., edges_from=earlier)` takes an earlier profile of the same table, and `profile create
--edges-from FILE` reads one.

- **Where the source is unchanged, the earlier profile is returned as it is.** That holds when its
  `source_fingerprint` matches the listing, and its engine, `k_min` and sampling match the run. So a
  reviewer's edits survive a re-run. A profile of every row stands for a sampled run too. No cache
  directory is written, because the library runs inside Glue executors.
- **Otherwise each column is counted against the earlier histogram edges and value list.** Values
  outside the edges are written as `histogram.overflow`, `below` and `above`.
- **`string.overflow.new_values` is the growth of `other_rate` since the list was first frozen.** It
  is the rows `other_rate` holds now, less the share it held then: the earlier profile's
  `other_rate`, less that profile's own `new_values`. So an unchanged column writes about 0, and
  drift is reported against the first profile however long the chain of re-profiles. On 200k rows
  with 5% already outside the list, an unchanged copy wrote 0. Giving 10% of rows a new value wrote
  9.5% of rows, because the 0.5% that were already outside added nothing, and so did a re-profile
  chained from that one.
- **`new_values` is counted from `other_rate`'s own rows**, so it discloses nothing that rate does
  not. A listed value now held by fewer than `k_min` rows folds into `other_rate`, and counts as
  growth with the rest. Counting only the rows outside the list would give that value's count away,
  as `other_rate` less `new_values`. The earlier profile holds no value outside its list, so a value
  never seen before cannot be told from a rare one that grew.
- **Two profiles of one table then compare bin for bin**, which the round-trip check and drift
  detection read. Rebinning stays explicit, by profiling again without `edges_from`.

The generator places each edge at its share of every row, the overflow included, so a frozen
profile generates the body it describes.

## Polars computes every statistic, and DuckDB only reads text

A second statistics backend would translate the catalog twice and need a test pinning the two
engines' agreement. So DuckDB reads CSV, TSV and JSON with every column as text, and hands Polars
the result. A profile records `generated.engine: polars` whichever reader ran.

DuckDB runs with extension installing and autoloading off. Its wheel links the CSV and JSON readers
in, so nothing downloads at run time. Its `httpfs` extension would download itself at first use, so
DuckDB reads only local files. Parquet and Arrow on S3 are read by Polars' own object store.

*Rejected:* DuckDB's `SUMMARIZE` as a statistics backend, for the reason above.

*Rejected:* Polars' own CSV and JSON readers. They read clean text as well, but DuckDB's CSV reader
also sniffs a dialect and records each malformed row in a rejects table, which Polars has no
counterpart for.

## S3 is listed through boto3, because profile credentials need it anyway

Polars reads an `s3://` prefix but cannot list one, and the profiler needs the listing for the
files' format and for `source_fingerprint`. Neither Polars nor obstore reads `AWS_PROFILE` or an SSO
profile without boto3 installed. With boto3 installed, Polars resolves the profile and its endpoint
itself. So boto3 lists the prefix, and the profile credentials a workstation uses work unchanged.

*Rejected:* obstore for the listing. It needs boto3 for profile credentials too, which would make it
a second S3 client doing one job.

A listing makes `S3_ATTEMPTS` attempts, as the AWS CLI's standard mode does. A failure names the
endpoint and the setting that chose it. `profile create` exits 4 when no answer came, and 1 when S3
answered and refused. A key with a format's suffix that holds no object is listed as a prefix,
because Spark writes a table as a `name.parquet/` directory.

A file or directory whose name starts with `.` or `_` is skipped, such as Spark's `_SUCCESS`. A
`key=value` directory below the root that was named is read as a Hive partition, so its key becomes
a column. One above it, such as the `run=7/` holding a file named on its own, is not.

## A source that changes while it is profiled fails the profile

A profile reads its source in several queries. After the last one, the files are listed again. A
file whose size or revision changed, or one that appeared or went away, fails the profile, and
`profile create` exits 1. A revision is the modification time on disk, or the S3 ETag.

`source_fingerprint` hashes the same listing. It tells two listings apart, and never hashes the
content. It is left out of the profile's digest, so a copy of the data profiles to the same
identity.

## The library declares no engine, so a job that only generates installs none

polars, duckdb, pyarrow and boto3 are imported only by the modules that profile. `ENGINES` names each
engine's packages, and `LISTING_PACKAGES` names boto3, which listing S3 needs whatever the engine.
`profile()` checks for every one of them before it imports one, and raises
`ProfilingEnginesMissing` naming each missing package. `doppeldata-cli` declares them, because
`profile create` is one of its verbs.

## A text column is read as the type most of its values parse as

Each non-null raw value is classified as the first of `RAW_TYPES` it parses as, in one chain, so the
counts are disjoint. A value parsing as none of them is a string. The shares become `type_mix`, and
the largest one is the column's type. A value of another type is null in the parsed column, and the
generator prints it back as text from `type_mix`. In a decimal column an integer is a decimal, so it
counts as one.

A digit run with a leading zero, such as a ZIP code, parses as no number. A column holding one is
read as text whatever its other values parse as, because padding means the digits are a code. A run
of digits too long for a 64-bit integer is a string too.

Dates and timestamps are tried against fixed strftime layouts, drawn from qsv's date parser, on a
sample of `LAYOUT_TRIAL_ROWS` rows. The layout that reads the most sampled values is
`temporal.format`. Where two layouts read every value, as `%m/%d/%Y` and `%d/%m/%Y` do when no day
exceeds 12, the month-first one wins.

The null rate comes from the raw text, so an unparsed value counts as a value rather than a null.

## A value a profile cannot hold is written as the nearest one it can

A float column's NaN and infinities count as nulls, because canonical JSON writes neither. An
integer extreme or quantile beyond ±2^53 is written as the nearest double. A sequence whose start or
step lies beyond it is not written.

## No literal rests on fewer than k_min rows

The default `k_min` is `DEFAULT_K_MIN`, from the privacy floor in
`profile-format-and-privacy-floor.md`. The profiler applies it to every literal it writes:

- **A column with fewer than `k_min` values** keeps its type, null rate and distinct count, and
  nothing else. The mean of one value is that value.
- **Extremes** are the values at rank `k_min` from each end, as `method: trimmed_k`. That holds for
  numeric, temporal and string-length extremes. They are omitted when fewer than `2 · k_min - 1`
  values would let the two cross.
- **A quantile** closer than `k_min` rows to either end is dropped, because it is an extreme under
  another name. The samplers then use the trimmed extremes as the inverse CDF's end knots.
- **A top value** is kept only when `k_min` rows hold it. While the remainder holds 1 to `k_min - 1`
  rows, the smallest kept value folds into it, so `other_rate` never reveals a small cell.
- **A histogram bin** of 1 to `k_min - 1` rows merges into its smaller non-empty neighbor. An empty
  bin discloses no row, so it stays, and frozen edges are kept wherever the floor allows. So a
  re-profile's edges are a subset of the frozen ones, and a diff sums the earlier bins over a merged
  one.
- **An overflow count** of 1 to `k_min - 1` rows is written as 0. A numeric column's rows join the
  outermost bin on their side, and a string column's stay in `other_rate`.
- **A fit** is made only on the values between the trimmed extremes. A support parameter lands on
  the sample's own extreme, as loguniform's `b`, uniform's `scale` and randint's `high` do, so no
  value fewer than `k_min` rows bound reaches one.
- **A shape** is kept only when `k_min` rows hold it.
- **A month, weekday or hour bin** of 1 to `k_min - 1` rows is emptied, and the rest are rescaled.
- **A pattern** is generalized by grex from at least `k_min` sampled values of the dominant shape. It
  is stored only when no literal sits in an alternation or optional part, and no character class
  but `\d` appears. Every literal left is then shared by all the values grex saw. grex gets a seeded
  draw of the distinct values, because the first ones in order would share one prefix. The dominant
  shape must also hold at least half the values, because the generator prefers a pattern to the
  shapes.

A gapless counting column is written as a `sequence`, whose `start` is its first value: the minimum
when it counts up, and the maximum when it counts down. The trimmed extreme at that end and the step
already give that value away, so storing it leaks nothing more.

## A sampled profile counts distinct values in every row

`--sample` profiles a seeded sample, and `dataset.row_count` stays the whole table's count. The
generator multiplies `distinct.ratio` by the rows it writes, and a distinct ratio does not scale from
a sample. So a sampled profile counts each column's distinct values in one pass over every row, and
takes the ratio over the whole table.

## Distinct values are counted exactly

`distinct.approx` holds an exact count on the Polars backend, so `distinct.ratio` does not move with
the engine or its version. A backend that estimates the count writes it to the same field.

## A profile's numbers are rounded so that profiling again gives the same digest

Rates keep `RATE_DIGITS` significant digits, and means and standard deviations keep
`MOMENT_DIGITS`. Skew and kurtosis are also rounded to `SHAPE_MOMENT_DECIMALS` decimal places. A
parallel sum's order varies between runs, and it moves a zero skew in its last bits. Quantiles and
extremes are data values, written as read.
