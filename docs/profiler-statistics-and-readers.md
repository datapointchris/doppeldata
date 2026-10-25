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

A profile is one aggregate over every column, then the frequency counts, then a sample. Values are
counted only in string columns holding at most `TOP_VALUES_THRESHOLD` distinct values, the
`DEFAULT_CARDINALITY_THRESHOLD` of Deequ's column profiler. Every string column's shapes are counted.
The sample feeds grex and the date-layout trial.

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
file whose size or revision changed, or one that appeared or went away, fails the profile with exit

1. A revision is the modification time on disk, or the S3 ETag.

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
