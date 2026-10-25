# How the profiler computes a table's statistics

## One catalog names every statistic, and each backend translates it

`doppeldata.profiling.statistic_catalog` lists each statistic a profile records: its name and the
column kinds it applies to. It imports no engine. A backend holds one translation per name, and
refuses to load when a name has none. So a statistic added to the catalog must be translated by
every backend before any of them runs, and two backends' profiles of one table hold the same fields.

The `Backend` protocol is six calls:

- `scan` reads a source into the engine's table;
- `schema` names each column's kind and Arrow type;
- `aggregate` answers a list of requests in one pass;
- `top_k` counts the most frequent values or shapes;
- `sample` takes a seeded uniform sample;
- `parse` reads a text column as a type.

A profile is two passes and a sample. The first pass is one aggregate over every column. The second
counts values only in string columns holding at most 120 distinct values, the
`DEFAULT_CARDINALITY_THRESHOLD` of Deequ's column profiler, and each string column's shapes. The
sample feeds grex and the date-layout trial.

## Polars computes every statistic, and DuckDB only reads text

A second statistics backend would translate the catalog twice and need a test pinning the two
engines' agreement. The Spark backend is already the catalog's second translation. So DuckDB reads
CSV, TSV and JSON with every column as text, and hands Polars the result. A profile records
`generated.engine: polars` whichever reader ran.

DuckDB runs with extension installing and autoloading off. Its wheel links the CSV and JSON readers
in, so nothing downloads at run time. Its `httpfs` extension would download itself at first use, so
DuckDB reads only local files. Parquet and Arrow on S3 are read by Polars' own object store.

*Rejected:* DuckDB's `SUMMARIZE` as a statistics backend, for the reason above.

## S3 is listed through boto3, because profile credentials need it anyway

Polars reads an `s3://` prefix but cannot list one, and the profiler needs the listing for the
files' format and for `source_fingerprint`. Neither Polars nor obstore reads `AWS_PROFILE` or an SSO
profile without boto3 installed. With boto3 installed, Polars resolves the profile and its endpoint
itself. So boto3 lists the prefix, and the profile credentials a workstation uses work unchanged.

*Rejected:* obstore for the listing. It needs boto3 for profile credentials too, which would make it
a second S3 client doing one job.

A file or directory whose name starts with `.` or `_` is skipped, such as Spark's `_SUCCESS`. A
`key=value` directory is read as a Hive partition, so its key becomes a column.

## The library declares no engine, so a job that only generates installs none

polars, duckdb, pyarrow and boto3 are imported only by the modules that profile. `profile()` checks
for all four before it imports one, and raises `ProfilingEnginesMissing` naming every missing
package. `doppeldata-cli` declares them, because `profile create` is one of its verbs.

## A text column is read as the type most of its values parse as

Each non-null raw value is classified as the first of these it parses as: integer, decimal,
boolean, date, timestamp. A value parsing as none of them is a string. The shares become
`type_mix`, and the largest one is the column's type. A value of another type is null in the parsed
column, and the generator prints it back as text from `type_mix`. In a decimal column an integer is
a decimal, so it counts as one.

Dates and timestamps are tried against fixed strftime layouts, the ones qsv's date parser
recognizes, on a sample of 10,000 rows. The layout that reads the most sampled values is
`temporal.format`. Where two layouts read every value, as `%m/%d/%Y` and `%d/%m/%Y` do when no day
exceeds 12, the month-first one wins.

The null rate comes from the raw text, so an unparsed value counts as a value rather than a null.

## No literal rests on fewer than k_min rows

The default `k_min` is 11, from the privacy floor in `profile-format-and-privacy-floor.md`. The
profiler applies it to every literal it writes:

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
  but `\d` appears. Every literal left is then shared by all the values grex saw. The dominant shape
  must also hold at least half the values, because the generator prefers a pattern to the shapes.

A gapless counting column is written as a `sequence`, whose `start` is the column's minimum. The
trimmed extreme and the step already give that value away, so storing it leaks nothing more.

## Polars counts distinct values exactly

`distinct.approx` holds an exact count on the Polars backend. Polars' `approx_n_unique` on the
streaming engine strayed by several percent on large columns, and the exact count fits in the
profile's time. The Spark backend estimates the same field.

## A profile's numbers are rounded so that profiling again gives the same digest

Rates keep 4 significant digits, and means and standard deviations keep 6. Skew and kurtosis are
also rounded to 6 decimal places. A parallel sum's order varies between runs, and it moves a zero
skew in its last bits. Quantiles and extremes are data values, written as read.
