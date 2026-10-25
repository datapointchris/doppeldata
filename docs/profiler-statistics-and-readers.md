# How the profiler computes a table's statistics

## One catalog names every statistic, and each backend translates it

`doppeldata.profiling.statistic_catalog` lists each statistic a profile records: its name and the
column kinds it applies to. It imports no engine. A backend holds one translation per name, and
refuses to load when a name has none. So a statistic added to the catalog must be translated by
every backend before any of them runs, and two backends' profiles of one table hold the same fields.
The raw text types with their order, and the time units with their lengths, are tables in the
catalog too, so a backend and the profile builder read one list.

The `Backend` protocol splits a profile into the queries an engine answers natively. One `aggregate`
answers every request it is given: Polars in one query, and Spark in one query per kind of pass, as
[profiling-with-spark.md](profiling-with-spark.md) says. Counting the most frequent values, sampling
and parsing text each need a query of their own. `doppeldata.profiling.table_profile` drives the calls,
so a backend holds no profiling logic.

A profile is one aggregate over every column, then a second that counts rows against the histogram
edges and value lists the first one chose, then the frequency counts, then samples. Values are
counted in string columns holding at most `TOP_VALUES_THRESHOLD` distinct values, the
`DEFAULT_CARDINALITY_THRESHOLD` of Deequ's column profiler. Every string column's shapes are counted.

A numeric column's values are counted where it holds at most `NUMERIC_TOP_VALUES_THRESHOLD`
distinct values, and is not a `sequence`. `VALUE_LISTS` in the statistic catalog names each kind
that lists its values, the section holding the list and its cut. A column whose privacy treatment
refuses top values lists none, whatever its kind, so no zero rate, negative rate or histogram bin
gives way to a list the profile would then withhold. Under every policy but `none`, that is each
identifier. It is also each quasi-identifier under `strict`, and each free-text, ZIP or age column
under the policy that treats it so.

The numeric cut sits between a code and a measurement. Location ids, rate codes and payment types
hold a few hundred values at most, and drawn from quantiles they land on the wrong codes. A fare,
tip or toll in cents holds thousands, and its quantiles carry it. On the NYC yellow taxi trips for
February to April 2024, generated at 10M rows:

| Numeric cut | Profile lines | Failed `data check` metrics | Location ids' value distance |
| --- | --- | --- | --- |
| 0, no numeric column listed | 476 | 2 of 91 | 0.43 and 0.39 |
| `TOP_VALUES_THRESHOLD` | 568 | 0 of 100 | 0.43 and 0.39 |
| `NUMERIC_TOP_VALUES_THRESHOLD` | 1,079 | 0 of 102 | 0.001 and 0.001 |

Each row sets the numeric entries of `VALUE_LISTS` to its cut, then runs:

```bash
doppeldata profile create yellow_tripdata_2024-02.parquet yellow_tripdata_2024-03.parquet \
  yellow_tripdata_2024-04.parquet --out taxi.profile.yaml
doppeldata data generate taxi.profile.yaml --rows 10000000 --out taxi-generated/
doppeldata data check taxi.profile.yaml taxi-generated/
```

The value distance is the total variation distance between the source's values and the generated
ones, each value's share counted by Polars. `PULocationID` and `DOLocationID` hold 262 and 261
values. The next column up, `tolls_amount`, holds 1,938.

A float column's `scale` is the fewest decimal places every value equals itself rounded to, up to
`DECIMAL_PLACES_TRIED`. A column needing more is left without one and generates unrounded.
Polars' `round(places)` compares exactly. *Rejected:* `(value · 10^places).round() / 10^places`.
Polars rewrites a division by a literal as a multiplication by its reciprocal, which turns
164 / 100 into 1.6400000000000001.
One sample of the parsed table feeds grex and the semantic detectors, and another the distribution
fits. The date-layout trial samples the raw text before it is parsed.

## A column's quantiles and bin edges come from one sort

`quantiles` takes a tuple of levels and answers every one, so a numeric column's named quantiles
and its interior bin edges are one request. Polars sorts once for a list of levels, and once per
level for separate requests. So one list of levels is several times faster than one request per
level.

`histogram_counts` takes the edges and answers with the rows below the first, in each bin, and
above the last. Polars sorts the column once and binary-searches each edge into it. That is faster
than `bin_intervals` with `value_counts`, and than one comparison per bin.

*Rejected:* Polars' `hist`. It is a little faster, but it is marked unstable, runs in memory only,
and closes each bin on the right where Spark's `Bucketizer` and numpy close it on the left.

## A histogram is equal-depth, and counted over every row

Equal-depth bins carry a skewed column's shape in fewer bins than equal-width ones. A column has
`HISTOGRAM_BINS` of them before the floor merges any. The outer edges are the trimmed extremes, and
each interior edge is the quantile at a multiple of one over `HISTOGRAM_BINS`. A repeated edge is
dropped, so a run of tied values sits in one bin.

Each bin holds the rows from its lower edge up to the next, and the last one holds its upper edge
too. The counts come from every row the profile describes, never from the fitting sample, because
the `k_min` floor is a promise about real rows. Rows below or above the trimmed extremes number fewer
than `k_min`, so they join the outermost bins.

A bin is floored on what the profile states beside it too. A reader can subtract a bin's listed
values, its zero rows and its rows below zero from its count, so each cell left must hold 0 rows or
at least `k_min`.

- **A bin leaving a smaller cell merges into its smaller neighbor**, as a bin under `k_min` rows
  does.
- **Where one bin still leaves one, `negative_rate` is left out and the bins merged again.**
  It is one rate, and the bins are several counts. A listed column whose few unlisted negative
  values fall under `k_min` keeps its bins this way.
- **Where that fails too, the histogram is left out.** With no histogram, a cell the stated counts
  leave across the whole column drops `negative_rate`, then `zero_rate`.
- **The arithmetic is exact.** Rates keep `RATE_DIGITS` significant digits, which hide a few rows
  only at large counts, so the floor never rests on that rounding.
- **`zero_rate` and `negative_rate` under `k_min` rows are written as 0.**

An integer column's edges sit half a unit below the least integer of each bin, and its last edge
half a unit above the trimmed high. An interior quantile equal to the trimmed high is a boundary
too, so the high value gets its own bin. The generator rounds a drawn value to the nearest
integer, so each bin's share is then drawn to the integers it counted. *Rejected:* edges on the
integers themselves. Rounding then drew half of each edge value's share as the integer above it, and
a count column with 18% zeros generated 11%.

## Each numeric column is fitted to the families the generator draws a tail from

The candidates are the families `_core.fitted_families()` names. That is the core crate's
`FITTED_FAMILIES`, the table `FittedFamily::from_fit` looks each fit up in, so the profiler fits
exactly what the generator reads. A discrete family is a candidate only on an integral column. Any
other family, such as `gamma`, leaves the column a linear tail, so it is not a candidate.

- **A fit is made on a seeded sample of `FIT_SAMPLE_ROWS`.** scipy's fit time grows with the
  values, so fitting runs on a sample.
- **A column is fitted only with `FIT_MIN_DISTINCT` distinct values or more.** Fewer are better
  described by their frequencies. A counting sequence is never fitted, because the generator draws
  nothing for it.
- **The lowest AIC wins, and its KS statistic is stored beside it.** A fit with a KS statistic
  above `POOR_FIT_KS` is kept and written `poor: true`, so a reader can tell a loose fit from a
  close one. `profile()` issues one `PoorFits` warning naming each such column, and each column no
  family fits.
- **An integral column ranks every family by the mass it gives each integer.** A discrete family's
  mass is its own. A continuous family's is its probability over the unit interval centered on the
  integer, which is what the generator's rounding gives it. So AIC compares masses with masses, and
  the KS statistic compares the step functions at each integer.
- **A zero-inflated column is fitted on the rest.** Where `zero_rate` is at least
  `ZERO_INFLATION_RATE`, the zeros are left out of the fit. `zero_inflation` records their share of
  the values the fit saw, which is the mixture's weight. `zero_rate` stays the share of every
  non-null value, and the generator's run at zero comes from it.
- **loguniform's `loc` is searched alone, with `scale` fixed at 1.** `scale` only rescales `a` and
  `b`. For a given `loc`, the likeliest `a` and `b` are the least and greatest values less `loc`.
  scipy's generic fit searches all three at once. On 20k draws of `exp(uniform(0, 5))` it stopped at
  a KS statistic of 0.082, and the search on `loc` alone reached 0.0075.

Most of what fitting adds to a profile is importing scipy, and the fits come next. The histogram's
count costs the same with fitting or without.

Every profile imports scipy, so that `generated.fitter` records the version that fitted it. A scipy
that is missing, or installed and failing to import, costs only the fits. Each column fitting
applies to is then written `fit: null`, `generated.fitter` is left out, and `profile()` issues one
`FitsSkipped` warning carrying the import's error. `profile create` prints every
`ProfilingWarning` as one line on stderr. scipy's newest release can need a later Python than the
library's floor, so a Glue job on 3.11 resolves an older scipy, which fits the same families.

A fit is profile content, so it is in the profile's digest. Two scipy versions that fit different
parameters write two profiles, and those generate different tails.

## An earlier profile freezes the edges, or stands unchanged

`profile(..., edges_from=earlier)` takes an earlier profile of the same table, and `profile create
--edges-from FILE` reads one.

- **Where nothing it was made from has changed, the earlier profile is returned as it is.** Each
  `generated` field `GENERATED_FIELDS` in `doppeldata.profile_format` marks `derived_from`, such as
  `source_fingerprint` and `tool`, must equal the one this run would write. Its `k_min` and sampling
  must match the run's too. So a reviewer's edits survive a re-run, while a new doppeldata, a scipy
  installed since, or a detector pack enabled or upgraded, profiles again. A profile of every row
  stands for a sampled run too. The fingerprint hashes each file's resolved path, so the same files
  match from any working directory. `profile create` says on stderr when it returns the earlier
  profile. Otherwise a `ProfilingWarning` names the first thing that differs, which `profile create`
  prints. No cache directory is written, because the library runs inside Glue executors.
- **Otherwise each column is counted against the earlier histogram edges and value list.** Values
  outside the edges are written as `histogram.overflow`, `below` and `above`. A column only one of
  the two tables holds issues one `ColumnsNotFrozen` warning naming it. So does a column whose list
  would move to another section, such as a code read as integers before and as strings now. It is
  profiled afresh.
- **`overflow.new_value_rows` is the growth of `other_rate` since the list was first frozen**, in
  `string.overflow` or `numeric.overflow`. It is the rows `other_rate` holds now, less the share it
  held then: the earlier profile's `other_rate`, less that profile's own `new_value_rows`. So an
  unchanged column writes about 0, and drift is reported against the first profile however long
  the chain of re-profiles.
  With 5% of rows already outside the list, giving 10% of rows a new value writes 9.5% of rows: the
  0.5% already outside add nothing.
- **`new_value_rows` is counted from `other_rate`'s own rows**, so it discloses nothing that rate
  does not. A listed value now held by fewer than `k_min` rows folds into `other_rate`, and counts as
  growth with the rest. Counting only the rows outside the list would give that value's count away,
  as `other_rate` less the count. The earlier profile holds no value outside its list, so a value
  never seen before cannot be told from a rare one that grew.
- **A numeric column needs its own `new_value_rows` beside the histogram's overflow.** A new value
  between the outer edges lands in a bin beside the listed values, and the bins cannot tell it from
  their growth. Re-profiling `[1, 2, 3, 4]` as `[1.5, 2, 3, 4]` writes `other_rate` 0.25 and
  `new_value_rows` 500 of 2,000 rows, and `profile diff` reports it.
- **Two profiles of one table then compare bin for bin**, which the round-trip check and drift
  detection read. Rebinning stays explicit, by profiling again without `edges_from`.

The generator places each edge at its share of every row, the overflow included, so a frozen
profile generates the body it describes. Reading `overflow` raised the stream version to 3.

## Polars or Spark computes every statistic, and DuckDB only reads or sniffs text

Polars computes a profile by default. Spark computes it under `--engine spark` or
`profile(engine='spark')`, for data on a cluster. Each translates the whole catalog, and
`spark-parity/check-profiler-parity.sh` holds Spark's profile to Polars'.

- **Under Polars, DuckDB reads CSV, TSV and JSON with every column as text**, and hands Polars the
  result.
- **Under Spark, Spark's own reader reads CSV and TSV with every column as text**, in the dialect
  DuckDB and CleverCSV sniff from the first block. Spark profiles no JSON or Arrow file, and names
  engine polars for them.
- **pyarrow reads Parquet footers**, for the layout alone.
- **`generated.engine` names the backend that computed the statistics**, whichever reader ran.
  `generated.readers` names each package that read or sniffed the files, with its version, because a
  DuckDB or CleverCSV upgrade can change what a profile of the same files says.

DuckDB runs with extension installing and autoloading off. Its wheel links the CSV and JSON readers
in, so nothing downloads at run time. Its `httpfs` extension would download itself at first use, so
DuckDB reads only local files. Parquet and Arrow on S3 are read by Polars' own object store. So
Polars profiles text from local files only. Spark reads text on S3 itself, and boto3 reads the first
block for the sniff, which DuckDB reads from a local copy.

*Rejected:* DuckDB's `SUMMARIZE` as a statistics backend. A backend translates the whole catalog and
is held to the others by a parity check. DuckDB reaches no data Polars cannot, so it would pay that
cost for nothing.

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
directory or prefix holding `_manifest.partial.json` is refused, because a doppeldata write there
stopped or is still running. A `key=value` directory below the root that was named is read as a Hive partition, so its key becomes
a column. One above it, such as the `run=7/` holding a file named on its own, is not.

## A source that changes while it is profiled fails the profile

A profile reads its source in several queries. After the last one, the files are listed again. A
file whose size or revision changed, or one that appeared or went away, fails the profile, and
`profile create` exits 1. A revision is the modification time on disk, or the S3 ETag.

`source_fingerprint` hashes the same listing. It tells two listings apart, and never hashes the
content. It is left out of the profile's digest, so a copy of the data profiles to the same
identity.

## The library declares no engine, so a job that only generates installs none

The profiling packages are imported only by the modules that profile, and boto3 also by a write to
S3. `ENGINES` in `doppeldata.profiling` names each engine's packages, each with the formats whose
files it reads or sniffs, and whether a source of any format needs it. CleverCSV, for one, is
needed only for CSV and TSV. `LISTING_PACKAGES` names the ones listing S3 needs whatever the engine.
`profile()` checks those any source needs before it imports one, and a format's own once the source
is listed. Either check raises `ProfilingEnginesMissing` naming each missing distribution, and a
command it stops exits 2.
`generated.readers` records the version of each package that reads the source's format, from the
same table. pyspark's is the session's Spark version, because the session reads the files. A Spark
Connect client's pyspark can differ from it, and so can one a launcher leaves in site-packages, as
the Glue 5.0 image's 3.5.2 sits beside its Spark 3.5.4. `doppeldata-cli` declares every one but
pyspark, because `profile create` is one of its verbs. pyspark comes with the Spark install it runs
in, whose version is the cluster's, so a missing pyspark is refused with that said.

## A text column is read as the type most of its values parse as

Each non-null raw value is classified as the first of `RAW_TYPES` it parses as, in one chain, so the
counts are disjoint. A value parsing as none of them is a string. The shares become `type_mix`. The
type that reads the most values is the column's, and a decimal column reads every integer too, so
decimal's count includes them. A value of another type is null in the parsed column, and the
generator prints it back as text from `type_mix`. In a decimal column the integers' share folds into
decimal's.

A digit run with a leading zero, such as a ZIP code, parses as no number. A column holding one is
read as text whatever its other values parse as, because padding means the digits are a code. A run
of digits too long for a 64-bit integer is a string too.

Dates and timestamps are tried against fixed strftime layouts, drawn from qsv's date parser, on a
sample of `LAYOUT_TRIAL_ROWS` rows. The layout that reads the most sampled values is
`temporal.format`. Where two layouts read every value, as `%m/%d/%Y` and `%d/%m/%Y` do when no day
exceeds 12, the month-first one wins.

The null rate comes from the raw text, so an unparsed value counts as a value rather than a null.

A delimited source's null tokens are null before any column is typed. Each column's empty fields and
its fields holding one of `NULL_TOKENS` are counted. The token the file writes most across all its
columns becomes `csv.null`. Each column's other tokens become its `missing_tokens`, each a share of
all rows, and one is kept only where `k_min` rows hold it.

A JSON value is typed by its JSON type rather than its text. `json_type` makes a number an integer or
a decimal and a boolean a boolean, and a JSON string is tried only as a date or a timestamp. So `"3"`
is a string in JSON, where in CSV it is an integer. A Hive key read from the path is text, and is
typed as a CSV field is.

## A delimited file's dialect is DuckDB's sniff, or CleverCSV's where the two disagree

The dialect comes from the first `FIRST_BLOCK_BYTES` of the first file holding a record,
decompressed. A block cut mid-record is trimmed at its last line break. A source none of whose
files holds a record is refused with a `SourceError` naming them.

- **A block that decodes as UTF-8 is read as UTF-8, and any other as Latin-1.** A UTF-8 BOM is
  recorded as `bom` and stripped. charset-normalizer's name for the block is recorded as
  `detected_encoding`, and decides nothing, because it names a short Latin-1 block `cp1250` or
  `cp775`.
- **DuckDB's `sniff_csv` and CleverCSV each name a delimiter, a quote and an escape.** CleverCSV is
  held to `GLUE_DELIMITERS`, the ones Glue's CSV classifier tries, as the AWS Glue Developer Guide's
  "Defining and managing classifiers" lists them. DuckDB's dialect is read where the two agree on
  the delimiter and the quote, or where CleverCSV finds none. Otherwise CleverCSV's is, and where
  neither names one, `chosen` is `default` and the format's own is read.
- **A quote neither sniffer names is read as `"`.** A block quoting no field names none, and a
  later file of the source may quote one. Reading `"` keeps that file's quoted delimiters inside
  their fields, where reading no quote splits them. An escape neither names doubles the quote.
- **`detector` records the two sniffers, whether they agreed, and which was read.** Its `sniffed`
  holds what each named, so a quote neither named shows as null there. Its `confidence` is the share
  of the block's records splitting into the most common number of fields.
- **Every other file's first record is checked against the dialect**, up to `FILES_CHECKED` of
  them, a sample seeded by `FILES_CHECKED_SEED` beyond that. Where there is a header, each file must
  start with the same one. Where there is none, its first record must split into as many fields. A
  file that does not is named in a `SourceError`, because the files are read as one table.
- **DuckDB misreads a file whose rows disagree.** On a pipe-delimited file holding a quoted comma, a
  stray quote, a short row and a long one, DuckDB 2.0.0 names `;` and one column. CleverCSV 0.8.5
  names `|` with no quote. `cli/tests/test_profile_file_traits.py` profiles that file.
- **The header is decided as Glue's CSV classifier decides it**, by the rules "Defining and managing
  classifiers" gives. That needs two columns and two data rows. Every name in the first record must
  be text, neither a number nor a boolean. Each but the last must be shorter than
  `GLUE_HEADER_FIELD_LENGTH`. Every one must match `GLUE_COLUMN_NAME`, the "Single-line string
  pattern" of the AWS Glue API's "Common data types". Some later record must hold a field that is
  not text. Where Glue would classify no CSV, DuckDB's sniffed header is read. So is it where every
  field is text, because Glue then reads none, and an all-text table's names would become a row.
- **`quoting` is read from the block's fields.** It is `always` where every field is quoted, and
  `non_numeric` where exactly the fields that are not numbers are. Otherwise it is `necessary`,
  including where no field is quoted, because a writer that never quotes writes that block alike.
- **`line_terminator_mix` gives each terminator's share of the block's line breaks.** The commoner
  one is `line_terminator`. `trailing_delimiter` is true where every record ends in an empty field.

*Rejected:* DuckDB's sniffer alone. It is the reader, so its dialect is right on clean files, but
on the file above it reads every row as one column and rejects nothing.

## A malformed row is counted by DuckDB's first error, and a value that does not parse by Polars

DuckDB reads every column as text, in the detected dialect, with `store_rejects`. A row it rejects
is left out of the table, and counted once, under its first error by position.

- **`cast` counts rows holding a value that does not parse as its column's type.** Polars counts it
  as `unparsed_rows`, because a read with every column as text casts nothing.
- **Each type's rate is a share of the data rows**, those read and those rejected. A type fewer
  than `k_min` rows give is left out, and `malformed.rate` is the sum of the rest.
- **A sampled profile's rates are still shares of every row read.** DuckDB rejects rows over the
  whole read, before the sample is drawn. `cast` is counted in the sample, so its share of the
  sample is scaled to the rows read.
- **The profiler reads in DuckDB's strict mode, which rejects a stray quote as `unquoted_value`.**
  With quote `"`, the field `"bad "quote"` gives `UNQUOTED VALUE` in DuckDB 2.0.0, where
  `strict_mode = false` gives `MISSING COLUMNS`. `read_delimited` passes `strict_mode = true`.
- **Spark gives no error, so it classifies a malformed row by its field count.** Spark's reader
  runs in `PERMISSIVE` mode and keeps each row it cannot split into the header's fields as text.
  That row splits on the delimiter into fewer fields than the header's for `missing_columns`, more
  for `too_many_columns`, and as many for `unquoted_value`. The split ignores quotes, so a quoted
  delimiter can move a row between the first two.

## A Parquet footer gives the layout, and never a value

A footer's min and max are single rows' values, so no column statistic comes from one. Every
statistic is taken from the rows, under the floor below.

- **At most `FOOTERS_READ` footers are read**, a sample seeded by `FOOTER_SEED` beyond that, on
  `FOOTER_THREADS` threads.
- **An S3 footer is read through the boto3 client that lists the source.** One ranged read of the
  last `TAIL_BYTES` holds most footers, and a longer one takes a second. So the listing's
  timeouts, retries and region redirects apply, and a failure exits as a listing's does, with 4
  where no answer came.
- **`source.parquet` holds the settings that reproduce the files**: the commonest codec, the
  largest row group as `row_group_rows`, page or chunk `statistics`, and `dictionary_columns`. The
  last is null where every column carries a dictionary.
- **It records the writer beside them**: `writer` from `created_by`, `spark_version` from Spark's
  `org.apache.spark.version` key, `format_version`, and quantiles of row groups per file, rows per
  group and bytes per group.
- **`timestamp_units` gives each timestamp column's stored unit.** `timestamp_unit` is set only where
  they agree and none is INT96. The writers cannot write INT96.
- **`decimal_scale_drift` is true where one decimal column's scale differs between files.**
- **Files holding different schemas are read as the union of their footers.** The reader gives
  Polars the union as its scan schema, so no file's own schema decides the table's. A column is
  read at the widest type a footer stores it as, and is null in a file that lacks it. A
  schema-drifted dataset, generated or not, then profiles as one table.
- **A sample of footers holding two schemas sends the reader to every footer.** A third schema
  outside the sample would otherwise lose its columns.
- **A column two files store as an integer and a string fails the read**, naming the column.
  Neither type widens to the other.

## A dataset's layout is its files, their rows, its Hive keys and its sort order

- **`files` gives the count and the sizes** from the listing. `rows_per_file` gives quantiles of each
  file's rows, from its footer or from the rows read.
- **A Hive key stays a column.** The writers partition by a column, so a key the profile drops could
  not be written again. Its type is the column's.
- **A key's `values.shares` is each value's share of rows**, kept where `k_min` rows hold it, as a
  top value is. `other_rate` holds the rest. They are written where the key takes no more than
  `TOP_VALUES_THRESHOLD` values.
- **`sort_order` is the sorting columns every footer declares.** Without them, it is the first
  integer column whose values never fall in read order.

## A directory whose folders hold unlike schemas is profiled as a bundle, as a Glue crawler splits one

A Glue crawler compares the schemas of the folders under a prefix. Where they match "and the
partition threshold is higher than 70 percent", the folders are partitions of one table. Otherwise
"the crawler creates a table for every folder", each named after its folder
(<https://repost.aws/knowledge-center/glue-crawler-multiple-tables>,
<https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html>). `profiling/table_grouping.py`
groups a directory or prefix the same way, with one difference below. It runs before an engine is
chosen, so Polars and Spark group alike.

- **Each folder directly under the root is a candidate table.** The files directly under the root
  are one more, named after the root. The listing resolves links, so a linked file whose target
  lies outside the root joins the root's own files.
- **A folder's schema is the union of up to `FILES_READ` of its files' schemas.** A Parquet or
  Arrow file gives (name, type) pairs. A CSV or TSV gives its header's names, or `col0`, `col1` and
  on where Glue would find no header. A JSON file gives the keys of its first `RECORDS_READ` records.
- **Two folders are one table where their schemas share more than `SIMILAR` of their pairs**, the
  crawler's 70 percent. Each folder joins the first cluster whose first folder it matches, in name
  order. A schema that could not be read matches any.
- **One cluster is one table, its `key=value` folders Hive partitions.**
- **Several clusters are a bundle of one table per cluster**, named after the cluster's first
  folder. This is the difference from the crawler, which makes a table of every folder. Two kinds
  of table under one prefix, each written as many folders, become two tables rather than one per
  folder.
- **More than `MOST_CLUSTERS` clusters make each folder its own table**, as the crawler does. That
  many kinds of table under one prefix says the folders share no structure worth keeping.
- **`profile()` returns a `Bundle` for several tables, and `profile create --out` writes one file**
  of `kind: bundle` holding them inline. Two tables taking one name fail the profile.
- **`edges_from` refuses a source that groups into several tables**, because an earlier profile's
  edges belong to one table. Each table's folder is profiled on its own instead.
- **Several paths named are one table**, as a crawler's include path is.

*Not established:* how the crawler measures a match. Neither page defines it, so the share of
(name, type) pairs is this design's reading of "70 percent".

## A JSON source is profiled path by path

DuckDB's `json_group_structure` merges every record's structure into one tree. Each scalar path is
read as text beside its `json_type`, and each array path as its length. An array's elements are a
table of their own, profiled as a table is.

- **A file is NDJSON where every whole line of its first block holds one JSON value**, whatever its
  suffix. `source.format` then says `ndjson`.
- **Any other `.json` file is read with DuckDB's `auto` format, unless it opens with `[`.** That
  reads objects concatenated with no delimiter, as Kinesis Data Firehose writes them unless its
  newline delimiter is turned on, and pretty-printed ones.

- **A field's `presence` is the share of its parent's objects holding its key.** Three records with
  an `addr` object, one holding `apt`, give `addr.apt` 0.333. The generator draws a field only where
  its parent is an object, so a share of all rows would shrink twice.
- **A nested field's `null_rate` is over the objects holding its key.** A field any object lacks is
  `nullable`, because Arrow holds it as null.
- **A nested field's `type_mix` is recorded as the files hold it.** `data generate` refuses it with
  `NotGeneratedYet`, because a text cell names a top-level row and has nowhere to sit inside a struct
  or a list.
- **`source.json` records `max_depth` and `explicit_nulls`**, which is true where any path holds a
  JSON null.
- **An array's element keeps no literal under a checked policy.** Its values are counted by element,
  so one record's eleven tags would pass a floor of rows. Its length and distinct count stay. A
  struct field's literals are held to the rows of its parent's objects holding its key.

## A value a profile cannot hold is written as the nearest one it can

A float column's NaN and infinities count as nulls, because canonical JSON writes neither. An
integer extreme or quantile beyond ±2^53 is written as the nearest double. A sequence whose start or
step lies beyond it is not written.

## No literal rests on fewer than k_min rows

The default `k_min` is `DEFAULT_K_MIN`, from the privacy floor in
`profile-format-and-privacy-floor.md`. The statistics pass applies it to every literal it computes,
and the policy's pass then removes what each column's rule withholds:

- **A column with fewer than `k_min` values** keeps its type, null rate and distinct count, and
  nothing else. The mean of one value is that value.
- **Extremes** are the values at rank `k_min` from each end, as `method: trimmed_k`. That holds for
  numeric, temporal and string-length extremes. They are omitted when fewer than `2 · k_min - 1`
  values would let the two cross.
- **A quantile** closer than `k_min` rows to either end is dropped, because it is an extreme under
  another name. The samplers then use the trimmed extremes as the inverse CDF's end knots.
- **A top value** is kept only when `k_min` rows hold it. While the remainder holds 1 to `k_min - 1`
  rows, the smallest kept value folds into it, so `other_rate` never reveals a small cell. That
  holds for a numeric column's list as for a string's.
- **A histogram bin** of 1 to `k_min - 1` rows merges into its smaller non-empty neighbor. An empty
  bin discloses no row, so it stays, and frozen edges are kept wherever the floor allows. So a
  re-profile's edges are a subset of the frozen ones, and a diff sums the earlier bins over a merged
  one.
- **An overflow count** of 1 to `k_min - 1` rows is written as 0. A numeric column's rows join the
  outermost bin on their side, and a string column's stay in `other_rate`.
- **The floor holds within one profile, not across two.** A value one profile lists and the next
  folds into `other_rate` shows its count as that rate's growth, where nothing else changed. A bin
  merged under the floor shows the same way against the earlier bins. Holding a series of profiles to
  the floor needs noise, which this profiler does not add.
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

## Distinct values are counted exactly by Polars, and estimated within about 1% by Spark

`distinct.approx` holds an exact count on the Polars backend, so `distinct.ratio` does not move with
its version. Spark estimates it with HyperLogLog++ at a relative standard deviation of 0.01, and
writes the estimate to the same field. A count of a few hundred falls in HyperLogLog++'s linear
counting, and lands within a few values of exact.
[profiling-with-spark.md](profiling-with-spark.md) gives the errors measured against Polars.

## A profile's numbers are rounded so that profiling again gives the same digest

Rates keep `RATE_DIGITS` significant digits, and means and standard deviations keep
`MOMENT_DIGITS`. Skew and kurtosis are also rounded to `SHAPE_MOMENT_DECIMALS` decimal places. A
parallel sum's order varies between runs, and it moves a zero skew in its last bits. Quantiles and
extremes are data values, written as read.
