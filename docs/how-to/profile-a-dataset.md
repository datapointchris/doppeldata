# Profile a dataset

`profile create` reads a table and writes a profile: a YAML file of its types, distributions, null
and error rates, the dependence between its columns, and how its files were written. It holds no
rows.

```bash
doppeldata profile create data/orders/ --out orders.profile.yaml
doppeldata profile create data/orders.csv --out orders.profile.yaml
doppeldata profile create s3://your-bucket/orders/ --aws-profile analytics --out orders.profile.yaml
doppeldata profile create s3://your-bucket/orders/ --aws-profile analytics --sample 5000 --out sampled.profile.yaml
```

```python
import doppeldata as sd

profile = sd.profile('data/orders/')
profile = sd.profile('s3://your-bucket/orders/', aws_profile='analytics', sample=5000)
profile.save('orders.profile.yaml')
```

Without `--out` the profile prints as YAML, or as JSON under `--json`. `--out` ending in `.json`
writes JSON.

## What `profile create` reads

| Source | Read by |
| --- | --- |
| Parquet and Arrow files, local or on S3 | Polars, with pyarrow reading each Parquet footer for the layout |
| CSV and TSV files, local | DuckDB, in the dialect DuckDB and CleverCSV sniff |
| JSON and NDJSON files, local | DuckDB, path by path |
| Parquet, CSV and TSV on S3, or a Spark DataFrame | Spark, under `--engine spark`, as [Profile with Spark or AWS Glue](profile-with-spark-or-glue.md) shows |

- **Each file is read as its suffix's format**, and every file named must share one. Two formats
  are refused before anything is read:

  ```text
  doppeldata: E-DATA-004: shop/ holds files of 2 formats, ndjson, parquet; profile one format at a time
  ```

- **Several files, directories or prefixes named together are one table.** A directory is read
  whole, and its `key=value` folders are Hive partitions whose keys become columns. A file or
  folder whose name starts with `.` or `_`, such as `_SUCCESS`, is skipped.
- **A directory whose folders hold unlike column names is refused**, naming each table it holds.
  `--tables` profiles them into a bundle, as
  [Generate related tables](generate-related-tables.md) shows.
- **S3 is reached with the AWS profile `--aws-profile` names**, or `aws_profile=`. Left out,
  `AWS_PROFILE` names it, then boto3's default chain.
- **A source that changes while it is read fails the profile, exit 1.** The files are listed again
  after the last read, and any file whose size or revision moved is named.

## `--help` prints every flag, and a page shows each one that changes the profile

`doppeldata profile create --help` prints each flag with its default, and `help(sd.profile)` each
argument. The flags that change the profile each have a page:

- `--policy` and `--k-min`, `policy=` and `k_min=`:
  [Review and commit a profile](review-and-commit-a-profile.md)
- `--pack`, `packs=`: [Write a detector pack](write-a-detector-pack.md)
- `--code-list`, `code_lists=`: [Profile healthcare data](profile-healthcare-data.md)
- `--edges-from`, `edges_from=`: [Measure drift between two profiles](measure-drift-between-profiles.md)
- `--tables`, `sd.profile_tables`: [Generate related tables](generate-related-tables.md)
- `--engine spark`, `engine='spark'`: [Profile with Spark or AWS Glue](profile-with-spark-or-glue.md)

`--sample N`, `sample=`, profiles a seeded random sample of N rows. Distinct counts, repeated rows
and keys still count every row. A sampled profile records how many rows it read:

```yaml
  sample: {strategy: reservoir, rows_read: 5000}
```

## `-v` prints each step, and a condition worth acting on prints as a warning

```text
$ doppeldata profile create data/orders/ --out orders.profile.yaml -v
doppeldata: data/orders/: 1 parquet file, 580,996 bytes
doppeldata: orders: profiled 20,000 rows and 8 columns with polars in 2.20 s
```

`profile create` prints each `ProfilingWarning` as one line on stderr, such as `PoorFits` naming
the columns no distribution family fits, or `FitsSkipped` where scipy is not installed. `-q` prints
nothing but a refusal or a failure. In Python they are warnings, so `warnings.simplefilter('error',
sd.ProfilingWarning)` turns each into an error, and the steps go to the `doppeldata` logger.

The library installs no profiling engine. `sd.profile` names each package a source needs and
which are missing, and the [README's install section](../../README.md#install) lists them.

## Why

[How the profiler computes a table's statistics](../profiler-statistics-and-readers.md) says what
each field is and how it is measured, how a directory is grouped into tables, and how a CSV's
dialect and malformed rows are found. [Semantic types and detector packs](../semantic-types-and-detector-packs.md)
says how a column is typed, and [The profile format and its privacy floor](../profile-format-and-privacy-floor.md)
why no stored value rests on fewer than `k_min` rows.
