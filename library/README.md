# doppeldata

Profile a dataset into a reviewable file holding no rows, then generate synthetic data from it at any
scale.

This is the library. The `doppeldata` command is the separate `doppeldata-cli` distribution, so a
project importing the library installs none of the CLI's dependencies.

```bash
uv add --index https://pypi.ichrisbirch.com/simple/ doppeldata
```

```python
import doppeldata as sd

table = sd.generate(['name', 'email', 'city', 'zip'], rows=1000)   # each column takes the fake its name hints
table = sd.generate({'age': sd.integers(18, 90), 'plan': sd.choice(['free', 'pro'])}, rows=1000)
table = sd.generate(sd.Profile.load('orders.profile.yaml'), rows=1_000_000, seed=7)
```

The table crosses through the Arrow PyCapsule interface, so Polars, DuckDB and pyarrow 14 or later
read it with no copy, and none of them is needed to generate. `sd.stream()` returns a
RecordBatchReader for data larger than memory. `sd.write()` writes a directory of files with a
manifest, or one file such as `people.csv`, and
`sd.plan()` says what it would write. A write that stops raises a `StoppedWrite`.
`sd.spark.generate()` and `sd.spark.write()` do the same inside Spark's executors, with the values
and files a local run gives. They need PySpark 3.5 or later, with pyarrow 15 or later and the pandas
PySpark asks for, and doppeldata installed on every executor.

The library installs no profiling engine, so a job that only generates carries none.
`doppeldata.profile()` needs polars, duckdb, pyarrow and boto3 installed beside it, and names any
that are missing. `profile(data, engine='spark')` computes the profile in Spark instead: of a Spark
DataFrame in its own session, or of files in the session `spark=` names, else the active one. It
needs pyspark, pyarrow and boto3, with duckdb, clevercsv and charset-normalizer to sniff a CSV or
TSV. It estimates distinct counts with a 1% standard error, and quantiles within a rank error of
1/10,000. A directory whose folders hold unlike schemas profiles to a `Bundle` of one table per
kind of folder, as an AWS Glue crawler splits one. With scipy installed too, it fits a distribution
family to each numeric column with enough distinct values. Without it, each such fit is written as null, and `profile()` issues
a `FitsSkipped` warning. Every warning `profile()` issues is a `ProfilingWarning`. A write to S3
needs boto3.

`profile(..., packs=['acme'])` types columns with a third party's detector pack beside the general
one. A pack registers under the `doppeldata.detector_packs` entry-point group, and is a
`DetectorPack` from `doppeldata.profiling.detector_packs`. A pack is code the profiler runs on the
sampled values, so enable only one trusted with the data.

Every release is published to that package index, and nothing else is. PyPI does not carry
doppeldata, so a package of that name there is someone else's. The library needs Python 3.11 or
later, so it installs into an AWS Glue job:
<https://github.com/datapointchris/doppeldata#run-the-library-inside-an-aws-glue-job>.

Source, documentation and issues: <https://github.com/datapointchris/doppeldata>.
