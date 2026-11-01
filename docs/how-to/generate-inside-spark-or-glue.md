# Generate inside Spark or AWS Glue

`doppeldata.spark` runs the wheel inside Spark's executors, so the rows are made where the cluster
reads them and never cross the driver. `generate` returns a DataFrame for any Spark writer. `write`
writes the files `doppeldata data generate` writes, byte for byte, and `_manifest.json`.

```python
# generate_in_spark.py
from pyspark.sql import SparkSession

import doppeldata as sd

spark = SparkSession.builder.getOrCreate()
profile = sd.Profile.load('orders.profile.yaml')

frame = sd.spark.generate(spark, profile, rows=1_000_000, seed=7)
print(frame.count(), frame.rdd.getNumPartitions())
frame.groupBy('status').count().orderBy('status').show()

manifest = sd.spark.write(spark, profile, 'orders/', rows=1_000_000, seed=7)
print(manifest.dataset_sha256, len(manifest.files))
manifest = sd.spark.write(spark, profile, 's3://your-bucket/spark/orders/', rows=1_000_000, seed=7)
print(manifest.dataset_sha256, len(manifest.files))

bundle = sd.Bundle.example('orders-customers')
frames = sd.spark.generate(spark, bundle, scale=0.1, seed=7)
print({name: frame.count() for name, frame in frames.items()})
manifest = sd.spark.write(spark, bundle, 'shop/', scale=0.1, seed=7)
print(manifest.dataset_sha256)

rows = sd.sample('eight-columns', rows=50, seed=1, overrides={'category': 'A'})
frame = sd.spark.to_spark(spark, rows, schema=sd.spark.struct_type('eight-columns'))
print(frame.count(), frame.select('category').distinct().collect())
```

```text
1000000 1
+----------+------+
|    status| count|
+----------+------+
| cancelled|100941|
| delivered|462158|
|processing|140279|
|  returned| 78907|
|   shipped|217715|
+----------+------+

sha256:… 50
sha256:… 50
{'customers': 4000, 'orders': 55000}
sha256:…
50 [Row(category='A')]
```

The same arguments on one machine write the same dataset. Each of the 50 files, and each of the
bundle's, is identical to the executors':

```text
$ doppeldata data generate orders.profile.yaml --rows 1000000 --seed 7 --out orders/
wrote 1,000,000 rows as parquet in 50 files, 33.9 MB, in 0.22 s on 16 threads: 4.52M rows/s
orders/_manifest.json names dataset sha256:…
$ doppeldata data generate orders-customers --scale 0.1 --seed 7 --out shop/
customers  wrote 4,000 rows as parquet in 1 file, 48.1 KB
orders     wrote 55,000 rows as parquet in 1 file, 898.4 KB
in 0.03 s on 16 threads; shop/_manifest.json names dataset sha256:…
```

## The four calls

| Call | Returns |
| --- | --- |
| `sd.spark.generate(spark, profile, rows=, seed=)` | a DataFrame of the rows `sd.generate` gives, or a dict of one per table for a bundle |
| `sd.spark.write(spark, profile, out, rows=, seed=)` | the `Manifest` `sd.write` returns, or a `BundleManifest` |
| `sd.spark.struct_type(profile)` | the DataFrame's Spark schema, refusing a column Spark cannot hold exactly |
| `sd.spark.to_spark(spark, table, schema=)` | a DataFrame built on the driver from an Arrow table, of up to `sd.spark.DRIVER_ROWS_AT_MOST` rows |

- **`partitions=` sets how many tasks share the work, and moves no value.** `write` takes at most
  one per planned file range, and one per range by default.
- **`scenario=` takes what `sd.generate` takes.** `generate` refuses the ops that shape files,
  because a DataFrame has none, and `write` applies them. Both refuse `cdc`, which `sd.write`
  writes from one process.
- **`write` takes `sd.write`'s `format`, `file_rows`, `partition_by`, `partition_style`, `writer`
  and `replace`.** A bundle takes `scale` in place of `rows`.
- **`write`'s destination is an `s3://` prefix**, which the driver and every executor reach. A
  local directory is taken only under a `local` master.
- **The profile may be a list of column names, a dict or a short YAML**, as for `sd.write`. The
  driver expands it before it plans.

## Install the wheel on the driver and every executor

- **Every executor runs the driver's doppeldata build.** One on another release or stream version
  refuses its task.
- **PySpark's `mapInArrow` needs pyarrow and pandas** on the driver and every executor, at the
  versions PySpark asks for.
- **A write to S3 needs boto3 on every executor.** Each task resolves its own credentials through
  boto3's chain.

## A Glue job generates from a stored profile

The job's script reads a profile from S3 and writes from the executors:

```python
# generate_table.py
import sys
import tempfile
from urllib.parse import urlparse

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

import doppeldata as sd

args = getResolvedOptions(sys.argv, ['profile', 'out', 'rows', 'seed'])
spark = SparkSession.builder.getOrCreate()
stored = urlparse(args['profile'])
with tempfile.NamedTemporaryFile(suffix='.profile.yaml') as local:
    boto3.client('s3').download_fileobj(stored.netloc, stored.path.lstrip('/'), local)
    local.flush()
    profile = sd.Profile.load(local.name)
manifest = sd.spark.write(spark, profile, args['out'], rows=int(args['rows']), seed=int(args['seed']))
print(manifest.dataset_sha256, len(manifest.files))
```

The job names the wheel alone, because Glue preinstalls the rest, as
[AWS Glue support](../aws-glue-support.md#a-glue-job-adds-only-the-wheel-because-glue-installs-the-pandas-and-pyarrow-doppeldataspark-needs)
lists. [Install the wheel on Glue](../../README.md#run-the-library-inside-an-aws-glue-job)
downloads it.

```bash
aws s3 cp generate_table.py s3://your-bucket/glue/
aws glue create-job --name generate-orders --role your-glue-role --glue-version 5.1 \
  --worker-type G.1X --number-of-workers 2 \
  --command Name=glueetl,ScriptLocation=s3://your-bucket/glue/generate_table.py,PythonVersion=3 \
  --default-arguments '{"--additional-python-modules": "s3://your-bucket/glue/doppeldata-<version>-cp311-abi3-manylinux2014_x86_64.manylinux_2_17_x86_64.whl"}'
aws glue start-job-run --job-name generate-orders \
  --arguments '{"--profile": "s3://your-bucket/profiles/orders.profile.yaml", "--out": "s3://your-bucket/glue-orders/", "--rows": "1000000", "--seed": "7"}'
```

```text
{
    "Name": "generate-orders"
}
{
    "JobRunId": "jr_ef9a08e93fb1088a54805d46bef7228d5e32dcb662428f4a16ca91d7ef879164"
}
```

The job printed the dataset's SHA-256 and 200 files. A `doppeldata data generate` of the same
profile, rows and seed named the same dataset. Check what the job wrote against its profile:

```text
$ doppeldata data check orders.profile.yaml s3://your-bucket/glue-orders/
# doppeldata check passed

`s3://your-bucket/glue-orders/`, 1,000,000 rows, against profile `orders` (sha256:…).

0 of 31 metrics failed.
```

A Glue 5.1 Spark Connect session generates too, with reattachable execution off.
[Generating inside Spark executors](../generating-inside-spark.md#over-a-glue-51-spark-connect-session-both-calls-match-a-local-run-once-reattachable-execution-is-off)
says what it needed.

## Why

[Generating inside Spark executors](../generating-inside-spark.md) says how each task makes whole
blocks, why no partition count moves a value, what each refusal guards, which Arrow types Spark
refuses, and the parity check that compares each call with a local run in each Spark image.
