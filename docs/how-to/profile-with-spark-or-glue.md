# Profile with Spark or AWS Glue

`--engine spark`, or `engine='spark'` from Python, computes the profile in Spark. A table on a
cluster is profiled where it sits, and only the statistics reach the driver. The profile is the one
Polars writes, with distinct counts and quantiles estimated.

```bash
doppeldata profile create data/orders/ --engine spark --out orders.spark.profile.yaml
SPARK_REMOTE=sc://localhost:15002 doppeldata profile create data/orders/ --engine spark --out orders.spark.profile.yaml
```

```python
# profile_in_spark.py
from pyspark.sql import SparkSession

import doppeldata as sd

spark = SparkSession.builder.getOrCreate()

profile = sd.profile('data/orders/', engine='spark', spark=spark)
profile.save('orders.spark.profile.yaml')

frame = spark.read.parquet('data/orders/').where("status != 'cancelled'")
shipped = sd.profile(frame, engine='spark')
shipped.save('shipped.spark.profile.yaml')
print(shipped.document['dataset']['rows'], [column['name'] for column in shipped.document['columns']])
```

```text
17974 ['order_id', 'customer_id', 'amount', 'quantity', 'status', 'ordered_at', 'customer_email', 'ship_zip']
```

- **With no `SPARK_REMOTE`, the CLI starts a session on `local[*]`.** From Python, `spark=` names
  the session that reads files. Left out, the active session reads them, else one the builder
  starts.
- **A DataFrame is profiled in its own session**, so a filter or a join the job already ran is
  profiled as it stands.
- **Spark reads Parquet, CSV and TSV, locally or from S3.** JSON and Arrow are refused before a
  session starts:

  ```text
  doppeldata: E-DATA-007: engine spark does not read ndjson; profile it with engine polars
    valid:
      polars
  ```

- **`--aws-profile` is refused under `--engine spark`**, exit 2. Spark reads S3 with its session's
  own credentials:

  ```text
  doppeldata: E-USAGE-024: spark reads S3 with its session's own credentials, so an AWS profile would reach the listing alone
  ```

Every flag `profile create` takes applies here too. [Profile a dataset](profile-a-dataset.md) lists
them.

## Spark's profile agrees with the one Polars writes

```text
$ doppeldata profile create data/orders/ --out orders.profile.yaml
$ doppeldata profile create data/orders/ --engine spark --out orders.spark.profile.yaml
$ doppeldata profile diff orders.profile.yaml orders.spark.profile.yaml
doppeldata: 3 metrics unmeasured and passed, because orders.spark.profile.yaml holds nothing to compare them with; a later profile made against orders.profile.yaml's edges carries the overflow boundary_adherence reads
# doppeldata profiles agree

`orders.spark.profile.yaml` (sha256:…) against `orders.profile.yaml` (sha256:…).

0 of 31 metrics failed.
```

The two SHA-256s differ. The hash keeps `generated.engine`, and Spark estimates the distinct counts
Polars counts. The Python script above and the CLI over Spark Connect wrote profiles of these files
with the same SHA-256.

## Install pyspark beside the CLI, or beside the library

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ doppeldata-cli --with pyspark==4.1.3
uv tool install --index https://pypi.ichrisbirch.com/simple/ doppeldata-cli --with 'pyspark[connect]==4.1.3'
```

```bash
uv pip install --index https://pypi.ichrisbirch.com/simple/ doppeldata pyspark==4.1.3 pyarrow scipy
uv pip install --index https://pypi.ichrisbirch.com/simple/ doppeldata 'pyspark[connect]==4.1.3' pyarrow scipy
```

- **A local session needs the Java its Spark release names**, which pyspark starts its JVM with.
- **Over Spark Connect, `pyspark[connect]` matches the server's Spark version.**
- **scipy fits each numeric column's distribution family.** Without it, each `fit` is written as
  null and the profile warns `FitsSkipped`.
- **The driver lists an `s3://` prefix through boto3.** The CLI declares it. From Python, install
  it beside the library to profile S3.
- **A CSV or TSV is sniffed on the driver by DuckDB and CleverCSV.** The CLI declares both. From
  Python, add `duckdb clevercsv charset-normalizer`.

## Over Spark Connect, the client builds the profile and the executors need the wheel alone

```bash
export SPARK_REMOTE=sc://localhost:15002
doppeldata profile create data/orders/ --engine spark --out orders.spark.profile.yaml
```

```python
spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()
sd.profile('data/orders/', engine='spark', spark=spark).save('orders.spark.profile.yaml')
```

The client lists the source, sniffs a CSV's dialect and builds the profile. The session computes
every statistic.

- **The executors need the client's doppeldata and its Python minor version.** A mismatch is
  refused before any read:

  ```text
  doppeldata.errors.SourceError: E-DATA-012: the Spark executors cannot run the profiling passes: pyspark.errors.exceptions.base.PySparkRuntimeError: [PYTHON_VERSION_MISMATCH] Python in worker has different version: 3.13 than that in driver: 3.11, PySpark cannot run with different minor versions.
  ```

- **The executors need the wheel alone**, beside the pyarrow and pandas PySpark's `mapInArrow`
  needs. scipy, boto3 and the CSV packages are the client's.
- **The CLI profiles only over a server whose executors run its own Python.** A session on an older
  Python is profiled from the library on that Python. [AWS Glue support](../aws-glue-support.md)
  gives each Glue's.
- **The server reads a local path itself**, so the files must be at that path on the server too.
  On a cluster, name an `s3://` prefix.

A Glue 5.1 Spark Connect session also needs a stream setting and its first query run with
reattachable execution off.
[profiling-with-spark.md](../profiling-with-spark.md#a-glue-51-spark-connect-session-profiles-once-each-result-stream-ends-within-10-seconds)
gives the session that profiled from one.

## A Glue job profiles a prefix and stores the YAML beside the data

The job's script:

```python
# profile_table.py
import sys

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

import doppeldata as sd

args = getResolvedOptions(sys.argv, ['source', 'out_bucket', 'out_key'])
spark = SparkSession.builder.getOrCreate()
profile = sd.profile(args['source'], engine='spark', spark=spark)
boto3.client('s3').put_object(Bucket=args['out_bucket'], Key=args['out_key'], Body=profile.to_yaml().encode())
```

Upload the script and the library's wheel, then create and run the job. [Install the wheel on
Glue](../../README.md#run-the-library-inside-an-aws-glue-job) downloads the wheel.

```bash
aws s3 cp profile_table.py s3://your-bucket/glue/
aws glue create-job --name profile-orders --role your-glue-role --glue-version 5.1 \
  --worker-type G.1X --number-of-workers 2 \
  --command Name=glueetl,ScriptLocation=s3://your-bucket/glue/profile_table.py,PythonVersion=3 \
  --default-arguments '{"--additional-python-modules": "s3://your-bucket/glue/doppeldata-<version>-cp311-abi3-manylinux2014_x86_64.manylinux_2_17_x86_64.whl,scipy"}'
aws glue start-job-run --job-name profile-orders \
  --arguments '{"--source": "s3://your-bucket/orders/", "--out_bucket": "your-bucket", "--out_key": "profiles/orders.profile.yaml"}'
```

```text
{
    "Name": "profile-orders"
}
{
    "JobRunId": "jr_37112e04de986ac44a470409a2b0bd515ba513924e21add020731642ec3ef1d4"
}
```

Read the stored profile back and check it like any other:

```text
$ aws s3 cp s3://your-bucket/profiles/orders.profile.yaml .
$ doppeldata profile check orders.profile.yaml
# doppeldata profile check passed

`orders.profile.yaml`, 1 table.

## orders

25 literal fields under policy `default`, `k_min` 11, and 0 issues.
```

Its `generated.readers` names the job's Spark, `pyspark: 3.5.4-amzn-0`, with `pyarrow: 17.0.0`.
`profile diff` against a Polars profile of the same prefix printed `0 of 31 metrics failed`.

`--additional-python-modules` names the wheel, then what the source needs:

| Source | Glue job | Glue Spark Connect session |
| --- | --- | --- |
| Parquet | the wheel, `scipy` | the wheel |
| CSV or TSV | the wheel, `scipy`, `duckdb`, `clevercsv`, `charset-normalizer` | the wheel |

- **The executors import the library for the `mapInArrow` passes**, so the wheel is named in every
  case.
- **The table names only what no supported Glue preinstalls.**
  [AWS Glue support](../aws-glue-support.md#a-glue-job-adds-only-the-wheel-because-glue-installs-the-pandas-and-pyarrow-doppeldataspark-needs)
  lists what each one does. A job without scipy fits no column.
- **Over Spark Connect the client builds the profile**, so the client holds scipy and the CSV
  packages.
- **Pin `duckdb` and `clevercsv` to the versions the release's `uv.lock` resolves.** A profile
  records each reader's version in `generated.readers`, and one read with other versions is never
  reused as one read with these.

## Why

[Profiling with Spark](../profiling-with-spark.md) says which pass computes each statistic and why,
why a sample does not move with the cluster, and how close Spark's profile lands to Polars' on each
Spark image and on Glue 5.1.
