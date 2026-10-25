# AWS Glue support

## Glue 5.0, 5.1 and 6.0 run the library, and Glue 4.0 does not

| Glue | Spark | Python | Preinstalled pyarrow | Supported | `doppeldata.spark` checked | `profile(engine='spark')` checked |
| --- | --- | --- | --- | --- | --- | --- |
| 6.0 | 4.1.1 | 3.13 | 23.0.1 | yes | on Apache's Spark 4.1.3 image, under Python 3.11 | on Apache's Spark 4.1.3 image, under Python 3.11 |
| 5.1 | 3.5.6 | 3.11 | 22.0.0 | yes | on Glue, at 1B rows | on Glue, as a job and over Spark Connect |
| 5.0 | 3.5.4 | 3.11 | 17.0.0 | yes | on `public.ecr.aws/glue/aws-glue-libs:5`, Glue 5.0's runtime | on `public.ecr.aws/glue/aws-glue-libs:5` |
| 4.0 | 3.3.0 | 3.10 | 10.0.0 | no | — | — |

Glue 4.0 is out for two reasons:

- its Python 3.10 sits below the library's 3.11 floor;
- its pyarrow 10 predates the Arrow PyCapsule import, which needs pyarrow 14 or later.

## A Glue job adds only the wheel, because Glue installs the pandas and pyarrow `doppeldata.spark` needs

PySpark's `mapInArrow` needs pandas and pyarrow beside Spark, and every supported Glue preinstalls
both. Glue 5.0 provides pandas 2.2.2, and Glue 5.1 and 6.0 provide 2.3.3. AWS lists each version's modules in
<https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html#glue-modules-provided>.
Glue 5.1 ran `sd.spark.generate` and `sd.spark.write` at 1B rows with
`--additional-python-modules` naming the library's wheel alone. A Glue 5.1 Spark Connect session
ran both at 10M rows with the same setting.
[generating-inside-spark.md](generating-inside-spark.md) holds what that run verified, and
[performance.md](performance.md) its throughput.

A job that profiles adds scipy, which no supported Glue preinstalls. A CSV or TSV source adds
`duckdb==2.0.0`, `clevercsv==0.8.5` and `charset-normalizer` too.
[profiling-with-spark.md](profiling-with-spark.md) gives the whole job.

## The library floors at Python 3.11 because Glue 5.0 and 5.1 run it

The library runs inside Glue executors, so its `requires-python` is set by the oldest Glue it
supports. The command line never runs on Glue, so it floors at 3.13 and ships as a separate
distribution.

## One wheel covers every supported Glue

Glue cannot compile native code in a job, so only a wheel installs. The library's wheel is built with
the stable ABI for Python 3.11 (`cp311-abi3`), so one wheel serves 3.11, 3.13 and every Python
between. It targets manylinux2014, glibc 2.17, which every supported Glue's platform accepts.

The README gives the install routes: a wheel downloaded from the package index alone and passed by
its S3 path, or a zip of wheels for a job with no internet access.
