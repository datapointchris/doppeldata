# AWS Glue support

## Glue 5.0, 5.1 and 6.0 run the library, and Glue 4.0 does not

| Glue | Spark | Python | Preinstalled pyarrow | Supported |
| --- | --- | --- | --- | --- |
| 6.0 | 4.1.1 | 3.13 | 23.0.1 | yes |
| 5.1 | 3.5.6 | 3.11 | 22.0.0 | yes |
| 5.0 | 3.5.4 | 3.11 | 17.0.0 | yes |
| 4.0 | 3.3.0 | 3.10 | 10.0.0 | no |

Glue 4.0 is out for two reasons:

- its Python 3.10 sits below the library's 3.11 floor;
- its pyarrow 10 predates the Arrow PyCapsule import, which needs pyarrow 14 or later.

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
