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

table = sd.generate(sd.Profile.load('orders.profile.yaml'), rows=1_000_000, seed=7)
```

The table crosses through the Arrow PyCapsule interface, so Polars, DuckDB and pyarrow 14 or later
read it with no copy, and none of them is needed to generate. `sd.stream()` returns a
RecordBatchReader for data larger than memory. `sd.write()` writes files with a manifest, and
`sd.plan()` says what it would write. A write that stops raises a `StoppedWrite`.

The library installs no profiling engine, so a job that only generates carries none.
`doppeldata.profile()` needs polars, duckdb, pyarrow and boto3 installed beside it, and names any
that are missing. With scipy installed too, it fits a distribution family to each numeric column
with enough distinct values. Without it, each such fit is written as null, and `profile()` issues
a `FitsSkipped` warning. Every warning `profile()` issues is a `ProfilingWarning`. A write to S3
needs boto3.

`profile(..., packs=['acme'])` types columns with a third party's detector pack beside the general
one. A pack registers under the `doppeldata.detector_packs` entry-point group, and implements the
`DetectorPack` protocol in `doppeldata.profiling.detector_packs`.

Every release is published to that package index, and nothing else is. PyPI does not carry
doppeldata, so a package of that name there is someone else's. The library needs Python 3.11 or
later, so it installs into an AWS Glue job:
<https://github.com/datapointchris/doppeldata#run-the-library-inside-an-aws-glue-job>.

Source, documentation and issues: <https://github.com/datapointchris/doppeldata>.
