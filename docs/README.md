# doppeldata docs

Why doppeldata is shaped the way it is. A page states its design as requirements the code must meet,
and says what was weighed and rejected.

| Page | Holds |
| --- | --- |
| [python-profiler-rust-generator.md](python-profiler-rust-generator.md) | why profiling stays in Python while generation is compiled Rust, and the comparison that split them |
| [profile-format-and-privacy-floor.md](profile-format-and-privacy-floor.md) | why a profile is YAML validated by a schema derived from the Rust model, how a profile and a bundle are hashed, what generated files record of their profile, and why no stored statistic rests on fewer than `k_min` rows |
| [profiler-statistics-and-readers.md](profiler-statistics-and-readers.md) | how the profiler computes a table's statistics through one catalog, why Polars computes and DuckDB only reads, why S3 lists through boto3, how a numeric column is binned and fitted to a family, how an earlier profile freezes the edges, and how every literal is held to `k_min` rows |
| [semantic-types-and-detector-packs.md](semantic-types-and-detector-packs.md) | how a column is given a semantic type by validators and name hints, why an integer column needs a hint, how each generator hint draws values its validator passes, and how a third party registers a detector pack |
| [determinism-contract.md](determinism-contract.md) | what identifies a generated dataset, and why its values never change with thread count, batch size, column order or platform |
| [column-samplers.md](column-samplers.md) | how each column kind is drawn from its profile, why a numeric tail comes from the fitted family, and how a string column holds its distinct ratio |
| [file-writers-and-manifest.md](file-writers-and-manifest.md) | how blocks become Parquet, Arrow, CSV and JSON files on any object store, how a run claims its destination, what the manifest records to regenerate them, and what was rejected |
| [generating-from-python-and-the-cli.md](generating-from-python-and-the-cli.md) | how a generated table reaches Python with no Arrow dependency, how Ctrl-C and SIGTERM stop the engine, why S3 credentials come from boto3, what `data plan` prints, which exit status each kind of stopped write takes, and how close the size estimate lands |
| [generating-inside-spark.md](generating-inside-spark.md) | how `doppeldata.spark` gives a local run's values at any partition count and its files from the executors, which Arrow types Spark refuses, what a Glue Spark Connect session needs, how executors get S3 credentials, what crossing into pyarrow copies, and the check that runs both calls inside each named Spark image and compares them with a local run |
| [performance.md](performance.md) | the speed the engine must reach, how fast it generates and writes against numpy with pyarrow, how fast its name provider runs against Faker and Mimesis, what a codec or a partition key costs a write, and how fast `doppeldata.spark` writes a billion rows on Glue 5.1 |
| [prior-art-and-licenses.md](prior-art-and-licenses.md) | which existing generators and checkers doppeldata borrows ideas from, and why none of them is a dependency |
| [aws-glue-support.md](aws-glue-support.md) | which AWS Glue versions run the library, and why Glue 4.0 does not |

Each page's headings state its findings, so a page's `##` lines are its contents.
