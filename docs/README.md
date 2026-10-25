# doppeldata docs

Why doppeldata is shaped the way it is. A page states its design as requirements the code must meet,
and says what was weighed and rejected.

| Page | Holds |
| --- | --- |
| [python-profiler-rust-generator.md](python-profiler-rust-generator.md) | why profiling stays in Python while generation is compiled Rust, and the comparison that split them |
| [profile-format-and-privacy-floor.md](profile-format-and-privacy-floor.md) | why a profile is YAML validated by a schema derived from the Rust model, how a profile and a bundle are hashed, what generated files record of their profile, and why no stored statistic rests on fewer than `k_min` rows |
| [profiler-statistics-and-readers.md](profiler-statistics-and-readers.md) | how the profiler computes a table's statistics through one catalog, why Polars computes and DuckDB only reads, why S3 lists through boto3, how a numeric column is binned and fitted to a family, how an earlier profile freezes the edges, and how every literal is held to `k_min` rows |
| [determinism-contract.md](determinism-contract.md) | what identifies a generated dataset, and why its values never change with thread count, batch size, column order or platform |
| [column-samplers.md](column-samplers.md) | how each column kind is drawn from its profile, why a numeric tail comes from the fitted family, and how a string column holds its distinct ratio |
| [file-writers-and-manifest.md](file-writers-and-manifest.md) | how blocks become Parquet, Arrow, CSV and JSON files on any object store, how a run claims its destination, what the manifest records to regenerate them, what was rejected, and how fast it writes |
| [prior-art-and-licenses.md](prior-art-and-licenses.md) | which existing generators and checkers doppeldata borrows ideas from, and why none of them is a dependency |
| [aws-glue-support.md](aws-glue-support.md) | which AWS Glue versions run the library, and why Glue 4.0 does not |

Each page's headings state its findings, so a page's `##` lines are its contents.
