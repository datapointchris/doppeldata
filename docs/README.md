# doppeldata docs

Why doppeldata is shaped the way it is. The code says what it does; these pages say why, and what
was weighed and rejected.

| Page | Holds |
| --- | --- |
| [python-profiler-rust-generator.md](python-profiler-rust-generator.md) | why profiling stays in Python while generation is compiled Rust, with the measurements that split them |
| [profile-format-and-privacy-floor.md](profile-format-and-privacy-floor.md) | why a profile is YAML validated by a schema derived from the Rust model, how it is hashed, and why no stored statistic rests on fewer than 11 rows |
| [determinism-contract.md](determinism-contract.md) | what identifies a generated dataset, and why its values never change with thread count, batch size or column order |
| [prior-art-and-licenses.md](prior-art-and-licenses.md) | which existing generators and checkers doppeldata borrows ideas from, and why none of them is a dependency |
| [aws-glue-support.md](aws-glue-support.md) | which AWS Glue versions run the library, and why Glue 4.0 does not |

Each page's headings state its findings, so a page's `##` lines are its contents.
