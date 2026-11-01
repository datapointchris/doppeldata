# doppeldata-cli

The `doppeldata` command: profile a dataset into a reviewable file holding no rows, then generate
synthetic data from it at any scale.

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ doppeldata-cli
doppeldata --version     # prints the installed release
```

Every release is published to that package index, and nothing else is. The command needs Python
3.13 or later. It pins the `doppeldata` library to its own release, so the two always match.

It also installs every engine the library leaves optional. So `profile create` reads Parquet and
Arrow locally or on S3, and CSV, TSV and JSON locally, with nothing more installed. Two features
need one package more, added with `--with`:

- `--engine spark` needs `pyspark`;
- `data check --html` needs `sweetviz`.

The README walks up from column names to a profile of real data, and links a page for every task:
<https://github.com/datapointchris/doppeldata#readme>. Source and issues live there too.
