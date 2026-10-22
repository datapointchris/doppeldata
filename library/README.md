# doppeldata

Profile a dataset into a reviewable file holding no rows, then generate synthetic data from it at any
scale.

This is the library. The `doppeldata` command is the separate `doppeldata-cli` distribution, so a
project importing the library installs none of the CLI's dependencies.

```bash
uv add --index https://pypi.ichrisbirch.com/simple/ doppeldata
```

Every release is published to that package index, and nothing else is. PyPI does not carry
doppeldata, so a package of that name there is someone else's. The library needs Python 3.11 or
later, so it installs into an AWS Glue job:
<https://github.com/datapointchris/doppeldata#run-the-library-inside-an-aws-glue-job>.

Source, documentation and issues: <https://github.com/datapointchris/doppeldata>.
