# doppeldata-cli

The `doppeldata` command: profile a dataset into a reviewable file holding no rows, then generate
synthetic data from it at any scale.

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ doppeldata-cli
doppeldata --version     # prints the installed release
```

Every release is published to that package index, and nothing else is. The command needs Python
3.13 or later. It pins the `doppeldata` library to its own release, so the two always match.

Source, documentation and issues: <https://github.com/datapointchris/doppeldata>.
