# Write a detector pack

A detector pack adds semantic types the profiler gives a column. A column a pack types takes the
pack's sensitivity, which the privacy policy treats it by, and the pack's generator hint, which the
generator draws from. A pack is a Python package, so no fork of doppeldata is needed.

```bash
uv pip install ./acme-detectors                       # beside the library
uv tool install --index https://pypi.ichrisbirch.com/simple/ --with ./acme-detectors doppeldata-cli  # beside the command line
doppeldata profile create orders.csv --pack acme --out orders.profile.yaml
```

```python
import doppeldata as sd

profile = sd.profile('orders.csv', packs=['acme'])
```

## A pack is a `DetectorPack` registered under the `doppeldata.detector_packs` entry point

```text
acme-detectors/
  pyproject.toml
  src/acme_detectors/__init__.py
```

```toml
# acme-detectors/pyproject.toml
[project]
name = "acme-detectors"
version = "0.1.0"
requires-python = ">=3.11"

[project.entry-points."doppeldata.detector_packs"]
acme = "acme_detectors:PACK"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

```python
# acme-detectors/src/acme_detectors/__init__.py
import pyarrow.compute as pc

from doppeldata.profiling.detector_packs import Detector
from doppeldata.profiling.detector_packs import DetectorPack

LOYALTY_CARD = r'^LC\d{10}$'

PACK = DetectorPack(
    name='acme',
    detectors=(
        Detector(
            type='acme_loyalty_card',
            sensitivity='identifier',
            name_hints=('loyalty_card', 'loyalty'),
            generator_hint={'pattern': LOYALTY_CARD},
            validate=lambda values: pc.match_substring_regex(values, LOYALTY_CARD),
            method='pattern',
        ),
    ),
)
```

- **The entry point's name is the pack's name**, which `--pack` and `packs=` enable it by.
- **`validate` takes a pyarrow string array holding no nulls**, and returns whether each value is
  of the type.
- **`help(Detector)` says what each field takes**, such as `hint_required` for a type only a
  column's name may assign, and `code_list` for one read from a list the caller supplies.

## The pack types the column, and the default policy suppresses it as an identifier

Any table holding such a column works. This one holds 5,000 rows:

```text
$ duckdb -c "copy (select i as order_id, 'LC' || lpad((hash(i) % 10000000000)::varchar, 10, '0') as loyalty_card, i % 9 + 1 as quantity from range(1, 5001) t(i)) to 'orders.csv'"
$ head -3 orders.csv
order_id,loyalty_card,quantity
1,LC9076358352,2
2,LC3917578834,3
```

Without the pack, the column has a pattern and no type, and nothing is suppressed:

```text
$ doppeldata profile create orders.csv --out plain.profile.yaml
$ yq '.columns[] | select(.name == "loyalty_card") | .semantic' plain.profile.yaml
null
$ yq '.privacy.suppressed' plain.profile.yaml
null
```

With it, the column is typed and suppressed:

```text
$ doppeldata profile create orders.csv --pack acme --out orders.profile.yaml
$ yq '.columns[] | select(.name == "loyalty_card") | .semantic' orders.profile.yaml
type: acme_loyalty_card
pack: acme
confidence: 1.0
detectors: [pattern, name_hint]
sensitivity: identifier
generator_hint: {pattern: '^LC\d{10}$'}
$ yq '.privacy.suppressed' orders.profile.yaml
- {column: loyalty_card, reason: identifier, semantic: acme_loyalty_card}
```

- **`semantic.pack` names the pack that typed the column**, so a reviewer sees whose rule it was.
- **`generated.detector_packs` records `acme: acme-detectors 0.1.0`** beside the general pack.
- **`detectors` lists `name_hint`** because `loyalty_card` holds a hint. A type with
  `hint_required` takes only such a column.

## The generator draws each value from the pack's hint

```text
$ doppeldata data generate orders.profile.yaml --rows 100000 --seed 7 --out generated/
wrote 100,000 rows as csv in 20 files, 2.1 MB, in 0.02 s on 16 threads: 4.17M rows/s
generated/_manifest.json names dataset sha256:5945424cfb8b79b0b74f0fbd647d8052ea946dbf49beeb5b42b4e44358a88a9f
$ duckdb -c "select count(*) as rows, count(*) filter (where regexp_full_match(loyalty_card, 'LC\d{10}')) as matching, count(distinct loyalty_card) as distinct_cards from 'generated/*.csv'"
┌────────┬──────────┬────────────────┐
│  rows  │ matching │ distinct_cards │
│ int64  │  int64   │     int64      │
├────────┼──────────┼────────────────┤
│ 100000 │   100000 │         100000 │
└────────┴──────────┴────────────────┘
$ doppeldata data check orders.profile.yaml generated/
# doppeldata check passed

`generated/`, 100,000 rows, against profile `orders` (sha256:2daa6aead208c0dc3a160aa6afd50fe9939076ef3ef90b7432d645ef9e9bdf73).

0 of 13 metrics failed.
...
```

Generation needs no pack installed, because the profile holds the hint. With `acme-detectors`
uninstalled, the same command writes the same dataset SHA-256, and `profile check` still passes.

## A pack is checked when it loads, and the refusal names what to fix

```text
$ doppeldata capabilities show | rg -o 'acme_loyalty_card \(acme\)'
acme_loyalty_card (acme)
$ doppeldata profile create orders.csv --pack acem --out orders.profile.yaml
doppeldata: E-DATA-022: no detector pack named 'acem' is installed. A pack registers under the 'doppeldata.detector_packs' entry-point group
  closest: acme
  valid:
    acme
    general
    healthcare
```

A pack naming a sensitivity the profile format lacks is refused when it loads:

```text
$ doppeldata profile create orders.csv --pack acme --out orders.profile.yaml
doppeldata: E-DATA-023: the detector pack 'acme' gives acme_loyalty_card the sensitivity 'secret'; choose from identifier, quasi_identifier, sensitive, none
```

- **A pack not installed exits 2**, and any other pack error exits 1.
- **`doppeldata schemas show profile` lists what a sensitivity, method and hint may be.**

## Why

[A pack is a plugin, registered under an entry point by its
name](../semantic-types-and-detector-packs.md#a-pack-is-a-plugin-registered-under-an-entry-point-by-its-name)
says why the general pack always runs, why two packs may not declare one type, and why a pack runs
only where a run names it.
