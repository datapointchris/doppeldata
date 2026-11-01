# Generate rows from column names, a dict or a short YAML

Rows need no data and no profile. A column's name picks its fake, and a dict or a short YAML sets
a range, a list of values, a null share or decimal places where the name is not enough.

```bash
doppeldata data generate --columns name,email,city,zip --rows 1000 --out people.csv
doppeldata data generate people.yaml --out people.csv
doppeldata profile expand people.yaml --out people.profile.yaml
```

```python
import doppeldata as sd

table = sd.generate({
    'name': None,
    'email': None,
    'age': sd.integers(18, 90),
    'plan': sd.choice(['free', 'pro', 'team'], weights=[0.7, 0.25, 0.05]),
    'monthly_spend': sd.decimals(0, 500, places=2, nulls=0.1),
    'signed_up': sd.dates('2023-01-01', '2025-12-31'),
}, rows=1000, seed=7)
```

`people.yaml` is a short YAML:

```yaml
rows: 1000
columns:
  name:
  email:
  city:
  zip:
  age: 18..90
  plan: [free, pro, team]
```

## A name takes the fake its last words name

`doppeldata capabilities show` lists every fake under `fakes`. A column takes one where its name is
the fake's, or ends in it, so `billing_city` draws cities and `customer_zip` ZIP codes:

```python
pl.DataFrame(sd.generate(['customer_zip', 'billing_city', 'signup_email', 'phone_number'], rows=3, seed=1))
```

```text
shape: (3, 4)
┌──────────────┬───────────────┬───────────────────────────────┬──────────────┐
│ customer_zip ┆ billing_city  ┆ signup_email                  ┆ phone_number │
│ ---          ┆ ---           ┆ ---                           ┆ ---          │
│ str          ┆ str           ┆ str                           ┆ str          │
╞══════════════╪═══════════════╪═══════════════════════════════╪══════════════╡
│ 83500        ┆ Grahamport    ┆ beau.ortiz@example.com        ┆ 352-487-1502 │
│ 65271        ┆ Nikolausville ┆ esperanza.collins@example.org ┆ 525-589-8553 │
│ 42423        ┆ Predovicmouth ┆ augusta.maggio@example.net    ┆ 541-294-3330 │
└──────────────┴───────────────┴───────────────────────────────┴──────────────┘
```

A name matching no fake is refused, exit 2, naming the closest fakes and what to give instead:

```text
$ doppeldata data generate --columns name,zipp --rows 10 --out bad.csv
doppeldata: E-PROFILE-021: the column list is not a valid profile:
  --columns: 'zipp' names no fake; give it a fake, a low..high range such as 18..90, or a list of values, in a short YAML; the closest are zip_code [E-PROFILE-021]
  hint: doppeldata capabilities show lists every fake
```

## A dict gives a column a fake, a range or a list of values

| Value | Draws |
| --- | --- |
| `None` | the fake the column's name hints, as the list does |
| `sd.fakes.city`, or any fake `sd.fakes` holds | that fake, whatever the column's name |
| `sd.integers(low, high)` | integers between the two ends, both included |
| `sd.decimals(low, high, places=2)` | numbers between the ends, rounded to `places` |
| `sd.dates('2023-01-01', '2025-12-31')` | dates between the ends |
| `sd.choice(values, weights=[...])` | each value at its weight, or each equally |

Each of the last four takes `nulls=`, the share of rows that are null. Every call that generates
takes the dict and the list, `sd.spark.generate` and `sd.spark.write` included.

## A short YAML gives a column a fake, a range or a list, and every PROFILE argument reads it

- **A column's value is a fake's name, a `low..high` range, a list, or empty.** A range of
  integers, numbers or ISO dates draws as `sd.integers`, `sd.decimals` or `sd.dates`. Empty takes
  the name's fake.
- **`0..1` is a range of integers.** Write `0.0..1` for decimals.
- **Null shares, weights and decimal places need the dict.** A short YAML holds none of them.
- **`rows` is the count `--rows` defaults to.** The table is named for the file, so `people.yaml`
  generates a table named `people`.
- **A file holding `kind` is read as a full profile.**

`data check people.yaml people.csv` checks what the short YAML wrote, as it checks any profile's
data.

## A path with a format's suffix is one file, and anything else a directory

```bash
doppeldata data generate people.yaml --out people.csv --manifest-beside
doppeldata data generate people.yaml --out people/
```

```text
wrote 1,000 rows as csv in 1 file, 69.3 KB, in 0.00 s on 16 threads: 0.33M rows/s
./_manifest.json names dataset sha256:…
wrote 1,000 rows as parquet in 1 file, 37.9 KB, in 0.00 s on 16 threads: 0.25M rows/s
people/_manifest.json names dataset sha256:…
```

- **`--out people.csv` writes that file and nothing beside it.** `--manifest-beside`, or
  `manifest_beside=True` on `sd.write`, adds `_manifest.json`. A directory or S3 destination always
  holds one.
- **`--out people/`, or a path with no format suffix, is a directory** of `part-*` files.
- **`--replace` writes over the file.** A directory whose manifest records another file refuses the
  write, even with `--replace`:

  ```text
  $ doppeldata data generate people.yaml --out people.parquet --replace
  doppeldata: E-WRITE-005: _manifest.json records people.csv as its dataset, so people.parquet cannot join it; write it to another directory
  ```

- **One file refuses what would split it, exit 2**: `--file-rows`, `--partition-by`, a `--format`
  its suffix does not name, and a scenario op that shapes files.

## `profile expand` writes the full profile, to edit once a column needs more

```bash
doppeldata profile expand --columns name,email,city,zip --rows 1000
```

```python
sd.Profile.expand({'name': None, 'age': sd.integers(18, 90)}, rows=1000).save('people.profile.yaml')
```

```yaml
kind: table
name: table
dataset: {rows: 1000}
columns:
  - name: name
    physical_type: string
    logical_type: string
    semantic:
      generator_hint: {provider: name}
```

The expanded profile generates the same bytes as the list, dict or short YAML it came from. From
there a column takes anything a profile states. [Generate a dataset](generate-a-dataset.md) writes
it at any size, and [Profile a dataset](profile-a-dataset.md) writes the same format from real data.

## Why

[Generating from Python and the command line](../generating-from-python-and-the-cli.md) says how a
name picks its fake, why an unknown name is refused rather than drawn as text, and why a one-file
write stages the file and moves it into place.
