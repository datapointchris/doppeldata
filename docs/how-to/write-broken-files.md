# Write broken files

A pipeline's bad-row handling is tested against files broken on purpose. The `malformed_rows` op
writes ragged, badly quoted, badly encoded and mistyped rows into a write's files. It lists every
fault in `_faults.jsonl` beside them, so a test knows which lines its quarantine must hold.

```bash
doppeldata scenario show bad-csv
doppeldata data generate orders --seed 7 --scenario bad-csv --out orders-bad/
jq -r .kind orders-bad/_faults.jsonl | sort | uniq -c
doppeldata data check orders orders-bad/ --scenario bad-csv
```

```python
import doppeldata as sd

written = sd.write('orders', 'orders-bad/', seed=7, scenario=sd.Scenario.example('bad-csv'))
written.faults                      # {'ragged_short': 335, 'ragged_long': 354, 'unescaped_quote': 337, ...}
report = sd.check('orders', 'orders-bad/', scenario=sd.Scenario.example('bad-csv'))
report.passed, report.verified      # (True, True)

ragged = sd.Scenario.load('ragged-rows.scenario.yaml')
sd.write('orders', 'orders-ragged/', seed=7, scenario=ragged, format='csv', file_rows=25_000)
```

## `bad-csv` breaks 2% of the rows and half the files of a CSV write

```text
$ doppeldata scenario show bad-csv
# Written for the orders example profile. The patch writes CSV whatever format a profile records.
scenario: bad-csv
description: CSV a pipeline must quarantine rows of, with ragged, badly quoted and badly encoded rows, and a cut last line or a byte order mark in half the files.
patch: {source: {format: csv, compression: null}}
perturbations:
  - op: malformed_rows
    fraction: 0.02
    kinds: [ragged_short, ragged_long, unescaped_quote, embedded_newline, type_invalid, invalid_utf8, truncated_line, bom]
    files_fraction: 0.5
expect:
  # A strict reader rejects or cannot cast every row kind but embedded_newline, which CSV allows inside quotes.
  - {metric: malformed.fraction, at_least: 0.013, at_most: 0.019}
```

- **`fraction` is the chance each row is broken**, shared evenly by the row kinds named.
- **`files_fraction` is the chance each file is broken**, shared evenly by the file kinds named. A
  100,000-row write is one file, and at seed 7 it took no file fault.
- **A profile with no column a kind needs is refused** as `E-SCENARIO-102`, such as
  `embedded_newline` on a table with no string column.

## A scenario of your own names the kinds

`doppeldata scenario show malformed_rows` lists every kind and the op's parameters.

```yaml
scenario: ragged-rows
perturbations:
  - {op: malformed_rows, fraction: 0.01, kinds: [ragged_short, ragged_long, bom], files_fraction: 0.25}
```

```text
$ doppeldata data generate orders --seed 7 --scenario ragged-rows.scenario.yaml --format csv --file-rows 25000 --out orders-ragged/
wrote 100,000 rows as csv in 4 files, 8.2 MB, in 0.07 s on 16 threads: 1.52M rows/s
orders-ragged/_manifest.json names dataset sha256:da9837ac5e58a671c6de6c23bcd34057ed2b44b683a4f895a4c615ffd545286a
$ jq -c .faults orders-ragged/_manifest.json
{"ragged_short":530,"ragged_long":496,"bom":2}
```

A kind the format cannot hold is refused before a row is written, exit 2. A Parquet write takes the
`parquet_` kinds, which change a file's schema rather than its rows:

```text
$ doppeldata data generate orders --seed 7 --scenario decimal-drift.scenario.yaml --format csv --out drift-csv/
doppeldata: E-SCENARIO-105: decimal-drift.scenario.yaml is not a valid scenario:
  /perturbations/0/kinds/0: parquet_decimal_scale_drift is not a fault a csv file can hold [E-SCENARIO-105]
  hint: doppeldata capabilities show lists the formats each op takes
$ doppeldata data generate orders --seed 7 --scenario decimal-drift.scenario.yaml --file-rows 25000 --out drift/
wrote 100,000 rows as parquet in 4 files, 2.8 MB, in 0.07 s on 16 threads: 1.35M rows/s
drift/_manifest.json names dataset sha256:77d587a19a457ac3647be70a701effd4f3213c848f705864773ab7149d8e05ad
$ cat drift/_faults.jsonl
{"file":"part-00001-77d587a1.parquet","line":null,"row":null,"kind":"parquet_decimal_scale_drift","column":null,"detail":{"columns":{"amount":"decimal128(13, 3)"}}}
{"file":"part-00002-77d587a1.parquet","line":null,"row":null,"kind":"parquet_decimal_scale_drift","column":null,"detail":{"columns":{"amount":"decimal128(13, 3)"}}}
```

`decimal-drift.scenario.yaml` holds one op: `{op: malformed_rows, kinds: [parquet_decimal_scale_drift], files_fraction: 0.5}`.

## `_faults.jsonl` names each fault's file, line, row and column

```text
$ jq -r .kind orders-bad/_faults.jsonl | sort | uniq -c
    338 embedded_newline
    317 invalid_utf8
    354 ragged_long
    335 ragged_short
    325 type_invalid
    337 unescaped_quote
$ head -2 orders-bad/_faults.jsonl
{"file":"part-00000-f337b5bc.csv","line":3,"row":1,"kind":"embedded_newline","column":"ship_zip","detail":{}}
{"file":"part-00000-f337b5bc.csv","line":132,"row":129,"kind":"unescaped_quote","column":"status","detail":{}}
```

- **`line` is the line a broken record starts on, from 1 with the header counted.** `row` is the
  dataset row it was generated as, from 0.
- **A file fault has no `line` or `row`.**
- **The manifest's `faults` counts them by kind**, and so does the `Manifest.faults` a Python write
  returns.
- **`doppeldata schemas show faults` prints a line's schema.** [The file writers and the
  manifest](../file-writers-and-manifest.md) give each kind's `detail`.
- **A one-file write keeps its list beside the file, named for it:**

  ```text
  $ doppeldata data generate orders --seed 7 --scenario bad-csv --out landing/orders.csv
  wrote 100,000 rows as csv in 1 file, 8.2 MB, in 0.23 s on 16 threads: 0.43M rows/s
  landing/orders.csv holds dataset sha256:f337b5bc390747853924abd1a2dc4244c70f5b65bafc7c0f784f765a69edd3cd
  $ eza -a landing/
  _orders.csv.faults.jsonl  orders.csv
  ```

## A quarantine test asserts it holds exactly the lines `_faults.jsonl` names

DuckDB's `reject_errors.line` counts records, where `line` counts physical lines. So an
`embedded_newline` earlier in a file shifts every record after it. This script places each reject by
its `line_byte_position` instead:

```python
import bisect, json, re
from pathlib import Path

import duckdb

data = Path('orders-bad')
columns = {'order_id': 'BIGINT', 'customer_id': 'BIGINT', 'amount': 'DECIMAL(12,2)', 'quantity': 'INTEGER',
           'status': 'VARCHAR', 'ordered_at': 'TIMESTAMPTZ', 'customer_email': 'VARCHAR', 'ship_zip': 'VARCHAR'}
faults = [json.loads(line) for line in (data / '_faults.jsonl').read_text().splitlines()]
# CSV allows a newline inside quotes, so a strict reader takes an embedded_newline row whole.
expected = {(f['file'], f['line']) for f in faults if f['line'] is not None and f['kind'] != 'embedded_newline'}

quarantined = set()
for path in sorted(data.glob('part-*.csv')):
    connection = duckdb.connect()
    connection.execute(
        'create table orders as select * from read_csv($path, columns = $columns, header = true, '
        'auto_detect = false, strict_mode = true, store_rejects = true)',
        {'path': str(path), 'columns': columns},
    )
    breaks = [match.start() for match in re.finditer(b'\n', path.read_bytes())]
    for (at,) in connection.execute('select min(line_byte_position) from reject_errors group by line').fetchall():
        quarantined.add((path.name, bisect.bisect_left(breaks, at) + 1))
assert quarantined == expected
print(len(quarantined), 'rows quarantined, each one _faults.jsonl names')
```

```text
$ python quarantine.py
1668 rows quarantined, each one _faults.jsonl names
```

## `data check --scenario` measures the malformed share, and skips what the faults move

```text
$ doppeldata data check orders orders-bad/ --scenario bad-csv
# doppeldata check passed

`orders-bad/`, 98,657 rows, against profile `orders` (sha256:95abee1ddc215b0b2aa73077d2e02925d48b8acfc2461ded17ba7cddbeec16e2) under scenario `bad-csv` (sha256:ac1bf35685eab09b7ec83724d12b9e18f58be8dcb9be562a174dd6d0c3d98e14).

0 of 20 metrics failed.

| Metric | Column | Value | Threshold | Expected | Observed | Passed |
| --- | --- | --- | --- | --- | --- | --- |
| ks | amount | 0 | ≤ 0.05 |  |  | yes |
| ks | quantity | 0 | ≤ 0.05 |  |  | yes |
| tvd | status | skipped by malformed_rows:table:0 | ≤ 0.05 |  |  | yes |
...
| Change | Column | Passed | Detail |
| --- | --- | --- | --- |
| kind_changed | amount | yes | amount is decimal in the source profile and float in the other |
...
0 of 1 expectations failed.

| Expectation | Column | Value | Bounds | Passed |
| --- | --- | --- | --- | --- |
| malformed.fraction |  | 0.01593 | ≥ 0.013 and ≤ 0.019 | yes |
```

- **The check profiles the files as `profile create` does.** A profile of `orders-bad/` records the
  same 98,657 rows and a `malformed.fraction` of 0.01593.
- **A metric the faults move is skipped**, naming the op's id.
- **A text read cannot tell a decimal from a float**, so `amount` read back as a float passes.
- **[Check generated data](check-generated-data.md) says what each metric measures.**

## With no `fraction`, `kinds` or `files_fraction`, the op writes the faults a profiled source held

A profile of a landing zone's CSV records the share of rows a strict reader rejected, by reason. The
op with no parameters writes each reason's kind at its share. Here `source/` holds the four files
the `ragged-rows` write made.

```yaml
scenario: reproduce
perturbations:
  - {op: malformed_rows}
```

```text
$ mkdir source && cp orders-ragged/*.csv source/
$ doppeldata profile create source/ --out source.profile.yaml
$ yq -o=json -I0 .source.csv.malformed source.profile.yaml
{"fraction":0.01026,"type_fractions":{"missing_columns":0.0053,"too_many_columns":0.00496}}
$ doppeldata data generate source.profile.yaml --seed 7 --scenario reproduce.scenario.yaml --out reproduced/
wrote 98,974 rows as csv in 4 files, 8.1 MB, in 0.08 s on 16 threads: 1.25M rows/s
reproduced/_manifest.json names dataset sha256:37985438ff5fef848c1e69bd446ea8f3d01d72558b42c966063731551386a645
$ jq -c .faults reproduced/_manifest.json
{"ragged_short":528,"ragged_long":436}
$ doppeldata data check source.profile.yaml reproduced/ --scenario reproduce.scenario.yaml
# doppeldata check passed

`reproduced/`, 98,010 rows, against profile `source` (sha256:ae6946f43e00ab79887a14914d561ccbcf12af5e0298dc136034f80f100983d2) under scenario `reproduce` (sha256:401d63fe6be3d26ed18b3c9525676865ade7c07a6eca92f8552134f092862a1b).

0 of 32 metrics failed.
...
| malformed_fraction_delta |  | 0.0540936 | ≤ 0.2 | 0.00496 | 0.004405 | yes |
```

`missing_columns` became `ragged_short` and `too_many_columns` became `ragged_long`. The source's
byte order marks were not reproduced: the op reproduces rejected rows, never file faults.

## Why

[Scenario overlays](../scenario-overlays.md) says why the op breaks bytes and never values, which
kinds each format holds, what DuckDB and Spark make of each kind, and which kind each rejected
reason becomes.
