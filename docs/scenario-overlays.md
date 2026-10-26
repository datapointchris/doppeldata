# Scenario overlays

## A scenario is a merge patch, ordered perturbations and declared expectations

A profile describes data as it is. A scenario bends it on purpose, for a test that needs a hot key,
late rows or a schema change. It is a file of three parts, validated by the JSON Schema derived from
the core crate's `scenario` model, which `doppeldata schemas show scenario` prints.

- **`patch` changes what a profile can express as a value.** It is an RFC 7396 merge patch with one
  extension: `columns` is a map keyed by column name, so a patch names only the columns it changes.
  RFC 7396 replaces an array whole, so a patch on the profile's list would restate every column. A
  column patched to null is removed.
- **`perturbations` apply faults a profile cannot express**, in list order. `doppeldata scenario
  list` prints each op, and `scenario show OP` its parameters.
- **`expect` declares what the scenario promises**, such as the share the hot keys hold. A check
  certifies those and nothing undeclared.

A scenario is applied with `scenario=` in Python, or `--scenario` on `data plan`, `data generate`
and `data check`. Several compose in order: their patches merge left to right, their perturbations
and expectations concatenate, and the composition's name joins theirs with `+`.

The manifest records the document the scenario's digest covers, and each composed member's digest.
So a dataset regenerates with no scenario file, and an edited member shows which one moved.

*Rejected:* a patched column the profile does not hold being added, as RFC 7396 would add it. A
misspelled column would then generate a new column from a fragment. It is refused as
`unknown-column`.

## `malformed_rows` writes broken files, and `_faults.jsonl` names every fault in them

A pipeline's bad-row handling is tested against files broken on purpose. The `bad-csv` example
writes a profile like `orders` as CSV. It breaks the share of rows its `rate` sets with ragged,
quoting, encoding and type faults. It gives the share of files its `files_share` sets a byte order
mark or a cut last line. A profile with no string column, or none of another type, is refused as
`op-not-for-column`:

```bash
doppeldata data generate orders --scenario bad-csv --out orders-bad/
doppeldata scenario show bad-csv     # its kinds and shares, to start a scenario of your own from
```

A scenario of your own names the kinds. `rate` is the chance each row is broken, shared evenly by
the row kinds named. `files_share` is the chance each file is broken, shared evenly by the file
kinds. `doppeldata scenario show malformed_rows` lists every kind.

```yaml
scenario: ragged-rows
perturbations:
  - {op: malformed_rows, rate: 0.01, kinds: [ragged_short, ragged_long, bom], files_share: 0.25}
```

```bash
doppeldata data generate orders --scenario ragged-rows.scenario.yaml --format csv --out orders-ragged/
```

`_faults.jsonl` beside the data lists each fault, one JSON object a line, and the manifest's
`faults` counts them by kind. `doppeldata schemas show faults` is a line's schema, and
[file-writers-and-manifest.md](file-writers-and-manifest.md) gives each kind's `detail`.

```bash
jq -r .kind orders-bad/_faults.jsonl | sort | uniq -c
```

```json
{"file":"part-00000-1cf678fb.csv","line":25,"row":23,"kind":"unescaped_quote","column":"customer_email","detail":{}}
```

A pipeline test then asserts that its quarantine holds exactly the lines `_faults.jsonl` names.
DuckDB's `reject_errors.line` counts records, where `line` counts physical lines, so an
`embedded_newline` earlier in the file shifts every record after it. A reject is placed by its
`line_byte_position` instead:

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
```

`doppeldata data check orders orders-bad/ --scenario bad-csv` verifies the example's
`malformed.rate` bound.

With no `rate`, `kinds` or `files_share`, the op reproduces what a profile records of its CSV
source. So a profile of a landing zone's files writes files broken as those were:

```bash
doppeldata profile create source/ --out source.profile.yaml
doppeldata data generate source.profile.yaml --scenario reproduce.scenario.yaml --out reproduced/
```

```yaml
scenario: reproduce
perturbations:
  - {op: malformed_rows}
```

## Strict DuckDB rejects each CSV row fault but `embedded_newline`, and Spark reads three as values

`malformed-file-readers/read-every-fault-kind.sh [IMAGE...]` writes `orders` under `bad-csv`, and
once for each kind alone in a format that holds it, at the shares `write_every_fault_kind.py`
sets. It then prints, per kind, what each reader caught of what `_faults.jsonl` names.

- DuckDB reads on the host with the columns declared. Its CSV read sets `strict_mode` and
  `store_rejects`.
- Spark reads in each image named, or each one `spark-parity/spark-images.txt` lists. Its CSV and
  JSON reads are PERMISSIVE with `_corrupt_record` declared. The images agree on every row below
  but decimal drift read as one table.

| Kind | DuckDB | Spark |
| --- | --- | --- |
| `ragged_short` | rejects each as `missing_columns` | a corrupt record for each |
| `ragged_long` | rejects each as `too_many_columns` | a corrupt record for each |
| `unescaped_quote` | rejects each as `unquoted_value` | reads each as a value. `unescapedQuoteHandling` `RAISE_ERROR` fails the read |
| `embedded_newline` | reads each whole, as CSV allows | splits each in two rows. The second is corrupt, and so is the first unless the break is in the last column. `multiLine` reads each whole |
| `type_invalid` | rejects each as `cast` | a corrupt record for each |
| `invalid_utf8` | rejects each as `invalid_encoding` | reads each as a value |
| `line_too_long` | rejects each as `line_size_over_maximum` | reads each as a value |
| `truncated_line` | rejects it as `missing_columns` | a corrupt record |
| `bom` | rejects nothing | rejects nothing, and leaves it off a header it infers |
| `mixed_line_endings` | fails the read: "reached an invalid state" | reads every row |
| `wrong_delimiter` | rejects each row of the file as `cast` | a corrupt record for each row of the file |
| `header_drift` | rejects nothing, and reads a swapped column's values under the other's name | the same. `enforceSchema` false fails the read: "CSV header does not conform to the schema" |
| `latin1_file` | rejects each line holding a character past ASCII as `invalid_encoding` | reads every row |
| `type_invalid`, JSON | fails the read. `ignore_errors` keeps every row | a corrupt record for each |
| `truncated_line`, JSON | fails the read: "Malformed JSON". `ignore_errors` keeps every row | a corrupt record |
| `json_unbalanced` | fails the read: "Malformed JSON". `ignore_errors` keeps every row | a corrupt record for each |
| `json_type_flip` | reads each, casting the string | a corrupt record for an integer, and a decimal read as its number |
| `json_layout_mix` | fails a newline-delimited read. `ignore_errors` reads each array as one row | reads each array's records as rows, because the array is one line |
| `parquet_timestamp_unit_drift` | reads each file at one type | reads each file at one type, with or without `mergeSchema` |
| `parquet_int96` | as one table, takes the first file's type, so an INT96 first file gives `TIMESTAMP`. By name, `TIMESTAMPTZ` | reads as `timestamp`, with or without `mergeSchema` |
| `parquet_decimal_scale_drift` | widens to the larger scale, as one table or by name | `mergeSchema` fails: `CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE`. As one table, Spark 3.5, Glue 5's included, fails with `SchemaColumnConvertNotSupportedException`, and Spark 4 widens |

## `malformed_rows` breaks bytes, so no value moves and the faults are the same on any split

- **Every write without the op is clean.** A profile's malformed rows, line-break mix and `type_mix`
  shape no plain write, whatever its source held.
- **The op breaks the encoded bytes, never the rows.** Each block generates the rows a clean write
  does. So no other perturbation moves, and the determinism fixtures hold no fault.
- **A row's fault is drawn from the op's stream, keyed by the block.** A file's fault is drawn from
  a stream keyed by its range and its partition directory. So the files and `_faults.jsonl` are
  byte for byte the same at any thread count and any Spark partition count.
- **A row takes at most one fault, and a file at most one file fault.** A copy `duplicates` adds is
  broken as any row is, and its record has no `row`.
- **A kind a format cannot hold is refused as `op-not-for-format` before any row is written.** CSV
  and TSV hold every row kind and every file kind but the JSON and Parquet ones. JSON and NDJSON
  hold `type_invalid`, `truncated_line` and the `json_` kinds. Parquet holds the `parquet_` kinds,
  and Arrow none. A kind a column must exist for, such as `embedded_newline` on a table with no
  string column, is refused as `op-not-for-column`.
- **An `invalid_utf8` fault never starts in a file's first block.** doppeldata's profiler guesses
  the encoding from that block, as many readers do. One invalid byte there reads the file as
  Latin-1, where nothing is rejected. A fault drawn there is written clean and not listed. So a
  file of a few hundred rows holds none, and a write whose largest file ends inside the first
  block is refused as `invalid-parameter`, naming the rows a file needs.
- **`truncated_line` cuts the file's last record mid-field**, as a cut-off upload leaves it.
- **A `latin1_file` holding only ASCII is not listed.** windows-1252 writes ASCII as UTF-8 does,
  so the file is unchanged.
- **The op applies only where files are written.** A one-file write keeps its list beside the file,
  named for it: `--out landing/orders.csv` writes `landing/_orders.csv.faults.jsonl`. So several
  one-file writes share a directory, and `replace` replaces a file's list with the file.

## With no rate, `malformed_rows` reproduces the profile's rejected rows by type

Each reason in `source.csv.malformed.by_type` becomes the kind DuckDB rejects for it, at its share:

| `by_type` | Kind written |
| --- | --- |
| `missing_columns` | `ragged_short` |
| `too_many_columns` | `ragged_long` |
| `unquoted_value` | `unescaped_quote` |
| `line_size_over_maximum` | `line_too_long` |
| `invalid_encoding` | `invalid_utf8` |
| `cast` | each typed column's `type_mix` values of other types, at their shares, or `type_invalid` where no column records one |

- **The profile's `line_terminator_mix` is not reproduced.** The profiler refuses a file whose
  records mix line breaks, so a mix it records comes from breaks inside quoted fields, in a file
  every reader opens. `kinds: [mixed_line_endings]` writes the fault where a test asks for it.
- **`invalid_state` is not reproduced.** No bytes were found that DuckDB stores as an
  `invalid_state` reject rather than refusing the whole read.
- **A profile recording none of these is refused** as `invalid-parameter`, because the op would
  write nothing.

## `cdc` writes an AWS DMS change stream, and `_expected` holds the table its merge must reach

A pipeline that merges change data capture output is tested against a stream whose answer is
known. The `cdc-dms` example writes changes to `orders` across its `period`, as AWS DMS writes them
to Amazon S3:

```bash
doppeldata data generate orders --scenario cdc-dms --format parquet --out orders-cdc/
```

The full load is `sales/orders/LOAD00000001.parquet`. The changes follow, one file for each
`file_interval` that holds a change, under date folders below `sales/orders/`. Each change record
starts with `Op`, which is `I`, `U` or `D`, then `dms_commit_ts`. `_expected/sales/orders/` holds
the table the merge must produce. [file-writers-and-manifest.md](file-writers-and-manifest.md)
gives the layout.

A merge keeps each key's last change by commit time, and drops a key whose last change is a
delete. In DuckDB, against `_expected`:

```sql
create view stream as
  select 'I' as Op, * from read_parquet('orders-cdc/sales/orders/LOAD*.parquet')
  union all by name
  select * from read_parquet('orders-cdc/sales/orders/**/[0-9]*.parquet', union_by_name = true);
create view merged as
  select * exclude (Op, dms_commit_ts) from stream
  qualify row_number() over (partition by order_id order by dms_commit_ts desc) = 1 and Op <> 'D';
create view expected as from read_parquet('orders-cdc/_expected/sales/orders/*.parquet');
select (select count(*) from (from merged except all from expected)) as merged_only,
       (select count(*) from (from expected except all from merged)) as expected_only;
```

Both counts are 0. A merge ordered by file name instead differs on exactly the keys of the
records `_faults.jsonl` lists. Each is a change delivered late or twice.

A scenario of your own sets the stream's shape. `doppeldata scenario show cdc` lists every
parameter:

```yaml
scenario: orders-day
perturbations:
  - op: cdc
    style: dms
    schema: sales
    updates: 0.2
    deletes: 0.01
    updates_per_row: {family: geometric, mean: 2}
    period: {start: "2026-10-01T00:00:00Z", duration: 1d}
    file_interval: 10m
    out_of_order_rate: 0.01
```

```bash
doppeldata data generate orders --scenario orders-day.scenario.yaml --format csv --writer '{"header": false}' --out orders-day/
```

Each parameter stands for a DMS endpoint setting, as the
[S3 target guide](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html) names it:

| Parameter | DMS setting |
| --- | --- |
| the write's format, `csv` or `parquet` | `DataFormat` |
| the CSV writer's `header` | `AddColumnName` |
| `timestamp_column` | `TimestampColumnName` |
| `file_interval` | `CdcMaxBatchInterval` |
| `date_partition` `sequence` and `delimiter` | `DatePartitionSequence` and `DatePartitionDelimiter` |
| `include_op_for_full_load` | `IncludeOpForFullLoad` |
| `timestamp_unit: ms`, or the Parquet writer's where the op sets none | `ParquetTimestampInMillisecond` |
| every full-load row stamped with the period's `start` | `UseTaskStartTimeForFullLoadTimestamp: true` |

- **A CSV write prints a header unless the writer's `header` is false.** `--writer '{"header":
  false}'` writes what an endpoint with `AddColumnName` off does.
- **A CSV stream carries no types, so its reader declares them.** Spark's `inferSchema` reads
  `ship_zip` as an integer and drops its leading zeros. A reader with the profile's types
  declared merges to `_expected` exactly.
- **`_expected` is Parquet whatever the stream's format.** It is the merge's answer, so it keeps
  every column's type.

## `cdc` draws every change from the period, and places a fault only where a file-order merge breaks

- **The full load is the table the profile generates.** `inserts`, `updates` and `deletes` are
  shares of its rows. An insert continues the key past the full load's last value. An inserted key
  is then updated and deleted at the same shares as a full-load key.
- **Every commit time is drawn across `period`, never read from the clock.** So the same seed
  writes the same bytes on any day. The full load's `dms_commit_ts` is `start`, as DMS stamps a
  full load under `UseTaskStartTimeForFullLoadTimestamp`, and every change commits after it. A
  key's changes commit in order: its insert first, its delete last.
- **Commit times are drawn to the microsecond, so no key's two changes share one.** A source whose
  commit times are coarser, such as MySQL's whole seconds, ties two changes to one key. The op does
  not model that.
- **An update redraws at least one column other than the key and the profile's `keys.primary`, and
  leaves the rest as they were.** Its new values come from each column's sampler, on the op's own
  stream. `updates_per_row` sets how many times an updated row changes.
- **An op that writes rows applies to the full load and the inserts.** An update's new values
  come from the samplers alone, so no other op reaches them.
- **A change file holds the changes committed in one `file_interval`.** It is named for the
  interval's start, as DMS names one `20141029-1134010000.csv`: the date, the time to the second,
  then four digits of 100 µs. An interval with no change writes no file.
- **Each fault rate is the chance an eligible change takes the fault.** A change is eligible when
  its key's last change comes after it, and deletes the row or leaves it different from this
  change's. A change file must also follow the one holding that last change. The faulted change
  goes into that next file, after the key's last change, so a merge in file order keeps the wrong
  row. The manifest's `faults` counts the faults placed.
- **One draw per change picks its fault.** Below `out_of_order_rate` the change moves to the later
  file. Below the two rates' sum it stays, and a second copy goes there. So the rates sum to 1 at
  most.
- **`schema_drift` beside `cdc` reshapes the changes committed past its cut.** The cut is
  `after_fraction` of the period, so the stream changes shape mid-period, as a source table's
  column change does. The interval holding the cut splits into two files there. A file starting at
  or past the cut drifts, and the full load never does. `_expected` takes the drifted shape. A column the drift adds is null where
  a key's last change came before the cut.
- **A drifted CSV stream is written, since DMS writes a source's column change into its CSV files
  too.** Each file's header names its own columns. A merge maps a renamed column back, as with
  `coalesce(amount_usd, amount)`.
- **The stream is held in memory while it is written.** A `cdc` write takes about 200 bytes a
  change beyond what a plain write of the same rows holds. `data plan` counts the changes first.

## The engine applies the patch and the perturbations, inside the block path

1. The engine applies the patch, giving the effective profile.
2. The samplers generate each block from it.
3. Each perturbation that writes rows applies to the block, in list order.
4. The writers apply the ops that shape files to the files they make.

- **The stream version governs the patch as it does the samplers.** A change to how a patch merges
  moves generated values, so it raises the stream version. The determinism test pins values
  generated under a scenario fixture that holds a patch.
- **The identity's `profile_digest` is the profile's as given, not the patched one's.** The
  scenario's digest covers the patch. So the manifest's profile path names the file whose digest
  the identity holds.
- **A scenario changes no draw a profile alone makes.** A dataset with no scenario draws only from
  the profile's streams, so no scenario code moves its values.
- **`Scenario.verify` and the expectation metrics are Python over a Polars LazyFrame.** The library
  reads data through the profiler's readers already, and a metric there is a few lines.

## Each perturbation draws from a stream keyed by its id, never its position

Each op's draws come from `stream_key(table, id, "perturbation.<draw>")`, and a value an op draws
from a column's sampler comes from that sampler on the table `"<table>\0perturbation <id>"`. So
inserting a perturbation leaves every other one's rows unchanged, compared by Arrow value.

- **The id defaults to `<op>:<column, or table>:<n>`.** n counts earlier entries of that op and
  column. So inserting a `noise` moves no `missingness`, while a second `hot_keys` on one column
  numbers after the first. An entry that must stay put under any edit names its own `id`.
- **Two entries sharing an id are refused** as `duplicate-perturbation-id`, because they would
  share a stream.
- **A scenario's digest is the SHA-256 of its RFC 8785 canonical JSON, with every id resolved and
  no `description` or `expect`.** So naming a default id, rewording the description or moving a
  bound keeps the identity. A bound is a claim about the data, not an input to it.

## Duplicates change the row count, so each block's count is planned up front

`rate` is the share of the rows written that are copies. So a block of n generated rows carries
`n × rate / (1 − rate)` copies, its fraction drawn on a stream keyed by the block. The copies follow
the block's generated rows, so block boundaries never move, and a Spark chunk generates what a
local run does.

- **`rows` and the identity's `dataset_rows` count the rows generated.** The copies come on top.
  `data plan` prints `rows_written`, the rows the files will hold.
- **Each row of a block is filed with a generated row of it, its home.** A range takes the rows
  homed in it, so a block cut between two files sends each copy with its home.
- **A `within_file` copy is homed to its source**, so it is written to its source's file at any
  file size.
- **An `across_files` copy comes from the block half the dataset away**, and is homed to a row of
  its own block. So with two files, the copy lands in the file its source does not.
- **A write is refused when a file holds more rows than the half-dataset reach**, or the dataset is
  one block. A copy could then share its source's file, so `invalid-parameter` names the op's
  `scope`.
- **`exact: false` varies one column of each copy**, taking that column's value from another row.
  A primary key column is never the one varied.

## The ops that shape files apply only where files are written

`schema_drift`, `small_files`, `malformed_rows` and `cdc` change the files rather than the rows.
`generate`, `stream` and `doppeldata.spark.generate` write no files, so each refuses them as
`op-needs-files`, naming the write that takes them. A write to one file refuses `schema_drift`,
`small_files` and `cdc`, which act across files.

- **`doppeldata.spark` refuses `cdc` as `op-needs-one-process`**, from `generate`, `struct_type`
  and `write` alike, as it refuses any op the engine says orders the whole table. The stream orders
  the whole table's changes by commit time, so one process writes it, and the refusal names
  `doppeldata.write`.
- **A `cdc` write refuses a format other than CSV or Parquet** as `op-not-for-format`, and
  `timestamp_unit` on CSV the same way. It refuses `partition_by` as `invalid-parameter`, because
  the change files are filed by commit date.
- **A Parquet writer's `timestamp_unit` of `ms` or `us` sets the stream's where the op sets none.**
  The writer's `s` or `ns`, and a writer's unit apart from the op's, are refused as
  `invalid-parameter`, since DMS stores every timestamp at one unit.
- **A file drifts when its first row lies at or past `after_fraction` of the rows.** Beside `cdc`,
  the cut is that fraction of the period instead. The writers apply the drift to the whole block
  before a range takes its rows. Its steps run as drop, widen, rename, add, then reorder.
- **A drift no file reaches is refused**, naming `after_fraction`. A one-file dataset drifts
  nothing, which would leave the scenario silently unapplied.
- **A partition key the drift drops or renames is refused**, because a drifted file could not be
  filed under it.
- **A drift written as CSV, TSV or Arrow is refused** as `op-not-for-format`. Their files must
  share one header or schema to read as one table. Parquet, JSON and NDJSON read back as the union.
  A `cdc` stream is the exception, because a CSV change stream changes shape as DMS writes one.
- **An added column is drawn per written row**, so a copy and its source differ in it.
- **`small_files` sizes the files from a lognormal**, tabulated as the layout's `rows_per_file`
  quantiles. Its sizes lay over the profile's, and a `file_rows` given to the write lays over both.

## The ops that need a choice make it this way

- **`hot_keys` takes the column's most frequent values, by stated rate.** A column listing fewer
  than `keys` values takes the rest from its own sampler, on the perturbation's stream. One that
  draws too few distinct values is refused at `keys`.
- **`missingness` under MAR nulls a row with chance `rate + (2r − 1) × min(rate, 1 − rate)`.** r is
  the row's `depends_on` value as an average-rank share of its block, and a null there counts as
  the middle. So the rate averages `rate`, and rises with the other column. MNAR ranks the column's
  own values, and MCAR is uniform.
- **`late_arrival` shifts the event time earlier by at least one tick.** `ingest` names a column
  holding each row's unshifted time. A profile lacking it gains it, at the event column's type.
  `late_arrival.rate` measures against it, so an expectation of it needs `ingest`.
- **`categorical_shift` moves as few rows as reach the new mix.** Each value it names takes its
  share, and every other value keeps its proportion of what is left.
- **`out_of_range` writes a sentinel cast to the column's type, or a value past an extreme.** The
  extremes come from the profile's `extremes`, else the outer quantiles.
- **`partition_skew` gives each value named its share**, so a write partitioned by the column fills
  its directories in those shares.
- **`cdc` keys every change by `key`, else the profile's one-column `dataset.keys.primary`.** A
  profile stating neither, or a primary key of several columns, is refused as `no-primary-key`. A
  key column must draw unique values, such as a sequence or a UUID with no nulls, or it is refused
  as `op-not-for-column`.
- **`cdc` refuses `duplicates` and `malformed_rows` beside it**, as `invalid-parameter`. A copied
  row would hold its key twice in the full load, and the stream's own faults are its two rates. An
  op writing the key, a `keys.primary` column, `Op` or the commit column is refused too, since
  every change holds those as they are.

## Each refusal names its JSON Pointer before anything is generated

A refusal is an `InvalidScenario` whose issues each carry a `code` and a `field`. The field is an
RFC 6901 pointer into the scenario, such as `/perturbations/2/keys`. `doppeldata scenario check
--json` prints them.

- **A code names what it refuses**, such as `unknown-column` or `op-not-for-format`.
  `invalid-parameter` says least: a parameter out of range, or one the files cannot hold, such as
  a drift no file reaches. The format's own faults carry the profile's codes.
- **An unknown op or metric lists every one doppeldata knows**, and its issue's `closest` holds the
  nearest names.
- **`doppeldata scenario check FILE PROFILE` exits 3 on a refusal.** It validates the scenario,
  applies its patch, and plans each perturbation and the file layout as `data generate` would with
  the same layout flags.
- **A data verb exits 2 on one**, as it does on an invalid profile.

## `data check --scenario` skips what a perturbation moves, and `verified` covers the expectations

The data is measured against the effective profile, so a patched null rate is the one expected.

- **`data check` refuses a scenario other than the one the data was written under.** Where DATA
  holds the `_manifest.json` a write left, a `--scenario` of another digest is refused as
  `scenario-differs-from-data`. No `--scenario` on data whose manifest records one is refused as
  `data-has-scenario`. Both exit 2. A scenario's skips would otherwise pass data it never touched.
- **A metric a perturbation moves is a row with no value and `skipped_by` naming it.** It passes. A
  perturbation moves every metric on a column it writes, as the engine reports its columns. So it
  moves each `--against` score on that column too, and each pair score holding it.
- **An op that changes the row count or the file sizes also moves the table metrics those set.**
  `MOVED_METRICS` in `profile_comparison/scenario_moves.py` lists them. A partition key's
  `partition_share_delta` is named for its column, so `partition_skew` moves it.
- **A column change on a column a perturbation writes passes**, its message naming it, as the
  columns a drift adds do.
- **A `cdc` write is checked by its full load alone.** The check reads the full-load files the
  manifest's `files` lists, and prints one notice naming them, that it left the change files and
  `_expected` unmeasured. They are a
  merge test's input and its answer, and their statistics move by design. The commit column passes
  as a column `cdc` writes.
- **Each expectation is measured by `Scenario.verify`.** `hot_keys.share` is the share of rows
  holding the column's `keys` most frequent values. `late_arrival.rate` is the share whose column
  is earlier than its `ingest`. `missingness.rate` is the share that is null, the profile's own
  nulls included.
- **`duplicates.rate` is the share whose `column` repeats an earlier row's**, a key each copy
  shares with its source. A later perturbation writing that column is refused, because a copy's
  key would then differ from its source's.
- **`malformed.rate` is the share of rows a strict reader rejects**, as a profile's
  `source.csv.malformed.rate` counts them. It measures the whole table, so it takes no `column`.
  Data that is neither CSV nor TSV leaves it unmeasured.
- **An expectation on a column the data lacks fails**, with `unmeasured` naming the column. A
  drift that drops it reaches this.
- **`verified` is false when an expectation misses its bounds**, and `data check` then exits 3.

*Rejected:* certifying a statistic the scenario did not declare. `verified` would then mean
something a reader of the scenario could not see.

## The examples are written for the `orders` profile

The example scenarios ship as package data beside the example profiles, and `doppeldata scenario
list` names them. `--scenario hot-key` takes one by name.

- **`Scenario.example(name, **parameters)` replaces parameters of the example's one perturbation.**
  `column=` moves its expectations with it.
- **A keyword changing any other value drops the expectations.** Each was written for the
  example's own values.
- **Any keyword drops the description**, which names those values.

## A drifted Parquet dataset reads as the union of its files' schemas

So `data check` profiles a written `schema_drift` as one table.
[profiler-statistics-and-readers.md](profiler-statistics-and-readers.md) says how the union is read.
