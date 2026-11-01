# Write a change stream

A pipeline that merges change data capture output is tested against a stream whose answer is known.
The `cdc` op writes a full load and the changes after it, as AWS DMS writes them to Amazon S3.
`_expected` beside them holds the table a correct merge produces.

```bash
doppeldata scenario show cdc-dms
doppeldata data plan orders --seed 7 --scenario cdc-dms --format parquet --out orders-cdc/
doppeldata data generate orders --seed 7 --scenario cdc-dms --format parquet --out orders-cdc/
duckdb < merge.sql
doppeldata data check orders orders-cdc/ --scenario cdc-dms
```

```python
import doppeldata as sd

written = sd.write('orders', 'orders-cdc/', seed=7, scenario=sd.Scenario.example('cdc-dms'), format='parquet')
written.faults                  # {'duplicate_delivery': 7, 'out_of_order': 7}
written.cdc['changes']          # {'inserts': 5000, 'updates': 14642, 'deletes': 1030}
```

## `cdc-dms` writes a week of changes to `orders`, one file for each minute holding one

```text
$ doppeldata scenario show cdc-dms
# Written for the orders example profile, whose dataset.keys.primary, order_id, keys every change.
# The write's format, csv or parquet, is the files' format, as DMS's DataFormat sets it.
scenario: cdc-dms
description: A week of changes to orders as AWS DMS writes them to Amazon S3, LOAD files then one change file a minute with changes in it, beside _expected, the table a correct merge produces.
perturbations:
  - op: cdc
    style: dms
    schema: sales
    insert_fraction: 0.05
    update_fraction: 0.10
    delete_fraction: 0.01
    updates_per_row: {family: geometric, mean: 1.4}
    period: {start: "2026-10-01T00:00:00Z", duration: 7d}
    file_interval: 1m
    timestamp_column: dms_commit_ts
    date_partition: {enabled: true, sequence: YYYYMMDD, delimiter: SLASH}
    duplicate_delivery_fraction: 0.001
    out_of_order_fraction: 0.002
$ doppeldata data plan orders --seed 7 --scenario cdc-dms --format parquet --out orders-cdc/
...
stream     20,672 changes as dms in up to 8,799 files under sales/orders/, beside 2 parts under _expected/sales/orders/
estimate   44.1 MB, from the first blocks encoded with these settings
$ doppeldata data generate orders --seed 7 --scenario cdc-dms --format parquet --out orders-cdc/
wrote 100,000 rows as parquet in 1 file, 2.3 MB, in 0.56 s on 16 threads: 0.18M rows/s
and 20,672 changes as dms in 8,798 files, 42.5 MB, under sales/orders/, beside _expected/sales/orders/, 2.4 MB, the table a merge in commit order reaches
orders-cdc/_manifest.json names dataset sha256:478e44683b92a29c94c094fac2460c679998256de2d80a9276301a187228cb44
$ eza --tree --level 4 orders-cdc
orders-cdc
├── _expected
│   └── sales
│       └── orders
│           ├── part-00000-478e4468.parquet
│           └── part-00001-478e4468.parquet
├── _faults.jsonl
├── _manifest.json
└── sales
    └── orders
        ├── 2026
        │   └── 10
        └── LOAD00000001.parquet
```

- **`sales/orders/LOAD00000001.parquet` is the full load**, the table the profile generates.
- **The changes lie under date folders**, such as `sales/orders/2026/10/01/20261001-0001000000.parquet`.
  Each file is named for its interval's start, and an interval with no change writes none.
- **Each change record starts with `Op`, which is `I`, `U` or `D`, then `dms_commit_ts`.**
- **`_expected/sales/orders/` is the table a merge must produce.**
- **[The file writers and the manifest](../file-writers-and-manifest.md) give the layout.**
  `doppeldata scenario show cdc` prints every parameter.

## A merge in commit order reaches `_expected`

A merge keeps each key's last change by commit time, and drops a key whose last change is a delete.
In DuckDB, `merge.sql`:

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

```text
$ duckdb < merge.sql
┌─────────────┬───────────────┐
│ merged_only │ expected_only │
│    int64    │     int64     │
├─────────────┼───────────────┤
│           0 │             0 │
└─────────────┴───────────────┘
```

## A merge in file order gets exactly the keys `_faults.jsonl` names wrong

Each fault is a change delivered late or twice, into a file after the one holding its key's last
change. So a merge that takes the last file's record keeps the wrong row.

```text
$ head -1 orders-cdc/_faults.jsonl
{"file":"sales/orders/2026/10/03/20261003-1452000000.parquet","line":null,"row":62551,"kind":"duplicate_delivery","column":null,"detail":{"commit_ts":"2026-10-03 10:15:01.152518","original_file":"sales/orders/2026/10/03/20261003-1015000000.parquet","record":0}}
```

`row` is the dataset row of the change's key. `order_id` is a sequence from 1, so the key is
`row + 1`. `merge-in-file-order.sql` orders by file name and record instead:

```sql
create view stream as
  select 'I' as Op, *, '' as filename, 0 as file_row_number from read_parquet('orders-cdc/sales/orders/LOAD*.parquet')
  union all by name
  select * from read_parquet('orders-cdc/sales/orders/**/[0-9]*.parquet', union_by_name = true, filename = true, file_row_number = true);
create view merged as
  select * exclude (Op, dms_commit_ts, filename, file_row_number) from stream
  qualify row_number() over (partition by order_id order by filename desc, file_row_number desc) = 1 and Op <> 'D';
create view expected as from read_parquet('orders-cdc/_expected/sales/orders/*.parquet');
create view wrong as
  select order_id from (from merged except all from expected)
  union select order_id from (from expected except all from merged);
create view faulted as
  select distinct row::bigint + 1 as order_id from read_json('orders-cdc/_faults.jsonl');
select (select count(*) from wrong) as wrong_keys,
       (select count(*) from faulted) as faulted_keys,
       (select count(*) from (from wrong except from faulted)) as wrong_not_faulted;
```

```text
$ duckdb < merge-in-file-order.sql
┌────────────┬──────────────┬───────────────────┐
│ wrong_keys │ faulted_keys │ wrong_not_faulted │
│   int64    │    int64     │       int64       │
├────────────┼──────────────┼───────────────────┤
│         14 │           14 │                 0 │
└────────────┴──────────────┴───────────────────┘
```

## A scenario of your own sets the stream's shape, and each parameter is a DMS setting

```yaml
scenario: orders-day
perturbations:
  - op: cdc
    style: dms
    schema: sales
    update_fraction: 0.2
    delete_fraction: 0.01
    updates_per_row: {family: geometric, mean: 2}
    period: {start: "2026-10-01T00:00:00Z", duration: 1d}
    file_interval: 10m
    out_of_order_fraction: 0.01
```

```text
$ doppeldata data generate orders --seed 7 --scenario orders-day.scenario.yaml --format csv --writer '{"header": false}' --out orders-day/
wrote 100,000 rows as csv in 1 file, 11.0 MB, in 0.18 s on 16 threads: 0.54M rows/s
and 40,728 changes as dms in 144 files, 4.5 MB, under sales/orders/, beside _expected/sales/orders/, 2.3 MB, the table a merge in commit order reaches
orders-day/_manifest.json names dataset sha256:dca79c195c56f6c352479b3c8ac6ce6803585c67fd3b7996fe848c4a037bdfd3
$ head -2 orders-day/sales/orders/LOAD00000001.csv
2026-10-01 00:00:00.000000,1,87816,80.73,4,cancelled,2025-12-03T22:42:06Z,lee.murazik@example.com,64077
2026-10-01 00:00:00.000000,2,72527,53.75,2,shipped,2025-12-29T05:13:09Z,gerald.leuschke@example.net,98334
$ head -2 orders-day/sales/orders/20261001-0000000000.csv
U,2026-10-01 00:00:03.721398,67174,759,318.69,4,delivered,2025-03-02T21:22:18Z,maurine.kulas@example.org,00040
U,2026-10-01 00:00:03.975485,58531,45538,72.07,2,delivered,2025-04-05T06:19:52Z,orland.erdman@example.org,65578
```

With no `date_partition`, the change files lie directly under `sales/orders/`. Each parameter stands
for a DMS endpoint setting, as the
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
  `ship_zip` as an integer and drops its leading zeros.
- **`_expected` is Parquet whatever the stream's format.** It is the merge's answer, so it keeps
  every column's type.

A merge of the headerless stream declares each column, `Op` first on a change file:

```sql
set variable orders = {'dms_commit_ts': 'TIMESTAMP', 'order_id': 'BIGINT', 'customer_id': 'BIGINT', 'amount': 'DECIMAL(12,2)',
  'quantity': 'INTEGER', 'status': 'VARCHAR', 'ordered_at': 'TIMESTAMPTZ', 'customer_email': 'VARCHAR', 'ship_zip': 'VARCHAR'};
create view stream as
  select 'I' as Op, * from read_csv('orders-day/sales/orders/LOAD*.csv', header = false, columns = getvariable('orders'))
  union all by name
  select * from read_csv('orders-day/sales/orders/[0-9]*.csv', header = false,
                         columns = struct_concat({'Op': 'VARCHAR'}, getvariable('orders')));
create view merged as
  select * exclude (Op, dms_commit_ts) from stream
  qualify row_number() over (partition by order_id order by dms_commit_ts desc) = 1 and Op <> 'D';
create view expected as from read_parquet('orders-day/_expected/sales/orders/*.parquet');
select (select count(*) from (from merged except all from expected)) as merged_only,
       (select count(*) from (from expected except all from merged)) as expected_only;
```

```text
$ duckdb < merge-csv.sql
┌─────────────┬───────────────┐
│ merged_only │ expected_only │
│    int64    │     int64     │
├─────────────┼───────────────┤
│           0 │             0 │
└─────────────┴───────────────┘
```

## `data check` measures the full load and leaves the changes to the merge test

```text
$ doppeldata data check orders orders-cdc/ --scenario cdc-dms
# doppeldata check passed

`orders-cdc/`, 100,000 rows, against profile `orders` (sha256:95abee1ddc215b0b2aa73077d2e02925d48b8acfc2461ded17ba7cddbeec16e2) under scenario `cdc-dms` (sha256:3ad3c44f45ac6323f95c745ede170e444fa15911c7534782f1112327abd24104).

0 of 20 metrics failed.
...
| column_added | dms_commit_ts | yes | dms_commit_ts (string) is not in the source profile; perturbation cdc:table:0 writes it, so it passes |
...
doppeldata: orders-cdc holds a cdc change stream, so the check read its full load, sales/orders/LOAD00000001.parquet, and left its 8,798 change files and _expected/sales/orders/ unmeasured: they are a merge test's input and its answer
```

## Why

[Scenario overlays](../scenario-overlays.md) says how each change is drawn from the period, where a
fault is placed and why, what an update redraws, and what `cdc` refuses beside it.
