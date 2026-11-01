# Drive doppeldata from a script

A script or a coding agent drives doppeldata through `--json` and the exit status. `capabilities
show --json` says what this install takes, each refusal names a registered code, and `data plan
--json` says how each column will draw.

```bash
doppeldata capabilities show --json | jq -c '.ops[] | select(.op == "cdc")'
doppeldata data plan orders --scenario hot-key --out out/ --json
doppeldata data plan ordres --out out/ --json; echo "exit $?"
```

```python
import doppeldata as sd

print(sd.capabilities()['ops'][0])
try:
    sd.generate('ordres', rows=10)
except sd.DoppeldataError as error:
    if error.code == sd.issue_codes.PROFILE_NOT_FOUND:
        print(error.code, error.exit_code, error.issues[0].field, error.issues[0].closest)
```

## `capabilities show` lists everything this install takes

```text
$ doppeldata capabilities show
doppeldata profile list       List the hand-written example profiles and bundles doppeldata ships, by the name every PROFILE argument takes.
doppeldata profile create     Profile a dataset into a file holding no rows, from local files or S3.
...
doppeldata capabilities show  List everything this install takes, read from the registries that refuse a value outside them.
formats         parquet, arrow, csv, tsv, json, ndjson
export formats  odcs, table-schema, json-schema, dqdl, pandera, dataframely, pyspark-schemas
engines         polars, spark
policies        default, strict, healthcare, none, k_min 11 unless given
...
ops             hot_keys, duplicates, late_arrival, missingness, out_of_range, swapped_values, categorical_shift, noise,
                scaling, broken_characters, outliers, schema_drift (parquet, json, ndjson), small_files, partition_skew,
                malformed_rows (parquet, csv, tsv, json, ndjson), cdc (parquet, csv)
...
```

`--json` prints each list whole, as one document. A program looks a value up there rather than
keeping its own copy:

```text
$ doppeldata capabilities show --json | jq -c '.ops[] | select(.op == "cdc")'
{"op":"cdc","formats":["parquet","csv"],"fields":["id","style","schema","key","insert_fraction","update_fraction","delete_fraction","updates_per_row","period","file_interval","include_op_for_full_load","timestamp_column","date_partition","timestamp_unit","duplicate_delivery_fraction","out_of_order_fraction"]}
$ doppeldata capabilities show --json | jq -c '.issue_codes[] | select(.code == "E-PROFILE-028")'
{"code":"E-PROFILE-028","area":"PROFILE","raised_when":"a profile named is neither a file nor an example doppeldata ships","exit_code":2,"hint":"doppeldata profile list names every example"}
$ doppeldata capabilities show --json | jq -c '.commands[] | select(.path == ["data", "generate"]) | [.flags[].name]'
["--out","--columns","--rows","--seed","--format","--file-rows","--partition-by","--no-partitions","--partition-style","--writer","--embed-profile","--replace","--threads","--scenario","--manifest","--scale","--aws-profile","--verbose","--quiet","--json"]
```

- **`jq -r 'keys[]'` names every list**, and `jq .exit_codes` says what each exit status means.
- **`sd.capabilities()` returns the same document**, with `commands` empty. Only the command line
  walks its own commands.

## Every refusal names a code, and `--json` prints each issue as one line on stderr

```text
$ doppeldata data plan ordres --out out/ --json; echo "exit $?"
{"error": {"code": "E-PROFILE-028", "message": "ordres is neither a profile file nor an example; name a file, or an example", "field": "PROFILE", "valid": ["claims", "eight-columns", "events-json", "orders", "orders-customers"], "hint": "doppeldata profile list names every example", "value": "ordres", "closest": ["orders"]}}
exit 2
```

Without `--json` the same issue prints as text:

```text
$ doppeldata data plan ordres --out out/
doppeldata: E-PROFILE-028: ordres is neither a profile file nor an example; name a file, or an example
  closest: orders
  valid:
    claims
    eight-columns
    events-json
    orders
    orders-customers
  hint: doppeldata profile list names every example
```

In Python, every refusal is a `DoppeldataError` holding the same issues. Branch on `.code`, and
compare it with the constant `sd.issue_codes` binds to that code. The SDK block at the top, saved
as `refusal.py`, prints:

```text
$ python refusal.py
{'op': 'hot_keys', 'formats': ['parquet', 'arrow', 'csv', 'tsv', 'json', 'ndjson'], 'fields': ['id', 'column', 'keys', 'fraction']}
E-PROFILE-028 2 profile ('orders',)
```

- **A refusal's field is the parameter as the caller spelled it.** The command line names
  `PROFILE`, and Python names the argument `profile`.
- **`.exit_code` is the status the command line ends on** for the same refusal.

## `data plan --json` traces each column to the profile fields it reads

```text
$ doppeldata data plan orders --scenario hot-key --out out/ --json | jq '.columns[] | select(.name == "customer_id")'
{
  "name": "customer_id",
  "physical_type": "int64",
  "sampler_kind": "numeric",
  "summary": "uniform between the extremes, at the profiled distinct ratio",
  "parameters": [
    {
      "name": "null_fraction",
      "field": "/columns/1/null_fraction",
      "value": 0.0
    },
    {
      "name": "distinct_ratio",
      "field": "/columns/1/distinct/ratio",
      "value": 0.2
    },
    {
      "name": "approx_values",
      "field": "/columns/1/distinct/approx_values",
      "value": 20000
    },
    {
      "name": "extremes",
      "field": "/columns/1/numeric/extremes",
      "value": {
        "high": 99999,
        "low": 10,
        "method": "digit_bounds"
      }
    },
    {
      "name": "integral",
      "field": "/columns/1/numeric/integral",
      "value": true
    }
  ],
  "null_fraction": 0.0,
  "origin": "overlaid",
  "reads": [],
  "ops": [
    "hot_keys:customer_id:0"
  ],
  "patched_fields": []
}
```

- **Each `field` is an RFC 6901 pointer into the profile**, so an agent edits the field that sets
  the value it wants changed.
- **`origin` is `profiled`, `derived`, `overlaid` or `pinned`.** `overlaid` means the scenario's
  patch sets the column's fields, or an op writes it. `ops` names each op by its id.
- **`reads` names the columns a derived column's values follow.**

## Progress is a log line on stderr, and a condition a caller may act on is a warning

```text
$ doppeldata profile create s3://your-bucket/orders-november/ --edges-from october.profile.yaml --aws-profile analytics --out november.profile.yaml -v
doppeldata: s3://your-bucket/orders-november/: 4 parquet files, 655,962 bytes
doppeldata: orders-november: profiled 20,000 rows and 8 columns with polars in 2.19 s
doppeldata: the earlier profile is not returned unchanged, because its generated.source_fingerprint is sha256:4930bca3c82280f05763b2a2b4b00c319b14e33988b2935b5db92339f62a9756, and this run writes sha256:82247caf702ca39a07e71993a36138a2749acba019df16cfba3a39ed0261b42f; each column it holds histogram edges or a value list for is counted against them
$ doppeldata profile create s3://your-bucket/orders-november/ --edges-from october.profile.yaml --aws-profile analytics --out november.profile.yaml -q
```

- **Warnings print by default.** `-v` adds each step, `-vv` each file and request, and `-q` leaves
  only failures.
- **Neither flag changes stdout.**

In Python, progress goes to the `doppeldata` logger, which writes nothing until a handler is added.
Each condition worth acting on is a subclass of `sd.ProfilingWarning`, which
`warnings.simplefilter` turns into an error. Saved as `warnings_as_errors.py`:

```python
import logging
import warnings

import doppeldata as sd

logging.basicConfig(format='%(name)s %(levelname)s %(message)s')
logging.getLogger('doppeldata').setLevel(logging.INFO)

warnings.simplefilter('error', sd.EarlierProfileNotReused)
try:
    sd.profile('s3://your-bucket/orders-november/', edges_from='october.profile.yaml', aws_profile='analytics')
except sd.EarlierProfileNotReused as warning:
    print('not reused:', warning.changed)
```

```text
$ python warnings_as_errors.py
doppeldata.profiling INFO s3://your-bucket/orders-november/: 4 parquet files, 655,962 bytes
not reused: its generated.source_fingerprint is sha256:4930bca3c82280f05763b2a2b4b00c319b14e33988b2935b5db92339f62a9756, and this run writes sha256:82247caf702ca39a07e71993a36138a2749acba019df16cfba3a39ed0261b42f
```

`october.profile.yaml` is the profile [Measure drift between two
profiles](measure-drift-between-profiles.md) makes of `s3://your-bucket/orders/`.

## A check prints Markdown without `--json`

`profile check`, `scenario check`, `data check` and `profile diff` print a GitHub Markdown report on
stdout, and the verdict on stderr. So `> report.md` keeps a report a pull request takes as it is.
[Check generated data](check-generated-data.md) shows one.

## Why

[Capabilities, the plan trace, reports and logging](../capabilities-plan-trace-reports-and-logging.md)
says where each list is read from, how the plan trace follows the engine's branch, and why progress
is a log record. [Issue codes](../issue-codes.md) says what each area and exit status covers, and
what an issue's keys hold.
