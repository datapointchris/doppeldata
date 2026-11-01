# Check generated data

`data check` profiles the generated data against the profile it came from, and measures the two
profiles against each other. It reads no real row unless `--against` names the real data. It exits
0 when every metric passes and 3 when one does not.

```bash
doppeldata data generate orders.profile.yaml --seed 7 --out generated/
doppeldata data check orders.profile.yaml generated/
doppeldata data check orders.profile.yaml generated/ --json | jq -c '.metrics[] | select(.passed | not)'
doppeldata data check orders.profile.yaml generated/ --thresholds thresholds.yaml
doppeldata data check orders.profile.yaml generated/ --against s3://your-bucket/orders/ --aws-profile analytics
doppeldata data check --help        # every metric, its summary and its default threshold
```

```python
import doppeldata as sd

report = sd.check('orders.profile.yaml', 'generated/')
report.passed, report.verified          # (True, True)
strict = sd.check('orders.profile.yaml', 'generated/', thresholds='thresholds.yaml')
[(m.metric, m.column, m.value) for m in strict.failures().metrics]
# [('ks', 'amount', 0.00525), ('null_fraction_delta', 'customer_email', 0.000562097)]
scored = sd.check('orders.profile.yaml', 'generated/', against='s3://your-bucket/orders/', aws_profile='analytics')
print(scored.to_markdown())
```

`orders.profile.yaml` here is the profile [Profile a dataset](profile-a-dataset.md) made of
`data/orders/`, 20,000 rows. `s3://your-bucket/orders/` holds the same rows in four files.

## A check prints one row per metric and column

```text
$ doppeldata data check orders.profile.yaml generated/
# doppeldata check passed

`generated/`, 20,000 rows, against profile `orders` (sha256:…).

0 of 31 metrics failed.

| Metric | Column | Value | Threshold | Expected | Observed | Passed |
| --- | --- | --- | --- | --- | --- | --- |
| ks | customer_id | 0.00475 | ≤ 0.05 |  |  | yes |
| ks | amount | 0.00525 | ≤ 0.05 |  |  | yes |
| ks | quantity | 0.00395 | ≤ 0.05 |  |  | yes |
| tvd | quantity | 0 | ≤ 0.05 |  |  | yes |
| tvd | status | 0 | ≤ 0.05 |  |  | yes |
| tvd | ordered_at | 0 | ≤ 0.05 |  |  | yes |
...
| null_fraction_delta | customer_email | 0.000562097 | ≤ 0.01 | 0.01895 | 0.0206 | yes |
...
| distinct_ratio_delta | customer_id | 0.0037805 | ≤ 0.1 | 0.6746 | 0.6772 | yes |
...
| boundary_adherence | amount | 1 | ≥ 0.99 |  |  | yes |
...
| files_delta |  | 0 | ≤ 0.1 | 1 | 1 | yes |
| rows_per_file_delta |  | 0 | ≤ 0.1 | 20000 | 20000 | yes |
```

- **`Expected` and `Observed` are the two profiles' values** where a metric compares one value.
- **`data check --help` lists every metric with its summary and default threshold.** A metric
  appears where the profile states what it compares. A profile with a Spearman pair gets
  `correlation_max_abs_diff`, as [Read and edit how columns depend on each
  other](edit-dependent-columns.md) shows.
- **A check at another row count measures against the range the profile allows at that count.**
  The same profile generated at 200,000 rows passes too, with `customer_id`'s distinct ratio at
  0.4321 against the profile's 0.6746:

  ```text
  $ doppeldata data check orders.profile.yaml big/
  ...
  `big/`, 200,000 rows, against profile `orders` (sha256:…).

  0 of 31 metrics failed.
  ...
  | distinct_ratio_delta | customer_id | 0 | ≤ 0.1 | 0.6746 | 0.4321 | yes |
  ...
  | files_delta |  | 0 | ≤ 0.1 | 10 | 10 | yes |
  ```

## A thresholds file moves a metric's bound, for every column or for one

```yaml
defaults: {ks: 0.005}
columns:
  customer_email: {null_fraction_delta: 0.0005}
```

```text
$ doppeldata data check orders.profile.yaml generated/ --thresholds thresholds.yaml
# doppeldata check failed

`generated/`, 20,000 rows, against profile `orders` (sha256:…).

2 of 31 metrics failed.
...
| ks | amount | 0.00525 | ≤ 0.005 |  |  | **no** |
| null_fraction_delta | customer_email | 0.000562097 | ≤ 0.0005 | 0.01895 | 0.0206 | **no** |
...
doppeldata: E-DATA-014: the data missed its profile on 2 of 31 metrics and 0 of 0 field changes: ks on amount, null_fraction_delta on customer_email (0.01895 to 0.0206)
```

- **A failed check exits 3**, and its last line names each miss.
- **A metric measuring a pair or a relationship is set under `defaults` alone.** `data check --help`
  names those.
- **`--json` prints the report as one object**, with `passed`, `verified`, `profile`, `data`,
  `metrics`, `changes`, `scenario`, `expectations` and `against`. Its refusal prints on stderr as a
  line of JSON:

  ```text
  $ doppeldata data check orders.profile.yaml generated/ --thresholds thresholds.yaml --json | jq -c '.metrics[] | select(.passed | not) | {metric, column, value, threshold}'
  {"error": {"code": "E-DATA-014", "message": "the data missed its profile on 2 of 31 metrics and 0 of 0 field changes: ks on amount, null_fraction_delta on customer_email (0.01895 to 0.0206)", "field": null, "valid": [], "hint": null, "value": null, "closest": []}}
  {"metric":"ks","column":"amount","value":0.00525,"threshold":0.005}
  {"metric":"null_fraction_delta","column":"customer_email","value":0.000562097,"threshold":0.0005}
  ```

- **`doppeldata schemas show check` prints the report's JSON Schema.**

## `--against` scores the data against the real data by SDMetrics' definitions

```text
$ doppeldata data check orders.profile.yaml generated/ --against s3://your-bucket/orders/ --aws-profile analytics
# doppeldata check passed

`generated/`, 20,000 rows, against profile `orders` (sha256:…).

Scored against the real data at `s3://your-bucket/orders/`, 20,000 rows, on a seeded sample of 20,000 rows from each side.

0 of 31 metrics failed.
...
Scores that decide nothing, because no thresholds file sets their threshold: 20. Missing the threshold shown: 0.

| Metric | Column | Value | Threshold | Expected | Observed | Meets it |
| --- | --- | --- | --- | --- | --- | --- |
| KSComplement | customer_id | 0.9945 | ≥ 0.8 |  |  | yes |
| KSComplement | amount | 0.99405 | ≥ 0.8 |  |  | yes |
...
| TVComplement | status | 0.99555 | ≥ 0.9 |  |  | yes |
| CategoryCoverage | status | 1 | ≥ 0.9 |  |  | yes |
| CorrelationSimilarity | customer_id, amount | 0.993311 | ≥ 0.8 | 0.000473419 | -0.0129048 | yes |
...
| ContingencySimilarity | customer_id, status | 0.9693 | ≥ 0.8 |  |  | yes |
...
```

- **A score decides nothing until a thresholds file sets it.** It then fails the check when
  missed, as a metric does. A pair score is set under `defaults` alone. With
  `defaults: {KSComplement: 0.995}`:

  ```text
  $ doppeldata data check orders.profile.yaml generated/ --against s3://your-bucket/orders/ --aws-profile analytics --thresholds scores.yaml
  # doppeldata check failed
  ...
  3 of 35 metrics failed.
  ...
  | KSComplement | customer_id | 0.9945 | ≥ 0.995 |  |  | **no** |
  | KSComplement | amount | 0.99405 | ≥ 0.995 |  |  | **no** |
  | KSComplement | ordered_at | 0.99215 | ≥ 0.995 |  |  | **no** |
  ...
  doppeldata: E-DATA-014: the data missed its profile on 3 of 35 metrics and 0 of 0 field changes: KSComplement on customer_id, KSComplement on amount, KSComplement on ordered_at
  ```

- **`--against-sample N` scores a seeded sample of N rows from each side.** `data check --help`
  prints its default.
- **`--html FILE` writes the two samples side by side with sweetviz**, which doppeldata does not
  install. Install it beside doppeldata first. The page shows the real data's extremes and
  categories, so it is not for sharing.

  ```bash
  doppeldata data check orders.profile.yaml generated/ --against s3://your-bucket/orders/ --aws-profile analytics --against-sample 5000 --html compare.html
  ```

- **The report holds scores, never a real row.**

## A bundle, a scenario or a broken file is checked the same way

- [Generate related tables](generate-related-tables.md) checks a bundle, table by table and
  relationship by relationship.
- [Bend data with a scenario](bend-data-with-a-scenario.md) checks data written under a scenario,
  skipping what a perturbation moves and measuring each expectation.
- [Write broken files](write-broken-files.md) and [Write a change stream](write-a-cdc-stream.md)
  check data written with faults in it.

## Why

[How generated data is checked against its profile](../checking-generated-data-and-profile-drift.md)
says what each metric measures, why a check compares two profiles rather than reading rows, and
what `--against` scores and leaves out.
