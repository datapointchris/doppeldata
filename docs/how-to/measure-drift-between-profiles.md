# Measure drift between two profiles

`profile diff A B` measures how far a later profile of a table moved from an earlier one, by the
metrics `data check` uses. It exits 0 when the two agree and 1 when they drifted, as `diff` does. A
profile holds no rows, so two months can be compared long after the first month's data is gone.

```bash
doppeldata profile create s3://your-bucket/orders/ --aws-profile analytics --out october.profile.yaml
doppeldata profile create s3://your-bucket/orders-november/ --aws-profile analytics --edges-from october.profile.yaml --out november.profile.yaml
doppeldata profile diff october.profile.yaml november.profile.yaml
doppeldata profile diff october.profile.yaml november.profile.yaml --thresholds thresholds.yaml --json
```

```python
import doppeldata as sd

november = sd.profile('s3://your-bucket/orders-november/', aws_profile='analytics', edges_from='october.profile.yaml')
report = sd.diff('october.profile.yaml', november)
report.drifted                          # True
[(m.metric, m.column, m.value) for m in report.failures().metrics]
# [('tvd', 'status', 0.231124), ('null_fraction_delta', 'ship_zip', 0.04895)]
```

## Profile the later data against the earlier profile's edges

`--edges-from A` counts the later data against A's histogram edges and value lists, so each bin
and value compares one for one. Its overflow counts are what `boundary_adherence` reads.

```text
$ doppeldata profile create s3://your-bucket/orders-november/ --aws-profile analytics --edges-from october.profile.yaml --out november.profile.yaml
doppeldata: the earlier profile is not returned unchanged, because its generated.source_fingerprint is sha256:4930bca3c82280f05763b2a2b4b00c319b14e33988b2935b5db92339f62a9756, and this run writes sha256:82247caf702ca39a07e71993a36138a2749acba019df16cfba3a39ed0261b42f; each column it holds histogram edges or a value list for is counted against them
```

- **The notice says the source changed.** Run against the files A was made from, `--edges-from`
  hands A back unchanged, and says so:

  ```text
  $ doppeldata profile create s3://your-bucket/orders/ --aws-profile analytics --edges-from october.profile.yaml --out same.profile.yaml
  doppeldata: october.profile.yaml was made from these files with these settings, so it is returned unchanged
  ```

- **A later profile made without `--edges-from` leaves `boundary_adherence` unmeasured**, and the
  diff names the fix:

  ```text
  $ doppeldata profile diff october.profile.yaml fresh.profile.yaml
  doppeldata: 3 metrics unmeasured and passed, because fresh.profile.yaml holds nothing to compare them with; a later profile made against october.profile.yaml's edges carries the overflow boundary_adherence reads
  # doppeldata profiles agree
  ...
  | boundary_adherence | amount | unmeasured: the other profile was not counted against the source's histogram edges | ≥ 0.99 |  |  | yes |
  ...
  ```

## A drifted month exits 1, its failed rows first

A month at the same distribution agrees:

```text
$ doppeldata profile diff october.profile.yaml steady.profile.yaml
# doppeldata profiles agree

`steady.profile.yaml` (sha256:a24881e5a15abdd76c6b9de4714a5132c46fe4162f057d4d71724b31a2cb3aa7) against `october.profile.yaml` (sha256:7a3c827a291ca3aade44e80b5b318a0401c8deafa82ccb03087d21367e3998b4).

0 of 31 metrics failed.
...
```

A month where more orders were delivered and `ship_zip` started arriving empty drifts:

```text
$ doppeldata profile diff october.profile.yaml november.profile.yaml
# doppeldata profiles drifted

`november.profile.yaml` (sha256:ce383ca6b9dfa5c2eb7ec0324a500adf75b5577102e46363df1553a78741701c) against `october.profile.yaml` (sha256:7a3c827a291ca3aade44e80b5b318a0401c8deafa82ccb03087d21367e3998b4).

2 of 31 metrics failed.

| Metric | Column | Value | Threshold | Expected | Observed | Passed |
| --- | --- | --- | --- | --- | --- | --- |
| tvd | status | 0.231124 | ≤ 0.05 |  |  | **no** |
| null_fraction_delta | ship_zip | 0.04895 | ≤ 0.01 | 0 | 0.04895 | **no** |
| ks | customer_id | 0.0044 | ≤ 0.05 |  |  | yes |
...

doppeldata: E-PROFILE-027: november.profile.yaml drifted from october.profile.yaml on 2 of 31 metrics and 0 of 0 field changes: tvd on status, null_fraction_delta on ship_zip (0 to 0.04895)
```

- **Exit 1 means drift and nothing else.** A diff that could not finish exits 3, and a missing or
  unreadable profile exits 2.
- **A diff fails on a changed field too**, such as a column added or retyped, or a CSV dialect
  setting that moved.
- **A thresholds file allows a move you expect.** With `defaults: {tvd: 0.3}` and
  `columns: {ship_zip: {null_fraction_delta: 0.05}}`, November agrees and exits 0:

  ```text
  $ doppeldata profile diff october.profile.yaml november.profile.yaml --thresholds thresholds.yaml --json | jq -c '{drifted, keys: keys, failed: [.metrics[] | select(.passed | not)]}'
  {"drifted":false,"keys":["a","b","changes","drifted","metrics"],"failed":[]}
  ```

- **`doppeldata schemas show diff` prints the report's JSON Schema.**
- **`profile diff --help` lists each metric with its default threshold.**

## Why

[How generated data is checked against its profile, and two profiles diffed](../checking-generated-data-and-profile-drift.md)
says what each metric measures, why a diff passes a metric it cannot measure where a check fails
it, and which fields a diff compares.
