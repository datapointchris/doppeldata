# Bend data with a scenario

A scenario bends a profile on purpose, for a test that needs a hot key, late rows, nulls, a shifted
category mix or a schema change. It is a file of three parts: a `patch` onto the profile,
`perturbations` applied in order, and the `expect`ations it promises. The profile stays as it is.

```bash
doppeldata scenario list
doppeldata scenario show hot-key
doppeldata scenario check promo-week.scenario.yaml orders
doppeldata data generate orders --seed 7 --scenario promo-week.scenario.yaml --out promo/
doppeldata data check orders promo/ --scenario promo-week.scenario.yaml
```

```python
import doppeldata as sd

promo = sd.Scenario.load('promo-week.scenario.yaml')
sd.write('orders', 'promo/', seed=7, scenario=promo)
report = sd.check('orders', 'promo/', scenario=promo)
report.passed, report.verified                     # (True, True)

three_keys = sd.Scenario.example('hot-key', keys=3)
table = sd.generate('orders', rows=10_000, seed=7, scenario=[sd.Scenario.example('hot-key'), sd.Scenario.example('late-data')])
```

## `scenario list` names every op, every metric an expectation measures, and the examples

`doppeldata scenario list` prints each op with what it does, then the metrics and the example
scenarios. `doppeldata scenario show OP` prints an op's parameters, and `scenario show NAME` an
example:

```text
$ doppeldata scenario show hot-key
# Written for the orders example profile. Scenario.example('hot-key', column=...) points it at another key.
scenario: hot-key
description: Five customers place 60% of the orders, so a join or group-by on customer_id lands most rows in one task.
perturbations:
  - {op: hot_keys, column: customer_id, keys: 5, fraction: 0.6}
expect:
  - {metric: hot_keys.fraction, column: customer_id, at_least: 0.55}
```

`--scenario hot-key` takes an example by name. The examples are written for the `orders` example
profile.

## A scenario file patches the profile, perturbs the rows, and declares what it promises

```yaml
scenario: promo-week
description: A promotion week. Half the orders are delivered, big orders lose their email more often, and amounts run higher.
patch:
  dataset: {rows: 200000}
  columns:
    amount:
      numeric:
        quantiles: {p01: 2.0, p50: 60.0, p99: 900.0}
perturbations:
  - {op: categorical_shift, column: status, to_fractions: {delivered: 0.5}}
  - {op: missingness, column: customer_email, mechanism: MAR, depends_on: amount, fraction: 0.1}
expect:
  - {metric: missingness.fraction, column: customer_email, at_least: 0.09, at_most: 0.13}
```

- **`patch` is a JSON merge patch onto the profile**, with `columns` keyed by column name, so it
  names only the columns it changes. A column patched to null is removed, and a column the profile
  lacks is refused as `E-SCENARIO-101`.
- **`perturbations` change what a profile cannot state**, in list order. Each op's random stream
  is keyed by its `id`, never its place in the list.
- **`expect` lists the metrics `data check` holds the data to.** It measures nothing undeclared.
- **`doppeldata schemas show scenario` prints the JSON Schema** an editor completes the keys from.

## `scenario check` refuses a scenario before anything is generated

It validates the scenario, applies its patch, and plans every perturbation and the file layout as
`data generate` would. It exits 0 when the scenario holds and 3 when it does not, at each fault's
pointer. A first draft of `promo-week` put `rows` at the top of `patch`:

```text
$ doppeldata scenario check promo-week.scenario.yaml orders
# doppeldata scenario check failed

promo-week.scenario.yaml does not hold for profile `orders`.

| Field | Code | Issue | Hint |
| --- | --- | --- | --- |
| `/patch/rows` | E-PROFILE-006 | 'rows' is not a field here | doppeldata schemas show profile prints every field |
doppeldata: E-SCENARIO-019: promo-week.scenario.yaml does not hold for profile orders in 1 place
```

`rows` belongs under `dataset`. With it there:

```text
$ doppeldata scenario check promo-week.scenario.yaml orders
# doppeldata scenario check passed

promo-week.scenario.yaml holds for profile `orders`: 2 perturbations and 1 expectation, scenario sha256:b93b806f5b350f146b62592bc92d936149e1d8e3f985999b3090055745313905.
```

`--json` prints `passed` and each issue's `code` and `field`. `data plan` and `data generate` refuse
the same scenario with exit 2.

`data plan --scenario` names the fields the patch sets and the op writing each column:

```text
$ doppeldata data plan orders --seed 7 --scenario promo-week.scenario.yaml --out promo/
...
scenario   promo-week sha256:b93b806f5b350f146b62592bc92d936149e1d8e3f985999b3090055745313905
...
amount          decimal128(12, 2)      quantiles with fitted lognorm tails, 3 fields patched
status          string                 5 top values, written by categorical_shift:status:0
customer_email  string                 the email provider, at the profiled distinct ratio, 0.02 null, written by missingness:customer_email:0
```

## `data check --scenario` skips what a perturbation moves, and measures each expectation

```text
$ doppeldata data check orders promo/ --scenario promo-week.scenario.yaml
# doppeldata check passed

`promo/`, 200,000 rows, against profile `orders` (sha256:95abee1ddc215b0b2aa73077d2e02925d48b8acfc2461ded17ba7cddbeec16e2) under scenario `promo-week` (sha256:b93b806f5b350f146b62592bc92d936149e1d8e3f985999b3090055745313905).

0 of 20 metrics failed.

| Metric | Column | Value | Threshold | Expected | Observed | Passed |
| --- | --- | --- | --- | --- | --- | --- |
| tvd | status | skipped by categorical_shift:status:0 | ≤ 0.05 |  |  | yes |
...
0 of 1 expectations failed.
| Expectation | Column | Value | Bounds | Passed |
| missingness.fraction | customer_email | 0.11779 | ≥ 0.09 and ≤ 0.13 | yes |
```

- **The data is measured against the patched profile**, so `amount` is held to the patched
  quantiles.
- **A metric a perturbation moves passes, naming the op's id.**
- **`report.verified` is false when an expectation misses its bounds**, and `data check` exits 3.
- **Data written under a scenario is checked under it.** Its manifest names the scenario, so
  checking it with none, or with another, is refused:

  ```text
  $ doppeldata data check orders promo/
  doppeldata: E-SCENARIO-015: promo/ cannot be checked with no scenario:
    /: promo/ was written under scenario promo-week (sha256:b93b806f5b350f146b62592bc92d936149e1d8e3f985999b3090055745313905); check it under that scenario [E-SCENARIO-015]
    hint: check it under the scenario its manifest names
  ```

## Several scenarios compose in order

`--scenario hot-key --scenario late-data`, or a list in Python, merges the patches left to right
and runs every perturbation and expectation. The manifest names the composition and each member:

```text
$ jq -c '.scenario | {name, path, composed_of: [.composed_of[] | {name, digest}]}' both/_manifest.json
{"name":"hot-key+late-data","path":null,"composed_of":[{"name":"hot-key","digest":"sha256:84079dcd4a5a5bcea2805efb529f1cf7c76141b2efb8f9fbbf295b99e587aa83"},{"name":"late-data","digest":"sha256:57c891fb8a72704ebe48eb414741ef351435288432ef0fa8fa99fac5f9efe9d4"}]}
```

`Scenario.example(name, **parameters)` replaces the example's parameters, such as `keys=3`. A
change to any value but `column` drops the example's expectations, which were written for its own
values.

## A bundle's scenario names its table, and may patch a relationship

```yaml
scenario: one-busy-customer
table: orders
patch:
  relationships:
    customer_id:
      fanout:
        top_fractions: {top1: 0.05, top10: 0.1, top100: 0.3}
```

```text
$ doppeldata data generate orders-customers --seed 5 --scenario one-busy-customer.scenario.yaml --out busy/
$ doppeldata data check orders-customers busy/ --scenario one-busy-customer.scenario.yaml
...
| top_fraction_delta | orders.customer_id → customers.customer_id | 8.20642e-07 | ≤ 0.001 | 0.05 | 0.0500008 | yes |
```

- **`table` names the table the patch and every op apply to.** A bundle refuses a scenario naming
  no table, or one it lacks, as `E-SCENARIO-108`. Several scenarios bend several tables, one each.
- **`patch.relationships` is keyed by the relationship's `from` columns, without their table**,
  joined by `,` for a composite key. Any other key is refused as `E-SCENARIO-109`, naming the keys
  the table takes.
- **`hot_keys` on a foreign key puts its rows on the parent's busiest keys**, and each inherited
  column copies the hot parent's.

[Write broken files](write-broken-files.md) and [Write a change stream](write-a-cdc-stream.md)
cover the two ops that write files a reader must survive.

## Why

[Scenario overlays](../scenario-overlays.md) says why a patch is keyed by column name, how each
perturbation keeps its own random stream, and why a check certifies only what a scenario declares.
