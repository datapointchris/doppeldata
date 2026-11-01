# Read and edit how columns depend on each other

`profile create` finds how a table's columns depend on each other, and `data generate` draws them
that way with no flag. Each dependence is a field of the profile, so you read it, edit it, and
check what the edit generates.

```bash
doppeldata profile create checkouts.parquet --out checkouts.profile.yaml
doppeldata data plan checkouts.profile.yaml --rows 1000000 --out generated/
doppeldata data generate checkouts.profile.yaml --rows 1000000 --seed 7 --out generated/
doppeldata data check checkouts.profile.yaml generated/
```

```python
import doppeldata as sd

profile = sd.Profile.load('checkouts.profile.yaml')
pair = next(c for c in profile.document['dataset']['correlations'] if (c['a'], c['b']) == ('quantity', 'unit_price'))
pair['value'] = -0.7
profile.save('checkouts.profile.yaml')      # comments and key order kept
for column in sd.plan(profile, 'generated/', rows=1_000_000, seed=7).columns:
    print(column.name, column.summary)
```

## `data plan` names the dependence each column is drawn through

```text
$ doppeldata data plan checkouts.profile.yaml --rows 1000000 --out generated/
...
order_id     int64        sequence from 1 by 1
region       string       3 top values
channel      string       its conditional table given region, else 3 top values
quantity     int64        10 top values, ranked with unit_price in a Gaussian copula
unit_price   double       quantiles with a linear lower tail and a fitted loguniform upper tail, ranked with quantity in a Gaussian copula
order_date   date32[day]  between the extremes, weighted by month, weekday
ship_date    date32[day]  order_date plus a lag at 9 quantiles
total        double       the expression unit_price * quantity
region_code  string       looked up from region, else 3 top values
```

`data plan --json` traces each column to the profile fields it reads.

## Each dependence is a field under `dataset`, or a column's `derived`

```text
$ yq '.dataset | del(.rows, .duplicate_row_fraction, .keys)' checkouts.profile.yaml
correlations:
  - {a: region, b: channel, method: cramers_v, value: 0.7131}
  - {a: region, b: region_code, method: cramers_v, value: 1.0}
  - {a: channel, b: region_code, method: cramers_v, value: 0.7131}
  - {a: quantity, b: unit_price, method: spearman, value: -0.4}
  - {a: unit_price, b: total, method: spearman, value: 0.7555}
  - {a: order_date, b: ship_date, method: spearman, value: 0.9996}
correlation_ratios:
  - {a: quantity, b: unit_price, value: 0.4026}
conditionals:
  - given: region
    column: channel
    fractions:
      north: {web: 0.9004, store: 0.0504, phone: 0.04922}
      south: {web: 0.09868, store: 0.801, phone: 0.1004}
      west: {web: 0.1994, store: 0.09933, phone: 0.7013}
dependencies:
  - kind: functional
    lhs: [region]
    rhs: region_code
    violation_fraction: 0.0
    table: {north: N, south: S, west: W}
```

| Kind | Field | Draws |
| --- | --- | --- |
| Spearman pair | `dataset.correlations`, `method: spearman` | both columns through one Gaussian copula, each keeping its own distribution |
| Conditional table | `dataset.conditionals` | a categorical column from the row its parent's value picks, `other_fractions` covering the rest |
| Quantiles or rates given a category | `dataset.conditional_quantiles`, `dataset.conditional_fractions` | a numeric column's curve, zeros or nulls within each value of its parent |
| Lookup | `dataset.dependencies`, `kind: functional` | the value `table` gives the parent's value, else the column's own statistics |
| Lag | a column's `derived`, `kind: timedelta` | `base` plus a lag drawn from its quantiles, between `min` and `max` |
| Expression | a column's `derived`, `kind: expression` | `+ - * /`, parentheses and `round()` over other columns |

```text
$ yq -o=json -I0 '.columns[] | select(.derived) | {"name": .name, "derived": .derived}' checkouts.profile.yaml
{"name":"ship_date","derived":{"kind":"timedelta","base":"order_date","lag":{"quantiles":{"p001":"0d","p01":"0d","p05":"0d","p25":"1d","p50":"2d","p75":"6d","p95":"9d","p99":"10d","p999":"14d"},"min":"0d","max":"14d"}}}
{"name":"total","derived":{"kind":"expression","expr":"unit_price * quantity","round":2,"violation_fraction":0.0}}
```

- **Cramér's V, a correlation ratio, and a Spearman pair with a date describe the table.** No
  draw reads them.
- **`violation_fraction` is the share of rows drawn from the column's own statistics instead.**
  At 0 every row holds its lookup or expression.
- **A lag's quantiles are a whole number and a unit**: `ms`, `s`, `m`, `h`, `d` or `w`.
- **Quote a lookup key or value YAML would read as another type.** A bare `N` is `false` in YAML 1.1.

## Edit a field, then plan it again

Every field is read as written. Changing the pair's `value: -0.4` to `-0.7` draws `quantity` and
`unit_price` at the new Spearman value. No other column's draw moves, and `total`, computed from
both, follows them.

A dependence the plan cannot draw is refused, exit 2, at the field's pointer:

```text
$ doppeldata data plan broken.profile.yaml --rows 1000000 --out generated/
doppeldata: E-PROFILE-113: broken.profile.yaml cannot be generated:
  /columns/7/derived/expr: the table holds no column "qty" [E-PROFILE-113]
$ doppeldata data plan cycle.profile.yaml --rows 1000000 --out generated/
doppeldata: E-PROFILE-114: cycle.profile.yaml cannot be generated:
  /columns/7/derived/expr: "total" would be drawn from "quantity", which is drawn from it [E-PROFILE-114]
```

`E-PROFILE-114` also names an expression that does not parse, a lag on a column that is not a date
or timestamp, and a lookup value its column's type cannot hold. An entry the plan can read but not
draw, such as a second table for one column, is set aside, and `data plan` names it beside its
column.

## `data check` measures each copula group and each conditional table

```text
$ doppeldata data check checkouts.profile.yaml generated/
# doppeldata check passed

`generated/`, 1,000,000 rows, against profile `checkouts` (sha256:…).

0 of 38 metrics failed.
...
| ks | total | 0.001164 | ≤ 0.05 |  |  | yes |
| correlation_max_abs_diff | quantity, unit_price | 0.0022 | ≤ 0.05 | -0.4 | -0.4022 | yes |
| conditional_kl | channel, region | 0.00104539 | ≤ 0.05 |  |  | yes |
```

The check holds every column to the statistics the profile states. So after the edit to -0.7, the
pair holds its new value, and `total`, computed from both, misses quantiles still profiled at -0.4:

```text
doppeldata: E-DATA-014: the data missed its profile on 1 of 38 metrics and 0 of 0 field changes: ks on total
```

```text
| ks | total | 0.086973 | ≤ 0.05 |  |  | **no** |
| correlation_max_abs_diff | quantity, unit_price | 0.0032 | ≤ 0.05 | -0.7 | -0.7032 | yes |
```

Edit the statistics of a column computed from the edited ones too, or read its failure as the
edit's effect.

## Why

[Generating columns that depend on each other](../generating-dependent-columns.md) says how each
kind is drawn, the order the planner draws the columns in, and what a `cdc` update or a strict
policy does to a dependence. [How the profiler computes a table's statistics](../profiler-statistics-and-readers.md)
says when the profiler writes each kind.
