# Generating columns that depend on each other

A profile states how its columns depend on each other, and the generator draws them that way.
Without it every column is right on its own, and every join, group-by and filter selectivity is
wrong. Each dependence is a few parameters, so it costs a few operations a row.

## A profile generates every dependence it states, with no flag

`profile create` writes the dependence it finds, and `data generate` draws what the profile
states. [Read and edit how columns depend on each other](how-to/edit-dependent-columns.md) gives
the commands. `data plan` names how each column is drawn. The committed fixture ends in columns of checkouts
that state each kind, and these are its last lines:

```bash
doppeldata data plan crates/core/tests/every-column-kind.profile.yaml --rows 1000000 --out generated/
```

```text
copula_amount  double             quantiles with linear tails, ranked with copula_items, copula_weight in a Gaussian copula
copula_items   int64              3 top values, the rest from quantiles with linear tails, ranked with copula_amount, copula_weight in a Gaussian copula
copula_weight  double             quantiles with linear tails, ranked with copula_amount, copula_items in a Gaussian copula, 0.05 null
region         string             3 top values
channel        string             its conditional table given region, else 3 top values, 0.02 null
paid_amount    double             quantiles with linear tails, its quantiles given channel and zero rate given channel, its null rate given channel, 0.03 null
region_code    string             looked up from region, else 3 top values
order_date     date32[day]        uniform between the extremes
ship_date      date32[day]        order_date plus a lag at 3 quantiles
shipped_at     timestamp[s]       timestamp_uniform plus a lag at 2 quantiles
quantity       int64              quantiles with linear tails
unit_price     decimal128(10, 2)  quantiles with linear tails
total          decimal128(12, 2)  the expression round(quantity * unit_price, 2)
```

Each kind is a field a reviewer reads and edits like any other. The sections below show each one
from that profile. [profiler-statistics-and-readers.md](profiler-statistics-and-readers.md) says
how the profiler finds each.

## Spearman correlations between numeric columns draw them through one Gaussian copula

```yaml
dataset:
  correlations:
    - {a: copula_amount, b: copula_items, method: spearman, value: 0.7}
    - {a: copula_amount, b: copula_weight, method: spearman, value: 0.4}
    - {a: copula_items, b: copula_weight, method: spearman, value: 0.3}
```

- **Columns joined by Spearman pairs form one copula group.** Each row draws one normal per
  member, correlates them through the group's Cholesky factor, and turns each into a uniform
  level. Each column reads its level through its own inverse CDF, so every marginal stays as
  profiled.
- **A pair the group does not state is filled as independent given the stated ones.** That is the
  maximum-determinant completion of Dempster's covariance selection, a Gaussian graphical model
  whose missing edges are conditional independences. A profile states only the pairs above its
  correlation floor, so a large group leaves many unstated.
- **A member is a numeric column drawn from its quantiles or fit.** A sequence, a looked-up or
  derived column, a date and a timestamp take no part. Their Spearman values describe the table
  and are not drawn. So do Cramér's V values and correlation ratios.
- **Editing a value moves that group's values and no other column's.** The group draws from one
  stream keyed by its sorted column names, as
  [determinism-contract.md](determinism-contract.md) states.
- **A Spearman value becomes the Pearson `2·sin(πρ/6)` between the normals.** A column many of whose
  rows hold one value, such as a count of items, ties its ranks, and lowers what that Pearson
  gives. Such a pair takes the Pearson whose values hold `ρ` with their ties, found by bisection
  over each column's rank scores, as NORTA (Cario and Nelson, 1997) calibrates one.
- **A pair whose ties hold its value under no Pearson takes the Pearson nearest it.** Two columns
  each holding one value on most rows reach a Spearman value near their comonotone bound only as
  the Pearson nears 1. The plan names such a pair: `its ties holding no copula at its Spearman
  with congestion_surcharge`.
- **Where no matrix holds every stated pair, the group holds the valid one nearest them.**
  Alternating projections between the positive definite matrices and those holding each stated
  Pearson settle on it, as Higham (2002) finds a nearest correlation matrix, and leave each
  unstated pair free. The plan says `its correlations repaired to the nearest valid matrix`. A
  column that is the sum of others, such as a fare total, gives such a group.

`cargo test --test dependent_columns`, run from `crates/`, draws rows from the fixture and holds
each stated Spearman value within the tolerance it sets. [performance.md](performance.md) gives
the copula's speed.

*Rejected:* the plain `2·sin(πρ/6)` for a tied column. Ties lower the Spearman value a Pearson
holds, so each tied pair drew under its stated value.

*Rejected:* filling an unstated pair with 0, and clipping the eigenvalues where that breaks the
matrix. Clipping shrinks every pair toward 0, the stated ones too.

## A conditional table draws a categorical column from the row its parent's value picks

```yaml
  conditionals:
    - given: region
      column: channel
      fractions:
        north: {web: 0.7, store: 0.2, phone: 0.1}
        south: {web: 0.3, store: 0.5, phone: 0.2}
      other_fractions: {web: 0.4, store: 0.3, phone: 0.3}
```

- **Each row of `fractions` sums to 1 at most, and `other_fractions` covers every value of `given`
  the table does not list.** `profile check` refuses a row over 1 as `E-PROFILE-015`. A null parent
  draws from the column's own values.
- **The share a row leaves short of 1 is the rows of child values it does not state.** The
  profiler leaves out a cell under `k_min` rows. Each child value the column lists takes as much of
  that share as no row states of it, each row weighted by its given value's share. That is one step
  of iterative proportional fitting toward the child's own shares, so its marginal holds.
- **The share of values the column lists none of draws only such values.** It draws as the
  column's own draw does when its listed draw misses. A string comes from the source below its
  `top_values`, and a number from the CDF with its listed values cut out. That draw has its own
  streams. The value at a distinct index is the one the column's own draw holds there, so the two
  share their distinct values.
- **Without `other_fractions`, a given value with no row draws what the rows leave of the child's
  own shares.** Each child value, and the share it lists none of, takes its own share less what
  every row draws of it, each row weighted by its given value's share. A row's draw is its cells and
  the rest spread over it. So the marginal holds where the profiler dropped the row of the values
  `given` does not list. A share left under 1% of its own takes none. Where nothing is left, or
  either column lists no values, such a row draws from the column's own values.
- **That draw states nothing the profile does not.** It is computed from the profile's rates
  alone. For a child value some row states, what is left is its rows outside the kept cells, which
  the writer holds at 0 or at least `k_min`. For a child value no row states, it is only what the
  spread estimates.
- **A value is a key as its text**: a string as itself, an integer in decimal, a boolean as
  `true` or `false`.
- **A column takes one parent.** Where two entries name the same child, the first is drawn and the
  second set aside.

The same test holds each share of that table within its tolerance, and the child's own shares,
its rest's among them, where a table states no `other_fractions`.

*Rejected:* drawing the share of values the column lists none of from the column's own draw. That
draw lands on a listed value at the listed values' share. So the values the column lists none of
were drawn too seldom, and their distinct count fell.

*Rejected:* drawing a given value with no row from the column's own values. Where the profiler
dropped the row of a rare rest, the child values that rest held were drawn too seldom.

*Rejected:* spreading a given value with no row over the shares no row states, as a row's rest is
spread. The rows' rests already draw most of those shares, so the values only the rowless given
values hold were still drawn too seldom.

*Rejected:* scaling a row's stated cells up to 1. A row missing its rare values then draws its
common ones too often, and the child's marginal drifts.

*Rejected:* drawing the whole rest from the column's own values. Those are mostly values other rows
already state, so the values only this row holds were drawn too seldom.

*Rejected:* spreading the rest over every value with any unstated share. Rounding a rate to 4
digits leaves a sliver of most values unstated. Drawing those slivers scattered a few rows of a
value into rows that hold none, too few for a profile to state. So a value whose unstated share is
under 1% of its own takes none.

## A numeric column given a category draws through that category's quantiles, zeros and nulls

```yaml
  conditional_quantiles:
    - given: channel
      column: paid_amount
      quantiles:
        web: {p25: 10.0, p50: 30.0, p75: 70.0}
        store: {p25: 2.0, p50: 8.0, p75: 20.0}
      other: {p25: 5.0, p50: 15.0, p75: 40.0}
  conditional_fractions:
    - given: channel
      column: paid_amount
      zero_fraction: {web: 0.02, store: 0.2}
      null_fraction: {phone: 0.15}
      other: {zero_fraction: 0.1, null_fraction: 0.01}
```

- **Each row of `quantiles` warps the column's own levels for rows holding that value.** Each
  stated quantile maps into the levels that draw its value. Between two of them the rows take the
  column's own values at the column's own spacing. So a category keeps the column's listed values,
  extremes and tails. A row's quantiles count its zeros, the way the column's own do. A value with
  no row, and a null, draw from `other`, else from the column's own levels.
- **A zero rate places each row's run of zeros.** It is the rate given the row's value, else
  `other`'s, else the column's own. A null rate replaces the column's for the rows it covers.
- **A copula member given a category reads its copula level through the category's warp**, so
  both dependencies hold. The category is drawn apart from the copula, so the member's ranks mix
  every category's warp. That lowers what each Pearson gives, so the member is calibrated against
  the mixed ranks, each category weighted by its share of the parent's rows.
- **A warp gives way to a copula pair it would leave unreachable.** Where through its warps a
  member's stated pair holds no Pearson its own values would hold, the copula keeps the pair and
  the warps are not drawn. The plan says `its zero rate given payment_type set aside for its
  Spearman with mta_tax, which no copula holds beside it`.
- **A curve or zero rate given a member of the column's own copula group is not drawn.** The
  group draws each member's level given every other member's. So among the rows holding one
  member's value, another member's levels are not uniform, and no warp of them holds the stated
  quantiles or zeros. The profiler writes no such entry unless `--edges-from` freezes it.

*Rejected:* a separate curve per category, drawn from its quantiles alone. It lost the column's
listed values and tails, so the column's own marginal missed its KS bound.

## A lookup table gives a column the value its parent's value determines

```yaml
  dependencies:
    - {kind: functional, lhs: [region], rhs: region_code, table: {north: "N", south: "S", west: "W"}}
```

A row whose `lhs` value the table lacks draws `rhs` from its own statistics. So does a share
`violation_fraction` gives of the rest. Quote a key or value YAML would read as another type: a bare
`N` is `false` in YAML 1.1.

## A derived column is a base date plus a lag, or an expression of other columns

```yaml
  - name: ship_date
    derived:
      kind: timedelta
      base: order_date
      lag:
        quantiles: {p01: 0d, p50: 2d, p99: 10d}
        min: 0d
        max: 14d
  - name: total
    derived:
      kind: expression
      expr: "round(quantity * unit_price, 2)"
      violation_fraction: 0.01
```

- **A lag is drawn from its own inverse CDF, then added to the base.** Its quantiles and ends are
  written as a scenario's durations are, a whole number and a unit of `ms`, `s`, `m`, `h`, `d` or
  `w`, with `-` before a negative one. A lag is rounded to the column's unit, or to its profiled
  `granularity` where that is coarser. So a date's lag is whole days, and a timestamp profiled to
  the second keeps the share of rows that repeat a value. A null base gives a null.
- **An expression holds column names, numbers, `+ - * /`, parentheses, and `round(x)` or
  `round(x, places)`.** A name holding a character other than a letter, a digit or `_` is quoted
  in backticks. The result is rounded to `round` places where the entry sets it, else to the
  column's `numeric.scale`. A null operand and a division by zero give a null.
- **`violation_fraction` is the share of rows that take the column's own statistics instead.** With
  `violation_fraction: 0`, every row holds its expression. A lag whose `min` is `0d` keeps `ship_date`
  on or after `order_date`.

## Each column takes one draw, in an order its parents allow

The planner orders the columns so each is drawn after every column it reads. A column's values
come from the first of these its profile states, and its nulls are drawn apart:

```text
values ── derived ──→ lookup ──→ conditional table ──→ own statistics
                                                       level: copula, else own uniform
                                                       curve: given a category, else own
                                                       zeros: given a category, else own
nulls ─── given a category, else the column's own null rate
```

**An entry the plan cannot draw is set aside, never refused.** That covers an entry naming a
column an earlier one draws, and one that would draw a column from itself. It covers a lookup
reading more than one column, and quantiles or a zero rate on a column that is not numeric.
`data plan` names each beside its column:

```text
city      string  looked up from zip_code, else 12 top values, its conditional table given zip_code set aside, since its lookup from zip_code draws it
```

`Plan.set_aside` lists each one's JSON pointer, and `data check` measures none of them. The
profiler plans its draft through the same planner, and writes no entry it sets aside.

A dependence on a column the table lacks is refused at plan time as `E-PROFILE-113`. A derived
column that would draw a column from itself is refused as `E-PROFILE-114`, as is an expression that
does not parse or reads a column that is not numeric, a lag on or from a column that is not a date
or timestamp, and a lookup or table value its column's type cannot hold.

**A column `sample` pins takes the given values in place of every draw above.** It sits at its own
place in the order, so a derived, lookup or conditional child reads the pinned values. A copula
group's other members keep their draws. So the pinned column's correlation with them is not held.

## A `cdc` update redraws a changed column from its own statistics

A scenario's `cdc` op updates rows of the full load. Each column an update changes is drawn alone,
from its own marginal, so a copula or conditional table does not hold among changed values. A
derived or looked-up column is recomputed wherever an update changes a column it reads. So an
after-image keeps its lags, expressions and lookups.

## `data check` measures each copula group and each conditional table

`correlation_max_abs_diff` is the largest change in a Spearman value within one copula group.
`conditional_kl` is a conditional table's KL divergence, weighted by each given value's share, and
names the given value adding most. `data check --help` gives each one's default threshold. Each
measures a pair of columns, so a `--thresholds` file sets it under `defaults` and is refused
where it sets it under `columns`.
[checking-generated-data-and-profile-drift.md](checking-generated-data-and-profile-drift.md)
shows both on generated and on shuffled rows.

## A quasi-identifier is never a parent under `strict` and `healthcare`

A parent's values are written as keys, so a table given a ZIP code or a birth date would list
them. Under `strict` and `healthcare`, `profile check` refuses a conditional table, quantiles,
rates or lookup given a quasi-identifier, and a lag counting from one, as
`E-POLICY-024`. The policy pass withholds each such entry. Under `healthcare` a
lag from or to a birth date is an age, so it is refused as `E-POLICY-023` and withheld. An
age's quantiles given a category are capped at 90, as its own are. `profile check` refuses one
above 90 as `E-POLICY-020`.
[profile-format-and-privacy-floor.md](profile-format-and-privacy-floor.md) says how each entry is
audited.
