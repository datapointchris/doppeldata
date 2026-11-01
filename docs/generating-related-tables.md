# Generating related tables

A bundle holds several tables' profiles and the foreign keys between them. It generates every table
parents first, and draws each foreign key from the keys its parent generated. Each parent then holds
the child rows its relationship states, so joins, skew and orphans come out as they were profiled.

## `profile create --tables` writes one bundle holding each table and every foreign key between them

```bash
doppeldata profile create --tables *.parquet --out tpch.bundle.yaml
```

Each file or folder named is one table, named for it. Over TPC-H at scale factor 1, this took 45.7 s
and 3.5 GB at peak, and wrote this relationship among its nine:

```yaml
relationships:
  - from: [lineitem.l_orderkey]
    to: [orders.o_orderkey]
    containment: 1.0
    orphan_fraction: 0.0
    cardinality: many_to_one
    fanout:
      quantiles: {p001: 1, p01: 1, p05: 1, p25: 2, p50: 4, p75: 6, p95: 7, p99: 7, p999: 7}
      zipf_s: 0.2974
    ordinal: l_linenumber
```

- **A column is a foreign key where its name refers to another table's key, and that key holds at
  least `CONTAINMENT_FLOOR` of its values.** [profiler-statistics-and-readers.md](profiler-statistics-and-readers.md)
  holds the three name rules and the alias prefixes, such as `ps_` on every column of `partsupp`.
- **A key of two columns is found as one of one.** `lineitem (l_partkey, l_suppkey)` refers to
  `partsupp (ps_partkey, ps_suppkey)`, and `from` and `to` list the columns in order.
- **`sd.profile_tables(paths)` returns the same `Bundle`**, and `Bundle.save` writes it.

## Each relationship states how many child rows each parent holds, and the generator holds them exactly

| Field | What it states | What the generator does with it |
| --- | --- | --- |
| `orphan_fraction` | the share of child rows naming a key the parent lacks | gives that share a key past every parent's |
| `containment` | the share of the child's distinct keys the parent holds | spreads the orphan rows over that many orphan keys |
| `fanout.quantiles` | child rows per parent, a parent no child names counted at 0 | draws a curve through them, so each quantile holds |
| `fanout.top_fractions` | the share of matched child rows the busiest 1, 10 and 100 parents hold | pins the curve to hold each share exactly |
| `fanout.zipf_s` | a Zipf law's exponent fitted to the counts | gives the curve its slope where no quantile or pin does |
| `inherited` | child columns equal to their parent row's on nearly every joined row | copies the parent's value on the stated share |
| `ordinal` | a child column numbering each parent's rows from 1 | writes each row's number among its parent's rows |

The counts are dealt by rank, busiest parent first, and sum to the matched rows exactly. A keyed
permutation then places the ranks on the parent's rows, and another places the child rows on the
ranks. So every block of child rows draws from every parent, as a table loaded in arrival order does.
[determinism-contract.md](determinism-contract.md) holds the streams each permutation draws from.

## `--scale` multiplies every table's rows, and each relationship keeps its shape

```bash
doppeldata data generate orders-customers --scale 0.1 --out shop/
```

- **Each table generates `round(rows × scale)` rows.** The 40,000 customers become 4,000, and
  the 550,000 orders 55,000.
- **Each quantile is a share of the parents, so it holds at any scale.** At scale 0.1 the example
  holds `p50` 4, `p95` 45 and `p99` 160 orders per customer exactly.
- **Each `top_fractions` holds over the same fraction of the parents.** `top10` of 40,000 customers is
  the busiest one of 4,000 at scale 0.1, and that customer holds 0.003 of the orders. A pin falling
  on less than one parent is left out.

## A relationship is edited in the bundle, or bent by a scenario without editing it

- **Every field above is read as written.** Each `top_fractions` counts the busiest parents' rows
  together, so `top10` below `top1` is refused as `invalid-fanout`.
- **Deleting an `inherited` entry or an `ordinal` draws the column from its own profile.** Deleting
  a relationship draws its foreign key from its column's profile too, so its values join only by
  chance.
- **A scenario's `patch.relationships` changes one relationship for one run.**
  [scenario-overlays.md](scenario-overlays.md) holds the format, and a `hot_keys` on a foreign key.

## An inherited column copies its parent row's value on the share its agreement states

```yaml
    inherited:
      - {child: region, parent: region, agreement_fraction: 0.995}
```

The profiler writes an entry where at least `AGREEMENT_FLOOR` of the joined rows hold their parent
row's value. The generator copies the parent's value on a share of the matched rows, and draws the
rest from the child's own profile. Where that draw agrees by chance, the copied share is lowered to
make up for it. On `orders-customers`, the generated orders agree on `region` within a thousandth of
its stated 0.995.

- **A copied column keeps the child's own mix, even where busy parents hold other values than idle
  ones.** A shop's northern customers may order three times as often as its southern ones. So
  `orders.region` is mostly north while `customers.region` is half north. The plan generates the
  parent's column first, then deals the busiest ranks to the values furthest short of the child's
  stated shares. The child's `top_values` are those shares.
- **`data plan --json` names the column a draw's ranks are `placed by`.** A child copying several
  columns is placed by the first listing `top_values`. A sorted draw, or a parent past
  `PLACED_PARENTS` rows, is placed by its permutation alone.

## An ordinal numbers each parent's child rows from 1, so a key of a foreign key and a line number holds

`ordinal` names a child column numbering the rows of each parent's key from 1 to their count. The
profiler writes it where an integer column runs 1 to n under every key, with no number repeated.
The generator writes each row's place in its parent's run, so the pair is unique. TPC-H's `lineitem`
numbers every order's lines from 1 with no gap.

The plan sets an `ordinal` aside where its column is another relationship's foreign key, a key a
child refers to, or copied by an `inherited` entry. The column is then drawn from its own profile.

## A column a relationship draws is text exactly where its parent's is

A text source's column holding values of other types generates as text, as
[column-samplers.md](column-samplers.md) says. A relationship's column holds its parent's values,
so its own `type_mix_fractions` gives way to the parent's form, and every join still holds.

- **A foreign key is text where its parent's key is, and typed where it is not.** It holds the
  parent's keys as written, values of other types included.
- **An orphan key lies past every parent's.** Under an integer key it is an integer past the largest
  the parent's own draws hold, written as text where the key is. Under a string key it is a uuid.
- **A copied column is text where the parent column it copies is, and typed where it is not.**
- **An ordinal is typed.** It is a row's number, whatever its `type_mix_fractions` states.
- **A parent's key holding a type mix repeats no value.** Each value of another type is derived from
  its row, so every child row joins one parent.
- **The exports type each column as the bundle generates it.** A table exported alone with `table`
  reads its foreign keys' form from the bundle's plan.

## A bridge whose primary key is its two foreign keys repeats no pair

`partsupp` refers to `part` and to `supplier`, and its primary key is the pair. Drawn apart, two of
one part's four suppliers would sometimes be the same one. The plan pairs the two draws instead:

- **The first draw places its rows as any relationship does.** It is the one the child's files are
  sorted by, else the earlier in the bundle.
- **The second takes each row's place from the first's, times a multiplier modulo the rows.** That
  is a bijection, so each relationship still holds its own counts exactly. The multiplier puts one
  parent's rows far enough apart to land on different parents of the second.
- **A bridge too dense for any multiplier is drawn unpaired.** That is where the busiest parent of
  one, times the busiest of the other, passes the child's rows. The second column's sampler in
  `data plan --json` names the draw it is `placed by` where the pair is placed.

## A composite key's single-column part is set aside, and the plan names what draws it

```text
lineitem.l_partkey → part.p_partkey, set aside, as lineitem.l_partkey, lineitem.l_suppkey → partsupp.ps_partkey, partsupp.ps_suppkey draws its columns
lineitem.l_partkey, lineitem.l_suppkey → partsupp.ps_partkey, partsupp.ps_suppkey
```

`data plan` prints every relationship, and what draws it where another does. The widest relationship
drawing a column draws it, so a part of a composite key decides nothing.

## A parent of fewer than `k_min` rows states no key, so its relationships are found only under `--policy none`

TPC-H's `region` holds 5 rows, under the default `k_min` of 11. Its profile writes no `keys`, and
records why:

```yaml
    privacy:
      withheld:
        - {field: /dataset/keys, rule: rows >= k_min}
```

With no key, nothing refers to it, so `nation.n_regionkey → region.r_regionkey` is not found.
`--policy none` keeps the key and finds it, at 5 nations to a region. `profile check` reports a key
on a table under `k_min` rows as `keys-under-k-min`.

## `data check` measures each relationship by its fan-out, its orphans and its busiest parents

```bash
doppeldata data check orders-customers shop/
```

| Metric | What it compares | Passes at |
| --- | --- | --- |
| `fanout_ks` | the child rows per parent against the stated quantiles | ≤ 0.05 |
| `orphan_fraction_delta` | the orphan rate against `orphan_fraction` | ≤ 0.005 |
| `top_fraction_delta` | the furthest of the busiest parents' shares from `top_fractions`, at the bundle's scale | ≤ 0.001 |

Each table is checked against its own profile first.
[checking-generated-data-and-profile-drift.md](checking-generated-data-and-profile-drift.md) holds
each metric's rules, and which column metrics a drawn key skips, each naming its relationship as
`drawn_by`. The New York taxi trips of March 2024 and their zones, profiled and generated, pass all
six on the pickup and drop-off relationships. Their generated trips hold within a twentieth of the
distinct pickup and drop-off pairs the real ones hold.
