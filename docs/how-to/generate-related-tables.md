# Generate related tables

A bundle holds several tables' profiles and the foreign keys between them. `profile create --tables`
writes one, and every command taking a profile takes it. Each table generates parents first, so
every foreign key joins its parent at the fan-out, skew and orphan rate the bundle states.

```bash
doppeldata profile create data/shop/ --tables --out shop.bundle.yaml
doppeldata data plan shop.bundle.yaml --scale 2 --seed 7 --out shop/
doppeldata data generate shop.bundle.yaml --scale 2 --seed 7 --out shop/
doppeldata data check shop.bundle.yaml shop/
```

```python
import pyarrow as pa

import doppeldata as sd

bundle = sd.profile_tables('data/shop/')
bundle.save('shop.bundle.yaml')
manifest = sd.write(bundle, 'shop/', scale=2, seed=7)
tables = sd.generate(bundle, scale=0.5, seed=7)      # a dict of Arrow tables, by name
{name: pa.table(table).num_rows for name, table in tables.items()}   # {'customers': 2000, 'orders': 27500}
```

`orders-customers` is an example bundle, so `doppeldata data generate orders-customers --scale 0.1
--seed 5 --out data/shop/` writes the source above.

## `--tables` profiles each folder or file as a table, and finds the foreign keys between them

```text
$ yq '.relationships' shop.bundle.yaml
- from: [orders.customer_id]
  to: [customers.customer_id]
  containment: 0.9944
  orphan_fraction: 0.002002
  cardinality: many_to_one
  fanout:
    quantiles: {p01: 0, p05: 0, p25: 1, p50: 4, p75: 10, p95: 45, p99: 160}
    top_fractions: {top1: 0.003009, top10: 0.02969, top100: 0.2693}
    zipf_s: 0.6956
  inherited:
    - {child: region, parent: region, agreement_fraction: 0.9947}
```

- **Each table is named for its folder or file.** A directory whose folders hold unlike columns
  names them, and several paths named together are one table each.
- **A column is a foreign key where its name refers to another table's key**, and that key holds
  nearly all of its values. `inherited` names a child column equal to its parent row's, and
  `ordinal` one numbering each parent's rows from 1.
- **A table of fewer than `k_min` rows states no key**, so nothing is found referring to it. Its
  profile records the key as withheld.

## `--scale` multiplies every table's rows, and `data plan` names what each table writes

```text
$ doppeldata data plan shop.bundle.yaml --scale 2 --seed 7 --out shop/
dataset    sha256:3c6883e6ef0cfd2d240e7b0cfe60ca31590dfa703429029c21862f9b77e43ab2
identity   118,000 rows at scale 2 from seed 7, bundle sha256:52b000b66cc94165bc2799bbe99161052603388e8a43532710a2db4706e70dc8, stream version 12

customers  8,000 rows as parquet, up to 2 files to shop/customers, estimate 93.8 KB
orders     110,000 rows as parquet, up to 2 files to shop/orders, estimate 1.8 MB

orders.customer_id → customers.customer_id
```

A bundle takes `--scale`, never `--rows`. Each table writes its own directory and manifest, and one
`_manifest.json` above them names the dataset:

```text
$ doppeldata data generate shop.bundle.yaml --scale 2 --seed 7 --out shop/
customers  wrote 8,000 rows as parquet in 2 files, 95.6 KB
orders     wrote 110,000 rows as parquet in 2 files, 1.8 MB
in 0.15 s on 16 threads; shop/_manifest.json names dataset sha256:3c6883e6ef0cfd2d240e7b0cfe60ca31590dfa703429029c21862f9b77e43ab2
$ eza -T shop
shop
├── _manifest.json
├── customers
│   ├── _manifest.json
│   ├── part-00000-722e78a5.parquet
│   └── part-00001-722e78a5.parquet
└── orders
    ├── _manifest.json
    ├── part-00000-3c5fcaf0.parquet
    └── part-00001-3c5fcaf0.parquet
```

`sd.write` with the same arguments returns a `BundleManifest` naming the same `dataset_digest`.

## `data check` measures each relationship, then each table against its own profile

```text
$ doppeldata data check shop.bundle.yaml shop/
# doppeldata bundle check passed

`shop/`, 118,000 rows in 2 tables, against bundle `shop` (sha256:52b000b66cc94165bc2799bbe99161052603388e8a43532710a2db4706e70dc8).

0 of 3 relationship metrics failed.

| Metric | Relationship | Value | Threshold | Expected | Observed | Passed |
| --- | --- | --- | --- | --- | --- | --- |
| fanout_ks | orders.customer_id → customers.customer_id | 0 | ≤ 0.05 |  |  | yes |
| orphan_fraction_delta | orders.customer_id → customers.customer_id | 2.002e-09 | ≤ 0.005 | 0.002002 | 0.002002 | yes |
| top_fraction_delta | orders.customer_id → customers.customer_id | 1.19449e-06 | ≤ 0.001 | 0.2693 | 0.269299 | yes |


## customers passed
...
```

[Check generated data](check-generated-data.md) says how to read each table's section.

## Edit a relationship in the bundle, or bend it for one run with a scenario

- **Every relationship field is read as written**, so a reviewer edits a fan-out quantile, an
  orphan rate or a busiest parent's share in the YAML. `profile check` then holds the edit to the
  model, and refuses a `top10` below `top1` as `E-PROFILE-122`.
- **Deleting an `inherited` entry or an `ordinal` draws that column from its own profile.**
  Deleting a relationship draws the foreign key from its column's profile, so it joins only by
  chance.
- **A scenario's `patch.relationships` changes a relationship for one run**, and a `hot_keys` op
  on a foreign key skews it. [Bend data with a scenario](bend-data-with-a-scenario.md) shows both.

## Why

[Generating related tables](../generating-related-tables.md) says how the generator holds each
relationship field exactly at any scale, how an inherited column keeps the child's own mix, and how
a bridge table's two keys repeat no pair.
