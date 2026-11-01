# Export a profile as a data contract

A profile already states what its data holds. `profile export` writes that as a contract another
tool validates data with: a schema module, a Table Schema, an ODCS contract or a Glue Data Quality
ruleset. doppeldata imports none of the tools that read them.

```bash
doppeldata profile export orders --format pandera --out orders_pandera.py
doppeldata profile export orders --format dataframely --out orders_dataframely.py
doppeldata profile export orders --format pyspark-schemas --out orders_spark.py
doppeldata profile export orders --format table-schema --out orders.tableschema.json
doppeldata profile export orders --format json-schema --out orders.schema.json
doppeldata profile export orders --format odcs --out orders.odcs.yaml
doppeldata profile export orders --format dqdl --out orders.dqdl
```

```python
import doppeldata as sd

contract = sd.export('orders', 'odcs')     # the contract's text
```

| `--format` | Writes | Read by |
| --- | --- | --- |
| `pandera` | a module of a `pandera.polars.DataFrameModel` of each table | pandera, `sd.generate` |
| `dataframely` | a module of a `dataframely.Schema` of each table | dataframely, `sd.generate` |
| `pyspark-schemas` | a module of each table's `StructType` and `pandera.pyspark.DataFrameModel` | Spark, pandera |
| `table-schema` | a Frictionless Table Schema v2, and for a bundle a Data Package v2 | frictionless, `profile import --from table-schema`, and `--from data-package` for a bundle's |
| `json-schema` | the record schema: a JSON Schema 2020-12 of one record | any JSON Schema validator, `profile import --from json-schema` |
| `odcs` | an Open Data Contract Standard v3.2.0 contract | datacontract-cli, `profile import --from odcs` |
| `dqdl` | an AWS Glue Data Quality ruleset of each table | Glue Data Quality, Deequ |

PROFILE is a profile file, a bundle, an example's name, or a data file holding an embedded profile.
Without `--out` the export prints.

## Each strict format names what it could not state of every row

```text
$ cat orders_pandera.py
"""The pandera models of orders, exported from its doppeldata profile.

profile_sha256: 95abee1ddc215b0b2aa73077d2e02925d48b8acfc2461ded17ba7cddbeec16e2
"""

# Left out:
# - order_id: the range 11 to 99990, trimmed at k_min rows from each end
# - amount: the range 0.5 to 4000.0, trimmed at k_min rows from each end
# - quantity: the range 1 to 10, trimmed at k_min rows from each end
# - ordered_at: the range 2025-01-01T00:00:00Z to 2025-12-31T23:59:59Z, trimmed at k_min rows from each end
# - customer_email: the lengths 9 to 64, trimmed at k_min rows from each end
# - ship_zip: the lengths 5 to 5, trimmed at k_min rows from each end
# - customer_email: the email format, which pandera has no check of

from typing import Annotated

import pandera.polars as pa
import polars as pl


class Orders(pa.DataFrameModel):
    order_id: pl.Int64 = pa.Field(unique=True)
    customer_id: pl.Int64 = pa.Field()
    amount: Annotated[pl.Decimal, 12, 2] = pa.Field()
    quantity: pl.Int32 = pa.Field()
    status: pl.String = pa.Field(isin=['delivered', 'shipped', 'processing', 'cancelled', 'returned'])
    ordered_at: Annotated[pl.Datetime, 'us', 'UTC'] = pa.Field()
    customer_email: pl.String = pa.Field(nullable=True)
    ship_zip: pl.String = pa.Field(str_matches='^\\d{5}$')
```

The `Left out` comment lists each range, length and format the module does not check, because fewer
than every row holds it. [A strict format carries a fact only where the profile states it of every
row](../profile-exports-and-embedded-profiles.md#a-strict-format-carries-a-fact-only-where-the-profile-states-it-of-every-row)
says why.

The other strict formats, cut to a few columns:

```text
$ jq -c '.fields[] | select(.name == "status" or .name == "customer_email")' orders.tableschema.json
{"name":"status","type":"string","constraints":{"required":true,"enum":["delivered","shipped","processing","cancelled","returned"]}}
{"name":"customer_email","type":"string","format":"email"}
$ jq -c '.properties | {customer_email, ship_zip}' orders.schema.json
{"customer_email":{"type":["string","null"],"format":"email"},"ship_zip":{"type":"string","pattern":"^\\d{5}$"}}
$ yq '.schema[0].properties[] | select(.name == "ship_zip")' orders.odcs.yaml
name: ship_zip
logicalType: string
physicalType: string
required: true
logicalTypeOptions:
  pattern: ^\d{5}$
classification: confidential
tags:
  - us_zip
examples:
  - '53877'
  - '63965'
  - '72296'
```

## DQDL states each share as a threshold

```text
$ cat orders.dqdl
# The ruleset of orders, exported from its doppeldata profile sha256:95abee1ddc215b0b2aa73077d2e02925d48b8acfc2461ded17ba7cddbeec16e2.
# Each threshold lies 3 standard errors below the share the profile states of 100000 rows.
# Left out:
#   customer_email: the lengths 9 to 64, trimmed, as ColumnLength takes no threshold
#   customer_email: the email format, which DQDL has no rule for
#   ship_zip: the lengths 5 to 5, trimmed, as ColumnLength takes no threshold
Rules = [
    IsPrimaryKey "order_id",
    IsComplete "order_id",
    ColumnValues "order_id" between 10 and 99991 with threshold >= 0.999665,
    IsComplete "customer_id",
    IsComplete "amount",
    ColumnValues "amount" between 0.49 and 4000.01 with threshold >= 0.999665,
    Mean "amount" between 90.8759 and 94.0241,
    StandardDeviation "amount" between 164.796 and 167.024,
    IsComplete "quantity",
    ColumnValues "quantity" between 0 and 11 with threshold >= 0.999665,
    IsComplete "status",
    ColumnValues "status" in ["delivered", "shipped", "processing", "cancelled", "returned"],
    IsComplete "ordered_at",
    ColumnValues "ordered_at" between "2024-12-31" and "2026-01-01" with threshold >= 0.999665,
    Completeness "customer_email" >= 0.978666,
    IsComplete "ship_zip",
    ColumnValues "ship_zip" matches "\d{5}"
]
```

[DQDL carries each share as a threshold three standard errors below
it](../profile-exports-and-embedded-profiles.md#dqdl-carries-each-share-as-a-threshold-three-standard-errors-below-it)
says how each threshold and widened range is set.

## Data generated from the profile passes each export

```text
$ doppeldata data generate orders --rows 10000 --seed 7 --out orders.csv
$ doppeldata data generate orders --rows 10000 --seed 7 --out orders/
$ uvx frictionless validate orders.csv --schema orders.tableschema.json
...
│ orders │ table │ orders.csv │ VALID  │
└────────┴───────┴────────────┴────────┘
$ uvx --from datacontract-cli datacontract lint orders.odcs.yaml
...
│ passed │ Data contract is valid against ODCS v3.2.0 │       │         │
╰────────┴────────────────────────────────────────────┴───────┴─────────╯
🟢 Data contract is valid. Ran 1 checks. Took 0.111886 seconds.
$ cat validate_exports.py
import polars as pl
from orders_dataframely import Orders as DataframelyOrders
from orders_pandera import Orders as PanderaOrders

orders = pl.read_parquet('orders/*.parquet')
print(PanderaOrders.validate(orders).shape)
print(DataframelyOrders.validate(orders).shape)
$ python validate_exports.py
(10000, 8)
(10000, 8)
```

## Spark reads the CSV with the exported `StructType`, and `pandera.pyspark` passes it

```text
$ doppeldata profile export orders --format pyspark-schemas --out orders_spark.py
$ cat validate_with_spark_schema.py
from orders_spark import ORDERS_STRUCT_TYPE
from orders_spark import Orders
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
frame = spark.read.option('header', True).schema(ORDERS_STRUCT_TYPE).csv('orders.csv')
validated = Orders.validate(check_obj=frame)
print(frame.count(), dict(validated.pandera.errors))
$ spark-submit validate_with_spark_schema.py
10000 {}
```

- **An empty `pandera.errors` means every check passed.**
- **Read a CSV with the `StructType`, never with `inferSchema`.** Inference types a column of digit
  strings, such as an NPI, as a long, which its `matches` rule then fails.
- **`pandera.pyspark` imports `pyspark.pandas`**, which fails on Spark 4.1.3 under pandas 3.1:
  `cannot import name 'DateOffset'`. The run above is Glue 5.0's Spark 3.5.4, with pandera
  installed beside it.

## A seed exports what its schema declared, as exact

```text
$ doppeldata profile import --from odcs orders.odcs.yaml --out orders.profile.yaml
$ doppeldata profile export orders.profile.yaml --format pandera
...
class Orders(pa.DataFrameModel):
    order_id: pl.Int64 = pa.Field(unique=True)
    status: pl.String = pa.Field(isin=['placed', 'shipped', 'returned'])
    amount: Annotated[pl.Decimal, 12, 2] = pa.Field(ge=decimal.Decimal('0.00'), le=decimal.Decimal('5000.00'))
    email: pl.String = pa.Field(nullable=True)
    ordered_on: pl.Date = pa.Field()
```

`orders.odcs.yaml` is the contract [Import a seed profile from a
schema](import-a-seed-from-a-schema.md) imports.

- **The contract bounds `amount` from 0 to 5000**, so the export does too.
- **The contract bounds `ordered_on` nowhere**, so the export leaves it unbounded. The seed still
  draws it from 2025-01-02 to 2025-12-31.
- **A seed exports without `profile check`**, which fails every seed.

## A bundle exports as a Data Package, and as DQDL with `ReferentialIntegrity`

```text
$ doppeldata profile export shop.bundle.yaml --format table-schema --out datapackage.json
$ doppeldata profile export shop.bundle.yaml --format dqdl --out shop.dqdl
$ rg 'Uniqueness|ReferentialIntegrity' shop.dqdl
    Uniqueness "customer_id" >= 0.99725,
    Uniqueness "order_id" >= 0.9998,
    ReferentialIntegrity "customer_id" "customers.customer_id" >= 0.996299,
$ doppeldata profile export shop.bundle.yaml --format json-schema
doppeldata: E-EXPORT-002: the record schema describes one table, and shop holds customers, orders; name one with table=, or --table
  valid:
    customers
    orders
$ doppeldata profile export shop.bundle.yaml --format json-schema --table orders | jq -c '{title, required}'
{"title":"orders","required":["order_id","region","amount","status","ordered_at"]}
```

- **`shop.bundle.yaml` is the bundle** [Generate related tables](generate-related-tables.md)
  profiles from `data/shop/`.
- **The Data Package states no foreign key.** Each resource's `description` names it as left out:
  "customer_id: the foreign key to customers.customer_id, which 99.8% of values hold."
- **`ReferentialIntegrity` names the parent by its table name.** A Glue Data Quality run passes
  the parent's frame as an additional data source under that alias.

## Why

[Profile exports and the embedded profile](../profile-exports-and-embedded-profiles.md) says how
each format's types map, why a profiled bundle holds no foreign key of every row, and what a
sampled profile's exports leave out.
