# Import a seed profile from a schema

A project with a schema and no data yet can still generate. A seed is a profile imported from the
schema: each column takes its type and constraints, and a fixed default where the schema is
silent. Every verb that reads a profile reads a seed, and `profile create` replaces it once real
data exists.

```bash
doppeldata profile import --from odcs orders.odcs.yaml --out orders.profile.yaml
doppeldata profile import --from json-schema orders.schema.json --out orders.profile.yaml
doppeldata profile import --from table-schema orders.tableschema.json --out orders.profile.yaml
doppeldata profile import --from data-package datapackage.json --out shop.bundle.yaml
doppeldata profile import --from data-package datapackage.json --table orders --out orders.profile.yaml
doppeldata profile import --from glue shop.orders --aws-profile analytics --region eu-west-1 --out orders.profile.yaml
doppeldata data generate orders.profile.yaml --rows 1000 --seed 7 --out orders/
doppeldata data check orders.profile.yaml orders/
```

```python
import doppeldata as sd

sd.Profile.from_odcs('orders.odcs.yaml')
sd.Profile.from_json_schema('orders.schema.json')
sd.Profile.from_table_schema('orders.tableschema.json')
sd.Profile.from_data_package('datapackage.json', table='orders')
sd.Bundle.from_data_package('datapackage.json').tables         # ['customers', 'orders']
sd.Profile.from_glue('shop', 'orders', region='eu-west-1', aws_profile='analytics', detected_entities={'contact': ['EMAIL']})
```

## An ODCS contract's properties each become a column

```yaml
apiVersion: v3.2.0
kind: DataContract
id: orders
version: 1.0.0
status: active
schema:
  - name: orders
    properties:
      - {name: order_id, logicalType: integer, logicalTypeOptions: {minimum: 1}, primaryKey: true, required: true}
      - {name: status, logicalType: string, required: true, enum: [{value: placed}, {value: shipped}, {value: returned}]}
      - {name: amount, logicalType: number, physicalType: 'DECIMAL(12, 2)', logicalTypeOptions: {minimum: 0, maximum: 5000}, required: true}
      - {name: email, logicalType: string, logicalTypeOptions: {format: email}, classification: restricted}
      - {name: ordered_on, logicalType: date, required: true}
```

```text
$ doppeldata profile import --from odcs orders.odcs.yaml --out orders.profile.yaml
$ yq '.generated, .dataset, .columns[0], .columns[3]' orders.profile.yaml
tool: doppeldata <version>
at: '2026-11-01T07:47:22Z'
engine: import
imported_from: odcs:orders.odcs.yaml#orders
declared:
  order_id:
    - low
  status:
    - top_values
  amount:
    - low
    - high
    - scale
  email:
    - format
keys:
  primary:
    columns:
      - order_id
name: order_id
physical_type: int64
logical_type: integer
nullable: false
null_fraction: 0.0
numeric:
  integral: true
  sequence:
    start: 1
    step: 1
name: email
physical_type: string
logical_type: string
nullable: true
null_fraction: 0.05
semantic:
  type: email
  generator_hint:
    provider: email
  sensitivity: sensitive
$ doppeldata data generate orders.profile.yaml --rows 1000 --seed 7 --out orders/
wrote 1,000 rows as parquet in 1 file, 22.4 KB, in 0.00 s on 16 threads: 0.50M rows/s
orders/_manifest.json names dataset sha256:…
$ doppeldata data check orders.profile.yaml orders/
# doppeldata check passed

`orders/`, 1,000 rows, against profile `orders` (sha256:…).

0 of 6 metrics failed.
```

- **`declared` names each fact the schema stated.** Everything else is a default:
  `ordered_on` draws inside 2025, and `email` is null in 5% of rows.
- **The seed states no row count**, so `--rows` sizes the write.
- **`profile check` fails a seed, exiting 3.** No privacy policy audited what it states, so commit
  a profile `profile create` makes once real data exists.
- **`--table` picks one schema object from a contract holding several.** A contract of several
  with no `--table` is refused, naming each.

## DDL, Avro or dbt reach a seed through datacontract-cli

datacontract-cli's `import` writes an ODCS v3 contract from a schema doppeldata does not read. It
is no dependency of doppeldata.

```text
$ cat orders.sql
create table orders (
  order_id bigint primary key,
  status varchar(10) not null,
  amount numeric(12, 2) not null,
  email varchar(320),
  ordered_on date not null
);
$ uvx --from datacontract-cli datacontract import sql --source orders.sql --dialect postgres --output orders-sql.odcs.yaml
... - WARNING - SQL import generated a server block with placeholder connection values. Update the following values before use: host, database, schema, port
Written result to orders-sql.odcs.yaml
$ doppeldata profile import --from odcs orders-sql.odcs.yaml --out orders-sql.profile.yaml
$ yq -o=json -I0 '[.columns[] | {"n": .name, "t": .physical_type}], .generated.declared' orders-sql.profile.yaml
[{"n":"order_id","t":"int64"},{"n":"status","t":"string"},{"n":"amount","t":"decimal128(12, 2)"},{"n":"email","t":"string"},{"n":"ordered_on","t":"date32[day]"}]
{"status":["max_length"],"amount":["scale"],"email":["max_length"]}
```

## A Data Package imports as a bundle, its foreign keys as relationships

```text
$ doppeldata profile import --from data-package datapackage.json --out shop.bundle.yaml
$ yq '.relationships' shop.bundle.yaml
- from:
    - orders.customer_id
  to:
    - customers.id
$ doppeldata data generate shop.bundle.yaml --seed 7 --out shop/
doppeldata: E-PROFILE-116: shop.bundle.yaml cannot be generated:
  /tables/customers/dataset/rows: customers records no row count for the scale to multiply [E-PROFILE-116]
  hint: write dataset.rows into the profile, or give the rows to generate
$ yq -i '.tables.customers.dataset.rows = 500 | .tables.orders.dataset.rows = 5000' shop.bundle.yaml
$ doppeldata data generate shop.bundle.yaml --seed 7 --out shop/
customers  wrote 500 rows as parquet in 1 file, 9.1 KB
orders     wrote 5,000 rows as parquet in 1 file, 66.1 KB
in 0.01 s on 16 threads; shop/_manifest.json names dataset sha256:…
$ doppeldata data check shop.bundle.yaml shop/
# doppeldata bundle check passed

`shop/`, 5,500 rows in 2 tables, against bundle `shop` (sha256:…).

0 of 1 relationship metrics failed.

| Metric | Relationship | Value | Threshold | Expected | Observed | Passed |
| --- | --- | --- | --- | --- | --- | --- |
| orphan_fraction_delta | orders.customer_id → customers.id | 0 | ≤ 0.005 | 0 | 0 | yes |
...
```

`datapackage.json` holds a `customers` resource keyed by `id` and an `orders` resource whose
`foreignKeys` name `customer_id` → `customers.id`. A seed states no fan-out, so every customer
holds about the same number of orders.

## A Glue table gives its format, dialect, header and partitions

```text
$ aws glue get-partitions --profile analytics --region eu-west-1 --database-name shop --table-name orders --query 'Partitions[].Values' --output text
eu 2025-03-01
us 2025-03-02
__HIVE_DEFAULT_PARTITION__ 2025-03-02
$ doppeldata profile import --from glue shop.orders --aws-profile analytics --region eu-west-1 --detected-entities entities.json --out glue.profile.yaml
doppeldata: glue:eu-west-1/shop/orders: columns of types the engine draws nothing of are left out: payload (binary)
$ yq -o=json -I0 '.generated.imported_from, .source, .dataset' glue.profile.yaml
"glue:eu-west-1/shop/orders@1"
{"format":"csv","csv":{"writer":{"delimiter":";","header":true,"quote":"\"","escape":"\\"}},"layout":{"partitioning":{"style":"hive","keys":[{"name":"region","type":"string","values":2},{"name":"order_date","type":"date32[day]","values":2}]}}}
{"rows":400}
$ doppeldata data generate glue.profile.yaml --seed 7 --out glue-orders/
wrote 400 rows as csv in 4 files, 16.5 KB, in 0.01 s on 16 threads: 0.08M rows/s
glue-orders/_manifest.json names dataset sha256:…
$ eza -D glue-orders/*
glue-orders/region=eu:
order_date=2025-03-01  order_date=2025-03-02

glue-orders/region=us:
order_date=2025-03-01  order_date=2025-03-02
```

- **The table is an OpenCSVSerde table**, `separatorChar` `;`, `skip.header.line.count` 1 and
  `recordCount` 400, partitioned by `region` and `order_date`.
- **The import calls `glue:GetTable` and `glue:GetPartitions`.** It needs boto3, and pyarrow for
  `--detected-entities`.
- **`--detected-entities` maps each column to the Glue entity types `classifyColumns` found**, here
  `{"contact": ["EMAIL"]}`, so `contact` draws emails.
- **A string key draws only the values the catalog lists**, and `__HIVE_DEFAULT_PARTITION__` is
  no value. A date key draws every day between the least and the most listed.

## A schema class generates rows wherever a profile is taken

`generate`, `stream`, `write` and `Profile.expand` take a pandera `DataFrameModel` or
`DataFrameSchema`, a dataframely `Schema`, and a Spark `StructType`. doppeldata imports none of
the three libraries.

```python
import dataframely as dy
import doppeldata as sd
import pandera.pandas as pa
import polars as pl
from pandera.typing import Series
from pyspark.sql import types as T


class Orders(dy.Schema):
    order_id = dy.Int64(primary_key=True, min=1)
    status = dy.Enum(['placed', 'shipped', 'returned'])
    amount = dy.Decimal(precision=10, scale=2, min=0, max=500)
    customer_email = dy.String(nullable=True)


orders = sd.generate(Orders, rows=1000)
print(Orders.validate(pl.DataFrame(orders), cast=True).shape)


class PanderaOrders(pa.DataFrameModel):
    order_id: Series[int] = pa.Field(ge=1, unique=True)
    status: Series[str] = pa.Field(isin=['placed', 'shipped', 'returned'])
    sku: Series[str] = pa.Field(str_matches=r'^[A-Z]{3}-\d{4}$')
    customer_email: Series[str] = pa.Field(nullable=True)


written = sd.write(PanderaOrders, 'orders.parquet', rows=1000)
print(written.dataset_digest)
sd.Profile.expand(PanderaOrders, rows=1000).save('pandera-orders.profile.yaml')
print(PanderaOrders.validate(pl.read_parquet('orders.parquet').to_pandas()).shape)

schema = T.StructType([
    T.StructField('order_id', T.LongType(), nullable=False),
    T.StructField('ordered_at', T.TimestampType(), nullable=False),
    T.StructField('tags', T.ArrayType(T.StringType())),
])
spark_orders = pl.DataFrame(sd.generate(schema, rows=1000))
print(spark_orders.schema)
```

```text
$ python schema_classes.py
schema_classes.py:16: ProfilingWarning: dataframely:__main__.Orders: columns of a dictionary type are drawn as text, so validate with cast=True: status
  orders = sd.generate(Orders, rows=1000)
(1000, 4)
sha256:…
(1000, 4)
Schema({'order_id': Int64, 'ordered_at': Datetime(time_unit='us', time_zone='UTC'), 'tags': List(String)})
```

- **A dictionary type is drawn as text, and a warning names it**, so the schema validates with
  dataframely's `cast=True` or pandera's `coerce=True`.
- **A check the seed cannot read shapes nothing, and a warning names it**, such as a custom pandera
  check.
- **A list or a struct column writes to Parquet or NDJSON.** A CSV holds neither.

## Why

[Seed profiles imported from a schema](../seed-profiles-from-schemas.md) says what each schema's
fields become, the default each silent column draws and why, and what a seed records.
