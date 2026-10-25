# Seed profiles imported from a schema

A project with a schema and no data yet can still generate. A seed is a profile imported from the
schema: each column takes its type and constraints, and a fixed default where the schema is silent.
Every verb that reads a profile reads a seed. `profile create` replaces it once real data exists.

## A schema class generates rows wherever a profile is taken

```python
import dataframely as dy
import doppeldata as sd

class Orders(dy.Schema):
    order_id = dy.Int64(primary_key=True, min=1)
    status = dy.Enum(['placed', 'shipped', 'returned'])
    amount = dy.Decimal(precision=10, scale=2, min=0, max=500)
    customer_email = dy.String(nullable=True)

orders = sd.generate(Orders, rows=1000)
```

```python
import pandera.pandas as pa
from pandera.typing import Series

class Orders(pa.DataFrameModel):
    order_id: Series[int] = pa.Field(ge=1, unique=True)
    status: Series[str] = pa.Field(isin=['placed', 'shipped', 'returned'])
    sku: Series[str] = pa.Field(str_matches=r'^[A-Z]{3}-\d{4}$')
    customer_email: Series[str] = pa.Field(nullable=True)

sd.write(Orders, 'orders.parquet', rows=1000)
sd.Profile.expand(Orders, rows=1000).save('orders.profile.yaml')
```

```python
from pyspark.sql import types as T

schema = T.StructType([
    T.StructField('order_id', T.LongType(), nullable=False),
    T.StructField('ordered_at', T.TimestampType(), nullable=False),
    T.StructField('tags', T.ArrayType(T.StringType())),
])
orders = sd.generate(schema, rows=1000)
```

`generate`, `stream`, `write` and `Profile.expand` take a pandera `DataFrameModel` or
`DataFrameSchema`, of the pandas or the Polars flavor, a dataframely `Schema`, and a Spark
`StructType`. The seed is named for the class, and a `StructType` for `table`.

- **A schema is read by its attributes, so none of the three libraries is a dependency.** The
  library imports none of pandera, dataframely and pyspark. A class is recognized by the module that
  defines it or a class it derives from. So a Glue job that only generates installs none of them.
- **pandera's built-in checks give the bounds, values, pattern and lengths.** `Field`'s `ge`, `le`,
  `gt`, `lt`, `in_range`, `isin`, `eq`, `str_matches` and `str_length` each build one, read by its
  name and statistics. `nullable` decides nulls, and pandera defaults it to false. `unique` makes a
  key.
  A custom check is a function the library cannot read, so it shapes nothing.
- **dataframely's rules are attributes of each column**: `min`, `max`, `min_exclusive`,
  `max_exclusive`, `regex`, `min_length`, `max_length`, `nullable`, `primary_key`, an `Enum`'s
  categories, and a `Decimal`'s precision and scale. `List` and `Struct` nest.
- **A Spark field takes its type from its Hive name, the one `simpleString()` prints.** So `bigint`,
  `decimal(10,2)`, `varchar(6)`, `array<string>` and `struct<city:string>` read as a Glue column
  does. `TimestampType` is an instant, so it is UTC. `TimestampNTZType` is a wall-clock reading.
- **A type the engine draws nothing of is left out, named in a warning**: `binary`, `map`, and in a
  file schema `geopoint`, `duration`, `vector` and the like.

## `profile import` writes a seed from a schema file or a Glue table

```bash
doppeldata profile import --from odcs orders.odcs.yaml --out orders.profile.yaml
doppeldata profile import --from json-schema orders.schema.json --out orders.profile.yaml
doppeldata profile import --from table-schema orders.tableschema.json --out orders.profile.yaml
doppeldata profile import --from data-package datapackage.json --out shop.bundle.yaml
doppeldata profile import --from data-package datapackage.json --table orders --out orders.profile.yaml
doppeldata profile import --from glue shop.orders --region eu-west-1 --out orders.profile.yaml
doppeldata data generate orders.profile.yaml --rows 1000 --out orders/
```

```python
sd.Profile.from_odcs('orders.odcs.yaml')
sd.Profile.from_json_schema('orders.schema.json')
sd.Profile.from_table_schema('orders.tableschema.json')
sd.Profile.from_data_package('datapackage.json', table='orders')
sd.Bundle.from_data_package('datapackage.json')
sd.Profile.from_glue('shop', 'orders', region='eu-west-1')
```

Without `--out` the seed prints as YAML. A file names its table up to its first dot, so
`orders.schema.json` imports as `orders`.

- **`--table` picks one table from a source holding several**: an ODCS contract's schema objects,
  or a Data Package's resources. It is refused for the other sources, which hold one table.
- **A contract of several objects with no `--table` is refused, naming each.** A bundle would be no
  use to `data generate`, which reads one table, and taking the first would pick silently.
- **A Data Package with no `--table` imports as a bundle.** Each resource is a table, and each
  foreign key of one field a relationship, as `orders.customer_id` to `customers.id`. A key of
  several fields, or naming a resource the package lacks, is left out with a warning. A resource's
  schema may be a file beside the package. A schema at a URL is refused.
- **`Profile.from_table_schema` reads one table and records no relationship.** A foreign key names a
  resource, and only a package declares one.
- **A failure exits as profiling's do.** A file that cannot be read or parsed, or a Glue table the
  catalog lacks, exits 2. A Glue call refused, such as for its credentials, exits 1. Throttling, a
  5xx or no answer exits 4.

## Where a schema is silent, a column draws a fixed default

`declared_columns.py` names each default once.

| A column | draws |
| --- | --- |
| number | uniformly between its bounds. A missing end lies 1,000,000 beyond the given one, and neither gives 0 to 1,000,000, each kept inside its type |
| date or timestamp | uniformly inside 2025-01-01 to 2025-12-31, a missing end the window's length beyond the given one. A timestamp draws whole seconds |
| text | its pattern, else its values, else its format's type, else the fake its name hints, else 8 to 16 letters and digits, kept inside its declared lengths |
| sole integer key, no upper bound | 1, 2, 3 and on, or counting from its lower bound |
| list | between its declared item counts, else the engine's 0 to 4 |
| any | nulls in 5% of rows where the schema allows them, and none where it requires a value |

- **The window is fixed rather than counted back from today.** So one seed generates the same rows
  on any day, as the determinism contract asks of any profile.
- **A text column with no hint draws letters and digits, where a bare column name is refused.** A
  schema declared the column, so it exists whatever its name. A name typed into `--columns` with
  no fake may be a typo, so it is refused there.
- **A format names a general-pack type**: `email`, `uuid`, `uri` and `url`, `ipv4` and `ipv6`. A
  pattern the engine cannot draw is left out with a warning, so the column falls to the next source.

## A seed records where it came from, and passes `data check` but not `profile check`

```yaml
generated:
  engine: import
  imported_from: odcs:orders.odcs.yaml#orders
```

- **`imported_from` names the kind of schema and the file, never its directory.** A Glue table is
  `glue:<region>/<database>/<table>`, never its ARN, which holds the account id. A seed is a file
  people commit.
- **`imported_from` is left out of the digest**, as `at` is. So a schema imported again from a copy
  elsewhere gives the same profile digest.
- **Data generated from a seed passes `data check` against it**, as any profile's does.
- **`profile check` fails a seed, exiting 3.** A seed was never written under a privacy policy, so
  its declared values are literals no policy audited.

## A Glue table gives its format, dialect, header and partitions

`Profile.from_glue` calls `glue:GetTable`, then `glue:GetPartitions` for a partitioned table, with
boto3 and the AWS environment's credentials. doppeldata-cli installs boto3. The library does not,
and without it the import is refused with exit 2, naming `pip install boto3`. `detected_entities`
reads the detector packs, which import pyarrow, so it needs pyarrow too.

- **The format comes from the table's `classification`, else its SerDe.** csv, tsv, parquet and
  json are read. ORC and Avro are written by no doppeldata writer, so neither is recorded.
- **A CSV's dialect comes from its SerDe.** OpenCSVSerde's `separatorChar`, `quoteChar` and
  `escapeChar` give the delimiter, quote and escape. LazySimpleSerDe's `field.delim` gives the
  delimiter. `skip.header.line.count` above 0 writes a header. `serialization.null.format` names
  the null.
- **A seed holds its partition values as written.** A string or integer key draws only the values
  `GetPartitions` lists, so a write makes their directories alone. A date key draws every day from
  the least listed to the most. A profile's temporal column holds no list of values, so a gap in the
  listed days is filled. `__HIVE_DEFAULT_PARTITION__`, Hive's null partition, is no value.
- **The row count is the table's `recordCount` or `numRows`**, as a crawler or a statistics run
  writes it.
- **`detected_entities` gives a column the type a shipped detector pack declares for it.** It maps
  each column to the Glue entity types `classifyColumns` found in it, such as `{"email": ["EMAIL"]}`.
  The first type a pack's `glue_type` names wins, with the pack's sensitivity. A type no pack
  declares is named in a warning and left off. `--detected-entities FILE` reads the same map as
  JSON.

## An ODCS contract's properties each become a column

```bash
datacontract import sql --source orders.sql --dialect postgres --output orders.odcs.yaml
doppeldata profile import --from odcs orders.odcs.yaml --out orders.profile.yaml
```

A project with DDL, Avro, dbt or another schema datacontract-cli reads can reach a seed through its
`import`, which writes an ODCS v3 contract. It is no dependency of doppeldata.

- **A column takes its `physicalName`, as the data store holds it.** A `physicalType` in Hive's
  names, such as `VARCHAR(36)` or `DECIMAL(12, 2)`, gives the Arrow type where its kind agrees with
  the `logicalType`. `logicalTypeOptions` give the bounds, lengths, pattern and format.
- **A `measure` property is no column**, because it is an expression over the rows.
- **`required`, `enum`, `primaryKey` and `unique` shape the column.** A sole primary key or a
  unique column is a key. `classification: restricted` or `confidential` marks it sensitive.
- **Quality checks of the library metrics give rates.** `nullValues` and `duplicateValues` with
  `mustBe: 0` give none. A percent `mustBe` gives that share. A percent ceiling gives half of it, so
  the generated data stays under it. A `rowCount` check gives the least row count it allows.
- **A `partitioned` property is a partition key recorded without values.** So a write makes a
  directory per drawn value, and `--no-partitions` writes one directory.

## A JSON Schema or a Table Schema describes one record

- **A JSON Schema's root object gives the columns.** A nested object is a struct and an array a
  list. A `$ref` within the file is followed. `anyOf` or `oneOf` of one type and `null` reads as
  that type, allowing nulls.
- **JSON Schema's formats `date`, `date-time` and `time` make a temporal column.** A `date-time` is
  an instant in UTC. OpenAPI's `int32`, `int64`, `float` and `double` give the Arrow type, and a
  `multipleOf` of 0.01 two decimal places.
- **Draft 2020-12's numeric `exclusiveMinimum` and Draft 4's boolean one both read.**
- **A Table Schema's first `missingValues` entry is the CSV null**, where it is not the empty
  string. `categories` read as `enum`. A `year` is an integer drawn inside the window.

## Rejected

- *Rejected:* a `profile explain` verb printing what each column will draw. The seed is a profile,
  and its YAML states each draw.
- *Rejected:* a window counted back from today. It would make one seed generate different rows each
  day.
- *Rejected:* datacontract-cli, pandera, dataframely or pyspark as a dependency. Each lands on every
  consumer's Glue job.
- *Rejected:* `Profile.from_table_schema` returning a bundle for a package. One name would have two
  return types.
