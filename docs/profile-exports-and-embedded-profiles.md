# Profile exports and the embedded profile

A profile already states what its data holds. `profile export` writes that contract in a format
another tool validates data with, and `--embed-profile` writes the profile into the data files it
generated. Both are opt-in: a write carries neither unless asked, and the library imports none of
the tools that read them.

## `profile export` writes a profile's contract in each format `--format` takes

```bash
doppeldata profile export orders --format pandera --out orders_schemas.py
doppeldata profile export orders --format dqdl
```

```python
import doppeldata as sd

contract = sd.export('orders', 'odcs')
```

| `--format` | Writes | Read by |
| --- | --- | --- |
| `odcs` | an Open Data Contract Standard v3.2.0 contract | datacontract-cli, `profile import --from odcs` |
| `table-schema` | a Frictionless Table Schema v2, and for a bundle a Data Package v2 | frictionless, `profile import --from table-schema`, and `--from data-package` for a bundle's |
| `json-schema` | the record schema: a JSON Schema 2020-12 of one record | any JSON Schema validator, `profile import --from json-schema` |
| `dqdl` | an AWS Glue Data Quality ruleset of each table | Glue Data Quality, Deequ |
| `pandera` | a module of a `pandera.polars.DataFrameModel` of each table | pandera, `sd.generate` |
| `dataframely` | a module of a `dataframely.Schema` of each table | dataframely, `sd.generate` |
| `pyspark-schemas` | a module of each table's `StructType` and `pandera.pyspark.DataFrameModel` | Spark, pandera |

PROFILE is a profile file, a bundle, an example's name, or a data file holding an embedded profile.
Without `--out` the export prints.

- **Each format's type names invert its importer's table**, so an export imports back as the same
  logical type, nullability, listed values and bounds. Table Schema has no integer width, decimal
  precision or time zone, and the record schema has no decimal. Those import back as the importer's
  default.
- **Data generated from a profile passes its exports.** A text source's column holding values of
  other types, such as `events-json`'s `user_id`, generates as text, and its exports type it as a
  string. The exports read which columns those are from the engine's plan, so the two never
  disagree. [column-samplers.md](column-samplers.md) says which columns generate as text.
- **A sampled profile's types are the sample's.** A column whose sampled values all parse as its
  type is exported as that type, though a row outside the sample may hold text. Profile every row
  where a strict export must accept the whole source.
- **Data a profile was made from passes its exports.** A test checks each export with the tool
  that reads it, against rows generated from a profile and against rows a profile was made from.
- **The same profile exports the same bytes.** An export names the profile's digest and holds no
  time. ODCS `examples` are generated from the profile with a fixed seed, never read from data.
- **A profile made from data is checked first.** One that fails `profile check` is refused, exit 2,
  as `CheckFailed` in Python. An export never carries a literal the privacy policy did not pass.
- **No validator is a dependency.** The modules import pandera, dataframely or pyspark when they
  run, and doppeldata imports none of them to write one.

The `orders` example exports as these, cut to a few columns:

```python
# doppeldata profile export orders --format pandera
class Orders(pa.DataFrameModel):
    order_id: pl.Int64 = pa.Field(unique=True)
    amount: Annotated[pl.Decimal, 12, 2] = pa.Field()
    status: pl.String = pa.Field(isin=['delivered', 'shipped', 'processing', 'cancelled', 'returned'])
    customer_email: pl.String = pa.Field(nullable=True)
    ship_zip: pl.String = pa.Field(str_matches='^\\d{5}$')
```

```python
# doppeldata profile export orders --format dataframely
class Orders(dy.Schema):
    order_id = dy.Int64(nullable=False, primary_key=True)
    status = dy.String(nullable=False, check=lambda column: column.is_in(['delivered', 'shipped', 'processing', 'cancelled', 'returned']))
    ship_zip = dy.String(nullable=False, regex='^\\d{5}$')
```

```python
# doppeldata profile export orders --format pyspark-schemas
ORDERS_STRUCT_TYPE = st.StructType(
    [
        st.StructField('order_id', st.LongType(), nullable=False),
        st.StructField('amount', st.DecimalType(12, 2), nullable=False),
        st.StructField('customer_email', st.StringType(), nullable=True),
    ]
)
```

```yaml
# doppeldata profile export orders --format odcs
      - name: ship_zip
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

```jsonc
// doppeldata profile export orders --format table-schema
{"name": "status", "type": "string", "constraints": {"required": true, "enum": ["delivered", "shipped", "processing", "cancelled", "returned"]}}
{"name": "customer_email", "type": "string", "format": "email"}
```

```jsonc
// doppeldata profile export orders --format json-schema
"customer_email": {"type": ["string", "null"], "format": "email"},
"ship_zip": {"type": "string", "pattern": "^\\d{5}$"}
```

```text
# doppeldata profile export orders --format dqdl
    ColumnValues "amount" between 0.49 and 4000.01 with threshold >= 0.999665,
    Mean "amount" between 90.8759 and 94.0241,
    Completeness "customer_email" >= 0.978666,
```

Each export passes the tool it is written for:

```bash
doppeldata data generate orders --rows 10000 --out orders.csv
doppeldata profile export orders --format table-schema --out orders.schema.json
uvx frictionless validate orders.csv --schema orders.schema.json
doppeldata profile export orders --format odcs --out orders.odcs.yaml
uvx --from datacontract-cli datacontract lint orders.odcs.yaml
```

## A strict format carries a fact only where the profile states it of every row

pandera, dataframely, Table Schema, the record schema and ODCS's `logicalTypeOptions` fail a frame
on one row that breaks a rule. So each carries a range, listed values, lengths, a pattern or a
format only where the profile states it holds for every row. The rest are named in the export's own
description or comment, with the share that holds:

```python
# Left out:
# - order_id: the range 11 to 99990, trimmed at k_min rows from each end
# - customer_email: the email format, which pandera has no check of
```

- **A profile's extremes are trimmed at `k_min` rows from each end**, so no strict format bounds a
  profiled column. A seed's declared bounds hold for every row, so its exports carry them.
- **A profile writes a share short of every row below 1.** Its rates keep four significant digits,
  so two breaking rows in 100,000 are written 0.9999, never 1.
- **A profile made from data never holds a key or a relationship of every row.** The privacy floor
  writes fewer than `k_min` rows sharing a key's value as none, and fewer than `k_min` orphans as
  none. So the strict formats leave each key and foreign key out and name it. DQDL carries
  `Uniqueness` and `ReferentialIntegrity` with a threshold. A seed's declared keys and
  relationships hold for every row.
- **A profile made from a sample holds nothing of every row.** Its shares are measured over the
  rows it read, so each strict format leaves out what it would carry, naming the sample.
- **A pattern is stored where its shape holds half the values.** In CMS's DE-SynPUF carrier claims
  file, `ICD9_DGNS_CD_3` matches `^\d{4}$` in about half its values. A strict format leaves it out.
  DQDL writes it with a threshold, and ODCS as an `invalidValues` quality check with a percent
  ceiling.
- **A pattern holds the share of the shape it was generalized from.** The `claims` example's
  `claim_id` is `^CLM\d{9}$`, from a shape of three letters and nine digits holding every value. A
  shape cannot say which letters, so only the dominant shape vouches for a literal.
- **A text column whose values do not all parse as its type is typed as a string.** In the carrier
  claims file, most of `HCPCS_CD_10`'s values parse as integers. A reader typed integer would turn
  the rest into nulls, so the strict formats type it string and name the integer type as left out.
  DQDL keeps its range over the values that parse.
- **The record schema of a JSON source allows each JSON type its values hold.** The `events-json`
  example's `user_id` is `{"type": ["integer", "string"]}`, and its importer reads the first.
- **The record schema requires only a column never null.** doppeldata's NDJSON writer leaves a
  null field out, as many writers do.
- **The record schema writes `multipleOf` only as 1.** A validator tests it in binary floating
  point, so `multipleOf: 0.01` rejects 68.18.

## DQDL carries each share as a threshold three standard errors below it

A ruleset is a statistical check, so it carries what a strict format leaves out. A share takes
three standard errors, `STANDARD_ERRORS`, at the profile's row count, and at least one row's worth. A mean takes 3σ/√n
and a standard deviation 3σ/√(2(n−1)). A range between trimmed extremes holds the share inside
them, 1 − 2(`k_min` − 1)/n. So `orders`' `amount` range, over 100,000 rows, takes
`with threshold >= 0.999665`.

- **DQDL's `between` excludes both ends**, so a range is widened by one step: a unit, a decimal
  place or a day.
- **A rule DQDL cannot weigh is left out and named.** `ColumnLength` takes no threshold, and DQDL
  has no rule for an email format.
- **A mean that would overflow is left out.** Spark sums a 64-bit integer column in 64 bits, so
  the mean of a 15-digit claim id wraps.
- **Read a CSV with the `pyspark-schemas` `StructType`, never with `inferSchema`.** Inference types
  a column of digit strings, such as an NPI, as a long, which its `matches` rule then fails.
- **Deequ ignores `with threshold` on a numeric or string `ColumnValues` rule.** It asserts
  every row and reports the share in its failure reason. The DQDL reference documents the
  threshold as the share of rows that must pass. The carrier claims export passes Deequ on the file
  it was profiled from, once each reported share is compared with its threshold.

```python
# doppeldata profile export orders --format pyspark-schemas --out orders_spark.py
from orders_spark import ORDERS_STRUCT_TYPE, Orders

frame = spark.read.option('header', True).schema(ORDERS_STRUCT_TYPE).csv('orders.csv')
validated = Orders.validate(check_obj=frame)
dict(validated.pandera.errors)   # empty where every check passes
```

## A seed exports only what its schema declared

```bash
doppeldata profile import --from odcs orders.odcs.yaml --out orders-seed.profile.yaml
doppeldata profile export orders-seed.profile.yaml --format pandera
```

A seed's `generated.declared` names each fact its schema stated, as
[seed-profiles-from-schemas.md](seed-profiles-from-schemas.md) lists. An export of a seed carries
those facts as exact, and no draw default. So `ordered_at`, which draws inside `DATE_WINDOW`
because the contract bounds it nowhere, exports unbounded. A seed is exported as it is, without
`profile check`, which fails every seed.

## A bundle exports as a Data Package and as DQDL with `ReferentialIntegrity`

```bash
doppeldata profile create shop/ --tables --out shop.bundle.yaml
doppeldata profile export shop.bundle.yaml --format table-schema --out datapackage.json
doppeldata profile export shop.bundle.yaml --format dqdl --out shop.dqdl
doppeldata profile export shop.bundle.yaml --format json-schema --table orders
```

```text
ReferentialIntegrity "customer_id" "customers.customer_id" >= 0.99945,
```

- **Each table is a resource or a ruleset.**
- **A seed bundle's relationships are foreign keys.** One imported from a Data Package holding
  `foreignKeys` exports them again, and DQDL as `ReferentialIntegrity ... = 1.0`.
- **A profiled bundle's Data Package names each relationship it left out.** The privacy floor
  writes fewer than `k_min` orphans as none, so no relationship of it holds for every row. DQDL
  takes a threshold below the share the profile states, as for any share, less the rows the floor
  may hide.
- **`ReferentialIntegrity` names the parent by its table name.** A run passes the parent's frame as
  an additional data source under that alias.
- **The record schema describes one record, so a bundle takes `--table`**, or `table=` in Python.
  ODCS writes each table as a schema object, and the modules a class each.

## `--embed-profile` writes the profile into every Parquet and Arrow file it generates

```bash
doppeldata data generate orders --rows 100000 --seed 7 --format parquet --embed-profile --out orders/
doppeldata profile export orders/part-00000-fe89a3c3.parquet --format pandera --out orders_schemas.py
doppeldata profile expand orders/part-00000-fe89a3c3.parquet --out orders.profile.yaml
doppeldata data generate orders/part-00000-fe89a3c3.parquet --out again/
```

```python
sd.write('orders', 'orders/', rows=100_000, seed=7, embed_profile=True)
profile = sd.Profile.from_data_file('orders/part-00000-fe89a3c3.parquet')
```

```sql
select decode(value) from parquet_kv_metadata('orders/*.parquet') where decode(key) = 'doppeldata.profile'
```

A file read alone then holds the profile and scenario that made it, beside the seed and rows its
provenance keys already name. Every PROFILE argument takes such a file.

- **The key is `doppeldata.profile`, beside the provenance keys.** It holds the whole profile as
  canonical JSON. Its digest, taken without the generated fields a digest leaves out, is
  `doppeldata.profile_digest`. It runs to kilobytes.
- **`doppeldata.scenario` holds the scenario's canonical document**, as the manifest's
  `scenario.document` does. A write under no scenario writes no such key.
- **A file whose profile or scenario is not the one its digest names is refused**, exit 2, as
  `SourceError` in Python.
- **Parquet holds it once, as a plain key-value pair.** The other `doppeldata.*` keys sit in
  `ARROW:schema` as well, and a copy of the profile there would double its bytes. Arrow IPC holds
  it in the schema metadata.
- **Only Parquet and Arrow IPC hold it.** A write in another format refuses `--embed-profile`
  before writing a byte, exit 2, and Python raises `WriteFailed`.
- **A profile made from data that fails `profile check` is refused**, before writing a byte, exit
  2, and Python raises `CheckFailed`. The file carries every literal the profile holds, so it must
  pass the check an export passes.
- **`data generate FILE` defaults `--seed`, `--rows` and `--scenario` to what the file's write
  recorded**, and prints each value it took from the file. So the last command above writes the
  dataset `orders/` holds. `data plan` takes the same defaults, and `scenario check` takes the
  seed and rows. Python's `generate`, `stream`, `plan`, `write` and `spark` verbs take a data file
  the same way, each value left None. A key is read only where its value is left unset, so a
  malformed seed is refused only when the seed is not given.
- **Given none of the three, a file whose dataset this release would not make again is refused**,
  exit 2, naming the key that differs, such as `doppeldata.stream_version`. Given any of them, the
  write generates the dataset they make.
- **It is the profile as given, before a scenario's patch.** The scenario applies again from
  `doppeldata.scenario`.
- **A `cdc` write embeds both in the `LOAD` files and the `_expected` parts, never in a change
  file.** A stream writes thousands of change files, and a copy of the profile can outweigh the
  rows in each one.
- **The manifest's `profile.embedded` is true.** `regenerate()` refuses such a manifest, because a
  plan holds the profile's digest but never its JSON. `data generate FILE --embed-profile` writes
  the same dataset digest again, and the same bytes where the write used the profile's own format
  and layout.
- **`Profile.from_data_file` reads the footer through the extension**, so it needs no pyarrow. A
  file holding no profile raises `SourceError`, naming `embed_profile=True`.

## Rejected

- *Rejected:* frictionless, pandera, dataframely or datacontract-cli as a library dependency. Each lands on every consumer's Glue job, and only the test suite runs them.
- *Rejected:* one module holding a model of each pandera backend, each defined under `try`. A class
  defined conditionally breaks a type checker, so `pyspark-schemas` is its own format.
- *Rejected:* a strict format bounding a column at its trimmed extremes. It fails real data the
  profile describes, on the rows the trim left out.
- *Rejected:* ODCS `examples` read from the data. A profile holds no rows, and an export holds no
  value its profile does not.
- *Rejected:* `profile import --from data-file`. `profile expand FILE --out` writes the embedded
  profile as YAML.
- *Rejected:* embedding the profile by default. Out of the box a write generates data alone.
