# Test with synthetic data in pytest

Installing doppeldata installs a pytest plugin. Its `synthetic` fixture generates a table or a
bundle inside a test, from a seed of the test's own. So a test gets the same rows on every run, and
a failure prints the seed that reruns it.

```python
def test_five_customers_place_most_orders(synthetic):
    orders = synthetic('orders', rows=5_000, scenario='hot-key')
    top_five = orders['customer_id'].value_counts(sort=True)['count'].head(5).sum()
    assert top_five / orders.height > 0.55


def test_shipped_orders_have_a_zip(synthetic):
    orders = synthetic('orders', rows=1_000, overrides={'status': 'shipped'})
    assert orders['status'].unique().to_list() == ['shipped']
    assert orders['ship_zip'].null_count() == 0


def test_every_order_names_a_customer(synthetic):
    shop = synthetic('orders-customers', scale=0.01)
    assert list(shop) == ['customers', 'orders']
    customers = set(shop['customers']['customer_id'])
    assert shop['orders']['customer_id'].drop_nulls().is_in(customers).mean() > 0.99


def test_people_from_the_project_profile(synthetic):
    people = synthetic('people', rows=200, as_='arrow')
    assert people.num_rows == 200
    assert set(people.column('plan').to_pylist()) <= {'free', 'pro', 'team'}
```

```toml
[tool.pytest.ini_options]
doppeldata_profiles = "tests/profiles"
doppeldata_scenarios = "tests/scenarios"
```

```bash
pytest
pytest --doppeldata-seed 9
pytest --fixtures | grep -A1 '^synthetic'
```

```text
$ pytest -q tests
....                                                                     [100%]
4 passed in 0.40s
```

- **The tests need doppeldata, pytest, and polars and pyarrow for the types they ask for**,
  installed as [the README](../../README.md#install) says.
- **`tests/profiles/people.yaml` is the short YAML** [Generate rows from column
  names](generate-rows-from-column-names.md) writes.
- **`orders`, `orders-customers` and `hot-key` are bundled examples.**

## `synthetic` takes a name, rows, a scenario and overrides

| Argument | Takes |
| --- | --- |
| the first | a name looked for in each `doppeldata_profiles` directory, as `NAME`, `NAME.profile.yaml`, `NAME.bundle.yaml`, `NAME.yaml`, `NAME.yml`, `NAME.json` or a bundle directory `NAME`, then a bundled example. A `Profile`, a list of column names, a dict of fakes or a path, as `sd.generate` takes them |
| `rows` | the rows of one table |
| `scale` | a bundle's scale. `rows` is refused with a bundle, and `scale` with one table |
| `scenario` | a name looked for in each `doppeldata_scenarios` directory, as `NAME`, `NAME.scenario.yaml`, `NAME.yaml`, `NAME.yml` or `NAME.json`, then a bundled example or a file. A `Scenario`, or a list composed in order |
| `overrides` | a value pinned per column, as `sd.sample` takes it. A conditional child of the column draws from the pinned value. Refused with a bundle |
| `seed` | a seed that outranks the test's own and `--doppeldata-seed` |
| `as_` | the type returned, below |

- **A project's own profile wins over a bundled example of the same name.**
- **A bundle gives a dict of each table's name to its table, parents first.**
- **`pytest --fixtures` lists the fixture wherever doppeldata is installed:**

  ```text
  synthetic -- .venv/lib/python3.12/site-packages/_doppeldata_pytest_plugin.py:277
      Generates a table or a bundle from this test's own seed: `synthetic('orders', rows=5_000, scenario='hot-key')`.
  ```

- **`pytest -p no:doppeldata` turns the plugin off.**

## `as_` picks the type a test gets

| `as_` | Returns | Needs |
| --- | --- | --- |
| `polars`, the default | a Polars DataFrame | polars |
| `arrow` | a pyarrow Table | pyarrow |
| `pandas` | a pandas DataFrame, through pyarrow | pyarrow, pandas |
| `spark` | a Spark DataFrame, on the session the project's own `spark` fixture gives | pyspark, pyarrow, pandas |

doppeldata installs none of them. A missing one fails the test before anything generates:

```text
E           doppeldata.errors.PackagesMissing: E-USAGE-019: synthetic(as_='pandas') needs pandas, which doppeldata does not install; run `pip install pandas`
```

`as_='spark'` needs a fixture named `spark` that returns a SparkSession, and raises `E-USAGE-015`
without one.

## A failed test prints the seed that reruns it

```text
$ pytest failing/test_amounts.py
...
>       assert orders['amount'].max() < 500
E       AssertionError: assert Decimal('1680.46') < 500
...
---------------------------------- doppeldata ----------------------------------
synthetic data generated from seed 310323816371029483, at stream version <n>; rerun with --doppeldata-seed 310323816371029483
$ pytest --doppeldata-seed 310323816371029483 failing/test_amounts.py::test_no_order_tops_500
...
doppeldata: every synthetic table generated from seed 310323816371029483
...
E       AssertionError: assert Decimal('1680.46') < 500
```

- **`--doppeldata-seed N` generates every test's tables from seed N**, and the report header says
  so. N is 0 to 2^64 − 1.
- **A test's own seed comes from its node id.** Renaming the test, moving its file or changing a
  parametrize id gives it other rows, and so does a release that raises the engine's stream
  version. A test asserting on a drawn value passes `seed=`, or pins the column with `overrides=`.

## Why

[Generating test data in pytest](../generating-test-data-in-pytest.md) says why the plugin imports
nothing until a test calls it, how a test's seed is derived, and how the tables are cached.
