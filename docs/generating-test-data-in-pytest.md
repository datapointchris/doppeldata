# Generating test data in pytest

The library installs a pytest plugin whose `synthetic` fixture generates a table or a bundle inside a test. Each test draws
from a seed of its own, so it gets the same data on every run, and a failure names the seed that reruns it.

## A test asks `synthetic` for a table by name, and gets it as a Polars DataFrame

```python
def test_five_customers_place_most_orders(synthetic):
    orders = synthetic('orders', rows=5_000, scenario='hot-key')
    top_five = orders['customer_id'].value_counts(sort=True)['count'].head(5).sum()
    assert top_five / orders.height > 0.55
```

```toml
[tool.pytest.ini_options]
doppeldata_profiles = "tests/profiles"
doppeldata_scenarios = "tests/scenarios"
```

- **The plugin loads with no configuration.** The library declares it under the `pytest11` entry point, which pytest reads
  and nothing else does. So a job importing doppeldata never imports pytest, and `pytest --fixtures` lists `synthetic`
  wherever doppeldata is installed.
- **The plugin is the top-level module `_doppeldata_pytest_plugin`, and imports doppeldata when a test first calls
  `synthetic`.** pytest imports every plugin at startup, and a module inside `doppeldata` would import the whole package
  with it. So a session that never calls `synthetic` imports no doppeldata module. A doppeldata that fails to import
  fails only the tests that call it. `pytest -p no:doppeldata` turns the plugin off.
- **A name is looked for in each directory `doppeldata_profiles` lists, in turn.** It is tried as `NAME`,
  `NAME.profile.yaml`, `NAME.bundle.yaml`, `NAME.yaml`, `NAME.yml`, `NAME.json`, and a bundle directory `NAME`. A name
  found in none is taken as `sd.generate` takes it, so a bundled example such as `orders` resolves. A project's own
  profile of that name wins over the example.
- **Any other source `sd.generate` takes is taken too**: a `Profile`, a list of column names, a dict of fakes, or a path.
- **`scenario` resolves the same way from `doppeldata_scenarios`**, as `NAME`, `NAME.scenario.yaml`, `NAME.yaml`,
  `NAME.yml` or `NAME.json`, then as a bundled example or a file. It also takes a `Scenario`, or a list composed in order.
- **`rows`, `scenario` and `overrides` are taken as `sd.sample` takes them.** So `overrides={'channel': 'store'}` pins a
  column, and a conditional child of it draws from the pinned value.
- **A bundle gives a dict of each table's name to its table, parents first**, at `scale`. Overrides are refused with a
  bundle, because they name one table's columns.

## A test's seed is a digest of its node id, and `--doppeldata-seed` sets every test's

```bash
pytest
pytest --doppeldata-seed 9
```

- **The seed is the first 8 bytes of the SHA-256 of the test's node id**, such as `tests/test_people.py::test_people`. A
  test gets the same data on every run, and two tests get different data. Python's `hash()` is salted per process, so it
  would change every run.
- **`--doppeldata-seed N` generates every test's tables from seed N.** The report header says so:

  ```text
  doppeldata: every synthetic table generated from seed 9
  ```

- **`seed=` in the call outranks both.**
- **A failed test's report prints its seed and the option that reruns it.** That option makes the same data, so
  `pytest --doppeldata-seed N path::test` reproduces the failure:

  ```text
  ---------------------------------- doppeldata ----------------------------------
  synthetic data generated from seed 11934420282479109664; rerun with --doppeldata-seed 11934420282479109664
  ```

- *Rejected:* `--doppeldata-seed` salting each test's node id. The printed seed would then not rerun the failure on its
  own. So under the option every test shares one seed, and two tests asking for the same table get the same rows.

## `as_` chooses the type, and each request converts the session's one table afresh

| `as_` | Returns | Needs |
| --- | --- | --- |
| `polars`, the default | a Polars DataFrame | polars |
| `arrow` | a pyarrow Table | pyarrow |
| `pandas` | a pandas DataFrame, through pyarrow | pyarrow, pandas |
| `spark` | a DataFrame from `doppeldata.spark.to_spark`, on the session the project's own `spark` fixture gives | pyspark, pyarrow, pandas |

- **A missing package is refused before anything generates**, as `PackagesMissing`:

  ```text
  synthetic(as_='spark') needs pyspark, pyarrow, pandas, which doppeldata does not install; run `pip install pyspark pyarrow pandas`
  ```

- **`as_='spark'` needs a fixture named `spark` that returns a SparkSession.** Without one it raises LookupError saying so.
  [generating-inside-spark.md](generating-inside-spark.md) says how `to_spark` builds the frame on each Spark version.
- **Each table generates once a session.** The cache holds the engine's Arrow table, keyed by the profile's or bundle's
  digest, the scenario's digest, the rows, the scale, the seed and the overrides. So ten tests asking for the same data pay
  once.
- **Each request converts the cached table into a new object.** A pandas DataFrame is mutable, so a cached one a test
  edited would leak into the next test.
