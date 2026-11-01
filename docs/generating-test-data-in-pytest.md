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
- **A bundle gives a dict of each table's name to its table, parents first**, at `scale`. `rows` is refused with a bundle
  and `scale` with one table, as `generate` refuses them. Overrides are refused with a bundle, because they name one
  table's columns.

## A test's seed is a digest of its node id, and `--doppeldata-seed` sets every test's

```bash
pytest
pytest --doppeldata-seed 9
```

- **The seed is the first 8 bytes of the SHA-256 of the test's node id**, such as `tests/test_people.py::test_people`. A
  test gets the same data on every run, and two tests get different data. Python's `hash()` is salted per process, so it
  would change every run.
- **Renaming a test, moving its file or changing a parametrize id changes its data.** Each changes the node id. So does a
  release raising the engine's stream version, which moves every test's data at once. A test asserting on a drawn value
  passes `seed=`, or pins the column with `overrides=`.
- **`--doppeldata-seed N` generates every test's tables from seed N.** N is 0 to 2^64 − 1, and any other value stops pytest
  with a usage error before it collects. The report header says so:

  ```text
  doppeldata: every synthetic table generated from seed 9
  ```

- **`seed=` in the call outranks both.**
- **A failed test's report prints each seed, the stream version, and the option that reruns the test's own seed.** That
  option makes the same data, so `pytest --doppeldata-seed N path::test` reproduces the failure. A table given `seed=` in
  the call is the same on every run already:

  ```text
  ---------------------------------- doppeldata ----------------------------------
  synthetic data generated from seed 15853056665299191586 and seed 7 given in the call, at stream version 12; rerun with --doppeldata-seed 15853056665299191586
  ```

- *Rejected:* `--doppeldata-seed` salting each test's node id. The printed seed would then not rerun the failure on its
  own. So under the option every test shares one seed, and two tests asking for the same table get the same rows.

## `as_` chooses the type, and each request converts the cached table afresh

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

- **`as_='spark'` needs a fixture named `spark` that returns a SparkSession.** Without one it raises InvalidArgument, `E-USAGE-015`, saying so.
  [generating-inside-spark.md](generating-inside-spark.md) says how `to_spark` builds the frame on each Spark version.
- **A table from the test's own seed generates once a test, and one from a shared seed once a session.** The cache holds
  the engine's Arrow table, keyed by the profile's or bundle's digest, the scenario's digest, the rows, the scale, the seed
  and the overrides. No other test holds the test's own seed, so that table goes with the test's fixture when the test
  ends, and a suite's memory stays flat. Under `--doppeldata-seed`, or with `seed=` in the call, tests asking for the same
  data share one table for the session.
- **Each request converts the cached table into a new object.** A pandas DataFrame is mutable, so a cached one a test
  edited would leak into the next test.
