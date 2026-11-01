# Generating test data in pytest

The library installs a pytest plugin whose `synthetic` fixture generates a table or a bundle inside a test. Each test draws
from a seed of its own, so it gets the same data on every run, and a failure names the seed that reruns it.
[Test with synthetic data in pytest](how-to/test-with-synthetic-data.md) shows the fixture's arguments, `as_`, and a
failure's report.

## The plugin loads with no configuration, and imports nothing until a test calls it

- **The library declares the plugin under the `pytest11` entry point**, which pytest reads and nothing else does. So a
  job importing doppeldata never imports pytest, and `pytest --fixtures` lists `synthetic` wherever doppeldata is
  installed.
- **The plugin is the top-level module `_doppeldata_pytest_plugin`, and imports doppeldata when a test first calls
  `synthetic`.** pytest imports every plugin at startup, and a module inside `doppeldata` would import the whole package
  with it. So a session that never calls `synthetic` imports no doppeldata module. A doppeldata that fails to import
  fails only the tests that call it.
- **A name is looked for in the project's directories before the bundled examples.** So a project's own profile named
  `orders` wins over the example, and a test reads as its project names its tables.

## A test's seed is taken from the SHA-256 of its node id, and `--doppeldata-seed` sets every test's

- **The seed is the first 8 bytes of the SHA-256 of the test's node id**, such as `tests/test_people.py::test_people`. A
  test gets the same data on every run, and two tests get different data. Python's `hash()` is salted per process, so it
  would change every run.
- **Renaming a test, moving its file or changing a parametrize id changes its data.** Each changes the node id. So does a
  release raising the engine's stream version, which moves every test's data at once.
- **`--doppeldata-seed N` generates every test's tables from seed N**, and `seed=` in the call outranks both. N is 0 to
  2^64 − 1, and any other value stops pytest with a usage error before it collects.
- **A failed test's report prints each seed, the stream version, and the option that reruns the test's own seed.** That
  option makes the same data, so `pytest --doppeldata-seed N path::test` reproduces the failure. A table given `seed=` in
  the call is the same on every run already.

*Rejected:* `--doppeldata-seed` salting each test's node id. The printed seed would then not rerun the failure on its
own. So under the option every test shares one seed, and two tests asking for the same table get the same rows.

## A table generates once per seed, and each request converts it afresh

- **A table from the test's own seed generates once a test, and one from a shared seed once a session.** The cache holds
  the engine's Arrow table, keyed by the profile's or bundle's SHA-256, the scenario's, the rows, the scale, the seed
  and the overrides. No other test holds the test's own seed, so that table goes with the test's fixture when the test
  ends, and a suite's memory stays flat. Under `--doppeldata-seed`, or with `seed=` in the call, tests asking for the same
  data share one table for the session.
- **Each request converts the cached table into a new object.** A pandas DataFrame is mutable, so a cached one a test
  edited would leak into the next test.
- **A missing package for `as_` is refused before anything generates.** doppeldata installs none of polars, pandas or
  pyspark, so a test names what it needs. [Generating inside Spark](generating-inside-spark.md) says how `to_spark`
  builds the frame `as_='spark'` returns on each Spark version.
