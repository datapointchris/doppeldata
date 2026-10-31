"""The pytest plugin doppeldata registers through the `pytest11` entry point: the `synthetic` fixture, which generates a table or
a bundle for each test, from a seed of its own.

pytest imports every `pytest11` plugin at startup, and importing a module inside `doppeldata` imports the whole package. So the
plugin is a top-level module importing only pytest and the standard library, and doppeldata is imported when a test first calls
`synthetic`. A session that never does pays nothing for it, and a doppeldata that fails to import fails only those tests. pytest
reads the entry point and nothing else does, so importing doppeldata never imports pytest. Polars, pyarrow, pandas and pyspark
are each imported only when a test asks for the type it makes.
"""

import dataclasses as dc
import hashlib
from collections.abc import Callable
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

import pytest

if TYPE_CHECKING:
    from doppeldata.column_overrides import Overrides
    from doppeldata.profile_sources import ProfileSource
    from doppeldata.scenario_format import Scenario

SEED_OPTION = '--doppeldata-seed'
PROFILES_INI = 'doppeldata_profiles'
SCENARIOS_INI = 'doppeldata_scenarios'
# Each file a profile or bundle named without a path is looked for as, in each directory `doppeldata_profiles` lists, in turn.
PROFILE_FILES = ('{}', '{}.profile.yaml', '{}.bundle.yaml', '{}.yaml', '{}.yml', '{}.json')
SCENARIO_FILES = ('{}', '{}.scenario.yaml', '{}.yaml', '{}.yml', '{}.json')
# What each `as_` makes from the generated table, and the distribution each needs, by the module it installs.
RETURN_TYPES: dict[str, dict[str, str]] = {
    'polars': {'polars': 'polars'},
    'arrow': {'pyarrow': 'pyarrow'},
    'pandas': {'pyarrow': 'pyarrow', 'pandas': 'pandas'},
    'spark': {'pyspark': 'pyspark', 'pyarrow': 'pyarrow', 'pandas': 'pandas'},
}

# Every table generated in the session, by what identifies it, so tests asking for the same data generate it once.
GENERATED = pytest.StashKey[dict[tuple[Any, ...], Any]]()
# The seeds a test's tables were generated from, which its failure report prints.
SEEDS_USED = pytest.StashKey[list[int]]()


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.getgroup('doppeldata').addoption(
        SEED_OPTION,
        type=int,
        default=None,
        metavar='N',
        help="generate every synthetic table of the run from seed N, in place of each test's own",
    )
    parser.addini(PROFILES_INI, 'directories the synthetic fixture finds a profile or bundle named without a path in', type='paths')
    parser.addini(SCENARIOS_INI, 'directories the synthetic fixture finds a scenario named without a path in', type='paths')


def pytest_report_header(config: pytest.Config) -> str | None:
    seed = config.getoption(SEED_OPTION)
    return f'doppeldata: every synthetic table generated from seed {seed}' if seed is not None else None


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Any:
    """A failed test's report, with the seeds its synthetic tables were generated from and the option that reruns them."""
    outcome = yield
    report = outcome.get_result()
    seeds = item.stash.get(SEEDS_USED, [])
    if report.failed and seeds:
        listed = ', '.join(str(seed) for seed in seeds)
        report.sections.append(('doppeldata', f'synthetic data generated from seed {listed}; rerun with {SEED_OPTION} {seeds[0]}'))


def node_seed(node_id: str) -> int:
    """A test's own seed: the first 8 bytes of its node id's SHA-256, so it is the same on every run and differs between tests."""
    return int.from_bytes(hashlib.sha256(node_id.encode()).digest()[:8], 'big')


def named_file(name: str, directories: Sequence[Path], patterns: Sequence[str]) -> Path | None:
    """The first file, or bundle directory, `name` names in `directories` by `patterns`, or None."""
    from doppeldata.profile_format import BUNDLE_INDEX

    for directory in directories:
        for pattern in patterns:
            path = directory / pattern.format(name)
            if path.is_file() or (path.is_dir() and (path / BUNDLE_INDEX).is_file()):
                return path
    return None


def scenario_of(given: 'str | Scenario', directories: Sequence[Path]) -> 'Scenario':
    """The scenario `given` names: a Scenario, a name in `directories`, a bundled example's name, or a file."""
    from doppeldata import example_scenarios
    from doppeldata.scenario_format import Scenario

    if isinstance(given, Scenario):
        return given
    path = named_file(given, directories, SCENARIO_FILES)
    if path is not None:
        return Scenario.load(path)
    if given in example_scenarios.example_names():
        return Scenario.example(given)
    if Path(given).is_file():
        return Scenario.load(given)
    searched = ', '.join(str(directory) for directory in directories) or f'no directory, since {SCENARIOS_INI} is not set'
    raise ValueError(
        f'scenario {given!r} is no file in {searched}, nor an example; name a file, or one of '
        f'{", ".join(example_scenarios.example_names())}'
    )


@dc.dataclass(frozen=True)
class SyntheticTables:
    """What the `synthetic` fixture gives a test: a call generating a table or a bundle from the test's own seed."""

    request: pytest.FixtureRequest
    seed: int

    def __call__(
        self,
        profile: 'ProfileSource | Any',
        *,
        rows: int | None = None,
        seed: int | None = None,
        scenario: 'str | Scenario | Sequence[str | Scenario] | None' = None,
        overrides: 'Overrides | None' = None,
        scale: float | None = None,
        as_: str = 'polars',
    ) -> Any:
        """The table `profile` names, `rows` rows of it from the test's seed, in the type `as_` names, as `doppeldata.sample`
        makes it. A bundle gives a dict of each table's name to its table, parents first, at `scale`.

        `profile` is a name looked for in each directory the `doppeldata_profiles` ini option lists, as NAME, NAME.profile.yaml,
        NAME.bundle.yaml, NAME.yaml, NAME.yml, NAME.json or a bundle directory NAME. Any other source `doppeldata.generate`
        takes is taken as it takes it, a bundled example's name among them. `scenario` is a name looked for the same way in
        `doppeldata_scenarios`, then a bundled example's name, a file or a Scenario, or a list of them composed in order.

        `seed`, where given, outranks the test's own: the `--doppeldata-seed` option where given, else its node id's digest.
        `as_` is `polars`, `arrow` for a pyarrow Table, `pandas`, or `spark`, which builds the DataFrame with
        `doppeldata.spark.to_spark` on the session the project's own `spark` fixture gives. Each table generates once a
        session for each profile, scenario, rows, scale, seed and overrides, and each call converts it afresh, so a test
        editing what it got changes no other test's.
        """
        from doppeldata.column_overrides import overrides_json
        from doppeldata.errors import PackagesMissing
        from doppeldata.errors import uninstalled
        from doppeldata.profile_sources import bundle_from
        from doppeldata.profile_sources import profile_from
        from doppeldata.scenario_format import Scenario
        from doppeldata.scenario_format import composed
        from doppeldata.table_or_bundle_generation import generate
        from doppeldata.table_or_bundle_generation import sample

        if as_ not in RETURN_TYPES:
            raise ValueError(f'as_ is one of {", ".join(RETURN_TYPES)}, not {as_!r}')
        missing = uninstalled(RETURN_TYPES[as_])
        if missing:
            raise PackagesMissing(as_, missing, doing=f'synthetic(as_={as_!r})')
        used = self.seed if seed is None else seed
        seeds = self.request.node.stash.setdefault(SEEDS_USED, [])
        if used not in seeds:
            seeds.append(used)
        config = self.request.config
        if isinstance(profile, str):
            found = named_file(profile, config.getini(PROFILES_INI), PROFILE_FILES)
            profile = found if found is not None else profile
        scenarios = None if scenario is None else [scenario] if isinstance(scenario, str | Scenario) else list(scenario)
        applied = composed([scenario_of(each, config.getini(SCENARIOS_INI)) for each in scenarios] if scenarios else None)
        scenario_digest = applied.digest if applied is not None else None
        bundle = bundle_from(profile)
        if bundle is not None:
            if overrides:
                raise ValueError("overrides pin one table's columns, and a bundle holds several; ask for the table's own profile")
            key: tuple[Any, ...] = ('bundle', bundle.digest, scenario_digest, scale, used)
            tables = self.generated(key, lambda: generate(bundle, seed=used, scenario=applied, scale=scale))
            return {name: self.converted(table, as_) for name, table in tables.items()}
        if scale is not None:
            raise ValueError("scale multiplies a bundle's tables, and this profile is one table; give rows")
        table_profile = profile_from(profile)
        key = ('table', table_profile.digest, scenario_digest, rows, used, overrides_json(overrides))
        table = self.generated(key, lambda: sample(table_profile, rows=rows, seed=used, scenario=applied, overrides=overrides))
        return self.converted(table, as_)

    def generated(self, key: tuple[Any, ...], generate_it: Callable[[], Any]) -> Any:
        """The session's table for `key`, generated the first time it is asked for."""
        tables = self.request.config.stash.setdefault(GENERATED, {})
        if key not in tables:
            tables[key] = generate_it()
        return tables[key]

    def converted(self, table: Any, as_: str) -> Any:
        """The engine's Arrow table as a new object of the type `as_` names."""
        if as_ == 'polars':
            import polars as pl

            return pl.DataFrame(table)
        import pyarrow as pa

        if as_ == 'arrow':
            return pa.table(table)
        if as_ == 'pandas':
            return pa.table(table).to_pandas()
        try:
            session = self.request.getfixturevalue('spark')
        except pytest.FixtureLookupError as error:
            raise LookupError(
                "synthetic(as_='spark') builds the DataFrame on the session a fixture named spark gives; define one"
            ) from error
        from doppeldata.spark import to_spark

        return to_spark(session, table)


@pytest.fixture
def synthetic(request: pytest.FixtureRequest) -> SyntheticTables:
    """Generates a table or a bundle from this test's own seed: `synthetic('orders', rows=5_000, scenario='hot-key')`.

    The seed is the first 8 bytes of the test's node id's SHA-256, or the `--doppeldata-seed` option's for every test of the
    run. So a test gets the same data on every run, and a failed test's report prints its seed.
    """
    chosen = request.config.getoption(SEED_OPTION)
    return SyntheticTables(request, node_seed(request.node.nodeid) if chosen is None else chosen)
