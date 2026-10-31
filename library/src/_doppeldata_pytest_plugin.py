"""The pytest plugin doppeldata registers through the `pytest11` entry point: the `synthetic` fixture, which generates a table or
a bundle for each test, from a seed of its own.

pytest imports every `pytest11` plugin at startup, and importing a module inside `doppeldata` imports the whole package. So the
plugin is a top-level module importing only pytest and the standard library, and doppeldata is imported when a test first calls
`synthetic`. A session that never does pays nothing for it, and a doppeldata that fails to import fails only those tests. pytest
reads the entry point and nothing else does, so importing doppeldata never imports pytest. Polars, pyarrow, pandas and pyspark
are each imported only when a test asks for the type it makes.
"""

import argparse
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
SEED_LIMIT = 2**64
# What each `as_` makes from the generated table, and the distribution each needs, by the module it installs.
RETURN_TYPES: dict[str, dict[str, str]] = {
    'polars': {'polars': 'polars'},
    'arrow': {'pyarrow': 'pyarrow'},
    'pandas': {'pyarrow': 'pyarrow', 'pandas': 'pandas'},
    'spark': {'pyspark': 'pyspark', 'pyarrow': 'pyarrow', 'pandas': 'pandas'},
}

# Every table generated in the session from a seed tests share, `--doppeldata-seed`'s or one given in the call, by what
# identifies it, so tests asking for the same data generate it once.
GENERATED = pytest.StashKey[dict[tuple[Any, ...], Any]]()
# The test's own seed where a table of it was generated from it, and each seed a call gave, which its failure report prints.
OWN_SEED = pytest.StashKey[int]()
GIVEN_SEEDS = pytest.StashKey[list[int]]()


def seed_option(text: str) -> int:
    """`--doppeldata-seed`'s value, refused before collection where the engine takes no such seed."""
    seed = int(text)
    if not 0 <= seed < SEED_LIMIT:
        raise argparse.ArgumentTypeError(f'a seed is 0 to {SEED_LIMIT - 1}, not {text}')
    return seed


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.getgroup('doppeldata').addoption(
        SEED_OPTION,
        type=seed_option,
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
    """A failed test's report: the seeds its synthetic tables were generated from, the stream version, and, where any came from
    the test's own seed, the option that reruns them. A seed given in the call regenerates its table on every run."""
    outcome = yield
    report = outcome.get_result()
    own, given = item.stash.get(OWN_SEED, None), item.stash.get(GIVEN_SEEDS, [])
    if not report.failed or (own is None and not given):
        return
    from doppeldata import _core

    seeds = ([f'seed {own}'] if own is not None else []) + [f'seed {seed} given in the call' for seed in given]
    text = f'synthetic data generated from {" and ".join(seeds)}, at stream version {_core.STREAM_VERSION}'
    report.sections.append(('doppeldata', text + (f'; rerun with {SEED_OPTION} {own}' if own is not None else '')))


def node_seed(node_id: str) -> int:
    """A test's own seed: the first 8 bytes of its node id's SHA-256, so it is the same on every run and differs between tests."""
    return int.from_bytes(hashlib.sha256(node_id.encode()).digest()[:8], 'big')


def named_file(name: str, directories: Sequence[Path], suffixes: Sequence[str]) -> Path | None:
    """The first file, or bundle directory, `name` with one of `suffixes` names in `directories`, or None."""
    from doppeldata.profile_format import BUNDLE_INDEX

    for directory in directories:
        for suffix in suffixes:
            path = directory / f'{name}{suffix}'
            if path.is_file() or (path.is_dir() and (path / BUNDLE_INDEX).is_file()):
                return path
    return None


def profile_suffixes() -> tuple[str, ...]:
    """What a profile or bundle named without a path is looked for as: the name, then each suffix the library reads."""
    from doppeldata.example_profiles import BUNDLE_SUFFIX
    from doppeldata.example_profiles import EXAMPLE_SUFFIX
    from doppeldata.profile_sources import DOCUMENT_SUFFIXES

    return ('', EXAMPLE_SUFFIX, BUNDLE_SUFFIX, *DOCUMENT_SUFFIXES)


def scenario_suffixes() -> tuple[str, ...]:
    """What a scenario named without a path is looked for as: the name, then each suffix the library reads."""
    from doppeldata.example_scenarios import EXAMPLE_SUFFIX
    from doppeldata.profile_sources import DOCUMENT_SUFFIXES

    return ('', EXAMPLE_SUFFIX, *DOCUMENT_SUFFIXES)


def scenario_of(given: 'str | Scenario', directories: Sequence[Path]) -> 'Scenario':
    """The scenario `given` names: a Scenario, a name in `directories`, a bundled example's name, or a file."""
    from doppeldata import example_scenarios
    from doppeldata.scenario_format import Scenario

    if isinstance(given, Scenario):
        return given
    path = named_file(given, directories, scenario_suffixes())
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
    """What the `synthetic` fixture gives a test: a call generating a table or a bundle from the test's own seed.

    `seed` is the test's own seed, which `shared` says came from `--doppeldata-seed` and so is every test's. A table generated
    from a seed no other test holds is kept in `own_tables`, which goes with the fixture when the test ends.
    """

    request: pytest.FixtureRequest
    seed: int
    shared: bool
    own_tables: dict[tuple[Any, ...], Any] = dc.field(default_factory=dict, repr=False)

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
        takes is taken as it takes it, and a bundled example's name, a bundle file or a bundle directory as a Bundle. `rows`
        sizes one table and `scale` a bundle, and each is refused on the other. `scenario` is a name looked for the same way in
        `doppeldata_scenarios`, then a bundled example's name, a file or a Scenario, or a list of them composed in order.

        `seed`, where given, outranks the test's own: the `--doppeldata-seed` option where given, else its node id's digest.
        `as_` is `polars`, `arrow` for a pyarrow Table, `pandas`, or `spark`, which builds the DataFrame with
        `doppeldata.spark.to_spark` on the session the project's own `spark` fixture gives. A table generates once for each
        profile, scenario, rows, scale, seed and overrides: once a test from the test's own seed, and once a session from a seed
        tests share. Each call converts it afresh, so a test editing what it got changes no other test's.
        """
        from doppeldata.bundle_generation import BundleRun
        from doppeldata.bundle_generation import run_of
        from doppeldata.column_overrides import materialized
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
        config = self.request.config
        if isinstance(profile, str):
            found = named_file(profile, config.getini(PROFILES_INI), profile_suffixes())
            profile = found if found is not None else profile
        bundle = bundle_from(profile)
        used = self.seed if seed is None else seed
        run = run_of(bundle if bundle is not None else profile, rows, used, scale)
        if isinstance(run, BundleRun) and overrides is not None:
            raise TypeError("overrides pin one table's columns, and a bundle holds several; ask synthetic for the table's profile")
        self.record(seed)
        scenarios = None if scenario is None else [scenario] if isinstance(scenario, str | Scenario) else list(scenario)
        applied = composed([scenario_of(each, config.getini(SCENARIOS_INI)) for each in scenarios] if scenarios else None)
        scenario_digest = applied.digest if applied is not None else None
        tables = self.request.config.stash.setdefault(GENERATED, {}) if self.shared or seed is not None else self.own_tables
        if isinstance(run, BundleRun):
            key: tuple[Any, ...] = ('bundle', run.bundle.digest, scenario_digest, run.scale, used)
            generated = self.generated(tables, key, lambda: generate(run.bundle, seed=used, scenario=applied, scale=run.scale))
            return {name: self.converted(table, as_) for name, table in generated.items()}
        table_profile = profile_from(profile)
        pinned = materialized(overrides)
        key = ('table', table_profile.digest, scenario_digest, rows, used, overrides_json(pinned))
        table = self.generated(tables, key, lambda: sample(table_profile, rows=rows, seed=used, scenario=applied, overrides=pinned))
        return self.converted(table, as_)

    def record(self, given: int | None) -> None:
        """Records the seed a call generated from for the failure report: the test's own, or the one the call gave."""
        stash = self.request.node.stash
        if given is None:
            stash[OWN_SEED] = self.seed
        elif given not in (seeds := stash.setdefault(GIVEN_SEEDS, [])):
            seeds.append(given)

    @staticmethod
    def generated(tables: dict[tuple[Any, ...], Any], key: tuple[Any, ...], generate_it: Callable[[], Any]) -> Any:
        """The table `tables` holds for `key`, generated the first time it is asked for."""
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
    if chosen is None:
        return SyntheticTables(request, node_seed(request.node.nodeid), shared=False)
    return SyntheticTables(request, chosen, shared=True)
