"""The hand-written example scenarios the library ships under `doppeldata/examples/`, each written for the `orders` example profile."""

import difflib
import importlib.resources
from importlib.resources.abc import Traversable

from doppeldata.errors import InvalidArgument
from doppeldata.issue_codes import SCENARIO_NOT_FOUND

EXAMPLE_SUFFIX = '.scenario.yaml'


def example_names() -> list[str]:
    """The example scenarios' names, as `Scenario.example` and `--scenario` take them."""
    examples = importlib.resources.files('doppeldata').joinpath('examples')
    return sorted(entry.name.removesuffix(EXAMPLE_SUFFIX) for entry in examples.iterdir() if entry.name.endswith(EXAMPLE_SUFFIX))


def example_resource(name: str) -> Traversable:
    if name not in example_names():
        examples = example_names()
        message = f'{name!r} is not an example scenario doppeldata ships; choose from {", ".join(examples)}'
        raise InvalidArgument(SCENARIO_NOT_FOUND, message, value=name, valid=examples, closest=difflib.get_close_matches(name, examples))
    return importlib.resources.files('doppeldata').joinpath('examples', name + EXAMPLE_SUFFIX)


def example_text(name: str) -> str:
    """An example scenario's YAML as shipped, comments included."""
    return example_resource(name).read_text()
