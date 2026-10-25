"""The hand-written example profiles the library ships under `doppeldata/examples/`."""

import importlib.resources
from importlib.resources.abc import Traversable

EXAMPLE_SUFFIX = '.profile.yaml'


def example_names() -> list[str]:
    """The example profiles' names, as `Profile.example` and `doppeldata profile show` take them."""
    examples = importlib.resources.files('doppeldata').joinpath('examples')
    return sorted(entry.name.removesuffix(EXAMPLE_SUFFIX) for entry in examples.iterdir() if entry.name.endswith(EXAMPLE_SUFFIX))


def example_resource(name: str) -> Traversable:
    if name not in example_names():
        raise ValueError(f'{name!r} is not an example profile doppeldata ships; choose from {", ".join(example_names())}')
    return importlib.resources.files('doppeldata').joinpath('examples', name + EXAMPLE_SUFFIX)


def example_text(name: str) -> str:
    """An example profile's YAML as shipped, comments included."""
    return example_resource(name).read_text()
