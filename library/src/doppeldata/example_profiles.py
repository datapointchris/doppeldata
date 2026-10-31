"""The hand-written example profiles and bundles the library ships under `doppeldata/examples/`."""

import importlib.resources
from importlib.resources.abc import Traversable

EXAMPLE_SUFFIX = '.profile.yaml'
BUNDLE_SUFFIX = '.bundle.yaml'


def shipped(suffix: str) -> list[str]:
    examples = importlib.resources.files('doppeldata').joinpath('examples')
    return sorted(entry.name.removesuffix(suffix) for entry in examples.iterdir() if entry.name.endswith(suffix))


def example_names() -> list[str]:
    """The example profiles' names, as `Profile.example` and `doppeldata profile show` take them."""
    return shipped(EXAMPLE_SUFFIX)


def bundle_example_names() -> list[str]:
    """The example bundles' names, as `Bundle.example` and the command line's PROFILE take them."""
    return shipped(BUNDLE_SUFFIX)


def example_resource(name: str) -> Traversable:
    if name not in example_names():
        raise ValueError(f'{name!r} is not an example profile doppeldata ships; choose from {", ".join(example_names())}')
    return importlib.resources.files('doppeldata').joinpath('examples', name + EXAMPLE_SUFFIX)


def bundle_example_resource(name: str) -> Traversable:
    if name not in bundle_example_names():
        raise ValueError(f'{name!r} is not an example bundle doppeldata ships; choose from {", ".join(bundle_example_names())}')
    return importlib.resources.files('doppeldata').joinpath('examples', name + BUNDLE_SUFFIX)


def example_text(name: str) -> str:
    """An example profile's YAML as shipped, comments included."""
    return example_resource(name).read_text()


def bundle_example_text(name: str) -> str:
    """An example bundle's YAML as shipped, comments included."""
    return bundle_example_resource(name).read_text()
