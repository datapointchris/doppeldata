"""The hand-written example profiles and bundles the library ships under `doppeldata/examples/`."""

import dataclasses as dc
import importlib.resources
from importlib.resources.abc import Traversable
from typing import Any

from doppeldata.errors import InvalidArgument
from doppeldata.issue_codes import PROFILE_NOT_FOUND

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
        raise InvalidArgument(PROFILE_NOT_FOUND, f'{name!r} is not an example profile doppeldata ships', value=name, valid=example_names())
    return importlib.resources.files('doppeldata').joinpath('examples', name + EXAMPLE_SUFFIX)


def bundle_example_resource(name: str) -> Traversable:
    if name not in bundle_example_names():
        message = f'{name!r} is not an example bundle doppeldata ships'
        raise InvalidArgument(PROFILE_NOT_FOUND, message, value=name, valid=bundle_example_names())
    return importlib.resources.files('doppeldata').joinpath('examples', name + BUNDLE_SUFFIX)


def example_text(name: str) -> str:
    """An example profile's YAML as shipped, comments included."""
    return example_resource(name).read_text()


def bundle_example_text(name: str) -> str:
    """An example bundle's YAML as shipped, comments included."""
    return bundle_example_resource(name).read_text()


@dc.dataclass(frozen=True)
class ShippedExample:
    """An example profile or bundle as shipped: its name, `table` or `bundle`, its YAML with comments, and its document."""

    name: str
    kind: str
    text: str
    document: dict[str, Any]


def shipped_example(name: str) -> ShippedExample:
    """The example profile or bundle `name`, refused at `name` with every example's name where it is neither."""
    from doppeldata.profile_format import Bundle
    from doppeldata.profile_format import Profile

    if name in bundle_example_names():
        return ShippedExample(name, 'bundle', bundle_example_text(name), Bundle.example(name).to_plain())
    if name in example_names():
        return ShippedExample(name, 'table', example_text(name), Profile.example(name).to_plain())
    message = f'{name!r} is not an example profile or bundle doppeldata ships'
    raise InvalidArgument(PROFILE_NOT_FOUND, message, field='name', value=name, valid=[*example_names(), *bundle_example_names()])


def shipped_examples() -> list[dict[str, Any]]:
    """Each example as `profile list` lists it: its name, kind and rows, then a table's columns or a bundle's tables and
    relationships."""
    from doppeldata.profile_format import Bundle
    from doppeldata.profile_format import Profile

    listed: list[dict[str, Any]] = []
    for name in example_names():
        document = Profile.example(name).document
        listed.append({'name': name, 'kind': 'table', 'rows': document['dataset']['rows'], 'columns': len(document['columns'])})
    for name in bundle_example_names():
        bundle = Bundle.example(name)
        listed.append(
            {
                'name': name,
                'kind': 'bundle',
                'rows': sum(table.document['dataset']['rows'] for table in bundle.tables.values()),
                'tables': len(bundle.tables),
                'relationships': len(bundle.document.get('relationships') or []),
            }
        )
    return listed
