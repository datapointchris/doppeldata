"""The short forms of a profile, each expanded to the ordinary profile that generates its data.

A short form is a list of column names, a dict of inline columns, or a short YAML file of `rows` and a `columns` map.
A column given no spec takes the fake its name hints: a type the general pack hints by that name first, then a provider
by its own name or one of `PROVIDER_HINTS`.
"""

import difflib
import os
import re
from pathlib import Path
from typing import Any

from ruamel.yaml.comments import CommentedMap

from doppeldata import fakes
from doppeldata.column_name_words import hint_runs_in
from doppeldata.column_name_words import name_words
from doppeldata.embedded_profiles import data_file_metadata
from doppeldata.embedded_profiles import data_file_profile
from doppeldata.example_profiles import example_names
from doppeldata.inline_columns import Fake
from doppeldata.inline_columns import InlineColumn
from doppeldata.inline_columns import choice
from doppeldata.inline_columns import dates
from doppeldata.inline_columns import decimals
from doppeldata.inline_columns import integers
from doppeldata.name_hinted_types import GENERAL
from doppeldata.name_hinted_types import type_named_by
from doppeldata.profile_format import FORMAT_VERSION
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import Issue
from doppeldata.profile_format import Profile
from doppeldata.profile_format import pointer
from doppeldata.profile_format import read_document
from doppeldata.profile_format import to_plain

# The table a profile expanded from columns given in Python or on the command line is named.
INLINE_TABLE = 'table'
LIST_SOURCE = 'the column list'
DICT_SOURCE = 'the column dict'
# The words that hint each provider the general pack hints no type for, beside the provider's own name. Each must end a
# column's name, so `billing_city` takes a city and `city_count` nothing.
PROVIDER_HINTS = {
    'street_address': ('street', 'address', 'address_line'),
    'city': ('city', 'town'),
    'country': ('country',),
    'company': ('company', 'company_name', 'employer', 'organization'),
    'job_title': ('job', 'occupation'),
    'username': ('user_name',),
    'state': ('state_name',),
}
CLOSEST_FAKES = 3
# How near a fake's name or hint must be to a name to be offered, as difflib's ratio. Below it, `age` offers `name`.
CLOSEST_CUTOFF = 0.7
# What a name matching no fake is told to give instead, by where the name was written.
PYTHON_REMEDY = 'give it one from sd.fakes, sd.integers, sd.decimals, sd.dates or sd.choice in a dict'
SHORT_YAML_REMEDY = 'give it a fake, a low..high range such as 18..90, or a list of values'
COMMAND_LINE_REMEDY = f'{SHORT_YAML_REMEDY}, in a short YAML'
SHORT_KEYS = ('rows', 'columns')
# A short YAML's range: two integers, two numbers or two ISO dates, joined by `..`.
RANGE = re.compile(r'^\s*(?P<low>\S+?)\s*\.\.\s*(?P<high>\S+)\s*$')
INTEGER = re.compile(r'^[+-]?\d+$')
NUMBER = re.compile(r'^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$')
DATE = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def providers() -> list[str]:
    """Every provider the engine draws from, in the order `sd.fakes` lists them."""
    return [value.provider for value in vars(fakes).values() if isinstance(value, Fake)]


def with_row_count(document: Any, rows: int) -> Any:
    """`document` recording `rows` as its row count, in a `dataset` section placed before the columns where it has none."""
    if 'dataset' in document:
        document['dataset']['row_count'] = rows
        return document
    at = list(document).index('columns') if 'columns' in document else len(document)
    if isinstance(document, CommentedMap):
        document.insert(at, 'dataset', {'row_count': rows})
        return document
    items = list(document.items())
    return dict([*items[:at], ('dataset', {'row_count': rows}), *items[at:]])


def file_profile(source: str | os.PathLike[str]) -> Profile:
    """A short or full YAML file's profile, the profile a Parquet or Arrow IPC file embeds, or the example `source` names."""
    path = Path(source)
    if not path.is_file():
        if isinstance(source, str) and source in example_names():
            return Profile.example(source)
        raise FileNotFoundError(
            f'{os.fspath(source)} is neither a profile file nor an example; name a file, or one of {", ".join(example_names())}'
        )
    metadata = data_file_metadata(path)
    if metadata is not None:
        return data_file_profile(path, metadata)
    document = to_plain(read_document(path))
    if not is_short(document):
        return Profile.load(path)
    return validated(short_expanded(document, path), str(path))


def is_short(document: Any) -> bool:
    """Whether a file's document is the short form: a `columns` key, and neither `profile_version` nor `kind`."""
    return isinstance(document, dict) and 'columns' in document and not {'profile_version', 'kind'} & document.keys()


def validated(document: dict[str, Any], source: str) -> Profile:
    profile = Profile(document)
    issues = profile.validate()
    if issues:
        raise InvalidProfile(source, issues)
    return profile


def short_expanded(document: dict[str, Any], path: Path) -> dict[str, Any]:
    """A short YAML's profile, named for its file. Each column's value is a fake, a `low..high` range, a list, or empty."""
    issues = [
        Issue('unknown-field', pointer((key,)), f'a short profile holds {" and ".join(SHORT_KEYS)}, not {key}')
        for key in document
        if key not in SHORT_KEYS
    ]
    rows = document.get('rows')
    if rows is not None and (isinstance(rows, bool) or not isinstance(rows, int) or rows < 0):
        issues.append(Issue('invalid-rows', '/rows', f'rows is a count of rows, 0 or more, not {rows!r}'))
        rows = None
    columns = document['columns']
    if not isinstance(columns, dict) or not columns:
        message = "columns maps each column's name to a fake, a low..high range, a list of values, or nothing to take its name's fake"
        raise InvalidProfile(str(path), [*issues, Issue('invalid-columns', '/columns', message)])
    specs: dict[str, InlineColumn | None] = {}
    for name, value in columns.items():
        try:
            specs[name] = written_column(value)
        except (TypeError, ValueError) as error:
            issues.append(Issue('invalid-column', pointer(('columns', name)), str(error), getattr(error, 'closest', ())))
    if issues:
        raise InvalidProfile(str(path), issues)
    return expanded(specs, rows, path.name.split('.')[0], str(path))


class UnknownFake(ValueError):
    def __init__(self, message: str, closest: tuple[str, ...]) -> None:
        super().__init__(message)
        self.closest = closest


def written_column(value: Any) -> InlineColumn | None:
    """The inline column a short YAML's value stands for, or None for an empty value, which takes the name's fake."""
    if value is None:
        return None
    if isinstance(value, list):
        return choice(value)
    if not isinstance(value, str):
        raise TypeError(f'a column is a fake, a low..high range or a list of values, not {value!r}')
    if value in providers():
        return Fake(value)
    bounds = RANGE.match(value)
    if bounds is None:
        closest = tuple(difflib.get_close_matches(value, providers(), n=CLOSEST_FAKES, cutoff=CLOSEST_CUTOFF))
        named = f'; the closest are {", ".join(closest)}' if closest else '; doppeldata schemas show profile lists them as Provider'
        raise UnknownFake(f'{value!r} is neither a fake nor a low..high range{named}', closest)
    low, high = bounds['low'], bounds['high']
    if INTEGER.match(low) and INTEGER.match(high):
        return integers(int(low), int(high))
    if NUMBER.match(low) and NUMBER.match(high):
        return decimals(float(low), float(high))
    if DATE.match(low) and DATE.match(high):
        return dates(low, high)
    raise ValueError(f'{value!r} is a range of neither two numbers nor two ISO dates, such as 18..90 or 2023-01-01..2025-12-31')


def expanded(specs: dict[str, InlineColumn | None], rows: int | None, table: str, source: str) -> dict[str, Any]:
    """The plain profile of the columns `specs` declares, each None taking the fake its name hints."""
    if not specs:
        raise InvalidProfile(source, [Issue('no-columns', '/columns', 'a profile needs at least one column')])
    issues, columns = [], []
    remedy = PYTHON_REMEDY if source in (LIST_SOURCE, DICT_SOURCE) else SHORT_YAML_REMEDY
    for name, spec in specs.items():
        if spec is None:
            column = named_column(name)
            if column is None:
                message, closest = no_fake_message(name, remedy)
                issues.append(Issue('no-fake-for-name', pointer(('columns', name)), message, closest))
                continue
        elif isinstance(spec, InlineColumn):
            column = spec.column(name)
        else:
            raise TypeError(
                f'column {name!r} is given {type(spec).__name__}; give it a fake such as sd.fakes.email, '
                'sd.integers, sd.decimals, sd.dates or sd.choice, or None to take the fake its name hints'
            )
        columns.append(column)
    if issues:
        raise InvalidProfile(source, issues)
    document: dict[str, Any] = {'profile_version': FORMAT_VERSION, 'kind': 'table', 'name': table}
    if rows is not None:
        document['dataset'] = {'row_count': rows}
    document['columns'] = columns
    return document


def named_column(name: str) -> dict[str, Any] | None:
    """The column a name alone gives: the general type it hints, with that type's generator hint, else a provider's fake.

    A name that is a provider's own draws that provider where the type it hints draws another, so `free_email` and `state`
    draw as `sd.fakes` does. A type drawn by a pattern keeps it, as `phone_number` does over fake's extensions.
    """
    hinted = type_named_by(name)
    exact = provider_called(name)
    if hinted is not None and (exact is None or hinted.generator_hint.get('provider', exact) == exact):
        semantic = {'type': hinted.type, 'generator_hint': dict(hinted.generator_hint)}
        return {'name': name, 'physical_type': 'string', 'logical_type': 'string', 'semantic': semantic}
    provider = provider_named_by(name)
    return Fake(provider).column(name) if provider is not None else None


def provider_called(name: str) -> str | None:
    """The provider `name` is, word for word, or None."""
    words = name_words(name)
    return next((provider for provider in providers() if name_words(provider) == words), None)


def provider_named_by(name: str) -> str | None:
    """The provider `name` is, word for word, or else the one a hint in `PROVIDER_HINTS` that ends it names."""
    named = provider_called(name)
    if named is not None:
        return named
    words = name_words(name)
    return next(
        (provider for provider, hints in PROVIDER_HINTS.items() if any(hint_runs_in(words, hint, at_end=True) for hint in hints)), None
    )


def no_fake_message(name: str, remedy: str) -> tuple[str, tuple[str, ...]]:
    """What a column named `name` matching no fake is told, ending in the closest fakes, and those fakes."""
    closest = closest_fakes(name)
    named = f'; the closest are {", ".join(closest)}' if closest else ''
    return f'{name!r} names no fake; {remedy}{named}', closest


def closest_fakes(name: str) -> tuple[str, ...]:
    """The fakes whose names, or the names that hint them, are closest to `name`, nearest first."""
    hinting: dict[str, str] = {}
    for provider in providers():
        hinting.setdefault(provider, provider)
    for provider, hints in PROVIDER_HINTS.items():
        for hint in hints:
            hinting.setdefault('_'.join(name_words(hint)), provider)
    for hinted in GENERAL:
        fake = hinted.generator_hint.get('provider')
        if fake is None:
            continue
        for hint in hinted.name_hints:
            hinting.setdefault('_'.join(name_words(hint)), fake)
    matches = difflib.get_close_matches('_'.join(name_words(name)), hinting, n=len(hinting), cutoff=CLOSEST_CUTOFF)
    return tuple(dict.fromkeys(hinting[match] for match in matches))[:CLOSEST_FAKES]
