"""Profile format v1: read, validate, save and identify a profile file.

A profile is committed as YAML and accepted as JSON. Both validate against the JSON Schema derived
from the engine's Rust model, and a profile's identity is the SHA-256 the engine takes over its
RFC 8785 canonical JSON.
"""

import dataclasses as dc
import importlib.resources
import json
import re
from collections.abc import Iterator
from collections.abc import Mapping
from functools import cache
from pathlib import Path
from typing import Any

import jsonschema
import jsonschema.exceptions
import rfc8785
from ruamel.yaml import YAML
from ruamel.yaml.nodes import ScalarNode
from ruamel.yaml.representer import RoundTripRepresenter

from doppeldata import _core

FORMAT_VERSION = 1
BUNDLE_INDEX = 'bundle.yaml'
# Left out of the digest, so profiling unchanged data again keeps the profile's identity.
UNHASHED_GENERATED_FIELDS = ('at', 'tool')
# Stored rates are rounded, so a set meant to sum to 1 is allowed this far off.
RATE_SUM_TOLERANCE = 0.01
QUANTILE_NAME = re.compile(r'^p[0-9]+$')
RATE_SETS_SUMMING_TO_ONE = ('type_mix', 'by_month', 'by_dow', 'by_hour')


@dc.dataclass(frozen=True)
class Issue:
    """One reason a profile is invalid, located by a JSON Pointer into the document."""

    code: str
    pointer: str
    message: str


class InvalidProfile(ValueError):
    def __init__(self, source: str, issues: list[Issue]) -> None:
        self.source = source
        self.issues = issues
        listed = '\n'.join(f'  {issue.pointer or "/"}: {issue.message} [{issue.code}]' for issue in issues)
        super().__init__(f'{source} is not a valid profile:\n{listed}')


def schema_text() -> str:
    """The profile's JSON Schema, as committed in the package."""
    return importlib.resources.files('doppeldata').joinpath('schemas/profile.v1.json').read_text()


@cache
def validators() -> dict[str | None, jsonschema.Draft202012Validator]:
    """One validator per `kind`, so an error inside a table is not reported as a bundle's."""
    schema = json.loads(schema_text())
    root = {key: value for key, value in schema.items() if key != 'oneOf'}
    by_kind: dict[str | None, jsonschema.Draft202012Validator] = {None: jsonschema.Draft202012Validator(schema)}
    for branch in schema['oneOf']:
        by_kind[branch['properties']['kind']['const']] = jsonschema.Draft202012Validator(root | branch)
    return by_kind


class NullWrittenOut(RoundTripRepresenter):
    """Writes None as `null`, where ruamel's round trip would leave the value empty."""

    def represent_none(self, data: None) -> ScalarNode:
        return self.represent_scalar('tag:yaml.org,2002:null', 'null')


NullWrittenOut.add_representer(type(None), NullWrittenOut.represent_none)


def yaml() -> YAML:
    loader = YAML()
    loader.Representer = NullWrittenOut
    loader.preserve_quotes = True
    loader.width = 4096
    loader.indent(mapping=2, sequence=4, offset=2)
    return loader


def read_document(path: Path) -> Any:
    """A profile file as written: comments, key order and unknown fields kept."""
    if path.suffix == '.json':
        return json.loads(path.read_text())
    with path.open() as fh:
        return yaml().load(fh)


def write_document(document: Any, path: Path) -> None:
    if path.suffix == '.json':
        path.write_text(json.dumps(to_plain(document), indent=2) + '\n')
        return
    with path.open('w') as fh:
        yaml().dump(document, fh)


def to_plain(value: Any) -> Any:
    """The document as plain dicts, lists and scalars, with ruamel's comment-keeping types removed."""
    if isinstance(value, Mapping):
        return {str(key): to_plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_plain(item) for item in value]
    if isinstance(value, bool) or value is None:
        return value
    for plain_type in (int, float, str):
        if isinstance(value, plain_type):
            return plain_type(value)
    return value


def without_unhashed_fields(plain: dict[str, Any]) -> dict[str, Any]:
    generated = plain.get('generated')
    if not isinstance(generated, dict):
        return plain
    kept = {key: value for key, value in generated.items() if key not in UNHASHED_GENERATED_FIELDS}
    return plain | {'generated': kept}


def pointer(path: list[Any]) -> str:
    return ''.join('/' + str(part).replace('~', '~0').replace('/', '~1') for part in path)


def check(plain: Any) -> list[Issue]:
    """Every reason the document is not a valid profile: the schema's, then the orderings it cannot state."""
    if not isinstance(plain, dict):
        return [Issue('not-a-mapping', '', 'a profile is a mapping of sections')]
    version = plain.get('profile_version')
    if isinstance(version, int) and not isinstance(version, bool) and version > FORMAT_VERSION:
        message = f'profile_version {version} is newer than {FORMAT_VERSION}, the version this doppeldata reads; upgrade doppeldata'
        return [Issue('newer-profile-version', '/profile_version', message)]
    kind = plain.get('kind')
    validator = validators().get(kind if isinstance(kind, str) else None, validators()[None])
    issues = []
    for error in validator.iter_errors(plain):
        cause = jsonschema.exceptions.best_match([error])
        issues.append(Issue(f'schema-{cause.validator}', pointer(list(cause.absolute_path)), cause.message))
    if issues:
        return issues
    return list(ordering_issues(plain, ''))


def ordering_issues(value: Any, at: str) -> Iterator[Issue]:
    """What JSON Schema cannot express: quantiles rising, rates summing to 1, bins matching edges."""
    if isinstance(value, list):
        for index, item in enumerate(value):
            yield from ordering_issues(item, f'{at}/{index}')
        return
    if not isinstance(value, dict):
        return
    if len(value) > 1 and all(isinstance(key, str) and QUANTILE_NAME.match(key) for key in value):
        yield from quantile_issues(value, at)
    for name in RATE_SETS_SUMMING_TO_ONE:
        rates = value.get(name)
        if rates:
            yield from rate_sum_issues(rates.values() if isinstance(rates, dict) else rates, f'{at}/{name}')
    if isinstance(value.get('given'), str) and isinstance(value.get('table'), dict):
        for given, row in value['table'].items():
            yield from rate_sum_issues(row.values(), f'{at}/table/{given}')
    histogram = value.get('histogram')
    if isinstance(histogram, dict):
        yield from histogram_issues(histogram, f'{at}/histogram')
    extremes = value.get('extremes')
    if (
        isinstance(extremes, dict)
        and all(isinstance(extremes.get(end), float | int) for end in ('low', 'high'))
        and extremes['low'] > extremes['high']
    ):
        yield Issue('extremes-reversed', f'{at}/extremes', f'low {extremes["low"]} is above high {extremes["high"]}')
    tables = value.get('tables')
    if value.get('kind') == 'bundle' and isinstance(tables, dict):
        for name, table in tables.items():
            if isinstance(table, dict) and table.get('name') != name:
                yield Issue('table-name-mismatch', f'{at}/tables/{name}/name', f'the table under {name!r} is named {table.get("name")!r}')
    for key, item in value.items():
        yield from ordering_issues(item, f'{at}/{key}')


def quantile_level(name: str) -> float:
    """The level a quantile name stands for: p05 is 0.05, p50 is 0.5 and p999 is 0.999."""
    return float('0.' + name[1:])


def quantile_issues(quantiles: dict[str, float], at: str) -> Iterator[Issue]:
    by_level = sorted(quantiles.items(), key=lambda item: quantile_level(item[0]))
    for (lower, low), (upper, high) in zip(by_level, by_level[1:], strict=False):
        if high < low:
            yield Issue('quantiles-not-rising', f'{at}/{upper}', f'{upper} ({high}) is below {lower} ({low})')


def rate_sum_issues(rates: Any, at: str) -> Iterator[Issue]:
    total = sum(rates)
    if abs(total - 1) > RATE_SUM_TOLERANCE:
        yield Issue('rates-not-summing-to-one', at, f'the rates sum to {total:.4g}, not 1')


def histogram_issues(histogram: dict[str, Any], at: str) -> Iterator[Issue]:
    edges, counts = histogram.get('edges', []), histogram.get('counts', [])
    if len(counts) != len(edges) - 1:
        yield Issue('histogram-bins-mismatch', at, f'{len(edges)} edges bound {len(edges) - 1} bins, and there are {len(counts)} counts')
    if any(high <= low for low, high in zip(edges, edges[1:], strict=False)):
        yield Issue('histogram-edges-not-rising', f'{at}/edges', 'each edge must be above the one before it')


class Profile:
    """One table's profile, kept as the document it was read from."""

    def __init__(self, document: Any, path: Path | None = None) -> None:
        self.document = document
        self.path = path

    @classmethod
    def load(cls, path: str | Path) -> 'Profile':
        """Read a YAML or JSON table profile, raising InvalidProfile when it does not validate."""
        path = Path(path)
        profile = cls(read_document(path), path)
        issues = profile.validate()
        if not issues and profile.document.get('kind') != 'table':
            issues = [Issue('not-a-table', '/kind', 'this file is a bundle, which Bundle.load reads')]
        if issues:
            raise InvalidProfile(str(path), issues)
        return profile

    def validate(self) -> list[Issue]:
        """Every reason the profile is invalid. Empty when it is valid."""
        return check(self.to_plain())

    def to_plain(self) -> dict[str, Any]:
        return to_plain(self.document)

    def canonical(self) -> bytes:
        """The RFC 8785 canonical JSON the digest is taken over, without `generated.at` and `generated.tool`."""
        return rfc8785.dumps(without_unhashed_fields(self.to_plain()))

    @property
    def digest(self) -> str:
        """`sha256:<hex>` over the canonical form, taken by the engine that reads it."""
        return _core.profile_digest(self.canonical())

    def save(self, path: str | Path | None = None) -> None:
        """Write the profile as YAML, or as JSON to a `.json` path, keeping comments, order and unknown fields."""
        target = Path(path) if path is not None else self.path
        if target is None:
            raise ValueError('this profile was never read from a file, so save needs a path')
        write_document(self.document, target)
        self.path = target


class Bundle:
    """Several tables' profiles and their relationships: a `kind: bundle` file, or a directory holding `bundle.yaml`."""

    def __init__(self, document: Any, path: Path, tables: dict[str, Profile]) -> None:
        self.document = document
        self.path = path
        self.tables = tables

    @classmethod
    def load(cls, path: str | Path) -> 'Bundle':
        """Read a bundle and every table it names, raising InvalidProfile when any of them does not validate."""
        path = Path(path)
        index = path / BUNDLE_INDEX if path.is_dir() else path
        document = read_document(index)
        issues = check(to_plain(document))
        if not issues and document.get('kind') != 'bundle':
            issues = [Issue('not-a-bundle', '/kind', 'this file is a table profile, which Profile.load reads')]
        if issues:
            raise InvalidProfile(str(index), issues)
        tables = {}
        for name, entry in document['tables'].items():
            if not isinstance(entry, str):
                tables[name] = Profile(entry, index)
                continue
            table = Profile.load(index.parent / entry)
            if table.document['name'] != name:
                message = f'{entry} is named {table.document["name"]!r}, and the bundle names it {name!r}'
                raise InvalidProfile(str(index), [Issue('table-name-mismatch', f'/tables/{name}', message)])
            tables[name] = table
        return cls(document, index, tables)

    def to_plain(self) -> dict[str, Any]:
        """The bundle with every table inline, as a single-file bundle would hold it."""
        return to_plain(self.document) | {'tables': {name: table.to_plain() for name, table in self.tables.items()}}

    def canonical(self) -> bytes:
        plain = self.to_plain()
        plain['tables'] = {name: without_unhashed_fields(table) for name, table in plain['tables'].items()}
        return rfc8785.dumps(without_unhashed_fields(plain))

    @property
    def digest(self) -> str:
        return _core.profile_digest(self.canonical())
