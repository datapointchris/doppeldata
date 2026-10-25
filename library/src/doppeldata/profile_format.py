"""Profile format v1: read, validate, save and identify a profile file.

A profile is committed as YAML and accepted as JSON. Both validate against the JSON Schema derived
from the engine's Rust model. A profile's identity is the SHA-256 of its RFC 8785 canonical JSON,
which the library alone computes and hands to the generator.
"""

import dataclasses as dc
import hashlib
import importlib.resources
import io
import json
import math
from collections.abc import Iterator
from collections.abc import Mapping
from functools import cache
from pathlib import Path
from typing import Any

import jsonschema
import jsonschema.exceptions
import rfc8785
from ruamel.yaml import YAML
from ruamel.yaml.constructor import RoundTripConstructor
from ruamel.yaml.error import YAMLError
from ruamel.yaml.nodes import ScalarNode
from ruamel.yaml.representer import RoundTripRepresenter

from doppeldata import _core
from doppeldata.example_profiles import example_resource
from doppeldata.published_schemas import schema_text

FORMAT_VERSION = _core.FORMAT_VERSION
BUNDLE_INDEX = 'bundle.yaml'
# Left out of the digest, so profiling unchanged data again keeps the profile's identity.
UNHASHED_GENERATED_FIELDS = ('at', 'tool')
# Stored rates are rounded, so a set meant to sum to 1 is allowed this far off.
RATE_SUM_TOLERANCE = 0.01
# RFC 8785 writes numbers as IEEE 754 doubles, which hold every integer only up to this magnitude.
LARGEST_EXACT_INTEGER = 2**53 - 1
INTEGER_FORMAT_RANGES = {
    'int32': (-(2**31), 2**31 - 1),
    'int64': (-(2**63), 2**63 - 1),
    'uint32': (0, 2**32 - 1),
    'uint64': (0, 2**64 - 1),
}
TEMPORAL_RATE_SETS = ('by_month', 'by_dow', 'by_hour')

PointerParts = tuple[str | int, ...]


@dc.dataclass(frozen=True)
class Issue:
    """One reason a profile is invalid. `field` is an RFC 6901 JSON Pointer, because map keys hold category values with dots and slashes."""

    code: str
    field: str
    message: str


class InvalidProfile(ValueError):
    def __init__(self, source: str, issues: list[Issue]) -> None:
        self.source = source
        self.issues = issues
        listed = '\n'.join(f'  {issue.field or "/"}: {issue.message} [{issue.code}]' for issue in issues)
        super().__init__(f'{source} is not a valid profile:\n{listed}')


def integer_format_checker() -> jsonschema.FormatChecker:
    """Checks the integer widths the Rust model declares, which jsonschema leaves unchecked by default."""
    checker = jsonschema.FormatChecker()
    for name, (low, high) in INTEGER_FORMAT_RANGES.items():
        checker.checks(name)(lambda value, low=low, high=high: not isinstance(value, int) or low <= value <= high)
    return checker


@cache
def validators() -> dict[str | None, jsonschema.Draft202012Validator]:
    """One validator per `kind`, so an error inside a table is not reported as a bundle's."""
    schema = json.loads(schema_text('profile'))
    checker = integer_format_checker()
    root = {key: value for key, value in schema.items() if key != 'oneOf'}
    by_kind: dict[str | None, jsonschema.Draft202012Validator] = {None: jsonschema.Draft202012Validator(schema, format_checker=checker)}
    for branch in schema['oneOf']:
        by_kind[branch['properties']['kind']['const']] = jsonschema.Draft202012Validator(root | branch, format_checker=checker)
    return by_kind


class TimestampText(str):
    """A YAML timestamp kept as the text written, so it validates as a string and saves as written."""


class ProfileConstructor(RoundTripConstructor):
    def construct_timestamp_text(self, node: ScalarNode) -> TimestampText:
        return TimestampText(self.construct_scalar(node))


ProfileConstructor.add_constructor('tag:yaml.org,2002:timestamp', ProfileConstructor.construct_timestamp_text)


class ProfileRepresenter(RoundTripRepresenter):
    """Writes None as `null`, where ruamel's round trip would leave the value empty."""

    def represent_none(self, data: None) -> ScalarNode:
        return self.represent_scalar('tag:yaml.org,2002:null', 'null')

    def represent_timestamp_text(self, data: TimestampText) -> ScalarNode:
        return self.represent_scalar('tag:yaml.org,2002:timestamp', str(data))


ProfileRepresenter.add_representer(type(None), ProfileRepresenter.represent_none)
ProfileRepresenter.add_representer(TimestampText, ProfileRepresenter.represent_timestamp_text)


def yaml() -> YAML:
    loader = YAML()
    loader.Constructor = ProfileConstructor
    loader.Representer = ProfileRepresenter
    loader.preserve_quotes = True
    loader.width = 4096
    loader.indent(mapping=2, sequence=4, offset=2)
    return loader


def read_document(path: Path) -> Any:
    """A profile file as written: comments, key order and unknown fields kept."""
    try:
        if path.suffix == '.json':
            return json.loads(path.read_text())
        with path.open() as fh:
            return yaml().load(fh)
    except (json.JSONDecodeError, YAMLError) as error:
        raise InvalidProfile(str(path), [Issue('not-parseable', '', str(error))]) from error


def write_document(document: Any, path: Path) -> None:
    """Write beside the target, then rename over it, so a write that fails leaves the old file whole."""
    staged = path.with_name(f'.{path.name}.tmp')
    try:
        if path.suffix == '.json':
            staged.write_text(json.dumps(to_plain(document), indent=2, allow_nan=False) + '\n')
        else:
            with staged.open('w') as fh:
                yaml().dump(document, fh)
        staged.replace(path)
    finally:
        staged.unlink(missing_ok=True)


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


def digest_of(canonical: bytes) -> str:
    return 'sha256:' + hashlib.sha256(canonical).hexdigest()


def pointer(path: PointerParts) -> str:
    return ''.join('/' + str(part).replace('~', '~0').replace('/', '~1') for part in path)


def check(plain: Any) -> list[Issue]:
    """Every reason the document is not a valid profile: numbers RFC 8785 cannot write, the schema's, then the orderings it cannot state."""
    if not isinstance(plain, dict):
        return [Issue('not-a-mapping', '', 'a profile is a mapping of sections')]
    version = plain.get('profile_version')
    if isinstance(version, int) and not isinstance(version, bool) and version > FORMAT_VERSION:
        message = f'profile_version {version} is newer than {FORMAT_VERSION}, the version this doppeldata reads; upgrade doppeldata'
        return [Issue('newer-profile-version', '/profile_version', message)]
    issues = list(unrepresentable_numbers(plain, ()))
    if issues:
        return issues
    kind = plain.get('kind')
    validator = validators().get(kind if isinstance(kind, str) else None, validators()[None])
    for error in validator.iter_errors(plain):
        cause = cause_of(error)
        issues.append(Issue(f'schema-{cause.validator}', pointer(tuple(cause.absolute_path)), cause.message))
    if issues:
        return issues
    if kind == 'bundle':
        return list(bundle_ordering_issues(plain))
    return list(table_ordering_issues(plain, ()))


def cause_of(error: jsonschema.exceptions.ValidationError) -> jsonschema.exceptions.ValidationError:
    """The error inside an `anyOf` or `oneOf` that names the fault, past the `null` branch every optional field has."""
    while error.context:
        branches = [inner for inner in error.context if not (inner.validator == 'type' and inner.validator_value == 'null')]
        best = jsonschema.exceptions.best_match(branches or error.context)
        if best is None:
            break
        error = best
    return error


def unrepresentable_numbers(value: Any, at: PointerParts) -> Iterator[Issue]:
    """Numbers anywhere in the document, unknown fields included, that canonical JSON cannot write."""
    if isinstance(value, dict):
        for key, item in value.items():
            yield from unrepresentable_numbers(item, (*at, key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from unrepresentable_numbers(item, (*at, index))
    elif isinstance(value, float) and not math.isfinite(value):
        yield Issue('not-finite', pointer(at), f'{value} is not a finite number')
    elif isinstance(value, int) and not isinstance(value, bool) and abs(value) > LARGEST_EXACT_INTEGER:
        yield Issue('integer-too-large', pointer(at), f'{value} is beyond ±2^53, the largest integer canonical JSON writes exactly')


def bundle_ordering_issues(bundle: dict[str, Any]) -> Iterator[Issue]:
    for name, table in bundle['tables'].items():
        if not isinstance(table, dict):
            continue
        if table['name'] != name:
            yield Issue('table-name-mismatch', pointer(('tables', name, 'name')), f'the table under {name!r} is named {table["name"]!r}')
        yield from table_ordering_issues(table, ('tables', name))
    for index, relationship in enumerate(bundle.get('relationships') or []):
        fanout = relationship.get('fanout') or {}
        yield from quantile_issues(fanout.get('quantiles'), ('relationships', index, 'fanout', 'quantiles'))


def table_ordering_issues(table: dict[str, Any], at: PointerParts) -> Iterator[Issue]:
    """What JSON Schema cannot express, checked at the fields the model gives each meaning."""
    layout = (table.get('source') or {}).get('layout') or {}
    yield from quantile_issues(layout.get('rows_per_file'), (*at, 'source', 'layout', 'rows_per_file'))
    for index, conditional in enumerate((table.get('dataset') or {}).get('conditionals') or []):
        for given, row in conditional['table'].items():
            yield from rate_sum_issues(row.values(), (*at, 'dataset', 'conditionals', index, 'table', given))
    for index, column in enumerate(table.get('columns') or []):
        yield from column_ordering_issues(column, (*at, 'columns', index))


def column_ordering_issues(column: dict[str, Any], at: PointerParts) -> Iterator[Issue]:
    if column.get('type_mix'):
        yield from rate_sum_issues(column['type_mix'].values(), (*at, 'type_mix'))
    numeric = column.get('numeric') or {}
    yield from quantile_issues(numeric.get('quantiles'), (*at, 'numeric', 'quantiles'))
    if numeric.get('histogram'):
        yield from histogram_issues(numeric['histogram'], (*at, 'numeric', 'histogram'))
    extremes = numeric.get('extremes') or {}
    if (
        isinstance(extremes.get('low'), int | float)
        and isinstance(extremes.get('high'), int | float)
        and extremes['low'] > extremes['high']
    ):
        yield Issue('extremes-reversed', pointer((*at, 'numeric', 'extremes')), f'low {extremes["low"]} is above high {extremes["high"]}')
    temporal = column.get('temporal') or {}
    for name in TEMPORAL_RATE_SETS:
        if temporal.get(name):
            yield from rate_sum_issues(temporal[name], (*at, 'temporal', name))
    length = (column.get('string') or {}).get('length') or {}
    yield from quantile_issues(length.get('quantiles'), (*at, 'string', 'length', 'quantiles'))
    listed = column.get('list') or {}
    yield from quantile_issues(listed.get('length'), (*at, 'list', 'length'))
    if listed.get('element'):
        yield from column_ordering_issues(listed['element'], (*at, 'list', 'element'))
    for index, field in enumerate((column.get('struct') or {}).get('fields') or []):
        yield from column_ordering_issues(field, (*at, 'struct', 'fields', index))


def quantile_level(name: str) -> float:
    """The level a quantile name stands for: p05 is 0.05, p50 is 0.5 and p999 is 0.999."""
    return float('0.' + name[1:])


def quantile_issues(quantiles: dict[str, float] | None, at: PointerParts) -> Iterator[Issue]:
    by_level = sorted((quantiles or {}).items(), key=lambda item: quantile_level(item[0]))
    for (lower, low), (upper, high) in zip(by_level, by_level[1:], strict=False):
        if high < low:
            yield Issue('quantiles-not-rising', pointer((*at, upper)), f'{upper} ({high}) is below {lower} ({low})')


def rate_sum_issues(rates: Any, at: PointerParts) -> Iterator[Issue]:
    total = sum(rates)
    if abs(total - 1) > RATE_SUM_TOLERANCE:
        yield Issue('rates-not-summing-to-one', pointer(at), f'the rates sum to {total:.4g}, not 1')


def histogram_issues(histogram: dict[str, Any], at: PointerParts) -> Iterator[Issue]:
    edges, counts = histogram['edges'], histogram['counts']
    if len(counts) != len(edges) - 1:
        message = f'{len(edges)} edges bound {len(edges) - 1} bins, and there are {len(counts)} counts'
        yield Issue('histogram-bins-mismatch', pointer(at), message)
    if any(high <= low for low, high in zip(edges, edges[1:], strict=False)):
        yield Issue('histogram-edges-not-rising', pointer((*at, 'edges')), 'each edge must be above the one before it')


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

    @classmethod
    def example(cls, name: str) -> 'Profile':
        """An example profile the library ships. It has no file of its own, so `save` needs a path."""
        with importlib.resources.as_file(example_resource(name)) as path:
            profile = cls.load(path)
        profile.path = None
        return profile

    def validate(self) -> list[Issue]:
        """Every reason the profile is invalid. Empty when it is valid."""
        return check(self.to_plain())

    def to_plain(self) -> dict[str, Any]:
        return to_plain(self.document)

    def to_yaml(self) -> str:
        """The profile as the YAML `save` writes."""
        text = io.StringIO()
        yaml().dump(self.document, text)
        return text.getvalue()

    def canonical(self) -> bytes:
        """The RFC 8785 canonical JSON the digest is taken over, without the unhashed generated fields."""
        return rfc8785.dumps(without_unhashed_fields(self.to_plain()))

    @property
    def digest(self) -> str:
        """`sha256:<hex>` over the canonical form."""
        return digest_of(self.canonical())

    def save(self, path: str | Path | None = None) -> None:
        """Write the profile as YAML, or as JSON to a `.json` path, keeping comments, order and unknown fields."""
        target = Path(path) if path is not None else self.path
        if target is None:
            raise ValueError('this profile has no file of its own, so save needs a path')
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
        """Read a bundle and every table it names, raising InvalidProfile when any of them does not validate.

        A table written inline has no file of its own, so its `save` needs a path.
        """
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
                tables[name] = Profile(entry)
                continue
            member = index.parent / entry
            if not member.is_file():
                raise InvalidProfile(str(index), [Issue('table-file-missing', pointer(('tables', name)), f'{entry} does not exist')])
            table = Profile.load(member)
            if table.document['name'] != name:
                message = f'{entry} is named {table.document["name"]!r}, and the bundle names it {name!r}'
                raise InvalidProfile(str(index), [Issue('table-name-mismatch', pointer(('tables', name)), message)])
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
        return digest_of(self.canonical())
