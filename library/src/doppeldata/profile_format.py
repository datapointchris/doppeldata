"""The profile format: read, validate, save and identify a profile file.

A profile is committed as YAML and accepted as JSON. Both validate against the JSON Schema derived
from the engine's Rust model, which refuses a key no section declares. A profile's identity is the
SHA-256 of its RFC 8785 canonical JSON, which the library alone computes and hands to the generator.
"""

import copy
import dataclasses as dc
import datetime as dt
import difflib
import hashlib
import importlib.resources
import io
import json
import math
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
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
from doppeldata.errors import DoppeldataError
from doppeldata.example_profiles import bundle_example_resource
from doppeldata.example_profiles import example_resource
from doppeldata.published_schemas import schema_text

BUNDLE_INDEX = 'bundle.yaml'


@dc.dataclass(frozen=True)
class GeneratedField:
    """What one `generated` field takes part in.

    `hashed` puts it in the profile's digest, so a field left out lets profiling unchanged data again keep the
    profile's identity. `derived_from` makes it one of the values an earlier profile must match to be returned
    unchanged, beside the rows, `k_min` and the sampling.
    """

    hashed: bool
    derived_from: bool


GENERATED_FIELDS = {
    'tool': GeneratedField(hashed=False, derived_from=True),
    'engine': GeneratedField(hashed=True, derived_from=True),
    'fitter': GeneratedField(hashed=False, derived_from=True),
    'detector_packs': GeneratedField(hashed=False, derived_from=True),
    'code_lists': GeneratedField(hashed=False, derived_from=True),
    'readers': GeneratedField(hashed=False, derived_from=True),
    'source_fingerprint': GeneratedField(hashed=False, derived_from=True),
    'at': GeneratedField(hashed=False, derived_from=False),
    'sample': GeneratedField(hashed=True, derived_from=False),
    # It names no directory, so a copy of the schema elsewhere keeps the seed's identity.
    'imported_from': GeneratedField(hashed=True, derived_from=False),
    # No draw reads it, so the digest leaves it out.
    'declared': GeneratedField(hashed=False, derived_from=False),
}
HASHED_GENERATED_FIELDS = tuple(name for name, field in GENERATED_FIELDS.items() if field.hashed)
DERIVED_FROM = tuple(name for name, field in GENERATED_FIELDS.items() if field.derived_from)
TOOL = f'doppeldata {_core.VERSION}'
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
# Each column section that can list values, as `top_values` and `other_rate`.
LISTING_SECTIONS = ('string', 'numeric')

PointerParts = tuple[str | int, ...]


@dc.dataclass(frozen=True)
class GivenList:
    """A dataset list whose entries each draw `column` given the values of the column `given`. `by_value` names the
    fields of an entry keyed by a listed given value, beside `other`, which holds every other value's. Where `nested`,
    each key's value is a group of literals, as a table's row is, and otherwise one literal. Where `child_keyed`, the
    child's values key those groups, so its column must list them."""

    by_value: tuple[str, ...]
    nested: bool
    child_keyed: bool

    def keys(self, entry: dict[str, Any]) -> set[str]:
        """The values of the given column the entry lists."""
        return {key for field in self.by_value if isinstance(by := entry.get(field), dict) for key in by}

    def groups(self, entry: dict[str, Any]) -> list[Any]:
        """The groups of literals the entry holds, each listed value's and then `other`."""
        fields = [entry.get(field) or {} for field in self.by_value]
        listed = [group for by in fields if isinstance(by, dict) for group in by.values()] if self.nested else fields
        return [*listed, entry.get('other') or {}]

    def literals(self, entry: dict[str, Any]) -> int:
        return sum(len(group) for group in self.groups(entry) if isinstance(group, dict))

    def child_values(self, entry: dict[str, Any]) -> set[str] | None:
        """The child's values the entry keys its groups by, or None where its values key nothing."""
        if not self.child_keyed:
            return None
        return {value for group in self.groups(entry) if isinstance(group, dict) for value in group}


# The dataset's lists naming a parent column and the values of it each entry is drawn given.
GIVEN_LISTS = {
    'conditionals': GivenList(('table',), nested=True, child_keyed=True),
    'conditional_quantiles': GivenList(('quantiles',), nested=True, child_keyed=False),
    'conditional_rates': GivenList(('zero_rate', 'null_rate'), nested=False, child_keyed=False),
}


@dc.dataclass(frozen=True)
class Issue:
    """One reason a profile is invalid. `field` is an RFC 6901 JSON Pointer, because map keys hold category values with dots and slashes."""

    code: str
    field: str
    message: str
    # The names an unknown name is closest to, nearest first, where the issue is one.
    closest: tuple[str, ...] = ()


class InvalidProfile(DoppeldataError, ValueError):
    def __init__(self, source: str, issues: list[Issue]) -> None:
        self.source = source
        self.issues = issues
        listed = '\n'.join(f'  {issue.field or "/"}: {issue.message} [{issue.code}]' for issue in issues)
        super().__init__(f'{source} is not a valid profile:\n{listed}')


class UngenerableProfile(InvalidProfile):
    """A valid profile the engine cannot generate, such as one holding a statistic it does not draw from yet."""

    exit_code = 2

    def __init__(self, source: str, issues: list[Issue]) -> None:
        super().__init__(source, issues)
        listed = '\n'.join(f'  {issue.field or "/"}: {issue.message} [{issue.code}]' for issue in issues)
        self.args = (f'{source} cannot be generated:\n{listed}',)


def integer_format_checker() -> jsonschema.FormatChecker:
    """Checks the integer widths the Rust model declares, which jsonschema leaves unchecked by default."""
    checker = jsonschema.FormatChecker()
    for name, (low, high) in INTEGER_FORMAT_RANGES.items():
        checker.checks(name)(lambda value, low=low, high=high: not isinstance(value, int) or low <= value <= high)
    return checker


@cache
def validators() -> dict[str, jsonschema.Draft202012Validator]:
    """One validator per `kind`, so an error inside a table is not reported as a bundle's."""
    schema = json.loads(schema_text('profile'))
    checker = integer_format_checker()
    root = {key: value for key, value in schema.items() if key != 'oneOf'}
    by_kind: dict[str, jsonschema.Draft202012Validator] = {}
    for branch in schema['oneOf']:
        (kind,) = tag_of(schema['$defs'][branch['$ref'].rsplit('/', 1)[1]]['properties']['kind'])
        by_kind[kind] = jsonschema.Draft202012Validator(root | branch, format_checker=checker)
    return by_kind


class TimestampText(str):
    """A YAML timestamp kept as the text written, so it validates as a string and saves as written."""


class ProfileConstructor(RoundTripConstructor):
    def construct_timestamp_text(self, node: ScalarNode) -> TimestampText:
        return TimestampText(self.construct_scalar(node))


ProfileConstructor.add_constructor('tag:yaml.org,2002:timestamp', ProfileConstructor.construct_timestamp_text)


class ProfileRepresenter(RoundTripRepresenter):
    """Writes None as `null`, where ruamel's round trip would leave the value empty, and quotes text opening with `?`,
    which ruamel writes plain inside a flow mapping and then cannot read there."""

    def represent_none(self, data: None) -> ScalarNode:
        return self.represent_scalar('tag:yaml.org,2002:null', 'null')

    def represent_str(self, data: str) -> ScalarNode:
        if data.startswith('?'):
            return self.represent_scalar('tag:yaml.org,2002:str', data, style="'")
        return super().represent_str(data)

    def represent_timestamp_text(self, data: TimestampText) -> ScalarNode:
        return self.represent_scalar('tag:yaml.org,2002:timestamp', str(data))


ProfileRepresenter.add_representer(type(None), ProfileRepresenter.represent_none)
ProfileRepresenter.add_representer(str, ProfileRepresenter.represent_str)
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
    """A profile file as written: comments and key order kept."""
    try:
        if path.suffix == '.json':
            return json.loads(path.read_text())
        with path.open() as fh:
            return yaml().load(fh)
    except (json.JSONDecodeError, YAMLError, UnicodeDecodeError) as error:
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
        return {plain_key(key): to_plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_plain(item) for item in value]
    if isinstance(value, bool) or value is None:
        return value
    for plain_type in (int, float, str):
        if isinstance(value, plain_type):
            return plain_type(value)
    return value


def plain_key(key: Any) -> str:
    """A mapping key as JSON spells it, so YAML's bare `null:`, `true:` and `false:` keys are those words."""
    if key is None:
        return 'null'
    if isinstance(key, bool):
        return 'true' if key else 'false'
    return str(key)


def generated_section(**fields: Any) -> dict[str, Any]:
    """A `generated` section made now: the tool and the time, then each of `fields` that is not None, in
    `GENERATED_FIELDS`' order.

    A field `GENERATED_FIELDS` does not declare raises ValueError, because the digest would not know whether it is hashed.
    """
    undeclared = sorted(set(fields) - set(GENERATED_FIELDS))
    if undeclared:
        raise ValueError(f'generated declares no field {", ".join(undeclared)}; add it to GENERATED_FIELDS')
    section: dict[str, Any] = {'tool': TOOL, 'at': dt.datetime.now(dt.UTC).isoformat(timespec='seconds').replace('+00:00', 'Z')}
    return section | {name: fields[name] for name in GENERATED_FIELDS if name not in section and fields.get(name) is not None}


def without_unhashed_fields(plain: dict[str, Any]) -> dict[str, Any]:
    generated = plain.get('generated')
    if not isinstance(generated, dict):
        return plain
    kept = {key: value for key, value in generated.items() if key in HASHED_GENERATED_FIELDS}
    return plain | {'generated': kept}


def digest_of(canonical: bytes) -> str:
    return 'sha256:' + hashlib.sha256(canonical).hexdigest()


def engine_plan(document: dict[str, Any], source: str, rows: int = 1) -> _core.Plan:
    """The engine's plan of `rows` rows of `document`, read as `source`: how it draws each column, which columns each
    Gaussian copula draws together, and which dependence entries it sets aside. Raises UngenerableProfile where the
    engine refuses the document."""
    canonical = rfc8785.dumps(without_unhashed_fields(document))
    return _core.Plan(json.dumps(document), digest_of(canonical), 0, rows, source)


def pointer(path: PointerParts) -> str:
    return ''.join('/' + str(part).replace('~', '~0').replace('/', '~1') for part in path)


def check(plain: Any) -> list[Issue]:
    """Every reason the document is not a valid profile: numbers RFC 8785 cannot write, the schema's, then the orderings it cannot state."""
    if not isinstance(plain, dict):
        return [Issue('not-a-mapping', '', 'a profile is a mapping of sections')]
    issues = list(unrepresentable_numbers(plain, ()))
    if issues:
        return issues
    kind = plain.get('kind')
    if 'kind' not in plain:
        return [Issue('schema-required', '', f"'kind' is a required property; a profile is one of {', '.join(validators())}")]
    if not isinstance(kind, str) or kind not in validators():
        return [tag_issue(kind, list(validators()), ('kind',))]
    for error in validators()[kind].iter_errors(plain):
        issues += schema_error_issues(error, ())
    if issues:
        return issues
    if kind == 'bundle':
        return list(bundle_ordering_issues(plain))
    return list(table_ordering_issues(plain, ()))


def schema_error_issues(error: jsonschema.exceptions.ValidationError, at: PointerParts) -> list[Issue]:
    """Each fault one schema error names, at its path below `at`.

    Inside an `anyOf` or `oneOf`, only the branch the value was meant for is read. A branch is not meant where the value
    is not its type, such as the `null` every optional field has, or where a field does not hold its tag, such as
    `derived`'s other `kind`. A value holding no branch's tag is that one fault. A key its section does not declare is an
    `unknown-field` at the key's own pointer, naming the closest field the section takes.
    """
    if error.context:
        branches: dict[Any, list[jsonschema.exceptions.ValidationError]] = {}
        for inner in error.context:
            branches.setdefault(inner.relative_schema_path[0], []).append(inner)
        typed = [errors for errors in branches.values() if not any(map(not_its_type, errors))]
        meant = [errors for errors in typed if not any(map(misses_tag, errors))]
        if len(meant) == 1:
            return [issue for inner in meant[0] for issue in schema_error_issues(inner, at)]
        missed = [inner for errors in typed for inner in errors if misses_tag(inner)]
        if not meant and missed:
            return [
                tag_issue(missed[0].instance, [tag for inner in missed for tag in tag_of(inner.schema)], (*at, *missed[0].absolute_path))
            ]
        best = jsonschema.exceptions.best_match([inner for errors in meant for inner in errors] or error.context)
        if best is not None:
            return schema_error_issues(best, at)
    path = (*at, *error.absolute_path)
    if error.validator == 'additionalProperties' and isinstance(error.instance, dict):
        known = list(error.schema.get('properties') or {})
        return [unknown_field(str(key), known, (*path, key)) for key in error.instance if key not in known]
    return [Issue(f'schema-{error.validator}', pointer(path), error.message)]


def not_its_type(error: jsonschema.exceptions.ValidationError) -> bool:
    """Whether an error inside one branch of a union says the value is not the branch's type."""
    return error.validator == 'type' and not error.relative_path


def misses_tag(error: jsonschema.exceptions.ValidationError) -> bool:
    """Whether an error inside one branch of a union says a field of the value does not hold the branch's tag."""
    return error.validator in ('const', 'enum') and len(error.relative_path) == 1 and bool(tag_of(error.schema))


def tag_of(schema: Any) -> tuple[Any, ...]:
    """The one value a tag field takes, written as `const` or as a one-value `enum`. Empty for any other field."""
    if not isinstance(schema, dict):
        return ()
    if 'const' in schema:
        return (schema['const'],)
    values = schema.get('enum')
    return tuple(values) if isinstance(values, list) and len(values) == 1 else ()


def unknown_field(key: str, known: list[str], at: PointerParts) -> Issue:
    closest = tuple(difflib.get_close_matches(key, known, n=3))
    hint = f'the closest is {closest[0]}, and ' if closest else ''
    return Issue('unknown-field', pointer(at), f'{key!r} is not a field here; {hint}this section takes {", ".join(known)}', closest)


def tag_issue(value: Any, tags: list[Any], at: PointerParts) -> Issue:
    """A tag field holding none of the tags its branches take, naming the closest."""
    names = [str(tag) for tag in tags]
    closest = tuple(difflib.get_close_matches(value, names, n=1)) if isinstance(value, str) else ()
    hint = f'the closest is {closest[0]}, and ' if closest else ''
    return Issue('schema-enum', pointer(at), f'{value!r} is not a {at[-1]} here; {hint}this field takes {", ".join(names)}', closest)


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
            yield from share_ceiling_issues(row.values(), (*at, 'dataset', 'conditionals', index, 'table', given))
        if conditional.get('other'):
            yield from share_ceiling_issues(conditional['other'].values(), (*at, 'dataset', 'conditionals', index, 'other'))
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
    stored = (column.get('string') or {}).get('pattern')
    hinted = ((column.get('semantic') or {}).get('generator_hint') or {}).get('pattern')
    for path, pattern in ((('string', 'pattern'), stored), (('semantic', 'generator_hint', 'pattern'), hinted)):
        problem = _core.pattern_problem(pattern) if isinstance(pattern, str) else None
        if problem:
            yield Issue('invalid-pattern', pointer((*at, *path)), problem)


def quantile_level(name: str) -> float:
    """The level a quantile name stands for: p05 is 0.05, p50 is 0.5 and p999 is 0.999."""
    return float('0.' + name[1:])


def listed_section(column: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    """The name of the section holding a profile column's `top_values`, with the section, or None where it lists none."""
    for name in LISTING_SECTIONS:
        section = column.get(name) or {}
        if section.get('top_values'):
            return name, section
    return None


def described_rows(document: dict[str, Any]) -> int | None:
    """The rows a profile's rates are shares of: those its sample read, or else every row."""
    sample = (document.get('generated') or {}).get('sample') or {}
    rows: int | None = sample.get('rows_read') or (document.get('dataset') or {}).get('row_count')
    return rows


def quantile_issues(quantiles: dict[str, float] | None, at: PointerParts) -> Iterator[Issue]:
    by_level = sorted((quantiles or {}).items(), key=lambda item: quantile_level(item[0]))
    for (lower, low), (upper, high) in zip(by_level, by_level[1:], strict=False):
        if high < low:
            yield Issue('quantiles-not-rising', pointer((*at, upper)), f'{upper} ({high}) is below {lower} ({low})')


def rate_sum_issues(rates: Any, at: PointerParts) -> Iterator[Issue]:
    total = sum(rates)
    if abs(total - 1) > RATE_SUM_TOLERANCE:
        yield Issue('rates-not-summing-to-one', pointer(at), f'the rates sum to {total:.4g}, not 1')


def share_ceiling_issues(shares: Any, at: PointerParts) -> Iterator[Issue]:
    """Where a table row's shares sum past 1. The share they leave short of it is the rows of values the row does not
    state."""
    total = sum(shares)
    if total - 1 > RATE_SUM_TOLERANCE:
        yield Issue('rates-over-one', pointer(at), f'the shares sum to {total:.4g}, over 1')


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
            tables = ', '.join(profile.document.get('tables') or {})
            message = f"this file is a bundle of the tables {tables}; profile one table's folder for a table profile"
            imported_from = (profile.document.get('generated') or {}).get('imported_from')
            if imported_from:
                message = f'this file is a bundle of the tables {tables} in {imported_from}; import one of them with --table, or table='
            issues = [Issue('not-a-table', '/kind', message)]
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

    @classmethod
    def expand(cls, columns: Any, *, rows: int | None = None) -> 'Profile':
        """The ordinary profile `columns` stands for, to save as YAML and shape by hand, or to check generated data against.

        `columns` is a list of column names, each taking the fake its name hints, or a dict of each name to a column such as
        `sd.fakes.email`, `sd.integers(18, 90)` or None. It may also be a short YAML file of `rows` and a `columns` map, a
        profile file, an example's name, a Profile, or a schema: a pandera `DataFrameModel` or `DataFrameSchema`, a
        dataframely `Schema`, or a Spark `StructType`. `rows` is the row count the profile records, in place of the source's.
        A name that hints no fake raises InvalidProfile naming the closest. `generate`, `write` and the rest take the same.
        """
        from doppeldata.profile_sources import expanded_profile

        return expanded_profile(columns, rows)

    @classmethod
    def from_data_file(cls, path: str | Path) -> 'Profile':
        """The profile a Parquet or Arrow IPC file holds, where `write` with `embed_profile=True` wrote it. A file holding
        none raises SourceError. Every argument taking a profile file also takes such a data file."""
        from doppeldata.embedded_profiles import data_file_profile

        return data_file_profile(path)

    @classmethod
    def from_glue(
        cls, database: str, table: str, *, region: str | None = None, detected_entities: Mapping[str, Sequence[str]] | None = None
    ) -> 'Profile':
        """A seed of a Glue Data Catalog table: its columns, format, CSV dialect and header, and partition keys drawing
        the values the catalog lists, else those its partition projection declares.

        Reads `glue:GetTable` and `glue:GetPartitions` through boto3, in `region` or the AWS environment's. The library
        does not install boto3. Without it the call raises PackagesMissing, which names how to install it.
        `detected_entities` maps a column to the Glue entity types `classifyColumns` found in it, such as
        `{'email': ['EMAIL']}`, and gives it the type the shipped detector pack declares for the first. A key naming no
        column raises SourceError. The packs import pyarrow, so `detected_entities` needs it installed.
        """
        from doppeldata.seed_profiles.glue_catalog import glue_profile

        return glue_profile(database, table, region=region, detected_entities=detected_entities)

    @classmethod
    def from_odcs(cls, path: str | Path, *, table: str | None = None) -> 'Profile':
        """A seed of one schema object in an Open Data Contract Standard v3 contract, named by `table` where it holds
        several. A property's types, `logicalTypeOptions`, `enum`, `required`, keys and quality checks shape its column.
        """
        from doppeldata.seed_profiles.odcs_contracts import odcs_profile

        return odcs_profile(path, table=table)

    @classmethod
    def from_json_schema(cls, path: str | Path) -> 'Profile':
        """A seed of the record a JSON Schema describes, each property a column and a nested object a struct."""
        from doppeldata.seed_profiles.json_schemas import json_schema_profile

        return json_schema_profile(path)

    @classmethod
    def from_table_schema(cls, path: str | Path) -> 'Profile':
        """A seed of the table a Frictionless Table Schema describes, its first missing value written as a CSV's null."""
        from doppeldata.seed_profiles.table_schemas import table_schema_profile

        return table_schema_profile(path)

    @classmethod
    def from_data_package(cls, path: str | Path, *, table: str | None = None) -> 'Profile':
        """A seed of one resource of a Frictionless Data Package, named by `table` where it holds several.

        `Bundle.from_data_package` reads every resource, and their foreign keys.
        """
        from doppeldata.seed_profiles.table_schemas import data_package_profile

        return data_package_profile(path, table=table)

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
        """Write the profile as YAML, or as JSON to a `.json` path, keeping comments and key order."""
        target = Path(path) if path is not None else self.path
        if target is None:
            raise ValueError('this profile has no file of its own, so save needs a path')
        write_document(self.document, target)
        self.path = target


@dc.dataclass(frozen=True)
class StatedRelationship:
    """A relationship a bundle states, with the child table and columns its `from` names, and the parent's its `to` does."""

    document: Mapping[str, Any]
    child: str
    child_columns: tuple[str, ...]
    parent: str
    parent_columns: tuple[str, ...]


class Bundle:
    """Several tables' profiles and their relationships: a `kind: bundle` file, or a directory holding `bundle.yaml`."""

    def __init__(self, document: Any, path: Path | None, tables: dict[str, Profile]) -> None:
        self.document = document
        self.path = path
        self.tables = tables

    @classmethod
    def of(cls, name: str, tables: dict[str, Profile], relationships: list[Any] | None = None) -> 'Bundle':
        """A bundle named `name` of `tables` and the `relationships` between them, with no file of its own, so its
        `save` needs a path."""
        document: dict[str, Any] = {'kind': 'bundle', 'name': name, 'tables': {}}
        if relationships:
            document['relationships'] = relationships
        return cls(document, None, tables)

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

    @classmethod
    def example(cls, name: str) -> 'Bundle':
        """An example bundle the library ships, every table inline. It has no file of its own, so `save` needs a path."""
        with importlib.resources.as_file(bundle_example_resource(name)) as path:
            bundle = cls.load(path)
        bundle.path = None
        return bundle

    @property
    def name(self) -> str:
        return str(self.document['name'])

    def relationships(self) -> list[StatedRelationship]:
        """Each relationship the bundle states, in order. An end is `table.column`, split at its first dot, as the engine
        reads it."""
        stated = []
        for relationship in to_plain(self.document).get('relationships') or []:
            child = [reference.split('.', 1) for reference in relationship['from']]
            parent = [reference.split('.', 1) for reference in relationship['to']]
            children, parents = tuple(column for _, column in child), tuple(column for _, column in parent)
            stated.append(StatedRelationship(relationship, child[0][0], children, parent[0][0], parents))
        return stated

    @classmethod
    def from_data_package(cls, path: str | Path) -> 'Bundle':
        """A bundle of a seed for each resource of a Frictionless Data Package, and a relationship for each foreign key
        of one field. It has no file of its own, so its `save` needs a path.
        """
        from doppeldata.seed_profiles.table_schemas import data_package_bundle

        return data_package_bundle(path)

    def to_plain(self) -> dict[str, Any]:
        """The bundle with every table inline, as a single-file bundle would hold it."""
        return to_plain(self.document) | {'tables': {name: table.to_plain() for name, table in self.tables.items()}}

    def validate(self) -> list[Issue]:
        """Every reason the bundle or one of its tables is invalid. Empty when all are valid."""
        return check(self.to_plain())

    def inline(self) -> Any:
        """The document with every table written inside it, as a single-file bundle holds them."""
        document = copy.copy(self.document)
        document['tables'] = type(self.document['tables'])((name, table.document) for name, table in self.tables.items())
        return document

    def to_yaml(self) -> str:
        """The bundle as the single-file YAML `save` writes."""
        text = io.StringIO()
        yaml().dump(self.inline(), text)
        return text.getvalue()

    def save(self, path: str | Path) -> None:
        """Write the bundle as one file with every table inline, as YAML, or as JSON to a `.json` path."""
        write_document(self.inline(), Path(path))
        self.path = Path(path)

    def canonical(self) -> bytes:
        plain = self.to_plain()
        plain['tables'] = {name: without_unhashed_fields(table) for name, table in plain['tables'].items()}
        return rfc8785.dumps(without_unhashed_fields(plain))

    @property
    def digest(self) -> str:
        return digest_of(self.canonical())
