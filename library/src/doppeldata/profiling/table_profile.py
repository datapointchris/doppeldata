"""One table's profile, computed through a backend's calls. No literal it stores rests on fewer than k_min rows."""

import collections
import dataclasses as dc
import datetime as dt
import importlib
import math
import random
import warnings
from collections.abc import Callable
from collections.abc import Mapping
from typing import TYPE_CHECKING
from typing import Any

from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.comments import CommentedSeq

from doppeldata import _core
from doppeldata.errors import FitsSkipped
from doppeldata.errors import PoorFits
from doppeldata.privacy_policy import ColumnFacts
from doppeldata.privacy_policy import Policy
from doppeldata.privacy_policy import Treatment
from doppeldata.privacy_policy import column_treatment
from doppeldata.privacy_policy import restricted_zip_prefixes
from doppeldata.privacy_policy import withheld
from doppeldata.privacy_policy.column_treatment import OLDEST_AGE
from doppeldata.privacy_policy.column_treatment import ValueFamily
from doppeldata.privacy_policy.literal_fields import FieldKind
from doppeldata.privacy_policy.literal_fields import sequence_start
from doppeldata.privacy_policy.treatment_rules import REFUSED
from doppeldata.privacy_policy.treatment_rules import ZIP_PREFIX
from doppeldata.profile_format import DERIVED_FROM
from doppeldata.profile_format import LARGEST_EXACT_INTEGER
from doppeldata.profile_format import TOOL
from doppeldata.profile_format import described_rows
from doppeldata.profile_format import generated_section
from doppeldata.profile_format import listed_section
from doppeldata.profiling import file_traits
from doppeldata.profiling import json_paths
from doppeldata.profiling import text_types
from doppeldata.profiling.backend import TEXT_FORMATS
from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import JsonNode
from doppeldata.profiling.backend import ParsedType
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import SourceFormat
from doppeldata.profiling.backend import Table
from doppeldata.profiling.backend import is_hidden
from doppeldata.profiling.column_dependence import DependenceColumn
from doppeldata.profiling.column_dependence import FrozenPairs
from doppeldata.profiling.column_dependence import column_dependence
from doppeldata.profiling.derived_columns import derived_columns
from doppeldata.profiling.detector_packs import EnabledPack
from doppeldata.profiling.detector_packs import SuppliedCodeList
from doppeldata.profiling.profiled_columns import Answers
from doppeldata.profiling.profiled_columns import ProfiledColumns
from doppeldata.profiling.recorded_counts import RATE_DIGITS
from doppeldata.profiling.recorded_counts import clear_of_the_ends
from doppeldata.profiling.recorded_counts import kept_values
from doppeldata.profiling.recorded_counts import rate
from doppeldata.profiling.recorded_counts import significant
from doppeldata.profiling.recorded_counts import written_number
from doppeldata.profiling.statistic_catalog import COLUMN_STATISTICS
from doppeldata.profiling.statistic_catalog import NUMERIC
from doppeldata.profiling.statistic_catalog import PROFILED
from doppeldata.profiling.statistic_catalog import QUANTILES
from doppeldata.profiling.statistic_catalog import TIME_UNITS
from doppeldata.profiling.statistic_catalog import VALUE_LISTS
from doppeldata.profiling.statistic_catalog import ColumnKind
from doppeldata.profiling.statistic_catalog import FrequencyKey
from doppeldata.profiling.statistic_catalog import ValueList
from doppeldata.profiling.statistic_catalog import shape_of
from doppeldata.profiling.table_keys import KeyColumn
from doppeldata.profiling.table_keys import table_keys
from doppeldata.profiling.text_types import TextColumn

if TYPE_CHECKING:
    import pyarrow as pa

    from doppeldata.profiling.semantic_types import Typing

SHAPES_KEPT = 20
SAMPLE_SEED = 0
# One seeded sample of this many rows feeds grex and the semantic detectors.
VALUE_SAMPLE_ROWS = 10_000
PATTERN_VALUES = 1_000
# A pattern stands for the dominant shape, and the generator prefers it to the shapes, so it is
# stored only where that shape holds most of the values.
PATTERN_SHAPE_SHARE = 0.5
MOMENT_DIGITS = 6
SHAPE_MOMENT_DECIMALS = 6
# A fit's parameters can cancel, as a near-normal lognorm's loc and scale do, so they keep more digits than a moment.
FIT_PARAMETER_DIGITS = 8
TRIMMED_STATISTICS = frozenset({'trimmed_low', 'trimmed_high', 'length_trimmed_low', 'length_trimmed_high', 'distinct_days'})
# Equal-depth bins carry a skewed column's shape in fewer bins than equal-width ones.
HISTOGRAM_BINS = 32
EQUAL_DEPTH_LEVELS = tuple(index / HISTOGRAM_BINS for index in range(1, HISTOGRAM_BINS))
# A numeric column's named quantiles and its interior bin edges come from one sort.
NUMERIC_LEVELS = tuple(sorted({*QUANTILES.values(), *EQUAL_DEPTH_LEVELS}))
LENGTH_LEVELS = tuple(QUANTILES.values())
# Each place tried is one more comparison of every float value. A column holding more places is drawn continuous.
DECIMAL_PLACES_TRIED = 9
# A scipy fit's time grows with its values, so a fit is made on a seeded sample this size, which the correlations read too.
FIT_SAMPLE_ROWS = 20_000
# A strictly monotonic integer column with gaps is a counting sequence where it holds this share of the integers between
# its trimmed extremes at its least step, so a key some of whose rows were rejected still generates unique.
SEQUENCE_FILL = 0.5
# What a counting sequence is decided from.
SEQUENCE_STATISTICS = ('null_count', 'step_min', 'step_max', 'min', 'max', 'trimmed_low', 'trimmed_high')
# A column with fewer distinct values is better described by its frequencies than by a family.
FIT_MIN_DISTINCT = 50
FITTED_FAMILIES_MODULE = 'doppeldata.profiling.fitted_families'
# Imported by name, because it imports pyarrow, which a library-only install lacks.
SEMANTIC_TYPES_MODULE = 'doppeldata.profiling.semantic_types'
LOGICAL_TYPES = {
    ColumnKind.INTEGER: 'integer',
    ColumnKind.FLOAT: 'number',
    ColumnKind.DECIMAL: 'number',
    ColumnKind.STRING: 'string',
    ColumnKind.BOOLEAN: 'boolean',
    ColumnKind.DATE: 'date',
    ColumnKind.DATETIME: 'timestamp',
}
CONTAINER_LOGICAL_TYPES = {'list': 'array', 'fixed_size_list': 'array', 'struct': 'object'}
VALUE_FAMILIES = {
    ColumnKind.INTEGER: ValueFamily.NUMERIC,
    ColumnKind.FLOAT: ValueFamily.NUMERIC,
    ColumnKind.DECIMAL: ValueFamily.NUMERIC,
    ColumnKind.STRING: ValueFamily.STRING,
    ColumnKind.DATE: ValueFamily.TEMPORAL,
    ColumnKind.DATETIME: ValueFamily.TEMPORAL,
}
# Every three-digit prefix there is, so none is left uncounted.
ZIP_PREFIXES_COUNTED = 1000
RESTRICTED_ZIP_PREFIX = '000'
ZIP_SUFFIX = r'\d{2}'
ZIP_PLUS_FOUR_SUFFIX = r'\d{2}-\d{4}'
ZIP_PLUS_FOUR_SHAPE = '99999-9999'
# The last bin of a capped age histogram stands for every age over 89, and its rows are drawn as 90.
AGE_BIN_EDGES = (OLDEST_AGE - 0.5, OLDEST_AGE + 0.5)


def profiled_kind(column: dict[str, Any]) -> ColumnKind:
    """The kind a profile column was profiled as, read back from the logical type `LOGICAL_TYPES` gave it.

    Float and decimal share the logical type `number`, so an Arrow decimal type tells them apart.
    """
    kinds = [kind for kind, logical in LOGICAL_TYPES.items() if logical == column.get('logical_type')]
    if len(kinds) > 1:
        decimal = (column.get('physical_type') or '').startswith('decimal')
        kinds = [kind for kind in kinds if (kind is ColumnKind.DECIMAL) == decimal]
    return kinds[0] if kinds else ColumnKind.OTHER


Stat = Callable[[str], Any]


@dc.dataclass(frozen=True)
class FrozenLayout:
    """An earlier profile's histogram edges and value lists, by column, which a re-profile counts its rows against, and
    the pairs of columns it states a dependence between, which a re-profile states again wherever its floors allow."""

    # Each numeric column's histogram kind and edges.
    histograms: dict[str, tuple[str, tuple[float, ...]]] = dc.field(default_factory=dict)
    values: dict[str, 'FrozenValues'] = dc.field(default_factory=dict)
    pairs: FrozenPairs = dc.field(default_factory=FrozenPairs)

    @classmethod
    def of(cls, document: dict[str, Any]) -> 'FrozenLayout':
        histograms, values = {}, {}
        described = described_rows(document)
        for column in document.get('columns') or []:
            histogram = (column.get('numeric') or {}).get('histogram')
            if histogram and len(histogram['edges']) > 1:
                histograms[column['name']] = (histogram['kind'], tuple(histogram['edges']))
            listing = listed_section(column)
            if listing:
                section_name, section = listing
                other = section.get('other_fraction') or 0.0
                # A list frozen from a profile before it carries that profile's drift in its new_value_rows, which the baseline leaves out.
                drifted = (section.get('overflow') or {}).get('new_value_rows') or 0
                held = described * (1 - (column.get('null_fraction') or 0.0)) if described else 0
                baseline = max(0.0, other - drifted / held) if held else other
                values[column['name']] = FrozenValues(tuple(entry['value'] for entry in section['top_values']), baseline, section_name)
        return cls(histograms, values, FrozenPairs.of(document))


def relisted_columns(earlier: dict[str, Any], now: dict[str, Any]) -> list[str]:
    """The columns of `now` whose values `earlier` lists in a section other than the one their kind lists in now."""
    listed = {column['name']: listing[0] for column in earlier.get('columns') or [] if (listing := listed_section(column))}
    relisted = []
    for column in now.get('columns') or []:
        listed_now = VALUE_LISTS.get(profiled_kind(column))
        if column['name'] in listed and (listed_now is None or listed_now.section != listed[column['name']]):
            relisted.append(column['name'])
    return relisted


@dc.dataclass(frozen=True)
class FrozenValues:
    """A column's top values, in the order an earlier profile lists them, the share of values outside them it started
    from, and the section that lists them, which names the kind of value they are.
    """

    values: tuple[Any, ...]
    baseline_other_rate: float
    section: str


@dc.dataclass(frozen=True)
class Binning:
    """The edges one numeric column's rows are counted against, whether an earlier profile froze them, and whether
    its last bin holds every row above it, as an age's over-89 bin does."""

    kind: str
    edges: tuple[float, ...]
    frozen: bool
    capped: bool = False


def answered(backend: Backend, table: Table, requests: list[Request]) -> Answers:
    return {(request.column, request.statistic): value for request, value in backend.aggregate(table, requests).items()}


@dc.dataclass(frozen=True)
class ProfiledTable:
    """A table's profile document, its every row at the types the profile gives its columns, and those columns."""

    document: dict[str, Any]
    table: Table
    specs: list[ColumnSpec]


def profile_table(
    backend: Backend,
    source: Source,
    *,
    sample: int | None,
    k_min: int,
    policy: Policy,
    packs: list[EnabledPack],
    code_lists: Mapping[str, SuppliedCodeList],
    readers: dict[str, str],
    frozen: FrozenLayout | None = None,
    type_mix_columns: frozenset[str] = frozenset(),
) -> ProfiledTable:
    """The profile of `source`, from every row or from a seeded sample of `sample` rows, written under `policy`.

    The detectors of `packs` type each column `semantic_types.detected_columns` names, reading `code_lists`, and the
    type decides which rule of the policy holds the column. With `frozen`, each column it names is counted against the
    earlier profile's edges and value list. Keys and repeated rows are counted over every row, sampled or not. Each
    string column `type_mix_columns` names is typed by its text, as a text source's columns are.
    """
    read = backend.read(source)
    table = read.table
    first = [Request(None, 'rows')]
    if sample is not None:
        # A distinct ratio does not scale from a sample to the whole table, so a sampled profile counts distinct values in every row.
        first += [Request(spec.name, 'distinct_count') for spec in visible_specs(backend, table) if spec.kind in PROFILED]
    counted = answered(backend, table, first)
    rows = counted.pop((None, 'rows'))
    whole_distinct: dict[str, int] = {column: count for (column, _), count in counted.items() if column is not None}
    sampled = False
    if sample is not None and rows > sample:
        table, sampled = backend.sampled(table, sample, SAMPLE_SEED), True
    elements = read.json.elements if read.json else {}
    settings = ColumnSettings(source.format, k_min, policy, packs, code_lists, frozen or FrozenLayout(), elements, type_mix_columns)
    json_root = read.json.paths if read.json else None
    profiled = profile_columns(backend, table, settings, rows, whole_distinct, json_root, read.table if sampled else None)
    full = profiled.table if profiled.whole is None else profiled.whole
    dataset: dict[str, Any] = {'rows': rows}
    cell_floors: dict[tuple[str, str, str], int] = {}
    if read.json is None and profiled.dependence_sample is not None:
        found, cell_floors = dataset_sections(backend, full, profiled, rows, whole_distinct, k_min, settings.frozen.pairs)
        dataset |= found
    fitting, _ = fitting_module()
    sampling = {'strategy': 'reservoir', 'rows_read': profiled.described} if profiled.described < rows else {'strategy': 'full'}
    derived = derivation(backend.name, source.fingerprint, fitting.FITTER if fitting else None, packs, code_lists, readers)
    generated = generated_section(**derived, sample=sampling)
    document: dict[str, Any] = {
        'kind': 'table',
        'name': source.name,
        'generated': generated,
        'privacy': {},
    }
    if source.format:
        document['source'] = file_traits.source_section(backend, source, read, profiled, rows, k_min)
    document['dataset'] = dataset
    document['columns'] = profiled.columns
    return ProfiledTable(reviewable(withheld(document, policy, k_min, cell_floors)), full, profiled.specs)


def dataset_sections(
    backend: Backend,
    full: Table,
    profiled: ProfiledColumns,
    rows: int,
    whole_distinct: dict[str, int],
    k_min: int,
    frozen: FrozenPairs,
) -> tuple[dict[str, Any], dict[tuple[str, str, str], int]]:
    """The dataset's repeated rows and keys over `full`, and its columns' dependence, with the fewest rows a literal of
    each entry naming a parent rests on. Each column computed from others gains its `derived` entry."""
    profiled_specs = [spec for spec in profiled.specs if spec.kind in PROFILED]
    distinct: dict[str, int] = {
        spec.name: whole_distinct[spec.name] if spec.name in whole_distinct else profiled.stats[(spec.name, 'distinct_count')]
        for spec in profiled_specs
    }
    entries = {entry['name']: entry for entry in profiled.columns}
    key_columns = [KeyColumn(spec, distinct[spec.name], profiled.stats[(spec.name, 'null_count')] > 0) for spec in profiled_specs]
    found = table_keys(backend, full, profiled.dependence_sample, key_columns, rows, k_min)
    columns = [DependenceColumn(spec, distinct[spec.name], entries[spec.name]) for spec in profiled_specs if spec.name in entries]
    derived = derived_columns(backend, profiled.dependence_sample, FIT_SAMPLE_ROWS, profiled.table, columns, k_min)
    dependence = column_dependence(backend, profiled.dependence_sample, profiled.table, columns, profiled.described, k_min, frozen, derived)
    for name, entry in derived.items():
        entries[name]['derived'] = entry
    return found | dependence.sections, dependence.cell_floors


def visible_specs(backend: Backend, table: Table) -> list[ColumnSpec]:
    """The table's columns a profile describes, leaving out those the reader added."""
    return [spec for spec in backend.schema(table) if not is_hidden(spec.name)]


@dc.dataclass(frozen=True)
class ColumnSettings:
    """What every table of one source is profiled with: its format, the floor, the privacy policy, the detector packs
    and the code lists they read, an earlier profile's edges and value lists, a JSON source's element tables by
    array path, and the string columns of a source of another format to type by their text, as a text source's are.
    """

    source_format: SourceFormat | None
    k_min: int
    policy: Policy
    packs: list[EnabledPack]
    code_lists: Mapping[str, SuppliedCodeList]
    frozen: FrozenLayout
    elements: dict[str, Table]
    type_mix_columns: frozenset[str] = frozenset()


def profile_columns(
    backend: Backend,
    table: Table,
    settings: ColumnSettings,
    rows: int,
    whole_distinct: dict[str, int],
    json_root: JsonNode | None,
    whole: Table | None = None,
) -> ProfiledColumns:
    """Every column of `table`, which holds `rows` rows, or the records or elements `json_root` heads in a JSON source.

    A delimited source's null tokens are read as null before its columns are typed, and each column's own are kept.
    Where `table` is a sample, `whole` is the table it was drawn from, which decides each counting sequence.
    """
    k_min, packs, layout = settings.k_min, settings.packs, settings.frozen
    text: dict[str, TextColumn] = {}
    unparsed_rows, null_token = 0, text_types.EMPTY
    paths: set[str] = set()
    parsed_types: dict[str, ParsedType] = {}
    if settings.source_format in TEXT_FORMATS:
        typed = typed_text(backend, table, json_root)
        table, text, paths = typed.table, typed.columns, typed.paths
        if json_root is None:
            null_token = most_written_token(typed.tokens)
            text = {name: dc.replace(column, missing_tokens=column_tokens(typed.tokens[name], null_token)) for name, column in text.items()}
            parsing = tuple((name, column.parsed) for name, column in text.items())
            unparsed_rows = answered(backend, table, [Request(None, 'unparsed_rows', parsing)])[(None, 'unparsed_rows')]
            parsed_types = {
                name: dc.replace(column.parsed, missing=tuple(sorted(set(typed.tokens[name]) - {text_types.EMPTY})))
                for name, column in text.items()
            }
        table = backend.parse(table, {name: column.parsed for name, column in text.items()})
    elif settings.type_mix_columns:
        named = [
            spec for spec in visible_specs(backend, table) if spec.name in settings.type_mix_columns and spec.kind is ColumnKind.STRING
        ]
        text = text_types.type_text_columns(backend, table, named, SAMPLE_SEED)
        table = backend.parse(table, {name: column.parsed for name, column in text.items()})
    specs = visible_specs(backend, table)
    stats = answered(backend, table, pass_one_requests(specs, k_min))
    described = stats[(None, 'rows')]
    shapes = {
        spec.name: [(shape, count) for shape, count in backend.top_k(table, spec.name, SHAPES_KEPT, FrequencyKey.SHAPE) if count >= k_min]
        for spec in specs
        if spec.kind is ColumnKind.STRING
    }
    detection = importlib.import_module(SEMANTIC_TYPES_MODULE)
    detected = detection.detected_columns(specs, packs)
    value_sample = backend.sample(table, VALUE_SAMPLE_ROWS, SAMPLE_SEED) if detected else None
    patterns = generalized_patterns(value_sample, shapes, stats, described, k_min)
    typings = detection.column_typings(value_sample, detected, patterns, k_min, settings.code_lists) if value_sample else {}
    treatments = {spec.name: column_treatment(column_facts(spec, typings.get(spec.name)), settings.policy) for spec in specs}
    binnings = {}
    for spec in specs:
        capped = treatments[spec.name] is Treatment.AGE
        binning = histogram_binning(spec, column_stat(stats, spec.name), described, k_min, layout, capped=capped)
        if binning:
            binnings[spec.name] = binning
    sections = {spec.name: VALUE_LISTS[spec.kind].section for spec in specs if spec.kind in VALUE_LISTS}
    # A list of another kind's values is not counted, so a column whose kind changed lists its values afresh.
    listed = {
        name: frozen
        for name, frozen in layout.values.items()
        if sections.get(name) == frozen.section and treatments[name] is not Treatment.ZIP_PREFIX
    }
    counting = [Request(name, 'histogram_counts', binning.edges) for name, binning in binnings.items()]
    counting += [Request(name, 'listed_value_counts', frozen_values.values) for name, frozen_values in listed.items()]
    stats |= answered(backend, table, counting)
    if whole is not None and parsed_types:
        whole = backend.parse(whole, parsed_types)
    sequences, whole_steps = counting_sequences(backend, whole if json_root is None else None, specs, stats, described, rows, k_min)
    stats |= whole_steps
    distinct: dict[str, tuple[int, int]] = {
        spec.name: (whole_distinct[spec.name] if spec.name in whole_distinct else stats[(spec.name, 'distinct_count')], rows)
        for spec in specs
        if spec.kind in PROFILED
    }
    fitting, import_error = fitting_module()
    # A fit's support would reach past what the policy withholds, so a column kept to no value, or an age, is not fitted.
    fitted = [spec for spec in specs if not treatments[spec.name].keeps_no_value and treatments[spec.name] is not Treatment.AGE]
    # The fits and the correlations read one sample, which a record table holds and an array's elements need not.
    dependence = backend.sampled(table, FIT_SAMPLE_ROWS, SAMPLE_SEED) if json_root is None else None
    fitted = [spec for spec in fitted if spec.name not in sequences]
    fits = column_fits(
        backend, table if dependence is None else dependence, fitted, stats, described, distinct, k_min, fitting, import_error
    )
    columns = [
        column_entry(
            backend,
            table,
            spec,
            column_stat(stats, spec.name),
            described,
            distinct,
            text.get(spec.name),
            shapes,
            patterns,
            ColumnLayout(binnings.get(spec.name), listed.get(spec.name), spec.name in fits, fits.get(spec.name), sequences.get(spec.name)),
            typings.get(spec.name),
            treatments[spec.name],
            k_min,
        )
        for spec in specs
    ]
    profiled = ProfiledColumns(
        columns,
        table,
        specs,
        stats,
        described,
        unparsed_rows,
        null_token,
        dependence_sample=dependence,
        parsed_types=parsed_types,
        whole=whole,
    )
    if json_root is None:
        return profiled
    json_table = json_profiled(backend, profiled, settings, json_root)
    if json_root.path:
        return dc.replace(profiled, json=json_table)
    outside = [entry for entry in columns if entry['name'] not in paths]
    return dc.replace(profiled, columns=json_paths.assembled(json_root, json_table) + outside, json=json_table)


def column_facts(spec: ColumnSpec, typing: 'Typing | None') -> ColumnFacts:
    """What the policy reads of a column before its entry is written: its kind, and the type a detector gave it."""
    detector = typing.candidate.detector if typing else None
    return ColumnFacts(
        spec.name,
        VALUE_FAMILIES.get(spec.kind, ValueFamily.OTHER),
        spec.kind is ColumnKind.INTEGER,
        detector.type if detector else None,
        detector.sensitivity if detector else None,
    )


@dc.dataclass(frozen=True)
class TypedText:
    """A text table with its null tokens read as null, each column's type, the rows of each null token by column, and the
    columns a JSON tree holds.
    """

    table: Table
    columns: dict[str, TextColumn]
    tokens: dict[str, dict[str, int]]
    paths: set[str]


def typed_text(backend: Backend, table: Table, json_root: JsonNode | None) -> TypedText:
    """The type each text column of `table` is read as. A delimited table's null tokens are read as null first."""
    visible = visible_specs(backend, table)
    delimited = json_root is None
    tokens = text_types.null_tokens(backend, table, visible) if delimited else {}
    nulled = {
        name: ParsedType('string', missing=missing) for name, found in tokens.items() if (missing := tuple(set(found) - {text_types.EMPTY}))
    }
    if nulled:
        table = backend.parse(table, nulled)
    # A column outside the JSON tree, such as a Hive key read from the path, is typed by its text alone.
    paths = {node.column for node in json_paths.table_nodes(json_root)} if json_root else set()
    from_text = [spec for spec in visible if spec.name not in paths]
    from_json = [spec for spec in visible if spec.name in paths]
    text = text_types.type_text_columns(backend, table, from_text, SAMPLE_SEED) if from_text else {}
    if from_json:
        text |= text_types.type_text_columns(backend, table, from_json, SAMPLE_SEED, json=True)
    return TypedText(table, text, tokens, paths)


def typed_table(backend: Backend, source: Source) -> Table:
    """The source's record table with each column at the type a profile of it gives the column."""
    read = backend.read(source)
    if source.format not in TEXT_FORMATS:
        return read.table
    typed = typed_text(backend, read.table, read.json.paths if read.json else None)
    return backend.parse(typed.table, {name: column.parsed for name, column in typed.columns.items()})


def json_profiled(backend: Backend, profiled: ProfiledColumns, settings: ColumnSettings, root: JsonNode) -> json_paths.ProfiledTable:
    """One JSON table's scalar entries, each path's rows by JSON type, each array's lengths, and its elements profiled."""
    table, k_min = profiled.table, settings.k_min
    nodes = json_paths.table_nodes(root)
    requests = []
    for node in nodes:
        typed = json_paths.type_column(node.column)
        requests += [Request(typed, 'null_count'), Request(typed, 'listed_value_counts', json_paths.COUNTED_TYPES)]
        if node.kind == 'array':
            length = json_paths.length_column(node.column)
            requests += [Request(length, 'quantiles', LENGTH_LEVELS), Request(length, 'listed_value_counts', (0,))]
    stats = answered(backend, table, requests)
    counts, lengths, elements = {}, {}, {}
    for node in nodes:
        typed = json_paths.type_column(node.column)
        null, objects, arrays = stats[(typed, 'listed_value_counts')]
        counts[node.column] = json_paths.TypeCounts(stats[(typed, 'null_count')], null, objects, arrays)
        if node.kind == 'array' and node.element is not None:
            length = json_paths.length_column(node.column)
            at_level = dict(zip(LENGTH_LEVELS, stats[(length, 'quantiles')], strict=True))
            quantiles = {name: int(at_level[level]) for name, level in QUANTILES.items() if clear_of_the_ends(level, arrays, k_min)}
            lengths[node.column] = json_paths.ArrayLengths(stats[(length, 'listed_value_counts')][0], quantiles)
            items = settings.elements[node.path]
            item_rows = answered(backend, items, [Request(None, 'rows')])[(None, 'rows')]
            item_settings = dc.replace(settings, frozen=FrozenLayout())
            elements[node.path] = profile_columns(backend, items, item_settings, item_rows, {}, node.element).json
    entries = {column['name']: column for column in profiled.columns}
    return json_paths.ProfiledTable(
        profiled.described, entries, counts, lengths, {path: table for path, table in elements.items() if table}
    )


def most_written_token(tokens: dict[str, dict[str, int]]) -> str:
    """The null token a delimited source writes most across its columns, the empty field where none is written more."""
    written: collections.Counter[str] = collections.Counter({text_types.EMPTY: 0})
    for found in tokens.values():
        written.update(found)
    return max(written, key=lambda token: (written[token], token == text_types.EMPTY))


def column_tokens(found: dict[str, int], null_token: str) -> dict[str, int]:
    """A column's null tokens other than the source's own, which a writer prints in place of the null it writes."""
    return {token: rows for token, rows in found.items() if token != null_token}


def derivation(
    engine: str,
    fingerprint: str | None,
    fitter: str | None,
    packs: list[EnabledPack],
    code_lists: Mapping[str, SuppliedCodeList],
    readers: dict[str, str],
) -> dict[str, Any]:
    """The `DERIVED_FROM` fields a profile made now with this engine, listing, fitter, detector packs, code lists and
    readers writes.
    """
    values = {
        'tool': TOOL,
        'engine': engine,
        'fitter': fitter,
        'detector_packs': {enabled.pack.name: enabled.provided_by for enabled in packs},
        'code_lists': {name: supplied.digest for name, supplied in sorted(code_lists.items())} or None,
        'readers': readers or None,
        'source_fingerprint': fingerprint,
    }
    return {field: values[field] for field in DERIVED_FROM if values[field] is not None}


def fitting_module() -> tuple[Any, str | None]:
    """The module that fits families, or None and the error that stopped its import.

    It is imported by name, because it imports scipy, which the library does not declare. scipy may be missing,
    or installed and built against another numpy, which fails only on import.
    """
    try:
        return importlib.import_module(FITTED_FAMILIES_MODULE), None
    except ImportError as error:
        return None, str(error)


@dc.dataclass(frozen=True)
class ColumnLayout:
    """What one column's rows were counted against and fitted to, after its first aggregate."""

    binning: Binning | None = None
    listed: FrozenValues | None = None
    # Whether fitting applies to the column, and the fit, which is None where it applied and no family fitted.
    fit_applies: bool = False
    fit: dict[str, Any] | None = None
    sequence: dict[str, int] | None = None


def pass_one_requests(specs: list[ColumnSpec], k_min: int) -> list[Request]:
    """Every catalog statistic of every column it applies to, and the row count, for one aggregate."""
    arguments = {name: k_min for name in TRIMMED_STATISTICS} | {
        'quantiles': NUMERIC_LEVELS,
        'length_quantiles': LENGTH_LEVELS,
        'decimal_places': DECIMAL_PLACES_TRIED,
    }
    requests = [Request(None, 'rows')]
    for spec in specs:
        for statistic in COLUMN_STATISTICS:
            if spec.kind in statistic.kinds:
                requests.append(Request(spec.name, statistic.name, arguments.get(statistic.name)))
    return requests


def nested_types(physical_type: str) -> dict[str, Any]:
    """A struct's fields or a list's element, each by its name and its Arrow type as pyarrow spells it, so a nested
    column the statistics do not reach still generates at the types it was read at."""
    container, _, inner = physical_type.partition('<')
    if container not in CONTAINER_LOGICAL_TYPES or not inner.endswith('>'):
        return {}
    children = []
    for part in top_level_parts(inner[:-1]):
        name, _, child_type = part.partition(': ')
        child: dict[str, Any] = {'name': name, 'physical_type': child_type}
        logical = CONTAINER_LOGICAL_TYPES.get(child_type.split('<')[0])
        if logical:
            child['logical_type'] = logical
        children.append(child | nested_types(child_type))
    if container == 'struct':
        return {'struct': {'fields': children}} if children else {}
    return {'list': {'element': children[0]}} if len(children) == 1 else {}


def top_level_parts(text: str) -> list[str]:
    """`text` split at each `, ` outside every bracket."""
    parts, depth, start = [], 0, 0
    for at, character in enumerate(text):
        depth += (character in '<([') - (character in '>)]')
        if depth == 0 and text.startswith(', ', at):
            parts.append(text[start:at])
            start = at + 2
    return [*parts, text[start:]] if text else parts


def column_stat(stats: Answers, column: str) -> Stat:
    return lambda name: stats[(column, name)]


def column_entry(
    backend: Backend,
    table: Table,
    spec: ColumnSpec,
    stat: Stat,
    described: int,
    distinct: dict[str, tuple[int, int]],
    text: TextColumn | None,
    shapes: dict[str, list[tuple[str, int]]],
    patterns: dict[str, str],
    layout: ColumnLayout,
    typing: 'Typing | None',
    treatment: Treatment,
    k_min: int,
) -> dict[str, Any]:
    """One column's entry. `distinct` holds each column's distinct count and the rows it was counted over.

    `treatment` decides what the statistics pass computes differently: a ZIP code's prefixes, no value list where it
    refuses one, and a date's extremes and gap rate to the year. What it withholds from the computed entry is left to
    `withheld`.
    """
    entry: dict[str, Any] = {'name': spec.name, 'physical_type': spec.physical_type}
    logical = LOGICAL_TYPES.get(spec.kind) or CONTAINER_LOGICAL_TYPES.get(spec.physical_type.split('<')[0])
    if logical:
        entry['logical_type'] = logical
    # A text value that did not parse is null in the parsed column, so the null rate comes from the raw text.
    nulls = text.null_count if text else stat('null_count')
    entry['nullable'] = nulls > 0
    entry['null_fraction'] = rate(nulls, described)
    tokens = {token: rate(rows, described) for token, rows in sorted(text.missing_tokens.items()) if rows >= k_min} if text else {}
    if tokens:
        entry['missing_token_fractions'] = tokens
    if text and text.non_null:
        entry['type_mix_fractions'] = {raw: rate(count, text.non_null) for raw, count in text.counts.items() if count}
    if spec.kind is ColumnKind.OTHER:
        return entry | nested_types(spec.physical_type)
    values = described - stat('null_count')
    count, counted_over = distinct[spec.name]
    prefixes = None
    if treatment is Treatment.ZIP_PREFIX and spec.kind is ColumnKind.STRING:
        prefixes = zip_prefix_counts(backend.top_k(table, spec.name, ZIP_PREFIXES_COUNTED, FrequencyKey.ZIP_PREFIX))
        # The column describes its prefixes, so its distinct count is theirs, and the generator holds that.
        count = len(prefixes)
    if counted_over:
        entry['distinct'] = {'approx_values': count, 'ratio': significant(min(1.0, count / counted_over), RATE_DIGITS)}
    # A statistic of fewer than k_min values can give one of them away, as the mean of one value does.
    if values < k_min:
        return entry
    listing = value_list(spec, treatment)
    if spec.kind in NUMERIC:
        entry['numeric'] = numeric_section(backend, table, spec, stat, values, layout, listing, k_min)
    elif spec.kind is ColumnKind.STRING:
        entry['string'] = string_section(
            backend,
            table,
            spec,
            stat,
            values,
            shapes[spec.name],
            patterns.get(spec.name),
            layout.listed,
            listing,
            k_min,
            prefixes=prefixes,
        )
    elif spec.kind in (ColumnKind.DATE, ColumnKind.DATETIME):
        entry['temporal'] = temporal_section(spec, stat, values, text, k_min, year_level=treatment is Treatment.DATE_TO_YEAR)
    elif spec.kind is ColumnKind.BOOLEAN:
        entry['boolean'] = {'true_fraction': significant(stat('true_fraction'), RATE_DIGITS)}
    if typing:
        entry |= semantic_sections(typing)
    return entry


def value_list(spec: ColumnSpec, treatment: Treatment) -> ValueList | None:
    """How `spec`'s values are listed, or None for a kind that lists none and for a column whose treatment refuses a list.

    A list the policy would withhold is never counted, so nothing stated beside it gives way to it.
    """
    if FieldKind.TOP_VALUES in REFUSED.get(treatment, frozenset()):
        return None
    return VALUE_LISTS.get(spec.kind)


def counting_sequences(
    backend: Backend, whole: Table | None, specs: list[ColumnSpec], stats: Answers, described: int, rows: int, k_min: int
) -> tuple[dict[str, dict[str, int]], Answers]:
    """Each integer column that counts, by name, with its `start` and `step`, and the statistics it is written with.

    A sample's steps span the rows it skipped, so a sampled table is decided over `whole`. A sequence found there is
    written with the whole table's steps and trimmed extremes, the ones its start follows from.
    """
    integers = [spec.name for spec in specs if spec.kind is ColumnKind.INTEGER]
    if whole is None:
        found = {name: sequence_of(column_stat(stats, name), described - stats[(name, 'null_count')], k_min) for name in integers}
        return {name: sequence for name, sequence in found.items() if sequence}, {}
    requested = [
        Request(name, statistic, k_min if statistic in TRIMMED_STATISTICS else None)
        for name in integers
        for statistic in SEQUENCE_STATISTICS
    ]
    counted = answered(backend, whole, requested)
    sequences: dict[str, dict[str, int]] = {}
    written: Answers = {}
    for name in integers:
        sequence = sequence_of(column_stat(counted, name), rows - counted[(name, 'null_count')], k_min)
        if sequence:
            sequences[name] = sequence
            written |= {(name, statistic): counted[(name, statistic)] for statistic in SEQUENCE_STATISTICS if statistic != 'null_count'}
    return sequences, written


def sequence_of(stat: Stat, values: int, k_min: int) -> dict[str, int] | None:
    """The `start` and `step` of a counting integer column holding no null, or None.

    A gapless column counts from its first row's value. A strictly monotonic one with gaps, as a key whose rejected or
    deleted rows left holes is, counts at its least step where it fills `SEQUENCE_FILL` of the integers between its
    trimmed extremes. It starts k_min - 1 steps before the trimmed extreme it counts from, so it repeats no value.
    """
    step_min, step_max = stat('step_min'), stat('step_max')
    if step_min is None or stat('null_count'):
        return None
    # The start is the first row's value, the minimum or the maximum by the step's sign. It follows from
    # the trimmed extreme at that end and the step, so storing it gives away no more than they do.
    start = int(stat('max') if step_min < 0 else stat('min'))
    if step_min == step_max != 0 and max(abs(start), abs(step_min)) <= LARGEST_EXACT_INTEGER:
        return {'start': start, 'step': int(step_min)}
    if not (step_min > 0 or step_max < 0) or not has_trimmed_extremes(values, k_min):
        return None
    step = int(step_min if step_min > 0 else step_max)
    low, high = int(stat('trimmed_low')), int(stat('trimmed_high'))
    if values - 2 * (k_min - 1) < SEQUENCE_FILL * ((high - low) // abs(step) + 1):
        return None
    start = sequence_start(low if step > 0 else high, step, k_min)
    return {'start': start, 'step': step} if max(abs(start), abs(step)) <= LARGEST_EXACT_INTEGER else None


def numeric_section(
    backend: Backend,
    table: Table,
    spec: ColumnSpec,
    stat: Stat,
    values: int,
    layout: ColumnLayout,
    listing: ValueList | None,
    k_min: int,
) -> dict[str, Any]:
    """A numeric column's section. It lists values only where `listing` says how, and never for a counting sequence,
    which draws none.

    The zero rows, the rows below zero and each listed value's rows are counts stated beside the histogram, so each
    rests on k_min rows, and no bin is left with 1 to k_min - 1 rows outside them.
    """
    integral = spec.kind is ColumnKind.INTEGER

    def number(value: Any) -> int | float:
        return written_number(value, integral)

    section: dict[str, Any] = {'integral': integral}
    if spec.kind is ColumnKind.DECIMAL:
        section |= {'precision': spec.precision, 'scale': spec.scale}
    # The generator rounds a float column to its scale, so cents generate as cents.
    if spec.kind is ColumnKind.FLOAT and stat('decimal_places') is not None:
        section['scale'] = stat('decimal_places')
    if has_trimmed_extremes(values, k_min):
        section['extremes'] = {'low': number(stat('trimmed_low')), 'high': number(stat('trimmed_high')), 'method': 'trimmed_k'}
    # A moment the engine returns as NaN, such as a constant column's skew, is written as null.
    for name in ('mean', 'std'):
        section[name] = significant(stat(name), MOMENT_DIGITS) if is_finite(stat(name)) else None
    for name in ('skew', 'kurtosis'):
        # Shape moments carry no units, and a parallel sum's order moves a zero one in its last bits from run to run.
        section[name] = significant(round(stat(name), SHAPE_MOMENT_DECIMALS), MOMENT_DIGITS) + 0.0 if is_finite(stat(name)) else None
    zeros, negatives = (floored_count(stat(name), values, k_min) for name in ('zero_fraction', 'negative_fraction'))
    section['zero_fraction'] = rate(zeros, values)
    section['negative_fraction'] = rate(negatives, values)
    at_level = dict(zip(NUMERIC_LEVELS, stat('quantiles'), strict=True))
    quantiles = {name: number(at_level[level]) for name, level in QUANTILES.items() if clear_of_the_ends(level, values, k_min)}
    if quantiles:
        section['quantiles'] = quantiles
    sequence = layout.sequence
    counted = numeric_values(backend, table, spec, stat, values, layout.listed, listing, k_min) if listing and not sequence else None
    kept = counted.kept if counted else []
    zero_point = [(0, zeros)] if zeros and not any(value == 0 for value, _ in kept) else []
    stated = StatedCounts((*kept, *zero_point), negatives)
    histogram = floored_histogram(layout.binning, stat('histogram_counts'), k_min, stated) if layout.binning else None
    if layout.binning and histogram is None and negatives:
        # The rows below zero are one rate and the bins are several counts, so the rate gives way first.
        histogram = floored_histogram(layout.binning, stat('histogram_counts'), k_min, StatedCounts((*kept, *zero_point)))
        if histogram:
            section['negative_fraction'] = None
    if histogram:
        section['histogram'] = histogram
    if histogram is None and small_cell(stated.unaccounted([Region(-math.inf, math.inf, False, values)]), k_min):
        # With no bin to merge, a count stated beside the list that leaves a small cell is left out.
        section['negative_fraction'] = None
        if small_cell(StatedCounts((*kept, *zero_point)).unaccounted([Region(-math.inf, math.inf, False, values)]), k_min):
            section['zero_fraction'] = None
    if layout.fit_applies:
        section['fit'] = layout.fit
    if integral and stat('step_min') is not None:
        step_min, step_max = stat('step_min'), stat('step_max')
        section['monotonic'] = 'increasing' if step_min > 0 else 'decreasing' if step_max < 0 else 'none'
        if sequence:
            section['sequence'] = sequence
    if counted:
        section |= counted.written(values, number)
    return section


def floored_count(share: float, values: int, k_min: int) -> int:
    """The rows `share` of `values` holds, or 0 where they are fewer than k_min."""
    rows = round(share * values)
    return rows if rows >= k_min else 0


def numeric_values(
    backend: Backend,
    table: Table,
    spec: ColumnSpec,
    stat: Stat,
    values: int,
    listed: FrozenValues | None,
    listing: ValueList,
    k_min: int,
) -> 'CountedValues | None':
    """A column's values counted against `listed`, or else counted where it holds no more distinct ones than the
    listing's threshold. A float column's NaN and infinities are nulls, so a count of one is left out.
    """
    if listed is not None:
        return counted_against(listed, stat('listed_value_counts'), values, k_min)
    if stat('distinct_count') > listing.threshold:
        return None
    counted = [(value, count) for value, count in backend.top_k(table, spec.name, listing.threshold) if is_finite(value)]
    return CountedValues(*kept_values(counted, values, k_min))


@dc.dataclass(frozen=True)
class CountedValues:
    """The values a list keeps with the rows each holds, the rows of every other value, and, for a list counted against
    an earlier profile's, the rows of new values.
    """

    kept: list[tuple[Any, int]]
    other: int
    new_value_rows: int | None = None

    def written(self, values: int, written_value: Callable[[Any], Any]) -> dict[str, Any]:
        """The section's `top_values`, `other_fraction` and `overflow`, each value written by `written_value`."""
        section: dict[str, Any] = {}
        if self.kept:
            section['top_values'] = [{'value': written_value(value), 'fraction': rate(count, values)} for value, count in self.kept]
            section['other_fraction'] = rate(self.other, values)
        if self.new_value_rows is not None:
            section['overflow'] = {'new_value_rows': self.new_value_rows}
        return section


def counted_against(listed: FrozenValues, counts: list[int], values: int, k_min: int) -> CountedValues:
    """The values of `listed` counted by `counts`, and the rows outside it beyond the share it started from."""
    kept, other = kept_values(list(zip(listed.values, counts, strict=True)), values, k_min)
    # Counted from `other_fraction`'s own rows, so it discloses nothing that rate does not. The rows outside the
    # list count too, so a listed value that fell under k_min reveals no count of its own.
    beyond = other - round(listed.baseline_other_rate * values)
    return CountedValues(kept, other, beyond if beyond >= k_min else 0)


@dc.dataclass(frozen=True)
class Region:
    """A stretch of a numeric column's values, from `low` up to `high` and through `high` where `closed`, holding `rows`."""

    low: float
    high: float
    closed: bool
    rows: int

    def holds(self, value: Any) -> bool:
        return bool(self.low <= value < self.high or (self.closed and value == self.high))

    def below_zero(self) -> bool:
        return self.high < 0 or (self.high == 0 and not self.closed)

    def straddles_zero(self) -> bool:
        return self.low < 0 and not self.below_zero()

    def joined(self, above: 'Region') -> 'Region':
        return Region(self.low, above.high, above.closed, self.rows + above.rows)


@dc.dataclass(frozen=True)
class StatedCounts:
    """The counts a numeric section states beside its histogram: each listed value's rows and the zero rows, as points,
    and the rows below zero.
    """

    points: tuple[tuple[Any, int], ...] = ()
    negative: int = 0

    def unaccounted(self, regions: list[Region]) -> list[list[int]]:
        """Each region's rows no stated count accounts for, in rising order. A region holding rows on both sides of zero
        has them split there where the rows below zero are stated, since the regions wholly below zero give the rest.
        """
        below_zero, cells = 0, []
        for region in regions:
            inside = [(value, rows) for value, rows in self.points if region.holds(value)]
            unlisted = region.rows - sum(rows for _, rows in inside)
            if self.negative and region.straddles_zero():
                negative = self.negative - below_zero - sum(rows for value, rows in inside if value < 0)
                cells.append([negative, unlisted - negative])
            else:
                cells.append([unlisted])
            if region.below_zero():
                below_zero += region.rows
        return cells


def small_cell(cells: list[list[int]], k_min: int) -> bool:
    return any(0 < rows < k_min for region in cells for rows in region)


def histogram_binning(
    spec: ColumnSpec, stat: Stat, described: int, k_min: int, layout: FrozenLayout, *, capped: bool = False
) -> Binning | None:
    """The edges a numeric column's rows are counted against: the earlier profile's, or its own equal-depth ones.

    The outer edges are the trimmed extremes, and each interior one is the value at a level `k_min` rows clear of
    both ends, so every edge is a value the profile could already store. An integer column's edges sit half a unit
    below the least integer of each bin, the trimmed high's own included, and its last edge half a unit above the
    trimmed high. The generator rounds a drawn value to the nearest integer, so each bin's share is then drawn to
    the integers it counted. A `capped` column is an age: an edge above 89.5 gives way to one bin from 89.5 to
    90.5, which counts every age over 89.
    """
    if spec.kind not in NUMERIC or described - stat('null_count') < k_min:
        return None
    if spec.name in layout.histograms:
        kind, edges = layout.histograms[spec.name]
        # An earlier profile's edges may part ages over 89, so they give way to the one bin above 89.5 as fresh ones do.
        if capped and edges[-1] > AGE_BIN_EDGES[0]:
            edges = (*(edge for edge in edges if edge < AGE_BIN_EDGES[0]), *AGE_BIN_EDGES)
        return Binning(kind, tuple(edges), frozen=True, capped=capped)
    values = described - stat('null_count')
    if not has_trimmed_extremes(values, k_min):
        return None
    integral = spec.kind is ColumnKind.INTEGER
    low, high = stat('trimmed_low'), stat('trimmed_high')
    at_level = dict(zip(NUMERIC_LEVELS, stat('quantiles'), strict=True))
    interior = [at_level[level] for level in EQUAL_DEPTH_LEVELS if clear_of_the_ends(level, values, k_min)]
    if integral:
        lowest = sorted({written_number(value, True) for value in (low, *(value for value in interior if low < value <= high))})
        rising = [value - 0.5 for value in lowest] + [written_number(high, True) + 0.5]
    else:
        rising = sorted({written_number(value, False) for value in (low, *(value for value in interior if low < value < high), high)})
    if capped and rising[-1] > AGE_BIN_EDGES[0]:
        rising = [edge for edge in rising if edge < AGE_BIN_EDGES[0]] + list(AGE_BIN_EDGES)
    return Binning('equal_depth', tuple(rising), frozen=False, capped=capped) if len(rising) > 1 else None


def floored_histogram(binning: Binning, counted: list[int], k_min: int, stated: StatedCounts | None = None) -> dict[str, Any] | None:
    """The histogram with no bin or overflow of 1 to k_min - 1 rows, or holding that many outside the counts `stated`
    holds, or None where no such histogram exists.

    An overflow that small joins the outermost bin on its side, and a bin that small merges into its smaller non-empty
    neighbor, so frozen edges are kept wherever the floor allows. An empty bin discloses no row and stays. A capped
    histogram's last bin takes every row above it.
    """
    stated = stated or StatedCounts()
    below, *bins, above = counted
    if binning.capped:
        bins[-1], above = bins[-1] + above, 0
    edges = list(binning.edges)
    last = len(bins) - 1
    regions = [Region(-math.inf, edges[0], False, below)]
    regions += [Region(edges[at], edges[at + 1], at == last, count) for at, count in enumerate(bins)]
    regions.append(Region(math.nextafter(edges[-1], math.inf), math.inf, False, above))
    while True:
        cells = stated.unaccounted(regions)
        small = [at for at, region in enumerate(regions) if small_cell([[region.rows], cells[at]], k_min)]
        if small and small[0] == 0:
            regions[0:2] = [Region(-math.inf, -math.inf, False, 0), regions[0].joined(regions[1])]
            continue
        if small and small[-1] == len(regions) - 1:
            regions[-2:] = [regions[-2].joined(regions[-1]), Region(math.inf, math.inf, False, 0)]
            continue
        if not small or len(regions) == 3:
            break
        index = min(small, key=lambda at: (regions[at].rows, at))
        neighbors = [at for at in (index - 1, index + 1) if 0 < at < len(regions) - 1]
        neighbor = min(neighbors, key=lambda at: (regions[at].rows == 0, regions[at].rows, at))
        first = min(index, neighbor)
        regions[first : first + 2] = [regions[first].joined(regions[first + 1])]
        del edges[first]
    if small:
        return None
    histogram: dict[str, Any] = {'kind': binning.kind, 'edges': edges, 'bin_rows': [region.rows for region in regions[1:-1]]}
    if binning.frozen:
        histogram['overflow'] = {'rows_below': regions[0].rows, 'rows_above': regions[-1].rows}
    return histogram


def string_section(
    backend: Backend,
    table: Table,
    spec: ColumnSpec,
    stat: Stat,
    values: int,
    shapes: list[tuple[str, int]],
    pattern: str | None,
    listed: FrozenValues | None,
    listing: ValueList | None,
    k_min: int,
    *,
    prefixes: dict[str, int] | None = None,
) -> dict[str, Any]:
    """A string column's section. It lists values only where `listing` says how, counted against `listed`, an earlier
    profile's list, where there is one.

    With `prefixes`, a ZIP column's counted prefixes, the section holds them in place of its top values.
    """
    length: dict[str, Any] = {}
    if has_trimmed_extremes(values, k_min):
        length |= {'min': int(stat('length_trimmed_low')), 'max': int(stat('length_trimmed_high'))}
    length['mean'] = significant(stat('length_mean'), MOMENT_DIGITS)
    at_level = dict(zip(LENGTH_LEVELS, stat('length_quantiles'), strict=True))
    quantiles = {name: int(at_level[level]) for name, level in QUANTILES.items() if clear_of_the_ends(level, values, k_min)}
    if quantiles:
        length['quantiles'] = quantiles
    section: dict[str, Any] = {'length': length, 'whitespace_edge_fraction': significant(stat('whitespace_edge_fraction'), RATE_DIGITS)}
    if shapes:
        section['shapes'] = [{'shape': shape, 'fraction': rate(count, values)} for shape, count in shapes]
    if pattern:
        section['pattern'] = pattern
    if prefixes is not None:
        return section | zip_prefixes_section(prefixes, shapes, values, k_min)
    if listing is None:
        return section
    if listed is not None:
        section |= counted_against(listed, stat('listed_value_counts'), values, k_min).written(values, str)
    elif stat('distinct_count') <= listing.threshold:
        section |= CountedValues(*kept_values(backend.top_k(table, spec.name, listing.threshold), values, k_min)).written(values, str)
    return section


def zip_prefix_counts(counted: list[tuple[Any, int]]) -> dict[str, int]:
    """Each value's first three characters and the rows holding them, a restricted prefix counted as `000`."""
    restricted = restricted_zip_prefixes()
    merged: dict[str, int] = {}
    for prefix, count in counted:
        key = RESTRICTED_ZIP_PREFIX if prefix in restricted else str(prefix)
        merged[key] = merged.get(key, 0) + count
    return merged


def zip_prefixes_section(merged: dict[str, int], shapes: list[tuple[str, int]], values: int, k_min: int) -> dict[str, Any]:
    """A ZIP column's three-digit prefixes held by k_min rows, and the digits the generator appends to each. A prefix
    that is not three digits is counted in `other_fraction`."""
    ordered = sorted(
        ((prefix, count) for prefix, count in merged.items() if ZIP_PREFIX.fullmatch(prefix)), key=lambda item: (-item[1], item[0])
    )
    kept, other = kept_values(ordered, values, k_min)
    if not kept:
        return {}
    plus_four = bool(shapes) and shapes[0][0] == ZIP_PLUS_FOUR_SHAPE
    listed = [{'value': prefix, 'fraction': rate(count, values)} for prefix, count in kept]
    return {
        'prefixes': {'values': listed, 'other_fraction': rate(other, values), 'suffix': ZIP_PLUS_FOUR_SUFFIX if plus_four else ZIP_SUFFIX}
    }


def semantic_sections(typing: 'Typing') -> dict[str, Any]:
    """A typed column's `semantic` section, and its `quality` where a validator counted the values failing it."""
    detector = typing.candidate.detector
    semantic: dict[str, Any] = {
        'type': detector.type,
        'pack': typing.candidate.pack,
        'confidence': significant(typing.confidence, RATE_DIGITS),
        'detectors': typing.methods,
        'sensitivity': detector.sensitivity,
    }
    if typing.generator_hint:
        semantic['generator_hint'] = typing.generator_hint
    sections: dict[str, Any] = {'semantic': semantic}
    if typing.passed is not None:
        sections['quality'] = {'invalid_fraction': rate(typing.sampled - typing.passed, typing.sampled)}
    return sections


def generalized_patterns(
    sample: 'pa.Table | None', shapes: dict[str, list[tuple[str, int]]], stats: Answers, described: int, k_min: int
) -> dict[str, str]:
    """Each string column's pattern, generalized by grex from the sampled values of its dominant shape."""
    dominant = {
        name: counted[0][0]
        for name, counted in shapes.items()
        if counted and counted[0][1] >= PATTERN_SHAPE_SHARE * (described - stats[(name, 'null_count')])
    }
    if not dominant or sample is None:
        return {}
    patterns = {}
    for name, shape in dominant.items():
        matching = [value for value in sample.column(name).to_pylist() if value is not None and shape_of(value) == shape]
        # Every literal grex keeps is shared by all the values it is given, so k_min of them must back it.
        if len(matching) >= k_min:
            distinct = sorted(set(matching))
            # A seeded draw rather than the first values in order, which would share the smallest prefix.
            drawn = random.Random(SAMPLE_SEED).sample(distinct, min(PATTERN_VALUES, len(distinct)))
            pattern = _core.generalize_pattern(sorted(drawn))
            if pattern:
                patterns[name] = pattern
    return patterns


def column_fits(
    backend: Backend,
    table: Table,
    specs: list[ColumnSpec],
    stats: Answers,
    described: int,
    distinct: dict[str, tuple[int, int]],
    k_min: int,
    fitting: Any,
    import_error: str | None,
) -> dict[str, dict[str, Any] | None]:
    """Each fittable numeric column's fit, from one seeded sample, or None for each when `fitting` did not import.

    A column is fittable when it has trimmed extremes and `FIT_MIN_DISTINCT` distinct values.
    """
    fittable = []
    for spec in specs:
        if spec.kind not in NUMERIC or distinct[spec.name][0] < FIT_MIN_DISTINCT:
            continue
        if has_trimmed_extremes(described - stats[(spec.name, 'null_count')], k_min):
            fittable.append(spec)
    if not fittable:
        return {}
    if fitting is None:
        warnings.warn(FitsSkipped(import_error or 'no error given'), stacklevel=4)
        return {spec.name: None for spec in fittable}
    sample = backend.sample(table, FIT_SAMPLE_ROWS, SAMPLE_SEED)
    fits = {}
    for spec in fittable:
        stat = column_stat(stats, spec.name)
        fitted = fitting.best_fit(
            fitting.finite_values(sample.column(spec.name)),
            integral=spec.kind is ColumnKind.INTEGER,
            low=float(stat('trimmed_low')),
            high=float(stat('trimmed_high')),
            zero_fraction=stat('zero_fraction'),
        )
        fits[spec.name] = fit_entry(fitted) if fitted else None
    poor = {name: (fit['family'], fit['ks']) for name, fit in fits.items() if fit and fit['poor']}
    unfitted = [name for name, fit in fits.items() if fit is None]
    if poor or unfitted:
        warnings.warn(PoorFits(poor, unfitted, fitting.POOR_FIT_KS), stacklevel=4)
    return fits


def fit_entry(fitted: Any) -> dict[str, Any]:
    """A fit as the profile writes it. A discrete family's whole-number parameters, such as its `loc`, are written exactly."""
    discrete = _core.fitted_families()[fitted.family]
    params = {
        name: int(value) if discrete and value.is_integer() else significant(value, FIT_PARAMETER_DIGITS) + 0.0
        for name, value in fitted.params.items()
    }
    entry: dict[str, Any] = {
        'family': fitted.family,
        'params': params,
        'ks': significant(fitted.ks, RATE_DIGITS),
        'aic': significant(fitted.aic, MOMENT_DIGITS),
        'sample_rows': fitted.rows,
    }
    if fitted.zero_inflation is not None:
        entry['zero_inflation'] = significant(fitted.zero_inflation, RATE_DIGITS)
    entry['poor'] = fitted.poor
    return entry


def temporal_section(
    spec: ColumnSpec, stat: Stat, values: int, text: TextColumn | None, k_min: int, *, year_level: bool = False
) -> dict[str, Any]:
    """A date or timestamp column's section. With `year_level`, its extremes and gap rate name no date finer than a year."""
    section: dict[str, Any] = {}
    if text and text.parsed.format:
        section['format'] = text.parsed.format
    if spec.timezone:
        section['timezone'] = spec.timezone
    if has_trimmed_extremes(values, k_min):
        low, high = stat('trimmed_low'), stat('trimmed_high')
        if year_level:
            low, high = low.replace(month=1, day=1), high.replace(month=12, day=31)
            if isinstance(low, dt.datetime):
                low, high = (value.replace(hour=0, minute=0, second=0, microsecond=0) for value in (low, high))
        section['extremes'] = {'low': low.isoformat(), 'high': high.isoformat(), 'granularity': granularity(spec, stat)}
    for field, counts in (('month_fractions', 'month_counts'), ('weekday_fractions', 'weekday_counts'), ('hour_fractions', 'hour_counts')):
        if spec.kind is ColumnKind.DATETIME or counts != 'hour_counts':
            shares = floored_shares(stat(counts), k_min)
            if shares:
                section[field] = shares
    # The generator empties this share of the days between the extremes, so it is counted between them.
    if has_trimmed_extremes(values, k_min):
        first, last = day_of(stat('trimmed_low')), day_of(stat('trimmed_high'))
        if year_level:
            first, last = dt.date(first.year, 1, 1), dt.date(last.year, 12, 31)
        section['gap_fraction'] = significant(1 - stat('distinct_days') / ((last - first).days + 1), RATE_DIGITS)
    return section


def granularity(spec: ColumnSpec, stat: Stat) -> str:
    if spec.kind is ColumnKind.DATE:
        return 'day'
    return next((name for unit, _, name in TIME_UNITS if stat(f'whole_{unit}')), 'nanosecond')


def day_of(value: dt.date) -> dt.date:
    return value.date() if isinstance(value, dt.datetime) else value


def floored_shares(counts: list[int], k_min: int) -> list[float] | None:
    """Each bin's share of rows, with a bin of 1 to k_min - 1 rows emptied so no share reveals a small cell."""
    kept = [count if count >= k_min else 0 for count in counts]
    total = sum(kept)
    return [rate(count, total) for count in kept] if total else None


def has_trimmed_extremes(values: int, k_min: int) -> bool:
    """Whether the values at rank k_min from each end exist and do not cross."""
    return values >= 2 * k_min - 1


def is_finite(value: Any) -> bool:
    return value is not None and math.isfinite(value)


def reviewable(value: Any) -> Any:
    """The document as ruamel types, with each collection of plain values on one line, as a reviewer reads them."""
    if isinstance(value, dict):
        mapping = CommentedMap((key, reviewable(item)) for key, item in value.items())
        if all(not isinstance(item, dict | list) for item in value.values()):
            mapping.fa.set_flow_style()
        return mapping
    if isinstance(value, list):
        sequence = CommentedSeq(reviewable(item) for item in value)
        if all(not isinstance(item, dict | list) for item in value):
            sequence.fa.set_flow_style()
        return sequence
    return value
