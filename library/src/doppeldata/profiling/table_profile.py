"""One table's profile, computed through a backend's calls. No literal it stores rests on fewer than k_min rows."""

import dataclasses as dc
import datetime as dt
import importlib
import importlib.util
import math
import random
import warnings
from collections.abc import Callable
from typing import Any

from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.comments import CommentedSeq

from doppeldata import _core
from doppeldata.errors import FitsSkipped
from doppeldata.profile_format import LARGEST_EXACT_INTEGER
from doppeldata.profiling import text_types
from doppeldata.profiling.backend import TEXT_FORMATS
from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import Table
from doppeldata.profiling.statistic_catalog import COLUMN_STATISTICS
from doppeldata.profiling.statistic_catalog import NUMERIC
from doppeldata.profiling.statistic_catalog import PROFILED
from doppeldata.profiling.statistic_catalog import QUANTILES
from doppeldata.profiling.statistic_catalog import TIME_UNITS
from doppeldata.profiling.statistic_catalog import ColumnKind
from doppeldata.profiling.statistic_catalog import FrequencyKey
from doppeldata.profiling.statistic_catalog import shape_of
from doppeldata.profiling.text_types import TextColumn

# Deequ's DEFAULT_CARDINALITY_THRESHOLD: a column with no more distinct values than this has its values counted.
TOP_VALUES_THRESHOLD = 120
SHAPES_KEPT = 20
SAMPLE_SEED = 0
PATTERN_SAMPLE_ROWS = 10_000
PATTERN_VALUES = 1_000
# A pattern stands for the dominant shape, and the generator prefers it to the shapes, so it is
# stored only where that shape holds most of the values.
PATTERN_SHAPE_SHARE = 0.5
RATE_DIGITS = 4
MOMENT_DIGITS = 6
SHAPE_MOMENT_DECIMALS = 6
# A fit's parameters can cancel, as a near-normal lognorm's loc and scale do, so they keep more digits than a moment.
FIT_PARAMETER_DIGITS = 8
TRIMMED_STATISTICS = frozenset({'trimmed_low', 'trimmed_high', 'length_trimmed_low', 'length_trimmed_high'})
# Equal-depth bins carry a skewed column's shape in fewer bins than equal-width ones.
HISTOGRAM_BINS = 32
EQUAL_DEPTH_LEVELS = tuple(index / HISTOGRAM_BINS for index in range(1, HISTOGRAM_BINS))
# A numeric column's named quantiles and its interior bin edges come from one sort.
NUMERIC_LEVELS = tuple(sorted({*QUANTILES.values(), *EQUAL_DEPTH_LEVELS}))
LENGTH_LEVELS = tuple(QUANTILES.values())
# A scipy fit's time grows with its values, so a fit is made on a seeded sample this size.
FIT_SAMPLE_ROWS = 20_000
# A column with fewer distinct values is better described by its frequencies than by a family.
FIT_MIN_DISTINCT = 50
FITTED_FAMILIES_MODULE = 'doppeldata.profiling.fitted_families'
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

# Each answer by its column, None for the table, and its statistic's name.
Answers = dict[tuple[str | None, str], Any]
Stat = Callable[[str], Any]


@dc.dataclass(frozen=True)
class FrozenLayout:
    """An earlier profile's histogram edges and value lists, by column, which a re-profile counts its rows against."""

    # Each numeric column's histogram kind and edges.
    histograms: dict[str, tuple[str, tuple[float, ...]]] = dc.field(default_factory=dict)
    # Each string column's top values, in the order the earlier profile lists them.
    values: dict[str, tuple[str, ...]] = dc.field(default_factory=dict)

    @classmethod
    def of(cls, document: dict[str, Any]) -> 'FrozenLayout':
        histograms, values = {}, {}
        for column in document.get('columns') or []:
            histogram = (column.get('numeric') or {}).get('histogram')
            if histogram and len(histogram['edges']) > 1:
                histograms[column['name']] = (histogram['kind'], tuple(histogram['edges']))
            listed = (column.get('string') or {}).get('top_values')
            if listed:
                values[column['name']] = tuple(entry['value'] for entry in listed)
        return cls(histograms, values)


@dc.dataclass(frozen=True)
class Binning:
    """The edges one numeric column's rows are counted against, and whether an earlier profile froze them."""

    kind: str
    edges: tuple[float, ...]
    frozen: bool


def answered(backend: Backend, table: Table, requests: list[Request]) -> Answers:
    return {(request.column, request.statistic): value for request, value in backend.aggregate(table, requests).items()}


def profile_table(
    backend: Backend, source: Source, *, sample: int | None, k_min: int, frozen: FrozenLayout | None = None
) -> dict[str, Any]:
    """The profile document of `source`, from every row or from a seeded sample of `sample` rows.

    With `frozen`, each column it names is counted against the earlier profile's edges and value list.
    """
    table = backend.scan(source)
    first = [Request(None, 'rows')]
    if sample is not None:
        # A distinct ratio does not scale from a sample to the whole table, so a sampled profile counts distinct values in every row.
        first += [Request(spec.name, 'distinct_count') for spec in backend.schema(table) if spec.kind in PROFILED]
    counted = answered(backend, table, first)
    rows = counted.pop((None, 'rows'))
    whole_distinct: dict[str, int] = {column: count for (column, _), count in counted.items() if column is not None}
    if sample is not None and rows > sample:
        table = backend.scan(Source(format=None, table=backend.sample(table, sample, SAMPLE_SEED)))
    text: dict[str, TextColumn] = {}
    if source.format in TEXT_FORMATS:
        text = text_types.type_text_columns(backend, table, backend.schema(table), SAMPLE_SEED)
        table = backend.parse(table, {name: column.parsed for name, column in text.items()})
    specs = backend.schema(table)
    stats = answered(backend, table, pass_one_requests(specs, k_min))
    described = stats[(None, 'rows')]
    layout = frozen or FrozenLayout()
    binnings = {}
    for spec in specs:
        binning = histogram_binning(spec, column_stat(stats, spec.name), described, k_min, layout)
        if binning:
            binnings[spec.name] = binning
    listed = {spec.name: layout.values[spec.name] for spec in specs if spec.kind is ColumnKind.STRING and spec.name in layout.values}
    counting = [Request(name, 'histogram_counts', binning.edges) for name, binning in binnings.items()]
    counting += [Request(name, 'listed_value_counts', values) for name, values in listed.items()]
    stats |= answered(backend, table, counting)
    shapes = {
        spec.name: [(shape, count) for shape, count in backend.top_k(table, spec.name, SHAPES_KEPT, FrequencyKey.SHAPE) if count >= k_min]
        for spec in specs
        if spec.kind is ColumnKind.STRING
    }
    patterns = generalized_patterns(backend, table, shapes, stats, described, k_min)
    distinct: dict[str, tuple[int, int]] = {
        spec.name: (whole_distinct[spec.name] if spec.name in whole_distinct else stats[(spec.name, 'distinct_count')], rows)
        for spec in specs
        if spec.kind in PROFILED
    }
    fits = column_fits(backend, table, specs, stats, described, distinct, k_min)
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
            ColumnLayout(binnings.get(spec.name), listed.get(spec.name), spec.name in fits, fits.get(spec.name)),
            k_min,
        )
        for spec in specs
    ]
    generated: dict[str, Any] = {
        'tool': f'doppeldata {_core.VERSION}',
        'at': dt.datetime.now(dt.UTC).isoformat(timespec='seconds').replace('+00:00', 'Z'),
        'engine': backend.name,
    }
    if source.fingerprint:
        generated['source_fingerprint'] = source.fingerprint
    generated['sample'] = {'strategy': 'reservoir', 'rows_read': described} if described < rows else {'strategy': 'full'}
    document: dict[str, Any] = {
        'profile_version': _core.FORMAT_VERSION,
        'kind': 'table',
        'name': source.name,
        'generated': generated,
        'privacy': {'k_min': k_min},
    }
    if source.format:
        document['source'] = {'format': source.format.value}
    document['dataset'] = {'row_count': rows}
    document['columns'] = columns
    return reviewable(document)


@dc.dataclass(frozen=True)
class ColumnLayout:
    """What one column's rows were counted against and fitted to, after its first aggregate."""

    binning: Binning | None = None
    listed: tuple[str, ...] | None = None
    # Whether fitting applies to the column, and the fit, which is None where it applied and no family fitted.
    fit_applies: bool = False
    fit: dict[str, Any] | None = None


def pass_one_requests(specs: list[ColumnSpec], k_min: int) -> list[Request]:
    """Every catalog statistic of every column it applies to, and the row count, for one aggregate."""
    arguments = {name: k_min for name in TRIMMED_STATISTICS} | {'quantiles': NUMERIC_LEVELS, 'length_quantiles': LENGTH_LEVELS}
    requests = [Request(None, 'rows')]
    for spec in specs:
        for statistic in COLUMN_STATISTICS:
            if spec.kind in statistic.kinds:
                requests.append(Request(spec.name, statistic.name, arguments.get(statistic.name)))
    return requests


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
    k_min: int,
) -> dict[str, Any]:
    """One column's entry. `distinct` holds each column's distinct count and the rows it was counted over."""
    entry: dict[str, Any] = {'name': spec.name, 'physical_type': spec.physical_type}
    logical = LOGICAL_TYPES.get(spec.kind) or CONTAINER_LOGICAL_TYPES.get(spec.physical_type.split('<')[0])
    if logical:
        entry['logical_type'] = logical
    # A text value that did not parse is null in the parsed column, so the null rate comes from the raw text.
    nulls = text.null_count if text else stat('null_count')
    entry['nullable'] = nulls > 0
    entry['null_rate'] = rate(nulls, described)
    if text and text.non_null:
        entry['type_mix'] = {raw: rate(count, text.non_null) for raw, count in text.counts.items() if count}
    if spec.kind is ColumnKind.OTHER:
        return entry
    values = described - stat('null_count')
    count, counted_over = distinct[spec.name]
    if counted_over:
        entry['distinct'] = {'approx': count, 'ratio': significant(min(1.0, count / counted_over), RATE_DIGITS)}
    # A statistic of fewer than k_min values can give one of them away, as the mean of one value does.
    if values < k_min:
        return entry
    if spec.kind in NUMERIC:
        entry['numeric'] = numeric_section(spec, stat, values, layout, k_min)
    elif spec.kind is ColumnKind.STRING:
        entry['string'] = string_section(
            backend, table, spec, stat, values, shapes[spec.name], patterns.get(spec.name), layout.listed, k_min
        )
    elif spec.kind in (ColumnKind.DATE, ColumnKind.DATETIME):
        entry['temporal'] = temporal_section(spec, stat, values, text, k_min)
    elif spec.kind is ColumnKind.BOOLEAN:
        entry['boolean'] = {'true_rate': significant(stat('true_rate'), RATE_DIGITS)}
    return entry


def written_number(value: Any, integral: bool) -> int | float:
    """A data value as the profile writes it. An integer canonical JSON cannot write exactly is written as the nearest double."""
    return int(value) if integral and abs(value) <= LARGEST_EXACT_INTEGER else float(value)


def sequence_of(stat: Stat) -> dict[str, int] | None:
    """The `start` and `step` of a gapless counting integer column, or None."""
    step_min, step_max = stat('step_min'), stat('step_max')
    if step_min is None:
        return None
    # The start is the first row's value, the minimum or the maximum by the step's sign. It follows from
    # the trimmed extreme at that end and the step, so storing it gives away no more than they do.
    start = int(stat('max') if step_min < 0 else stat('min'))
    if step_min == step_max != 0 and stat('null_count') == 0 and max(abs(start), abs(step_min)) <= LARGEST_EXACT_INTEGER:
        return {'start': start, 'step': int(step_min)}
    return None


def numeric_section(spec: ColumnSpec, stat: Stat, values: int, layout: ColumnLayout, k_min: int) -> dict[str, Any]:
    integral = spec.kind is ColumnKind.INTEGER

    def number(value: Any) -> int | float:
        return written_number(value, integral)

    section: dict[str, Any] = {'integral': integral}
    if spec.kind is ColumnKind.DECIMAL:
        section |= {'precision': spec.precision, 'scale': spec.scale}
    if has_trimmed_extremes(values, k_min):
        section['extremes'] = {'low': number(stat('trimmed_low')), 'high': number(stat('trimmed_high')), 'method': 'trimmed_k'}
    # A moment the engine returns as NaN, such as a constant column's skew, is written as null.
    for name in ('mean', 'std'):
        section[name] = significant(stat(name), MOMENT_DIGITS) if is_finite(stat(name)) else None
    for name in ('skew', 'kurtosis'):
        # Shape moments carry no units, and a parallel sum's order moves a zero one in its last bits from run to run.
        section[name] = significant(round(stat(name), SHAPE_MOMENT_DECIMALS), MOMENT_DIGITS) + 0.0 if is_finite(stat(name)) else None
    section['zero_rate'] = significant(stat('zero_rate'), RATE_DIGITS)
    section['negative_rate'] = significant(stat('negative_rate'), RATE_DIGITS)
    at_level = dict(zip(NUMERIC_LEVELS, stat('quantiles'), strict=True))
    quantiles = {name: number(at_level[level]) for name, level in QUANTILES.items() if clear_of_the_ends(level, values, k_min)}
    if quantiles:
        section['quantiles'] = quantiles
    if layout.binning:
        histogram = floored_histogram(layout.binning, stat('histogram_counts'), k_min)
        if histogram:
            section['histogram'] = histogram
    if layout.fit_applies:
        section['fit'] = layout.fit
    if integral and stat('step_min') is not None:
        step_min, step_max = stat('step_min'), stat('step_max')
        section['monotonic'] = 'increasing' if step_min > 0 else 'decreasing' if step_max < 0 else 'none'
        sequence = sequence_of(stat)
        if sequence:
            section['sequence'] = sequence
    return section


def histogram_binning(spec: ColumnSpec, stat: Stat, described: int, k_min: int, layout: FrozenLayout) -> Binning | None:
    """The edges a numeric column's rows are counted against: the earlier profile's, or its own equal-depth ones.

    The outer edges are the trimmed extremes, and each interior one is the value at a level `k_min` rows clear of
    both ends, so every edge is a value the profile could already store.
    """
    if spec.kind not in NUMERIC or described - stat('null_count') < k_min:
        return None
    if spec.name in layout.histograms:
        kind, edges = layout.histograms[spec.name]
        return Binning(kind, edges, frozen=True)
    values = described - stat('null_count')
    if not has_trimmed_extremes(values, k_min):
        return None
    integral = spec.kind is ColumnKind.INTEGER
    low, high = stat('trimmed_low'), stat('trimmed_high')
    at_level = dict(zip(NUMERIC_LEVELS, stat('quantiles'), strict=True))
    interior = [at_level[level] for level in EQUAL_DEPTH_LEVELS if clear_of_the_ends(level, values, k_min)]
    rising = sorted({written_number(value, integral) for value in (low, *(value for value in interior if low < value < high), high)})
    return Binning('equal_depth', tuple(rising), frozen=False) if len(rising) > 1 else None


def floored_histogram(binning: Binning, counted: list[int], k_min: int) -> dict[str, Any] | None:
    """The histogram with no bin or overflow count of 1 to k_min - 1 rows, or None where no such histogram exists.

    An overflow count that small joins the outermost bin on its side, and a bin that small merges into its smaller
    non-empty neighbor, so frozen edges are kept wherever the floor allows. An empty bin discloses no row and stays.
    """
    below, *bins, above = counted
    if 0 < below < k_min:
        bins[0], below = bins[0] + below, 0
    if 0 < above < k_min:
        bins[-1], above = bins[-1] + above, 0
    edges = list(binning.edges)
    while len(bins) > 1:
        small = [index for index, count in enumerate(bins) if 0 < count < k_min]
        if not small:
            break
        index = min(small, key=lambda at: (bins[at], at))
        neighbors = [at for at in (index - 1, index + 1) if 0 <= at < len(bins)]
        neighbor = min(neighbors, key=lambda at: (bins[at] == 0, bins[at], at))
        first = min(index, neighbor)
        bins[first : first + 2] = [bins[first] + bins[first + 1]]
        del edges[first + 1]
    if any(0 < count < k_min for count in bins):
        return None
    histogram: dict[str, Any] = {'kind': binning.kind, 'edges': edges, 'counts': bins}
    if binning.frozen:
        histogram['overflow'] = {'below': below, 'above': above}
    return histogram


def string_section(
    backend: Backend,
    table: Table,
    spec: ColumnSpec,
    stat: Stat,
    values: int,
    shapes: list[tuple[str, int]],
    pattern: str | None,
    listed: tuple[str, ...] | None,
    k_min: int,
) -> dict[str, Any]:
    """A string column's section. `listed` is an earlier profile's value list, which its top values are counted against."""
    length: dict[str, Any] = {}
    if has_trimmed_extremes(values, k_min):
        length |= {'min': int(stat('length_trimmed_low')), 'max': int(stat('length_trimmed_high'))}
    length['mean'] = significant(stat('length_mean'), MOMENT_DIGITS)
    at_level = dict(zip(LENGTH_LEVELS, stat('length_quantiles'), strict=True))
    quantiles = {name: int(at_level[level]) for name, level in QUANTILES.items() if clear_of_the_ends(level, values, k_min)}
    if quantiles:
        length['quantiles'] = quantiles
    section: dict[str, Any] = {'length': length, 'whitespace_edge_rate': significant(stat('whitespace_edge_rate'), RATE_DIGITS)}
    if shapes:
        section['shapes'] = [{'shape': shape, 'rate': rate(count, values)} for shape, count in shapes]
    if pattern:
        section['pattern'] = pattern
    if listed is not None:
        counts = stat('listed_value_counts')
        section |= top_values(list(zip(listed, counts, strict=True)), values, k_min)
        outside = values - sum(counts)
        section['overflow'] = {'new_values': outside if outside >= k_min else 0}
    elif stat('distinct_count') <= TOP_VALUES_THRESHOLD:
        section |= top_values(backend.top_k(table, spec.name, TOP_VALUES_THRESHOLD), values, k_min)
    return section


def top_values(counted: list[tuple[Any, int]], values: int, k_min: int) -> dict[str, Any]:
    """The values at least k_min rows hold, and the rest as `other_rate`, which is also never 1 to k_min - 1 rows."""
    kept = [(value, count) for value, count in counted if count >= k_min]
    other = values - sum(count for _, count in kept)
    while kept and 0 < other < k_min:
        # The least held value folds in, and the last of them where several tie.
        smallest = min(range(len(kept)), key=lambda at: (kept[at][1], -at))
        other += kept.pop(smallest)[1]
    if not kept:
        return {}
    return {
        'top_values': [{'value': str(value), 'rate': rate(count, values)} for value, count in kept],
        'other_rate': rate(other, values),
    }


def generalized_patterns(
    backend: Backend, table: Table, shapes: dict[str, list[tuple[str, int]]], stats: Answers, described: int, k_min: int
) -> dict[str, str]:
    """Each string column's pattern, generalized from sampled values of its dominant shape by grex."""
    dominant = {
        name: counted[0][0]
        for name, counted in shapes.items()
        if counted and counted[0][1] >= PATTERN_SHAPE_SHARE * (described - stats[(name, 'null_count')])
    }
    if not dominant:
        return {}
    sample = backend.sample(table, PATTERN_SAMPLE_ROWS, SAMPLE_SEED)
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
) -> dict[str, dict[str, Any] | None]:
    """Each fittable numeric column's fit, from one seeded sample, or None for each when scipy is not installed.

    A column is fittable when it has trimmed extremes, `FIT_MIN_DISTINCT` distinct values, and is no counting sequence.
    """
    fittable = []
    for spec in specs:
        stat = column_stat(stats, spec.name)
        if spec.kind not in NUMERIC or distinct[spec.name][0] < FIT_MIN_DISTINCT:
            continue
        if has_trimmed_extremes(described - stat('null_count'), k_min) and not (spec.kind is ColumnKind.INTEGER and sequence_of(stat)):
            fittable.append(spec)
    if not fittable:
        return {}
    if importlib.util.find_spec('scipy') is None:
        warnings.warn(FitsSkipped(), stacklevel=4)
        return {spec.name: None for spec in fittable}
    # Imported by name, because it imports scipy, which the library does not declare.
    fitting = importlib.import_module(FITTED_FAMILIES_MODULE)
    sample = backend.sample(table, FIT_SAMPLE_ROWS, SAMPLE_SEED)
    fits = {}
    for spec in fittable:
        stat = column_stat(stats, spec.name)
        fitted = fitting.best_fit(
            fitting.finite_values(sample.column(spec.name)),
            integral=spec.kind is ColumnKind.INTEGER,
            low=float(stat('trimmed_low')),
            high=float(stat('trimmed_high')),
            zero_rate=stat('zero_rate'),
        )
        fits[spec.name] = fit_entry(fitted) if fitted else None
    return fits


def fit_entry(fitted: Any) -> dict[str, Any]:
    """A fit as the profile writes it. A discrete family's locations are whole numbers, written exactly."""
    discrete = fitted.family in ('geom', 'randint')
    params = {
        name: int(value) if discrete and name != 'p' else significant(value, FIT_PARAMETER_DIGITS) + 0.0
        for name, value in fitted.params.items()
    }
    entry: dict[str, Any] = {
        'family': fitted.family,
        'params': params,
        'ks': significant(fitted.ks, RATE_DIGITS),
        'aic': significant(fitted.aic, MOMENT_DIGITS),
        'sample': fitted.rows,
    }
    if fitted.zero_inflation is not None:
        entry['zero_inflation'] = significant(fitted.zero_inflation, RATE_DIGITS)
    entry['poor'] = fitted.poor
    return entry


def temporal_section(spec: ColumnSpec, stat: Stat, values: int, text: TextColumn | None, k_min: int) -> dict[str, Any]:
    section: dict[str, Any] = {}
    if text and text.parsed.format:
        section['format'] = text.parsed.format
    if spec.timezone:
        section['timezone'] = spec.timezone
    if has_trimmed_extremes(values, k_min):
        section['extremes'] = {
            'low': stat('trimmed_low').isoformat(),
            'high': stat('trimmed_high').isoformat(),
            'granularity': granularity(spec, stat),
        }
    for field, counts in (('by_month', 'month_counts'), ('by_dow', 'weekday_counts'), ('by_hour', 'hour_counts')):
        if spec.kind is ColumnKind.DATETIME or counts != 'hour_counts':
            shares = floored_shares(stat(counts), k_min)
            if shares:
                section[field] = shares
    days = (day_of(stat('max')) - day_of(stat('min'))).days + 1
    section['gap_rate'] = significant(1 - stat('distinct_days') / days, RATE_DIGITS)
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


def clear_of_the_ends(level: float, values: int, k_min: int) -> bool:
    """Whether the quantile sits at least k_min rows from both ends, so it is not an extreme in disguise."""
    position = level * (values - 1)
    return position >= k_min - 1 and values - 1 - position >= k_min - 1


def rate(count: int, total: int) -> float:
    return significant(count / total, RATE_DIGITS) if total else 0.0


def significant(value: float, digits: int) -> float:
    return float(f'{value:.{digits}g}')


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
