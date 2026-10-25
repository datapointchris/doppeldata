"""One table's profile, computed through a backend in two passes. No literal it stores rests on fewer than k_min rows."""

import datetime as dt
import math
from collections.abc import Callable
from typing import Any

from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.comments import CommentedSeq

from doppeldata import _core
from doppeldata.profiling import text_types
from doppeldata.profiling.backend import TEXT_FORMATS
from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import Request
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import Table
from doppeldata.profiling.statistic_catalog import COLUMN_STATISTICS
from doppeldata.profiling.statistic_catalog import QUANTILES
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
TRIMMED_STATISTICS = frozenset({'trimmed_low', 'trimmed_high', 'length_trimmed_low', 'length_trimmed_high'})
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
# Each unit the values may all be whole numbers of, coarsest first, with the granularity it gives.
GRANULARITIES = (
    ('whole_days', 'day'),
    ('whole_hours', 'hour'),
    ('whole_minutes', 'minute'),
    ('whole_seconds', 'second'),
    ('whole_milliseconds', 'millisecond'),
    ('whole_microseconds', 'microsecond'),
)

Stat = Callable[[str], Any]


def profile_table(backend: Backend, source: Source, *, sample: int | None, k_min: int) -> dict[str, Any]:
    """The profile document of `source`, from every row or from a seeded sample of `sample` rows."""
    table = backend.scan(source)
    rows = backend.aggregate(table, [Request(None, 'rows')])[Request(None, 'rows')]
    if sample is not None and rows > sample:
        table = backend.scan(Source(format=None, table=backend.sample(table, sample, SAMPLE_SEED)))
    text: dict[str, TextColumn] = {}
    if source.format in TEXT_FORMATS:
        text = text_types.type_text_columns(backend, table, backend.schema(table), SAMPLE_SEED)
        table = backend.parse(table, {name: column.parsed for name, column in text.items()})
    specs = backend.schema(table)
    stats = backend.aggregate(table, pass_one_requests(specs, k_min))
    described = stats[Request(None, 'rows')]
    shapes = {
        spec.name: [(shape, count) for shape, count in backend.top_k(table, spec.name, SHAPES_KEPT, FrequencyKey.SHAPE) if count >= k_min]
        for spec in specs
        if spec.kind is ColumnKind.STRING
    }
    patterns = generalized_patterns(backend, table, shapes, stats, described, k_min)
    columns = [column_entry(backend, table, spec, stats, described, text.get(spec.name), shapes, patterns, k_min) for spec in specs]
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


def pass_one_requests(specs: list[ColumnSpec], k_min: int) -> list[Request]:
    """Every catalog statistic of every column it applies to, and the row count, for one aggregate."""
    requests = [Request(None, 'rows')]
    for spec in specs:
        for statistic in COLUMN_STATISTICS:
            if spec.kind in statistic.kinds:
                requests.append(Request(spec.name, statistic.name, k_min if statistic.name in TRIMMED_STATISTICS else None))
    return requests


def column_entry(
    backend: Backend,
    table: Table,
    spec: ColumnSpec,
    stats: dict[Request, Any],
    described: int,
    text: TextColumn | None,
    shapes: dict[str, list[tuple[str, int]]],
    patterns: dict[str, str],
    k_min: int,
) -> dict[str, Any]:
    def stat(name: str) -> Any:
        return stats[Request(spec.name, name, k_min if name in TRIMMED_STATISTICS else None)]

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
    if described:
        approx = stat('distinct_count')
        entry['distinct'] = {'approx': approx, 'ratio': significant(min(1.0, approx / described), RATE_DIGITS)}
    # A statistic of fewer than k_min values can give one of them away, as the mean of one value does.
    if values < k_min:
        return entry
    if spec.kind in (ColumnKind.INTEGER, ColumnKind.FLOAT, ColumnKind.DECIMAL):
        entry['numeric'] = numeric_section(spec, stat, values, k_min)
    elif spec.kind is ColumnKind.STRING:
        entry['string'] = string_section(backend, table, spec, stat, values, shapes[spec.name], patterns.get(spec.name), k_min)
    elif spec.kind in (ColumnKind.DATE, ColumnKind.DATETIME):
        entry['temporal'] = temporal_section(spec, stat, values, text, k_min)
    elif spec.kind is ColumnKind.BOOLEAN:
        entry['boolean'] = {'true_rate': significant(stat('true_rate'), RATE_DIGITS)}
    return entry


def numeric_section(spec: ColumnSpec, stat: Stat, values: int, k_min: int) -> dict[str, Any]:
    integral = spec.kind is ColumnKind.INTEGER

    def number(value: Any) -> int | float:
        return int(value) if integral else float(value)

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
    quantiles = {name: number(stat(name)) for name, level in QUANTILES.items() if clear_of_the_ends(level, values, k_min)}
    if quantiles:
        section['quantiles'] = quantiles
    if integral and stat('step_min') is not None:
        step_min, step_max = stat('step_min'), stat('step_max')
        section['monotonic'] = 'increasing' if step_min > 0 else 'decreasing' if step_max < 0 else 'none'
        if step_min == step_max != 0 and stat('null_count') == 0:
            section['sequence'] = {'start': int(stat('min')), 'step': int(step_min)}
    return section


def string_section(
    backend: Backend,
    table: Table,
    spec: ColumnSpec,
    stat: Stat,
    values: int,
    shapes: list[tuple[str, int]],
    pattern: str | None,
    k_min: int,
) -> dict[str, Any]:
    length: dict[str, Any] = {}
    if has_trimmed_extremes(values, k_min):
        length |= {'min': int(stat('length_trimmed_low')), 'max': int(stat('length_trimmed_high'))}
    length['mean'] = significant(stat('length_mean'), MOMENT_DIGITS)
    quantiles = {name: int(stat(f'length_{name}')) for name, level in QUANTILES.items() if clear_of_the_ends(level, values, k_min)}
    if quantiles:
        length['quantiles'] = quantiles
    section: dict[str, Any] = {'length': length, 'whitespace_edge_rate': significant(stat('whitespace_edge_rate'), RATE_DIGITS)}
    if shapes:
        section['shapes'] = [{'shape': shape, 'rate': rate(count, values)} for shape, count in shapes]
    if pattern:
        section['pattern'] = pattern
    if stat('distinct_count') <= TOP_VALUES_THRESHOLD:
        section |= top_values(backend.top_k(table, spec.name, TOP_VALUES_THRESHOLD), values, k_min)
    return section


def top_values(counted: list[tuple[Any, int]], values: int, k_min: int) -> dict[str, Any]:
    """The values at least k_min rows hold, and the rest as `other_rate`, which is also never 1 to k_min - 1 rows."""
    kept = [(value, count) for value, count in counted if count >= k_min]
    other = values - sum(count for _, count in kept)
    while kept and 0 < other < k_min:
        other += kept.pop()[1]
    if not kept:
        return {}
    return {
        'top_values': [{'value': str(value), 'rate': rate(count, values)} for value, count in kept],
        'other_rate': rate(other, values),
    }


def generalized_patterns(
    backend: Backend, table: Table, shapes: dict[str, list[tuple[str, int]]], stats: dict[Request, Any], described: int, k_min: int
) -> dict[str, str]:
    """Each string column's pattern, generalized from sampled values of its dominant shape by grex."""
    dominant = {
        name: counted[0][0]
        for name, counted in shapes.items()
        if counted and counted[0][1] >= PATTERN_SHAPE_SHARE * (described - stats[Request(name, 'null_count')])
    }
    if not dominant:
        return {}
    sample = backend.sample(table, PATTERN_SAMPLE_ROWS, SAMPLE_SEED)
    patterns = {}
    for name, shape in dominant.items():
        matching = [value for value in sample.column(name).to_pylist() if value is not None and shape_of(value) == shape]
        # Every literal grex keeps is shared by all the values it is given, so k_min of them must back it.
        if len(matching) >= k_min:
            pattern = _core.generalize_pattern(sorted(set(matching))[:PATTERN_VALUES])
            if pattern:
                patterns[name] = pattern
    return patterns


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
    return next((unit for whole, unit in GRANULARITIES if stat(whole)), 'nanosecond')


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
