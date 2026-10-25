"""The statistics a profile records, as data: each one's name and the column kinds it applies to.

Every backend translates each name once, so the Polars and Spark backends compute one field set.
Nothing here imports an engine.
"""

import dataclasses as dc
import enum
import string


class ColumnKind(enum.Enum):
    INTEGER = 'integer'
    FLOAT = 'float'
    DECIMAL = 'decimal'
    STRING = 'string'
    BOOLEAN = 'boolean'
    DATE = 'date'
    DATETIME = 'datetime'
    # Profiled by name, type and null rate only: lists, structs, times of day, durations, binary.
    OTHER = 'other'


NUMERIC = frozenset({ColumnKind.INTEGER, ColumnKind.FLOAT, ColumnKind.DECIMAL})
TEMPORAL = frozenset({ColumnKind.DATE, ColumnKind.DATETIME})
PROFILED = frozenset(ColumnKind) - {ColumnKind.OTHER}


@dc.dataclass(frozen=True)
class Statistic:
    name: str
    kinds: frozenset[ColumnKind]


# The profile's quantile names and the levels they stand for. A name's digits after `p` are the
# level's decimal digits. The outermost pair are knots the generator's inverse CDF needs for its tails.
QUANTILES = {
    'p001': 0.001,
    'p01': 0.01,
    'p05': 0.05,
    'p25': 0.25,
    'p50': 0.5,
    'p75': 0.75,
    'p95': 0.95,
    'p99': 0.99,
    'p999': 0.999,
}

# A request for `trimmed_low` or `trimmed_high` carries k_min as its argument: the value at rank
# k_min from that end. An exact minimum or maximum is one row's value, so only these are stored.
COLUMN_STATISTICS = (
    Statistic('null_count', frozenset(ColumnKind)),
    # Non-null distinct values, exact or estimated as the engine affords, written as `distinct.approx`.
    Statistic('distinct_count', PROFILED),
    Statistic('min', NUMERIC | TEMPORAL),
    Statistic('max', NUMERIC | TEMPORAL),
    Statistic('trimmed_low', NUMERIC | TEMPORAL),
    Statistic('trimmed_high', NUMERIC | TEMPORAL),
    Statistic('mean', NUMERIC),
    Statistic('std', NUMERIC),
    Statistic('skew', NUMERIC),
    Statistic('kurtosis', NUMERIC),
    Statistic('zero_rate', NUMERIC),
    Statistic('negative_rate', NUMERIC),
    *(Statistic(name, NUMERIC) for name in QUANTILES),
    # The least and greatest difference between neighboring rows, which find a counting sequence.
    Statistic('step_min', frozenset({ColumnKind.INTEGER})),
    Statistic('step_max', frozenset({ColumnKind.INTEGER})),
    Statistic('length_mean', frozenset({ColumnKind.STRING})),
    Statistic('length_trimmed_low', frozenset({ColumnKind.STRING})),
    Statistic('length_trimmed_high', frozenset({ColumnKind.STRING})),
    *(Statistic(f'length_{name}', frozenset({ColumnKind.STRING})) for name in QUANTILES),
    Statistic('whitespace_edge_rate', frozenset({ColumnKind.STRING})),
    Statistic('true_rate', frozenset({ColumnKind.BOOLEAN})),
    # Rows in each month from January, each weekday from Monday, each hour from 00, as one list.
    Statistic('month_counts', TEMPORAL),
    Statistic('weekday_counts', TEMPORAL),
    Statistic('hour_counts', frozenset({ColumnKind.DATETIME})),
    Statistic('distinct_days', TEMPORAL),
    # Whether every value is a whole number of the unit in local time, which sets the granularity.
    *(
        Statistic(f'whole_{unit}', frozenset({ColumnKind.DATETIME}))
        for unit in ('microseconds', 'milliseconds', 'seconds', 'minutes', 'hours', 'days')
    ),
)

TABLE_STATISTICS = (Statistic('rows', frozenset()),)

# Counts of a text source's raw values by the type they parse as, each excluding the types before
# it, so the counts are disjoint. `raw_date` and `raw_timestamp` take a strftime format argument.
TEXT_TYPE_STATISTICS = (
    Statistic('raw_integer', frozenset({ColumnKind.STRING})),
    Statistic('raw_decimal', frozenset({ColumnKind.STRING})),
    Statistic('raw_boolean', frozenset({ColumnKind.STRING})),
    Statistic('raw_date', frozenset({ColumnKind.STRING})),
    Statistic('raw_timestamp', frozenset({ColumnKind.STRING})),
)

ALL_STATISTICS = (*COLUMN_STATISTICS, *TABLE_STATISTICS, *TEXT_TYPE_STATISTICS)

# The raw-value patterns behind the `raw_*` counts, in syntax both Rust's regex and Java's accept.
INTEGER_TEXT = r'^[+-]?[0-9]+$'
DECIMAL_TEXT = r'^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?$'
BOOLEAN_TEXT = r'(?i)^(true|false)$'

# The date and timestamp layouts a text column is tried against, as strftime patterns. They are the
# layouts qsv's date parser recognizes, month-first before day-first where both read a value.
DATE_FORMATS = (
    '%Y-%m-%d',
    '%Y/%m/%d',
    '%Y%m%d',
    '%m/%d/%Y',
    '%d/%m/%Y',
    '%m-%d-%Y',
    '%d-%m-%Y',
    '%d.%m.%Y',
    '%m/%d/%y',
    '%d %b %Y',
    '%d %B %Y',
    '%b %d, %Y',
    '%B %d, %Y',
)
TIMESTAMP_FORMATS = (
    '%Y-%m-%dT%H:%M:%S%.f',
    '%Y-%m-%dT%H:%M:%S%.f%#z',
    '%Y-%m-%d %H:%M:%S%.f',
    '%Y-%m-%d %H:%M:%S%.f%#z',
    '%Y-%m-%d %H:%M',
    '%Y/%m/%d %H:%M:%S',
    '%m/%d/%Y %H:%M:%S',
    '%m/%d/%Y %H:%M',
    '%m/%d/%Y %I:%M:%S %p',
    '%d/%m/%Y %H:%M:%S',
    '%a, %d %b %Y %H:%M:%S %z',
    '%b %d, %Y %H:%M:%S',
)


class FrequencyKey(enum.Enum):
    """What `top_k` counts: each value as written, or each value's shape."""

    VALUE = 'value'
    SHAPE = 'shape'


# A value's shape maps each ASCII letter to `A` or `a` and each digit to `9`, and keeps the rest.
SHAPE_FROM = string.ascii_uppercase + string.ascii_lowercase + string.digits
SHAPE_TO = 'A' * 26 + 'a' * 26 + '9' * 10
SHAPE_TRANSLATION = str.maketrans(SHAPE_FROM, SHAPE_TO)


def shape_of(value: str) -> str:
    return value.translate(SHAPE_TRANSLATION)
