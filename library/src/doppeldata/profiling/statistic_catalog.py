"""The statistics a profile records, as data: each one's name and the column kinds it applies to.

Every backend translates each name once, so two backends compute one field set. Nothing here
imports an engine.
"""

import dataclasses as dc
import enum
import string

from doppeldata import _core
from doppeldata.profile_format import quantile_level


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


# Each unit a timestamp column's values may all be whole numbers of, coarsest first: its name, its
# length in nanoseconds, and the `granularity` it gives.
TIME_UNITS = (
    ('days', 86_400 * 10**9, 'day'),
    ('hours', 3_600 * 10**9, 'hour'),
    ('minutes', 60 * 10**9, 'minute'),
    ('seconds', 10**9, 'second'),
    ('milliseconds', 10**6, 'millisecond'),
    ('microseconds', 10**3, 'microsecond'),
)


# The quantiles a profile records, by name, and the levels the format reads those names as. The
# outermost pair are knots the generator's inverse CDF needs for its tails.
QUANTILES = {name: quantile_level(name) for name in ('p001', 'p01', 'p05', 'p25', 'p50', 'p75', 'p95', 'p99', 'p999')}

# A request for `trimmed_low` or `trimmed_high` carries k_min as its argument: the value at rank
# k_min from that end. An exact minimum or maximum is one row's value, so only these are stored.
# `distinct_days` carries k_min too, and counts the days holding a value between those two.
# `quantiles` and `length_quantiles` carry a tuple of levels, and answer with the nearest-rank value
# at each, in order. `histogram_counts` carries a tuple of rising edges, and answers with the rows
# below the first, in each bin from one edge up to the next with the last bin closed, and above the
# last. `listed_value_counts` carries a tuple of values, and answers with the rows holding each.
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
    Statistic('quantiles', NUMERIC),
    # The least and greatest difference between neighboring rows, which find a counting sequence.
    Statistic('step_min', frozenset({ColumnKind.INTEGER})),
    Statistic('step_max', frozenset({ColumnKind.INTEGER})),
    Statistic('length_mean', frozenset({ColumnKind.STRING})),
    Statistic('length_trimmed_low', frozenset({ColumnKind.STRING})),
    Statistic('length_trimmed_high', frozenset({ColumnKind.STRING})),
    Statistic('length_quantiles', frozenset({ColumnKind.STRING})),
    Statistic('whitespace_edge_rate', frozenset({ColumnKind.STRING})),
    Statistic('true_rate', frozenset({ColumnKind.BOOLEAN})),
    # Rows in each month from January, each weekday from Monday, each hour from 00, as one list.
    Statistic('month_counts', TEMPORAL),
    Statistic('weekday_counts', TEMPORAL),
    Statistic('hour_counts', frozenset({ColumnKind.DATETIME})),
    Statistic('distinct_days', TEMPORAL),
    # Whether every value is a whole number of the unit in local time, which sets the granularity.
    *(Statistic(f'whole_{unit}', frozenset({ColumnKind.DATETIME})) for unit, _, _ in TIME_UNITS),
)

# Counted against the edges and values the first aggregate's answers, or an earlier profile, choose.
COUNTING_STATISTICS = (
    Statistic('histogram_counts', NUMERIC),
    Statistic('listed_value_counts', frozenset({ColumnKind.STRING})),
)

# `unparsed_rows` carries each column's name and `ParsedType`, and answers with the rows holding a
# non-null value that does not parse as its column's type, which a typed reader rejects as `cast`.
TABLE_STATISTICS = (Statistic('rows', frozenset()), Statistic('unparsed_rows', frozenset()))

# The types a text source's raw value may parse as, in the order a value is tried and a tie between
# two types' shares is broken. A value parsing as none of them is a string.
RAW_TYPES = ('integer', 'decimal', 'boolean', 'date', 'timestamp')
READ_AS = (*RAW_TYPES, 'string')
# A column read as the key reads the listed types' values too, so they count as the key's.
RAW_TYPE_FOLDS = {'decimal': ('integer',)}

# A text column's non-null values counted by the first raw type each parses as, in `READ_AS` order,
# so the counts are disjoint. The argument pairs each raw type in `RAW_TYPE_LAYOUTS` with the
# strftime layout it is read through, and a type it leaves out is not tried.
TEXT_TYPE_STATISTICS = (
    Statistic('raw_type_counts', frozenset({ColumnKind.STRING})),
    # Digit runs with a leading zero, such as ZIP codes. Padding means the digits are a code, so a
    # column holding one is read as text whatever its other values parse as.
    Statistic('padded_digits', frozenset({ColumnKind.STRING})),
    # A JSON path's values counted as `raw_type_counts` counts them, except that a number or a
    # boolean is its JSON type and a JSON string is a date, a timestamp or a string. The argument
    # pairs the layouts with the column holding each value's JSON type.
    Statistic('json_raw_type_counts', frozenset({ColumnKind.STRING})),
)

ALL_STATISTICS = (*COLUMN_STATISTICS, *COUNTING_STATISTICS, *TABLE_STATISTICS, *TEXT_TYPE_STATISTICS)

# The raw-value patterns behind the raw type counts, in syntax both Rust's regex and Java's accept.
# A number with a leading zero, such as a ZIP code, matches neither, so it stays a string.
DIGITS_TEXT = r'^[+-]?[0-9]+$'
PADDED_DIGITS_TEXT = r'^[+-]?0[0-9]+$'
INTEGER_TEXT = r'^[+-]?(0|[1-9][0-9]*)$'
DECIMAL_TEXT = r'^[+-]?((0|[1-9][0-9]*)(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
BOOLEAN_TEXT = r'(?i)^(true|false)$'

# The date and timestamp layouts a text column is tried against, as strftime patterns, drawn from
# qsv's date parser. Month-first comes before day-first, so it wins where both read every value.
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
RAW_TYPE_LAYOUTS = {'date': DATE_FORMATS, 'timestamp': TIMESTAMP_FORMATS}


class FrequencyKey(enum.Enum):
    """What `top_k` counts: each value as written, each value's shape, or each value's first three characters."""

    VALUE = 'value'
    SHAPE = 'shape'
    ZIP_PREFIX = 'zip_prefix'


# The characters a shape classes, and the class the engine's `shape_of` gives each; every other
# character is kept. The table lets a backend shape a whole column in one replace.
SHAPE_FROM = string.ascii_uppercase + string.ascii_lowercase + string.digits
SHAPE_TO = _core.shape_of(SHAPE_FROM)
shape_of = _core.shape_of
