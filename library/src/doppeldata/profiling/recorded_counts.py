"""How a count of rows is written into a profile: as a rate to a few significant digits, and only where k_min rows hold it."""

from typing import Any

from doppeldata.profile_format import LARGEST_EXACT_INTEGER

# Deequ's DEFAULT_CARDINALITY_THRESHOLD: a column with no more distinct values than this has its values counted.
TOP_VALUES_THRESHOLD = 120
# A numeric code, such as a location id, holds a few hundred values, and a money or distance column
# thousands. A code drawn from its quantiles lands on the wrong codes, so its values are counted.
NUMERIC_TOP_VALUES_THRESHOLD = 1_000
RATE_DIGITS = 4


def kept_values(counted: list[tuple[Any, int]], values: int, k_min: int) -> tuple[list[tuple[Any, int]], int]:
    """The values at least k_min rows hold, and the rows of the rest, which are never 1 to k_min - 1. A rest of that
    many rows is counted with the most held value, the first of them where several tie, so every count stays whole over
    `values`."""
    kept = [(value, count) for value, count in counted if count >= k_min]
    other = values - sum(count for _, count in kept)
    if kept and 0 < other < k_min:
        modal = max(range(len(kept)), key=lambda at: (kept[at][1], -at))
        kept[modal] = (kept[modal][0], kept[modal][1] + other)
        other = 0
    return kept, other


def rate(count: int, total: int) -> float:
    """`count` of `total` to `RATE_DIGITS` significant digits, where 1 means every row, so a share short of every row is
    written below it."""
    if not total:
        return 0.0
    written = significant(count / total, RATE_DIGITS)
    return 1 - 10.0**-RATE_DIGITS if written >= 1 and count < total else written


def significant(value: float, digits: int) -> float:
    return float(f'{value:.{digits}g}')


def clear_of_the_ends(level: float, values: int, k_min: int) -> bool:
    """Whether the quantile sits at least k_min rows from both ends, so it is not an extreme in disguise."""
    position = level * (values - 1)
    return position >= k_min - 1 and values - 1 - position >= k_min - 1


def rows_to_the_nearer_end(level: float, values: int) -> int:
    """The rows between a quantile and the nearer end of `values`, counting its own."""
    return round(min(level * (values - 1), (1 - level) * (values - 1)) + 1)


def written_number(value: Any, integral: bool) -> int | float:
    """A data value as the profile writes it. An integer canonical JSON cannot write exactly is written as the nearest double."""
    return int(value) if integral and abs(value) <= LARGEST_EXACT_INTEGER else float(value)
