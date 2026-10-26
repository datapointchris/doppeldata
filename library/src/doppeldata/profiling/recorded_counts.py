"""How a count of rows is written into a profile: as a rate to a few significant digits, and only where k_min rows hold it."""

from typing import Any

# Deequ's DEFAULT_CARDINALITY_THRESHOLD: a column with no more distinct values than this has its values counted.
TOP_VALUES_THRESHOLD = 120
# A numeric code, such as a location id, holds a few hundred values, and a money or distance column
# thousands. A code drawn from its quantiles lands on the wrong codes, so its values are counted.
NUMERIC_TOP_VALUES_THRESHOLD = 1_000
RATE_DIGITS = 4


def kept_values(counted: list[tuple[Any, int]], values: int, k_min: int) -> tuple[list[tuple[Any, int]], int]:
    """The values at least k_min rows hold, and the rows of the rest, which are also never 1 to k_min - 1."""
    kept = [(value, count) for value, count in counted if count >= k_min]
    other = values - sum(count for _, count in kept)
    while kept and 0 < other < k_min:
        # The least held value folds in, and the last of them where several tie.
        smallest = min(range(len(kept)), key=lambda at: (kept[at][1], -at))
        other += kept.pop(smallest)[1]
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
