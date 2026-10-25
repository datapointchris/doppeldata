"""How a count of rows is written into a profile: as a rate to a few significant digits, and only where k_min rows hold it."""

from typing import Any

# Deequ's DEFAULT_CARDINALITY_THRESHOLD: a column with no more distinct values than this has its values counted.
TOP_VALUES_THRESHOLD = 120
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
    return significant(count / total, RATE_DIGITS) if total else 0.0


def significant(value: float, digits: int) -> float:
    return float(f'{value:.{digits}g}')
