"""Columns declared in Python, each standing for one profile column: a fake, a range of integers, decimals or dates, or a choice.

Each takes `nulls=`, the share of its rows that are null. `Profile.expand` writes the profile column each stands for, to
shape further in YAML once a keyword no longer reaches.
"""

import dataclasses as dc
import datetime as dt
import math
from collections.abc import Sequence
from typing import Any

# The decimal places a choice's rates are written to, which keeps their sum within the profile's RATE_SUM_TOLERANCE of 1.
RATE_PLACES = 6


@dc.dataclass(frozen=True)
class InlineColumn:
    """One column declared in Python. `nulls` is the share of its rows that are null."""

    nulls: float = dc.field(default=0.0, kw_only=True)

    def __post_init__(self) -> None:
        if not 0.0 <= self.nulls <= 1.0:
            raise ValueError(f'nulls is a share of rows, from 0 to 1, not {self.nulls}')

    def column(self, name: str) -> dict[str, Any]:
        """The profile column this stands for, named `name`."""
        column = {'name': name, **self.described()}
        if self.nulls:
            column['null_fraction'] = self.nulls
        return column

    def described(self) -> dict[str, Any]:
        raise NotImplementedError


@dc.dataclass(frozen=True)
class Fake(InlineColumn):
    """Text from one of the engine's providers, such as `sd.fakes.email`. Calling it sets a keyword: `sd.fakes.email(nulls=0.1)`."""

    provider: str

    def __call__(self, *, nulls: float = 0.0) -> 'Fake':
        return dc.replace(self, nulls=nulls)

    def __repr__(self) -> str:
        return f'sd.fakes.{self.provider}' + (f'(nulls={self.nulls})' if self.nulls else '')

    def described(self) -> dict[str, Any]:
        return {'physical_type': 'string', 'logical_type': 'string', 'semantic': {'generator_hint': {'provider': self.provider}}}


@dc.dataclass(frozen=True)
class Integers(InlineColumn):
    """Integers from `low` to `high`, both included, each equally likely."""

    low: int
    high: int

    def __post_init__(self) -> None:
        super().__post_init__()
        if not all(isinstance(bound, int) and not isinstance(bound, bool) for bound in (self.low, self.high)):
            raise TypeError(f'integers takes two ints, not {self.low!r} and {self.high!r}; decimals takes other numbers')
        if self.low > self.high:
            raise ValueError(f'integers runs from low to high, and {self.low} is above {self.high}')

    def described(self) -> dict[str, Any]:
        # One bin half an integer beyond each end, as the profiler bins an integer column, so the ends draw as often as the rest.
        histogram = {'kind': 'equal_width', 'edges': [self.low - 0.5, self.high + 0.5], 'bin_rows': [1]}
        numeric = {'extremes': {'low': self.low, 'high': self.high}, 'histogram': histogram}
        return {'physical_type': 'int64', 'logical_type': 'integer', 'numeric': numeric}


@dc.dataclass(frozen=True)
class Decimals(InlineColumn):
    """Numbers spread evenly from `low` to `high`, rounded to `places` decimal places where it is given."""

    low: float
    high: float
    places: int | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if not all(
            isinstance(bound, int | float) and not isinstance(bound, bool) and math.isfinite(bound) for bound in (self.low, self.high)
        ):
            raise TypeError(f'decimals takes two finite numbers, not {self.low!r} and {self.high!r}')
        if self.low > self.high:
            raise ValueError(f'decimals runs from low to high, and {self.low} is above {self.high}')
        if self.places is not None and (isinstance(self.places, bool) or not isinstance(self.places, int) or self.places < 0):
            raise ValueError(f'places is a count of decimal places, 0 or more, not {self.places!r}')

    def described(self) -> dict[str, Any]:
        numeric: dict[str, Any] = {'extremes': {'low': self.low, 'high': self.high}}
        if self.places is not None:
            numeric['scale'] = self.places
        return {'physical_type': 'double', 'logical_type': 'number', 'numeric': numeric}


@dc.dataclass(frozen=True)
class Dates(InlineColumn):
    """Days from `start` to `end`, both included, each equally likely."""

    start: dt.date
    end: dt.date

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.start > self.end:
            raise ValueError(f'dates runs from start to end, and {self.start} is after {self.end}')

    def described(self) -> dict[str, Any]:
        extremes = {'low': self.start.isoformat(), 'high': self.end.isoformat(), 'granularity': 'day'}
        return {'physical_type': 'date32[day]', 'logical_type': 'date', 'temporal': {'extremes': extremes}}


@dc.dataclass(frozen=True)
class Choice(InlineColumn):
    """One of `values` in each row, by `weights` where they are given, else each equally often."""

    values: tuple[Any, ...]
    weights: tuple[float, ...] | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.values:
            raise ValueError('choice takes at least one value')
        if len(set(self.values)) != len(self.values):
            raise ValueError(f'choice takes each value once, and {list(self.values)} repeats one')
        kinds = {kind_of(value) for value in self.values}
        if len(kinds) != 1 or None in kinds:
            raise TypeError(f'choice takes values of one kind, all str, all int, all float or all bool, not {list(self.values)}')
        if self.weights is not None:
            if len(self.weights) != len(self.values):
                raise ValueError(f'choice takes a weight for each of its {len(self.values)} values, not {len(self.weights)}')
            if any(not math.isfinite(weight) or weight < 0 for weight in self.weights) or not sum(self.weights):
                raise ValueError(f'choice takes weights of 0 or more, with one above 0, not {list(self.weights)}')

    def rates(self) -> list[float]:
        weights = self.weights or (1.0,) * len(self.values)
        total = sum(weights)
        return [round(weight / total, RATE_PLACES) for weight in weights]

    def described(self) -> dict[str, Any]:
        kind = kind_of(self.values[0])
        listed = [{'value': value, 'fraction': rate} for value, rate in zip(self.values, self.rates(), strict=True)]
        if kind is bool:
            true_fraction = sum(rate for value, rate in zip(self.values, self.rates(), strict=True) if value)
            return {'physical_type': 'bool', 'logical_type': 'boolean', 'boolean': {'true_fraction': true_fraction}}
        if kind is str:
            return {'physical_type': 'string', 'logical_type': 'string', 'string': {'top_values': listed, 'other_fraction': 0.0}}
        extremes = {'low': min(self.values), 'high': max(self.values)}
        numeric = {'top_values': listed, 'other_fraction': 0.0, 'extremes': extremes}
        if kind is int:
            return {'physical_type': 'int64', 'logical_type': 'integer', 'numeric': numeric}
        return {'physical_type': 'double', 'logical_type': 'number', 'numeric': numeric}


def kind_of(value: Any) -> type | None:
    """The kind of value a choice lists, or None for one no profile can list."""
    for kind in (bool, int, float, str):
        if isinstance(value, kind):
            return kind
    return None


def as_date(value: dt.date | str, argument: str) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f'{argument} is a date or an ISO date such as "2025-01-31", not {value!r}') from error


def integers(low: int, high: int, *, nulls: float = 0.0) -> Integers:
    """A column of integers from `low` to `high`, both included, each equally likely: `sd.integers(18, 90)`."""
    return Integers(low, high, nulls=nulls)


def decimals(low: float, high: float, *, places: int | None = None, nulls: float = 0.0) -> Decimals:
    """A column of numbers spread evenly from `low` to `high`, rounded to `places` where it is given: `sd.decimals(0, 500, places=2)`."""
    return Decimals(low, high, places, nulls=nulls)


def dates(start: dt.date | str, end: dt.date | str, *, nulls: float = 0.0) -> Dates:
    """A column of days from `start` to `end`, both included, each a date or an ISO date: `sd.dates('2023-01-01', '2025-12-31')`."""
    return Dates(as_date(start, 'start'), as_date(end, 'end'), nulls=nulls)


def choice(values: Sequence[Any], *, weights: Sequence[float] | None = None, nulls: float = 0.0) -> Choice:
    """A column holding one of `values` in each row, by `weights` where given: `sd.choice(['free', 'pro'], weights=[0.8, 0.2])`."""
    return Choice(tuple(values), tuple(weights) if weights is not None else None, nulls=nulls)
