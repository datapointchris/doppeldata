"""The values `sample` pins columns to, each tagged with its Python kind for the engine, which decides what each column takes.

A value is one for every row. Any other iterable but a str or bytes holds one value per row. The engine casts each to its
column's type, and refuses a kind the column does not take or a value it cannot hold.
"""

import datetime as dt
import decimal
import json
import numbers
from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any

Overrides = Mapping[str, Any]


def given_value(column: str, value: object) -> dict[str, str] | None:
    """`value` as the engine reads it: its kind and its text, or None for a null."""
    if value is None:
        return None
    if isinstance(value, bool):
        return {'kind': 'boolean', 'text': 'true' if value else 'false'}
    if isinstance(value, numbers.Integral):
        return {'kind': 'integer', 'text': str(int(value))}
    if isinstance(value, decimal.Decimal):
        return {'kind': 'decimal', 'text': format(value, 'f')}
    if isinstance(value, numbers.Real):
        return {'kind': 'float', 'text': repr(float(value))}
    if isinstance(value, str):
        return {'kind': 'string', 'text': value}
    # A datetime is a date, so it is tested first.
    if isinstance(value, dt.datetime):
        return {'kind': 'timestamp', 'text': value.isoformat()}
    if isinstance(value, dt.date):
        return {'kind': 'date', 'text': value.isoformat()}
    if isinstance(value, dt.time):
        return {'kind': 'time', 'text': value.isoformat()}
    raise TypeError(
        f'the override of column {column!r} holds a {type(value).__name__}, which no column holds; give a str, int, float, '
        'Decimal, bool, date, datetime, time or None'
    )


def per_row(values: object) -> bool:
    """Whether an override's `values` hold one value per row: any iterable but a str or bytes."""
    return isinstance(values, Iterable) and not isinstance(values, str | bytes)


def materialized(overrides: Overrides | None) -> Overrides | None:
    """`overrides` with each per-row iterable read into a list, so a generator can be read twice."""
    if not isinstance(overrides, Mapping):
        return overrides
    return {column: list(values) if per_row(values) else values for column, values in overrides.items()}


def overrides_json(overrides: Overrides | None) -> str | None:
    """The engine's JSON for `overrides`, sorted by column, or None where nothing is pinned."""
    if overrides is None:
        return None
    if not isinstance(overrides, Mapping):
        raise TypeError(f"overrides maps each column's name to its value or values, not {type(overrides).__name__}")
    pinned: list[dict[str, Any]] = []
    for column, values in sorted(overrides.items()):
        if not isinstance(column, str):
            raise TypeError(f'overrides are keyed by column names, as strs, not {column!r}')
        if per_row(values):
            pinned.append({'column': column, 'values': {'each_row': [given_value(column, value) for value in values]}})
        else:
            pinned.append({'column': column, 'values': {'every': given_value(column, values)}})
    return json.dumps(pinned, allow_nan=False) if pinned else None
