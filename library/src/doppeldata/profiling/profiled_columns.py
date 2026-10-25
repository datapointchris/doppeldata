"""A table's columns once profiled, which the column profiler builds and the source sections read."""

import dataclasses as dc
from typing import Any

from doppeldata.profiling import json_paths
from doppeldata.profiling import text_types
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.backend import Table

# Each answer by its column, None for the table, and its statistic's name.
Answers = dict[tuple[str | None, str], Any]


@dc.dataclass(frozen=True)
class ProfiledColumns:
    """One table's columns profiled. `table` is parsed, `described` counts the rows read, and `unparsed_rows` the rows
    holding a value that does not parse as its column's type. `null_token` is what a delimited source writes most for a
    missing value, and a JSON table's paths and type counts are in `json`.
    """

    columns: list[dict[str, Any]]
    table: Table
    specs: list[ColumnSpec]
    stats: Answers
    described: int
    unparsed_rows: int = 0
    null_token: str = text_types.EMPTY
    json: json_paths.ProfiledTable | None = None
