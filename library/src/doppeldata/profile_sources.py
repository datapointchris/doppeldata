"""Every source a call that generates takes, and the profile each stands for.

A source is a Profile, a profile or short YAML file, an example's name, a list of column names, a dict of inline columns,
or a schema class of a library `seed_profiles.schema_classes` reads. The short forms are `short_profiles`' to expand, and
the schema classes `seed_profiles`', so this module sits above both.
"""

import copy
import os
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import Protocol

from doppeldata.inline_columns import InlineColumn
from doppeldata.profile_format import Profile
from doppeldata.seed_profiles.schema_classes import LIBRARIES
from doppeldata.seed_profiles.schema_classes import schema_class_profile
from doppeldata.short_profiles import DICT_SOURCE
from doppeldata.short_profiles import INLINE_TABLE
from doppeldata.short_profiles import LIST_SOURCE
from doppeldata.short_profiles import expanded
from doppeldata.short_profiles import file_profile
from doppeldata.short_profiles import validated
from doppeldata.short_profiles import with_row_count


class FieldsSchema(Protocol):
    """A schema listing its fields, as Spark's `StructType` does."""

    @property
    def fields(self) -> Sequence[Any]: ...


class ColumnsSchema(Protocol):
    """A schema mapping each column's name to its declaration, as pandera's `DataFrameSchema` does."""

    @property
    def columns(self) -> Mapping[Any, Any]: ...


class SchemaModel(Protocol):
    """A schema class that builds its schema, as pandera's `DataFrameModel` does."""

    @classmethod
    def to_schema(cls) -> Any: ...


class ColumnsModel(Protocol):
    """A schema class mapping each column's name to its declaration, as dataframely's `Schema` does."""

    @classmethod
    def columns(cls) -> Mapping[str, Any]: ...


# The schema libraries are named by their shapes, so none is imported to name them.
ProfileSource = (
    Profile
    | str
    | os.PathLike[str]
    | Sequence[str]
    | Mapping[str, InlineColumn | None]
    | type[SchemaModel]
    | type[ColumnsModel]
    | FieldsSchema
    | ColumnsSchema
)


def profile_from(source: ProfileSource) -> Profile:
    """The profile `source` stands for, as every call that generates reads it. A Profile is itself."""
    if isinstance(source, Profile):
        return source
    if isinstance(source, str | os.PathLike):
        return file_profile(source)
    seeded = schema_class_profile(source)
    if seeded is not None:
        return seeded
    if isinstance(source, Mapping):
        if not all(isinstance(name, str) for name in source):
            raise TypeError(f'the columns given are keyed by their names, as strs, not {list(source)}')
        return validated(expanded(dict(source), None, INLINE_TABLE, DICT_SOURCE), DICT_SOURCE)
    if isinstance(source, Sequence) and all(isinstance(name, str) for name in source):
        repeated = sorted({name for name in source if list(source).count(name) > 1})
        if repeated:
            raise ValueError(f'the column list names {", ".join(repeated)} more than once')
        return validated(expanded(dict.fromkeys(source), None, INLINE_TABLE, LIST_SOURCE), LIST_SOURCE)
    raise TypeError(
        'generation takes a Profile, a profile or short YAML file, an example name, a list of column names, a dict of '
        f'columns such as {{"email": sd.fakes.email}}, or a schema of {", ".join(LIBRARIES[:-1])} or {LIBRARIES[-1]}, '
        f'not {type(source).__name__}'
    )


def expanded_profile(source: ProfileSource, rows: int | None = None) -> Profile:
    """The profile `source` stands for, recording `rows` as its row count where given. `Profile.expand` documents `source`."""
    profile = profile_from(source)
    if rows is None:
        return profile
    if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
        raise ValueError(f'rows must be zero or more, not {rows!r}')
    return Profile(with_row_count(copy.deepcopy(profile.document), rows))
