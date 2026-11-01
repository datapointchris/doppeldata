"""Every source a call that generates takes, and the profile each stands for.

A source is a Profile, a profile or short YAML file, an example's name, a list of column names, a dict of inline columns,
or a schema class of a library `seed_profiles.schema_classes` reads. The short forms are `short_profiles`' to expand, and
the schema classes `seed_profiles`', so this module sits above both.
"""

import contextlib
import copy
import dataclasses as dc
import os
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from typing import Protocol

from doppeldata import issue_codes
from doppeldata.errors import DoppeldataError
from doppeldata.errors import InvalidArgument
from doppeldata.errors import UnsupportedArgument
from doppeldata.inline_columns import InlineColumn
from doppeldata.issue_codes import Issue
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import Profile
from doppeldata.profile_format import read_document
from doppeldata.seed_profiles.schema_classes import LIBRARIES
from doppeldata.seed_profiles.schema_classes import schema_class_profile
from doppeldata.short_profiles import COMMAND_LINE_REMEDY
from doppeldata.short_profiles import DICT_SOURCE
from doppeldata.short_profiles import INLINE_TABLE
from doppeldata.short_profiles import LIST_SOURCE
from doppeldata.short_profiles import bundle_named
from doppeldata.short_profiles import expanded
from doppeldata.short_profiles import file_profile
from doppeldata.short_profiles import is_bundle_document
from doppeldata.short_profiles import named_column
from doppeldata.short_profiles import no_fake_message
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

DOCUMENT_SUFFIXES = ('.yaml', '.yml', '.json')


def bundle_from(source: Any) -> Bundle | None:
    """The bundle `source` names: a Bundle, a bundle file, a directory holding `bundle.yaml`, or an example bundle's name.
    None for any other source, which names one table."""
    if isinstance(source, Bundle):
        return source
    if not isinstance(source, str | os.PathLike):
        return None
    bundled = bundle_named(source)
    if isinstance(bundled, Path):
        return Bundle.load(bundled)
    if bundled is not None:
        return Bundle.example(bundled)
    path = Path(source)
    if path.is_file() and path.suffix.lower() in DOCUMENT_SUFFIXES and is_bundle_document(read_document(path)):
        return Bundle.load(path)
    return None


@contextlib.contextmanager
def read_as(field: str, *, relabeled: tuple[str, ...] = ('profile',)) -> Iterator[None]:
    """Name `field` on each issue of a refusal inside the block that names no place, or names one of `relabeled`, by
    default `profile`, the argument a profile is read from. A file that cannot be read is refused as FILE_UNREADABLE at
    `field`. So a caller reading two profiles learns which of its arguments a refusal is about."""
    try:
        yield
    except DoppeldataError as error:
        error.issues = tuple(dc.replace(issue, field=field) if issue.field in (None, *relabeled) else issue for issue in error.issues)
        raise
    except OSError as error:
        named = os.fspath(error.filename) if error.filename is not None else None
        message = f'{named} could not be read: {error.strerror or error}'
        raise InvalidArgument(issue_codes.FILE_UNREADABLE, message, field=field, value=named) from error


def profile_or_bundle(source: Profile | Bundle | str | os.PathLike[str], field: str = 'profile') -> Profile | Bundle:
    """The profile or bundle `source` names: itself, a file or directory holding one, a data file's embedded profile, or an
    example's name. A refusal names `field`."""
    if isinstance(source, Profile | Bundle):
        return source
    with read_as(field):
        bundle = bundle_from(source)
        return bundle if bundle is not None else file_profile(source)


def profile_or_columns(profile: str | os.PathLike[str] | None, columns: Sequence[str] | None) -> str | os.PathLike[str] | Profile | Bundle:
    """What a caller naming a profile, or the column names to expand into one, generates from. A `profile` naming a bundle
    gives the Bundle, and any other is returned as given, so a data file's write is read where it is generated. Else each
    column is drawn by the fake its name hints. Both or neither is refused at `profile`, and each column name no fake draws
    at `columns`."""
    if (profile is None) == (columns is None):
        given = 'both were given' if profile is not None else 'neither was given'
        message = f'name a profile or the columns to expand into one, and {given}'
        raise InvalidArgument(issue_codes.PROFILE_OR_COLUMNS, message, field='profile')
    if profile is not None:
        with read_as('profile'):
            return bundle_from(profile) or profile
    names = list(columns or ())
    issues = [
        Issue(issue_codes.NO_FAKE_FOR_NAME, 'columns', message, name, (), closest)
        for name in names
        if named_column(name) is None
        for message, closest in [no_fake_message(name, COMMAND_LINE_REMEDY)]
    ]
    if issues:
        raise InvalidProfile(LIST_SOURCE, issues)
    with read_as('columns'):
        return profile_from(names)


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
            message = f'the columns given are keyed by their names, as strs, not {list(source)}'
            raise UnsupportedArgument(issue_codes.INVALID_COLUMNS, message)
        return validated(expanded(dict(source), None, INLINE_TABLE, DICT_SOURCE), DICT_SOURCE)
    if isinstance(source, Sequence) and all(isinstance(name, str) for name in source):
        repeated = sorted({name for name in source if list(source).count(name) > 1})
        if repeated:
            raise InvalidArgument(
                issue_codes.DUPLICATE_COLUMN, f'the column list names {", ".join(repeated)} more than once', value=repeated[0]
            )
        return validated(expanded(dict.fromkeys(source), None, INLINE_TABLE, LIST_SOURCE), LIST_SOURCE)
    message = (
        'generation takes a Profile, a profile or short YAML file, an example name, a list of column names, a dict of '
        f'columns such as {{"email": sd.fakes.email}}, or a schema of {", ".join(LIBRARIES[:-1])} or {LIBRARIES[-1]}, '
        f'not {type(source).__name__}'
    )
    raise UnsupportedArgument(issue_codes.UNSUPPORTED_PROFILE_SOURCE, message, value=type(source).__name__)


def expanded_profile(source: ProfileSource, rows: int | None = None) -> Profile:
    """The profile `source` stands for, recording `rows` as its row count where given. `Profile.expand` documents `source`."""
    profile = profile_from(source)
    if rows is None:
        return profile
    if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
        raise InvalidArgument(issue_codes.INVALID_ROWS, f'rows must be zero or more, not {rows!r}', field='rows', value=rows)
    return Profile(with_row_count(copy.deepcopy(profile.document), rows))
