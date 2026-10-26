"""The profile a Parquet or Arrow IPC file holds where its write embedded it, and the provenance keys beside it.

A file is told apart by the bytes it opens with, and its metadata is read by the extension, so no Arrow package is needed.
"""

import json
import os
from pathlib import Path
from typing import Any

from doppeldata import _core
from doppeldata.errors import SourceError
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import Profile


def data_file_metadata(path: str | os.PathLike[str]) -> dict[str, str] | None:
    """The key-value metadata of a local Parquet or Arrow IPC file, or None for a file of neither format. A footer or
    schema that does not read raises SourceError, and a file that does not open raises OSError."""
    try:
        return _core.data_file_metadata(Path(path))
    except ValueError as error:
        raise SourceError(f'{os.fspath(path)} could not be read: {error}') from error


def data_file_profile(path: str | os.PathLike[str], metadata: dict[str, str] | None = None) -> Profile:
    """The profile `path`'s write embedded in it, given the file's `metadata` where it was already read.

    A file of neither format, or one holding no profile, raises SourceError, and a profile that does not validate raises
    InvalidProfile.
    """
    if metadata is None:
        metadata = data_file_metadata(path)
    if metadata is None:
        raise SourceError(f'{os.fspath(path)} is neither a Parquet nor an Arrow IPC file, so it holds no profile')
    profile = Profile(embedded_document(path, metadata))
    issues = profile.validate()
    if issues:
        raise InvalidProfile(os.fspath(path), issues)
    return profile


def embedded_document(path: str | os.PathLike[str], metadata: dict[str, str]) -> Any:
    """The profile document a data file's `metadata` holds, unvalidated, raising SourceError where it holds none."""
    embedded = metadata.get(_core.PROFILE_KEY)
    if embedded is None:
        raise SourceError(
            f'{os.fspath(path)} holds no profile; a Parquet or Arrow write embeds one with embed_profile=True, '
            'or data generate --embed-profile'
        )
    return json.loads(embedded)


def provenance_defaults(metadata: dict[str, str]) -> tuple[int | None, int | None]:
    """The seed and the generated rows a data file's provenance names, each None where the file names none."""
    seed, rows = metadata.get(_core.SEED_KEY), metadata.get(_core.DATASET_ROWS_KEY)
    return (int(seed) if seed is not None else None, int(rows) if rows is not None else None)
