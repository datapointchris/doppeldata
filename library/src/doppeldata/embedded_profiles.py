"""The profile and scenario a Parquet or Arrow IPC file holds where its write embedded them, and the provenance keys beside them.

A file is told apart by the bytes it opens with, and its metadata is read by the extension, so no Arrow package is needed.
"""

import dataclasses as dc
import json
import os
from pathlib import Path
from typing import Any

from doppeldata import _core
from doppeldata.errors import SourceError
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import Profile
from doppeldata.scenario_format import InvalidScenario
from doppeldata.scenario_format import Scenario

# Each value of a plan's identity, by the metadata key a file records it under.
IDENTITY_KEYS = {
    'dataset_rows': _core.DATASET_ROWS_KEY,
    'profile_digest': _core.PROFILE_DIGEST_KEY,
    'scenario_digest': _core.SCENARIO_DIGEST_KEY,
    'seed': _core.SEED_KEY,
    'stream_version': _core.STREAM_VERSION_KEY,
}


@dc.dataclass(frozen=True)
class WrittenDataFile:
    """What a write recorded in one Parquet or Arrow IPC file: the profile and scenario it embedded, each checked against its
    digest, the seed and rows, and the metadata they were read from."""

    path: str
    profile: Profile
    scenario: Scenario | None
    seed: int | None
    rows: int | None
    metadata: dict[str, str]

    def identity_differences(self, identity: dict[str, Any]) -> list[str]:
        """The metadata keys whose value differs from the plan `identity` names, in key order."""
        return [key for name, key in IDENTITY_KEYS.items() if self.metadata.get(key) != recorded_text(identity[name])]


def recorded_text(value: Any) -> str | None:
    """A plan's identity value as file metadata writes it: text, and None for no scenario."""
    return None if value is None else str(value)


def data_file_metadata(path: str | os.PathLike[str]) -> dict[str, str] | None:
    """The key-value metadata of a local Parquet or Arrow IPC file, or None for a file of neither format. A footer or
    schema that does not read raises SourceError, and a file that does not open raises OSError."""
    try:
        return _core.data_file_metadata(Path(path))
    except ValueError as error:
        raise SourceError(f'{os.fspath(path)} could not be read: {error}', role='profile') from error


def written_data_file(path: str | os.PathLike[str], metadata: dict[str, str] | None = None) -> WrittenDataFile:
    """What `path`'s write recorded in it, given the file's `metadata` where it was already read.

    A file of neither format, one holding no profile, and one whose profile or scenario is not the one its digest names
    raise SourceError. An embedded profile that does not validate raises InvalidProfile, and a scenario InvalidScenario.
    """
    name = os.fspath(path)
    if metadata is None:
        metadata = data_file_metadata(path)
    if metadata is None:
        raise SourceError(f'{name} is neither a Parquet nor an Arrow IPC file, so it holds no profile', role='profile')
    profile = Profile(embedded_document(path, metadata))
    issues = profile.validate()
    if issues:
        raise InvalidProfile(name, issues)
    matching_digest(name, 'profile', profile.digest, metadata.get(_core.PROFILE_DIGEST_KEY))
    scenario = None
    if _core.SCENARIO_KEY in metadata:
        scenario = Scenario(embedded_json(name, metadata, _core.SCENARIO_KEY))
        issues = scenario.validate()
        if issues:
            raise InvalidScenario(f'the scenario {name} holds', issues)
        matching_digest(name, 'scenario', scenario.digest, metadata.get(_core.SCENARIO_DIGEST_KEY))
    seed = recorded_integer(name, metadata, _core.SEED_KEY)
    rows = recorded_integer(name, metadata, _core.DATASET_ROWS_KEY)
    return WrittenDataFile(name, profile, scenario, seed, rows, metadata)


def embedded_document(path: str | os.PathLike[str], metadata: dict[str, str]) -> Any:
    """The profile document a data file's `metadata` holds, unvalidated, raising SourceError where it holds none."""
    if _core.PROFILE_KEY not in metadata:
        raise SourceError(
            f'{os.fspath(path)} holds no profile; a Parquet or Arrow write embeds one with embed_profile=True, '
            'or data generate --embed-profile',
            role='profile',
        )
    return embedded_json(os.fspath(path), metadata, _core.PROFILE_KEY)


def embedded_json(name: str, metadata: dict[str, str], key: str) -> Any:
    try:
        return json.loads(metadata[key])
    except json.JSONDecodeError as error:
        raise SourceError(f'{name} holds {key} as text that is not JSON: {error}', role='profile') from error


def matching_digest(name: str, document: str, digest: str, recorded: str | None) -> None:
    """Refuse an embedded document whose digest is not the one its write recorded beside it."""
    if digest != recorded:
        raise SourceError(
            f'{name} holds a {document} whose digest is {digest}, not the {recorded} its write recorded beside it', role='profile'
        )


def recorded_integer(name: str, metadata: dict[str, str], key: str) -> int | None:
    value = metadata.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError as error:
        raise SourceError(f'{name} holds {key} as {value!r}, which is not an integer', role='profile') from error


def data_file_profile(path: str | os.PathLike[str], metadata: dict[str, str] | None = None) -> Profile:
    """The profile `path`'s write embedded in it, refused as `written_data_file` refuses it."""
    return written_data_file(path, metadata).profile
