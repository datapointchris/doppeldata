"""The record a write leaves beside a generated dataset, which with its profile and scenario regenerates every file."""

import dataclasses as dc
import importlib.util
import json
import os
from pathlib import Path
from typing import Any

MANIFEST_FILE_NAME = '_manifest.json'


@dc.dataclass(frozen=True)
class Manifest:
    """A generated dataset's manifest, as `_manifest.json` holds it. `doppeldata schemas show manifest` is its schema."""

    document: dict[str, Any]

    @classmethod
    def from_json(cls, text: str) -> 'Manifest':
        return cls(json.loads(text))

    @property
    def identity(self) -> dict[str, Any]:
        """The five values the dataset's rows are a pure function of, keyed as the identity's canonical JSON."""
        identity: dict[str, Any] = self.document['identity']
        return identity

    @property
    def dataset_digest(self) -> str:
        """`sha256:` and the hex SHA-256 of the identity's canonical JSON, which names the dataset."""
        return str(self.document['dataset_digest'])

    @property
    def files(self) -> list[dict[str, Any]]:
        """Every object written, in path order, each with its rows, bytes and SHA-256."""
        files: list[dict[str, Any]] = self.document['files']
        return files

    @property
    def partial(self) -> bool:
        """True when the run stopped early, so `files` lists only the objects that completed."""
        return bool(self.document['partial'])


def written_manifest(data: Any) -> Manifest | None:
    """The manifest a doppeldata write left at the root `data` names, a directory or an `s3://` prefix. None where `data` is
    anything else, or the root holds none."""
    if not isinstance(data, str | os.PathLike):
        return None
    item = os.fspath(data)
    if not item.startswith('s3://'):
        path = Path(item) / MANIFEST_FILE_NAME
        return Manifest.from_json(path.read_text(encoding='utf-8')) if path.is_file() else None
    # Without boto3 nothing reads S3, and profiling the data refuses it next, naming boto3.
    if importlib.util.find_spec('boto3') is None:
        return None
    from doppeldata.profiling import sources

    return sources.s3_manifest(item)
