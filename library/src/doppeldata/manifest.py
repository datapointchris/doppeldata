"""The record a write leaves beside a generated dataset, which with its profile and scenario regenerates every file."""

import dataclasses as dc
import importlib.util
import json
import os
from pathlib import Path
from typing import Any

from doppeldata import _core

MANIFEST_FILE_NAME = _core.MANIFEST_FILE_NAME
PARTIAL_MANIFEST_FILE_NAME = _core.PARTIAL_MANIFEST_FILE_NAME
FAULTS_FILE_NAME = _core.FAULTS_FILE_NAME


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
        """Every object of the rows written, in path order, each with its rows, bytes and SHA-256. Under `cdc` these are the
        full-load files, and `cdc` lists the change files and the expected table."""
        files: list[dict[str, Any]] = self.document['files']
        return files

    @property
    def format(self) -> str:
        """The format every file was written in, as `doppeldata schemas show manifest` names it, such as `csv`."""
        return str(self.document['format'])

    @property
    def partial(self) -> bool:
        """True when the run stopped early, so `files` lists only the objects that completed."""
        return bool(self.document['partial'])

    @property
    def faults(self) -> dict[str, int] | None:
        """The faults the write's fault list holds, counted by kind, or None for a write that lists none: one with no
        `malformed_rows`, and no `cdc` with a fault rate."""
        faults: dict[str, int] | None = self.document.get('faults')
        return faults

    @property
    def cdc(self) -> dict[str, Any] | None:
        """What a `cdc` write stores beside its full load: the stream's prefixes, change counts, change files and expected
        parts. None for any other write."""
        cdc: dict[str, Any] | None = self.document.get('cdc')
        return cdc


@dc.dataclass(frozen=True)
class BundleManifest:
    """A bundle's manifest, as `_manifest.json` at its destination holds it, naming the manifest each table's directory holds.
    `doppeldata schemas show bundle-manifest` is its schema. `tables` holds each table's own manifest where a write returns it.
    """

    document: dict[str, Any]
    tables: dict[str, Manifest] = dc.field(default_factory=dict)

    @property
    def identity(self) -> dict[str, Any]:
        """The bundle's own identity: no table, and `dataset_rows` every table's rows summed."""
        identity: dict[str, Any] = self.document['identity']
        return identity

    @property
    def dataset_digest(self) -> str:
        return str(self.document['dataset_digest'])

    @property
    def partial(self) -> bool:
        """True until every table is whole."""
        return bool(self.document['partial'])

    @property
    def listed(self) -> list[dict[str, Any]]:
        """Every table, parents first: its name, its manifest's path under the destination, its `dataset_digest` and rows,
        and whether it is partial."""
        listed: list[dict[str, Any]] = self.document['tables']
        return listed


def is_bundle_manifest(document: Any) -> bool:
    """Whether a manifest's document is a bundle's, as its `kind` says."""
    return isinstance(document, dict) and document.get('kind') == 'bundle'


def written_manifest(data: Any) -> Manifest | None:
    """The manifest a doppeldata write left at the root `data` names, a directory or an `s3://` prefix, or beside the one
    file `data` names where it records that file alone. None where `data` is anything else, no such manifest is there, or
    the one there is a bundle's, which `written_bundle_manifest` reads."""
    document = written_document(data)
    return Manifest(document) if document is not None and not is_bundle_manifest(document) else None


def written_bundle_manifest(data: Any) -> BundleManifest | None:
    """The bundle manifest at the root `data` names, with each table's manifest its directory holds, or None where there is
    none."""
    document = written_document(data)
    if document is None or not is_bundle_manifest(document):
        return None
    root = os.fspath(data).rstrip('/')
    tables = {}
    for listed in document['tables']:
        table = written_document(f'{root}/{listed["name"]}')
        if table is not None:
            tables[listed['name']] = Manifest(table)
    return BundleManifest(document, tables)


def unfinished(root: str) -> str:
    return (
        f'{root} holds {PARTIAL_MANIFEST_FILE_NAME}, so the doppeldata write there stopped or is still running and its files are '
        'incomplete; once no write is running there, generate the dataset again, replacing what it left'
    )


def holds_unfinished_write(data: Any) -> bool:
    """Whether the directory or `s3://` prefix `data` names holds the claim a doppeldata write keeps there until it finishes."""
    if not isinstance(data, str | os.PathLike):
        return False
    item = os.fspath(data)
    if not item.startswith('s3://'):
        return (Path(item) / PARTIAL_MANIFEST_FILE_NAME).is_file()
    # Without boto3 nothing reads S3, and reading the tables refuses it next, naming boto3.
    if importlib.util.find_spec('boto3') is None:
        return False
    from doppeldata.profiling import s3_sources

    return s3_sources.s3_holds(item, PARTIAL_MANIFEST_FILE_NAME)


def written_document(data: Any) -> dict[str, Any] | None:
    """The JSON of the manifest at the root `data` names, or beside the one file it names where that records the file alone."""
    if not isinstance(data, str | os.PathLike):
        return None
    item = os.fspath(data)
    if not item.startswith('s3://'):
        local = Path(item)
        if local.is_file():
            beside = local.parent / MANIFEST_FILE_NAME
            recorded = json.loads(beside.read_text(encoding='utf-8')) if beside.is_file() else None
            if recorded is None or is_bundle_manifest(recorded):
                return None
            return recorded if [file['path'] for file in recorded['files']] == [local.name] else None
        path = local / MANIFEST_FILE_NAME
        document: dict[str, Any] | None = json.loads(path.read_text(encoding='utf-8')) if path.is_file() else None
        return document
    # Without boto3 nothing reads S3, and profiling the data refuses it next, naming boto3.
    if importlib.util.find_spec('boto3') is None:
        return None
    from doppeldata.profiling import s3_sources

    held = s3_sources.s3_manifest(item)
    return held.document if held is not None else None
