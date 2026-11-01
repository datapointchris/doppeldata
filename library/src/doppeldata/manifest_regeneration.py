"""A dataset written again from its manifest, each file compared with the manifest's record of it.

The manifest settles every input but the profile, so the caller gives only that. A profile whose SHA-256 is not the one
the manifest records is refused before the manifest's scenario is applied to it. The engine then refuses a manifest it
cannot write again, writes every file the manifest names and compares each with its record. A bundle's manifest names
each table's, and the engine checks every one before any table is written into `<out>/<table>/`, parents first.
"""

import dataclasses as dc
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from doppeldata import _core
from doppeldata import issue_codes
from doppeldata.aws_sessions import require_boto3
from doppeldata.bundle_generation import bundle_arguments
from doppeldata.bundle_generation import bundle_written_table_by_table
from doppeldata.counted_nouns import counted
from doppeldata.dataset_generation import destination_of
from doppeldata.dataset_generation import engine_plan
from doppeldata.dataset_generation import one_file_of
from doppeldata.dataset_generation import s3_store_of
from doppeldata.errors import InvalidArgument
from doppeldata.errors import StoppedWrite
from doppeldata.errors import WriteFailed
from doppeldata.issue_codes import Issue
from doppeldata.manifest import MANIFEST_FILE_NAME
from doppeldata.manifest import BundleManifest
from doppeldata.manifest import Manifest
from doppeldata.manifest import in_s3
from doppeldata.manifest import is_bundle_manifest
from doppeldata.manifest import with_table_manifests
from doppeldata.profile_format import Bundle
from doppeldata.profile_sources import ProfileSource
from doppeldata.profile_sources import profile_from
from doppeldata.scenario_format import InvalidScenario
from doppeldata.scenario_format import Scenario


def manifest_text(location: str) -> str:
    """The text of the manifest file at `location`, a local path or an S3 URL naming the object."""
    if in_s3(location):
        require_boto3(f'reading {location}')
        from doppeldata import s3_access

        prefix, _, name = location.rpartition('/')
        text = s3_access.s3_object_text(prefix, name)
        if text is None:
            message = f'no object is at {location}; name the _manifest.json a write left'
            raise InvalidArgument(issue_codes.MANIFEST_NOT_FOUND, message, field='manifest', value=location)
        return text
    try:
        return Path(location).read_text(encoding='utf-8')
    except (FileNotFoundError, IsADirectoryError, NotADirectoryError) as error:
        message = f'{location} is no manifest file; name the _manifest.json a write left'
        raise InvalidArgument(issue_codes.MANIFEST_NOT_FOUND, message, field='manifest', value=location) from error
    except OSError as error:
        message = f'{location} could not be read: {error.strerror or error}'
        raise InvalidArgument(issue_codes.FILE_UNREADABLE, message, field='manifest', value=location) from error


def manifest_document(location: str) -> dict[str, Any]:
    """The JSON object of the manifest at `location`, refused where it holds none."""
    text = manifest_text(location)
    try:
        document = json.loads(text)
    except json.JSONDecodeError as error:
        message = f'{location} is not JSON: {error}'
        raise InvalidArgument(issue_codes.MANIFEST_UNREADABLE, message, field='manifest', value=location) from error
    if not isinstance(document, dict) or document.get('kind') not in ('table', 'bundle'):
        message = f'{location} is not a doppeldata manifest, whose kind is table or bundle'
        raise InvalidArgument(issue_codes.MANIFEST_UNREADABLE, message, field='manifest', value=location)
    return document


def manifest_at(location: str | os.PathLike[str]) -> Manifest | BundleManifest:
    """The manifest file at `location`, and for a bundle's, each table's manifest its directory beside it holds."""
    named = os.fspath(location)
    document = manifest_document(named)
    if not is_bundle_manifest(document):
        return Manifest(document)
    root = named.rpartition('/')[0] if in_s3(named) else os.fspath(Path(named).parent)
    return with_table_manifests(document, root)


def recorded_scenario(document: dict[str, Any]) -> Scenario:
    """The scenario a manifest records as `document`, its canonical form, refused as InvalidScenario where it is not one."""
    scenario = Scenario(document)
    issues = scenario.validate()
    if issues:
        raise InvalidScenario(f"the manifest's scenario {document.get('scenario')!r}", issues)
    return scenario


def recorded_bundle_scenarios(record: dict[str, Any] | None) -> dict[str, Scenario]:
    """Each table a bundle's manifest records a scenario for, and that scenario, from the map its `document` holds."""
    if record is None:
        return {}
    return {table: recorded_scenario(document) for table, document in record['document'].items()}


def recorded_location(recorded: Manifest | BundleManifest) -> str | None:
    """Where the write `recorded` records stored its manifest file, or None for a write to memory, which keeps none."""
    destination: str = recorded.document['destination']
    if _core.destination_scheme(destination)[0] == 'memory':
        return None
    return f'{destination.rstrip("/")}/{MANIFEST_FILE_NAME}'


def holds(out: str, location: str) -> bool:
    """Whether the destination `out` holds the file or object at `location`, at its root or under it."""
    if in_s3(out) or in_s3(location):
        if not (in_s3(out) and in_s3(location)):
            return False
        root, named = urlparse(out), urlparse(location)
        prefix = root.path.strip('/')
        return root.netloc == named.netloc and named.path.lstrip('/').startswith(f'{prefix}/' if prefix else '')
    if '://' in out:
        return False
    return Path(location).resolve().is_relative_to(Path(out).resolve())


def regenerated_destination(out: str | os.PathLike[str], manifest_location: str | None) -> tuple[str, str | None]:
    """`out` as the engine writes to it and its S3 bucket. It is refused where it names one file, and where it holds the
    manifest at `manifest_location`, because a run there with replace would store its fresh manifest over that record."""
    named = os.fspath(out)
    if one_file_of(out) is not None:
        message = f'{named} names one file, and a regeneration writes the directory of parts its manifest records'
        raise InvalidArgument(issue_codes.REGENERATES_A_DIRECTORY, message, field='out', value=named)
    if manifest_location is not None and holds(named, manifest_location):
        message = (
            f'{named} holds {manifest_location}, the manifest the regeneration compares with, and a run there with replace would '
            'store its fresh manifest over it'
        )
        raise InvalidArgument(issue_codes.REGENERATES_OVER_ITS_MANIFEST, message, field='out', value=named)
    destination, _, bucket = destination_of(out)
    return destination, bucket


def refuse_other_profile(sha256: str, identity: dict[str, Any]) -> None:
    """Refuse a profile whose SHA-256 is not the one the manifest's `identity` records. It runs before the manifest's
    scenario is applied, because a changed profile may no longer hold the columns that scenario bends."""
    recorded = identity['profile_sha256']
    if sha256 != recorded:
        message = (
            f"the profile's SHA-256 {sha256} is not the manifest's {recorded}, so the two identities differ in profile_sha256; "
            'give the profile that wrote the dataset'
        )
        raise WriteFailed(issue_codes.IDENTITY_MISMATCH, message)


def recorded_bundle_plan(bundle: Bundle, recorded: Manifest | BundleManifest) -> '_core.BundlePlan':
    """The plan of `bundle` at the seed, scale and scenarios `recorded` records, the bundle's manifest or a table's."""
    identity = recorded.identity
    refuse_other_profile(bundle.sha256, identity)
    scenarios = recorded_bundle_scenarios(recorded.document.get('scenario'))
    return bundle_arguments(bundle, int(identity['seed']), float(identity['scale']), scenarios).plan()


def recorded_plan(recorded: Manifest, source: ProfileSource | Bundle) -> '_core.Plan':
    """The plan of the table `recorded` records, from `source`: the table's profile, or the bundle a bundle's table was
    written from."""
    identity = recorded.identity
    table = identity.get('table')
    if table is None:
        if isinstance(source, Bundle):
            message = "the profile is a bundle, and the manifest records one table generated alone; give that table's profile"
            raise WriteFailed(issue_codes.IDENTITY_MISMATCH, message)
        profile = profile_from(source)
        refuse_other_profile(profile.sha256, identity)
        record = recorded.document.get('scenario')
        scenario = recorded_scenario(record['document']) if record is not None else None
        return engine_plan(profile, int(identity['dataset_rows']), int(identity['seed']), scenario)
    if not isinstance(source, Bundle):
        message = f'the manifest records table {table} of a bundle, and the profile is one table; give the bundle'
        raise WriteFailed(issue_codes.IDENTITY_MISMATCH, message)
    planned = dict(recorded_bundle_plan(source, recorded).tables())
    if table not in planned:
        message = f'the manifest records table {table}, which the bundle does not hold: {", ".join(planned)}'
        raise WriteFailed(issue_codes.IDENTITY_MISMATCH, message)
    return planned[table]


def regenerate_table(
    source: ProfileSource | Bundle,
    recorded: Manifest,
    out: str | os.PathLike[str],
    *,
    manifest_location: str | None,
    threads: int,
    replace: bool,
) -> Manifest:
    """Write the files of one table's manifest, read from `manifest_location` where it was, again to `out`, from `source`.
    A manifest of a bundle's table takes the bundle."""
    destination, bucket = regenerated_destination(out, manifest_location)
    engine = recorded_plan(recorded, source)
    store = s3_store_of(destination, bucket)
    written = engine.regenerate(json.dumps(recorded.document), destination, threads=threads, replace=replace, s3_store=store)
    return Manifest.from_json(written)


def regenerate_bundle(
    bundle: Bundle,
    recorded: BundleManifest,
    out: str | os.PathLike[str],
    *,
    manifest_location: str | None,
    threads: int,
    replace: bool,
) -> BundleManifest:
    """Write every table of a bundle's manifest again under `out`, each compared with its own manifest, parents first.

    A table whose files differ is still written whole, and the rest after it. The error naming every differing file, as
    `<table>/<path>`, is raised once the bundle's manifest is stored.
    """
    destination, bucket = regenerated_destination(out, manifest_location)
    engine = recorded_bundle_plan(bundle, recorded)
    engine.check_regenerable(json.dumps(recorded.document), {name: json.dumps(table.document) for name, table in recorded.tables.items()})
    missing = [name for name, _ in engine.tables() if name not in recorded.tables]
    if missing:
        message = f'the bundle manifest holds no manifest of {", ".join(missing)}; read it with its tables from the path it was written to'
        raise InvalidArgument(issue_codes.MANIFEST_NOT_FOUND, message, field='manifest', value=missing[0])
    store = s3_store_of(destination, bucket)
    differing: list[Issue] = []

    def regenerate_one(name: str, table: '_core.Plan', table_destination: str) -> str:
        table_manifest = json.dumps(recorded.tables[name].document)
        try:
            written: str = table.regenerate(table_manifest, table_destination, threads=threads, replace=False, s3_store=store)
        except WriteFailed as error:
            if error.code != issue_codes.REGENERATED_FILES_DIFFER or error.manifest is None:
                raise
            differing.extend(dc.replace(issue, message=f'{name}/{issue.message}', value=f'{name}/{issue.value}') for issue in error.issues)
            return json.dumps(error.manifest.document)
        return written

    records = (recorded.document['profile'], recorded.document.get('scenario'))
    written = bundle_written_table_by_table(engine, records, engine.tables(), destination, replace, store, regenerate_one, (StoppedWrite,))
    if differing:
        headline = (
            f"{counted(len(differing), 'regenerated file')} {'differs' if len(differing) == 1 else 'differ'} from the manifests' record"
        )
        raise WriteFailed(issue_codes.REGENERATED_FILES_DIFFER, headline, manifest_stored=True, issues=differing)
    return written
