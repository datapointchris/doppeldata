"""Each kind of schema a seed profile is imported from, by the name a caller gives it, and the options each reads."""

import dataclasses as dc
import json
import os
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from doppeldata import issue_codes
from doppeldata.errors import InvalidArgument
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import Profile
from doppeldata.profile_sources import read_as

# What each option beyond `source` names, for the refusal of one an import kind reads none of.
OPTION_CONCEPTS = {
    'table': 'table to pick',
    'region': 'region',
    'detected_entities': 'detected entity types',
    'aws_profile': 'AWS profile',
}


@dc.dataclass(frozen=True)
class ImportKind:
    """A kind of schema: the options beyond `source` it reads, and how its source becomes a seed."""

    name: str
    reads: frozenset[str]
    imported: Callable[[str, dict[str, Any]], Profile | Bundle]


def glue_seed(source: str, options: dict[str, Any]) -> Profile:
    """The Glue Data Catalog table `source` names as DATABASE.TABLE."""
    database, dot, name = source.partition('.')
    if not dot or not database or not name:
        raise InvalidArgument(issue_codes.GLUE_TABLE_NAME, f'{source!r} is not DATABASE.TABLE', field='source', value=source)
    entities = entity_types(options['detected_entities'])
    return Profile.from_glue(database, name, region=options['region'], detected_entities=entities, aws_profile=options['aws_profile'])


def data_package_seed(source: str, options: dict[str, Any]) -> Profile | Bundle:
    """The resource `table` names, else a bundle of every resource."""
    table = options['table']
    return Bundle.from_data_package(source) if table is None else Profile.from_data_package(source, table=table)


KINDS = {
    kind.name: kind
    for kind in (
        ImportKind('glue', frozenset({'region', 'detected_entities', 'aws_profile'}), glue_seed),
        ImportKind('odcs', frozenset({'table'}), lambda source, options: Profile.from_odcs(source, table=options['table'])),
        ImportKind('json-schema', frozenset(), lambda source, options: Profile.from_json_schema(source)),
        ImportKind('table-schema', frozenset(), lambda source, options: Profile.from_table_schema(source)),
        ImportKind('data-package', frozenset({'table'}), data_package_seed),
    )
}
IMPORT_KINDS = tuple(KINDS)


def import_seed(
    kind: str,
    source: str | os.PathLike[str],
    *,
    table: str | None = None,
    region: str | None = None,
    detected_entities: Mapping[str, Sequence[str]] | str | os.PathLike[str] | None = None,
    aws_profile: str | None = None,
) -> Profile | Bundle:
    """The seed profile `source` describes as the schema `kind` names, one of IMPORT_KINDS.

    `glue` reads the AWS Glue Data Catalog table `source` names as DATABASE.TABLE, in `region` with the AWS profile
    `aws_profile` names, typing each column by the entity types `detected_entities` gives it, a mapping or a JSON file of
    one. `odcs` and `data-package` take `table` to pick one of several tables, and a Data Package without it is a bundle of
    every resource. Each option `kind` reads none of is refused, as is a kind not listed, each at its own name. A schema
    refused is refused at `source`.
    """
    found = KINDS.get(kind)
    if found is None:
        message = f'{kind!r} is not a kind of schema to import'
        raise InvalidArgument(issue_codes.UNKNOWN_IMPORT_KIND, message, field='kind', value=kind, valid=IMPORT_KINDS)
    options = {'table': table, 'region': region, 'detected_entities': detected_entities, 'aws_profile': aws_profile}
    for name, given in options.items():
        if given is not None and name not in found.reads:
            readers = ' or '.join(each.name for each in KINDS.values() if name in each.reads)
            message = f'an import from {kind} takes no {OPTION_CONCEPTS[name]}, which only an import from {readers} reads'
            raise InvalidArgument(issue_codes.IMPORT_OPTION_UNREAD, message, field=name, value=str(given))
    with read_as('source', relabeled=('profile', 'schema')):
        return found.imported(os.fspath(source), options)


def entity_types(given: Mapping[str, Sequence[str]] | str | os.PathLike[str] | None) -> Mapping[str, Sequence[str]] | None:
    """Each column's Glue entity types, as given or read from the JSON file named, refused in any other shape."""
    if given is None or isinstance(given, Mapping):
        return given
    path = Path(given)
    try:
        found = json.loads(path.read_text())
    except OSError as error:
        message = f'{path} could not be read: {error.strerror or error}'
        raise InvalidArgument(issue_codes.FILE_UNREADABLE, message, field='detected_entities', value=str(path)) from error
    except json.JSONDecodeError as error:
        message = f'{path} is not JSON: {error}'
        raise InvalidArgument(issue_codes.DETECTED_ENTITIES_UNREADABLE, message, field='detected_entities', value=str(path)) from error
    if not isinstance(found, dict) or not all(
        isinstance(types, list) and all(isinstance(each, str) for each in types) for types in found.values()
    ):
        message = f'{path} is not a JSON object of each column to a list of entity types, such as {{"email": ["EMAIL"]}}'
        raise InvalidArgument(issue_codes.DETECTED_ENTITIES_UNREADABLE, message, field='detected_entities', value=str(path))
    return found
