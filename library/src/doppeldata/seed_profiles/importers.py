"""Each kind of schema a seed profile is imported from, by the name a caller gives it, and the options each reads."""

import json
import os
from collections.abc import Mapping
from collections.abc import Sequence
from pathlib import Path

from doppeldata import issue_codes
from doppeldata.errors import InvalidArgument
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import Profile

# The kinds that hold several tables, so `table` picks one.
SEVERAL_TABLES = ('odcs', 'data-package')
IMPORT_KINDS = ('glue', 'odcs', 'json-schema', 'table-schema', 'data-package')


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
    every resource. Each option `kind` reads none of is refused, as is a kind not listed, each at its own name.
    """
    if kind not in IMPORT_KINDS:
        message = f'{kind!r} is not a kind of schema to import'
        raise InvalidArgument(issue_codes.UNKNOWN_IMPORT_KIND, message, field='kind', value=kind, valid=IMPORT_KINDS)
    if table is not None and kind not in SEVERAL_TABLES:
        message = f'a {kind} source holds one table, so table picks nothing'
        raise InvalidArgument(issue_codes.IMPORT_OPTION_UNREAD, message, field='table', value=table)
    if kind != 'glue':
        for name, given in (('region', region), ('detected_entities', detected_entities), ('aws_profile', aws_profile)):
            if given is not None:
                message = f'{name} is read only from the AWS Glue Data Catalog, and {kind} reads a schema file'
                raise InvalidArgument(issue_codes.IMPORT_OPTION_UNREAD, message, field=name, value=str(given))
    named = os.fspath(source)
    if kind == 'glue':
        database, dot, name = named.partition('.')
        if not dot or not database or not name:
            message = f'{named!r} is not DATABASE.TABLE'
            raise InvalidArgument(issue_codes.GLUE_TABLE_NAME, message, field='source', value=named)
        entities = entity_types(detected_entities)
        return Profile.from_glue(database, name, region=region, detected_entities=entities, aws_profile=aws_profile)
    if kind == 'odcs':
        return Profile.from_odcs(named, table=table)
    if kind == 'json-schema':
        return Profile.from_json_schema(named)
    if kind == 'table-schema':
        return Profile.from_table_schema(named)
    return Bundle.from_data_package(named) if table is None else Profile.from_data_package(named, table=table)


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
