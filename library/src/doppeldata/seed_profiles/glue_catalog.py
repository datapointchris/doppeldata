"""A seed from a table in the AWS Glue Data Catalog, read through `glue:GetTable` and `glue:GetPartitions` with boto3.

boto3 is imported inside the call, never at module load, and the library declares it nowhere. The seed records the
table's region, database and name, and never its ARN, which holds the account id. A Lake Formation resource link is
followed to the table it shares. A table the catalog lists no partition of, as one using Athena's partition projection,
draws each key's projected values where the projection lists them or bounds them in the key's type.
"""

import contextlib
import dataclasses as dc
import importlib
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

from doppeldata.errors import PackagesMissing
from doppeldata.errors import SourceError
from doppeldata.errors import uninstalled
from doppeldata.profile_format import Profile
from doppeldata.profiling.backend import SourceFormat
from doppeldata.published_schemas import published_enum
from doppeldata.seed_profiles.declared_columns import DeclaredColumn
from doppeldata.seed_profiles.declared_columns import day
from doppeldata.seed_profiles.declared_columns import instant
from doppeldata.seed_profiles.declared_columns import warn_left_out
from doppeldata.seed_profiles.declared_tables import DeclaredTable
from doppeldata.seed_profiles.declared_tables import PartitionKey
from doppeldata.seed_profiles.hive_types import hive_column

# A table's `classification`, as a crawler writes it, and the source format it names. ORC and Avro are left out,
# because doppeldata writes neither.
CLASSIFICATIONS = {
    'csv': SourceFormat.CSV,
    'tsv': SourceFormat.TSV,
    'parquet': SourceFormat.PARQUET,
    'json': SourceFormat.NDJSON,
}
# A SerDe's class, where the table names no classification, and the format it reads.
SERDE_FORMATS = {
    'OpenCSVSerde': SourceFormat.CSV,
    'LazySimpleSerDe': SourceFormat.CSV,
    'ParquetHiveSerDe': SourceFormat.PARQUET,
    'JsonSerDe': SourceFormat.NDJSON,
}
DELIMITED = (SourceFormat.CSV, SourceFormat.TSV)
# OpenCSVSerde's own dialect, where its parameters name none.
OPEN_CSV_DEFAULTS = {'separatorChar': ',', 'quoteChar': '"', 'escapeChar': '\\'}
# LazySimpleSerDe's delimiter where neither `field.delim` nor `serialization.format` names one.
LAZY_SIMPLE_DELIMITER = '\x01'
# What Hive writes for a partition whose key is null, which is no value of the key.
NULL_PARTITION = '__HIVE_DEFAULT_PARTITION__'
# How a partition key of each logical type reads a listed value, raising ValueError for one that is no value of it.
PARTITION_VALUE_READERS: dict[str, Callable[[str], Any]] = {'integer': int, 'number': float, 'date': day, 'timestamp': instant}


def glue_profile(
    database: str, table: str, *, region: str | None = None, detected_entities: Mapping[str, Sequence[str]] | None = None
) -> Profile:
    """The seed of the Glue table `database`.`table`. `Profile.from_glue` documents the arguments."""
    # The detector packs that type a detected entity import pyarrow.
    missing = uninstalled({'boto3': 'boto3', **({'pyarrow': 'pyarrow'} if detected_entities else {})})
    if missing:
        raise PackagesMissing(
            'glue',
            missing,
            doing='importing a seed from the Glue Data Catalog',
            installed_by=', or install doppeldata-cli, which declares them',
        )
    client = glue_client(region)
    named = f'glue:{client.meta.region_name}/{database}/{table}'
    with catalog_errors(named, client.meta.endpoint_url):
        described = client.get_table(DatabaseName=database, Name=table)['Table']
        located: dict[str, str] = {'DatabaseName': database}
        target = described.get('TargetTable')
        if target:
            # A resource link holds no columns. The table it shares may sit in another account or region.
            if target.get('Region') and target['Region'] != client.meta.region_name:
                client = glue_client(target['Region'])
            located = {'DatabaseName': target['DatabaseName'], **({'CatalogId': target['CatalogId']} if target.get('CatalogId') else {})}
            described = client.get_table(**located, Name=target['Name'])['Table']
        partitions: list[list[str]] = []
        if described.get('PartitionKeys'):
            pages = client.get_paginator('get_partitions').paginate(**located, TableName=described['Name'], ExcludeColumnSchema=True)
            partitions = [partition['Values'] for page in pages for partition in page['Partitions']]
    return catalog_table(described, partitions, named, detected_entities or {}).profile()


def glue_client(region: str | None) -> Any:
    """A Glue client of the AWS environment's, in `region` where it is given."""
    boto3 = importlib.import_module('boto3')
    exceptions = importlib.import_module('botocore.exceptions')
    from doppeldata.aws_access import CLIENT_CONFIG

    try:
        return boto3.Session().client('glue', region_name=region, config=CLIENT_CONFIG)
    except exceptions.NoRegionError as error:
        message = "the Glue Data Catalog is regional, and the AWS environment names no region; give the catalog's, or set AWS_REGION"
        raise SourceError(message, role='region') from error
    except exceptions.InvalidRegionError as error:
        raise SourceError(f"{str(error).rstrip('.')}; give the catalog's region, such as eu-west-1", role='region') from error
    except exceptions.ProfileNotFound as error:
        raise SourceError(f'{error}; set AWS_PROFILE to a profile the AWS config holds', role='environment') from error
    except ValueError as error:
        # botocore refuses an endpoint that is no URL with a bare ValueError.
        raise SourceError(f'{error}, from AWS_ENDPOINT_URL_GLUE, AWS_ENDPOINT_URL or the AWS profile', role='environment') from error


@contextlib.contextmanager
def catalog_errors(named: str, endpoint: str) -> Iterator[None]:
    """Each boto3 failure reading `named` as a SourceError: exit 2 for a table the catalog lacks, 4 when no answer came."""
    from doppeldata.aws_access import AWS_ERRORS
    from doppeldata.aws_access import CREDENTIAL_ADVICE
    from doppeldata.aws_access import CREDENTIAL_ERRORS
    from doppeldata.aws_access import answered
    from doppeldata.aws_access import error_code
    from doppeldata.aws_access import no_answer
    from doppeldata.aws_access import unanswered

    exceptions = importlib.import_module('botocore.exceptions')
    try:
        yield
    except CREDENTIAL_ERRORS as error:
        raise SourceError(f'{named} could not be read: {error}; {CREDENTIAL_ADVICE}', exit_code=1, role='schema') from error
    except AWS_ERRORS as error:
        if unanswered(error):
            raise SourceError(no_answer(endpoint, 'Glue', error), exit_code=4, role='schema') from error
        if not isinstance(error, exceptions.ClientError):
            raise SourceError(f'{named} could not be read through {endpoint}: {error}', exit_code=1, role='schema') from error
        if error_code(error) == 'EntityNotFoundException':
            raise SourceError(f'{named} names no table the catalog holds: {answered(error)}', exit_code=2, role='schema') from error
        raise SourceError(f'{named} could not be read: {endpoint} answered {answered(error)}', exit_code=1, role='schema') from error


def catalog_table(
    described: Mapping[str, Any], partitions: list[list[str]], named: str, detected_entities: Mapping[str, Sequence[str]]
) -> DeclaredTable:
    """The table GetTable's `Table` describes, each partition key drawing the values `partitions` list for it, else the
    values its partition projection declares.
    """
    storage = described.get('StorageDescriptor') or {}
    parameters = {**(storage.get('Parameters') or {}), **(described.get('Parameters') or {})}
    serde = storage.get('SerdeInfo') or {}
    unread: list[str] = []
    columns: list[DeclaredColumn] = []
    for listed in storage.get('Columns') or []:
        declared = hive_column(listed['Name'], listed['Type'])
        if declared is None:
            unread.append(f'{listed["Name"]} ({listed["Type"]})')
            continue
        columns.append(declared)
    partition_keys, defaulted = [], []
    for index, key in enumerate(described.get('PartitionKeys') or []):
        declared = hive_column(key['Name'], key['Type'], required=True)
        if declared is None:
            unread.append(f'{key["Name"]} ({key["Type"]})')
            continue
        values = sorted({values[index] for values in partitions if len(values) > index and values[index] != NULL_PARTITION})
        declared, count = listing(declared, values, named) if values else projected(declared, parameters) or (declared, None)
        if not declared.values and declared.low is None:
            defaulted.append(declared.name)
        columns.append(declared)
        partition_keys.append(PartitionKey(declared.name, declared.arrow_type(), count))
    if unread:
        warn_left_out(f'{named}: columns of types the engine draws nothing of are left out: {", ".join(unread)}')
    if not columns:
        listed = ', '.join(unread) or 'it lists none'
        raise SourceError(f'{named} holds no column of a type the engine draws, so it makes no seed: {listed}', role='schema')
    if defaulted:
        warn_left_out(
            f'{named}: no value of the partition keys {", ".join(defaulted)} is listed or projected, so each draws its '
            "type's default, which may write a directory for every row; give each its values in the seed"
        )
    columns = typed(columns, detected_entities, named)
    return DeclaredTable(
        name=described['Name'],
        columns=tuple(columns),
        imported_from=f'{named}@{described["VersionId"]}' if described.get('VersionId') else named,
        partition_keys=tuple(partition_keys),
        source=source_of(parameters, serde),
        row_count=recorded_rows(parameters),
    )


def listing(key: DeclaredColumn, values: list[str], named: str) -> tuple[DeclaredColumn, int | None]:
    """A partition key drawing only the values the catalog lists, and how many it draws.

    A date or timestamp key draws every value between the least and the most, because a profile's temporal column holds
    no list of values. Its count is the days between them, and None for a timestamp's. A listed value that is no value
    of the key's type is left out, named in a warning.
    """
    reader = PARTITION_VALUE_READERS.get(key.logical_type, str)
    read, unread = set(), []
    for value in values:
        try:
            read.add(reader(value))
        except ValueError:
            unread.append(value)
    if unread:
        warn_left_out(f'{named}: partition values of {key.name} that are no {key.logical_type} are left out: {", ".join(unread)}')
    if not read:
        return key, None
    ordered = sorted(read)
    # The partitions listed today, not a declaration, so the next one written falls outside them.
    listed = frozenset({'low', 'high', 'top_values'})
    if key.logical_type == 'date':
        return dc.replace(key, low=ordered[0], high=ordered[-1], undeclared=listed), (ordered[-1] - ordered[0]).days + 1
    if key.logical_type == 'timestamp':
        return dc.replace(key, low=ordered[0], high=ordered[-1], undeclared=listed), None
    return dc.replace(key, values=tuple(ordered), undeclared=listed), len(ordered)


def projected(key: DeclaredColumn, parameters: Mapping[str, str]) -> tuple[DeclaredColumn, int] | None:
    """A partition key drawing what Athena's partition projection declares of it: an `enum`'s values, or an `integer` or
    `date` range whose ends are values of the key's type. None for any other projection, such as one ending at `NOW`.
    """
    if str(parameters.get('projection.enabled', '')).lower() != 'true':
        return None
    setting = f'projection.{key.name}.'
    kind = parameters.get(f'{setting}type')
    if kind == 'enum':
        values = [value.strip() for value in parameters.get(f'{setting}values', '').split(',') if value.strip()]
        try:
            read = sorted({PARTITION_VALUE_READERS.get(key.logical_type, str)(value) for value in values})
        except ValueError:
            return None
        return (dc.replace(key, values=tuple(read)), len(read)) if read else None
    ends = [end.strip() for end in parameters.get(f'{setting}range', '').split(',')]
    if len(ends) != 2 or (kind, key.logical_type) not in (('integer', 'integer'), ('date', 'date')):
        return None
    try:
        if key.logical_type == 'integer':
            low, high = int(ends[0]), int(ends[1])
            return dc.replace(key, low=low, high=high), high - low + 1
        first, last = day(ends[0]), day(ends[1])
    except ValueError:
        return None
    return dc.replace(key, low=first, high=last), (last - first).days + 1


def source_of(parameters: Mapping[str, str], serde: Mapping[str, Any]) -> dict[str, Any]:
    """The profile's `source` fields a table's parameters and SerDe record: its format, codec and delimited dialect."""
    library = (serde.get('SerializationLibrary') or '').rsplit('.', 1)[-1]
    serde_parameters = serde.get('Parameters') or {}
    found = CLASSIFICATIONS.get((parameters.get('classification') or '').lower()) or SERDE_FORMATS.get(library)
    delimiter = None
    if library == 'OpenCSVSerde':
        delimiter = serde_parameters.get('separatorChar', OPEN_CSV_DEFAULTS['separatorChar'])
    elif library == 'LazySimpleSerDe':
        delimiter = serde_parameters.get('field.delim') or serde_parameters.get('serialization.format') or LAZY_SIMPLE_DELIMITER
    if found is SourceFormat.CSV and delimiter == '\t':
        found = SourceFormat.TSV
    source: dict[str, Any] = {}
    if found is not None:
        source['format'] = found.value
    compression = (parameters.get('compressionType') or '').lower()
    if compression in published_enum('Compression'):
        source['compression'] = compression
    if found in DELIMITED:
        skipped = serde_parameters.get('skip.header.line.count') or parameters.get('skip.header.line.count') or '0'
        dialect: dict[str, Any] = {'delimiter': delimiter or ',', 'header': skipped.isdigit() and int(skipped) > 0}
        if library == 'OpenCSVSerde':
            dialect['quote'] = serde_parameters.get('quoteChar', OPEN_CSV_DEFAULTS['quoteChar'])
            dialect['escape'] = serde_parameters.get('escapeChar', OPEN_CSV_DEFAULTS['escapeChar'])
        elif serde_parameters.get('escape.delim'):
            dialect['escape'] = serde_parameters['escape.delim']
        null = serde_parameters.get('serialization.null.format', parameters.get('serialization.null.format'))
        if null is not None:
            dialect['null'] = null
        source['csv'] = {'writer': dialect}
    return source


def recorded_rows(parameters: Mapping[str, str]) -> int | None:
    """The row count a crawler or a statistics run recorded, as `recordCount` or `numRows`, where it is above 0."""
    for name in ('recordCount', 'numRows'):
        recorded = str(parameters.get(name, ''))
        if recorded.isdigit() and int(recorded) > 0:
            return int(recorded)
    return None


def typed(columns: list[DeclaredColumn], detected_entities: Mapping[str, Sequence[str]], named: str) -> list[DeclaredColumn]:
    """`columns`, each `detected_entities` names given the type of the first of its Glue entity types a shipped pack declares.

    `detected_entities` maps a column to its entity types, as Glue's `EntityDetector.classifyColumns` returns them.
    """
    if not detected_entities:
        return columns
    from doppeldata.profiling.detector_packs import BUILT_IN
    from doppeldata.profiling.detector_packs import load

    by_glue_type: dict[str, tuple[str, Any]] = {}
    for name in BUILT_IN:
        pack = load(name).pack
        for detector in pack.detectors:
            if detector.glue_type is not None:
                by_glue_type.setdefault(detector.glue_type, (pack.name, detector))
    names = {column.name for column in columns}
    unmatched = [column for column in detected_entities if column not in names]
    if unmatched:
        raise SourceError(
            f'the detected entities name {", ".join(unmatched)}, which {named} holds no column of; it holds {", ".join(sorted(names))}',
            role='detected-entities',
        )
    ignored: list[str] = []
    found = []
    for column in columns:
        entity_types = (
            [detected_entities[column.name]]
            if isinstance(detected_entities.get(column.name), str)
            else detected_entities.get(column.name, ())
        )
        known = [entity for entity in entity_types if entity in by_glue_type]
        ignored.extend(f'{entity} on {column.name}' for entity in entity_types if entity not in by_glue_type)
        if not known:
            found.append(column)
            continue
        pack_name, detector = by_glue_type[known[0]]
        semantic: dict[str, Any] = {'type': detector.type, 'pack': pack_name, 'detectors': ['glue'], 'sensitivity': detector.sensitivity}
        if column.logical_type == 'string' and detector.generator_hint:
            semantic['generator_hint'] = dict(detector.generator_hint)
        found.append(dc.replace(column, semantic=semantic))
    if ignored:
        warn_left_out(f'{named}: detected entities no shipped detector pack declares are left off: {", ".join(ignored)}')
    return found
