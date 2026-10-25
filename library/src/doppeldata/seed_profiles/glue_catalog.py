"""A seed from a table in the AWS Glue Data Catalog, read through `glue:GetTable` and `glue:GetPartitions` with boto3.

boto3 is imported only here, and declared nowhere. The seed records the table's region, database and name, and never
its ARN, which holds the account id.
"""

import contextlib
import dataclasses as dc
import datetime as dt
import importlib
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

from doppeldata.errors import PackagesMissing
from doppeldata.errors import SourceError
from doppeldata.errors import uninstalled
from doppeldata.profile_format import Profile
from doppeldata.seed_profiles.declared_columns import DeclaredColumn
from doppeldata.seed_profiles.declared_columns import warn_left_out
from doppeldata.seed_profiles.declared_tables import DeclaredTable
from doppeldata.seed_profiles.declared_tables import PartitionKey
from doppeldata.seed_profiles.hive_types import hive_column

# A table's `classification`, as a crawler writes it, and the source format it names. ORC and Avro are left out,
# because doppeldata writes neither.
CLASSIFICATIONS = {'csv': 'csv', 'tsv': 'tsv', 'parquet': 'parquet', 'json': 'ndjson'}
# A SerDe's class, where the table names no classification, and the format it reads.
SERDE_FORMATS = {'OpenCSVSerde': 'csv', 'LazySimpleSerDe': 'csv', 'ParquetHiveSerDe': 'parquet', 'JsonSerDe': 'ndjson'}
COMPRESSIONS = ('none', 'gzip', 'bzip2', 'zstd', 'snappy', 'lz4', 'lzo')
# OpenCSVSerde's own dialect, where its parameters name none.
OPEN_CSV_DEFAULTS = {'separatorChar': ',', 'quoteChar': '"', 'escapeChar': '\\'}
# LazySimpleSerDe's delimiter where neither `field.delim` nor `serialization.format` names one.
LAZY_SIMPLE_DELIMITER = '\x01'
# What Hive writes for a partition whose key is null, which is no value of the key.
NULL_PARTITION = '__HIVE_DEFAULT_PARTITION__'
SHIPPED_PACKS = ('general', 'healthcare')


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
        partitions: list[list[str]] = []
        if described.get('PartitionKeys'):
            pages = client.get_paginator('get_partitions').paginate(DatabaseName=database, TableName=table, ExcludeColumnSchema=True)
            partitions = [partition['Values'] for page in pages for partition in page['Partitions']]
    return catalog_table(described, partitions, named, detected_entities or {}).profile()


def glue_client(region: str | None) -> Any:
    """A Glue client of the AWS environment's, in `region` where it is given."""
    boto3 = importlib.import_module('boto3')
    exceptions = importlib.import_module('botocore.exceptions')
    from doppeldata.s3_access import S3_CLIENT_CONFIG

    try:
        return boto3.Session().client('glue', region_name=region, config=S3_CLIENT_CONFIG)
    except exceptions.NoRegionError as error:
        raise SourceError('the Glue Data Catalog is regional; give region=, --region, or set AWS_REGION', role='schema') from error
    except exceptions.ProfileNotFound as error:
        raise SourceError(f'{error}; set AWS_PROFILE to a profile the AWS config holds', role='schema') from error


@contextlib.contextmanager
def catalog_errors(named: str, endpoint: str) -> Iterator[None]:
    """Each boto3 failure reading `named` as a SourceError: exit 2 for a table the catalog lacks, 4 when no answer came."""
    from doppeldata.s3_access import CREDENTIAL_ADVICE
    from doppeldata.s3_access import CREDENTIAL_ERRORS
    from doppeldata.s3_access import S3_ATTEMPTS
    from doppeldata.s3_access import UNANSWERED_ERRORS

    exceptions = importlib.import_module('botocore.exceptions')
    try:
        yield
    except exceptions.ClientError as error:
        reported = error.response.get('Error', {})
        status = error.response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
        answer = f'{reported.get("Code")}, {reported.get("Message")}'
        if status == 429 or status >= 500 or reported.get('Code') == 'ThrottlingException':
            raise SourceError(f'no answer from {endpoint}: Glue answered {answer} on every attempt', exit_code=4, role='schema') from error
        if reported.get('Code') == 'EntityNotFoundException':
            raise SourceError(f'{named} names no table the catalog holds: {answer}', exit_code=2, role='schema') from error
        raise SourceError(f'{named} could not be read: {endpoint} answered {answer}', exit_code=1, role='schema') from error
    except CREDENTIAL_ERRORS as error:
        raise SourceError(f'{named} could not be read: {error}; {CREDENTIAL_ADVICE}', exit_code=1, role='schema') from error
    except UNANSWERED_ERRORS as error:
        raise SourceError(f'no answer from {endpoint}: {error}, {S3_ATTEMPTS} attempts', exit_code=4, role='schema') from error


def catalog_table(
    described: Mapping[str, Any], partitions: list[list[str]], named: str, detected_entities: Mapping[str, Sequence[str]]
) -> DeclaredTable:
    """The table GetTable's `Table` describes, each partition key drawing the values `partitions` list for it."""
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
    partition_keys = []
    for index, key in enumerate(described.get('PartitionKeys') or []):
        declared = hive_column(key['Name'], key['Type'], required=True)
        if declared is None:
            unread.append(f'{key["Name"]} ({key["Type"]})')
            continue
        values = sorted({values[index] for values in partitions if len(values) > index and values[index] != NULL_PARTITION})
        declared = listing(declared, values)
        columns.append(declared)
        partition_keys.append(PartitionKey(declared.name, declared.arrow_type(), drawn_count(declared, values)))
    if unread:
        warn_left_out(f'{named}: columns of types the engine draws nothing of are left out: {", ".join(unread)}')
    columns = typed(columns, detected_entities, named)
    return DeclaredTable(
        name=described['Name'],
        columns=tuple(columns),
        imported_from=named,
        partition_keys=tuple(partition_keys),
        source=source_of(parameters, serde),
        row_count=recorded_rows(parameters),
    )


def drawn_count(key: DeclaredColumn, values: list[str]) -> int | None:
    """How many values a partition key draws: each listed one, a date key every day from the least to the most, and None
    for a timestamp key or a table the catalog lists no partition of.
    """
    if not values or key.logical_type == 'timestamp':
        return None
    if key.logical_type == 'date':
        return (dt.date.fromisoformat(values[-1]) - dt.date.fromisoformat(values[0])).days + 1
    return len(key.values) or len(values)


def listing(key: DeclaredColumn, values: list[str]) -> DeclaredColumn:
    """A partition key drawing only the values the catalog lists, but a date or timestamp key every value between the
    least and the most, because a profile's temporal column holds no list of values.
    """
    if not values:
        return key
    if key.logical_type in ('date', 'timestamp'):
        return dc.replace(key, low=values[0], high=values[-1])
    if key.logical_type == 'integer':
        return dc.replace(key, values=tuple(sorted({int(value) for value in values if value.lstrip('+-').isdigit()})))
    if key.logical_type == 'number':
        return dc.replace(key, values=tuple(sorted({float(value) for value in values})))
    return dc.replace(key, values=tuple(values))


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
    if found == 'csv' and delimiter == '\t':
        found = 'tsv'
    source: dict[str, Any] = {}
    if found is not None:
        source['format'] = found
    compression = (parameters.get('compressionType') or '').lower()
    if compression in COMPRESSIONS:
        source['compression'] = compression
    if found in ('csv', 'tsv'):
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
        source['csv'] = dialect
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
    from doppeldata.profiling.detector_packs import load

    by_glue_type: dict[str, tuple[str, Any]] = {}
    for name in SHIPPED_PACKS:
        pack = load(name).pack
        for detector in pack.detectors:
            if detector.glue_type is not None:
                by_glue_type.setdefault(detector.glue_type, (pack.name, detector))
    names = {column.name for column in columns}
    ignored = [f'{column} (not a column)' for column in detected_entities if column not in names]
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
