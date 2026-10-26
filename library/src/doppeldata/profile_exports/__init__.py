"""A profile's contract written in a format other tools validate data with. `export` documents each format."""

import os
from collections.abc import Callable
from pathlib import Path

from doppeldata.embedded_profiles import data_file_metadata
from doppeldata.errors import CheckFailed
from doppeldata.privacy_policy.file_check import ProfileFileCheck
from doppeldata.privacy_policy.file_check import table_check
from doppeldata.profile_exports.contract_columns import ContractDocument
from doppeldata.profile_exports.contract_columns import contract_document
from doppeldata.profile_exports.dataframely_schemas import dataframely_module
from doppeldata.profile_exports.dqdl_rulesets import dqdl_rulesets
from doppeldata.profile_exports.generated_examples import generated_examples
from doppeldata.profile_exports.json_schemas import record_schema
from doppeldata.profile_exports.odcs_contracts import odcs_contract
from doppeldata.profile_exports.pandera_models import pandera_module
from doppeldata.profile_exports.pyspark_schemas import pyspark_module
from doppeldata.profile_exports.table_schemas import table_schema
from doppeldata.profile_format import BUNDLE_INDEX
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import Profile
from doppeldata.profile_format import read_document


def odcs(document: ContractDocument) -> str:
    examples = {table.name: generated_examples(table.profile) for table in document.tables if table.profile is not None}
    return odcs_contract(document, examples)


def json_schema(document: ContractDocument) -> str:
    if document.bundle:
        tables = ', '.join(table.name for table in document.tables)
        raise ValueError(f'the record schema describes one table, and {document.name} holds {tables}; name one with table=, or --table')
    return record_schema(document.tables[0])


EXPORTERS: dict[str, Callable[[ContractDocument], str]] = {
    'odcs': odcs,
    'table-schema': table_schema,
    'json-schema': json_schema,
    'dqdl': dqdl_rulesets,
    'pandera': pandera_module,
    'dataframely': dataframely_module,
    'pyspark-schemas': pyspark_module,
}
EXPORT_FORMATS = tuple(EXPORTERS)


def export(profile: Profile | Bundle | str | os.PathLike[str], format: str, *, table: str | None = None) -> str:
    """The contract `profile` states, as the text of `format`. `profile` is a table profile or a bundle, its file, or an
    example's name. `table` exports one table of a bundle alone.

    - `odcs`: an Open Data Contract Standard v3.2.0 contract, a bundle's tables each a schema object. Each property's
      `examples` are generated from the profile with seed 0, never read from data.
    - `table-schema`: a Frictionless Table Schema v2, and for a bundle a Data Package v2 with its foreign keys.
    - `json-schema`: the record schema, a JSON Schema 2020-12 of one record. A bundle takes `table`.
    - `dqdl`: an AWS Glue Data Quality ruleset of each table, with each share's threshold three standard errors below it.
    - `pandera`: a module of a `pandera.polars.DataFrameModel` of each table.
    - `dataframely`: a module of a `dataframely.Schema` of each table.
    - `pyspark-schemas`: a module of each table's `StructType` and `pandera.pyspark.DataFrameModel`.

    Every format but DQDL carries a range, listed values, lengths or a pattern only where the profile states it of every
    value, and names in its own description what it left out. A seed exports only what its schema declared.

    Raises CheckFailed, as `doppeldata profile check` exits 3, where a profile made from data fails its schema or privacy
    policy, because an export never carries a literal the policy did not pass. ValueError names an unknown format or table.
    """
    exporter = EXPORTERS.get(format)
    if exporter is None:
        raise ValueError(f'{format!r} is not an export format; choose from {", ".join(EXPORT_FORMATS)}')
    found = loaded(profile)
    if table is not None:
        found = one_table(found, table)
    checked(found)
    return exporter(contract_document(found))


def loaded(source: Profile | Bundle | str | os.PathLike[str]) -> Profile | Bundle:
    """The profile or bundle `source` names: itself, a file or directory holding one, or an example's name."""
    if isinstance(source, Profile | Bundle):
        return source
    path = Path(source)
    if (path / BUNDLE_INDEX).is_file() or (path.is_file() and data_file_metadata(path) is None and bundle_file(path)):
        return Bundle.load(path)
    from doppeldata.short_profiles import file_profile

    return file_profile(str(source))


def bundle_file(path: Path) -> bool:
    document = read_document(path)
    return isinstance(document, dict) and document.get('kind') == 'bundle'


def one_table(found: Profile | Bundle, table: str) -> Profile:
    if isinstance(found, Bundle):
        if table not in found.tables:
            raise ValueError(f'{found.document["name"]} holds no table {table}; it holds {", ".join(found.tables)}')
        return found.tables[table]
    if found.document['name'] != table:
        raise ValueError(f'the profile is of {found.document["name"]}, not {table}; table= names one table of a bundle')
    return found


def checked(found: Profile | Bundle) -> None:
    """Raise CheckFailed where a table made from data fails `profile check`. A seed was written under no policy."""
    tables = found.tables if isinstance(found, Bundle) else {found.document['name']: found}
    at = ('tables',) if isinstance(found, Bundle) else ()
    checks = [
        table_check(profile.to_plain(), (*at, name) if at else ())
        for name, profile in tables.items()
        if (profile.document.get('generated') or {}).get('engine') != 'import'
    ]
    if any(check.violations for check in checks):
        raise CheckFailed(ProfileFileCheck.of(str(found.document['name']), checks))
