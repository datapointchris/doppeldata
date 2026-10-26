import importlib.util
import json
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

import dataframely as dy
import doppeldata as sd
import frictionless
import jsonschema
import pandera.polars as pa
import polars as pl
import pytest
from doppeldata import Profile
from ruamel.yaml import YAML

# The ODCS v3.2.0 JSON Schema, from bitol-io/open-data-contract-standard, under Apache-2.0.
ODCS_SCHEMA = Path(__file__).parent / 'fixtures' / 'odcs-json-schema-v3.2.0.json'
ROWS = 500


@pytest.fixture(scope='module')
def orders_data(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The orders example written as CSV, NDJSON and Parquet, one file each."""
    out = tmp_path_factory.mktemp('orders')
    for suffix in ('csv', 'ndjson', 'parquet'):
        writer = {} if suffix == 'parquet' else {'compression': 'none'}
        sd.write(Profile.example('orders'), out / f'orders.{suffix}', rows=ROWS, format=suffix, partition_by=[], writer=writer)
    return out


def odcs(text: str, data: Path) -> None:
    schema = json.loads(ODCS_SCHEMA.read_text())
    jsonschema.validators.validator_for(schema)(schema).validate(YAML(typ='safe').load(text))


def record_schema(text: str, data: Path) -> None:
    schema = json.loads(text)
    validator = jsonschema.validators.validator_for(schema)
    validator.check_schema(schema)
    for line in (data / 'orders.ndjson').read_text().splitlines():
        validator(schema, format_checker=validator.FORMAT_CHECKER).validate(json.loads(line))


def table_schema(text: str, data: Path) -> None:
    (data / 'orders.schema.json').write_text(text)
    report = frictionless.validate('orders.csv', schema='orders.schema.json', basepath=str(data))
    assert report.valid, report.flatten(['rowNumber', 'fieldName', 'type', 'note'])


def exported_module(text: str, data: Path, name: str) -> ModuleType:
    path = data / f'{name}.py'
    path.write_text(text)
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def pandera_model(text: str, data: Path) -> None:
    module = exported_module(text, data, 'orders_pandera')
    model = next(value for value in vars(module).values() if isinstance(value, type) and issubclass(value, pa.DataFrameModel))
    model.validate(pl.read_parquet(data / 'orders.parquet'))


def dataframely_schema(text: str, data: Path) -> None:
    module = exported_module(text, data, 'orders_dataframely')
    schema = next(value for value in vars(module).values() if isinstance(value, type) and issubclass(value, dy.Schema))
    schema.validate(pl.read_parquet(data / 'orders.parquet'), cast=False)


# Each format but DQDL and the PySpark schemas, which only Spark reads, checked by the tool that reads it.
CHECKS: dict[str, Callable[[str, Path], None]] = {
    'odcs': odcs,
    'json-schema': record_schema,
    'table-schema': table_schema,
    'pandera': pandera_model,
    'dataframely': dataframely_schema,
}


@pytest.mark.parametrize('export_format', CHECKS)
def test_each_export_of_the_orders_example_accepts_the_data_generated_from_it(export_format, orders_data):
    CHECKS[export_format](sd.export(Profile.example('orders'), export_format), orders_data)
