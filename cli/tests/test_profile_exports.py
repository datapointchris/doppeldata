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
# Past 20,000 rows, two breaking rows' share rounds to 1 at four significant digits.
SOURCE_ROWS = 50_000


@pytest.fixture(scope='module')
def described(tmp_path_factory: pytest.TempPathFactory) -> dict[str, tuple[Profile, Path]]:
    """Each case's profile, and the directory holding the rows it describes as CSV, NDJSON and Parquet."""
    generated = tmp_path_factory.mktemp('orders')
    for suffix in ('csv', 'ndjson', 'parquet'):
        writer = {} if suffix == 'parquet' else {'compression': 'none'}
        sd.write(Profile.example('orders'), generated / f'rows.{suffix}', rows=ROWS, format=suffix, partition_by=[], writer=writer)
    # Fewer than k_min rows break the key, the type, the shape and the listed values, so the profile states each as held.
    source = tmp_path_factory.mktemp('source')
    ids = list(range(SOURCE_ROWS))
    ids[1], ids[3] = ids[0], ids[2]
    frame = pl.DataFrame(
        {
            'id': ids,
            'zip': ['N/A-X' if row in (7, 11) else f'{10000 + row:05d}' for row in range(SOURCE_ROWS)],
            'claim': ['legacy-x' if row in (5, 9) else f'CLM{row:06d}' for row in range(SOURCE_ROWS)],
            'status': ['rare' if row % 1250 == 0 else 'abc'[row % 3] for row in range(SOURCE_ROWS)],
        }
    )
    frame.write_csv(source / 'rows.csv')
    frame.write_ndjson(source / 'rows.ndjson')
    frame.write_parquet(source / 'rows.parquet')
    return {
        'the orders example, against rows generated from it': (Profile.example('orders'), generated),
        'a profile of rows fewer than k_min of which break a fact, against those rows': (sd.profile(source / 'rows.csv'), source),
        'a profile of a sample, against every row': (sd.profile(source / 'rows.parquet', sample=1000), source),
    }


def odcs(text: str, data: Path) -> None:
    schema = json.loads(ODCS_SCHEMA.read_text())
    jsonschema.validators.validator_for(schema)(schema).validate(YAML(typ='safe').load(text))


def record_schema(text: str, data: Path) -> None:
    schema = json.loads(text)
    validator = jsonschema.validators.validator_for(schema)
    validator.check_schema(schema)
    for line in (data / 'rows.ndjson').read_text().splitlines():
        validator(schema, format_checker=validator.FORMAT_CHECKER).validate(json.loads(line))


def table_schema(text: str, data: Path) -> None:
    (data / 'rows.schema.json').write_text(text)
    report = frictionless.validate('rows.csv', schema='rows.schema.json', basepath=str(data))
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
    module = exported_module(text, data, 'rows_pandera')
    model = next(value for value in vars(module).values() if isinstance(value, type) and issubclass(value, pa.DataFrameModel))
    model.validate(pl.read_parquet(data / 'rows.parquet'))


def dataframely_schema(text: str, data: Path) -> None:
    module = exported_module(text, data, 'rows_dataframely')
    schema = next(value for value in vars(module).values() if isinstance(value, type) and issubclass(value, dy.Schema))
    schema.validate(pl.read_parquet(data / 'rows.parquet'), cast=False)


# Each format but DQDL and the PySpark schemas, which only Spark reads, checked by the tool that reads it.
CHECKS: dict[str, Callable[[str, Path], None]] = {
    'odcs': odcs,
    'json-schema': record_schema,
    'table-schema': table_schema,
    'pandera': pandera_model,
    'dataframely': dataframely_schema,
}
CASES = (
    'the orders example, against rows generated from it',
    'a profile of rows fewer than k_min of which break a fact, against those rows',
    'a profile of a sample, against every row',
)


@pytest.mark.parametrize('export_format', CHECKS)
@pytest.mark.parametrize('case', CASES)
def test_each_export_accepts_the_rows_its_profile_describes(case, export_format, described):
    # A team validating real data reaches the profiled cases: rare breaks a profile writes as none, and a --sample profile.
    profile, data = described[case]
    CHECKS[export_format](sd.export(profile, export_format), data)
