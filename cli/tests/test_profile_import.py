import datetime as dt
import json

import boto3
import polars as pl
from botocore.stub import Stubber
from doppeldata import Profile
from doppeldata.seed_profiles import glue_catalog
from doppeldata_cli.main import app
from typer.testing import CliRunner

# GetTable's answer for a crawled OpenCSVSerDe table, in the shapes botocore's Glue model gives.
GLUE_TABLE = {
    'Table': {
        'Name': 'orders',
        'DatabaseName': 'shop',
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {'classification': 'csv', 'skip.header.line.count': '1', 'recordCount': '400'},
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'order_id', 'Type': 'bigint'},
                {'Name': 'contact', 'Type': 'string'},
                {'Name': 'amount', 'Type': 'decimal(12,2)'},
                {'Name': 'payload', 'Type': 'binary'},
            ],
            'Location': 's3://example-bucket/shop/orders/',
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                'Parameters': {'separatorChar': ';', 'quoteChar': '"', 'escapeChar': '\\'},
            },
        },
        'PartitionKeys': [{'Name': 'region', 'Type': 'string'}, {'Name': 'order_date', 'Type': 'date'}],
    }
}
GLUE_PARTITIONS = {
    'Partitions': [
        {'Values': ['eu', '2025-03-01']},
        {'Values': ['us', '2025-03-02']},
        {'Values': ['__HIVE_DEFAULT_PARTITION__', '2025-03-02']},
    ]
}
CONTRACT = """\
apiVersion: v3.2.0
kind: DataContract
id: orders
version: 1.0.0
status: active
schema:
  - name: orders
    properties:
      - {name: order_id, logicalType: integer, logicalTypeOptions: {minimum: 1, maximum: 99999}, primaryKey: true, required: true}
      - {name: status, logicalType: string, required: true, enum: [{value: placed}, {value: shipped}]}
      - {name: email, logicalType: string, logicalTypeOptions: {format: email}}
      - {name: ordered_on, logicalType: date, required: true}
"""


def test_a_glue_csv_table_imports_its_dialect_header_and_listed_partitions_and_generates_them(tmp_path, monkeypatch):
    client = boto3.client('glue', region_name='eu-west-1', aws_access_key_id='stub', aws_secret_access_key='stub')
    stubber = Stubber(client)
    stubber.add_response('get_table', GLUE_TABLE, {'DatabaseName': 'shop', 'Name': 'orders'})
    expected = {'DatabaseName': 'shop', 'TableName': 'orders', 'ExcludeColumnSchema': True}
    stubber.add_response('get_partitions', GLUE_PARTITIONS, expected)
    monkeypatch.setattr(glue_catalog, 'glue_client', lambda region: client)
    entities = tmp_path / 'entities.json'
    entities.write_text(json.dumps({'contact': ['EMAIL']}))
    seed = tmp_path / 'orders.profile.yaml'
    runner = CliRunner()
    with stubber:
        imported = runner.invoke(
            app, ['profile', 'import', '--from', 'glue', 'shop.orders', '--detected-entities', str(entities), '--out', str(seed)]
        )
    assert imported.exit_code == 0, imported.output
    document = Profile.load(seed).document
    assert 'payload' not in [column['name'] for column in document['columns']]
    assert document['generated']['imported_from'] == 'glue:eu-west-1/shop/orders'
    assert document['source']['csv'] == {'writer': {'delimiter': ';', 'header': True, 'quote': '"', 'escape': '\\'}}
    out = tmp_path / 'orders'
    generated = runner.invoke(app, ['data', 'generate', str(seed), '--out', f'{out}/'])
    assert generated.exit_code == 0, generated.output
    directories = {path.relative_to(out).as_posix() for path in out.glob('*/*') if path.is_dir()}
    assert {directory.split('/')[0] for directory in directories} == {'region=eu', 'region=us'}
    assert {directory.split('/')[1] for directory in directories} <= {'order_date=2025-03-01', 'order_date=2025-03-02'}
    first = next(out.rglob('*.csv'))
    assert first.read_text().splitlines()[0] == 'order_id;contact;amount'
    rows = pl.read_csv(list(out.rglob('*.csv')), separator=';')
    assert rows.height == 400
    assert rows['contact'].drop_nulls().str.contains('@').all()
    checked = runner.invoke(app, ['data', 'check', str(seed), f'{out}/'])
    assert checked.exit_code == 0, checked.output


def test_an_odcs_contract_with_a_bounded_primary_key_imports_to_a_seed_drawing_distinct_keys(tmp_path):
    contract = tmp_path / 'orders.odcs.yaml'
    contract.write_text(CONTRACT)
    seed = tmp_path / 'orders.profile.yaml'
    runner = CliRunner()
    imported = runner.invoke(app, ['profile', 'import', '--from', 'odcs', str(contract), '--out', str(seed)])
    assert imported.exit_code == 0, imported.output
    out = tmp_path / 'orders.csv'
    generated = runner.invoke(app, ['data', 'generate', str(seed), '--rows', '1000', '--out', str(out)])
    assert generated.exit_code == 0, generated.output
    rows = pl.read_csv(out, try_parse_dates=True)
    assert rows['order_id'].to_list() == list(range(1, 1001))
    assert set(rows['status']) <= {'placed', 'shipped'}
    assert rows['ordered_on'].null_count() == 0
    assert dt.date(2025, 1, 1) <= rows['ordered_on'].min() and rows['ordered_on'].max() <= dt.date(2025, 12, 31)
    assert rows['email'].drop_nulls().str.contains('@').all()
