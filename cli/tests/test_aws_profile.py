import json

import boto3
from doppeldata_cli.main import app
from moto.server import ThreadedMotoServer
from typer.testing import CliRunner


def test_aws_profile_gives_an_s3_write_its_credentials_and_a_glue_import_its_region(tmp_path, monkeypatch):
    # Only the analytics profile holds credentials, and only its region holds the Glue table.
    (tmp_path / 'config').write_text('[default]\nregion = us-east-1\n[profile analytics]\nregion = eu-west-1\n')
    (tmp_path / 'credentials').write_text('[analytics]\naws_access_key_id = testing\naws_secret_access_key = testing\n')
    server = ThreadedMotoServer(ip_address='127.0.0.1', port=0, verbose=False)
    server.start()
    try:
        host, port = server.get_host_and_port()
        for variable in (
            'AWS_PROFILE',
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY',
            'AWS_SESSION_TOKEN',
            'AWS_REGION',
            'AWS_DEFAULT_REGION',
        ):
            monkeypatch.delenv(variable, raising=False)
        monkeypatch.setenv('AWS_CONFIG_FILE', str(tmp_path / 'config'))
        monkeypatch.setenv('AWS_SHARED_CREDENTIALS_FILE', str(tmp_path / 'credentials'))
        monkeypatch.setenv('AWS_ENDPOINT_URL', f'http://{host}:{port}')
        session = boto3.Session(profile_name='analytics')
        session.client('s3').create_bucket(Bucket='bucket', CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})
        glue = session.client('glue')
        glue.create_database(DatabaseInput={'Name': 'sales'})
        columns = [{'Name': 'order_id', 'Type': 'bigint'}, {'Name': 'email', 'Type': 'string'}]
        glue.create_table(DatabaseName='sales', TableInput={'Name': 'orders', 'StorageDescriptor': {'Columns': columns}})

        write = ['data', 'generate', 'orders', '--rows', '100', '--out', 's3://bucket/orders/', '--json']
        assert CliRunner().invoke(app, write).exit_code == 1
        written = CliRunner().invoke(app, [*write, '--aws-profile', 'analytics'])
        assert written.exit_code == 0, written.output
        assert session.client('s3').head_object(Bucket='bucket', Key='orders/_manifest.json')

        imported = CliRunner().invoke(app, ['profile', 'import', '--from', 'glue', 'sales.orders', '--aws-profile', 'analytics', '--json'])
        assert imported.exit_code == 0, imported.output
        assert [column['name'] for column in json.loads(imported.stdout)['columns']] == ['order_id', 'email']
    finally:
        server.stop()
