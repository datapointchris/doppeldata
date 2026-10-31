import json

import jsonschema
from doppeldata.published_schemas import schema_names
from doppeldata_cli.main import app
from typer.testing import CliRunner


def test_a_caller_asking_schemas_show_for_each_listed_schema_gets_a_draft_2020_12_schema():
    for name in schema_names():
        result = CliRunner().invoke(app, ['schemas', 'show', name])
        assert result.exit_code == 0, (name, result.output)
        jsonschema.Draft202012Validator.check_schema(json.loads(result.output))
