from doppeldata.profile import schema_text
from doppeldata_cli.main import app
from typer.testing import CliRunner


def test_schema_show_profile_prints_the_schema_profiles_are_validated_against():
    result = CliRunner().invoke(app, ['schema', 'show', 'profile'])
    assert result.exit_code == 0
    assert result.output == schema_text()
