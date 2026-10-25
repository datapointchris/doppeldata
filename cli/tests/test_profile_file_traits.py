import pyarrow as pa
import pyarrow.compute as pc
import pytest
from doppeldata import Profile
from doppeldata import generate
from doppeldata_cli.main import app
from typer.testing import CliRunner

# A quoted comma, a stray quote, a short row, a long row, and an amount and a date that do not parse, which DuckDB's sniffer
# misreads.
MESSY_CSV = """id|name|amount|dt|flag
1|"Smith, J"|10.5|2024-01-03|true
2|Lee|11.25|2024-01-04|false
3|"bad "quote"|x|2024-01-05|true
4|Kim|12|2024-01-06
5|Ng|13.75|2024-01-07|true|extra
6|Ola|14|not-a-date|false
"""
# `addr` is null once, `zip` is in every object, and `apt` is missing from two of three.
MIXED_NDJSON = """{"id":1,"amt":10.5,"tags":["a","b"],"addr":{"zip":"27601","st":"NC"}}
{"id":2,"amt":"11.25","tags":[],"addr":{"zip":"27602"}}
{"id":"3","amt":null,"addr":null,"extra":true}
{"id":4,"amt":12,"tags":["c"],"addr":{"zip":"27603","st":"NC","apt":"2B"}}
"""


def profiled(tmp_path, name, text):
    (tmp_path / name).write_text(text)
    out = tmp_path / f'{name}.profile.yaml'
    result = CliRunner().invoke(app, ['profile', 'create', str(tmp_path / name), '--k-min', '1', '--out', str(out)])
    assert result.exit_code == 0, result.output
    return Profile.load(out)


def test_a_messy_csv_records_clevercsvs_dialect_where_duckdb_disagrees_and_its_malformed_rows_by_type(tmp_path):
    csv = profiled(tmp_path, 'messy.csv', MESSY_CSV).to_plain()['source']['csv']

    assert csv['delimiter'] == '|'
    assert csv['detector']['agreed'] is False
    assert csv['detector']['chosen'] == 'clevercsv'
    assert set(csv['malformed']['by_type']) == {'cast', 'missing_columns', 'too_many_columns', 'unquoted_value'}


def test_nested_ndjson_records_type_mix_and_presence_the_engine_generates_back(tmp_path):
    profile = profiled(tmp_path, 'mixed.ndjson', MIXED_NDJSON)
    columns = {column['name']: column for column in profile.to_plain()['columns']}

    assert columns['id']['type_mix'] == {'integer': 0.75, 'string': 0.25}
    assert columns['addr']['struct']['presence']['apt'] == pytest.approx(1 / 3, abs=1e-4)
    addr = pa.table(generate(profile, rows=4000)).column('addr').combine_chunks()
    objects = addr.filter(addr.is_valid())
    assert addr.null_count / len(addr) == pytest.approx(1 / 4, abs=0.03)
    assert objects.field('zip').null_count == 0
    assert pc.mean(objects.field('apt').is_valid().cast(pa.int8())).as_py() == pytest.approx(1 / 3, abs=0.03)
