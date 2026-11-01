import doppeldata as sd
import pytest

MIXED_COLUMNS = """\
kind: table
name: mixed
dataset: {rows: 20000}
columns:
  - name: integer_mix
    physical_type: int64
    numeric: {extremes: {low: 0, high: 5000}}
    type_mix_fractions: {integer: 0.75, string: 0.1, boolean: 0.1, timestamp: 0.05}
  - name: float_mix
    physical_type: double
    numeric: {extremes: {low: 0, high: 100}}
    type_mix_fractions: {decimal: 0.85, string: 0.1, boolean: 0.05}
  - name: decimal_mix
    physical_type: "decimal128(10, 2)"
    numeric: {extremes: {low: 0, high: 1000}}
    type_mix_fractions: {decimal: 0.9, date: 0.1}
  - name: boolean_mix
    physical_type: bool
    type_mix_fractions: {boolean: 0.9, integer: 0.1}
  - name: date_mix
    physical_type: date32
    temporal: {extremes: {low: "2024-01-01", high: "2024-12-31"}}
    type_mix_fractions: {date: 0.85, string: 0.1, timestamp: 0.05}
  - name: timestamp_mix
    physical_type: "timestamp[us]"
    temporal: {extremes: {low: "2024-01-01T00:00:00", high: "2024-12-31T00:00:00"}}
    type_mix_fractions: {timestamp: 0.9, date: 0.05, string: 0.05}
"""


@pytest.mark.parametrize('written_as', ['csv', 'ndjson'])
def test_text_the_engine_writes_profiles_back_as_the_type_mix_it_was_drawn_from(tmp_path, written_as):
    """events-json's export, and any column drawn as text: a profiler reads each type's share back, so no value of
    one type is written as text another reader takes for a different type."""
    path = tmp_path / 'mixed.profile.yaml'
    path.write_text(MIXED_COLUMNS)
    drawn = sd.Profile.load(path)
    sd.write(drawn, tmp_path / 'out', seed=1, format=written_as)
    profiled = {column['name']: column for column in sd.profile(str(tmp_path / 'out')).to_plain()['columns']}
    for column in drawn.to_plain()['columns']:
        read = profiled[column['name']].get('type_mix_fractions', {})
        assert read.keys() == column['type_mix_fractions'].keys(), column['name']
        for raw, share in column['type_mix_fractions'].items():
            assert read[raw] == pytest.approx(share, abs=0.02), (column['name'], raw, read)
