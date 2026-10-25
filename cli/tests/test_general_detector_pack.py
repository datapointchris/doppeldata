import pyarrow as pa
from doppeldata import Profile
from doppeldata import _core
from doppeldata import generate
from doppeldata.profiling.detector_packs.general import PACK
from doppeldata.profiling.semantic_types import passes

ROWS = 20_000
INVALID_RATE = 0.003
TOLERANCE = 0.002


def test_every_general_pack_hint_generates_values_its_own_validator_passes():
    columns = []
    for index, detector in enumerate(PACK.detectors):
        column = {
            'name': f'{detector.type}_{index}',
            'physical_type': 'string',
            'semantic': {'type': detector.type, 'generator_hint': dict(detector.generator_hint)},
        }
        if 'check_digit' in detector.generator_hint:
            column['quality'] = {'invalid_rate': INVALID_RATE}
        columns.append(column)
    document = {
        'profile_version': _core.FORMAT_VERSION,
        'kind': 'table',
        'name': 'hints',
        'dataset': {'row_count': ROWS},
        'columns': columns,
    }
    table = pa.table(generate(Profile(document)))

    failures = []
    for detector, column in zip(PACK.detectors, columns, strict=True):
        assert detector.validate is not None
        values = table.column(column['name']).combine_chunks()
        rate = passes(detector.validate, detector.generator_hint.get('check_digit'), values, column['name']) / ROWS
        expected = 1 - column.get('quality', {}).get('invalid_rate', 0)
        if rate < expected - TOLERANCE:
            failures.append(f'{column["name"]} {dict(detector.generator_hint)} passed {rate:.4f}, expected {expected}')
    assert not failures, '\n'.join(failures)
