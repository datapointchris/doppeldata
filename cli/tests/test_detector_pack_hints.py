import pyarrow as pa
import pytest
from doppeldata import Profile
from doppeldata import generate
from doppeldata.profiling.detector_packs import BUILT_IN
from doppeldata.profiling.detector_packs import load
from doppeldata.profiling.semantic_types import bound
from doppeldata.profiling.semantic_types import passes

ROWS = 20_000
INVALID_RATE = 0.003
TOLERANCE = 0.002


@pytest.mark.parametrize('pack_name', sorted(BUILT_IN))
def test_every_shipped_pack_hint_generates_values_its_own_validator_passes(pack_name):
    """A generated column typed by a shipped pack keeps its type when `data check` profiles it again."""
    detectors = [detector for detector in load(pack_name).pack.detectors if detector.generator_hint]
    columns = []
    for index, detector in enumerate(detectors):
        column = {
            'name': f'{detector.type}_{index}',
            'physical_type': 'string',
            'semantic': {'type': detector.type, 'generator_hint': dict(detector.generator_hint)},
        }
        if 'check_digit' in detector.generator_hint:
            column['quality'] = {'invalid_fraction': INVALID_RATE}
        columns.append(column)
    document = {
        'kind': 'table',
        'name': 'hints',
        'dataset': {'rows': ROWS},
        'columns': columns,
    }
    table = pa.table(generate(Profile(document)))

    failures = []
    for detector, column in zip(detectors, columns, strict=True):
        assert detector.validate is not None
        values = table.column(column['name']).combine_chunks()
        # A check profiles generated data with no code list, so a detector reading one validates without it.
        rate = passes(bound(detector, {}), detector.generator_hint.get('check_digit'), values, column['name']) / ROWS
        expected = 1 - column.get('quality', {}).get('invalid_fraction', 0)
        if rate < expected - TOLERANCE:
            failures.append(f'{column["name"]} {dict(detector.generator_hint)} passed {rate:.4f}, expected {expected}')
    assert not failures, '\n'.join(failures)
