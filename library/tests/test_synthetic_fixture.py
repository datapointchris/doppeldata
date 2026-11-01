import re

import pytest

pytest.importorskip('polars')
pytest_plugins = ['pytester']

TESTS = """
import hashlib


def sha256_of(frame):
    return hashlib.sha256(frame.write_csv().encode()).hexdigest()


def test_first(synthetic):
    print('first', sha256_of(synthetic(['name', 'email', 'city'], rows=50)))


def test_second(synthetic):
    print('second', sha256_of(synthetic(['name', 'email', 'city'], rows=50)))
    assert False
"""


def printed_sha256s(result: pytest.RunResult) -> dict[str, str]:
    return dict(re.findall(r'(first|second) ([0-9a-f]{64})', result.stdout.str()))


def test_a_synthetic_table_is_the_same_every_run_differs_by_test_and_seed_and_its_failure_reruns_from_the_printed_seed(pytester):
    pytester.makepyfile(TESTS)
    run = printed_sha256s(pytester.runpytest('-s', '-p', 'no:cacheprovider'))
    failed = pytester.runpytest('-s', '-p', 'no:cacheprovider')
    assert printed_sha256s(failed) == run
    assert run['first'] != run['second']
    assert printed_sha256s(pytester.runpytest('-s', '-p', 'no:cacheprovider', '--doppeldata-seed', '9'))['first'] != run['first']
    seed = re.search(r'rerun with --doppeldata-seed (\d+)', failed.stdout.str()).group(1)
    rerun = pytester.runpytest('-s', '-p', 'no:cacheprovider', '--doppeldata-seed', seed, '-k', 'second')
    assert printed_sha256s(rerun)['second'] == run['second']
