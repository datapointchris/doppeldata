"""Fails when the installed doppeldata wheel generates the determinism contract's fixture to a hash other than the pinned one,
or names a stream version other than the pinned one.

The test's constants are read from its source, so the pin has one home. Run it with the Python the built wheel is installed into.
Usage: python crates/check-pinned-values-through-wheel.py
"""

import json
import re
import sys
from pathlib import Path

from doppeldata import Profile
from doppeldata import _core
from ruamel.yaml import YAML

TESTS = Path(__file__).resolve().parent / 'core' / 'tests'
CONSTANT = re.compile(r'^const (?P<name>[A-Z_]+): [^=]+ = "?(?P<value>[0-9a-z_:]+)"?;$', re.MULTILINE)


def main() -> int:
    constants = {match['name']: match['value'] for match in CONSTANT.finditer((TESTS / 'determinism_contract.rs').read_text())}
    pinned = constants['PINNED_STREAM_VERSION']
    if int(pinned) != _core.STREAM_VERSION:
        print(
            f'check-pinned-values-through-wheel: the wheel names stream version {_core.STREAM_VERSION}, not the pinned {pinned}',
            file=sys.stderr,
        )
        return 1
    profile_json = json.dumps(Profile.load(TESTS / 'every-column-kind.profile.yaml').to_plain(), allow_nan=False)
    scenario_json = json.dumps(YAML(typ='safe').load(TESTS / 'every-column-kind.scenario.yaml'), allow_nan=False)
    cdc_json = json.dumps(YAML(typ='safe').load(TESTS / 'every-column-kind.cdc.scenario.yaml'), allow_nan=False)
    runs = {
        'PINNED_VALUES': None,
        'PINNED_SCENARIO_VALUES': (scenario_json, constants['SCENARIO_DIGEST']),
        'PINNED_CDC_VALUES': (cdc_json, constants['SCENARIO_DIGEST']),
    }
    for pin, scenario in runs.items():
        values = _core.determinism_values_digest(
            profile_json, constants['PROFILE_DIGEST'], int(constants['SEED']), int(constants['ROWS']), threads=0, scenario=scenario
        )
        if values != constants[pin]:
            print(
                f'check-pinned-values-through-wheel: the wheel generates {values}, not the pinned {pin} {constants[pin]}', file=sys.stderr
            )
            return 1
        print(f'check-pinned-values-through-wheel: doppeldata {_core.VERSION} generates the pinned {pin} {values}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
