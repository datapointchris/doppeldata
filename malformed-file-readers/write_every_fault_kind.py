"""Write the orders example under the bad-csv example scenario, and once for each fault kind alone in a format that holds it.

    write_every_fault_kind.py OUT

Each dataset lands in OUT/<format>/<kind>/, bad-csv in OUT/csv/bad-csv/, each beside its `_faults.jsonl`. OUT/columns.json
names each column's physical type, which the readers declare a schema from.
"""

import json
import sys
from pathlib import Path
from typing import Any

import doppeldata as sd
from doppeldata import _core
from doppeldata.scenario_format import fault_kinds

ROWS = 40_000
FILE_ROWS = 10_000
ROW_RATE = 0.02
# Each row it breaks is over 2 MiB.
LINE_TOO_LONG_RATE = 0.0005
FILES_SHARE = 0.5
# Each kind is written in each of these the engine says holds it.
FORMATS = ('csv', 'ndjson', 'parquet')


def kinds_in(format: str) -> list[str]:
    return [kind for kind in fault_kinds() if _core.fault_kind_facts(kind, format)[1]]


def alone(kind: str, orders: sd.Profile) -> sd.Scenario:
    breaks_files = _core.fault_kind_facts(kind, 'csv')[0]
    share = {'files_fraction': FILES_SHARE} if breaks_files else {'fraction': LINE_TOO_LONG_RATE if kind == 'line_too_long' else ROW_RATE}
    patch = {'patch': accented_status(orders)} if kind == 'latin1_file' else {}
    return sd.Scenario({'scenario': f'only-{kind}', **patch, 'perturbations': [{'op': 'malformed_rows', 'kinds': [kind], **share}]})


def accented_status(orders: sd.Profile) -> dict[str, Any]:
    """A patch writing `returned` as `retourné`. orders holds only ASCII, which windows-1252 writes as UTF-8 does."""
    status = next(column for column in orders.document['columns'] if column['name'] == 'status')
    values = [
        {**value, 'value': 'retourné' if value['value'] == 'returned' else value['value']} for value in status['string']['top_values']
    ]
    return {'columns': {'status': {'string': {'top_values': values}}}}


def main(out: str) -> int:
    root = Path(out)
    orders = sd.Profile.example('orders')
    sd.write(orders, f'{root / "csv" / "bad-csv"}/', rows=ROWS, file_rows=FILE_ROWS, scenario=sd.Scenario.example('bad-csv'))
    for format in FORMATS:
        for kind in kinds_in(format):
            sd.write(orders, f'{root / format / kind}/', rows=ROWS, file_rows=FILE_ROWS, format=format, scenario=alone(kind, orders))
    columns = [{'name': column['name'], 'physical_type': column['physical_type']} for column in orders.document['columns']]
    (root / 'columns.json').write_text(json.dumps(columns))
    return 0


if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
