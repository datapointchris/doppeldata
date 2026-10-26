import datetime as dt
import json
import re
from pathlib import Path

import doppeldata as sd

ORDERS = Path(__file__).parent / 'fixtures' / 'orders_schemas' / 'orders.schema.json'


def test_a_json_schema_seed_draws_inside_every_declared_constraint_and_a_default_where_none_is_declared(tmp_path):
    seed = sd.Profile.from_json_schema(ORDERS)
    out, again = tmp_path / 'orders.ndjson', tmp_path / 'again.ndjson'
    sd.write(seed, out, rows=1000, seed=3)
    sd.write(seed, again, rows=1000, seed=3)
    assert out.read_bytes() == again.read_bytes()
    # NDJSON leaves a null field out of its line.
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(rows) == 1000
    assert all(row['order_id'] >= 1 for row in rows)
    assert {row['status'] for row in rows} == {'pending', 'processing', 'shipped', 'cancelled'}
    assert {row.get('priority') for row in rows} <= {1, 2, 3, None}
    assert all('@' in row['customer_email'] for row in rows)
    assert all(re.fullmatch(r'[A-Z]{3}-[0-9]{4}', row['sku']) for row in rows)
    assert all(0 <= row['amount'] <= 10000 and round(row['amount'], 2) == row['amount'] for row in rows)
    days = [dt.date.fromisoformat(row['order_date']) for row in rows]
    assert dt.date(2025, 1, 1) <= min(days) and max(days) <= dt.date(2025, 12, 31)
    refs = [row['batch_ref'] for row in rows if 'batch_ref' in row]
    assert 0 < len(refs) < 1000
    assert all(re.fullmatch(r'[A-Za-z0-9]{8,16}', ref) for ref in refs)
    addresses = [row['shipping'] for row in rows if 'shipping' in row]
    assert all('city' in address for address in addresses)
    assert all(re.fullmatch(r'[0-9]{5}', address['postcode']) for address in addresses if 'postcode' in address)
    tags = [row['tags'] for row in rows if 'tags' in row]
    assert all(len(listed) <= 3 and all(8 <= len(tag) <= 12 for tag in listed) for listed in tags)
