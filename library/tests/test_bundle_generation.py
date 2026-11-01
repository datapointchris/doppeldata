import collections
import csv

import doppeldata as sd


def table_rows(root, table):
    """Every row a bundle write left under `root/table/`, read through CSV, because the library's tests install no Arrow reader."""
    rows = []
    for part in sorted((root / table).glob('part-*.csv')):
        with part.open(newline='') as handle:
            rows += list(csv.DictReader(handle))
    return rows


def test_the_orders_customers_bundle_draws_each_order_s_customer_at_its_orphan_rate_and_fanout(tmp_path):
    sd.write(sd.Bundle.example('orders-customers'), tmp_path, scale=0.1, seed=0, format='csv')
    customers = {row['customer_id'] for row in table_rows(tmp_path, 'customers')}
    named = [row['customer_id'] for row in table_rows(tmp_path, 'orders') if row['customer_id'] != '']
    matched = [customer for customer in named if customer in customers]
    assert abs(1 - len(matched) / len(named) - 0.002) < 0.001
    per_customer = collections.Counter(matched)
    counts = sorted(per_customer.get(customer, 0) for customer in customers)
    stated = {0.5: 4, 0.95: 45, 0.99: 160}
    assert {level: counts[round(level * (len(counts) - 1))] for level in stated} == stated
    # At scale 0.1 the stated top10 of 40,000 customers is the busiest one of 4,000.
    assert abs(counts[-1] / len(matched) - 0.003) < 0.0003


def test_regenerate_writes_every_table_of_a_bundle_again_from_its_manifest_and_its_scenario(tmp_path):
    hot_orders = sd.Scenario(sd.Scenario.example('hot-key').to_plain() | {'table': 'orders'})
    bundle = sd.Bundle.example('orders-customers')
    written = sd.write(bundle, f'{tmp_path}/shop/', scale=0.05, seed=3, scenario=hot_orders)
    again = sd.regenerate(bundle, f'{tmp_path}/shop/_manifest.json', f'{tmp_path}/again/')
    assert again.dataset_sha256 == written.dataset_sha256
    assert again.document['scenario'] == written.document['scenario']
    sha256s = {name: [file['sha256'] for file in table.files] for name, table in written.tables.items()}
    assert {name: [file['sha256'] for file in table.files] for name, table in again.tables.items()} == sha256s
