"""Scenarios over generated data. They sit here because a check profiles with polars and pyarrow, which the CLI declares."""

import bisect
import json
import re
from pathlib import Path

import doppeldata as sd
import duckdb
import jsonschema
import pyarrow as pa
import pyarrow.compute as pc
from doppeldata.manifest import FAULTS_FILE_NAME
from doppeldata.published_schemas import schema_text

# Three engine blocks, so each perturbation draws across block boundaries.
ROWS = 150_000


def test_the_hot_key_example_gives_five_customers_its_share_and_a_check_verifies_it():
    orders, hot_key = sd.Profile.example('orders'), sd.Scenario.example('hot-key')
    table = pa.table(sd.generate(orders, rows=ROWS, scenario=hot_key))
    counts = sorted(pc.value_counts(table['customer_id']).field('counts').to_pylist(), reverse=True)
    assert abs(sum(counts[:5]) / table.num_rows - 0.6) <= 0.01

    report = sd.check(orders, table, scenario=hot_key)
    jsonschema.validate(report.to_plain(), json.loads(schema_text('check')))
    assert report.passed and report.verified
    assert {result.skipped_by for result in report.metrics if result.column == 'customer_id'} == {'hot_keys:customer_id:0'}


def test_a_strict_reader_rejects_exactly_the_lines_the_bad_csv_example_lists_and_a_check_verifies_its_rate(tmp_path: Path):
    orders, bad_csv = sd.Profile.example('orders'), sd.Scenario.example('bad-csv')
    sd.write(orders, f'{tmp_path}/', rows=ROWS, file_rows=ROWS // 3, scenario=bad_csv)
    faults = [json.loads(line) for line in (tmp_path / FAULTS_FILE_NAME).read_text().splitlines()]
    # CSV allows a newline inside quotes, so a strict reader takes an embedded_newline row whole.
    listed = {(fault['file'], fault['line']) for fault in faults if fault['line'] is not None and fault['kind'] != 'embedded_newline'}
    columns = {
        'order_id': 'BIGINT',
        'customer_id': 'BIGINT',
        'amount': 'DECIMAL(12,2)',
        'quantity': 'INTEGER',
        'status': 'VARCHAR',
        'ordered_at': 'TIMESTAMPTZ',
        'customer_email': 'VARCHAR',
        'ship_zip': 'VARCHAR',
    }
    rejected = set()
    for path in sorted(tmp_path.glob('part-*.csv')):
        with duckdb.connect() as connection:
            connection.execute(
                'create table orders as select * from read_csv($path, columns = $columns, header = true, '
                'auto_detect = false, strict_mode = true, store_rejects = true)',
                {'path': str(path), 'columns': columns},
            )
            starts = connection.execute('select min(line_byte_position) from reject_errors group by line').fetchall()
        # DuckDB's `line` counts records, so a reject is placed on its physical line by its byte.
        breaks = [match.start() for match in re.finditer(b'\n', path.read_bytes())]
        rejected |= {(path.name, bisect.bisect_left(breaks, at) + 1) for (at,) in starts}
    assert rejected == listed

    report = sd.check(orders, f'{tmp_path}/', scenario=bad_csv)
    assert report.passed and report.verified


def test_inserting_a_perturbation_leaves_the_rows_every_other_one_writes_unchanged():
    orders = sd.Profile.example('orders')
    others = [
        {'op': 'hot_keys', 'column': 'customer_id', 'keys': 5, 'share': 0.6},
        {'op': 'missingness', 'column': 'amount', 'mechanism': 'MAR', 'depends_on': 'quantity', 'rate': 0.2},
        {'op': 'out_of_range', 'column': 'quantity', 'rate': 0.01, 'values': [-1, 0]},
        {
            'op': 'late_arrival',
            'column': 'ordered_at',
            'rate': 0.05,
            'lag': {'family': 'lognormal', 'median': '2d'},
            'ingest': 'ingested_at',
        },
        {'op': 'categorical_shift', 'column': 'status', 'to': {'returned': 0.3}},
    ]
    inserted = {'op': 'broken_characters', 'column': 'customer_email', 'rate': 0.5}
    before = pa.table(sd.generate(orders, rows=ROWS, scenario=sd.Scenario({'scenario': 'before', 'perturbations': others})))
    after = pa.table(sd.generate(orders, rows=ROWS, scenario=sd.Scenario({'scenario': 'after', 'perturbations': [inserted, *others]})))
    assert before.column_names == after.column_names
    assert [name for name in before.column_names if not before[name].equals(after[name])] == ['customer_email']


def test_a_commit_order_merge_of_the_cdc_dms_stream_is_its_expected_table_and_a_file_order_one_misses_the_listed_faults(
    tmp_path: Path,
):
    cdc = sd.Scenario.example(
        'cdc-dms',
        period={'start': '2026-10-01T00:00:00Z', 'duration': '1d'},
        file_interval='10m',
        duplicate_delivery_rate=0.05,
        out_of_order_rate=0.05,
    )
    sd.write(sd.Profile.example('orders'), f'{tmp_path}/', rows=20_000, format='parquet', scenario=cdc)
    stream = tmp_path / 'sales' / 'orders'
    with duckdb.connect() as connection:
        # DMS writes no Op on a full load, so every LOAD row is an insert.
        connection.execute(
            f"create view stream as select 'I' as Op, * from read_parquet('{stream}/LOAD*.parquet', filename = true, "
            f"file_row_number = true) union all by name select * from read_parquet('{stream}/**/[0-9]*.parquet', "
            'union_by_name = true, filename = true, file_row_number = true)'
        )
        connection.execute(f"create view expected as select * from read_parquet('{tmp_path}/_expected/sales/orders/*.parquet')")

        def differing(order: str) -> set[int]:
            rows = connection.execute(
                'with merged as (select * exclude (Op, dms_commit_ts, filename, file_row_number) from stream '
                f"qualify row_number() over (partition by order_id order by {order}) = 1 and Op <> 'D') "
                'select order_id from (from merged except all from expected) '
                'union select order_id from (from expected except all from merged)'
            ).fetchall()
            return {key for (key,) in rows}

        assert differing('dms_commit_ts desc') == set()
        faults = [json.loads(line) for line in (tmp_path / FAULTS_FILE_NAME).read_text().splitlines()]
        assert {fault['kind'] for fault in faults} == {'duplicate_delivery', 'out_of_order'}
        listed = {(f'{tmp_path}/{fault["file"]}', fault['detail']['record']) for fault in faults}
        records = connection.execute('select order_id, filename, file_row_number from stream').fetchall()
        faulted = {key for key, file, record in records if (file, record) in listed}
        assert differing("filename not like '%/LOAD%' desc, parse_filename(filename) desc, file_row_number desc") == faulted
