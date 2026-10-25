#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = ["numpy==2.5.4", "pyarrow==26.0.0"]
# ///
"""The control arm: the eight-columns table vectorized in numpy, one pyarrow table per file, on a pool of `--workers` threads.

Each file is ZSTD-1 Parquet holding one row group, as the engine arm writes. Prints one Markdown table row per arm.
"""

import argparse
import os
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ALPHANUMERIC = np.frombuffer(b'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789', dtype=np.uint8)
CATEGORIES = np.array([f'category_{i:02}' for i in range(50)])
ZIPF_RANKS = np.arange(1, 51, dtype=np.float64) ** -1.1
ZIPF_WEIGHTS = ZIPF_RANKS / ZIPF_RANKS.sum()
YEAR_2025_US = 1_735_689_600_000_000
YEAR_US = 365 * 86_400 * 1_000_000


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=10_000_000)
    parser.add_argument('--file-rows', type=int, default=1_000_000)
    parser.add_argument('--workers', type=int, nargs='+', default=[1, os.process_cpu_count() or 1])
    parser.add_argument('--runs', type=int, default=5, help='Timed runs per arm, after one untimed run.')
    parser.add_argument('--scratch', type=Path, help='Where the Parquet is written. Defaults to the system temp directory.')
    args = parser.parse_args()
    if args.rows % args.file_rows:
        parser.error(f'--rows {args.rows} is not a whole number of --file-rows {args.file_rows}')
    return args


def rate_range(rates: list[float]) -> str:
    return f'{min(rates) / 1e6:.3g}M to {max(rates) / 1e6:.3g}M'


def row(*cells: object, load_before: float) -> None:
    """One table row, ending with the one-minute load average before the arm's first run and after its last."""
    print('| ' + ' | '.join(str(cell) for cell in (*cells, f'{load_before:.1f} to {os.getloadavg()[0]:.1f}')) + ' |', flush=True)


def file_table(file: int, file_rows: int) -> pa.Table:
    rng = np.random.default_rng([42, file])
    start = file * file_rows
    code = ALPHANUMERIC[rng.integers(0, len(ALPHANUMERIC), size=(file_rows, 10))].view('S10').ravel()
    return pa.table(
        {
            'id': pa.array(np.arange(start, start + file_rows, dtype=np.int64)),
            'amount': pa.array(rng.lognormal(3.0, 1.0, file_rows)),
            'qty': pa.array(rng.poisson(4.0, file_rows).astype(np.int64)),
            'category': pa.array(CATEGORIES[rng.choice(len(CATEGORIES), size=file_rows, p=ZIPF_WEIGHTS)]).cast(pa.string()),
            'ts': pa.array(YEAR_2025_US + rng.integers(0, YEAR_US, file_rows), type=pa.timestamp('us')),
            'flag': pa.array(rng.random(file_rows) < 0.3),
            'score': pa.array(rng.normal(0.0, 1.0, file_rows), mask=rng.random(file_rows) < 0.05),
            'code': pa.array(code).cast(pa.string()),
        }
    )


def run_seconds(args: argparse.Namespace, workers: int, out: Path | None) -> tuple[float, int]:
    """Seconds to make every file's table, writing each into `out` when given, then the bytes written."""

    def one(file: int) -> None:
        table = file_table(file, args.file_rows)
        if out:
            pq.write_table(table, out / f'part-{file:05}.parquet', compression='zstd', compression_level=1, row_group_size=args.file_rows)

    if out:
        out.mkdir()
    start = time.perf_counter()
    with ThreadPoolExecutor(workers) as pool:
        list(pool.map(one, range(args.rows // args.file_rows)))
    seconds = time.perf_counter() - start
    size = sum(part.stat().st_size for part in out.iterdir()) if out else 0
    if out:
        shutil.rmtree(out)
    return seconds, size


def main() -> None:
    args = arguments()
    arm = f'numpy {np.__version__} + pyarrow {pa.__version__}'
    rows = f'{args.rows / 1e6:g}M rows'
    with tempfile.TemporaryDirectory(dir=args.scratch) as scratch:
        for workers in args.workers:
            for work, out in ((f'generate {rows}', None), (f'write {rows} as ZSTD-1 Parquet', Path(scratch) / 'out')):
                load_before = os.getloadavg()[0]
                run_seconds(args, workers, out)
                timed = [run_seconds(args, workers, out) for _ in range(args.runs)]
                written = f'{timed[0][1] / 1e6:.1f} MB' if out else '—'
                row(arm, workers, work, rate_range([args.rows / seconds for seconds, _ in timed]), written, load_before=load_before)


if __name__ == '__main__':
    main()
