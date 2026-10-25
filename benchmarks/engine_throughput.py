#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = ["doppeldata", "doppeldata-cli"]
#
# [tool.uv.sources]
# doppeldata = { path = "../library", editable = true }
# doppeldata-cli = { path = "../cli", editable = true }
# ///
"""The engine arm: the bundled eight-columns profile through `sd.generate`, and through `doppeldata data generate` as ZSTD-1 Parquet.

Prints one Markdown table row per arm. Every write at every thread count must hash the same, or the run fails.
"""

import argparse
import hashlib
import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import doppeldata as sd


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=10_000_000)
    parser.add_argument('--file-rows', type=int, default=1_000_000)
    parser.add_argument('--threads', type=int, nargs='+', default=[1, os.process_cpu_count() or 1])
    parser.add_argument('--runs', type=int, default=5, help='Timed runs per arm, after one untimed run.')
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--scratch', type=Path, help='Where the Parquet is written. Defaults to the system temp directory.')
    return parser.parse_args()


def rate_range(rates: list[float]) -> str:
    return f'{min(rates) / 1e6:.3g}M to {max(rates) / 1e6:.3g}M'


def row(*cells: object, load_before: float | None = None) -> None:
    """One table row, ending with the one-minute load average before the arm's first run and after its last."""
    load = '—' if load_before is None else f'{load_before:.1f} to {os.getloadavg()[0]:.1f}'
    print('| ' + ' | '.join(str(cell) for cell in (*cells, load)) + ' |', flush=True)


def generate_seconds(profile: sd.Profile, args: argparse.Namespace, threads: int) -> float:
    start = time.perf_counter()
    table = sd.generate(profile, rows=args.rows, seed=args.seed, threads=threads)
    seconds = time.perf_counter() - start
    del table
    return seconds


def write(args: argparse.Namespace, threads: int, out: Path) -> tuple[float, int, str]:
    """Rows a second as the manifest's `elapsed_ms` counts them, then the part files' bytes and their SHA-256 in path order.

    The manifest's clock starts once the CLI has imported, as the numpy arm's starts once its script has.
    """
    writer = {'compression': 'zstd', 'compression_level': 1, 'row_group_rows': args.file_rows}
    command = [str(Path(sys.executable).with_name('doppeldata')), 'data', 'generate', 'eight-columns', '--rows', str(args.rows)]
    command += ['--seed', str(args.seed), '--format', 'parquet', '--file-rows', str(args.file_rows), '--writer', json.dumps(writer)]
    command += ['--threads', str(threads), '--out', str(out)]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
    elapsed_ms = json.loads((out / '_manifest.json').read_text())['elapsed_ms']
    sha256 = hashlib.sha256()
    size = 0
    for part in sorted(out.rglob('*.parquet')):
        size += part.stat().st_size
        with part.open('rb') as file:
            while chunk := file.read(1 << 20):
                sha256.update(chunk)
    shutil.rmtree(out)
    return args.rows / elapsed_ms * 1000, size, sha256.hexdigest()


def main() -> None:
    args = arguments()
    profile = sd.Profile.example('eight-columns')
    version = importlib.metadata.version('doppeldata-cli')
    rows = f'{args.rows / 1e6:g}M rows'
    for threads in args.threads:
        load_before = os.getloadavg()[0]
        generate_seconds(profile, args, threads)
        rates = [args.rows / generate_seconds(profile, args, threads) for _ in range(args.runs)]
        row(f'engine {version}, `sd.generate`', threads, f'generate {rows}', rate_range(rates), '—', load_before=load_before)

    runs_by_sha256: dict[str, list[str]] = {}
    with tempfile.TemporaryDirectory(dir=args.scratch) as scratch:
        for threads in args.threads:
            load_before = os.getloadavg()[0]
            rates = []
            for run in range(args.runs + 1):
                rate, size, sha256 = write(args, threads, Path(scratch) / f'{threads}-threads-run-{run}')
                runs_by_sha256.setdefault(sha256, []).append(f'{threads} threads, run {run}')
                rates.append(rate)
            written = f'{size / 1e6:.1f} MB'
            row(
                f'engine {version}, `data generate`',
                threads,
                f'write {rows} as ZSTD-1 Parquet',
                rate_range(rates[1:]),
                written,
                load_before=load_before,
            )

    if len(runs_by_sha256) > 1:
        sys.exit(f'the Parquet differs between runs: {json.dumps(runs_by_sha256, indent=2)}')
    compared = ' and '.join(str(threads) for threads in args.threads)
    row(f'engine {version}, `data generate`', compared, 'SHA-256 of every run', 'identical', next(iter(runs_by_sha256))[:16])


if __name__ == '__main__':
    main()
