#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = ["doppeldata"]
#
# [tool.uv.sources]
# doppeldata = { path = "../library", editable = true }
# ///
"""The type-mix arms: the bundled eight-columns profile with one integer column added, drawn as its type and then as
text holding a quarter strings, all through `sd.generate`.

Prints one Markdown table row per arm.
"""

import argparse
import copy
import importlib.metadata
import os
import time
from typing import Any

import doppeldata as sd

USER_ID = {
    'name': 'user_id',
    'physical_type': 'int64',
    'logical_type': 'integer',
    'nullable': False,
    'null_fraction': 0.0,
    'distinct': {'approx_values': 25_000, 'ratio': 0.1042},
    'numeric': {'integral': True, 'extremes': {'low': 0, 'high': 99_999, 'method': 'digit_bounds'}, 'monotonic': 'none'},
}
STRING_SHARE = {'integer': 0.75, 'string': 0.25}


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=10_000_000)
    parser.add_argument('--threads', type=int, nargs='+', default=[1, os.process_cpu_count() or 1])
    parser.add_argument('--runs', type=int, default=5, help='Timed runs per arm, after one untimed run.')
    parser.add_argument('--seed', type=int, default=42)
    return parser.parse_args()


def with_user_id(type_mix: dict[str, float] | None) -> sd.Profile:
    document: dict[str, Any] = copy.deepcopy(sd.Profile.example('eight-columns').document)
    column = copy.deepcopy(USER_ID)
    if type_mix is not None:
        column['type_mix_fractions'] = type_mix
    document['columns'].append(column)
    return sd.Profile(document)


def row(*cells: object, load_before: float) -> None:
    print('| ' + ' | '.join(str(cell) for cell in (*cells, f'{load_before:.1f} to {os.getloadavg()[0]:.1f}')) + ' |', flush=True)


def rows_per_second(profile: sd.Profile, args: argparse.Namespace, threads: int) -> float:
    start = time.perf_counter()
    table = sd.generate(profile, rows=args.rows, seed=args.seed, threads=threads)
    seconds = time.perf_counter() - start
    del table
    return args.rows / seconds


def main() -> None:
    args = arguments()
    version = importlib.metadata.version('doppeldata')
    rows = f'{args.rows / 1e6:g}M rows'
    arms = [
        (with_user_id(None), 'eight columns and `user_id`, as `int64`'),
        (with_user_id(STRING_SHARE), 'the same, `user_id` drawn as text holding 25% strings'),
    ]
    for threads in args.threads:
        for profile, work in arms:
            load_before = os.getloadavg()[0]
            rows_per_second(profile, args, threads)
            rates = [rows_per_second(profile, args, threads) for _ in range(args.runs)]
            low, high = min(rates) / 1e6, max(rates) / 1e6
            row(f'engine {version}, `sd.generate`', threads, f'{work}, {rows}', f'{low:.3g}M to {high:.3g}M', '—', load_before=load_before)


if __name__ == '__main__':
    main()
