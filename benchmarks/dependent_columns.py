#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = ["doppeldata", "pyarrow==26.0.0", "polars==2.0.0"]
#
# [tool.uv.sources]
# doppeldata = { path = "../library", editable = true }
# ///
"""The dependent-columns arms: the bundled eight-columns profile with one column added, drawn independently and then
through a 4-column Spearman copula or a conditional table, all through `sd.generate`. Then a 40-column copula group
whose stated pairs no valid matrix holds, planned and drawn for 10 rows.

Prints one Markdown table row per arm, then one row per copula pair, measured on 1M rows of the copula arm.
"""

import argparse
import copy
import importlib.metadata
import os
import random
import time
from typing import Any

import doppeldata as sd
import polars as pl
import pyarrow as pa

# Pairwise Spearman values from 0.2 to 0.9 over four columns, whose latent matrix stays positive definite.
COPULA = {
    ('amount', 'qty'): 0.9,
    ('amount', 'score'): 0.6,
    ('amount', 'discount'): 0.4,
    ('qty', 'score'): 0.5,
    ('qty', 'discount'): 0.3,
    ('score', 'discount'): 0.2,
}
SPEARMAN_ROWS = 1_000_000
# Every pair of the repaired group stated, half its members tying most of their rows at one value.
REPAIRED_MEMBERS = 40
REPAIRED_TIED = {
    'quantiles': {'p01': -1.0, 'p05': 1.0, 'p50': 1.0, 'p99': 1.0},
    'extremes': {'low': -1.0, 'high': 1.0},
    'top_values': [{'value': 1.0, 'fraction': 0.95}, {'value': -1.0, 'fraction': 0.03}, {'value': 0.0, 'fraction': 0.02}],
    'other_fraction': 0.0,
}
REPAIRED_CONTINUOUS = {'quantiles': {'p01': -3.0, 'p25': 1.0, 'p50': 2.5, 'p75': 4.0, 'p99': 9.0}, 'extremes': {'low': -5.0, 'high': 12.0}}
DISCOUNT = {
    'name': 'discount',
    'physical_type': 'float64',
    'logical_type': 'number',
    'nullable': False,
    'null_fraction': 0.0,
    'numeric': {
        'integral': False,
        'extremes': {'low': 2.0, 'high': 18.0, 'method': 'trimmed_k'},
        'mean': 10.0,
        'std': 2.0,
        'quantiles': {'p01': 5.347, 'p05': 6.71, 'p25': 8.651, 'p50': 10.0, 'p75': 11.349, 'p95': 13.29, 'p99': 14.653},
        'fit': {'family': 'norm', 'params': {'loc': 10.0, 'scale': 2.0}},
        'monotonic': 'none',
    },
}
TIER = {
    'name': 'tier',
    'physical_type': 'string',
    'logical_type': 'string',
    'nullable': False,
    'null_fraction': 0.0,
    'distinct': {'approx_values': 3, 'ratio': 0.0000003},
    'string': {
        'top_values': [{'value': 'bronze', 'fraction': 0.6}, {'value': 'silver', 'fraction': 0.3}, {'value': 'gold', 'fraction': 0.1}]
    },
}


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=10_000_000)
    parser.add_argument('--threads', type=int, nargs='+', default=[1, os.process_cpu_count() or 1])
    parser.add_argument('--runs', type=int, default=5, help='Timed runs per arm, after one untimed run.')
    parser.add_argument('--seed', type=int, default=42)
    return parser.parse_args()


def with_column(column: dict[str, Any]) -> dict[str, Any]:
    document = copy.deepcopy(sd.Profile.example('eight-columns').document)
    document['columns'].append(column)
    return document


def copula_arms() -> tuple[sd.Profile, sd.Profile]:
    document = with_column(DISCOUNT)
    correlated = copy.deepcopy(document)
    correlated['dataset']['correlations'] = [{'a': a, 'b': b, 'method': 'spearman', 'value': value} for (a, b), value in COPULA.items()]
    return sd.Profile(document), sd.Profile(correlated)


def conditional_arms() -> tuple[sd.Profile, sd.Profile]:
    """`tier` drawn alone, then given each of the 50 categories, its share of gold rising from 2% to 32%."""
    document = with_column(TIER)
    conditioned = copy.deepcopy(document)
    categories = [entry['value'] for entry in document['columns'][3]['string']['top_values']]
    fractions = {}
    for at, category in enumerate(categories):
        gold = round(0.02 + 0.3 * at / len(categories), 4)
        fractions[category] = {'bronze': round(0.8 - gold, 4), 'silver': 0.2, 'gold': gold}
    conditioned['dataset']['conditionals'] = [{'given': 'category', 'column': 'tier', 'fractions': fractions}]
    return sd.Profile(document), sd.Profile(conditioned)


def repaired_group() -> sd.Profile:
    """`REPAIRED_MEMBERS` columns, each pair stated at a Spearman value drawn from ±0.8, which no valid matrix holds."""
    draws = random.Random(7)
    names = [f'member_{at:02d}' for at in range(REPAIRED_MEMBERS)]
    columns = [
        {'name': name, 'physical_type': 'double', 'numeric': REPAIRED_TIED if at % 2 else REPAIRED_CONTINUOUS}
        for at, name in enumerate(names)
    ]
    correlations = [
        {'a': a, 'b': b, 'method': 'spearman', 'value': round(draws.uniform(-0.8, 0.8), 3)}
        for at, a in enumerate(names)
        for b in names[at + 1 :]
    ]
    return sd.Profile({'kind': 'table', 'name': 'repaired_group', 'columns': columns, 'dataset': {'correlations': correlations}})


def plans_per_second(profile: sd.Profile, args: argparse.Namespace) -> float:
    start = time.perf_counter()
    sd.generate(profile, rows=10, seed=args.seed, threads=1)
    return 1 / (time.perf_counter() - start)


def row(*cells: object, load_before: float | None = None) -> None:
    load = '—' if load_before is None else f'{load_before:.1f} to {os.getloadavg()[0]:.1f}'
    print('| ' + ' | '.join(str(cell) for cell in (*cells, load)) + ' |', flush=True)


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
    independent, copula = copula_arms()
    alone, conditional = conditional_arms()
    arms = [
        (independent, 'eight columns and `discount`, independently'),
        (copula, 'the same, with a 4-column copula'),
        (alone, 'eight columns and `tier`, independently'),
        (conditional, 'the same, with `tier` given `category`'),
    ]
    for threads in args.threads:
        for profile, work in arms:
            load_before = os.getloadavg()[0]
            rows_per_second(profile, args, threads)
            rates = [rows_per_second(profile, args, threads) for _ in range(args.runs)]
            low, high = min(rates) / 1e6, max(rates) / 1e6
            row(f'engine {version}, `sd.generate`', threads, f'{work}, {rows}', f'{low:.3g}M to {high:.3g}M', '—', load_before=load_before)
    repaired = repaired_group()
    load_before = os.getloadavg()[0]
    plans_per_second(repaired, args)
    rates = [plans_per_second(repaired, args) for _ in range(args.runs)]
    pairs = REPAIRED_MEMBERS * (REPAIRED_MEMBERS - 1) // 2
    work = f'plan and draw 10 rows of a {REPAIRED_MEMBERS}-column copula group, its {pairs} stated pairs held by no valid matrix'
    row(f'engine {version}, `sd.generate`', 1, work, f'{min(rates):.3g} to {max(rates):.3g}', '—', load_before=load_before)
    measured = pl.DataFrame(pa.table(sd.generate(copula, rows=SPEARMAN_ROWS, seed=args.seed)))
    for (a, b), value in COPULA.items():
        spearman = measured.select(a, b).drop_nulls().select(pl.corr(a, b, method='spearman')).item()
        row(f'engine {version}, `sd.generate`', '—', f'Spearman {a} x {b} over 1M rows, stated {value}', f'{spearman:.4f}', '—')


if __name__ == '__main__':
    main()
