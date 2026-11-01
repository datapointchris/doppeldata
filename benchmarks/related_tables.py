#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = ["doppeldata"]
#
# [tool.uv.sources]
# doppeldata = { path = "../library", editable = true }
# ///
"""The related-tables arms: one table of a bundle generated from its own profile alone, then with every table it draws
from, its foreign keys drawn by their relationships, then with its inherited columns copied too, all through
`sd.generate`.

A bundle arm generates the table's parents with it, parents first, so its rate is the table's rows over the whole
generation. Prints one Markdown table row per arm.
"""

import argparse
import copy
import importlib.metadata
import os
import time
from typing import Any

import doppeldata as sd


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', default='orders-customers', help='A bundle file, directory or example name.')
    parser.add_argument('--table', default='orders')
    parser.add_argument('--scale', type=float, default=10.0)
    parser.add_argument('--threads', type=int, nargs='+', default=[1, os.process_cpu_count() or 1])
    parser.add_argument('--runs', type=int, default=5, help='Timed runs per arm, after one untimed run.')
    parser.add_argument('--seed', type=int, default=42)
    return parser.parse_args()


def table_of(reference: str) -> str:
    return reference.split('.', 1)[0]


def drawn_from(bundle: sd.Bundle, table: str, inherited: bool) -> sd.Bundle:
    """`table` and every table its relationships reach, with the relationships between them, each `inherited` entry
    kept only where `inherited`."""
    stated = bundle.document.get('relationships') or []
    names, waiting = {table}, [table]
    while waiting:
        child = waiting.pop()
        for relationship in stated:
            parent = table_of(relationship['to'][0])
            if table_of(relationship['from'][0]) == child and parent not in names:
                names.add(parent)
                waiting.append(parent)
    relationships: list[dict[str, Any]] = []
    for relationship in stated:
        if table_of(relationship['from'][0]) in names and table_of(relationship['to'][0]) in names:
            kept = copy.deepcopy(dict(relationship))
            if not inherited:
                kept.pop('inherited', None)
            relationships.append(kept)
    return sd.Bundle.of(bundle.document['name'], {name: bundle.tables[name] for name in sorted(names)}, relationships)


def row(*cells: object, load_before: float) -> None:
    print('| ' + ' | '.join(str(cell) for cell in (*cells, f'{load_before:.1f} to {os.getloadavg()[0]:.1f}')) + ' |', flush=True)


def main() -> None:
    args = arguments()
    version = importlib.metadata.version('doppeldata')
    bundle = sd.Bundle.load(args.bundle) if os.path.exists(args.bundle) else sd.Bundle.example(args.bundle)
    alone = bundle.tables[args.table]
    rows = round(alone.document['dataset']['rows'] * args.scale)
    related, copying = drawn_from(bundle, args.table, inherited=False), drawn_from(bundle, args.table, inherited=True)
    copied = sum(len(relationship.get('inherited') or []) for relationship in copying.document.get('relationships') or [])
    parents = ', '.join(name for name in related.tables if name != args.table)

    def alone_arm(threads: int) -> None:
        sd.generate(alone, rows=rows, seed=args.seed, threads=threads)

    def bundle_arm(drawn: sd.Bundle) -> Any:
        return lambda threads: sd.generate(drawn, scale=args.scale, seed=args.seed, threads=threads)

    arms = [
        (alone_arm, f'`{args.table}` alone, from its own profile'),
        (bundle_arm(related), f'`{args.table}` with {parents}, each foreign key drawn by its relationship'),
    ]
    if copied:
        arms.append((bundle_arm(copying), f'the same, copying {copied} inherited column{"s" if copied > 1 else ""}'))
    for threads in args.threads:
        for generate, work in arms:
            load_before = os.getloadavg()[0]
            generate(threads)
            rates = []
            for _ in range(args.runs):
                start = time.perf_counter()
                generate(threads)
                rates.append(rows / (time.perf_counter() - start))
            low, high = min(rates) / 1e6, max(rates) / 1e6
            work_done = f'{work}, {rows / 1e6:g}M rows of `{args.table}`'
            row(f'engine {version}, `sd.generate`', threads, work_done, f'{low:.3g}M to {high:.3g}M', '—', load_before=load_before)


if __name__ == '__main__':
    main()
