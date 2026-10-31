#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = ["doppeldata", "faker==40.43.0", "mimesis==22.2.0"]
#
# [tool.uv.sources]
# doppeldata = { path = "../library", editable = true }
# ///
"""Person names a second on one thread: Faker, Mimesis, and the engine's `name` provider as a one-column table.

Prints one Markdown table row per generator.
"""

import argparse
import os
import time
from collections.abc import Callable

import doppeldata as sd
import faker
import mimesis
from mimesis.locales import Locale

NAME_COLUMN = {'name': 'name', 'physical_type': 'string', 'semantic': {'generator_hint': {'provider': 'name'}}}


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--names', type=int, default=200_000)
    parser.add_argument('--runs', type=int, default=5, help='Timed runs per generator, after one untimed run.')
    return parser.parse_args()


def row(*cells: object, load_before: float) -> None:
    """One table row, ending with the one-minute load average before the arm's first run and after its last."""
    print('| ' + ' | '.join(str(cell) for cell in (*cells, f'{load_before:.1f} to {os.getloadavg()[0]:.1f}')) + ' |', flush=True)


def per_second(rate: float) -> str:
    return f'{rate / 1e6:.3g}M' if rate >= 1e6 else f'{rate / 1e3:.3g}k'


def names_a_second(make_names: Callable[[int], object], names: int, runs: int) -> str:
    rates = []
    for _ in range(runs + 1):
        start = time.perf_counter()
        make_names(names)
        rates.append(names / (time.perf_counter() - start))
    return f'{per_second(min(rates[1:]))} to {per_second(max(rates[1:]))}'


def main() -> None:
    args = arguments()
    faker.Faker.seed(42)
    faker_en = faker.Faker('en_US')
    person = mimesis.Person(Locale.EN, seed=42)
    profile = sd.Profile({'kind': 'table', 'name': 'names', 'columns': [NAME_COLUMN]})
    generators: list[tuple[str, Callable[[int], object]]] = [
        (f'Faker {faker.VERSION}, `name()`', lambda n: [faker_en.name() for _ in range(n)]),
        (f'Mimesis {mimesis.__version__}, `full_name()`', lambda n: [person.full_name() for _ in range(n)]),
        ('engine, `name` provider', lambda n: sd.generate(profile, rows=n, seed=42, threads=1)),
    ]
    for arm, make_names in generators:
        load_before = os.getloadavg()[0]
        row(arm, 1, f'{args.names:,} names', names_a_second(make_names, args.names, args.runs), '—', load_before=load_before)


if __name__ == '__main__':
    main()
