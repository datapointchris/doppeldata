"""Hold a Spark profile of one table to a Polars profile of the same data, for check-profiler-parity.sh.

compare_spark_profile_with_polars.py POLARS SPARK DATA

Row counts, null rates, types and the trimmed extremes must be equal, but for a timestamp's unit, which Spark always holds
as microseconds. Each Spark quantile's rank error is measured
against the data and must stay within RANK_ERROR. Each distinct count must stay within DISTINCT_ERROR of Polars' exact
count. Every other field that differs is printed, except those a sample or a float sum's order decides.
"""

import re
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from doppeldata import Profile

# The bar the Spark backend's Greenwald-Khanna accuracy of 10,000 stays far inside.
RANK_ERROR = 0.01
# Three of HyperLogLog++'s relative standard deviations at the backend's 0.01.
DISTINCT_ERROR = 0.03
QUANTILE_LEVELS = {
    'p001': 0.001,
    'p01': 0.01,
    'p05': 0.05,
    'p25': 0.25,
    'p50': 0.5,
    'p75': 0.75,
    'p95': 0.95,
    'p99': 0.99,
    'p999': 0.999,
}
# Fields an estimate, a sample or a parallel sum's order decides, which the bars above or nothing hold.
UNCOMPARED = (
    '/quantiles',
    '/histogram',
    '/fit',
    '/distinct',
    '/mean',
    '/std',
    '/skew',
    '/kurtosis',
    '/semantic/confidence',
    '/quality',
    '/pattern',
)
TIMESTAMP_UNIT = re.compile(r'^timestamp\[(s|ms|us|ns)')


def as_spark_holds(physical_type: Any) -> Any:
    """A physical type with any timestamp unit as microseconds, the only unit Spark holds."""
    return TIMESTAMP_UNIT.sub('timestamp[us', physical_type) if isinstance(physical_type, str) else physical_type


def leaves(value: Any, path: str = '') -> Iterator[tuple[str, Any]]:
    if isinstance(value, dict):
        for key, item in value.items():
            yield from leaves(item, f'{path}/{key}')
    elif isinstance(value, list) and value and isinstance(value[0], dict):
        for index, item in enumerate(value):
            yield from leaves(item, f'{path}/{index}')
    else:
        yield path, value


def scanned(data: Path) -> pl.LazyFrame:
    """The data's rows. Files whose names start with `_` or `.`, such as a write's manifest, are not data, as the listing
    skips them. Parquet files holding different schemas are scanned at their union, as the Polars backend reads them."""
    files = sorted(path for path in data.rglob('*') if path.is_file() and not path.name.startswith(('_', '.'))) if data.is_dir() else [data]
    if all(path.suffix == '.parquet' for path in files):
        schemas = [pq.read_schema(path) for path in files]
        if all(schema.equals(schemas[0]) for schema in schemas):
            return pl.scan_parquet(files)
        union = pa.unify_schemas(schemas, promote_options='permissive')
        return pl.scan_parquet(
            files,
            schema=pl.DataFrame(union.empty_table()).schema,
            missing_columns='insert',
            extra_columns='ignore',
            cast_options=pl.ScanCastOptions(integer_cast='upcast', float_cast='upcast'),
        )
    return pl.scan_csv(files, infer_schema=False, separator='\t' if files[0].suffix == '.tsv' else ',')


def rank_error(values: pl.Series, value: float, level: float) -> float:
    low, high = int((values < value).sum()) / values.len(), int((values <= value).sum()) / values.len()
    return 0.0 if low <= level <= high else min(abs(level - low), abs(level - high))


def compared(polars: dict[str, Any], spark: dict[str, Any], frame: pl.LazyFrame) -> list[str]:
    failures = []
    if polars['dataset']['row_count'] != spark['dataset']['row_count']:
        failures.append(f'row_count {polars["dataset"]["row_count"]} against {spark["dataset"]["row_count"]}')
    found_columns = {column['name']: column for column in spark['columns']}
    worst_rank: tuple[float, str] = (0.0, '')
    worst_distinct: tuple[float, str] = (0.0, '')
    for expected in polars['columns']:
        name = expected['name']
        found = found_columns.get(name)
        if found is None:
            failures.append(f'{name}: missing from the Spark profile')
            continue
        if as_spark_holds(expected.get('physical_type')) != found.get('physical_type'):
            failures.append(f'{name}: physical_type {expected.get("physical_type")!r} against {found.get("physical_type")!r}')
        for field in ('logical_type', 'null_rate', 'nullable'):
            if expected.get(field) != found.get(field):
                failures.append(f'{name}: {field} {expected.get(field)!r} against {found.get(field)!r}')
        for section in ('numeric', 'temporal'):
            want, got = (expected.get(section) or {}).get('extremes'), (found.get(section) or {}).get('extremes')
            if want != got:
                failures.append(f'{name}: {section} extremes {want} against {got}')
        exact, estimated = (expected.get('distinct') or {}).get('approx'), (found.get('distinct') or {}).get('approx')
        if exact and estimated is not None and abs(estimated - exact) / exact > worst_distinct[0]:
            worst_distinct = (abs(estimated - exact) / exact, f'{name}: {estimated} against {exact}')
        quantiles = (found.get('numeric') or {}).get('quantiles') or {}
        length_quantiles = ((found.get('string') or {}).get('length') or {}).get('quantiles') or {}
        if quantiles or length_quantiles:
            raw = frame.select(pl.col(name)).collect().to_series()
            if quantiles:
                values = raw.cast(pl.Float64, strict=False).drop_nulls().drop_nans()
                for level, value in quantiles.items():
                    error = rank_error(values, value, QUANTILE_LEVELS[level])
                    worst_rank = max(worst_rank, (error, f'{name} {level}: {value}'))
            if length_quantiles:
                lengths = raw.cast(pl.String).drop_nulls().str.len_chars()
                for level, value in length_quantiles.items():
                    error = rank_error(lengths, value, QUANTILE_LEVELS[level])
                    worst_rank = max(worst_rank, (error, f'{name} length {level}: {value}'))
        want_leaves, got_leaves = dict(leaves(expected)), dict(leaves(found))
        differing = [
            path
            for path in sorted(want_leaves.keys() | got_leaves.keys())
            if want_leaves.get(path) != got_leaves.get(path) and not any(part in path for part in UNCOMPARED)
        ]
        if differing:
            print(f'note {name}: ' + ', '.join(f'{path} {want_leaves.get(path)!r} -> {got_leaves.get(path)!r}' for path in differing[:6]))
    print(f'worst quantile rank error {worst_rank[0]:.5f} ({worst_rank[1]})')
    print(f'worst distinct relative error {worst_distinct[0]:.5f} ({worst_distinct[1]})')
    if worst_rank[0] > RANK_ERROR:
        failures.append(f'quantile rank error {worst_rank[0]:.5f} above {RANK_ERROR}: {worst_rank[1]}')
    if worst_distinct[0] > DISTINCT_ERROR:
        failures.append(f'distinct relative error {worst_distinct[0]:.5f} above {DISTINCT_ERROR}: {worst_distinct[1]}')
    return failures


def main() -> None:
    polars_path, spark_path, data = sys.argv[1:4]
    failures = compared(Profile.load(polars_path).to_plain(), Profile.load(spark_path).to_plain(), scanned(Path(data)))
    for failure in failures:
        print('FAIL', failure)
    sys.exit(1 if failures else 0)


if __name__ == '__main__':
    main()
