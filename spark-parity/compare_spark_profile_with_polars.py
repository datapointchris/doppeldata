"""Hold a Spark profile of one table, or a bundle of several, to a Polars profile of the same data, for check-profiler-parity.sh.

compare_spark_profile_with_polars.py POLARS SPARK DATA

Row counts, null rates, types and the trimmed extremes must be equal, but for a timestamp's unit, which Spark always holds
as microseconds. Each Spark quantile's rank error is measured
against the data and must stay within RANK_ERROR, and so must each quantile of a column given another's value, among the
rows holding that value. Each distinct count must stay within DISTINCT_ERROR of Polars' exact count. Every other field
that differs is printed, except those a sample or a float sum's order decides.

The dataset section and a bundle's relationships must hold the same entries, and each value the same. Correlations are read
from each engine's own sample, as the fits are, and so is which pairs get a conditional table, quantiles or rates, and which
composite key short of unique is nominated. So their
differences are printed and fail nothing, while a conditional table or rate both profiles state must match cell for cell. A
bundle's DATA is a directory holding each table in a folder of the table's name.
"""

import collections
import re
import sys
from collections.abc import Callable
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from doppeldata import Bundle
from doppeldata import Profile
from doppeldata.profile_format import read_document
from doppeldata.published_schemas import published_fields

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
# The fields `dataset_entries` and `relationship_entries` read, by schema definition. A field the format gains stops the
# script until one of them reads it, so no field goes uncompared.
READ_FIELDS = {
    'Dataset': {
        'rows',
        'duplicate_row_fraction',
        'keys',
        'correlations',
        'correlation_ratios',
        'conditionals',
        'conditional_quantiles',
        'conditional_fractions',
        'dependencies',
    },
    'Keys': {'primary', 'candidates'},
    'Key': {'columns', 'uniqueness'},
    'Correlation': {'a', 'b', 'method', 'value'},
    'CorrelationRatio': {'a', 'b', 'value'},
    'Conditional': {'given', 'column', 'fractions', 'other_fractions'},
    'ConditionalQuantiles': {'given', 'column', 'quantiles', 'other'},
    'ConditionalFractions': {'given', 'column', 'zero_fraction', 'null_fraction', 'other'},
    'OtherFractions': {'zero_fraction', 'null_fraction'},
    'Dependency': {'kind', 'lhs', 'rhs', 'violation_fraction', 'table'},
    'Relationship': {'from', 'to', 'containment', 'orphan_fraction', 'cardinality', 'fanout', 'inherited', 'ordinal'},
    'Fanout': {'quantiles', 'top_fractions', 'zipf_s'},
    'Inherited': {'child', 'parent', 'agreement_fraction'},
}
for definition, read in READ_FIELDS.items():
    unread = sorted(set(published_fields(definition)) - read)
    if unread:
        raise SystemExit(
            f'the parity check reads no {definition} field {", ".join(unread)}; compare it in dataset_entries or relationship_entries'
        )


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


def dataset_entries(dataset: dict[str, Any]) -> Iterator[tuple[str, str, Any]]:
    """Each value the dataset section states, as the field, what the value describes, and the value."""
    yield 'rows', '', dataset.get('rows')
    yield 'duplicate_row_fraction', '', dataset.get('duplicate_row_fraction')
    keys = dataset.get('keys') or {}
    for role, key in [('primary', keys.get('primary')), *(('candidate', key) for key in keys.get('candidates') or [])]:
        if key:
            yield 'keys', f'{role} {"+".join(key["columns"])}', key.get('uniqueness')
    for correlation in dataset.get('correlations') or []:
        yield f'correlations {correlation["method"]}', f'{correlation["a"]} x {correlation["b"]}', correlation['value']
    for ratio in dataset.get('correlation_ratios') or []:
        yield 'correlation_ratios', f'{ratio["a"]} x {ratio["b"]}', ratio['value']
    for conditional in dataset.get('conditionals') or []:
        rows = {
            **{f'{conditional["given"]}={given}': row for given, row in conditional['fractions'].items()},
            '(other)': conditional.get('other_fractions') or {},
        }
        for given, row in rows.items():
            for value, rate in row.items():
                yield 'conditionals', f'{conditional_name(conditional)}: {conditional["column"]}={value} | {given}', rate
    for stated in dataset.get('conditional_quantiles') or []:
        groups = {**{f'{stated["given"]}={given}': row for given, row in stated['quantiles'].items()}, '(other)': stated.get('other') or {}}
        for given, quantiles in groups.items():
            for level in quantiles:
                yield 'conditional_quantiles', f'{conditional_name(stated)}: {level} | {given}', None
    for stated in dataset.get('conditional_fractions') or []:
        for rate in ('zero_fraction', 'null_fraction'):
            for given, value in (stated.get(rate) or {}).items():
                yield 'conditional_fractions', f'{conditional_name(stated)}: {rate} | {stated["given"]}={given}', value
            yield 'conditional_fractions', f'{conditional_name(stated)}: {rate} | (other)', (stated.get('other') or {}).get(rate)
    for dependency in dataset.get('dependencies') or []:
        described = f'{dependency["kind"]} {"+".join(dependency["lhs"])} -> {dependency["rhs"]}'
        yield 'dependencies', described, dependency.get('violation_fraction')
        for value, determined in (dependency.get('table') or {}).items():
            yield 'dependencies table', f'{described}: {value}', determined


def relationship_entries(relationships: list[dict[str, Any]]) -> Iterator[tuple[str, str, Any]]:
    """Each value a bundle's relationships state, as the field, the relationship and part, and the value."""
    for relationship in relationships:
        named = f'{", ".join(relationship["from"])} -> {", ".join(relationship["to"])}'
        for field in ('containment', 'orphan_fraction', 'cardinality', 'ordinal'):
            yield f'relationships {field}', named, relationship.get(field)
        fanout = relationship.get('fanout') or {}
        for part in ('quantiles', 'top_fractions'):
            for level, value in (fanout.get(part) or {}).items():
                yield f'relationships fanout {part}', f'{named} {level}', value
        yield 'relationships fanout zipf_s', named, fanout.get('zipf_s')
        for entry in relationship.get('inherited') or []:
            yield 'relationships inherited agreement', f'{named} {entry["child"]} from {entry["parent"]}', entry['agreement_fraction']


def conditional_name(conditional: dict[str, Any]) -> str:
    return f'{conditional["column"]} given {conditional["given"]}'


def entries_compared(
    expected: Iterator[tuple[str, str, Any]], found: Iterator[tuple[str, str, Any]], sampled: Callable[[str, str], bool] = lambda *_: False
) -> list[str]:
    """Print the worst difference in each field, and return a failure for each entry one side lacks or values that differ.
    An entry `sampled` names is printed and fails nothing."""
    want, got = ({(field, described): value for field, described, value in entries} for entries in (expected, found))
    failures: list[str] = []
    worst: dict[str, tuple[float, str]] = {}
    alone: collections.Counter[tuple[str, str]] = collections.Counter()
    for entry in sorted(want.keys() | got.keys()):
        field, described = entry
        if entry not in got or entry not in want:
            if sampled(field, described):
                alone[field, 'Polars' if entry in want else 'Spark'] += 1
            else:
                failures.append(f'{field} {described}: only the {"Polars" if entry in want else "Spark"} profile states it')
            continue
        a, b = want[entry], got[entry]
        numeric = isinstance(a, int | float) and isinstance(b, int | float) and not isinstance(a, bool)
        difference = abs(a - b) if numeric else float(a != b)
        if difference >= worst.get(field, (-1.0, ''))[0]:
            worst[field] = (difference, f'{described}: {a!r} against {b!r}')
        if difference > 0 and not sampled(field, described):
            failures.append(f'{field} {described}: {a!r} against {b!r}')
    for field, (difference, described) in sorted(worst.items()):
        print(f'worst {field} difference {difference:.6g} ({described})')
    for (field, side), count in sorted(alone.items()):
        print(f'{field}: {count} entries only the {side} profile states, from its own sample')
    return failures


def compared(polars: dict[str, Any], spark: dict[str, Any], frame: pl.LazyFrame) -> list[str]:
    shared = {
        field: {conditional_name(entry) for entry in polars['dataset'].get(field) or []}
        & {conditional_name(entry) for entry in spark['dataset'].get(field) or []}
        for field in ('conditionals', 'conditional_quantiles', 'conditional_fractions')
    }

    nominated = {
        f'candidate {"+".join(key["columns"])}'
        for profile in (polars, spark)
        for key in (profile['dataset'].get('keys') or {}).get('candidates') or []
        if key['uniqueness'] < 1
    }

    def sampled(field: str, described: str) -> bool:
        return (
            field.startswith('correlation')
            or (field in shared and described.split(': ', 1)[0] not in shared[field])
            or (field == 'keys' and described in nominated)
        )

    failures = entries_compared(dataset_entries(polars['dataset']), dataset_entries(spark['dataset']), sampled)
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
        for field in ('logical_type', 'null_fraction', 'nullable'):
            if expected.get(field) != found.get(field):
                failures.append(f'{name}: {field} {expected.get(field)!r} against {found.get(field)!r}')
        for section in ('numeric', 'temporal'):
            want, got = (expected.get(section) or {}).get('extremes'), (found.get(section) or {}).get('extremes')
            if want != got:
                failures.append(f'{name}: {section} extremes {want} against {got}')
        exact, estimated = (expected.get('distinct') or {}).get('approx_values'), (found.get('distinct') or {}).get('approx_values')
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
    for stated in spark['dataset'].get('conditional_quantiles') or []:
        given, column, listed = stated['given'], stated['column'], list(stated['quantiles'])
        rows = (
            frame.select(pl.col(given).cast(pl.String).alias('given'), pl.col(column).cast(pl.Float64, strict=False).alias('value'))
            .filter(pl.col('value').is_not_null() & pl.col('value').is_not_nan())
            .collect()
        )
        groups = {
            **{f'={value}': (rows.filter(pl.col('given') == value), quantiles) for value, quantiles in stated['quantiles'].items()},
            ' (other)': (rows.filter(~pl.col('given').is_in(listed) | pl.col('given').is_null()), stated.get('other') or {}),
        }
        for label, (group, quantiles) in groups.items():
            for level, value in quantiles.items():
                error = rank_error(group['value'], value, QUANTILE_LEVELS[level])
                worst_rank = max(worst_rank, (error, f'{column} given {given}{label} {level}: {value}'))
    print(f'worst quantile rank error {worst_rank[0]:.5f} ({worst_rank[1]})')
    print(f'worst distinct relative error {worst_distinct[0]:.5f} ({worst_distinct[1]})')
    if worst_rank[0] > RANK_ERROR:
        failures.append(f'quantile rank error {worst_rank[0]:.5f} above {RANK_ERROR}: {worst_rank[1]}')
    if worst_distinct[0] > DISTINCT_ERROR:
        failures.append(f'distinct relative error {worst_distinct[0]:.5f} above {DISTINCT_ERROR}: {worst_distinct[1]}')
    return failures


def bundle_compared(polars: dict[str, Any], spark: dict[str, Any], data: Path) -> list[str]:
    failures = entries_compared(
        relationship_entries(polars.get('relationships') or []), relationship_entries(spark.get('relationships') or [])
    )
    for name, table in polars['tables'].items():
        print(f'== table {name}')
        if name not in spark['tables']:
            failures.append(f'{name}: missing from the Spark bundle')
            continue
        failures += [f'{name}: {failure}' for failure in compared(table, spark['tables'][name], scanned(data / name))]
    return failures


def main() -> None:
    polars_path, spark_path, data = sys.argv[1:4]
    if read_document(Path(polars_path)).get('kind') == 'bundle':
        failures = bundle_compared(Bundle.load(polars_path).to_plain(), Bundle.load(spark_path).to_plain(), Path(data))
    else:
        failures = compared(Profile.load(polars_path).to_plain(), Profile.load(spark_path).to_plain(), scanned(Path(data)))
    for failure in failures:
        print('FAIL', failure)
    sys.exit(1 if failures else 0)


if __name__ == '__main__':
    main()
