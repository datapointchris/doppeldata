"""A profile's `source` section: the files' format, codec and layout, the dialect they were written in, and the rows a
reader rejects. No value statistic of a column is read from a file: a Parquet footer's min and max are real values.
"""

import collections
from collections.abc import Iterable
from pathlib import PurePosixPath
from typing import TYPE_CHECKING
from typing import Any

from doppeldata.profiling import json_paths
from doppeldata.profiling.backend import Backend
from doppeldata.profiling.backend import DialectFound
from doppeldata.profiling.backend import ParquetFooter
from doppeldata.profiling.backend import Source
from doppeldata.profiling.backend import SourceFormat
from doppeldata.profiling.backend import SourceRead
from doppeldata.profiling.statistic_catalog import QUANTILES
from doppeldata.profiling.statistic_catalog import ColumnKind

if TYPE_CHECKING:
    from doppeldata.profiling.table_profile import ProfiledColumns

# A text file's codec, by its suffix.
TEXT_CODECS = {'.gz': 'gzip', '.zst': 'zstd', '.bz2': 'bzip2'}
# The codecs a text writer writes, which a profile's `csv` and `json` settings take.
WRITTEN_TEXT_CODECS = frozenset({'gzip', 'zstd'})
# Each Parquet codec, as a footer names it, by the `source.compression` and writer setting it is.
PARQUET_CODECS = {'UNCOMPRESSED': 'none', 'SNAPPY': 'snappy', 'GZIP': 'gzip', 'ZSTD': 'zstd', 'LZ4': 'lz4', 'LZ4_RAW': 'lz4', 'LZO': 'lzo'}
WRITTEN_PARQUET_CODECS = frozenset({'none', 'snappy', 'gzip', 'zstd', 'lz4'})


def source_section(backend: Backend, source: Source, read: SourceRead, profiled: 'ProfiledColumns', k_min: int) -> dict[str, Any]:
    assert source.format is not None
    lines = read.json is not None and read.json.lines
    section: dict[str, Any] = {'format': SourceFormat.NDJSON.value if lines else source.format.value}
    codec = compression(source, read.footers)
    if codec:
        section['compression'] = codec
    layout = layout_section(backend, source, read, profiled, k_min)
    if layout:
        section['layout'] = layout
    text_codec = codec if codec in WRITTEN_TEXT_CODECS else 'none'
    if read.dialect is not None:
        section['csv'] = csv_section(read.dialect, read.rejected, profiled, k_min) | {'compression': text_codec}
    if read.footers:
        section['parquet'] = parquet_section(read.footers)
    if read.json is not None and profiled.json is not None:
        section['json'] = {
            'explicit_nulls': any(counts.null for counts in all_counts(profiled.json)),
            'compression': text_codec,
            'max_depth': json_paths.depth(read.json.paths),
        }
    return section


def compression(source: Source, footers: tuple[ParquetFooter, ...]) -> str | None:
    if footers:
        return PARQUET_CODECS.get(most_common(footer.codec for footer in footers) or '')
    suffixes = {TEXT_CODECS.get(PurePosixPath(file.path).suffix.lower(), 'none') for file in source.files}
    return suffixes.pop() if len(suffixes) == 1 and source.format is not SourceFormat.ARROW else None


def layout_section(backend: Backend, source: Source, read: SourceRead, profiled: 'ProfiledColumns', k_min: int) -> dict[str, Any]:
    sizes = sorted(file.size for file in source.files)
    layout: dict[str, Any] = {
        'files': {
            'count': len(sizes),
            'size_bytes': {'min': sizes[0], 'p50': quantiles_of(sizes)['p50'], 'max': sizes[-1], 'total': sum(sizes)},
        }
    }
    rows = [footer.rows for footer in read.footers] if read.footers else list(read.file_rows)
    if rows:
        layout['rows_per_file'] = quantiles_of(sorted(rows))
    if source.partition_keys:
        layout['partitioning'] = {'style': 'hive', 'keys': [partition_key(backend, profiled, key, k_min) for key in source.partition_keys]}
    order = sort_order(read.footers, profiled)
    if order:
        layout['sort_order'] = order
    return layout


def partition_key(backend: Backend, profiled: 'ProfiledColumns', key: str, k_min: int) -> dict[str, Any]:
    """A Hive key's type and values. Its shares are of every row, each held by at least k_min rows, as top values are."""
    from doppeldata.profiling.table_profile import TOP_VALUES_THRESHOLD
    from doppeldata.profiling.table_profile import kept_values
    from doppeldata.profiling.table_profile import rate

    spec = next(spec for spec in profiled.specs if spec.name == key)
    distinct = profiled.stats[(key, 'distinct_count')]
    values: dict[str, Any] = {'count': distinct}
    if distinct <= TOP_VALUES_THRESHOLD:
        held = profiled.described - profiled.stats[(key, 'null_count')]
        kept, other = kept_values(backend.top_k(profiled.table, key, TOP_VALUES_THRESHOLD), held, k_min)
        values['shares'] = {str(value): rate(count, profiled.described) for value, count in kept}
        values['other_rate'] = rate(other, profiled.described)
    return {'name': key, 'type': spec.physical_type, 'values': values}


def sort_order(footers: tuple[ParquetFooter, ...], profiled: 'ProfiledColumns') -> list[str]:
    """The columns every footer declares its rows sorted by, else the first integer column that never falls in read order."""
    declared = {footer.sorting_columns for footer in footers}
    if len(declared) == 1 and (columns := declared.pop()):
        return list(columns)
    for spec in profiled.specs:
        step = profiled.stats.get((spec.name, 'step_min')) if spec.kind is ColumnKind.INTEGER else None
        if step is not None and step >= 0 and profiled.stats[(spec.name, 'distinct_count')] > 1:
            return [spec.name]
    return []


def csv_section(found: DialectFound, rejected: dict[str, int], profiled: 'ProfiledColumns', k_min: int) -> dict[str, Any]:
    """The writer's dialect, what the first block's bytes hold, how the two sniffers read it, and the malformed rows."""
    from doppeldata.profiling.table_profile import rate
    from doppeldata.profiling.table_profile import significant

    dialect = found.dialect
    breaks = sum(found.line_terminators.values())
    data_rows = profiled.described + sum(rejected.values())
    return {
        'delimiter': dialect.delimiter,
        'quote': dialect.quote or '"',
        'escape': dialect.escape if dialect.escape and dialect.escape != dialect.quote else None,
        'quoting': found.quoting,
        'header': dialect.header,
        'null': profiled.null_token,
        'line_terminator': 'crlf' if found.line_terminators['crlf'] > found.line_terminators['lf'] else 'lf',
        'bom': found.bom,
        'encoding': 'latin1' if dialect.encoding == 'latin-1' else 'utf8',
        'detected_encoding': found.detected_encoding,
        'line_terminator_mix': {name: rate(count, breaks) for name, count in found.line_terminators.items() if count},
        'trailing_delimiter': found.trailing_delimiter,
        'detector': {
            'primary': 'duckdb',
            'confirm': 'clevercsv',
            'agreed': found.agreed,
            'chosen': found.chosen,
            'confidence': significant(found.confidence, 4),
        },
        'malformed': malformed_rows(rejected | {'cast': profiled.unparsed_rows}, data_rows, k_min),
    }


def malformed_rows(rejected: dict[str, int], data_rows: int, k_min: int) -> dict[str, Any]:
    """Each reason's share of the data rows, leaving out a reason fewer than k_min rows give, and their sum."""
    from doppeldata.profiling.table_profile import rate

    kept = {reason: rows for reason, rows in sorted(rejected.items()) if rows >= k_min}
    return {'rate': rate(sum(kept.values()), data_rows), 'by_type': {reason: rate(rows, data_rows) for reason, rows in kept.items()}}


def parquet_section(footers: tuple[ParquetFooter, ...]) -> dict[str, Any]:
    """The writer settings that reproduce the files' layout, and what their footers say of the writer that made them."""
    groups = sorted(rows for footer in footers for rows, _ in footer.row_groups)
    codec = PARQUET_CODECS.get(most_common(footer.codec for footer in footers) or '', 'zstd')
    statistics = (
        'page' if most(footer.page_index for footer in footers) else 'chunk' if most(footer.statistics for footer in footers) else 'none'
    )
    dictionary = sorted(
        {
            column
            for footer in footers
            for column in footer.dictionary_columns
            if most(column in other.dictionary_columns for other in footers)
        }
    )
    every_column = {column for footer in footers for column in footer.columns}
    units = {
        column: most_common(footer.timestamp_units[column] for footer in footers if column in footer.timestamp_units)
        for footer in footers
        for column in footer.timestamp_units
    }
    reproducible = {unit for unit in units.values() if unit != 'int96'}
    scales: dict[str, set[int]] = collections.defaultdict(set)
    for footer in footers:
        for column, scale in footer.decimal_scales.items():
            scales[column].add(scale)
    section: dict[str, Any] = {
        'compression': codec if codec in WRITTEN_PARQUET_CODECS else 'zstd',
        'row_group_rows': max(groups) if groups else 1024 * 1024,
        'statistics': statistics,
        'dictionary_columns': None if set(dictionary) == every_column else dictionary,
        'timestamp_unit': reproducible.pop() if len(reproducible) == 1 and 'int96' not in units.values() else None,
        'writer': most_common(footer.created_by for footer in footers),
        'spark_version': most_common(footer.spark_version for footer in footers),
        'format_version': most_common(footer.format_version for footer in footers),
        'row_groups': {
            'per_file': quantiles_of(sorted(len(footer.row_groups) for footer in footers)),
            'rows': quantiles_of(groups),
            'compressed_bytes': quantiles_of(sorted(size for footer in footers for _, size in footer.row_groups)),
        },
        'decimal_scale_drift': any(len(found) > 1 for found in scales.values()),
        'footers_read': len(footers),
    }
    if units:
        section['timestamp_units'] = dict(sorted(units.items()))
    return {name: value for name, value in section.items() if value is not None or name in ('dictionary_columns', 'timestamp_unit')}


def quantiles_of(ordered: list[int]) -> dict[str, int]:
    """The nearest-rank value at each named quantile of a rising list of counts."""
    return {name: ordered[round(level * (len(ordered) - 1))] for name, level in QUANTILES.items()} if ordered else {}


def most_common(values: Iterable[Any]) -> Any:
    counted = collections.Counter(value for value in values if value is not None)
    return counted.most_common(1)[0][0] if counted else None


def most(flags: Iterable[bool]) -> bool:
    counted = collections.Counter(bool(flag) for flag in flags)
    return counted[True] > counted[False]


def all_counts(table: json_paths.ProfiledTable) -> list[json_paths.TypeCounts]:
    return [*table.counts.values(), *(counts for element in table.elements.values() for counts in all_counts(element))]
