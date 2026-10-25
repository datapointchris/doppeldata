"""A delimited source's dialect and the traits of its bytes, found from the first block of its first file holding a record.

DuckDB's sniffer reads first, and CleverCSV confirms it on the block. DuckDB misreads a file whose
rows disagree, as one with an unbalanced quote, a short row and a long row does, where CleverCSV
does not. So where the two disagree, CleverCSV's dialect is read. The header is decided as AWS
Glue's CSV classifier decides it.
"""

import collections
import dataclasses as dc
import re
from typing import Any

import charset_normalizer
import clevercsv
import duckdb

from doppeldata.errors import SourceError
from doppeldata.profiling.backend import CsvDialect
from doppeldata.profiling.backend import DialectFound
from doppeldata.profiling.backend import SourceFile
from doppeldata.profiling.first_blocks import FIRST_BLOCK_BYTES
from doppeldata.profiling.first_blocks import first_block
from doppeldata.profiling.statistic_catalog import BOOLEAN_TEXT
from doppeldata.profiling.statistic_catalog import DECIMAL_TEXT

# The delimiters the built-in CSV classifier tries, which CleverCSV is held to, from the AWS Glue Developer Guide's
# "Defining and managing classifiers".
GLUE_DELIMITERS = (',', '|', '\t', ';', '\x01')
# That classifier reads a first row as a header only where each name but the last is shorter than this.
GLUE_HEADER_FIELD_LENGTH = 150
# The "Single-line string pattern" of the AWS Glue API's "Common data types", which a column name must match. Its
# surrogate pair range is every code point past U+FFFF.
GLUE_COLUMN_NAME = re.compile(r'[\u0020-\uD7FF\uE000-\uFFFD\U00010000-\U0010FFFF\t]*')
# DuckDB's sniffer writes a missing quote or escape as this text.
DUCKDB_NONE = '(empty)'
UTF8_BOM = b'\xef\xbb\xbf'
UTF16_BOMS = (b'\xff\xfe', b'\xfe\xff')
NUMBER = re.compile(DECIMAL_TEXT)
BOOLEAN = re.compile(BOOLEAN_TEXT)
# The encodings charset-normalizer names that DuckDB reads as Latin-1, and the writers write as windows-1252.
LATIN1_ENCODINGS = frozenset({'cp1252', 'latin_1', 'iso8859_15'})


@dc.dataclass(frozen=True)
class Field:
    text: str
    quoted: bool


def detected(files: tuple[SourceFile, ...], delimiter: str | None, config: dict[str, Any]) -> DialectFound:
    """The dialect the source's files are read with, from the first file holding a record. `delimiter` fixes the
    delimiter, as a TSV's is.
    """
    path, raw = first_record_block(files)
    bom = raw.startswith(UTF8_BOM)
    detected_encoding: str | None = 'utf_16'
    if raw.startswith(UTF16_BOMS):
        encoding = 'utf-16'
    else:
        best = charset_normalizer.from_bytes(raw.removeprefix(UTF8_BOM)).best()
        detected_encoding = best.encoding if best else None
        encoding = 'latin-1' if detected_encoding in LATIN1_ENCODINGS else 'utf-8'
    text = raw.decode({'utf-16': 'utf-16', 'latin-1': 'cp1252'}.get(encoding, 'utf-8'), errors='replace').removeprefix(UTF8_BOM.decode())
    if len(raw) >= FIRST_BLOCK_BYTES and max(text.rfind('\n'), text.rfind('\r')) > 0:
        # The block ends mid-line, and a partial record would read as a short row.
        text = text[: max(text.rfind('\n'), text.rfind('\r'))]
    sniffed = duckdb_dialect(path, encoding, config)
    confirmed = clevercsv_dialect(text, delimiter)
    agree = sniffed is not None and confirmed is not None and read_as(sniffed)[:2] == read_as(confirmed)[:2]
    chosen = 'duckdb' if agree or confirmed is None else 'clevercsv'
    found = sniffed if chosen == 'duckdb' else confirmed
    separator, quote, escape = read_as(found) if found else (delimiter or ',', '"', '"')
    if delimiter:
        separator = delimiter
    records = split_records(text, separator, quote, escape)
    header = glue_header(records)
    if header is None:
        header = duckdb_header(path, encoding, config) if sniffed else True
    dialect = CsvDialect(separator, quote, escape, header, encoding, column_names(records, header))
    return DialectFound(
        dialect=dialect,
        duckdb=sniffed,
        clevercsv=confirmed,
        agreed=agree,
        chosen=chosen,
        confidence=consistency(records),
        detected_encoding=detected_encoding,
        bom=bom,
        line_terminators=line_terminators(text),
        quoting=quoting_of(records),
        trailing_delimiter=bool(records) and all(len(record) > 1 and record[-1].text == '' and not record[-1].quoted for record in records),
    )


def first_record_block(files: tuple[SourceFile, ...]) -> tuple[str, bytes]:
    """The first file whose first block holds more than a byte order mark and white space, and that block."""
    for file in files:
        raw = first_block(file.path)
        if raw.removeprefix(UTF8_BOM).strip():
            return file.path, raw
    named = files[0].path if len(files) == 1 else f'every one of the {len(files)} files from {files[0].path}'
    raise SourceError(f'{named} holds no record, so no dialect can be read from it', exit_code=1)


def read_as(found: tuple[str, str, str]) -> tuple[str, str, str]:
    """A sniffer's delimiter, quote and escape as the files are read.

    A sniffer names no quote where the block quotes no field. Reading `"` then still reads a later file that quotes one,
    where reading none would split its quoted delimiters. DuckDB doubles a quote where no escape is named, as RFC 4180 does.
    """
    delimiter, quote, escape = found
    quote = quote or '"'
    return delimiter, quote, escape or quote


def duckdb_dialect(path: str, encoding: str, config: dict[str, Any]) -> tuple[str, str, str] | None:
    """DuckDB's delimiter, quote and escape for the file, with an empty quote or escape where it found none."""
    try:
        with duckdb.connect(config=config) as connection:
            relation = connection.sql(
                'select Delimiter, Quote, Escape from sniff_csv($path, encoding = $encoding)', params={'path': path, 'encoding': encoding}
            )
            rows = relation.fetchall() if relation is not None else []
    except duckdb.Error:
        return None
    if not rows:
        return None
    delimiter, quote, escape = ('' if value == DUCKDB_NONE else value for value in rows[0])
    return delimiter, quote, escape


def duckdb_header(path: str, encoding: str, config: dict[str, Any]) -> bool:
    with duckdb.connect(config=config) as connection:
        relation = connection.sql(
            'select HasHeader from sniff_csv($path, encoding = $encoding)', params={'path': path, 'encoding': encoding}
        )
        return bool(relation.fetchall()[0][0]) if relation is not None else True


def clevercsv_dialect(text: str, delimiter: str | None) -> tuple[str, str, str] | None:
    found = clevercsv.Detector().detect(text, delimiters=[delimiter] if delimiter else list(GLUE_DELIMITERS))
    if found is None or not found.delimiter:
        return None
    return found.delimiter, found.quotechar or '', found.escapechar or ''


def split_records(text: str, delimiter: str, quote: str, escape: str) -> list[list[Field]]:
    """The block's records, each field with whether it was quoted. A quote is a field's quote only where the field opens with it."""
    records: list[list[Field]] = []
    record: list[Field] = []
    chars: list[str] = []
    quoted = in_quotes = False
    index, end = 0, len(text)
    while index < end:
        char = text[index]
        following = text[index + 1] if index + 1 < end else ''
        if in_quotes:
            if char == escape and escape != quote and following:
                chars.append(following)
                index += 1
            elif char == quote and following == quote and escape == quote:
                chars.append(quote)
                index += 1
            elif char == quote:
                in_quotes = False
            else:
                chars.append(char)
        elif quote and char == quote and not chars:
            in_quotes = quoted = True
        elif char == delimiter:
            record.append(Field(''.join(chars), quoted))
            chars, quoted = [], False
        elif char in '\r\n':
            if char == '\r' and following == '\n':
                index += 1
            record.append(Field(''.join(chars), quoted))
            records.append(record)
            record, chars, quoted = [], [], False
        else:
            chars.append(char)
        index += 1
    if chars or record:
        record.append(Field(''.join(chars), quoted))
        records.append(record)
    return records


def glue_header(records: list[list[Field]]) -> bool | None:
    """Whether Glue's CSV classifier reads the first record as a header, or None where it classifies no CSV.

    It needs two columns and two data rows. The first record is a header when every field in it is text, each but
    the last is shorter than `GLUE_HEADER_FIELD_LENGTH`, every one is a Glue column name, and some later record holds
    a field that is not text.
    """
    if len(records) < 3 or len(records[0]) < 2:
        return None
    first = [field.text for field in records[0]]
    return (
        all(is_text(name) for name in first)
        and all(len(name) < GLUE_HEADER_FIELD_LENGTH for name in first[:-1])
        and all(GLUE_COLUMN_NAME.fullmatch(name) for name in first)
        and any(not is_text(field.text) for record in records[1:] for field in record)
    )


def is_text(value: str) -> bool:
    """Whether Glue would type the value as a string: neither a number nor a boolean."""
    return not (NUMBER.match(value) or BOOLEAN.match(value))


def column_names(records: list[list[Field]], header: bool) -> tuple[str, ...]:
    """The header's names, with an empty or repeated one numbered, or `column0` onward where there is no header."""
    width = collections.Counter(len(record) for record in records).most_common(1)[0][0] if records else 1
    if not header or not records:
        return tuple(f'column{index}' for index in range(width))
    names: list[str] = []
    for index, field in enumerate(records[0]):
        name = field.text or f'column{index}'
        while name in names:
            name = f'{name}_{index}'
        names.append(name)
    return tuple(names)


def consistency(records: list[list[Field]]) -> float:
    if not records:
        return 0.0
    _, modal = collections.Counter(len(record) for record in records).most_common(1)[0]
    return modal / len(records)


def line_terminators(text: str) -> dict[str, int]:
    crlf = text.count('\r\n')
    return {'lf': text.count('\n') - crlf, 'crlf': crlf}


def quoting_of(records: list[list[Field]]) -> str:
    """How the writer quoted: every field, every field that is not a number, or where a field needs it.

    A block quoting no field reads as `necessary`, because a writer that never quotes writes it alike.
    """
    fields = [field for record in records for field in record if field.text or field.quoted]
    if not any(field.quoted for field in fields):
        return 'necessary'
    if all(field.quoted for field in fields):
        return 'always'
    if all(field.quoted != bool(NUMBER.match(field.text)) for field in fields):
        return 'non_numeric'
    return 'necessary'
