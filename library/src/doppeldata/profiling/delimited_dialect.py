"""A delimited source's dialect and the traits of its bytes, found from the first block of its first file holding a record.

DuckDB's sniffer reads first, and CleverCSV confirms it on the block. DuckDB misreads a file whose
rows disagree, as one with an unbalanced quote, a short row and a long row does, where CleverCSV
does not. So where the two disagree, CleverCSV's dialect is read, and where neither names one, the
format's own. The header is decided as AWS Glue's CSV classifier decides it. Every other file's
first record is checked against the dialect, so a source mixing two is refused.
"""

import codecs
import collections
import dataclasses as dc
import random
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
# Python's codec for each encoding DuckDB reads. A Latin-1 block is decoded as windows-1252, which the writers write.
PYTHON_CODECS = {'utf-8': 'utf-8', 'utf-16': 'utf-16', 'latin-1': 'cp1252'}
# The sniffers, the one read first leading.
SNIFFERS = ('duckdb', 'clevercsv')
# The most other files whose first record is checked against the dialect, from a seeded sample beyond that.
FILES_CHECKED = 64
FILES_CHECKED_SEED = 0


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
    detected_encoding, encoding = encodings_of(raw)
    text = block_text(raw, encoding)
    sniffed = duckdb_dialect(path, encoding, config)
    confirmed = clevercsv_dialect(text, delimiter)
    agree = sniffed is not None and confirmed is not None and read_as(sniffed)[:2] == read_as(confirmed)[:2]
    if sniffed is not None and (agree or confirmed is None):
        chosen, found = 'duckdb', sniffed
    elif confirmed is not None:
        chosen, found = 'clevercsv', confirmed
    else:
        chosen, found = 'default', (delimiter or ',', '', '')
    separator, quote, escape = read_as(found)
    if delimiter:
        separator = delimiter
    records = split_records(text, separator, quote, escape)
    header = glue_header(records)
    if header is None:
        header = duckdb_header(path, encoding, config) if sniffed else True
    dialect = CsvDialect(separator, quote, escape, header, encoding, column_names(records, header))
    check_first_records(files, path, dialect, records[0])
    return DialectFound(
        dialect=dialect,
        named=dict(zip(SNIFFERS, (sniffed, confirmed), strict=True)),
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


def encodings_of(raw: bytes) -> tuple[str | None, str]:
    """charset-normalizer's name for the block's encoding, and the encoding DuckDB reads the files in.

    A block that decodes as UTF-8 is read as UTF-8, and any other as Latin-1. The name decides nothing, because
    charset-normalizer names a short Latin-1 block `cp1250` or `cp775`.
    """
    if raw.startswith(UTF16_BOMS):
        return 'utf_16', 'utf-16'
    best = charset_normalizer.from_bytes(raw.removeprefix(UTF8_BOM)).best()
    try:
        # Not final, so a character the block's end cuts in two is no error.
        codecs.getincrementaldecoder('utf-8')().decode(raw, final=False)
    except UnicodeDecodeError:
        return best.encoding if best else None, 'latin-1'
    return best.encoding if best else None, 'utf-8'


def block_text(raw: bytes, encoding: str) -> str:
    """The block decoded, with no byte order mark, cut at its last line break where it ends mid-record."""
    text = raw.decode(PYTHON_CODECS[encoding], errors='replace').removeprefix(UTF8_BOM.decode())
    cut = max(text.rfind('\n'), text.rfind('\r'))
    return text[:cut] if len(raw) >= FIRST_BLOCK_BYTES and cut > 0 else text


def check_first_records(files: tuple[SourceFile, ...], path: str, dialect: CsvDialect, first: list[Field]) -> None:
    """Refuse a source whose files are in more than one dialect.

    Each other file's first record, read in the dialect, must be the header `path` starts with, or split into as many
    fields where there is none. A sample of `FILES_CHECKED` files is checked beyond that many.
    """
    others = [file for file in files if file.path != path]
    if len(others) > FILES_CHECKED:
        others = sorted(random.Random(FILES_CHECKED_SEED).sample(others, FILES_CHECKED), key=lambda file: file.path)
    names = [field.text for field in first]
    for file in others:
        raw = first_block(file.path)
        if not raw.removeprefix(UTF8_BOM).strip():
            continue
        record = split_records(block_text(raw, dialect.encoding), dialect.delimiter, dialect.quote, dialect.escape, limit=1)[0]
        if dialect.header and [field.text for field in record] != names:
            raise SourceError(f'{file.path} starts with another header than {path}, so the two are not one table', exit_code=1)
        if not dialect.header and len(record) != len(dialect.names):
            message = (
                f'{file.path} splits its first record into {len(record)} fields in the dialect read from {path}, '
                f'whose records hold {len(dialect.names)}'
            )
            raise SourceError(message, exit_code=1)


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


def split_records(text: str, delimiter: str, quote: str, escape: str, *, limit: int | None = None) -> list[list[Field]]:
    """The block's records, or its first `limit`, each field with whether it was quoted. A quote is a field's quote only
    where the field opens with it.
    """
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
            if limit is not None and len(records) >= limit:
                return records
            record, chars, quoted = [], [], False
        else:
            chars.append(char)
        index += 1
    if chars or record:
        record.append(Field(''.join(chars), quoted))
        records.append(record)
    return records


def glue_header(records: list[list[Field]]) -> bool | None:
    """Whether Glue's CSV classifier reads the first record as a header, or None where it cannot tell.

    It needs two columns and two data rows. The first record is a header when every field in it is text, each but
    the last is shorter than `GLUE_HEADER_FIELD_LENGTH`, every one is a Glue column name, and some later record holds
    a field that is not text. Where every later field is text too, Glue reads no header, which would read an all-text
    table's names as a row. So that case is None, and DuckDB's sniffer decides it.
    """
    if len(records) < 3 or len(records[0]) < 2:
        return None
    first = [field.text for field in records[0]]
    named = (
        all(is_text(name) for name in first)
        and all(len(name) < GLUE_HEADER_FIELD_LENGTH for name in first[:-1])
        and all(GLUE_COLUMN_NAME.fullmatch(name) for name in first)
    )
    if not named:
        return False
    return True if any(not is_text(field.text) for record in records[1:] for field in record) else None


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
