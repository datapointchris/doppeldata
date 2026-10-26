"""Hive's type names, as the Glue Data Catalog, Spark's `simpleString` and most ODCS `physicalType`s write them, read as
declared columns: `bigint`, `decimal(12,2)`, `varchar(18)`, `array<struct<sku:string,qty:int>>`.

A type the engine draws nothing of, such as `binary` or `map<string,int>`, reads as None, and its column is left out.
"""

import dataclasses as dc
import re

from doppeldata.seed_profiles.declared_columns import DeclaredColumn

# Each scalar's Arrow type and the profile's logical type for it.
SCALARS = {
    'tinyint': ('int8', 'integer'),
    'byte': ('int8', 'integer'),
    'smallint': ('int16', 'integer'),
    'short': ('int16', 'integer'),
    'int': ('int32', 'integer'),
    'integer': ('int32', 'integer'),
    'bigint': ('int64', 'integer'),
    'long': ('int64', 'integer'),
    'float': ('float', 'number'),
    'real': ('float', 'number'),
    'double': ('double', 'number'),
    'string': ('string', 'string'),
    'boolean': ('bool', 'boolean'),
    'bool': ('bool', 'boolean'),
    'date': ('date32[day]', 'date'),
    'timestamp': ('timestamp[us]', 'timestamp'),
    'timestamp_ntz': ('timestamp[us]', 'timestamp'),
}
# Hive's precision and scale for a `decimal` that names neither.
DECIMAL_DEFAULT = (10, 0)
WORD = re.compile(r'\s*([A-Za-z_][A-Za-z0-9_]*)\s*')
ARGUMENTS = re.compile(r'\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)\s*')


class UnreadType(ValueError):
    """A type name that is not Hive's, or one the engine draws nothing of."""


def hive_column(name: str, type_name: str, *, required: bool = False) -> DeclaredColumn | None:
    """The column a Hive type declares, or None for a type the engine draws nothing of, such as `binary`."""
    try:
        column, rest = parsed(name, type_name.strip(), 0)
    except ValueError:
        return None
    if rest != len(type_name.strip()):
        return None
    return dc.replace(column, required=required)


def parsed(name: str, text: str, at: int) -> tuple[DeclaredColumn, int]:
    """The column the type at `text[at:]` declares, and where it ends."""
    word = WORD.match(text, at)
    if word is None:
        raise UnreadType(f'{text!r} holds no type name at {at}')
    head, at = word[1].lower(), word.end()
    if head in ('array', 'struct'):
        at = expect(text, at, '<')
        if head == 'array':
            element, at = parsed('element', text, at)
            return DeclaredColumn(name=name, logical_type='array', element=element), expect(text, at, '>')
        fields = []
        while True:
            colon = text.index(':', at)
            field_name = text[at:colon].strip().strip('`')
            field, at = parsed(field_name, text, colon + 1)
            fields.append(field)
            if text[at : at + 1] == ',':
                at += 1
                continue
            return DeclaredColumn(name=name, logical_type='object', fields=tuple(fields)), expect(text, at, '>')
    arguments = ARGUMENTS.match(text, at)
    if arguments is not None:
        at = arguments.end()
    if head in ('decimal', 'numeric', 'dec'):
        precision, scale = (int(arguments[1]), int(arguments[2] or 0)) if arguments else DECIMAL_DEFAULT
        return DeclaredColumn(name=name, logical_type='number', physical_type=f'decimal128({precision}, {scale})'), at
    if head in ('varchar', 'char'):
        length = int(arguments[1]) if arguments else None
        least = length if head == 'char' else None
        return DeclaredColumn(name=name, logical_type='string', min_length=least, max_length=length), at
    if head not in SCALARS:
        raise UnreadType(f'{head} is a type the engine draws nothing of')
    physical, logical = SCALARS[head]
    return DeclaredColumn(name=name, logical_type=logical, physical_type=physical), at


def expect(text: str, at: int, character: str) -> int:
    stripped = len(text) - len(text[at:].lstrip())
    if text[stripped : stripped + 1] != character:
        raise UnreadType(f'{text!r} has no {character!r} at {stripped}')
    return stripped + 1
