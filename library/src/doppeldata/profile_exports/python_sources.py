"""What the pandera, dataframely and PySpark exports share: a module's header and imports, a class's name, each
column's attribute, and a bound written as Python.
"""

import datetime as dt
import keyword
import re
from collections.abc import Iterable
from typing import Any

from doppeldata.profile_exports.contract_columns import ContractColumn
from doppeldata.profile_exports.contract_columns import ContractDocument

WORD = re.compile(r'[A-Za-z0-9]+')
# The standard-library imports a module's body may use, each written only where the body names it.
STANDARD_IMPORTS = (('dt.', 'import datetime as dt'), ('decimal.', 'import decimal'), ('Annotated[', 'from typing import Annotated'))


def class_name(name: str) -> str:
    """A table's name as a class name, such as `CarrierClaims` for `carrier_claims`."""
    joined = ''.join(word[:1].upper() + word[1:] for word in WORD.findall(name)) or 'Table'
    return f'Table{joined}' if joined[0].isdigit() else joined


def constant_name(name: str) -> str:
    """A table's name as a constant's, such as `CARRIER_CLAIMS` for `carrier_claims`."""
    joined = '_'.join(word.upper() for word in WORD.findall(name)) or 'TABLE'
    return f'TABLE_{joined}' if joined[0].isdigit() else joined


def attribute_names(columns: Iterable[ContractColumn], reserved: Iterable[str]) -> list[tuple[str, bool]]:
    """Each column's attribute, and whether it differs from the column's name, so the class gives the name as an alias.
    A name that is no identifier, is a keyword, starts with an underscore or names a method of the base class is
    written in snake case, with a suffix where two would collide."""
    taken = set(reserved)
    found = []
    for column in columns:
        name = column.name
        usable = name.isidentifier() and not keyword.iskeyword(name) and not name.startswith('_') and name not in taken
        attribute = name if usable else snake(name)
        base, suffix = attribute, 2
        while attribute in taken:
            attribute, suffix = f'{base}_{suffix}', suffix + 1
        taken.add(attribute)
        found.append((attribute, attribute != name))
    return found


def snake(name: str) -> str:
    words = '_'.join(word.lower() for word in WORD.findall(name)) or 'column'
    return f'column_{words}' if words[0].isdigit() or keyword.iskeyword(words) else words


def literal(value: Any, column: ContractColumn) -> str:
    """`value`, a bound or a listed value of `column`, written as Python of the column's type."""
    if column.logical_type == 'date' and isinstance(value, str | dt.date):
        day = value if isinstance(value, dt.date) else dt.date.fromisoformat(value[:10])
        return f'dt.date({day.year}, {day.month}, {day.day})'
    if column.logical_type == 'timestamp' and isinstance(value, str | dt.datetime):
        instant = value if isinstance(value, dt.datetime) else dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
        if instant.tzinfo is not None:
            instant = instant.astimezone(dt.UTC)
        parts = [instant.year, instant.month, instant.day, instant.hour, instant.minute, instant.second]
        if instant.microsecond:
            parts.append(instant.microsecond)
        written = ', '.join(map(str, parts))
        return f'dt.datetime({written}, tzinfo=dt.timezone.utc)' if column.timezone else f'dt.datetime({written})'
    decimal = column.decimal
    if decimal is not None and isinstance(value, int | float):
        return f"decimal.Decimal('{value:.{decimal[1]}f}')"
    return repr(value)


def module_source(document: ContractDocument, kind: str, notes: Iterable[str], third_party: Iterable[str], body: str) -> str:
    """A module: a docstring naming the profile, a comment naming each fact left out, the imports the body uses, then
    the body."""
    lines = [
        f'"""{kind} of {document.name}, exported from its doppeldata profile.',
        '',
        f'profile_sha256: {document.digest}',
        f'profile_version: {document.profile_version}',
        '"""',
        '',
    ]
    listed = list(notes)
    if listed:
        lines.append('# Left out:')
        lines.extend(f'# - {note}' for note in listed)
        lines.append('')
    standard = [line for marker, line in STANDARD_IMPORTS if marker in body]
    if standard:
        lines.extend([*sorted(standard, key=import_order), ''])
    lines.extend([*third_party, '', '', body.rstrip('\n'), ''])
    return '\n'.join(lines)


def import_order(line: str) -> tuple[int, str]:
    """Plain imports ahead of `from` imports, as isort sorts them."""
    return (line.startswith('from '), line)
