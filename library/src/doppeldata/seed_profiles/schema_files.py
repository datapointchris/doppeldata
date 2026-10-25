"""Reading a schema file a seed is imported from, as JSON or YAML, and the table name a file gives its seed."""

import json
import os
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from doppeldata.errors import SourceError
from doppeldata.profile_format import to_plain


def schema_document(path: str | os.PathLike[str]) -> tuple[Path, Any]:
    """The file at `path`, as plain JSON values: JSON where it ends in `.json`, else YAML, which JSON also parses as."""
    path = Path(path)
    try:
        text = path.read_text()
    except OSError as error:
        raise SourceError(f'{os.fspath(path)} cannot be read: {error.strerror or error}', role='schema') from error
    try:
        if path.suffix == '.json':
            return path, json.loads(text)
        return path, to_plain(YAML(typ='safe').load(text))
    except (json.JSONDecodeError, YAMLError) as error:
        raise SourceError(f'{path} is neither JSON nor YAML: {error}', role='schema') from error


def table_name(path: Path) -> str:
    """The name a seed from one file takes: the file's name up to its first dot, as a short YAML's table takes."""
    return path.name.split('.')[0]
