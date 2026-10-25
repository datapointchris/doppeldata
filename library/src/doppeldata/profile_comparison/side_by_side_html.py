"""The HTML showing the real data's sample beside the checked data's, column by column, rendered by sweetviz.

It shows each numeric or temporal column's smallest, largest and most frequent real values, and each categorical
column's real categories. So it is a view for someone who already holds the real data, and never one to share.
"""

import contextlib
import io
import os
from pathlib import Path
from typing import TYPE_CHECKING

from doppeldata.errors import PackagesMissing
from doppeldata.errors import uninstalled

if TYPE_CHECKING:
    import polars as pl

# sweetviz declares pandas, which the frames are handed over as.
HTML_PACKAGES = {'sweetviz': 'sweetviz'}


def missing_packages() -> None:
    """Refuse with PackagesMissing where a package the HTML imports is not installed."""
    missing = uninstalled(HTML_PACKAGES)
    if missing:
        listed = ' '.join(f'--with {name}' for name in missing)
        route = (
            f', or reinstall doppeldata-cli with {"it" if len(missing) == 1 else "them"}, as `uv tool install {listed} doppeldata-cli` does'
        )
        raise PackagesMissing('comparison', missing, doing='writing the side-by-side HTML', installed_by=route)


def unwritable(path: Path) -> str | None:
    """Why the HTML cannot be written to `path`, or None where it can."""
    if path.is_dir():
        return f'{path} is a directory; name a FILE'
    if not path.parent.is_dir():
        return f'{path.parent} is not a directory; create it, or name a FILE in one that exists'
    if not os.access(path if path.exists() else path.parent, os.W_OK):
        return f'{path} cannot be written'
    return None


def write_side_by_side(real: 'pl.DataFrame', checked: 'pl.DataFrame', path: str | Path) -> None:
    """Write the HTML to `path`, from the frames the scores read, which hold no column a score leaves out."""
    import sweetviz

    # sweetviz prints its progress and a line naming the file, which would land in a report a command prints.
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        report = sweetviz.compare([real.to_pandas(), 'real data'], [checked.to_pandas(), 'checked data'])
        report.show_html(str(path), open_browser=False, layout='vertical')
