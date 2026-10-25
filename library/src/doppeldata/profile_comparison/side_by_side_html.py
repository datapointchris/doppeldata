"""The HTML showing the real data's sample beside the checked data's, column by column, rendered by sweetviz.

It shows each numeric or temporal column's smallest, largest and most frequent real values, and each categorical
column's real categories. So it is a view for someone who already holds the real data, and never one to share.
"""

import contextlib
import importlib.util
import io
from pathlib import Path
from typing import TYPE_CHECKING

from doppeldata.errors import PackagesMissing

if TYPE_CHECKING:
    import polars as pl

# sweetviz declares pandas, which the frames are handed over as.
HTML_PACKAGES = {'sweetviz': 'sweetviz'}


def missing_packages() -> None:
    """Refuse with PackagesMissing where a package the HTML imports is not installed."""
    missing = [distribution for distribution, module in HTML_PACKAGES.items() if importlib.util.find_spec(module) is None]
    if missing:
        raise PackagesMissing('comparison', missing, doing='writing the side-by-side HTML')


def write_side_by_side(real: 'pl.DataFrame', checked: 'pl.DataFrame', path: str | Path) -> None:
    """Write the HTML to `path`, from the frames the scores read, which hold no column a score leaves out."""
    import sweetviz

    # sweetviz prints its progress and a line naming the file, which would land in a report a command prints.
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        report = sweetviz.compare([real.to_pandas(), 'real data'], [checked.to_pandas(), 'checked data'])
        report.show_html(str(path), open_browser=False, layout='vertical')
