"""A row's influence on a Spearman correlation, written once over every engine's columns.

Each column is read over the rows holding both as its mid-distribution function less a half, `u` and `w`: its mean
rank, a tie at the mean of its first and last, less a half, over the rows, less a half. A row's influence on their
covariance is its product, less three times the covariance, plus what it moves the other column's mean above it: each
`g`. On a variance it is its square, less three times the variance, plus twice the column's own mean above it: each
`h`. The correlation's influence follows from the three by the quotient rule. Each engine computes the six terms its
own way, and the moments as columns or as numbers, since the arithmetic reads either.
"""

import dataclasses as dc
from typing import Any


@dc.dataclass(frozen=True)
class InfluenceTerms:
    """A row's six terms, each an engine's column: `u` and `w`, and each column's `g` and `h`, the sum of a centered
    column over the rows above the row in it, and half of it over the rows tied with it, over the rows. `g_x` sums `w`
    and `h_x` sums `u` in `x`'s order, and `g_y` sums `u` and `h_y` sums `w` in `y`'s."""

    u: Any
    w: Any
    g_x: Any
    h_x: Any
    g_y: Any
    h_y: Any


def row_influence(terms: InfluenceTerms, covariance: Any, spread_x: Any, spread_y: Any) -> Any:
    """A row's influence on the correlation of `u` and `w`, from its terms, their covariance and each one's variance."""
    u, w = terms.u, terms.w
    scale = (spread_x * spread_y) ** 0.5
    on_covariance = u * w + terms.g_x + terms.g_y - 3 * covariance
    on_x = u * u + 2 * terms.h_x - 3 * spread_x
    on_y = w * w + 2 * terms.h_y - 3 * spread_y
    return on_covariance / scale - covariance / scale / 2 * (on_x / spread_x + on_y / spread_y)
