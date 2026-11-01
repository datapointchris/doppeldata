"""A numeric column's sample fitted to each scipy.stats family the generator draws a tail from, ranked by AIC.

Imported only where scipy is installed, which the library never declares.
"""

import dataclasses as dc
import functools
import math
import warnings
from collections.abc import Callable
from typing import Any

import numpy as np
import pyarrow as pa
import scipy
from scipy import optimize
from scipy import stats

from doppeldata import _core

FAMILIES = _core.fitted_families()
# What a profile's `generated.fitter` records.
FITTER = f'scipy {scipy.__version__}'
# Where loc must stay below the least value, its search reaches this many spans of the values below it.
LOC_SEARCH_SPANS = 1_000.0
# A fit with a larger KS statistic is kept, and written `poor: true`.
POOR_FIT_KS = 0.1
# A column holding at least this share of zeros is the family mixed with a run of zeros, so the zeros are left out of the fit.
ZERO_INFLATION_RATE = 0.05
# A failed optimization is one candidate lost, never the profile.
FIT_FAILURES = (ValueError, RuntimeError, FloatingPointError, OverflowError, ZeroDivisionError)


@dc.dataclass(frozen=True)
class Fitted:
    family: str
    params: dict[str, float]
    ks: float
    aic: float
    rows: int
    zero_inflation: float | None

    @property
    def poor(self) -> bool:
        return self.ks > POOR_FIT_KS


@dc.dataclass(frozen=True)
class Candidate:
    family: str
    params: dict[str, float]
    log_likelihood: float
    free_parameters: int
    # The fitted CDF at each distinct value, and just below it, where the empirical CDF jumps.
    cdf_at: np.ndarray
    cdf_before: np.ndarray


def finite_values(column: pa.ChunkedArray) -> np.ndarray:
    """The column's values as the nearest doubles, with nulls, NaN and infinities left out as the profile counts them."""
    values = np.asarray(column.cast(pa.float64(), safe=False).to_numpy(zero_copy_only=False), dtype=np.float64)
    return values[np.isfinite(values)]


def best_fit(values: np.ndarray, *, integral: bool, low: float, high: float, zero_fraction: float) -> Fitted | None:
    """The family with the lowest AIC, fitted to the values from `low` to `high`, or None when no family fits.

    `low` and `high` are the column's trimmed extremes. A support parameter such as loguniform's `b` lands on
    the sample's own extreme, so a value fewer than k_min rows bound never reaches the fit. An integral column
    ranks every family by the mass it gives each integer: a discrete family's own, and a continuous family's
    over the unit interval the generator rounds to that integer. So AIC compares masses with masses.
    """
    x = np.sort(values[(values >= low) & (values <= high)])
    zero_inflation = None
    if zero_fraction >= ZERO_INFLATION_RATE and x.size:
        zero_inflation = float(np.mean(x == 0))
        x = x[x != 0]
    distinct, counts = np.unique(x, return_counts=True)
    if distinct.size < 2:
        return None
    reached = np.cumsum(counts) / x.size
    ecdf = (reached, reached - counts / x.size)
    fitters: list[Callable[[], Candidate]] = [
        functools.partial(DISCRETE_FITTERS[family], x, distinct)
        if discrete
        else functools.partial(continuous, family, x, distinct, integral)
        for family, discrete in FAMILIES.items()
        if integral or not discrete
    ]
    fits = []
    with warnings.catch_warnings(), np.errstate(all='ignore'):
        warnings.simplefilter('ignore')
        for fitter in fitters:
            try:
                candidate = fitter()
            except FIT_FAILURES:
                continue
            ks = float(max(np.max(np.abs(ecdf[0] - candidate.cdf_at)), np.max(np.abs(ecdf[1] - candidate.cdf_before))))
            aic = 2 * candidate.free_parameters - 2 * candidate.log_likelihood
            if math.isfinite(aic) and math.isfinite(ks) and all(map(math.isfinite, candidate.params.values())):
                fits.append(Fitted(candidate.family, candidate.params, ks, aic, int(x.size), zero_inflation))
    return min(fits, key=lambda fitted: fitted.aic, default=None)


def loguniform_parameters(x: np.ndarray) -> tuple[list[float], int]:
    """loguniform's a, b and loc by maximum likelihood, with scale fixed at 1, because scale only rescales a and b.

    For a given loc the likeliest a and b are the least and greatest values less loc, so only loc is searched.
    scipy's generic fit searches all three at once, and can stop far from the likeliest loc.
    """
    low, high = float(x[0]), float(x[-1])
    span = high - low

    def negative_log_likelihood(loc: float) -> float:
        return float(np.sum(np.log(x - loc)) + x.size * math.log(math.log((high - loc) / (low - loc))))

    bounds = (low - LOC_SEARCH_SPANS * span, low - 1e-9 * span)
    loc = float(optimize.minimize_scalar(negative_log_likelihood, bounds=bounds, method='bounded').x)
    return [low - loc, high - loc, loc, 1.0], 3


def scipy_parameters(family: str, x: np.ndarray) -> tuple[list[float], int]:
    fitted = [float(value) for value in getattr(stats, family).fit(x)]
    return fitted, len(fitted)


def continuous(family: str, x: np.ndarray, distinct: np.ndarray, integral: bool) -> Candidate:
    distribution = getattr(stats, family)
    fitted, free_parameters = loguniform_parameters(x) if family == 'loguniform' else scipy_parameters(family, x)
    frozen = distribution(*fitted)
    # scipy answers NaN for the support of parameters outside a family's domain, which the generator refuses too.
    if any(math.isnan(bound) for bound in frozen.support()):
        raise ValueError(f'{family} fitted parameters outside its domain')
    names = [*(distribution.shapes.split(', ') if distribution.shapes else []), 'loc', 'scale']
    params = dict(zip(names, fitted, strict=True))
    if integral:
        mass = unit_interval_mass(frozen, x)
        log_likelihood = float(np.sum(np.log(mass)))
        cdf_at, cdf_before = frozen.cdf(distinct + 0.5), frozen.cdf(distinct - 0.5)
    else:
        log_likelihood = float(np.sum(frozen.logpdf(x)))
        cdf_at = cdf_before = frozen.cdf(distinct)
    return Candidate(family, params, log_likelihood, free_parameters, cdf_at, cdf_before)


def unit_interval_mass(frozen: Any, x: np.ndarray) -> np.ndarray:
    """The probability of the unit interval centered on each integer.

    Above the median it is taken from the survival function, where a difference of CDFs near 1 cancels.
    """
    from_cdf = frozen.cdf(x + 0.5) - frozen.cdf(x - 0.5)
    from_sf = frozen.sf(x - 0.5) - frozen.sf(x + 0.5)
    return np.where(x > frozen.median(), from_sf, from_cdf)


def geometric(x: np.ndarray, distinct: np.ndarray) -> Candidate:
    """scipy's geom counts from 1, so `loc` shifts it to start at the least value, and `p` is its maximum-likelihood estimate."""
    loc = float(x[0] - 1)
    p = min(1.0, 1.0 / float(np.mean(x - loc)))
    frozen = stats.geom(p, loc=loc)
    return Candidate('geom', {'p': p, 'loc': loc}, float(np.sum(frozen.logpmf(x))), 2, frozen.cdf(distinct), frozen.cdf(distinct - 1))


def discrete_uniform(x: np.ndarray, distinct: np.ndarray) -> Candidate:
    """Every integer from the least value to the greatest equally likely, the maximum-likelihood randint."""
    low, high = float(x[0]), float(x[-1] + 1)
    frozen = stats.randint(low, high)
    params = {'low': low, 'high': high, 'loc': 0.0}
    return Candidate('randint', params, float(np.sum(frozen.logpmf(x))), 2, frozen.cdf(distinct), frozen.cdf(distinct - 1))


# scipy fits no discrete family generically, so each has its own maximum-likelihood estimate.
DISCRETE_FITTERS: dict[str, Callable[[np.ndarray, np.ndarray], Candidate]] = {'geom': geometric, 'randint': discrete_uniform}
