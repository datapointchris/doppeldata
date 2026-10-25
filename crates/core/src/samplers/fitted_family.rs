//! A profile's `fit`, a scipy.stats family and its parameters, as a CDF and its inverse. scipy
//! shifts and scales a family's standard form by `loc` and `scale`, and a discrete family by `loc`
//! alone. Only families whose CDF and inverse are closed forms over libm are here, so a tail drawn
//! from one regenerates to the same bits on every platform.

use crate::profile::Fit;

#[derive(Debug, Clone, Copy)]
enum Standard {
    Normal,
    LogNormal { s: f64 },
    Exponential,
    Weibull { c: f64 },
    Pareto { b: f64 },
    Uniform,
    Logistic,
    LogUniform { a: f64, b: f64 },
    Triangular { c: f64 },
    Geometric { p: f64 },
    DiscreteUniform { low: f64, high: f64 },
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FittedFamily {
    standard: Standard,
    loc: f64,
    scale: f64,
}

impl FittedFamily {
    /// The fit as a distribution, or `None` for a family with no closed-form inverse here, such as
    /// `gamma`, `beta`, `poisson` or `skewnorm`. Such a column keeps a linear tail.
    pub(crate) fn from_fit(fit: &Fit) -> Result<Option<Self>, String> {
        let param = |name: &str| {
            fit.params
                .get(name)
                .copied()
                .ok_or_else(|| format!("{} has no `{name}` parameter", fit.family))
        };
        let positive = |name: &str| {
            let value = param(name)?;
            if value > 0.0 && value.is_finite() {
                Ok(value)
            } else {
                Err(format!("{} parameter `{name}` is {value}, not positive", fit.family))
            }
        };
        let standard = match fit.family.as_str() {
            "norm" => Standard::Normal,
            "lognorm" => Standard::LogNormal { s: positive("s")? },
            "expon" => Standard::Exponential,
            "weibull_min" => Standard::Weibull { c: positive("c")? },
            "pareto" => Standard::Pareto { b: positive("b")? },
            "uniform" => Standard::Uniform,
            "logistic" => Standard::Logistic,
            "loguniform" | "reciprocal" => {
                let (a, b) = (positive("a")?, positive("b")?);
                if b <= a {
                    return Err(format!("{} needs a {a} below b {b}", fit.family));
                }
                Standard::LogUniform { a, b }
            }
            "triang" => {
                let c = param("c")?;
                if !(0.0..=1.0).contains(&c) {
                    return Err(format!("triang parameter `c` is {c}, outside [0, 1]"));
                }
                Standard::Triangular { c }
            }
            "geom" => {
                let p = param("p")?;
                if !(p > 0.0 && p <= 1.0) {
                    return Err(format!("geom parameter `p` is {p}, outside (0, 1]"));
                }
                Standard::Geometric { p }
            }
            "randint" => {
                let (low, high) = (param("low")?.floor(), param("high")?.floor());
                if high <= low {
                    return Err(format!("randint needs low {low} below high {high}"));
                }
                Standard::DiscreteUniform { low, high }
            }
            _ => return Ok(None),
        };
        let scale = fit.params.get("scale").copied().unwrap_or(1.0);
        if !(scale > 0.0 && scale.is_finite()) {
            return Err(format!("{} parameter `scale` is {scale}, not positive", fit.family));
        }
        Ok(Some(Self {
            standard,
            loc: fit.params.get("loc").copied().unwrap_or(0.0),
            scale,
        }))
    }

    fn is_discrete(&self) -> bool {
        matches!(self.standard, Standard::Geometric { .. } | Standard::DiscreteUniform { .. })
    }

    /// P(X ≤ x).
    pub(crate) fn cdf(&self, x: f64) -> f64 {
        if self.is_discrete() {
            self.discrete_cdf((x - self.loc).floor())
        } else {
            self.continuous_cdf((x - self.loc) / self.scale)
        }
    }

    /// P(X < x), which differs from [`Self::cdf`] only at a discrete family's support points.
    pub(crate) fn cdf_below(&self, x: f64) -> f64 {
        if self.is_discrete() {
            self.discrete_cdf((x - self.loc).ceil() - 1.0)
        } else {
            self.cdf(x)
        }
    }

    /// The smallest x with P(X ≤ x) ≥ p, for p strictly inside (0, 1).
    pub(crate) fn inverse(&self, p: f64) -> f64 {
        let p = p.clamp(f64::MIN_POSITIVE, 1.0 - f64::EPSILON);
        let standard = match self.standard {
            Standard::Normal => normal_quantile(p),
            Standard::LogNormal { s } => libm::exp(s * normal_quantile(p)),
            Standard::Exponential => -libm::log1p(-p),
            Standard::Weibull { c } => libm::pow(-libm::log1p(-p), 1.0 / c),
            Standard::Pareto { b } => libm::pow(1.0 - p, -1.0 / b),
            Standard::Uniform => p,
            Standard::Logistic => libm::log(p / (1.0 - p)),
            Standard::LogUniform { a, b } => a * libm::pow(b / a, p),
            Standard::Triangular { c } if p < c => libm::sqrt(p * c),
            Standard::Triangular { c } => 1.0 - libm::sqrt((1.0 - p) * (1.0 - c)),
            Standard::Geometric { p: success } => (libm::log1p(-p) / libm::log1p(-success)).ceil().max(1.0),
            Standard::DiscreteUniform { low, high } => (low + (p * (high - low)).ceil() - 1.0).clamp(low, high - 1.0),
        };
        if self.is_discrete() {
            self.loc + standard
        } else {
            self.loc + self.scale * standard
        }
    }

    fn continuous_cdf(&self, y: f64) -> f64 {
        match self.standard {
            Standard::Normal => normal_cdf(y),
            Standard::LogNormal { s } if y > 0.0 => normal_cdf(libm::log(y) / s),
            Standard::Exponential if y > 0.0 => -libm::expm1(-y),
            Standard::Weibull { c } if y > 0.0 => -libm::expm1(-libm::pow(y, c)),
            Standard::Pareto { b } if y > 1.0 => 1.0 - libm::pow(y, -b),
            Standard::Uniform => y.clamp(0.0, 1.0),
            Standard::Logistic => 1.0 / (1.0 + libm::exp(-y)),
            Standard::LogUniform { a, b } => (libm::log(y.clamp(a, b) / a) / libm::log(b / a)).clamp(0.0, 1.0),
            Standard::Triangular { c } => {
                let y = y.clamp(0.0, 1.0);
                if y <= c && c > 0.0 {
                    y * y / c
                } else if c < 1.0 {
                    1.0 - (1.0 - y) * (1.0 - y) / (1.0 - c)
                } else {
                    y * y
                }
            }
            _ => 0.0,
        }
    }

    fn discrete_cdf(&self, k: f64) -> f64 {
        match self.standard {
            Standard::Geometric { p } if k >= 1.0 => -libm::expm1(k * libm::log1p(-p)),
            Standard::DiscreteUniform { low, high } => ((k - low + 1.0) / (high - low)).clamp(0.0, 1.0),
            _ => 0.0,
        }
    }
}

/// Φ(x), the standard normal CDF.
fn normal_cdf(x: f64) -> f64 {
    0.5 * libm::erfc(-x / std::f64::consts::SQRT_2)
}

/// Φ⁻¹(p) by Wichura's algorithm AS 241 (PPND16), accurate to about 1e-16, from log and sqrt alone.
fn normal_quantile(p: f64) -> f64 {
    const A: [f64; 8] = [
        3.3871328727963665,
        133.14166789178438,
        1971.5909503065513,
        13731.69376550946,
        45921.95393154987,
        67265.7709270087,
        33430.57558358813,
        2509.0809287301227,
    ];
    const B: [f64; 8] = [
        1.0,
        42.31333070160091,
        687.1870074920579,
        5394.196021424751,
        21213.794301586597,
        39307.89580009271,
        28729.085735721943,
        5226.495278852854,
    ];
    const C: [f64; 8] = [
        1.4234371107496835,
        4.630337846156546,
        5.769497221460691,
        3.6478483247632045,
        1.2704582524523684,
        0.2417807251774506,
        0.022723844989269184,
        0.0007745450142783414,
    ];
    const D: [f64; 8] = [
        1.0,
        2.053191626637759,
        1.6763848301838038,
        0.6897673349851,
        0.14810397642748008,
        0.015198666563616457,
        0.0005475938084995345,
        1.0507500716444169e-09,
    ];
    const E: [f64; 8] = [
        6.657904643501103,
        5.463784911164114,
        1.7848265399172913,
        0.29656057182850487,
        0.026532189526576124,
        0.0012426609473880784,
        2.7115555687434876e-05,
        2.0103343992922881e-07,
    ];
    const F: [f64; 8] = [
        1.0,
        0.599832206555888,
        0.1369298809227358,
        0.014875361290850615,
        0.0007868691311456133,
        1.8463183175100548e-05,
        1.421511758316446e-07,
        2.0442631033899397e-15,
    ];
    let ratio = |numerator: &[f64; 8], denominator: &[f64; 8], r: f64| horner(numerator, r) / horner(denominator, r);
    let q = p - 0.5;
    if q.abs() <= 0.425 {
        return q * ratio(&A, &B, 0.180625 - q * q);
    }
    let tail = if q < 0.0 { p } else { 1.0 - p };
    let r = libm::sqrt(-libm::log(tail));
    let magnitude = if r <= 5.0 { ratio(&C, &D, r - 1.6) } else { ratio(&E, &F, r - 5.0) };
    if q < 0.0 { -magnitude } else { magnitude }
}

/// A polynomial with coefficients from degree 0 up, at `x`.
fn horner(coefficients: &[f64; 8], x: f64) -> f64 {
    coefficients.iter().rev().fold(0.0, |sum, &c| sum * x + c)
}
