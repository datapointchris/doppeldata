//! A Gaussian copula over a group of numeric columns. The profile's Spearman values become Pearson
//! correlations, and each pair the profile leaves out is filled as conditionally independent given
//! the stated ones. Only where no such matrix exists is it repaired, to the valid matrix whose Spearman
//! values lie nearest the stated ones. Each row's correlated normals become one uniform level per
//! column. Every operation is IEEE arithmetic, `sqrt` or a libm function, so the levels are the same
//! bits on every platform.
//!
//! Two continuous columns hold the Spearman value `ρ` under the Pearson `2·sin(πρ/6)`. A column
//! many of whose rows hold one value ties their ranks, which lowers the Spearman value its pairs
//! hold. So a pair with such a column takes the Pearson whose values hold `ρ` with their ties, as
//! NORTA (Cario and Nelson, 1997) calibrates one: each column's tie-averaged ranks as a series in
//! Hermite polynomials of its normal, whose correlation is a power series in the Pearson. Two members
//! warped by one parent's value hold some Spearman value through that parent alone, which the series
//! holds as its constant term.

use std::f64::consts::PI;

use crate::rng::open_unit;
use crate::rng::rng_for;
use crate::rng::stream_key;
use crate::samplers::fitted_family::normal_cdf;
use crate::samplers::fitted_family::normal_quantile;

/// The least eigenvalue a repaired matrix keeps, so its Cholesky factor exists.
const EIGEN_FLOOR: f64 = 1e-6;
/// The most Jacobi sweeps the repair takes.
const JACOBI_SWEEPS: usize = 64;
/// The most sweeps the completion of unstated pairs takes, and the largest change of an entry that settles it.
const COMPLETION_SWEEPS: usize = 500;
const COMPLETION_TOLERANCE: f64 = 1e-12;
/// The most projections the repair of stated pairs no matrix holds takes, and the largest change of an entry that
/// settles it.
const REPAIR_SWEEPS: usize = 2000;
const REPAIR_TOLERANCE: f64 = 1e-10;
/// The most Levenberg–Marquardt steps the fit of a repaired group's Spearman values takes, the damping it starts from,
/// the factor that damping moves by, its least and most values, and the share of the squared misses a step must remove
/// for the fit to go on.
const FIT_STEPS: usize = 200;
const FIT_DAMPING: f64 = 1e-3;
const FIT_DAMPING_STEP: f64 = 10.0;
const FIT_DAMPING_LEAST: f64 = 1e-12;
const FIT_DAMPING_MOST: f64 = 1e8;
const FIT_TOLERANCE: f64 = 1e-12;
/// The normals a column's rank scores are integrated over: this many points, evenly spaced across ±`LATENT_REACH`.
const LATENT_POINTS: usize = 4001;
const LATENT_REACH: f64 = 8.0;
/// The Hermite polynomials a rank score's series holds.
const HERMITE_TERMS: usize = 256;
/// A column ties its ranks where one value spans more than this share of its levels.
pub(crate) const TIE_SHARE: f64 = 0.001;
/// The bisection steps that find a calibrated Pearson, each halving its interval.
const CALIBRATION_STEPS: usize = 60;
/// The largest Pearson magnitude a calibration reaches.
const PEARSON_REACH: f64 = 0.9999;

/// The normals a rank score is integrated over, their levels, and each one's share of the normal's mass.
pub(crate) struct LatentGrid {
    normals: Vec<f64>,
    pub(crate) levels: Vec<f64>,
    weights: Vec<f64>,
}

impl LatentGrid {
    pub(crate) fn new() -> Self {
        let step = 2.0 * LATENT_REACH / (LATENT_POINTS - 1) as f64;
        let normals: Vec<f64> = (0..LATENT_POINTS).map(|at| -LATENT_REACH + at as f64 * step).collect();
        let levels = normals.iter().map(|&z| normal_cdf(z)).collect();
        let density: Vec<f64> = normals.iter().map(|&z| libm::exp(-z * z / 2.0)).collect();
        let total: f64 = density.iter().sum();
        Self {
            normals,
            levels,
            weights: density.into_iter().map(|mass| mass / total).collect(),
        }
    }
}

/// One member's rank scores as the coefficients of their standardized series in the normalized Hermite polynomials
/// of its normal, the first left out, or `None` where its values hold no ties and the closed form holds.
#[derive(Debug, Clone)]
pub(crate) struct RankScores(Option<Vec<f64>>);

impl RankScores {
    /// The scores `scores` gives at each of `grid`'s levels, tied or not.
    pub(crate) fn of(grid: &LatentGrid, scores: &[f64], tied: bool) -> Self {
        if !tied {
            return Self(None);
        }
        CurveScores::of(grid, &[(1.0, scores)]).map_or(Self(None), |curves| curves.mixed())
    }

    /// The continuous column's scores, its level itself, as a tied column's partner reads them.
    fn continuous(grid: &LatentGrid) -> Self {
        Self::of(grid, &grid.levels, true)
    }
}

/// A member drawn through one of several curves, kept curve by curve, so that a partner whose rows choose their curves
/// by the same parent's value pairs each row's two curves.
#[derive(Debug, Clone)]
pub(crate) struct CurveScores {
    /// Each curve's share of rows, its mean score less the member's, and the coefficients of its scores in the
    /// normalized Hermite polynomials of the member's normal, the first left out.
    curves: Vec<(f64, f64, Vec<f64>)>,
    /// The spread of the member's scores over every curve.
    scale: f64,
}

impl CurveScores {
    /// The member whose rows are drawn through `curves`, each `(share, scores)` the share of rows drawn through it and
    /// its score at each of `grid`'s levels, or `None` where its scores do not vary.
    pub(crate) fn of(grid: &LatentGrid, curves: &[(f64, &[f64])]) -> Option<Self> {
        let total: f64 = curves.iter().map(|(share, _)| share).sum();
        let means: Vec<f64> = curves
            .iter()
            .map(|(_, scores)| grid.weights.iter().zip(*scores).map(|(w, s)| w * s).sum())
            .collect();
        let mean: f64 = curves.iter().zip(&means).map(|((share, _), m)| share / total * m).sum();
        let variance: f64 = curves
            .iter()
            .map(|(share, scores)| {
                share / total
                    * grid
                        .weights
                        .iter()
                        .zip(*scores)
                        .map(|(w, s)| w * (s - mean) * (s - mean))
                        .sum::<f64>()
            })
            .sum();
        if variance <= 0.0 {
            return None;
        }
        let curves = curves
            .iter()
            .zip(&means)
            .map(|((share, scores), curve_mean)| (share / total, curve_mean - mean, hermite(grid, scores, mean)))
            .collect();
        Some(Self {
            curves,
            scale: variance.sqrt(),
        })
    }

    /// The scores a partner reads where a row's curve is chosen apart from its partner's: the curves' share-weighted
    /// mean, standardized by the spread of every curve's scores.
    pub(crate) fn mixed(&self) -> RankScores {
        let mut coefficients = vec![0.0; HERMITE_TERMS];
        for (share, _, curve) in &self.curves {
            for (sum, c) in coefficients.iter_mut().zip(curve) {
                *sum += share * c;
            }
        }
        RankScores(Some(coefficients.into_iter().map(|c| c / self.scale).collect()))
    }
}

/// The coefficients of `scores` less `mean` in the normalized Hermite polynomials of the normal at `grid`'s points,
/// the first left out.
fn hermite(grid: &LatentGrid, scores: &[f64], mean: f64) -> Vec<f64> {
    let mut coefficients = vec![0.0; HERMITE_TERMS];
    for ((&z, &weight), &score) in grid.normals.iter().zip(&grid.weights).zip(scores) {
        let (mut previous, mut current) = (1.0, z);
        for (k, coefficient) in coefficients.iter_mut().enumerate() {
            *coefficient += weight * (score - mean) * current;
            let next = (z * current - ((k + 1) as f64).sqrt() * previous) / ((k + 2) as f64).sqrt();
            (previous, current) = (current, next);
        }
    }
    coefficients
}

/// The Spearman value two members hold as a function of their normals' Pearson `r`: the closed form where neither
/// ties, else a constant and the coefficient of each power of `r` from the first.
pub(crate) enum PairSeries {
    Closed,
    Series(f64, Vec<f64>),
}

impl PairSeries {
    /// Two members whose rows choose their curves apart.
    pub(crate) fn apart(a: &RankScores, b: &RankScores, grid: &LatentGrid) -> Self {
        let (a, b) = match (&a.0, &b.0) {
            (None, None) => return Self::Closed,
            (Some(_), None) => (a.clone(), RankScores::continuous(grid)),
            (None, Some(_)) => (RankScores::continuous(grid), b.clone()),
            (Some(_), Some(_)) => (a.clone(), b.clone()),
        };
        let (Some(a), Some(b)) = (a.0, b.0) else {
            unreachable!("both sides hold a series")
        };
        Self::Series(0.0, a.iter().zip(&b).map(|(x, y)| x * y).collect())
    }

    /// Two members whose rows choose their curves together, `joint` holding each pair of their curves' places and the
    /// share of rows choosing both. Their covariance within each pair of curves meets the covariance of the two
    /// curves' means, which no Pearson moves.
    pub(crate) fn joint(a: &CurveScores, b: &CurveScores, joint: &[(usize, usize, f64)]) -> Self {
        let scale = a.scale * b.scale;
        let mut constant = 0.0;
        let mut terms = vec![0.0; HERMITE_TERMS];
        for &(first, second, share) in joint {
            let ((_, mean_a, curve_a), (_, mean_b, curve_b)) = (&a.curves[first], &b.curves[second]);
            constant += share * mean_a * mean_b / scale;
            for ((term, x), y) in terms.iter_mut().zip(curve_a).zip(curve_b) {
                *term += share * x * y / scale;
            }
        }
        Self::Series(constant, terms)
    }

    /// The Spearman value the two members hold under the Pearson `r` between their normals.
    fn spearman(&self, r: f64) -> f64 {
        match self {
            Self::Closed => 6.0 / PI * libm::asin(r / 2.0),
            Self::Series(constant, terms) => {
                let mut power = 1.0;
                terms.iter().fold(*constant, |sum, term| {
                    power *= r;
                    sum + term * power
                })
            }
        }
    }

    /// The rate the Spearman value changes at the Pearson `r`.
    fn slope(&self, r: f64) -> f64 {
        match self {
            Self::Closed => 3.0 / PI / (1.0 - r * r / 4.0).sqrt(),
            Self::Series(_, terms) => {
                let mut power = 1.0;
                terms.iter().enumerate().fold(0.0, |sum, (at, term)| {
                    let added = sum + (at + 1) as f64 * term * power;
                    power *= r;
                    added
                })
            }
        }
    }
}

/// The Pearson between two members' normals under which their values hold the Spearman value `rho`, by the series
/// of their scores, or `None` where no Pearson within [`PEARSON_REACH`] holds it.
pub(crate) fn calibrated(rho: f64, series: &PairSeries) -> Option<f64> {
    if let PairSeries::Closed = series {
        return Some(2.0 * libm::sin(PI * rho / 6.0));
    }
    let (mut low, mut high) = (-PEARSON_REACH, PEARSON_REACH);
    if rho < series.spearman(low) || rho > series.spearman(high) {
        return None;
    }
    for _ in 0..CALIBRATION_STEPS {
        let middle = (low + high) / 2.0;
        if series.spearman(middle) < rho {
            low = middle;
        } else {
            high = middle;
        }
    }
    Some((low + high) / 2.0)
}

#[derive(Debug, Clone)]
pub(crate) struct CopulaGroup {
    /// The members' column indices, in the order of their sorted names.
    pub(crate) members: Vec<usize>,
    pub(crate) names: Vec<String>,
    /// The lower-triangular Cholesky factor of the members' Pearson matrix, row by row.
    factor: Vec<f64>,
    key: u64,
    /// Whether no valid matrix holds every stored pair, so the group holds the one nearest them.
    pub(crate) repaired: bool,
    /// Whether no valid matrix was found near the stored pairs either, so every member draws its level alone.
    pub(crate) independent: bool,
    /// The stored pairs, by the members' names, whose Spearman value their ties hold under no Pearson. Each enters the
    /// matrix at [`PEARSON_REACH`] with its value's sign, which a repaired group's fit moves.
    pub(crate) unreachable: Vec<(String, String)>,
}

impl CopulaGroup {
    /// The group of `members`, each its column index and name, with `spearman` giving each stored pair's value by the
    /// members' names, and `series` the series the Spearman value of two members, by their column indices, takes in
    /// their normals' Pearson. A pair it does not store takes the value `completed` fills.
    pub(crate) fn new(
        table: &str,
        mut members: Vec<(usize, String)>,
        spearman: impl Fn(&str, &str) -> Option<f64>,
        series: impl Fn(usize, usize) -> PairSeries,
    ) -> Self {
        members.sort_by(|a, b| a.1.cmp(&b.1));
        let k = members.len();
        let mut matrix = identity(k);
        let mut stated = vec![false; k * k];
        let mut pairs = Vec::new();
        let mut unreachable = Vec::new();
        for i in 0..k {
            for j in 0..i {
                let Some(rho) = spearman(&members[i].1, &members[j].1) else {
                    continue;
                };
                let rho = rho.clamp(-1.0, 1.0);
                let pair = series(members[i].0, members[j].0);
                let value = calibrated(rho, &pair).unwrap_or_else(|| {
                    unreachable.push((members[j].1.clone(), members[i].1.clone()));
                    PEARSON_REACH.copysign(rho)
                });
                (matrix[i * k + j], matrix[j * k + i]) = (value, value);
                (stated[i * k + j], stated[j * k + i]) = (true, true);
                pairs.push(StatedPair {
                    later: i,
                    earlier: j,
                    rho,
                    series: pair,
                });
            }
        }
        let (factor, repaired) = match completed(&matrix, &stated, k).and_then(|filled| cholesky(&filled, k)) {
            Some(factor) => (Some(factor), false),
            None => {
                let projected = cholesky(&nearest_on_stated(&matrix, &stated, k), k);
                (projected.map(|factor| spearman_fitted(factor, &pairs, k)), true)
            }
        };
        let names: Vec<String> = members.iter().map(|(_, name)| name.clone()).collect();
        Self {
            key: stream_key(table, &names.join("\0"), "copula"),
            members: members.into_iter().map(|(index, _)| index).collect(),
            names,
            independent: factor.is_none(),
            factor: factor.unwrap_or_else(|| identity(k)),
            repaired,
            unreachable,
        }
    }

    /// Each member's levels for the block's rows, in the members' order. A row draws one normal per member
    /// from the group's stream, multiplies them by the factor, and takes each result's normal CDF.
    pub(crate) fn levels(&self, seed: u64, block: u64, rows: usize) -> Vec<Vec<f64>> {
        let k = self.members.len();
        let mut rng = rng_for(seed, self.key, block);
        let mut levels = vec![Vec::with_capacity(rows); k];
        let mut normals = vec![0.0; k];
        for _ in 0..rows {
            for normal in &mut normals {
                *normal = normal_quantile(open_unit(&mut rng));
            }
            for (i, member) in levels.iter_mut().enumerate() {
                let row = &self.factor[i * k..i * k + i + 1];
                let z: f64 = row.iter().zip(&normals).map(|(l, n)| l * n).sum();
                member.push(normal_cdf(z).min(1.0 - f64::EPSILON));
            }
        }
        levels
    }
}

fn identity(k: usize) -> Vec<f64> {
    let mut matrix = vec![0.0; k * k];
    for i in 0..k {
        matrix[i * k + i] = 1.0;
    }
    matrix
}

/// The lower-triangular factor `L` with `L·Lᵀ = matrix`, or `None` where the matrix is not positive definite or holds
/// a value that is not finite.
fn cholesky(matrix: &[f64], k: usize) -> Option<Vec<f64>> {
    let mut factor = vec![0.0; k * k];
    for i in 0..k {
        for j in 0..=i {
            let sum = matrix[i * k + j] - (0..j).map(|m| factor[i * k + m] * factor[j * k + m]).sum::<f64>();
            if i == j {
                if sum.is_nan() || sum <= EIGEN_FLOOR / 2.0 {
                    return None;
                }
                factor[i * k + i] = sum.sqrt();
            } else {
                factor[i * k + j] = sum / factor[j * k + j];
            }
        }
    }
    Some(factor)
}

/// `matrix` with each pair `stated` leaves out filled so that its inverse is zero there, every stated entry kept: the
/// maximum-determinant completion of Dempster's covariance selection, a Gaussian graphical model whose missing edges
/// are conditional independences. Each sweep regresses every member on its stated partners, as Hastie, Tibshirani
/// and Friedman's Algorithm 17.1 does. `None` where a regression is singular, the sweeps do not settle, or a filled
/// value is not finite, as it becomes where no valid matrix holds the stated pairs.
fn completed(matrix: &[f64], stated: &[bool], k: usize) -> Option<Vec<f64>> {
    let mut filled = matrix.to_vec();
    for _ in 0..COMPLETION_SWEEPS {
        let mut moved = 0.0_f64;
        for j in 0..k {
            let partners: Vec<usize> = (0..k).filter(|&i| stated[i * k + j]).collect();
            let n = partners.len();
            let mut system = vec![0.0; n * n];
            for (a, &p) in partners.iter().enumerate() {
                for (b, &q) in partners.iter().enumerate() {
                    system[a * n + b] = filled[p * k + q];
                }
            }
            let weights = solved(system, partners.iter().map(|&p| matrix[p * k + j]).collect(), n)?;
            for i in (0..k).filter(|&i| i != j) {
                let value: f64 = partners.iter().zip(&weights).map(|(&p, w)| filled[i * k + p] * w).sum();
                if !value.is_finite() {
                    return None;
                }
                moved = moved.max((value - filled[i * k + j]).abs());
                (filled[i * k + j], filled[j * k + i]) = (value, value);
            }
        }
        if moved <= COMPLETION_TOLERANCE {
            return Some(filled);
        }
    }
    None
}

/// The `x` with `system·x = target`, `system` being `n × n` row by row, by Gaussian elimination with partial
/// pivoting. `None` where a pivot vanishes.
fn solved(mut system: Vec<f64>, mut target: Vec<f64>, n: usize) -> Option<Vec<f64>> {
    for column in 0..n {
        let pivot = (column..n).max_by(|&a, &b| system[a * n + column].abs().total_cmp(&system[b * n + column].abs()))?;
        if system[pivot * n + column].abs() <= EIGEN_FLOOR {
            return None;
        }
        for at in 0..n {
            system.swap(column * n + at, pivot * n + at);
        }
        target.swap(column, pivot);
        for row in column + 1..n {
            let factor = system[row * n + column] / system[column * n + column];
            for at in column..n {
                system[row * n + at] -= factor * system[column * n + at];
            }
            target[row] -= factor * target[column];
        }
    }
    let mut x = vec![0.0; n];
    for row in (0..n).rev() {
        let known: f64 = (row + 1..n).map(|at| system[row * n + at] * x[at]).sum();
        x[row] = (target[row] - known) / system[row * n + row];
    }
    Some(x)
}

/// The correlation matrix nearest `matrix` on the pairs `stated` holds, each other pair free: alternating projections
/// between the matrices whose eigenvalues reach [`EIGEN_FLOOR`] and those holding every stated pair and a unit
/// diagonal, which settle on the valid matrix nearest the stated pairs, as Higham (2002) finds a nearest correlation
/// matrix. It is rescaled to a unit diagonal last.
fn nearest_on_stated(matrix: &[f64], stated: &[bool], k: usize) -> Vec<f64> {
    let mut held = matrix.to_vec();
    for _ in 0..REPAIR_SWEEPS {
        let clipped = eigen_clipped(&held, k);
        let mut moved = 0.0_f64;
        for at in 0..k * k {
            let target = match (at % (k + 1) == 0, stated[at]) {
                (true, _) => 1.0,
                (false, true) => matrix[at],
                (false, false) => clipped[at],
            };
            moved = moved.max((target - held[at]).abs());
            held[at] = target;
        }
        if moved <= REPAIR_TOLERANCE {
            break;
        }
    }
    let mut rebuilt = eigen_clipped(&held, k);
    let scale: Vec<f64> = (0..k).map(|i| rebuilt[i * k + i].sqrt()).collect();
    for i in 0..k {
        for j in 0..k {
            rebuilt[i * k + j] = if i == j { 1.0 } else { rebuilt[i * k + j] / (scale[i] * scale[j]) };
        }
    }
    rebuilt
}

/// A pair of members a profile states, by their places in the group, its Spearman value, and the series that value
/// takes in their normals' Pearson.
struct StatedPair {
    later: usize,
    earlier: usize,
    rho: f64,
    series: PairSeries,
}

/// The Pearson between two members, the dot product of their rows of a factor whose rows are unit vectors.
fn row_product(rows: &[f64], first: usize, second: usize, k: usize) -> f64 {
    let shared = first.min(second);
    (0..=shared)
        .map(|m| rows[first * k + m] * rows[second * k + m])
        .sum::<f64>()
        .clamp(-1.0, 1.0)
}

/// Each stated pair's Spearman value under the factor `rows`, less the value stated.
fn spearman_misses(rows: &[f64], pairs: &[StatedPair], k: usize) -> Vec<f64> {
    pairs
        .iter()
        .map(|pair| pair.series.spearman(row_product(rows, pair.later, pair.earlier, k)) - pair.rho)
        .collect()
}

/// The lower-triangular `free` with each row scaled to a unit vector, and each row's length before.
fn unit_rows(free: &[f64], k: usize) -> (Vec<f64>, Vec<f64>) {
    let mut rows = free.to_vec();
    let mut lengths = vec![0.0; k];
    for i in 0..k {
        lengths[i] = (0..=i).map(|m| free[i * k + m] * free[i * k + m]).sum::<f64>().sqrt();
        for m in 0..=i {
            rows[i * k + m] = free[i * k + m] / lengths[i];
        }
    }
    (rows, lengths)
}

/// The factor whose unit rows put the stated pairs' Spearman values nearest those stated, by least squares through
/// each pair's series, which is what `data check` measures. Each row is a point on a sphere, as Rebonato and Jäckel
/// (1999) parametrize a correlation matrix, so every step stays a valid matrix and the factor needs no repair after.
/// Levenberg–Marquardt steps from `factor`, the projection's, in the pairs' space: `(J·Jᵀ + λ·I)·y = −e` and `δ = Jᵀ·y`,
/// since the stated pairs are fewer than the factor's entries. `factor` itself where no step improves on it.
fn spearman_fitted(factor: Vec<f64>, pairs: &[StatedPair], k: usize) -> Vec<f64> {
    let squared = |misses: &[f64]| misses.iter().map(|miss| miss * miss).sum::<f64>();
    let (mut free, mut rows, mut lengths) = (factor.clone(), factor.clone(), vec![1.0; k]);
    let mut misses = spearman_misses(&rows, pairs, k);
    let mut cost = squared(&misses);
    let (mut damping, mut best) = (FIT_DAMPING, None);
    let n = pairs.len();
    for _ in 0..FIT_STEPS {
        // Each pair's misses move with its two members' rows alone, so its gradient is kept as those two rows.
        let jacobian: Vec<[(usize, Vec<f64>); 2]> = pairs
            .iter()
            .map(|pair| {
                let (i, j) = (pair.later, pair.earlier);
                let r = row_product(&rows, i, j, k);
                let slope = pair.series.slope(r);
                let partial = |own: usize, other: usize| {
                    let row = (0..=own)
                        .map(|m| {
                            let across = if m <= other { rows[other * k + m] } else { 0.0 };
                            slope * (across - r * rows[own * k + m]) / lengths[own]
                        })
                        .collect();
                    (own, row)
                };
                [partial(i, j), partial(j, i)]
            })
            .collect();
        let mut system = vec![0.0; n * n];
        for p in 0..n {
            for q in 0..=p {
                let mut product = 0.0;
                for (own, row) in &jacobian[p] {
                    for (other, across) in &jacobian[q] {
                        if own == other {
                            product += row.iter().zip(across).map(|(a, b)| a * b).sum::<f64>();
                        }
                    }
                }
                (system[p * n + q], system[q * n + p]) = (product, product);
            }
        }
        let mut stepped = None;
        while damping <= FIT_DAMPING_MOST {
            let mut damped = system.clone();
            for p in 0..n {
                damped[p * n + p] += damping;
            }
            let moved = positive_definite_solved(&damped, &misses.iter().map(|miss| -miss).collect::<Vec<_>>(), n).map(|y| {
                let mut moved = free.clone();
                for (gradient, weight) in jacobian.iter().zip(&y) {
                    for (own, row) in gradient {
                        for (m, partial) in row.iter().enumerate() {
                            moved[own * k + m] += weight * partial;
                        }
                    }
                }
                moved
            });
            if let Some(moved) = moved {
                let (moved_rows, moved_lengths) = unit_rows(&moved, k);
                let moved_misses = spearman_misses(&moved_rows, pairs, k);
                let moved_cost = squared(&moved_misses);
                if moved_cost < cost {
                    stepped = Some((moved, moved_rows, moved_lengths, moved_misses, moved_cost));
                    damping = (damping / FIT_DAMPING_STEP).max(FIT_DAMPING_LEAST);
                    break;
                }
            }
            damping *= FIT_DAMPING_STEP;
        }
        let Some((moved, moved_rows, moved_lengths, moved_misses, moved_cost)) = stepped else {
            break;
        };
        let settled = cost - moved_cost <= FIT_TOLERANCE * cost;
        (free, rows, lengths, misses, cost) = (moved, moved_rows, moved_lengths, moved_misses, moved_cost);
        best = Some(rows.clone());
        if settled {
            break;
        }
    }
    best.unwrap_or(factor)
}

/// The `x` with `system·x = target`, `system` being symmetric positive definite and `n × n` row by row, by its Cholesky
/// factor. `None` where a pivot is not positive.
fn positive_definite_solved(system: &[f64], target: &[f64], n: usize) -> Option<Vec<f64>> {
    let mut factor = vec![0.0; n * n];
    for i in 0..n {
        for j in 0..=i {
            let sum = system[i * n + j] - (0..j).map(|m| factor[i * n + m] * factor[j * n + m]).sum::<f64>();
            if i == j {
                if sum.is_nan() || sum <= 0.0 {
                    return None;
                }
                factor[i * n + i] = sum.sqrt();
            } else {
                factor[i * n + j] = sum / factor[j * n + j];
            }
        }
    }
    let mut forward = vec![0.0; n];
    for i in 0..n {
        let known: f64 = (0..i).map(|m| factor[i * n + m] * forward[m]).sum();
        forward[i] = (target[i] - known) / factor[i * n + i];
    }
    let mut x = vec![0.0; n];
    for i in (0..n).rev() {
        let known: f64 = (i + 1..n).map(|m| factor[m * n + i] * x[m]).sum();
        x[i] = (forward[i] - known) / factor[i * n + i];
    }
    Some(x)
}

/// The symmetric `matrix` with each eigenvalue raised to at least [`EIGEN_FLOOR`].
fn eigen_clipped(matrix: &[f64], k: usize) -> Vec<f64> {
    let (values, vectors) = symmetric_eigen(matrix, k);
    let clipped: Vec<f64> = values.iter().map(|&value| value.max(EIGEN_FLOOR)).collect();
    let mut rebuilt = vec![0.0; k * k];
    for i in 0..k {
        for j in 0..k {
            rebuilt[i * k + j] = (0..k).map(|m| vectors[i * k + m] * clipped[m] * vectors[j * k + m]).sum();
        }
    }
    rebuilt
}

/// The eigenvalues of a symmetric matrix and its eigenvectors as columns, by cyclic Jacobi rotations. The sweeps stop
/// once what is left off the diagonal is under the rounding of what is on it.
fn symmetric_eigen(matrix: &[f64], k: usize) -> (Vec<f64>, Vec<f64>) {
    let mut a = matrix.to_vec();
    let mut v = identity(k);
    for _ in 0..JACOBI_SWEEPS {
        let off: f64 = (0..k)
            .flat_map(|p| (p + 1..k).map(move |q| (p, q)))
            .map(|(p, q)| a[p * k + q] * a[p * k + q])
            .sum();
        let diagonal: f64 = (0..k).map(|p| a[p * k + p] * a[p * k + p]).sum();
        if off <= f64::EPSILON * f64::EPSILON * diagonal {
            break;
        }
        for p in 0..k {
            for q in p + 1..k {
                let apq = a[p * k + q];
                if apq == 0.0 {
                    continue;
                }
                let theta = (a[q * k + q] - a[p * k + p]) / (2.0 * apq);
                let t = theta.signum() / (theta.abs() + (theta * theta + 1.0).sqrt());
                let c = 1.0 / (t * t + 1.0).sqrt();
                let s = t * c;
                for r in 0..k {
                    let (arp, arq) = (a[r * k + p], a[r * k + q]);
                    a[r * k + p] = c * arp - s * arq;
                    a[r * k + q] = s * arp + c * arq;
                }
                for r in 0..k {
                    let (apr, aqr) = (a[p * k + r], a[q * k + r]);
                    a[p * k + r] = c * apr - s * aqr;
                    a[q * k + r] = s * apr + c * aqr;
                }
                for r in 0..k {
                    let (vrp, vrq) = (v[r * k + p], v[r * k + q]);
                    v[r * k + p] = c * vrp - s * vrq;
                    v[r * k + q] = s * vrp + c * vrq;
                }
            }
        }
    }
    ((0..k).map(|i| a[i * k + i]).collect(), v)
}
