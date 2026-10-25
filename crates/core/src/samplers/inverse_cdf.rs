//! A piecewise-linear inverse CDF over (level, value) knots. One uniform draw becomes one value,
//! whatever the knots came from.

/// Knots in level order, each level and value no lower than the one before. Two knots may share a
/// level, which makes a jump with no values between them.
#[derive(Debug, Clone, Default)]
pub(crate) struct Knots(Vec<(f64, f64)>);

impl Knots {
    /// Knots from a list already in order, or `None` when a value falls.
    pub(crate) fn from_sorted(knots: Vec<(f64, f64)>) -> Option<Self> {
        let rising = knots.windows(2).all(|pair| pair[0].0 <= pair[1].0 && pair[0].1 <= pair[1].1);
        rising.then_some(Self(knots))
    }

    pub(crate) fn first(&self) -> Option<(f64, f64)> {
        self.0.first().copied()
    }

    pub(crate) fn last(&self) -> Option<(f64, f64)> {
        self.0.last().copied()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Adds a knot where it keeps every value in order, and reports whether the knot is in place,
    /// added now or already present. A refused knot leaves the others as they were.
    #[must_use = "a refused knot is missing from the distribution"]
    pub(crate) fn insert(&mut self, level: f64, value: f64) -> bool {
        if !(0.0..=1.0).contains(&level) || !value.is_finite() {
            return false;
        }
        let at = self.0.partition_point(|&(l, v)| l < level || (l == level && v < value));
        if self.0.get(at) == Some(&(level, value)) {
            return true;
        }
        let after_previous = at == 0 || self.0[at - 1].1 <= value;
        let before_next = self.0.get(at).is_none_or(|&(_, next)| value <= next);
        if after_previous && before_next {
            self.0.insert(at, (level, value));
        }
        after_previous && before_next
    }

    /// Inserts each knot as [`Knots::insert`] does, and returns how many it refused.
    #[must_use = "a refused knot is missing from the distribution"]
    pub(crate) fn extend(&mut self, knots: impl IntoIterator<Item = (f64, f64)>) -> usize {
        knots.into_iter().filter(|&(level, value)| !self.insert(level, value)).count()
    }

    /// Adds a flat run at `value` from level `start` to `end`, clamped between the last knot below
    /// `value` and the first above it. Stored rates are rounded, so a run they place can reach past
    /// a neighboring knot by the rounding.
    pub(crate) fn insert_run(&mut self, start: f64, end: f64, value: f64) {
        let floor = self.0.iter().filter(|&&(_, v)| v < value).map(|&(l, _)| l).fold(0.0, f64::max);
        let ceiling = self.0.iter().filter(|&&(_, v)| v > value).map(|&(l, _)| l).fold(1.0, f64::min);
        let end = end.clamp(floor, ceiling);
        let start = start.clamp(floor, end);
        let placed = self.insert(start, value) && self.insert(end, value);
        debug_assert!(placed, "a run between its neighbors keeps every value in order");
    }
}

const BUCKETS: usize = 1024;

#[derive(Debug, Clone)]
pub(crate) struct InverseCdf {
    levels: Vec<f64>,
    values: Vec<f64>,
    slopes: Vec<f64>,
    /// For each bucket `b`, the count of knots at or below level `b / BUCKETS`, so a draw searches
    /// only the knots inside its bucket.
    buckets: Vec<u32>,
}

impl InverseCdf {
    pub(crate) fn new(knots: Knots) -> Self {
        let (levels, values): (Vec<f64>, Vec<f64>) = knots.0.into_iter().unzip();
        let slopes = levels
            .windows(2)
            .zip(values.windows(2))
            .map(|(l, v)| if l[1] > l[0] { (v[1] - v[0]) / (l[1] - l[0]) } else { 0.0 })
            .collect();
        let buckets = (0..=BUCKETS)
            .map(|b| {
                let edge = b as f64 / BUCKETS as f64;
                levels.partition_point(|&l| l <= edge) as u32
            })
            .collect();
        Self {
            levels,
            values,
            slopes,
            buckets,
        }
    }

    /// Each piece of the curve from level 0 to 1, as (level, value) at its start and at its end. The
    /// value is flat before the first knot and after the last.
    pub(crate) fn segments(&self) -> Vec<((f64, f64), (f64, f64))> {
        let (Some(&first), Some(&last)) = (self.values.first(), self.values.last()) else {
            return Vec::new();
        };
        let knots: Vec<(f64, f64)> = std::iter::once((0.0, first))
            .chain(self.levels.iter().copied().zip(self.values.iter().copied()))
            .chain(std::iter::once((1.0, last)))
            .collect();
        knots
            .windows(2)
            .map(|pair| (pair[0], pair[1]))
            .filter(|((start, _), (end, _))| end > start)
            .collect()
    }

    /// The value at level `u`, for `u` in [0, 1).
    pub(crate) fn value_at(&self, u: f64) -> f64 {
        let bucket = ((u * BUCKETS as f64) as usize).min(BUCKETS - 1);
        let (low, high) = (self.buckets[bucket] as usize, self.buckets[bucket + 1] as usize);
        let at = low + self.levels[low..high].partition_point(|&l| l <= u);
        match at {
            0 => self.values[0],
            at if at == self.levels.len() => self.values[at - 1],
            at => self.values[at - 1] + (u - self.levels[at - 1]) * self.slopes[at - 1],
        }
    }
}
