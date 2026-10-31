//! The child rows each parent holds, by the parent's rank, busiest first.
//!
//! A curve over the continuous rank t in [0, P] gives the rows at each rank, and rank k holds what
//! its span (k − 1, k] integrates to, rounded so the counts sum to the matched rows exactly. Each
//! `top_share` pin sits at its rank times the table's scale, and the curve integrates to its share
//! there. Each band between two pins bends the curve between its quantiles until it holds its rows,
//! so the quantiles stay as stated, and is scaled only past that.

use crate::profile::Fanout;
use crate::profile::TopShare;
use crate::samplers::quantile_knots;
/// The steepest power law fitted above the top quantile.
const STEEPEST: f64 = 20.0;
const FIT_STEPS: usize = 64;
/// The furthest a segment between two quantiles bends from a straight line, either way.
const MOST_BEND: f64 = 64.0;
const SEEK_STEPS: usize = 12;
/// Rows short of a slot that end the search for its rank, which `rank_of` then settles exactly.
const CLOSE_ENOUGH: f64 = 0.01;
/// The most parents whose counts are held per rank, one `u64` each, so a row's rank is a search
/// over integers rather than a curve's inverse. A bent piece's inverse takes up to `SEEK_STEPS`
/// powers.
const TABULATED_PARENTS: u64 = 1 << 22;

#[derive(Debug, Clone, Copy)]
enum Shape {
    /// `value` at the piece's start, changing by `slope` per rank.
    Linear { value: f64, slope: f64 },
    /// `value · (t / at)^−exponent`.
    Power { value: f64, at: f64, exponent: f64 },
    /// `high + (low − high) · u^bend`, u running from 0 at `origin` to 1 at `origin + span`.
    Bent {
        origin: f64,
        span: f64,
        high: f64,
        low: f64,
        bend: f64,
    },
}

impl Shape {
    /// The rows between the piece's `start` and `x`.
    fn area(self, start: f64, x: f64) -> f64 {
        match self {
            Self::Linear { value, slope } => {
                let span = x - start;
                value * span + slope * span * span / 2.0
            }
            Self::Bent { .. } => self.bent_rows(x) - self.bent_rows(start),
            Self::Power { value, at, exponent } => {
                let scale = value * libm::pow(at, exponent);
                let rise = 1.0 - exponent;
                if rise.abs() < 1e-12 {
                    scale * libm::log(x / start)
                } else {
                    scale * (libm::pow(x, rise) - libm::pow(start, rise)) / rise
                }
            }
        }
    }

    /// The `x` past `start` whose area from `start` is `area`, near enough to start a search from.
    fn reaching(self, start: f64, area: f64) -> f64 {
        match self {
            Self::Linear { value, slope } => {
                let root = value * value + 2.0 * slope * area;
                let below = value + root.max(0.0).sqrt();
                if below > 0.0 { start + 2.0 * area / below } else { start }
            }
            Self::Bent { origin, span, .. } => {
                let wanted = self.bent_rows(start) + area;
                let (mut low, mut high, mut x) = (start, origin + span, start);
                for _ in 0..SEEK_STEPS {
                    let short = wanted - self.bent_rows(x);
                    if short.abs() < CLOSE_ENOUGH {
                        break;
                    }
                    if short > 0.0 {
                        low = x
                    } else {
                        high = x
                    }
                    let value = self.bent_value(x);
                    let step = if value > 0.0 { x + short / value } else { f64::NAN };
                    x = if step > low && step < high { step } else { (low + high) / 2.0 };
                }
                x
            }
            Self::Power { value, at, exponent } => {
                let scale = value * libm::pow(at, exponent);
                let rise = 1.0 - exponent;
                if rise.abs() < 1e-12 {
                    start * libm::exp(area / scale)
                } else {
                    libm::pow((libm::pow(start, rise) + area * rise / scale).max(0.0), 1.0 / rise)
                }
            }
        }
    }

    /// The same curve, from `split` on.
    fn from(self, start: f64, split: f64) -> Self {
        match self {
            Self::Linear { value, slope } => Self::Linear {
                value: value + slope * (split - start),
                slope,
            },
            other @ (Self::Power { .. } | Self::Bent { .. }) => other,
        }
    }

    fn bent(self, bend: f64) -> Self {
        match self {
            Self::Bent {
                origin, span, high, low, ..
            } => Self::Bent {
                origin,
                span,
                high,
                low,
                bend,
            },
            other => other,
        }
    }

    /// The rows per rank at `x`, of the piece starting at `start`.
    fn value(self, start: f64, x: f64) -> f64 {
        match self {
            Self::Linear { value, slope } => value + slope * (x - start),
            Self::Power { value, at, exponent } => value * libm::pow(x / at, -exponent),
            Self::Bent { .. } => self.bent_value(x),
        }
    }

    fn bent_value(self, x: f64) -> f64 {
        let Self::Bent {
            origin,
            span,
            high,
            low,
            bend,
        } = self
        else {
            return 0.0;
        };
        let u = ((x - origin) / span).clamp(0.0, 1.0);
        high + (low - high) * libm::pow(u, bend)
    }

    /// The rows from `origin` to `x`.
    fn bent_rows(self, x: f64) -> f64 {
        let Self::Bent {
            origin,
            span,
            high,
            low,
            bend,
        } = self
        else {
            return 0.0;
        };
        let u = ((x - origin) / span).clamp(0.0, 1.0);
        span * (high * u + (low - high) * libm::pow(u, bend + 1.0) / (bend + 1.0))
    }
}

#[derive(Debug, Clone, Copy)]
struct Piece {
    start: f64,
    end: f64,
    shape: Shape,
    factor: f64,
    /// The rows of every rank before `start`.
    before: f64,
    /// The rows of every rank before `end`.
    after: f64,
}

impl Piece {
    fn rows_to(&self, x: f64) -> f64 {
        self.before + self.factor * self.shape.area(self.start, x)
    }
}

/// The child rows of each of a relationship's parents, by rank.
#[derive(Debug, Clone)]
pub(crate) struct RankedCounts {
    parents: u64,
    matched: u64,
    pieces: Vec<Piece>,
    tabulated: Option<Tabulated>,
}

/// The counts held per rank, where the parents are at most `TABULATED_PARENTS`.
#[derive(Debug, Clone)]
struct Tabulated {
    /// `cumulative` at every rank, from 0.
    cumulative: Vec<u64>,
    /// The matched slots each of `starts` covers.
    width: u64,
    /// The rank holding the first slot of each run of `width`, so a slot's rank lies between its
    /// run's start and the next run's.
    starts: Vec<u32>,
}

impl Tabulated {
    fn new(cumulative: Vec<u64>, matched: u64) -> Self {
        let parents = cumulative.len() as u64 - 1;
        let width = matched.div_ceil(parents.max(1)).max(1);
        let mut starts = Vec::with_capacity((matched / width) as usize + 2);
        let mut rank = 0;
        for run in 0..=matched / width + 1 {
            let slot = run * width;
            while rank < parents && cumulative[rank as usize] <= slot {
                rank += 1;
            }
            starts.push(rank as u32);
        }
        Self { cumulative, width, starts }
    }

    fn rank_of(&self, slot: u64) -> u64 {
        let run = (slot / self.width) as usize;
        let (low, high) = (self.starts[run] as usize, self.starts[run + 1] as usize);
        (low + self.cumulative[low..=high].partition_point(|&rows| rows <= slot)) as u64
    }
}

impl RankedCounts {
    /// The counts `fanout` describes, for `matched` child rows over `parents` parents. A table
    /// generated at `scale` pins `top10` at `10 × scale` parents, so the busiest same fraction of
    /// them holds the share, as each quantile already is a fraction. A pin on under one parent is
    /// left out.
    pub(crate) fn new(fanout: Option<&Fanout>, parents: u64, matched: u64, scale: f64) -> Result<Self, String> {
        let total = parents as f64;
        let mut points = Vec::new();
        for (level, value) in quantile_knots(fanout.and_then(|f| f.quantiles.as_ref()))?.into_iter().rev() {
            if !value.is_finite() || value < 0.0 {
                return Err(format!("a fan-out quantile of {value} is not a count of rows"));
            }
            points.push((total * (1.0 - level), value));
        }
        if points.windows(2).any(|pair| pair[1].1 > pair[0].1) {
            return Err("the fan-out quantiles fall".into());
        }
        let shares = fanout.and_then(|f| f.top_share.as_ref()).map(TopShare::pins).unwrap_or_default();
        let mut pins: Vec<(f64, f64)> = Vec::new();
        for (name, rank, share) in shares {
            let at = rank as f64 * scale;
            if at < 1.0 || at >= total {
                continue;
            }
            let rows = share.0 * matched as f64;
            if pins.last().is_some_and(|&(_, below)| rows < below) {
                return Err(format!("top_share.{name} is below a busier rank's share"));
            }
            pins.push((at, rows));
        }
        let zipf = fanout.and_then(|f| f.zipf_s).filter(|s| s.is_finite() && *s >= 0.0);
        let (last_from, last_rows) = pins.last().map_or((0.0, 0.0), |&(rank, rows)| (rank, rows));
        let last_wanted = matched as f64 - last_rows;
        let top = points.first().map_or(0.0, |&(rank, _)| rank);
        let exponent = match points.is_empty() {
            true => zipf,
            false if last_from < top => {
                let steepest = falling_from_pins(&pins, points[0]).unwrap_or(STEEPEST);
                Some(fitted_exponent(&points, total, last_from, last_wanted).min(steepest))
            }
            false => Some(pinned_exponent(&pins, points[0]).or(zipf).unwrap_or(0.0)),
        };
        let base = base_curve(&points, exponent, total);
        let mut bounds: Vec<(f64, f64)> = pins.clone();
        bounds.push((total, matched as f64));
        let mut pieces = Vec::new();
        let mut bands = Vec::new();
        let (mut from, mut rows_before) = (0.0, 0.0);
        for (to, rows_to) in bounds {
            let first = pieces.len();
            let band: Vec<(f64, f64, Shape)> = base
                .iter()
                .filter(|(start, end, _)| *end > from && *start < to)
                .map(|&(start, end, shape)| {
                    let (low, high) = (start.max(from), end.min(to));
                    (low, high, shape.from(start, low))
                })
                .collect();
            let wanted = rows_to - rows_before;
            let bend = band_bend(&band, wanted);
            let band: Vec<(f64, f64, Shape)> = band.into_iter().map(|(low, high, shape)| (low, high, shape.bent(bend))).collect();
            let area: f64 = band.iter().map(|&(low, high, shape)| shape.area(low, high)).sum();
            let filled: Vec<(f64, f64, Shape, f64)> = match area > 0.0 {
                true => band
                    .into_iter()
                    .map(|(low, high, shape)| (low, high, shape, wanted / area))
                    .collect(),
                false if to > from => vec![(
                    from,
                    to,
                    Shape::Linear {
                        value: wanted / (to - from),
                        slope: 0.0,
                    },
                    1.0,
                )],
                false => Vec::new(),
            };
            let mut before = rows_before;
            for (low, high, shape, factor) in filled {
                let after = before + factor * shape.area(low, high);
                pieces.push(Piece {
                    start: low,
                    end: high,
                    shape,
                    factor,
                    before,
                    after,
                });
                before = after;
            }
            bands.push(Band {
                from,
                to,
                rows_before,
                wanted,
                pieces: first..pieces.len(),
            });
            (from, rows_before) = (to, rows_to);
        }
        let free = bands.iter().take_while(|band| band.to <= top).count();
        let mut counts = Self {
            parents,
            matched,
            pieces: falling(pieces, &bands, free),
            tabulated: None,
        };
        if parents <= TABULATED_PARENTS {
            let cumulative = (0..=parents).map(|rank| counts.on_curve(rank)).collect();
            counts.tabulated = Some(Tabulated::new(cumulative, matched));
        }
        Ok(counts)
    }

    /// The child rows of ranks 1 to `rank`, rounded, so rank k holds `cumulative(k) − cumulative(k − 1)`.
    pub(crate) fn cumulative(&self, rank: u64) -> u64 {
        match &self.tabulated {
            Some(tabulated) => tabulated.cumulative[rank.min(self.parents) as usize],
            None => self.on_curve(rank),
        }
    }

    fn on_curve(&self, rank: u64) -> u64 {
        if rank >= self.parents {
            return self.matched;
        }
        let x = rank as f64;
        let at = self
            .pieces
            .partition_point(|piece| piece.end < x)
            .min(self.pieces.len().saturating_sub(1));
        let Some(piece) = self.pieces.get(at) else { return 0 };
        let rows = piece.rows_to(x.clamp(piece.start, piece.end));
        ((rows + 0.5).floor().max(0.0) as u64).min(self.matched)
    }

    /// The rank, from 1, of the parent holding matched slot `slot`, slots laid out busiest rank first.
    pub(crate) fn rank_of(&self, slot: u64) -> u64 {
        if let Some(tabulated) = &self.tabulated {
            return tabulated.rank_of(slot);
        }
        let wanted = slot as f64 + 0.5;
        let at = self
            .pieces
            .partition_point(|piece| piece.after <= wanted)
            .min(self.pieces.len() - 1);
        let piece = &self.pieces[at];
        let guess = match piece.factor > 0.0 {
            true => piece.shape.reaching(piece.start, (wanted - piece.before) / piece.factor),
            false => piece.start,
        };
        let mut rank = (guess.ceil().max(1.0) as u64).min(self.parents);
        for _ in 0..4 {
            if rank > 1 && self.cumulative(rank - 1) > slot {
                rank -= 1;
            } else if self.cumulative(rank) <= slot {
                rank += 1;
            } else {
                return rank;
            }
        }
        let (mut low, mut high) = (1, self.parents);
        while low < high {
            let middle = low + (high - low) / 2;
            if self.cumulative(middle) > slot {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        low
    }

    /// The parents holding a child row, counted as the ranks before the first past which none does.
    pub(crate) fn held_parents(&self) -> u64 {
        let (mut low, mut high) = (0, self.parents);
        while low < high {
            let middle = low + (high - low) / 2;
            if self.cumulative(middle) >= self.matched {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        low
    }
}

/// One band between two pins: its ranks, the rows before it and in it, and its pieces.
struct Band {
    from: f64,
    to: f64,
    rows_before: f64,
    wanted: f64,
    pieces: std::ops::Range<usize>,
}

/// `pieces` with each of the first `free` bands, which lie above the top quantile and so hold no quantile, drawn as
/// one straight line holding its rows. Each runs down to where the band after it starts, so the counts fall
/// across the pin between them, and starts no higher than the band before it holds on average, falling short of
/// its end where that caps it. A band whose rows average below where the next starts stays flat at that average.
fn falling(pieces: Vec<Piece>, bands: &[Band], free: usize) -> Vec<Piece> {
    let mut lines: Vec<Option<Piece>> = vec![None; free];
    let mut below = bands
        .get(free)
        .and_then(|band| pieces.get(band.pieces.start))
        .map_or(0.0, |piece| piece.factor * piece.shape.value(piece.start, piece.start));
    for index in (0..free).rev() {
        let band = &bands[index];
        let width = band.to - band.from;
        if width <= 0.0 {
            continue;
        }
        let mean = band.wanted / width;
        let above = index
            .checked_sub(1)
            .map(|before| &bands[before])
            .filter(|before| before.to > before.from)
            .map_or(f64::INFINITY, |before| before.wanted / (before.to - before.from));
        let (high, low) = match mean < below {
            true => (mean, mean),
            false => {
                let high = (2.0 * mean - below).min(above);
                (high, 2.0 * mean - high)
            }
        };
        lines[index] = Some(Piece {
            start: band.from,
            end: band.to,
            shape: Shape::Linear {
                value: high,
                slope: (low - high) / width,
            },
            factor: 1.0,
            before: band.rows_before,
            after: band.rows_before + band.wanted,
        });
        below = high;
    }
    let mut drawn = Vec::with_capacity(pieces.len());
    for (index, band) in bands.iter().enumerate() {
        match lines.get(index).copied().flatten() {
            Some(line) => drawn.push(line),
            None => drawn.extend_from_slice(&pieces[band.pieces.clone()]),
        }
    }
    drawn
}

/// The unscaled curve over [0, `total`]: through the quantiles' `points`, rising by rank, with a power
/// law of `exponent` above the top one and the lowest segment's slope run on to zero below the last.
/// Without quantiles, the power law from rank 1, or every rank alike.
fn base_curve(points: &[(f64, f64)], exponent: Option<f64>, total: f64) -> Vec<(f64, f64, Shape)> {
    let flat = |value: f64| Shape::Linear { value, slope: 0.0 };
    let Some(&(top, top_value)) = points.first() else {
        return match exponent {
            Some(exponent) if total > 1.0 => vec![
                (0.0, 1.0, flat(1.0)),
                (
                    1.0,
                    total,
                    Shape::Power {
                        value: 1.0,
                        at: 1.0,
                        exponent,
                    },
                ),
            ],
            _ => vec![(0.0, total, flat(1.0))],
        };
    };
    let exponent = exponent.unwrap_or(0.0);
    let mut curve = Vec::new();
    if top > 0.0 {
        let one = top.min(1.0);
        curve.push((0.0, one, flat(top_value * libm::pow(one / top, -exponent))));
        if top > 1.0 {
            curve.push((
                1.0,
                top,
                Shape::Power {
                    value: top_value,
                    at: top,
                    exponent,
                },
            ));
        }
    }
    for pair in points.windows(2) {
        let ((start, high), (end, low)) = (pair[0], pair[1]);
        if end > start {
            curve.push((
                start,
                end,
                Shape::Bent {
                    origin: start,
                    span: end - start,
                    high,
                    low,
                    bend: 1.0,
                },
            ));
        }
    }
    let &(last, last_value) = points.last().expect("points holds the top one");
    let slope = match points {
        [.., (start, value), (end, next)] if end > start => (next - value) / (end - start),
        _ => 0.0,
    };
    if last < total {
        let zero = if slope < 0.0 { last - last_value / slope } else { f64::INFINITY };
        let stop = zero.min(total);
        if stop > last {
            curve.push((last, stop, Shape::Linear { value: last_value, slope }));
        }
        if stop < total {
            curve.push((stop, total, flat(0.0)));
        }
    }
    curve
}

/// The bend that gives the segments between quantiles in `band` the `wanted` rows, or the nearest
/// bend to it. A bend below 1 sags toward the lower quantile, as a long tail's counts do.
fn band_bend(band: &[(f64, f64, Shape)], wanted: f64) -> f64 {
    let area = |log_bend: f64| -> f64 {
        let bend = libm::exp(log_bend);
        band.iter().map(|&(low, high, shape)| shape.bent(bend).area(low, high)).sum()
    };
    let (mut low, mut high) = (-libm::log(MOST_BEND), libm::log(MOST_BEND));
    if area(low) >= wanted {
        return libm::exp(low);
    }
    if area(high) <= wanted {
        return libm::exp(high);
    }
    for _ in 0..FIT_STEPS {
        let middle = (low + high) / 2.0;
        if area(middle) < wanted {
            low = middle;
        } else {
            high = middle;
        }
    }
    libm::exp((low + high) / 2.0)
}

/// The power law's exponent above the top quantile that gives the ranks from `from` on the `wanted`
/// rows, so the quantiles below it hold as stated. Flat where even that holds too many.
fn fitted_exponent(points: &[(f64, f64)], total: f64, from: f64, wanted: f64) -> f64 {
    let area = |exponent: f64| -> f64 {
        base_curve(points, Some(exponent), total)
            .into_iter()
            .filter(|(_, end, _)| *end > from)
            .map(|(start, end, shape)| {
                let low = start.max(from);
                shape.from(start, low).area(low, end)
            })
            .sum()
    };
    if area(0.0) >= wanted {
        return 0.0;
    }
    let (mut low, mut high) = (0.0, STEEPEST);
    if area(high) <= wanted {
        return high;
    }
    for _ in 0..FIT_STEPS {
        let middle = (low + high) / 2.0;
        if area(middle) < wanted {
            low = middle;
        } else {
            high = middle;
        }
    }
    (low + high) / 2.0
}

/// The steepest power law above the top quantile whose rows at the last pin stay within that pin's
/// band's mean, so no rank past a pin holds more than the ranks it pins. None without a pin.
fn falling_from_pins(pins: &[(f64, f64)], (top, top_value): (f64, f64)) -> Option<f64> {
    let (rank, rows) = *pins.last()?;
    let (from, rows_before) = if pins.len() > 1 { pins[pins.len() - 2] } else { (0.0, 0.0) };
    let mean = (rows - rows_before) / (rank - from);
    if top_value <= 0.0 || mean <= top_value || rank >= top {
        return Some(0.0);
    }
    Some(libm::log(mean / top_value) / libm::log(top / rank))
}

/// The power law's exponent that carries the last pinned band's mean rows at its middle rank down to
/// the top quantile's value at its rank, where a pin falls above it. Zero where the band holds no more.
fn pinned_exponent(pins: &[(f64, f64)], (top, top_value): (f64, f64)) -> Option<f64> {
    let at = pins.iter().rposition(|&(rank, _)| rank < top)?;
    let (rank, rows) = pins[at];
    let (from, rows_before) = if at == 0 { (0.0, 0.0) } else { pins[at - 1] };
    let mean = (rows - rows_before) / (rank - from);
    let middle = (from + rank) / 2.0;
    if top_value <= 0.0 || mean <= top_value || middle >= top {
        return Some(0.0);
    }
    Some(libm::log(mean / top_value) / libm::log(top / middle.max(0.5)))
}
