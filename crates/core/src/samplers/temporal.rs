//! Date, timestamp and time-of-day columns. With no month, weekday or hour weights and no gaps, a
//! value is uniform over the extremes at the column's granularity. Otherwise a day is drawn from
//! the range, weighted by `by_month` and `by_dow` with `gap_rate`'s share of days left empty, then a
//! time within it weighted by `by_hour`.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::types::Date32Type;
use arrow_array::types::Date64Type;
use arrow_array::types::Time32MillisecondType;
use arrow_array::types::Time32SecondType;
use arrow_array::types::Time64MicrosecondType;
use arrow_array::types::Time64NanosecondType;
use arrow_array::types::TimestampMicrosecondType;
use arrow_array::types::TimestampMillisecondType;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::types::TimestampSecondType;
use arrow_buffer::NullBuffer;
use arrow_schema::DataType;
use arrow_schema::TimeUnit;
use chrono::DateTime;
use chrono::Datelike;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::NaiveTime;
use chrono::Timelike;
use rand::RngExt;
use rand_chacha::ChaCha8Rng;
use rand_distr::Distribution;
use rand_distr::weighted::WeightedAliasIndex;

use super::numeric::primitive;
use crate::profile::Granularity;
use crate::profile::TemporalStats;
use crate::rng::below;

/// The range a timestamp or date column with no extremes is drawn from.
const DEFAULT_RANGE: (&str, &str) = ("2025-01-01T00:00:00", "2025-12-31T23:59:59.999999");
const SECONDS_PER_DAY: i64 = 86_400;

/// The Arrow type a temporal column is stored as.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TemporalOutput {
    Timestamp(TimeUnit, Option<Arc<str>>),
    Date32,
    Date64,
    Time32(TimeUnit),
    Time64(TimeUnit),
}

fn ticks_per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

impl TemporalOutput {
    pub(crate) fn from_type(data_type: &DataType) -> Option<Self> {
        Some(match data_type {
            DataType::Timestamp(unit, tz) => Self::Timestamp(*unit, tz.clone()),
            DataType::Date32 => Self::Date32,
            DataType::Date64 => Self::Date64,
            DataType::Time32(unit) => Self::Time32(*unit),
            DataType::Time64(unit) => Self::Time64(*unit),
            _ => return None,
        })
    }

    pub(crate) fn data_type(&self) -> DataType {
        match self {
            Self::Timestamp(unit, tz) => DataType::Timestamp(*unit, tz.clone()),
            Self::Date32 => DataType::Date32,
            Self::Date64 => DataType::Date64,
            Self::Time32(unit) => DataType::Time32(*unit),
            Self::Time64(unit) => DataType::Time64(*unit),
        }
    }

    /// Ticks per second, or `None` for a date counted in days.
    fn ticks_per_second(&self) -> Option<i64> {
        match self {
            Self::Timestamp(unit, _) | Self::Time32(unit) | Self::Time64(unit) => Some(ticks_per_second(*unit)),
            Self::Date32 => None,
            Self::Date64 => Some(1_000),
        }
    }

    fn ticks_per_day(&self) -> i64 {
        self.ticks_per_second().map_or(1, |per_second| per_second * SECONDS_PER_DAY)
    }

    fn is_time_of_day(&self) -> bool {
        matches!(self, Self::Time32(_) | Self::Time64(_))
    }

    fn is_date(&self) -> bool {
        matches!(self, Self::Date32 | Self::Date64)
    }

    /// The column's value of `text`, or `None` when it does not parse.
    fn ticks(&self, text: &str) -> Option<i64> {
        if self.is_time_of_day() {
            let time = NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
                .or_else(|_| NaiveTime::parse_from_str(text, "%H:%M"))
                .ok()?;
            let nanos = i64::from(time.num_seconds_from_midnight()) * 1_000_000_000 + i64::from(time.nanosecond());
            let per_second = self.ticks_per_second()?;
            return Some(nanos / (1_000_000_000 / per_second));
        }
        let instant = parse_instant(text)?.and_utc();
        Some(match self.ticks_per_second() {
            None => instant.timestamp().div_euclid(SECONDS_PER_DAY),
            Some(1) => instant.timestamp(),
            Some(1_000) => instant.timestamp_millis(),
            Some(1_000_000) => instant.timestamp_micros(),
            Some(_) => instant.timestamp_nanos_opt()?,
        })
    }

    fn build(&self, rows: usize, nulls: Option<NullBuffer>, mut value: impl FnMut() -> i64) -> ArrayRef {
        match self {
            Self::Timestamp(unit, tz) => match unit {
                TimeUnit::Second => Arc::new(primitive::<TimestampSecondType>(rows, nulls, |_| value()).with_timezone_opt(tz.clone())),
                TimeUnit::Millisecond => {
                    Arc::new(primitive::<TimestampMillisecondType>(rows, nulls, |_| value()).with_timezone_opt(tz.clone()))
                }
                TimeUnit::Microsecond => {
                    Arc::new(primitive::<TimestampMicrosecondType>(rows, nulls, |_| value()).with_timezone_opt(tz.clone()))
                }
                TimeUnit::Nanosecond => {
                    Arc::new(primitive::<TimestampNanosecondType>(rows, nulls, |_| value()).with_timezone_opt(tz.clone()))
                }
            },
            Self::Date32 => Arc::new(primitive::<Date32Type>(rows, nulls, |_| value() as i32)),
            Self::Date64 => Arc::new(primitive::<Date64Type>(rows, nulls, |_| value())),
            Self::Time32(TimeUnit::Second) => Arc::new(primitive::<Time32SecondType>(rows, nulls, |_| value() as i32)),
            Self::Time32(_) => Arc::new(primitive::<Time32MillisecondType>(rows, nulls, |_| value() as i32)),
            Self::Time64(TimeUnit::Microsecond) => Arc::new(primitive::<Time64MicrosecondType>(rows, nulls, |_| value())),
            Self::Time64(_) => Arc::new(primitive::<Time64NanosecondType>(rows, nulls, |_| value())),
        }
    }
}

fn parse_instant(text: &str) -> Option<NaiveDateTime> {
    if let Ok(instant) = DateTime::parse_from_rfc3339(text) {
        return Some(instant.naive_utc());
    }
    ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"]
        .iter()
        .find_map(|format| NaiveDateTime::parse_from_str(text, format).ok())
        .or_else(|| NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()?.and_hms_opt(0, 0, 0))
}

#[derive(Debug, Clone)]
enum TemporalDraw {
    /// `low + k · granule` for `k` uniform in `0..count`.
    Uniform {
        low: i64,
        granule: i64,
        count: u64,
    },
    Weighted(Box<WeightedDays>),
}

#[derive(Debug, Clone)]
struct WeightedDays {
    day_starts: Vec<i64>,
    days: WeightedAliasIndex<f64>,
    hours: Option<WeightedAliasIndex<f64>>,
    whole_days: bool,
    granule: i64,
    granules_per_day: u64,
    granules_per_hour: u32,
    ticks_per_hour: i64,
    low: i64,
    high: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct TemporalSampler {
    output: TemporalOutput,
    draw: TemporalDraw,
}

impl TemporalSampler {
    /// `gap_days` shuffles the range's days when `gap_rate` leaves some empty.
    pub(crate) fn new(
        stats: Option<&TemporalStats>,
        output: TemporalOutput,
        gap_days: impl FnOnce() -> ChaCha8Rng,
    ) -> Result<Self, String> {
        let extremes = stats.and_then(|s| s.extremes.as_ref());
        let ticks_per_day = output.ticks_per_day();
        let parse = |text: Option<&String>, default: &str| match text {
            Some(text) => output
                .ticks(text)
                .ok_or_else(|| format!("extreme {text:?} is not a {output:?} value")),
            None => output.ticks(default).ok_or_else(|| format!("{output:?} has no default range")),
        };
        let (default_low, default_high) = if output.is_time_of_day() {
            ("00:00:00", "23:59:59.999999999")
        } else {
            DEFAULT_RANGE
        };
        let low = parse(extremes.and_then(|e| e.low.as_ref()), default_low)?;
        let high = parse(extremes.and_then(|e| e.high.as_ref()), default_high)?.max(low);
        let granule = granule(extremes.and_then(|e| e.granularity), &output);
        let whole_days = output.is_date() || granule >= ticks_per_day;
        let by_month = stats.and_then(|s| s.by_month.as_ref());
        let by_dow = stats.and_then(|s| s.by_dow.as_ref());
        let by_hour = stats.and_then(|s| s.by_hour.as_ref()).filter(|_| !whole_days);
        let gap_rate = stats.and_then(|s| s.gap_rate).map_or(0.0, |r| r.0);
        if by_month.is_none() && by_dow.is_none() && by_hour.is_none() && gap_rate == 0.0 {
            let granule = if whole_days { ticks_per_day } else { granule };
            let count = ((high - low) / granule) as u64 + 1;
            return Ok(Self {
                output,
                draw: TemporalDraw::Uniform { low, granule, count },
            });
        }
        let (first_day, last_day) = if output.is_time_of_day() {
            (0, 0)
        } else {
            (low.div_euclid(ticks_per_day), high.div_euclid(ticks_per_day))
        };
        let calendar: Vec<(usize, usize)> = (first_day..=last_day)
            .map(|day| {
                let date = DateTime::from_timestamp(day * SECONDS_PER_DAY, 0).map(|d| d.date_naive());
                date.map_or((0, 0), |d| (d.month0() as usize, d.weekday().num_days_from_monday() as usize))
            })
            .collect();
        let share = |weights: Option<&Vec<crate::profile::Rate>>, index: usize, slots: &[usize]| match weights {
            Some(weights) if slots[index] > 0 => weights.get(index).map_or(0.0, |r| r.0) / slots[index] as f64,
            _ => 1.0,
        };
        let mut months = [0usize; 12];
        let mut weekdays = [0usize; 7];
        for &(month, weekday) in &calendar {
            months[month] += 1;
            weekdays[weekday] += 1;
        }
        let mut weights: Vec<f64> = calendar
            .iter()
            .map(|&(month, weekday)| share(by_month, month, &months) * share(by_dow, weekday, &weekdays))
            .collect();
        let gaps = ((gap_rate * weights.len() as f64).round() as usize).min(weights.len().saturating_sub(1));
        if gaps > 0 {
            let mut rng = gap_days();
            let mut order: Vec<usize> = (0..weights.len()).collect();
            for at in 0..gaps {
                let pick = at + rng.random_range(0..order.len() - at);
                order.swap(at, pick);
                weights[order[at]] = 0.0;
            }
        }
        let days = WeightedAliasIndex::new(weights).map_err(|error| format!("day weights are unusable: {error}"))?;
        let hours = match by_hour {
            Some(by_hour) => Some(
                WeightedAliasIndex::new(by_hour.iter().map(|r| r.0).collect()).map_err(|error| format!("by_hour is unusable: {error}"))?,
            ),
            None => None,
        };
        let ticks_per_hour = ticks_per_day / 24;
        Ok(Self {
            output,
            draw: TemporalDraw::Weighted(Box::new(WeightedDays {
                day_starts: (first_day..=last_day).map(|day| day * ticks_per_day).collect(),
                days,
                hours,
                whole_days,
                granule,
                granules_per_day: (ticks_per_day / granule).max(1) as u64,
                granules_per_hour: (ticks_per_hour / granule).max(1) as u32,
                ticks_per_hour,
                low,
                high,
            })),
        })
    }

    pub(crate) fn data_type(&self) -> DataType {
        self.output.data_type()
    }

    pub(crate) fn generate(&self, rng: &mut ChaCha8Rng, rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
        match &self.draw {
            &TemporalDraw::Uniform { low, granule, count } => {
                self.output.build(rows, nulls, || low + rng.random_range(0..count) as i64 * granule)
            }
            TemporalDraw::Weighted(days) => self.output.build(rows, nulls, || {
                let start = days.day_starts[days.days.sample(rng)];
                let offset = if days.whole_days {
                    0
                } else if let Some(hours) = &days.hours {
                    hours.sample(rng) as i64 * days.ticks_per_hour + i64::from(below(rng, days.granules_per_hour)) * days.granule
                } else {
                    rng.random_range(0..days.granules_per_day) as i64 * days.granule
                };
                (start + offset).clamp(days.low, days.high)
            }),
        }
    }
}

/// The ticks between two neighboring values at `granularity`, at least one.
fn granule(granularity: Option<Granularity>, output: &TemporalOutput) -> i64 {
    let Some(per_second) = output.ticks_per_second() else {
        return 1;
    };
    let ticks = match granularity {
        Some(Granularity::Year | Granularity::Month | Granularity::Day) => per_second * SECONDS_PER_DAY,
        Some(Granularity::Hour) => per_second * 3_600,
        Some(Granularity::Minute) => per_second * 60,
        Some(Granularity::Second) => per_second,
        Some(Granularity::Millisecond) => per_second / 1_000,
        Some(Granularity::Microsecond) => per_second / 1_000_000,
        Some(Granularity::Nanosecond) | None => 1,
    };
    ticks.max(1)
}
