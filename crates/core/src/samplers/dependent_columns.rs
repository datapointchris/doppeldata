//! Columns drawn from each other, as a profile's dataset section and `derived` fields state it.
//!
//! Numeric columns a profile correlates by Spearman's coefficient draw their levels together from a
//! Gaussian copula, one group per connected set of pairs. A categorical child draws from the row of
//! its conditional table its parent's value names, and a numeric child through the quantiles stated
//! for its parent's value. A column's zero and null rates can follow a parent too. A derived column
//! is computed from its bases, and a dependency's table looks its right-hand side up. Every column is
//! drawn after the ones it depends on, in one topological pass per block.

mod copula;
mod formula;
mod parent_keys;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::BooleanArray;
use arrow_array::PrimitiveArray;
use arrow_array::cast::AsArray;
use arrow_array::types::Date32Type;
use arrow_array::types::Date64Type;
use arrow_array::types::Float64Type;
use arrow_array::types::Int64Type;
use arrow_array::types::TimestampMicrosecondType;
use arrow_array::types::TimestampMillisecondType;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::types::TimestampSecondType;
use arrow_buffer::BooleanBuffer;
use arrow_buffer::NullBuffer;
use arrow_buffer::ScalarBuffer;
use arrow_schema::DataType;
use arrow_schema::TimeUnit;
use rand_distr::Distribution;
use rand_distr::weighted::WeightedAliasIndex;

use self::copula::CopulaGroup;
use self::copula::CurveScores;
use self::copula::LatentGrid;
use self::copula::PairSeries;
use self::copula::RankScores;
use self::copula::TIE_SHARE;
use self::copula::calibrated;
use self::formula::Formula;
use self::parent_keys::NULL;
use self::parent_keys::OTHER;
use self::parent_keys::ParentKeys;
use super::BlockPosition;
use super::ColumnPlan;
use super::PlanContext;
use super::StreamKeys;
use super::ValueSampler;
use super::inverse_cdf::InverseCdf;
use super::numeric::NumericOutput;
use super::numeric::Rounding;
use super::numeric::bounded_cdf;
use super::numeric::quantile_knots;
use super::ranked_marginal::LevelWarp;
use super::ranked_marginal::RankedMarginal;
use super::ranked_marginal::with_zero_share;
use super::temporal::TemporalOutput;
use super::text::StringOutput;
use crate::plan::ColumnFault;
use crate::plan::PlanError;
use crate::profile::Column;
use crate::profile::Conditional;
use crate::profile::CorrelationMethod;
use crate::profile::Derived;
use crate::profile::Granularity;
use crate::profile::Quantiles;
use crate::profile::Rate;
use crate::profile::TableProfile;
use crate::related_tables::GivenColumn;
use crate::rng::Bernoulli;
use crate::rng::rng_for;
use crate::rng::unit;

const NANOS_PER_MICRO: i128 = 1_000;
const NANOS_PER_DAY: i128 = 86_400_000_000_000;

/// How every top-level column of a table is drawn given the others, and the order that draws each after
/// what it depends on.
#[derive(Debug, Clone, Default)]
pub(crate) struct DependentColumns {
    order: Vec<usize>,
    groups: Vec<CopulaGroup>,
    draws: Vec<ColumnDraw>,
    /// The JSON pointer of each dependence entry the plan does not draw, in the order it was set aside.
    set_aside: Vec<String>,
}

/// One block's columns as drawn, each beside the rows its null mask drew, which its text cells follow.
pub(crate) struct DrawnColumns {
    pub(crate) arrays: Vec<ArrayRef>,
    pub(crate) nulls: Vec<Option<BooleanBuffer>>,
}

#[derive(Debug, Clone, Default)]
struct ColumnDraw {
    values: Option<ValueDraw>,
    nulls: Option<ParentTrials>,
    /// Each profile entry the draw reads, by its kind and pointer.
    sources: Vec<(&'static str, String)>,
}

#[derive(Debug, Clone)]
enum ValueDraw {
    Ranked(Box<RankedDraw>),
    Table(Box<TableDraw>),
    Lookup(Box<LookupDraw>),
    Timedelta(Box<TimedeltaDraw>),
    Formula(Box<FormulaDraw>),
}

/// A numeric column drawn at one level per row through its own rising curve, the level warped first where its
/// parent's value names a warp. The level comes from its copula group, or from its own values stream.
#[derive(Debug, Clone)]
struct RankedDraw {
    copula: Option<(usize, usize)>,
    own: RankedMarginal,
    curves: Option<ParentCurves>,
    zeros: Option<ParentShares>,
    output: NumericOutput,
}

/// The warp of each listed parent value's quantiles onto the column's own curve, and of every other value's.
#[derive(Debug, Clone)]
struct ParentCurves {
    parent: usize,
    keys: ParentKeys,
    curves: Vec<LevelWarp>,
    other: Option<LevelWarp>,
}

impl ParentCurves {
    fn warp(&self, place: u32) -> Option<&LevelWarp> {
        match place {
            NULL => None,
            OTHER => self.other.as_ref(),
            listed => Some(&self.curves[listed as usize]),
        }
    }
}

/// A share for each listed value of a parent, and one for every other value, where stated.
#[derive(Debug, Clone)]
struct ParentShares {
    parent: usize,
    keys: ParentKeys,
    shares: Vec<f64>,
    other: Option<f64>,
}

impl ParentShares {
    fn share(&self, place: u32) -> Option<f64> {
        match place {
            NULL => None,
            OTHER => self.other,
            listed => Some(self.shares[listed as usize]),
        }
    }
}

/// A Bernoulli trial for each listed value of a parent, and one for every other value, where stated.
#[derive(Debug, Clone)]
struct ParentTrials {
    parent: usize,
    keys: ParentKeys,
    trials: Vec<Bernoulli>,
    other: Option<Bernoulli>,
}

impl ParentTrials {
    fn mask(&self, own: Bernoulli, at: &BlockPosition, keys: &StreamKeys, rows: usize, arrays: &[Option<ArrayRef>]) -> BooleanBuffer {
        let places = self.keys.places(drawn(arrays, self.parent));
        let mut rng = rng_for(at.seed, keys.nulls, at.block);
        BooleanBuffer::collect_bool(rows, |row| {
            let trial = match places[row] {
                NULL => None,
                OTHER => self.other,
                listed => Some(self.trials[listed as usize]),
            };
            trial.unwrap_or(own).draw(&mut rng)
        })
    }
}

/// The values a child takes from a table, stored as its column stores them.
#[derive(Debug, Clone)]
enum ChildValues {
    Text(StringOutput, Vec<String>),
    Numeric(NumericOutput, Vec<f64>),
    Boolean(Vec<bool>),
}

#[derive(Debug, Clone)]
struct TableRow {
    choice: WeightedAliasIndex<f64>,
    /// Each slot's place among the child's values, and `None` for the share of the row's rest the column's own draw
    /// fills.
    values: Vec<Option<u32>>,
}

#[derive(Debug, Clone)]
struct TableDraw {
    parent: usize,
    keys: ParentKeys,
    rows: Vec<TableRow>,
    other: Option<TableRow>,
    values: ChildValues,
}

/// A right-hand column looked up by its left-hand value: the value at the place `keys` gives it.
#[derive(Debug, Clone)]
struct LookupDraw {
    lhs: usize,
    keys: ParentKeys,
    values: ChildValues,
    violation: Option<Bernoulli>,
}

#[derive(Debug, Clone)]
struct TimedeltaDraw {
    base: usize,
    /// The lag in microseconds, at a level per row.
    lag: InverseCdf,
    /// The microseconds between neighboring values of the column, at its unit or its granularity where coarser, which
    /// each lag is rounded to.
    step: f64,
    output: TemporalOutput,
}

#[derive(Debug, Clone)]
struct FormulaDraw {
    formula: Formula,
    /// The column each of the formula's names reads.
    refs: Vec<usize>,
    rounding: Rounding,
    output: NumericOutput,
    violation: Option<Bernoulli>,
}

fn drawn(arrays: &[Option<ArrayRef>], index: usize) -> &ArrayRef {
    arrays[index].as_ref().expect("a column is drawn after every column it depends on")
}

/// The dependence entries a plan does not draw, each by its JSON pointer, and what each column's plan says of those
/// set aside for it.
#[derive(Default)]
struct SetAside {
    pointers: Vec<String>,
    notes: Vec<(usize, String)>,
}

impl SetAside {
    fn entry(&mut self, pointer: String, child: usize, what: String, why: String) {
        self.pointers.push(pointer);
        self.notes.push((child, format!("its {what} set aside, since {why}")));
    }
}

fn refuse(field: String, message: String) -> PlanError {
    PlanError::Column {
        code: ColumnFault::InvalidDependence,
        field,
        message,
    }
}

/// What a dependence between two columns is planned against: each column by name, and the edges from each
/// column to the ones drawn from it.
struct Planner<'a> {
    profile: &'a [Column],
    columns: &'a [ColumnPlan],
    names: HashMap<&'a str, usize>,
    children: Vec<BTreeSet<usize>>,
}

impl<'a> Planner<'a> {
    fn column(&self, name: &str, field: &str) -> Result<usize, PlanError> {
        self.names.get(name).copied().ok_or_else(|| PlanError::Column {
            code: ColumnFault::UnknownColumn,
            field: field.to_string(),
            message: format!("the table holds no column {name:?}"),
        })
    }

    /// Whether `later` is drawn after `column`, through the edges planned so far, or is it.
    fn draws_from(&self, later: usize, column: usize) -> bool {
        let mut reached = vec![false; self.columns.len()];
        let mut stack = vec![column];
        while let Some(at) = stack.pop() {
            if at == later {
                return true;
            }
            if !std::mem::replace(&mut reached[at], true) {
                stack.extend(self.children[at].iter().copied());
            }
        }
        false
    }

    /// Draws `child` after `parent`, refusing an edge that would make a column depend on itself.
    fn depend(&mut self, parent: usize, child: usize, field: &str) -> Result<(), PlanError> {
        if self.draws_from(parent, child) {
            let (parent, child) = (&self.profile[parent].name, &self.profile[child].name);
            return Err(refuse(
                field.to_string(),
                format!("{child:?} would be drawn from {parent:?}, which is drawn from it"),
            ));
        }
        self.children[parent].insert(child);
        Ok(())
    }

    /// The keys of `values` as `parent` holds them, refusing a parent that lists no values as a category.
    fn keys(&self, values: impl IntoIterator<Item = &'a String>) -> ParentKeys {
        ParentKeys::new(values)
    }

    fn child_values(&self, child: usize, values: &[String], field: &str) -> Result<ChildValues, PlanError> {
        let name = &self.profile[child].name;
        let parsed = |kind: &str| {
            refuse(
                field.to_string(),
                format!("{name:?} is {kind}, and holds no value {values:?} names"),
            )
        };
        Ok(match &self.columns[child].values {
            ValueSampler::Categorical(sampler) => ChildValues::Text(sampler.output, values.to_vec()),
            ValueSampler::Text(sampler) => ChildValues::Text(sampler.output, values.to_vec()),
            ValueSampler::Numeric(sampler) => ChildValues::Numeric(
                sampler.output,
                values
                    .iter()
                    .map(|value| value.parse().map_err(|_| parsed("numeric")))
                    .collect::<Result<_, _>>()?,
            ),
            ValueSampler::Sequence(sampler) => ChildValues::Numeric(
                sampler.output,
                values
                    .iter()
                    .map(|value| value.parse().map_err(|_| parsed("numeric")))
                    .collect::<Result<_, _>>()?,
            ),
            ValueSampler::Boolean(_) => ChildValues::Boolean(
                values
                    .iter()
                    .map(|value| match value.as_str() {
                        "true" => Ok(true),
                        "false" => Ok(false),
                        _ => Err(parsed("boolean")),
                    })
                    .collect::<Result<_, _>>()?,
            ),
            _ => {
                return Err(refuse(
                    field.to_string(),
                    format!("{name:?} is a list, struct or temporal column, which no table draws"),
                ));
            }
        })
    }
}

/// The shortfall from 1 a table row's shares may hold and still draw no rest, so a sum off in its last bits draws none.
const SHARE_SLACK: f64 = 1e-9;

/// The share of a value's own a table's rows may leave unstated and still spread none of their rest to it. Below it
/// the gap is the rounding of the rates it is taken from, and drawing it would scatter a few rows of the value into
/// rows that hold none.
const UNSTATED_FLOOR: f64 = 0.01;

/// Each value a column lists, keyed as a table keys it, with its share of the column's values, and the share of
/// values it lists none of.
fn listed_shares(column: &Column) -> Option<(Vec<(String, f64)>, f64)> {
    let other = |rate: Option<Rate>| rate.map_or(0.0, |rate| rate.0);
    if let Some(string) = &column.string
        && let Some(top) = &string.top_values
    {
        return Some((
            top.iter().map(|listed| (listed.value.clone(), listed.fraction.0)).collect(),
            other(string.other_fraction),
        ));
    }
    if let Some(numeric) = &column.numeric
        && let Some(top) = &numeric.top_values
    {
        let listed = top.iter().map(|listed| (format!("{}", listed.value), listed.fraction.0)).collect();
        return Some((listed, other(numeric.other_fraction)));
    }
    let yes = column.boolean.as_ref()?.true_fraction?.0;
    Some((vec![("true".to_string(), yes), ("false".to_string(), 1.0 - yes)], 0.0))
}

/// Where the share a conditional table's row leaves short of 1 is drawn. Each value the child lists takes the share
/// of it the table's rows leave unstated, each row weighted by its given value's share, which is one step of
/// iterative proportional fitting toward the child's own shares. The share of values the child lists none of takes
/// its own draw.
struct RestShares {
    unstated: Vec<(String, f64)>,
    unlisted: f64,
}

impl RestShares {
    /// The rest of `conditional`'s rows, or `None` where its given or child column lists no values, which leaves each
    /// row's rest to the child's own draw.
    fn of(conditional: &Conditional, given: &Column, child: &Column) -> Option<Self> {
        let (child_listed, unlisted) = listed_shares(child)?;
        let (given_listed, given_other) = listed_shares(given)?;
        let share_of = |key: &str| given_listed.iter().find(|(value, _)| value == key).map_or(0.0, |(_, rate)| *rate);
        let other_share = given_other
            + given_listed
                .iter()
                .filter(|(value, _)| !conditional.fractions.contains_key(value))
                .map(|(_, rate)| rate)
                .sum::<f64>();
        let rows = conditional.fractions.iter().map(|(given, cells)| (share_of(given), cells));
        let mut stated: HashMap<&str, f64> = HashMap::new();
        for (weight, cells) in rows.chain(conditional.other_fractions.iter().map(|cells| (other_share, cells))) {
            for (value, rate) in cells {
                *stated.entry(value.as_str()).or_default() += weight * rate.0;
            }
        }
        let unstated = child_listed
            .into_iter()
            .filter_map(|(value, own)| {
                let left = own - stated.get(value.as_str()).copied().unwrap_or(0.0);
                (left > UNSTATED_FLOOR * own).then_some((value, left))
            })
            .collect();
        Some(Self { unstated, unlisted })
    }
}

/// A row of a conditional table. The share its cells leave short of 1 is the rows whose value the table does not
/// state, spread as `rest` says, or drawn as the column's own value where it says nothing.
fn table_row(
    cells: &BTreeMap<String, Rate>,
    rest: Option<&RestShares>,
    values: &mut Vec<String>,
    field: &str,
) -> Result<TableRow, PlanError> {
    let mut place_of = |value: &String| {
        let place = values.iter().position(|known| known == value).unwrap_or_else(|| {
            values.push(value.clone());
            values.len() - 1
        });
        Some(place as u32)
    };
    let mut places: Vec<Option<u32>> = cells.keys().map(&mut place_of).collect();
    let mut shares: Vec<f64> = cells.values().map(|rate| rate.0).collect();
    let left = 1.0 - shares.iter().sum::<f64>();
    if left > SHARE_SLACK {
        let spread: Vec<&(String, f64)> = rest
            .into_iter()
            .flat_map(|rest| &rest.unstated)
            .filter(|(value, share)| *share > 0.0 && !cells.contains_key(value))
            .collect();
        let unlisted = rest.map_or(0.0, |rest| rest.unlisted);
        let total = spread.iter().map(|(_, share)| share).sum::<f64>() + unlisted;
        if spread.is_empty() {
            shares.push(left);
            places.push(None);
        } else {
            for (value, share) in spread {
                shares.push(left * share / total);
                places.push(place_of(value));
            }
            if unlisted > 0.0 {
                shares.push(left * unlisted / total);
                places.push(None);
            }
        }
    }
    let choice =
        WeightedAliasIndex::new(shares).map_err(|error| refuse(field.to_string(), format!("the row's shares are unusable: {error}")))?;
    Ok(TableRow { choice, values: places })
}

/// The microseconds a value of `output` at `granularity` steps by, or `None` for a time of day, which no lag offsets.
fn temporal_step(output: &TemporalOutput, granularity: Option<Granularity>) -> Option<f64> {
    let unit = match output {
        TemporalOutput::Date32 => 86_400_000_000.0,
        TemporalOutput::Date64 | TemporalOutput::Timestamp(TimeUnit::Millisecond, _) => 1_000.0,
        TemporalOutput::Timestamp(TimeUnit::Second, _) => 1_000_000.0,
        TemporalOutput::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _) => 1.0,
        TemporalOutput::Time32(_) | TemporalOutput::Time64(_) => return None,
    };
    let granule = match granularity {
        Some(Granularity::Year | Granularity::Month | Granularity::Day) => 86_400_000_000.0,
        Some(Granularity::Hour) => 3_600_000_000.0,
        Some(Granularity::Minute) => 60_000_000.0,
        Some(Granularity::Second) => 1_000_000.0,
        Some(Granularity::Millisecond) => 1_000.0,
        Some(Granularity::Microsecond | Granularity::Nanosecond) | None => 1.0,
    };
    Some(f64::max(unit, granule))
}

impl DependentColumns {
    /// How `columns`, the plans of `table`'s columns in order, are drawn given each other. It refuses a dependence
    /// naming a column the table lacks, a column of a kind the dependence cannot apply to, an expression that does
    /// not parse, and a cycle. Each column's plan description gains what it is drawn from. Each entry drawing a
    /// column `fixed` holds is set aside, for the reason it holds.
    pub(crate) fn plan(
        context: &PlanContext,
        table: &TableProfile,
        columns: &mut [ColumnPlan],
        fixed: &BTreeMap<usize, String>,
    ) -> Result<Self, PlanError> {
        let profile = table.columns.as_deref().unwrap_or_default();
        let dataset = table.dataset.as_ref();
        let mut draws = vec![ColumnDraw::default(); columns.len()];
        let mut described: Vec<Option<String>> = vec![None; columns.len()];
        let mut notes: Vec<Vec<String>> = vec![Vec::new(); columns.len()];
        let mut set_aside = SetAside::default();
        // What draws each column's values, once an entry does, as an entry set aside for it names it.
        let mut drawn_by: Vec<Option<String>> = vec![None; columns.len()];
        let mut planner = Planner {
            profile,
            columns,
            names: profile
                .iter()
                .enumerate()
                .map(|(index, column)| (column.name.as_str(), index))
                .collect(),
            children: vec![BTreeSet::new(); profile.len()],
        };
        let already = |drawn_by: &[Option<String>], child: usize| {
            fixed
                .get(&child)
                .cloned()
                .or_else(|| drawn_by[child].as_ref().map(|drawn| format!("{drawn} draws it")))
        };
        for (index, column) in profile.iter().enumerate() {
            let Some(derived) = &column.derived else { continue };
            let field = format!("/columns/{index}/derived");
            if let Some(why) = fixed.get(&index) {
                set_aside.entry(field, index, "derived values".into(), why.clone());
                continue;
            }
            let (draw, note) = match derived {
                Derived::Timedelta(timedelta) => {
                    let base = planner.column(&timedelta.base, &format!("{field}/base"))?;
                    let output = match &planner.columns[index].values {
                        ValueSampler::Temporal(sampler) => sampler.output.clone(),
                        _ => {
                            return Err(refuse(
                                field,
                                format!("{:?} is not a date or timestamp, which a lag offsets", column.name),
                            ));
                        }
                    };
                    let granularity = column
                        .temporal
                        .as_ref()
                        .and_then(|t| t.extremes.as_ref())
                        .and_then(|e| e.granularity);
                    let step = temporal_step(&output, granularity)
                        .ok_or_else(|| refuse(field.clone(), format!("{:?} is a time of day, which no lag offsets", column.name)))?;
                    if !matches!(planner.columns[base].values, ValueSampler::Temporal(_)) {
                        return Err(refuse(
                            format!("{field}/base"),
                            format!("{:?} is not a date or timestamp, which a lag is added to", timedelta.base),
                        ));
                    }
                    planner.depend(base, index, &format!("{field}/base"))?;
                    let micros = |duration: &crate::profile::SignedDuration| duration.microseconds as f64;
                    let quantiles =
                        timedelta.lag.quantiles.as_ref().map(|quantiles| {
                            Quantiles(quantiles.iter().map(|(name, duration)| (name.clone(), micros(duration))).collect())
                        });
                    let low = timedelta.lag.min.as_ref().map(micros);
                    let high = timedelta.lag.max.as_ref().map(micros);
                    let lag = bounded_cdf(quantiles.as_ref(), low, high, (low.unwrap_or(0.0), high.or(low).unwrap_or(0.0))).map_err(
                        |message| PlanError::Column {
                            code: ColumnFault::InvalidQuantiles,
                            field: format!("{field}/lag"),
                            message,
                        },
                    )?;
                    let count = quantiles.as_ref().map_or(0, |q| q.0.len());
                    let draw = TimedeltaDraw { base, lag, step, output };
                    drawn_by[index] = Some(format!("its lag from {}", timedelta.base));
                    (
                        ValueDraw::Timedelta(Box::new(draw)),
                        format!("{} plus a lag at {count} quantiles", timedelta.base),
                    )
                }
                Derived::Expression(expression) => {
                    let field = format!("{field}/expr");
                    let (formula, names) = Formula::parse(&expression.expr).map_err(|message| refuse(field.clone(), message))?;
                    let output = match &planner.columns[index].values {
                        ValueSampler::Numeric(sampler) => sampler.output,
                        _ => {
                            return Err(refuse(
                                field,
                                format!("{:?} is not a numeric column, which an expression computes", column.name),
                            ));
                        }
                    };
                    let mut refs = Vec::with_capacity(names.len());
                    for name in &names {
                        let reference = planner.column(name, &field)?;
                        if !matches!(
                            planner.columns[reference].values,
                            ValueSampler::Numeric(_) | ValueSampler::Sequence(_)
                        ) {
                            return Err(refuse(
                                field,
                                format!("{name:?} is not a numeric column, which an expression reads"),
                            ));
                        }
                        planner.depend(reference, index, &field)?;
                        refs.push(reference);
                    }
                    let rounding = match expression.round {
                        Some(places) => Rounding::Decimals(super::numeric::power_of_ten(places)),
                        None => Rounding::for_column(column.numeric.as_ref(), &output),
                    };
                    let violation = expression
                        .violation_fraction
                        .map(|rate| Bernoulli::new(rate.0))
                        .filter(|trial| !trial.never());
                    let draw = FormulaDraw {
                        formula,
                        refs,
                        rounding,
                        output,
                        violation,
                    };
                    drawn_by[index] = Some(format!("the expression {}", expression.expr));
                    (ValueDraw::Formula(Box::new(draw)), format!("the expression {}", expression.expr))
                }
            };
            draws[index].values = Some(draw);
            draws[index].sources.push(("derived", format!("/columns/{index}/derived")));
            described[index] = Some(note);
        }
        let looped = |parent: usize| format!("{} is drawn from it", profile[parent].name);
        for (at, dependency) in dataset.and_then(|d| d.dependencies.as_ref()).into_iter().flatten().enumerate() {
            let Some(table) = &dependency.table else { continue };
            let field = format!("/dataset/dependencies/{at}");
            let rhs = planner.column(&dependency.rhs, &format!("{field}/rhs"))?;
            let what = format!("lookup from {}", dependency.lhs.join(", "));
            let [lhs] = dependency.lhs.as_slice() else {
                set_aside.entry(format!("{field}/table"), rhs, what, "a lookup reads one column".into());
                continue;
            };
            let lhs_index = planner.column(lhs, &format!("{field}/lhs/0"))?;
            if let Some(why) = already(&drawn_by, rhs) {
                set_aside.entry(format!("{field}/table"), rhs, what, why);
                continue;
            }
            if planner.draws_from(lhs_index, rhs) {
                set_aside.entry(format!("{field}/table"), rhs, what, looped(lhs_index));
                continue;
            }
            let values: Vec<String> = table.values().cloned().collect();
            let child = planner.child_values(rhs, &values, &format!("{field}/table"))?;
            planner.depend(lhs_index, rhs, &format!("{field}/lhs/0"))?;
            let violation = dependency
                .violation_fraction
                .map(|rate| Bernoulli::new(rate.0))
                .filter(|trial| !trial.never());
            draws[rhs].values = Some(ValueDraw::Lookup(Box::new(LookupDraw {
                lhs: lhs_index,
                keys: planner.keys(table.keys()),
                values: child,
                violation,
            })));
            draws[rhs].sources.push(("lookup", field));
            drawn_by[rhs] = Some(format!("its lookup from {lhs}"));
            described[rhs] = Some(format!("looked up from {lhs}, else {}", planner.columns[rhs].sampler));
        }
        for (at, conditional) in dataset.and_then(|d| d.conditionals.as_ref()).into_iter().flatten().enumerate() {
            let field = format!("/dataset/conditionals/{at}");
            let child = planner.column(&conditional.column, &format!("{field}/column"))?;
            let parent = planner.column(&conditional.given, &format!("{field}/given"))?;
            let what = format!("conditional table given {}", conditional.given);
            if let Some(why) = already(&drawn_by, child) {
                set_aside.entry(field, child, what, why);
                continue;
            }
            if planner.draws_from(parent, child) {
                set_aside.entry(field, child, what, looped(parent));
                continue;
            }
            let rest = RestShares::of(conditional, &planner.profile[parent], &planner.profile[child]);
            let mut values = Vec::new();
            let mut rows = Vec::with_capacity(conditional.fractions.len());
            for (given, cells) in &conditional.fractions {
                rows.push(table_row(
                    cells,
                    rest.as_ref(),
                    &mut values,
                    &format!("{field}/table/{}", crate::perturbations::pointer_token(given)),
                )?);
            }
            let other = conditional
                .other_fractions
                .as_ref()
                .map(|cells| table_row(cells, rest.as_ref(), &mut values, &format!("{field}/other")))
                .transpose()?;
            let child_values = planner.child_values(child, &values, &format!("{field}/table"))?;
            planner.depend(parent, child, &format!("{field}/given"))?;
            draws[child].values = Some(ValueDraw::Table(Box::new(TableDraw {
                parent,
                keys: planner.keys(conditional.fractions.keys()),
                rows,
                other,
                values: child_values,
            })));
            draws[child].sources.push(("conditional_table", field));
            let own = &planner.columns[child].sampler;
            described[child] = Some(format!("its conditional table given {}, else {own}", conditional.given));
            drawn_by[child] = Some(format!("its {what}"));
        }
        let ranked = |draws: &[ColumnDraw], index: usize| {
            draws[index].values.is_none()
                && !fixed.contains_key(&index)
                && matches!(planner.columns[index].values, ValueSampler::Numeric(_))
        };
        let correlations = dataset.and_then(|d| d.correlations.as_deref()).unwrap_or_default();
        for (at, correlation) in correlations.iter().enumerate() {
            let (Some(&a), Some(&b)) = (planner.names.get(correlation.a.as_str()), planner.names.get(correlation.b.as_str())) else {
                continue;
            };
            let Some((held, other, why)) = [(a, b), (b, a)]
                .into_iter()
                .find_map(|(held, other)| fixed.get(&held).map(|why| (held, other, why)))
            else {
                continue;
            };
            let what = format!("correlation with {}", profile[other].name);
            set_aside.entry(format!("/dataset/correlations/{at}"), held, what, why.clone());
        }
        let spearman = spearman_pairs(table, profile, |index| ranked(&draws, index));
        let component = copula_components(&spearman, profile.len());
        // A copula level is drawn given every other member's, so a member's levels are not uniform among the rows
        // holding one value of another member, and no warp of them holds a quantile or a zero rate given that value.
        let same_copula = |a: usize, b: usize| component[a].is_some() && component[a] == component[b];
        let mut curves: Vec<Option<ParentCurves>> = vec![None; profile.len()];
        // The pointer of each entry warping a column's level, which is set aside with the warp.
        let mut warp_entries: Vec<Vec<(&'static str, String)>> = vec![Vec::new(); profile.len()];
        // Why a warp of `child`'s level given `parent` cannot be drawn, where it cannot, `held` naming the parent of
        // the warp of that kind the child already takes.
        let unwarped = |planner: &Planner, draws: &[ColumnDraw], child: usize, parent: usize, held: Option<(&str, usize)>| {
            if let Some(why) = already(&drawn_by, child) {
                return Some(why);
            }
            if !ranked(draws, child) {
                return Some("it is not numeric".to_string());
            }
            if let Some((kind, held)) = held {
                return Some(format!("its {kind} is given {}", profile[held].name));
            }
            if same_copula(child, parent) {
                return Some(format!("{} shares its copula", profile[parent].name));
            }
            planner.draws_from(parent, child).then(|| looped(parent))
        };
        for (at, conditional) in dataset
            .and_then(|d| d.conditional_quantiles.as_ref())
            .into_iter()
            .flatten()
            .enumerate()
        {
            let field = format!("/dataset/conditional_quantiles/{at}");
            let child = planner.column(&conditional.column, &format!("{field}/column"))?;
            let parent = planner.column(&conditional.given, &format!("{field}/given"))?;
            let held = curves[child].as_ref().map(|curves| ("quantiles", curves.parent));
            if let Some(why) = unwarped(&planner, &draws, child, parent, held) {
                set_aside.entry(field, child, format!("quantiles given {}", conditional.given), why);
                continue;
            }
            let ValueSampler::Numeric(sampler) = &planner.columns[child].values else {
                unreachable!("a ranked column is numeric")
            };
            let own = RankedMarginal::of(sampler);
            let rates = dataset
                .and_then(|d| d.conditional_fractions.as_ref())
                .into_iter()
                .flatten()
                .find(|rates| rates.given == conditional.given && rates.column == conditional.column);
            let other_zeros = rates.and_then(|rates| rates.other.as_ref()).and_then(|other| other.zero_fraction);
            let zeros_of = |given: Option<&String>| {
                let rates = rates.filter(|rates| rates.zero_fraction.is_some())?;
                given
                    .and_then(|value| rates.zero_fraction.as_ref().and_then(|by| by.get(value)).copied())
                    .or(other_zeros)
            };
            let curve = |quantiles: &Quantiles, zero_fraction: Option<Rate>, field: String| {
                let refused = |message: String| PlanError::Column {
                    code: ColumnFault::InvalidQuantiles,
                    field: field.clone(),
                    message,
                };
                let stated = quantile_knots(Some(quantiles)).map_err(refused)?;
                if stated.iter().any(|(_, value)| !value.is_finite()) || stated.windows(2).any(|pair| pair[1].1 < pair[0].1) {
                    return Err(refused("the quantiles fall, or one is not finite".into()));
                }
                Ok(own.warp(&stated, zero_fraction.map(|rate| rate.0)))
            };
            let mut by_given = Vec::with_capacity(conditional.quantiles.len());
            for (given, quantiles) in &conditional.quantiles {
                by_given.push(curve(
                    quantiles,
                    zeros_of(Some(given)),
                    format!("{field}/quantiles/{}", crate::perturbations::pointer_token(given)),
                )?);
            }
            let other = conditional
                .other
                .as_ref()
                .map(|quantiles| curve(quantiles, zeros_of(None), format!("{field}/other")))
                .transpose()?;
            planner.depend(parent, child, &format!("{field}/given"))?;
            curves[child] = Some(ParentCurves {
                parent,
                keys: planner.keys(conditional.quantiles.keys()),
                curves: by_given,
                other,
            });
            warp_entries[child].push(("conditional_quantiles", field));
        }
        let mut zeros: Vec<Option<ParentShares>> = vec![None; profile.len()];
        for (at, rates) in dataset
            .and_then(|d| d.conditional_fractions.as_ref())
            .into_iter()
            .flatten()
            .enumerate()
        {
            let field = format!("/dataset/conditional_fractions/{at}");
            let child = planner.column(&rates.column, &format!("{field}/column"))?;
            let parent = planner.column(&rates.given, &format!("{field}/given"))?;
            let other = rates.other.as_ref();
            if let Some(zero_fraction) = &rates.zero_fraction {
                let pointer = format!("{field}/zero_fraction");
                let held = zeros[child].as_ref().map(|zeros| ("zero rate", zeros.parent));
                if let Some(why) = unwarped(&planner, &draws, child, parent, held) {
                    set_aside.entry(pointer, child, format!("zero rate given {}", rates.given), why);
                } else {
                    planner.depend(parent, child, &format!("{field}/given"))?;
                    zeros[child] = Some(ParentShares {
                        parent,
                        keys: planner.keys(zero_fraction.keys()),
                        shares: zero_fraction.values().map(|rate| rate.0).collect(),
                        other: other.and_then(|other| other.zero_fraction).map(|rate| rate.0),
                    });
                    warp_entries[child].push(("conditional_zero_fraction", pointer));
                }
            }
            if let Some(null_fraction) = &rates.null_fraction {
                let why = if let Some(why) = fixed.get(&child) {
                    Some(why.clone())
                } else if !planner.columns[child].field.is_nullable() {
                    Some("its field holds no nulls".to_string())
                } else if let Some(nulls) = &draws[child].nulls {
                    Some(format!("its null rate is given {}", profile[nulls.parent].name))
                } else {
                    planner.draws_from(parent, child).then(|| looped(parent))
                };
                if let Some(why) = why {
                    set_aside.entry(
                        format!("{field}/null_fraction"),
                        child,
                        format!("null rate given {}", rates.given),
                        why,
                    );
                    continue;
                }
                planner.depend(parent, child, &format!("{field}/given"))?;
                draws[child].nulls = Some(ParentTrials {
                    parent,
                    keys: planner.keys(null_fraction.keys()),
                    trials: null_fraction.values().map(|rate| Bernoulli::new(rate.0)).collect(),
                    other: other.and_then(|other| other.null_fraction).map(|rate| Bernoulli::new(rate.0)),
                });
                draws[child]
                    .sources
                    .push(("conditional_null_fraction", format!("{field}/null_fraction")));
                notes[child].push(format!("its null rate given {}", rates.given));
            }
        }
        let scores = |grid: &LatentGrid, index: usize| {
            let ValueSampler::Numeric(sampler) = &planner.columns[index].values else {
                unreachable!("a ranked column is numeric")
            };
            let marginal = RankedMarginal::of(sampler);
            let (levels, tied) = marginal.tie_scores(&grid.levels, TIE_SHARE);
            let own = RankScores::of(grid, &levels, tied);
            if curves[index].is_none() && zeros[index].is_none() {
                return MemberScores { own, warped: None };
            }
            let (zero_low, zero_high) = marginal.zero_band();
            let (curves, zeros) = (curves[index].as_ref(), zeros[index].as_ref());
            let (combos, rows) = parent_mixture(planner.columns, curves, zeros);
            let scored: Vec<(f64, Vec<f64>)> = combos
                .into_iter()
                .map(|(share, warp, zero)| {
                    let at = |level: f64| {
                        marginal_level(&marginal, warp, zero, level).map_or((zero_low + zero_high) / 2.0, |at| marginal.score_at(at))
                    };
                    (share, grid.levels.iter().map(|&level| at(level)).collect())
                })
                .collect();
            let mixture: Vec<(f64, &[f64])> = scored.iter().map(|(share, scores)| (*share, scores.as_slice())).collect();
            let warped = CurveScores::of(grid, &mixture).map(|by_curve| Warped {
                mixed: by_curve.mixed(),
                by_curve,
                rows,
                parents: [curves.map(|c| c.parent), zeros.map(|z| z.parent)].into_iter().flatten().collect(),
            });
            MemberScores { own, warped }
        };
        let (groups, warps_aside) = copula_groups(context, profile, &spearman, &component, scores);
        let name = |index: usize| profile[index].name.as_str();
        let given = |curves: Option<&ParentCurves>, zeros: Option<&ParentShares>| {
            let kinds = [
                curves.map(|curves| format!("quantiles given {}", name(curves.parent))),
                zeros.map(|zeros| format!("zero rate given {}", name(zeros.parent))),
            ];
            kinds.into_iter().flatten().collect::<Vec<_>>().join(" and ")
        };
        for (index, partners) in warps_aside {
            let (curves, zeros) = (curves[index].take(), zeros[index].take());
            let note = format!(
                "its {} set aside for its Spearman with {}, which no copula holds beside it",
                given(curves.as_ref(), zeros.as_ref()),
                partners.join(", ")
            );
            set_aside.pointers.extend(warp_entries[index].drain(..).map(|(_, pointer)| pointer));
            set_aside.notes.push((index, note));
        }
        for (child, note) in set_aside.notes {
            notes[child].push(note);
        }
        for index in 0..profile.len() {
            if curves[index].is_some() || zeros[index].is_some() {
                notes[index].insert(0, format!("its {}", given(curves[index].as_ref(), zeros[index].as_ref())));
            }
        }
        let mut membership: Vec<Option<(usize, usize)>> = vec![None; profile.len()];
        for (group_at, group) in groups.iter().enumerate() {
            for (member_at, &member) in group.members.iter().enumerate() {
                membership[member] = Some((group_at, member_at));
                let others: Vec<&str> = group
                    .names
                    .iter()
                    .filter(|name| **name != profile[member].name)
                    .map(String::as_str)
                    .collect();
                let repaired = match (group.independent, group.repaired) {
                    (true, _) => ", each member drawn alone, since no valid matrix lies near its correlations",
                    (false, true) => ", its correlations repaired to the nearest valid matrix",
                    (false, false) => "",
                };
                let name = profile[member].name.as_str();
                let beyond: Vec<&str> = group
                    .unreachable
                    .iter()
                    .filter_map(|(a, b)| (a == name).then_some(b.as_str()).or((b == name).then_some(a.as_str())))
                    .collect();
                let beyond = match beyond.is_empty() {
                    true => String::new(),
                    false => format!(", its ties holding no copula at its Spearman with {}", beyond.join(", ")),
                };
                notes[member].push(format!("ranked with {} in a Gaussian copula{repaired}{beyond}", others.join(", ")));
            }
        }
        for index in 0..profile.len() {
            let (copula, curves, zeros) = (membership[index], curves[index].take(), zeros[index].take());
            if copula.is_none() && curves.is_none() && zeros.is_none() {
                continue;
            }
            let ValueSampler::Numeric(sampler) = &planner.columns[index].values else {
                unreachable!("a ranked column is numeric")
            };
            draws[index].values = Some(ValueDraw::Ranked(Box::new(RankedDraw {
                copula,
                own: RankedMarginal::of(sampler),
                curves,
                zeros,
                output: sampler.output,
            })));
            draws[index].sources.append(&mut warp_entries[index]);
        }
        let order = topological_order(&planner.children);
        drop(planner);
        for ((column, described), notes) in columns.iter_mut().zip(described).zip(notes) {
            if let Some(described) = described {
                column.sampler = described;
            }
            if !notes.is_empty() {
                column.sampler = format!("{}, {}", column.sampler, notes.join(", "));
            }
        }
        Ok(Self {
            order,
            groups,
            draws,
            set_aside: set_aside.pointers,
        })
    }

    /// Whether every column is drawn alone, as it is from a profile stating no dependence.
    pub(crate) fn is_empty(&self) -> bool {
        self.groups.is_empty() && self.draws.iter().all(|draw| draw.values.is_none() && draw.nulls.is_none())
    }

    /// Each Gaussian copula's member names, sorted.
    pub(crate) fn copula_groups(&self) -> Vec<Vec<String>> {
        self.groups.iter().map(|group| group.names.clone()).collect()
    }

    /// The JSON pointer of each dependence entry the plan does not draw.
    pub(crate) fn set_aside(&self) -> &[String] {
        &self.set_aside
    }

    /// Each dependence entry drawing the `index`th column, by its kind and pointer.
    pub(crate) fn sources(&self, index: usize) -> &[(&'static str, String)] {
        self.draws.get(index).map_or(&[], |draw| draw.sources.as_slice())
    }

    /// The columns derived or looked up from others, in the order that computes each after what it reads.
    pub(crate) fn following(&self) -> FollowingColumns {
        let columns = self.order.iter().filter_map(|&index| match &self.draws[index].values {
            Some(draw @ (ValueDraw::Lookup(_) | ValueDraw::Timedelta(_) | ValueDraw::Formula(_))) => Some((index, draw.clone())),
            _ => None,
        });
        FollowingColumns {
            columns: columns.collect(),
        }
    }

    /// The block's columns at `at`, each drawn after the columns it depends on, and each `given` holds taken
    /// as it holds it.
    pub(crate) fn generate(&self, columns: &[ColumnPlan], at: &BlockPosition, rows: usize, given: &[Option<GivenColumn>]) -> DrawnColumns {
        let levels: Vec<Vec<Vec<f64>>> = self.groups.iter().map(|group| group.levels(at.seed, at.block, rows)).collect();
        let mut arrays: Vec<Option<ArrayRef>> = vec![None; columns.len()];
        let mut masks: Vec<Option<BooleanBuffer>> = vec![None; columns.len()];
        for &index in &self.order {
            if let Some(Some(given)) = given.get(index) {
                arrays[index] = Some(given.array.clone());
                masks[index] = given.nulls.clone();
                continue;
            }
            let column = &columns[index];
            let draw = &self.draws[index];
            let drawn_nulls = match &draw.nulls {
                Some(trials) => Some(trials.mask(column.nulls.unwrap_or(Bernoulli::new(0.0)), at, &column.keys, rows, &arrays)),
                None => column.drawn_nulls(at, rows),
            };
            let nulls = drawn_nulls
                .as_ref()
                .map(|nulls| NullBuffer::new(!nulls))
                .filter(|nulls| nulls.null_count() > 0);
            let array = match &draw.values {
                None => column.values.generate(at, &column.keys, rows, nulls),
                Some(values) => values.generate(column, at, rows, nulls, &arrays, &levels),
            };
            arrays[index] = Some(array);
            masks[index] = drawn_nulls;
        }
        // Every dependence read its parents' own typed draws, so a column generating as text is written so last.
        let given_at = |index: usize| matches!(given.get(index), Some(Some(_)));
        DrawnColumns {
            arrays: arrays
                .into_iter()
                .enumerate()
                .map(|(index, array)| {
                    let array = array.expect("the order holds every column");
                    if given_at(index) {
                        array
                    } else {
                        columns[index].rendered(at, array)
                    }
                })
                .collect(),
            nulls: masks,
        }
    }
}

/// `table`'s Spearman correlations between columns `ranked` admits, by the pair's indexes, the lower first.
fn spearman_pairs(table: &TableProfile, profile: &[Column], ranked: impl Fn(usize) -> bool) -> BTreeMap<(usize, usize), f64> {
    let names: HashMap<&str, usize> = profile
        .iter()
        .enumerate()
        .map(|(index, column)| (column.name.as_str(), index))
        .collect();
    let correlations = table.dataset.as_ref().and_then(|d| d.correlations.as_deref()).unwrap_or_default();
    let mut stored: BTreeMap<(usize, usize), f64> = BTreeMap::new();
    for correlation in correlations
        .iter()
        .filter(|c| c.method == CorrelationMethod::Spearman && c.value.is_finite())
    {
        let (Some(&a), Some(&b)) = (names.get(correlation.a.as_str()), names.get(correlation.b.as_str())) else {
            continue;
        };
        if a != b && ranked(a) && ranked(b) {
            stored.entry((a.min(b), a.max(b))).or_insert(correlation.value);
        }
    }
    stored
}

/// Each of `columns` columns' copula group among the pairs `stored` holds, as the least index of its connected set, or
/// `None` where it holds no pair.
fn copula_components(stored: &BTreeMap<(usize, usize), f64>, columns: usize) -> Vec<Option<usize>> {
    let mut root: Vec<usize> = (0..columns).collect();
    fn find(root: &mut [usize], mut at: usize) -> usize {
        while root[at] != at {
            root[at] = root[root[at]];
            at = root[at];
        }
        at
    }
    for &(a, b) in stored.keys() {
        let (a, b) = (find(&mut root, a), find(&mut root, b));
        root[a.max(b)] = a.min(b);
    }
    let mut component = vec![None; columns];
    for &(a, b) in stored.keys() {
        for index in [a, b] {
            component[index] = Some(find(&mut root, index));
        }
    }
    component
}

/// A copula member's rank scores through its own marginal, and through its parents' warps where it has any.
struct MemberScores {
    own: RankScores,
    warped: Option<Warped>,
}

/// A member's scores through its parents' warps: curve by curve, mixed as a partner choosing its curves apart reads
/// them, the curve each row of [`parent_mixture`]'s draw takes, and the parents choosing it.
struct Warped {
    by_curve: CurveScores,
    mixed: RankScores,
    rows: Vec<u32>,
    parents: Vec<usize>,
}

impl MemberScores {
    fn scores(&self) -> &RankScores {
        self.warped.as_ref().map_or(&self.own, |warped| &warped.mixed)
    }
}

/// The series two members' Spearman value takes in their normals' Pearson. Members warped by one parent choose their
/// curves together, row by row, so their pairs of curves are counted from the same rows of their parents' draw.
fn pair_series(a: &MemberScores, b: &MemberScores, grid: &LatentGrid) -> PairSeries {
    match (&a.warped, &b.warped) {
        (Some(a), Some(b)) if a.parents.iter().any(|parent| b.parents.contains(parent)) => {
            let mut counts: BTreeMap<(u32, u32), usize> = BTreeMap::new();
            for pair in a.rows.iter().copied().zip(b.rows.iter().copied()) {
                *counts.entry(pair).or_default() += 1;
            }
            let joint: Vec<(usize, usize, f64)> = counts
                .into_iter()
                .map(|((first, second), count)| (first as usize, second as usize, count as f64 / a.rows.len() as f64))
                .collect();
            PairSeries::joint(&a.by_curve, &b.by_curve, &joint)
        }
        _ => PairSeries::apart(a.scores(), b.scores(), grid),
    }
}

/// The copula groups of the Spearman correlations `stored` holds, one per `component`, its members ordered by name
/// and each scored on the latent grid by `scores`. A member's warps are set aside where through them a stated pair
/// holds no Pearson that its own marginal would hold, so the copula keeps the pair. Each member set aside is
/// returned with the partners of those pairs.
fn copula_groups(
    context: &PlanContext,
    profile: &[Column],
    stored: &BTreeMap<(usize, usize), f64>,
    component: &[Option<usize>],
    scores: impl Fn(&LatentGrid, usize) -> MemberScores,
) -> (Vec<CopulaGroup>, Vec<(usize, Vec<String>)>) {
    let names: HashMap<&str, usize> = profile
        .iter()
        .enumerate()
        .map(|(index, column)| (column.name.as_str(), index))
        .collect();
    let mut members: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for (index, root) in component.iter().enumerate() {
        if let Some(root) = root {
            members.entry(*root).or_default().push(index);
        }
    }
    let grid = LatentGrid::new();
    let mut groups = Vec::with_capacity(members.len());
    let mut set_aside = Vec::new();
    for group in members.into_values() {
        let mut scored: Vec<(usize, MemberScores)> = group.into_iter().map(|index| (index, scores(&grid, index))).collect();
        for at in 0..scored.len() {
            let (index, member) = (scored[at].0, &scored[at].1);
            if member.warped.is_none() {
                continue;
            }
            let beyond: Vec<String> = scored
                .iter()
                .filter(|(other, _)| *other != index)
                .filter_map(|(other, partner)| {
                    let rho = stored.get(&(index.min(*other), index.max(*other)))?.clamp(-1.0, 1.0);
                    let lost = calibrated(rho, &pair_series(member, partner, &grid)).is_none()
                        && calibrated(rho, &PairSeries::apart(&member.own, partner.scores(), &grid)).is_some();
                    lost.then(|| profile[*other].name.clone())
                })
                .collect();
            if !beyond.is_empty() {
                scored[at].1.warped = None;
                set_aside.push((index, beyond));
            }
        }
        let by_index: HashMap<usize, &MemberScores> = scored.iter().map(|(index, scores)| (*index, scores)).collect();
        let members = scored.iter().map(|(index, _)| (*index, profile[*index].name.clone())).collect();
        groups.push(CopulaGroup::new(
            context.table,
            members,
            |a, b| {
                let (a, b) = (names[a], names[b]);
                stored.get(&(a.min(b), a.max(b))).copied()
            },
            |a, b, rho| calibrated(rho, &pair_series(by_index[&a], by_index[&b], &grid)),
        ));
    }
    (groups, set_aside)
}

/// The rows a parent's places are counted over, in a draw of its own values.
const SHARE_ROWS: usize = 1 << 16;

/// One curve a member's rows draw through: the share of rows taking it, its parent's warp, and its zero share.
type CurveChoice<'a> = (f64, Option<&'a LevelWarp>, Option<f64>);

/// Each `(share, warp, zero share)` a member's rows draw through, by the share of rows its parents' values send
/// through it, counted in a draw of each parent's own values, and the place among them each row of that draw takes.
/// A parent outside the member's copula group is drawn apart from it, so its value is independent of the member's
/// level.
fn parent_mixture<'a>(
    columns: &[ColumnPlan],
    curves: Option<&'a ParentCurves>,
    zeros: Option<&ParentShares>,
) -> (Vec<CurveChoice<'a>>, Vec<u32>) {
    let at = BlockPosition {
        seed: 0,
        block: 0,
        first_row: 0,
    };
    let places = |parent: usize, keys: &ParentKeys| {
        let column = &columns[parent];
        let nulls = column
            .drawn_nulls(&at, SHARE_ROWS)
            .map(|nulls| NullBuffer::new(!&nulls))
            .filter(|nulls| nulls.null_count() > 0);
        keys.places(&column.values.generate(&at, &column.keys, SHARE_ROWS, nulls))
    };
    let curve_places = curves.map(|curves| places(curves.parent, &curves.keys));
    let zero_places = zeros.map(|zeros| places(zeros.parent, &zeros.keys));
    let combos: Vec<(u32, u32)> = (0..SHARE_ROWS)
        .map(|row| {
            let place = |places: &Option<Vec<u32>>| places.as_ref().map_or(NULL, |places| places[row]);
            (place(&curve_places), place(&zero_places))
        })
        .collect();
    let mut counts: BTreeMap<(u32, u32), usize> = BTreeMap::new();
    for combo in &combos {
        *counts.entry(*combo).or_default() += 1;
    }
    let at: HashMap<(u32, u32), u32> = counts.keys().enumerate().map(|(at, combo)| (*combo, at as u32)).collect();
    let mixture = counts
        .into_iter()
        .map(|((curve, zero), count)| {
            (
                count as f64 / SHARE_ROWS as f64,
                curves.and_then(|curves| curves.warp(curve)),
                zeros.and_then(|zeros| zeros.share(zero)),
            )
        })
        .collect();
    (mixture, combos.iter().map(|combo| at[combo]).collect())
}

/// The level of `own` a row at `level` draws, through its parent's `warp` and `zero` share where stated, or `None`
/// where the row draws zero.
fn marginal_level(own: &RankedMarginal, warp: Option<&LevelWarp>, zero: Option<f64>, level: f64) -> Option<f64> {
    let mut level = level;
    if let Some(share) = zero {
        level = with_zero_share(level, warp.map_or(own.zero_band(), LevelWarp::zero_band), share)?;
    }
    Some(warp.map_or(level, |warp| warp.apply(level)))
}

/// Every column, each after the columns it depends on, ties broken by the columns' order.
fn topological_order(children: &[BTreeSet<usize>]) -> Vec<usize> {
    let mut parents = vec![0usize; children.len()];
    for child in children.iter().flatten() {
        parents[*child] += 1;
    }
    let mut ready: BTreeSet<usize> = (0..children.len()).filter(|&index| parents[index] == 0).collect();
    let mut order = Vec::with_capacity(children.len());
    while let Some(index) = ready.pop_first() {
        order.push(index);
        for &child in &children[index] {
            parents[child] -= 1;
            if parents[child] == 0 {
                ready.insert(child);
            }
        }
    }
    order
}

impl ValueDraw {
    fn generate(
        &self,
        column: &ColumnPlan,
        at: &BlockPosition,
        rows: usize,
        nulls: Option<NullBuffer>,
        arrays: &[Option<ArrayRef>],
        levels: &[Vec<Vec<f64>>],
    ) -> ArrayRef {
        let keys = &column.keys;
        match self {
            Self::Ranked(draw) => {
                let own;
                let levels: &[f64] = match draw.copula {
                    Some((group, member)) => &levels[group][member],
                    None => {
                        let mut rng = rng_for(at.seed, keys.values, at.block);
                        own = (0..rows).map(|_| unit(&mut rng)).collect::<Vec<_>>();
                        &own
                    }
                };
                let curve_places = draw.curves.as_ref().map(|curves| curves.keys.places(drawn(arrays, curves.parent)));
                let zero_places = draw.zeros.as_ref().map(|zeros| zeros.keys.places(drawn(arrays, zeros.parent)));
                let mut row = 0;
                draw.output.drawn_array(rows, nulls, || {
                    let at_row = row;
                    row += 1;
                    let warp = draw
                        .curves
                        .as_ref()
                        .zip(curve_places.as_ref())
                        .and_then(|(curves, places)| curves.warp(places[at_row]));
                    let zero = draw
                        .zeros
                        .as_ref()
                        .zip(zero_places.as_ref())
                        .and_then(|(zeros, places)| zeros.share(places[at_row]));
                    match marginal_level(&draw.own, warp, zero, levels[at_row]) {
                        Some(level) => draw.own.value_at(level, at.seed, keys),
                        None => 0.0,
                    }
                })
            }
            Self::Table(draw) => {
                let places = draw.keys.places(drawn(arrays, draw.parent));
                let mut rng = rng_for(at.seed, keys.given, at.block);
                let chosen: Vec<Option<u32>> = places
                    .iter()
                    .map(|&place| {
                        let row = match place {
                            NULL => None,
                            OTHER => draw.other.as_ref(),
                            listed => Some(&draw.rows[listed as usize]),
                        };
                        row.and_then(|row| row.values[row.choice.sample(&mut rng)])
                    })
                    .collect();
                draw.values.array(&chosen, column, at, rows, nulls)
            }
            Self::Lookup(draw) => {
                let places = draw.keys.places(drawn(arrays, draw.lhs));
                let mut rng = rng_for(at.seed, keys.given, at.block);
                let chosen: Vec<Option<u32>> = places
                    .iter()
                    .map(|&place| {
                        let violated = draw.violation.is_some_and(|violation| violation.draw(&mut rng));
                        (place != NULL && place != OTHER && !violated).then_some(place)
                    })
                    .collect();
                draw.values.array(&chosen, column, at, rows, nulls)
            }
            Self::Timedelta(draw) => {
                let mut rng = rng_for(at.seed, keys.values, at.block);
                let lags: Vec<i128> = (0..rows)
                    .map(|_| ((draw.lag.value_at(unit(&mut rng)) / draw.step).round() * draw.step) as i128 * NANOS_PER_MICRO)
                    .collect();
                draw.offset(drawn(arrays, draw.base), |row| Some(lags[row]), nulls.as_ref())
            }
            Self::Formula(draw) => {
                let refs: Vec<&ArrayRef> = draw.refs.iter().map(|&reference| drawn(arrays, reference)).collect();
                let computed = draw.computed(&refs, rows, nulls.as_ref());
                let Some(violation) = draw.violation else {
                    return computed;
                };
                let mut rng = rng_for(at.seed, keys.given, at.block);
                let holds = BooleanArray::from(BooleanBuffer::collect_bool(rows, |_| !violation.draw(&mut rng)));
                if holds.true_count() == rows {
                    return computed;
                }
                let own = column.values.generate(at, keys, rows, nulls);
                arrow_select::zip::zip(&holds, &computed, &own).expect("both arrays hold the column's type and the block's rows")
            }
        }
    }
}

impl ChildValues {
    /// The column with each row's chosen value, or its own draw where none was chosen.
    fn array(&self, chosen: &[Option<u32>], column: &ColumnPlan, at: &BlockPosition, rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
        let picked = self.picked(chosen, rows, nulls.as_ref());
        if chosen.iter().all(Option::is_some) {
            return picked;
        }
        let own = column.values.generate(at, &column.keys, rows, nulls);
        let from_table = BooleanArray::from(chosen.iter().map(Option::is_some).collect::<Vec<_>>());
        arrow_select::zip::zip(&from_table, &picked, &own).expect("both arrays hold the column's type and the block's rows")
    }

    /// The column with each row's chosen value, a placeholder where none was chosen.
    fn picked(&self, chosen: &[Option<u32>], rows: usize, nulls: Option<&NullBuffer>) -> ArrayRef {
        let mut row = 0;
        let mut next = || {
            row += 1;
            chosen[row - 1].map(|place| place as usize)
        };
        match self {
            Self::Text(output, values) => output.build(rows, nulls, |out| {
                if let Some(place) = next() {
                    out.push_str(&values[place]);
                }
            }),
            Self::Numeric(output, values) => output.drawn_array(rows, nulls.cloned(), || next().map_or(0.0, |place| values[place])),
            Self::Boolean(values) => {
                let picked = BooleanBuffer::collect_bool(rows, |row| chosen[row].is_some_and(|place| values[place as usize]));
                Arc::new(BooleanArray::new(picked, nulls.cloned()))
            }
        }
    }
}

impl TimedeltaDraw {
    /// The column holding each row of `base` moved by its lag in nanoseconds, null where either is.
    fn offset(&self, base: &ArrayRef, lag: impl Fn(usize) -> Option<i128>, nulls: Option<&NullBuffer>) -> ArrayRef {
        let base_nanos = temporal_nanos(base);
        let lags: Vec<Option<i128>> = (0..base.len()).map(lag).collect();
        let nanos: Vec<i128> = base_nanos.iter().zip(&lags).map(|(base, lag)| base + lag.unwrap_or(0)).collect();
        let lagged = NullBuffer::from(lags.iter().map(Option::is_some).collect::<Vec<_>>());
        let nulls = NullBuffer::union(nulls, base.logical_nulls().as_ref());
        temporal_array(&self.output, &nanos, NullBuffer::union(nulls.as_ref(), Some(&lagged)))
    }
}

impl FormulaDraw {
    /// The expression over `refs`, each the column one of its names reads, null where a reference is null or the
    /// result is not a finite number.
    fn computed(&self, refs: &[&ArrayRef], rows: usize, nulls: Option<&NullBuffer>) -> ArrayRef {
        let refs: Vec<ArrayRef> = refs
            .iter()
            .map(|array| arrow_cast::cast(array, &DataType::Float64).expect("a numeric column casts to Float64"))
            .collect();
        let floats: Vec<&PrimitiveArray<Float64Type>> = refs.iter().map(|array| array.as_primitive::<Float64Type>()).collect();
        let mut inputs = vec![0.0; floats.len()];
        let mut values = Vec::with_capacity(rows);
        let mut valid = Vec::with_capacity(rows);
        for row in 0..rows {
            let value = match floats.iter().all(|array| array.is_valid(row)) {
                true => {
                    for (input, array) in inputs.iter_mut().zip(&floats) {
                        *input = array.value(row);
                    }
                    self.rounding.apply(self.formula.evaluate(&inputs))
                }
                false => f64::NAN,
            };
            valid.push(value.is_finite());
            values.push(if value.is_finite() { value } else { 0.0 });
        }
        let computed_nulls = NullBuffer::union(nulls, Some(&NullBuffer::from(valid)));
        let mut row = 0;
        self.output.drawn_array(rows, computed_nulls, || {
            row += 1;
            values[row - 1]
        })
    }
}

/// The columns whose every value follows others': derived, or looked up. A change to a row recomputes them, and
/// never draws them anew.
#[derive(Debug, Clone, Default)]
pub(crate) struct FollowingColumns {
    /// Each column and its draw, every one after the columns it reads.
    columns: Vec<(usize, ValueDraw)>,
}

impl FollowingColumns {
    pub(crate) fn follows(&self, index: usize) -> bool {
        self.columns.iter().any(|(column, _)| *column == index)
    }

    /// Each following column with the columns it reads.
    pub(crate) fn reads(&self) -> impl Iterator<Item = (usize, Vec<usize>)> + '_ {
        self.columns.iter().filter_map(|(index, draw)| Some((*index, read_columns(draw)?)))
    }

    /// Recomputes each following column of `image` on the rows where a column it reads changed. `image` holds rows
    /// of `base`, the `i`th at `base_rows[i]`. `changed[column][i]` holds where the image's value is not the base
    /// row's, and it gains every row recomputed. A lag keeps its base row's length.
    pub(crate) fn refresh(&self, image: &mut [ArrayRef], base: &[ArrayRef], base_rows: &[usize], changed: &mut [Vec<bool>]) {
        let rows = base_rows.len();
        for (index, draw) in &self.columns {
            let Some(reads) = read_columns(draw) else { continue };
            let mut recompute: Vec<bool> = (0..rows).map(|row| reads.iter().any(|&read| changed[read][row])).collect();
            if !recompute.contains(&true) {
                continue;
            }
            let nulls = image[*index].logical_nulls();
            let recomputed = match draw {
                ValueDraw::Lookup(lookup) => {
                    let places = lookup.keys.places(&image[lookup.lhs]);
                    let chosen: Vec<Option<u32>> = places
                        .iter()
                        .map(|&place| (place != NULL && place != OTHER).then_some(place))
                        .collect();
                    for (recompute, chosen) in recompute.iter_mut().zip(&chosen) {
                        *recompute &= chosen.is_some();
                    }
                    lookup.values.picked(&chosen, rows, nulls.as_ref())
                }
                ValueDraw::Timedelta(timedelta) => {
                    let (lagged, from) = (&base[*index], &base[timedelta.base]);
                    let (lagged_nanos, from_nanos) = (temporal_nanos(lagged), temporal_nanos(from));
                    let base_lag = |row: usize| {
                        let at = base_rows[row];
                        (lagged.is_valid(at) && from.is_valid(at)).then(|| lagged_nanos[at] - from_nanos[at])
                    };
                    timedelta.offset(&image[timedelta.base], base_lag, None)
                }
                ValueDraw::Formula(formula) => {
                    let refs: Vec<&ArrayRef> = formula.refs.iter().map(|&reference| &image[reference]).collect();
                    formula.computed(&refs, rows, None)
                }
                ValueDraw::Ranked(_) | ValueDraw::Table(_) => unreachable!("only a following column is recomputed"),
            };
            let mask = BooleanArray::from(recompute.clone());
            image[*index] = arrow_select::zip::zip(&mask, &recomputed, &image[*index])
                .expect("both arrays hold the column's type and the image's rows");
            for (changed, recomputed) in changed[*index].iter_mut().zip(recompute) {
                *changed |= recomputed;
            }
        }
    }
}

/// The columns a following column's `draw` reads, None for a draw that follows none.
fn read_columns(draw: &ValueDraw) -> Option<Vec<usize>> {
    match draw {
        ValueDraw::Lookup(lookup) => Some(vec![lookup.lhs]),
        ValueDraw::Timedelta(timedelta) => Some(vec![timedelta.base]),
        ValueDraw::Formula(formula) => Some(formula.refs.clone()),
        ValueDraw::Ranked(_) | ValueDraw::Table(_) => None,
    }
}

/// Each row of a date or timestamp column in nanoseconds since the epoch, a null as 0.
fn temporal_nanos(array: &ArrayRef) -> Vec<i128> {
    let scaled = |per: i128| {
        let integers = arrow_cast::cast(array, &DataType::Int64).expect("a date or timestamp casts to Int64");
        let integers = integers.as_primitive::<Int64Type>();
        (0..array.len()).map(|row| i128::from(integers.value(row)) * per).collect()
    };
    match array.data_type() {
        DataType::Date32 => {
            let days = array.as_primitive::<Date32Type>();
            (0..array.len()).map(|row| i128::from(days.value(row)) * NANOS_PER_DAY).collect()
        }
        DataType::Date64 => scaled(1_000_000),
        DataType::Timestamp(TimeUnit::Second, _) => scaled(1_000_000_000),
        DataType::Timestamp(TimeUnit::Millisecond, _) => scaled(1_000_000),
        DataType::Timestamp(TimeUnit::Microsecond, _) => scaled(1_000),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => scaled(1),
        _ => vec![0; array.len()],
    }
}

/// A column of `output` holding each instant, floored to its unit.
fn temporal_array(output: &TemporalOutput, nanos: &[i128], nulls: Option<NullBuffer>) -> ArrayRef {
    let floored = |per: i128| -> ScalarBuffer<i64> {
        nanos
            .iter()
            .map(|nanos| nanos.div_euclid(per).clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64)
            .collect()
    };
    match output {
        TemporalOutput::Date32 => {
            let days: ScalarBuffer<i32> = nanos
                .iter()
                .map(|nanos| nanos.div_euclid(NANOS_PER_DAY).clamp(i128::from(i32::MIN), i128::from(i32::MAX)) as i32)
                .collect();
            Arc::new(PrimitiveArray::<Date32Type>::new(days, nulls))
        }
        TemporalOutput::Date64 => Arc::new(PrimitiveArray::<Date64Type>::new(floored(1_000_000), nulls)),
        TemporalOutput::Timestamp(unit, zone) => match unit {
            TimeUnit::Second => {
                Arc::new(PrimitiveArray::<TimestampSecondType>::new(floored(1_000_000_000), nulls).with_timezone_opt(zone.clone()))
            }
            TimeUnit::Millisecond => {
                Arc::new(PrimitiveArray::<TimestampMillisecondType>::new(floored(1_000_000), nulls).with_timezone_opt(zone.clone()))
            }
            TimeUnit::Microsecond => {
                Arc::new(PrimitiveArray::<TimestampMicrosecondType>::new(floored(1_000), nulls).with_timezone_opt(zone.clone()))
            }
            TimeUnit::Nanosecond => {
                Arc::new(PrimitiveArray::<TimestampNanosecondType>::new(floored(1), nulls).with_timezone_opt(zone.clone()))
            }
        },
        TemporalOutput::Time32(_) | TemporalOutput::Time64(_) => unreachable!("a time of day takes no lag"),
    }
}
