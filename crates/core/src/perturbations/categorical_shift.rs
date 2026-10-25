//! The category mix moved. Each value named takes its share of the non-null rows, and every other
//! value keeps its proportion of what is left. Only rows of a value above its new share move, each
//! to a value below its own, so as few rows change as reach the mix. Values outside a string
//! column's `top_values` can shrink but never receive rows, since none of them is known.

use std::collections::HashMap;

use arrow_array::ArrayRef;
use arrow_schema::DataType;
use rand_distr::Distribution;
use rand_distr::weighted::WeightedAliasIndex;

use super::Block;
use super::BlockPerturbation;
use super::Planned;
use super::Planning;
use super::ScenarioFault;
use super::Stage;
use super::array_edits;
use super::pointer_token;
use crate::physical_type::physical_type_name;
use crate::plan::PlanError;
use crate::rng::unit;
use crate::scenario::CategoricalShift;

/// The slack allowed when the shares named sum to 1, so a sum off in its last bits is not refused.
const SHARE_SLACK: f64 = 1e-9;

#[derive(Debug)]
struct ShiftRows {
    column: usize,
    /// Each known value's category.
    categories: HashMap<String, usize>,
    /// The chance a row of each category moves. The last is every value outside the known ones.
    leave: Vec<f64>,
    /// The values a moved row takes, and a draw over them.
    destinations: ArrayRef,
    destination: WeightedAliasIndex<f64>,
    rows_key: u64,
    targets_key: u64,
}

pub(super) fn plan(op: &CategoricalShift, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    let (column, field) = planning.column(&op.column, "/column")?;
    let profiled = planning.profile_column(&op.column);
    let mut current: Vec<(String, f64)> = match field.data_type() {
        DataType::Boolean => {
            let true_rate = profiled
                .and_then(|c| c.boolean.as_ref())
                .and_then(|b| b.true_rate)
                .map_or(0.5, |r| r.0);
            vec![("true".into(), true_rate), ("false".into(), 1.0 - true_rate)]
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let string = profiled.and_then(|c| c.string.as_ref());
            let top: Vec<(String, f64)> = string
                .iter()
                .flat_map(|s| s.top_values.iter().flatten())
                .map(|top| (top.value.clone(), top.rate.0))
                .collect();
            if top.is_empty() {
                return Err(planning.refuse(
                    ScenarioFault::OpNotForColumn,
                    "/column",
                    format!("{:?} lists no top_values, so it has no category mix to move", op.column),
                ));
            }
            let other = string.and_then(|s| s.other_rate).map_or(0.0, |r| r.0);
            let total = top.iter().map(|(_, rate)| rate).sum::<f64>() + other;
            top.into_iter()
                .map(|(value, rate)| (value, rate / total))
                .chain([(String::new(), other / total)])
                .collect()
        }
        other => {
            return Err(planning.refuse(
                ScenarioFault::OpNotForColumn,
                "/column",
                format!(
                    "{:?} is {}, and categorical_shift acts on a string or boolean",
                    op.column,
                    physical_type_name(other)
                ),
            ));
        }
    };
    let string_column = field.data_type() != &DataType::Boolean;
    let known = current.len() - usize::from(string_column);
    for value in op.to.keys() {
        if !current[..known].iter().any(|(known, _)| known == value) {
            if !string_column {
                return Err(planning.refuse(
                    ScenarioFault::InvalidParameter,
                    &format!("/to/{}", pointer_token(value)),
                    format!("{value:?} is not a boolean value; name true or false"),
                ));
            }
            current.insert(known, (value.clone(), 0.0));
        }
    }
    let known = current.len() - usize::from(string_column);
    let named_target: f64 = op.to.values().map(|r| r.0).sum();
    let named_now: f64 = current.iter().filter(|(value, _)| op.to.contains_key(value)).map(|(_, p)| p).sum();
    if named_target > 1.0 + SHARE_SLACK {
        return Err(planning.refuse(
            ScenarioFault::InvalidParameter,
            "/to",
            format!("the shares sum to {named_target}, above 1"),
        ));
    }
    let rest_now = 1.0 - named_now;
    if rest_now <= SHARE_SLACK && named_target < 1.0 - SHARE_SLACK {
        return Err(planning.refuse(
            ScenarioFault::InvalidParameter,
            "/to",
            format!(
                "every value of {:?} is named, so the shares must sum to 1, not {named_target}",
                op.column
            ),
        ));
    }
    let scale = match rest_now > SHARE_SLACK {
        true => (1.0 - named_target).max(0.0) / rest_now,
        false => 0.0,
    };
    let target = |index: usize| {
        let (value, now) = &current[index];
        match (index < known).then(|| op.to.get(value)).flatten() {
            Some(share) => share.0,
            None => now * scale,
        }
    };
    let leave: Vec<f64> = (0..current.len())
        .map(|index| {
            let now = current[index].1;
            match now > 0.0 {
                true => ((now - target(index)) / now).max(0.0),
                false => 0.0,
            }
        })
        .collect();
    let receiving: Vec<(usize, f64)> = (0..known)
        .map(|index| (index, target(index) - current[index].1))
        .filter(|&(_, gain)| gain > 0.0)
        .collect();
    let (texts, weights): (Vec<Option<String>>, Vec<f64>) = match receiving.is_empty() {
        true => (vec![Some(current[0].0.clone())], vec![1.0]),
        false => receiving
            .iter()
            .map(|&(index, gain)| (Some(current[index].0.clone()), gain))
            .unzip(),
    };
    let destinations = array_edits::cast_texts(&texts, field.data_type())
        .map_err(|(_, message)| planning.refuse(ScenarioFault::InvalidParameter, "/to", message))?;
    let op = ShiftRows {
        column,
        categories: current[..known]
            .iter()
            .enumerate()
            .map(|(index, (value, _))| (value.clone(), index))
            .collect(),
        leave: match receiving.is_empty() {
            true => vec![0.0; current.len()],
            false => leave,
        },
        destinations,
        destination: WeightedAliasIndex::new(weights).expect("every weight is above zero"),
        rows_key: planning.key("rows"),
        targets_key: planning.key("targets"),
    };
    Ok(planning.rows(op))
}

impl BlockPerturbation for ShiftRows {
    fn apply(&self, stage: &Stage<'_>, block: Block) -> Block {
        let column = block.batch.column(self.column).clone();
        let mut leaving = stage.stream(self.rows_key);
        let mut targets = stage.stream(self.targets_key);
        let outside = self.leave.len() - 1;
        let mut rows: Vec<u32> = Vec::new();
        let mut picks: Vec<u32> = Vec::new();
        for (row, value) in array_edits::display_values(&column).into_iter().enumerate() {
            let draw = unit(&mut leaving);
            let Some(value) = value else { continue };
            let category = self.categories.get(&value).copied().unwrap_or(outside);
            if draw < self.leave[category] {
                rows.push(row as u32);
                picks.push(self.destination.sample(&mut targets) as u32);
            }
        }
        let values = array_edits::take_rows(&self.destinations, &picks);
        let written = array_edits::replace_rows(&column, &rows, &values);
        block.with_column(stage.schema, self.column, written)
    }
}
