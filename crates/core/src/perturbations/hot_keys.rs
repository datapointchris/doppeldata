//! `share` of rows take one of `keys` values, uniformly: the column's most frequent values first,
//! then values its own sampler draws on the op's stream.

use std::collections::BTreeSet;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_select::concat::concat;

use super::Block;
use super::BlockPerturbation;
use super::Planned;
use super::Planning;
use super::ScenarioFault;
use super::Stage;
use super::array_edits;
use crate::plan::PlanError;
use crate::rng::BLOCK_ROWS;
use crate::rng::below;
use crate::samplers::BlockPosition;
use crate::samplers::ColumnSite;
use crate::samplers::column_plan;
use crate::scenario::HotKeys;

/// The most blocks of the column's own values read while looking for distinct keys.
const KEY_SEARCH_BLOCKS: u64 = 16;

#[derive(Debug)]
struct HotKeyRows {
    column: usize,
    keys: ArrayRef,
    share: f64,
    rows_key: u64,
    keys_key: u64,
}

pub(super) fn plan(op: &HotKeys, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    let (column, field) = planning.column(&op.column, "/column")?;
    let wanted = op.keys as usize;
    let Some((profile_index, profiled)) = planning
        .table
        .columns
        .iter()
        .flatten()
        .enumerate()
        .find(|(_, profiled)| profiled.name == op.column)
    else {
        return Err(planning.refuse(
            ScenarioFault::OpNotForColumn,
            "/column",
            format!(
                "{:?} is written by another perturbation, and hot_keys draws keys from a profiled column",
                op.column
            ),
        ));
    };
    let mut top: Vec<(&str, f64)> = profiled
        .string
        .iter()
        .flat_map(|string| string.top_values.iter().flatten())
        .map(|top| (top.value.as_str(), top.rate.0))
        .collect();
    top.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(b.0)));
    let texts: Vec<Option<String>> = top.iter().take(wanted).map(|(value, _)| Some(value.to_string())).collect();
    let mut parts: Vec<ArrayRef> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    if let Ok(top_keys) = array_edits::cast_texts(&texts, field.data_type()) {
        seen.extend(array_edits::display_values(&top_keys).into_iter().flatten());
        parts.push(top_keys);
    }
    let table = planning.sampler_table();
    let context = planning.sampler_context(&table);
    let sampler = column_plan(&context, profiled, &ColumnSite::top_level(profile_index, profiled))?;
    let draw_rows = (wanted * 8).clamp(1_024, BLOCK_ROWS as usize);
    let mut block = 0;
    while seen.len() < wanted && block < KEY_SEARCH_BLOCKS {
        let position = BlockPosition {
            seed: planning.seed,
            block,
            first_row: block * BLOCK_ROWS,
        };
        let drawn = sampler.generate(&position, draw_rows);
        let mut fresh: Vec<u32> = Vec::new();
        for (row, value) in array_edits::display_values(&drawn).into_iter().enumerate() {
            if let Some(value) = value
                && seen.len() < wanted
                && seen.insert(value)
            {
                fresh.push(row as u32);
            }
        }
        parts.push(array_edits::take_rows(&drawn, &fresh));
        block += 1;
    }
    if seen.len() < wanted {
        return Err(planning.refuse(
            ScenarioFault::InvalidParameter,
            "/keys",
            format!(
                "{:?} yields {} distinct values, fewer than the {wanted} keys asked for",
                op.column,
                seen.len()
            ),
        ));
    }
    let parts: Vec<&dyn Array> = parts.iter().map(AsRef::as_ref).collect();
    Ok(planning.rows(HotKeyRows {
        column,
        keys: concat(&parts).expect("every key has the column's type"),
        share: op.share.0,
        rows_key: planning.key("rows"),
        keys_key: planning.key("keys"),
    }))
}

impl BlockPerturbation for HotKeyRows {
    fn apply(&self, stage: &Stage<'_>, mut block: Block) -> Block {
        let rows = array_edits::drawn_rows(&mut stage.stream(self.rows_key), self.share, block.batch.num_rows());
        let mut picks = stage.stream(self.keys_key);
        let chosen: Vec<u32> = rows.iter().map(|_| below(&mut picks, self.keys.len() as u32)).collect();
        let values = array_edits::take_rows(&self.keys, &chosen);
        let written = array_edits::replace_rows(block.batch.column(self.column), &rows, &values);
        array_edits::drop_cells(&mut block.cells, self.column, &rows);
        block.with_column(stage.schema, self.column, written)
    }
}
