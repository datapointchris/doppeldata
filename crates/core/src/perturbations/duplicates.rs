//! Copies of rows appended to each block, so that `rate` of the rows written are copies. Each
//! block's count is drawn before any row, so block boundaries never move.
//!
//! A `within_file` copy carries its source's home row, so it is filed with its source. An
//! `across_files` copy comes from the block half the dataset away, at the same offset as the home
//! row it is filed with. Its source and it then sit at least half the dataset apart, which no file
//! holding fewer rows than that can contain both of.

use arrow_array::RecordBatch;
use arrow_select::concat::concat_batches;
use arrow_select::take::take_record_batch;

use super::Block;
use super::BlockPerturbation;
use super::Planned;
use super::Planning;
use super::ScenarioFault;
use super::Stage;
use super::array_edits;
use crate::plan::PlanError;
use crate::rng::BLOCK_ROWS;
use crate::rng::Bernoulli;
use crate::rng::below;
use crate::rng::rng_for;
use crate::samplers::TextCell;
use crate::scenario::DuplicateScope;
use crate::scenario::Duplicates;

#[derive(Debug)]
struct CopyRows {
    /// Copies per row a block holds as it reaches the op: rate / (1 − rate).
    per_row: f64,
    scope: DuplicateScope,
    /// The columns a near-duplicate may vary, every one but the primary key's.
    varied: Option<Vec<usize>>,
    block_count: u64,
    rows: u64,
    count_key: u64,
    sources_key: u64,
    varied_key: u64,
}

pub(super) fn plan(op: &Duplicates, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    let rate = op.fraction.0;
    if rate >= 1.0 {
        return Err(planning.refuse(
            ScenarioFault::InvalidParameter,
            "/rate",
            "a rate of 1 makes every row a copy, of no row; it must lie below 1",
        ));
    }
    let keys: Vec<&str> = planning
        .table
        .dataset
        .as_ref()
        .and_then(|dataset| dataset.keys.as_ref())
        .and_then(|keys| keys.primary.as_ref())
        .into_iter()
        .flat_map(|key| key.columns.iter().map(String::as_str))
        .collect();
    let varied = match op.exact {
        true => None,
        false => {
            let candidates: Vec<usize> = (0..planning.schema.fields().len())
                .filter(|&index| !keys.contains(&planning.schema.field(index).name().as_str()))
                .collect();
            if candidates.is_empty() {
                return Err(planning.refuse(
                    ScenarioFault::InvalidParameter,
                    "/exact",
                    "a near-duplicate varies a column outside the primary key, and the table has none",
                ));
            }
            Some(candidates)
        }
    };
    Ok(planning.rows(CopyRows {
        per_row: rate / (1.0 - rate),
        scope: op.scope,
        varied,
        block_count: planning.block_count,
        rows: planning.rows,
        count_key: planning.key("count"),
        sources_key: planning.key("sources"),
        varied_key: planning.key("varied"),
    }))
}

impl CopyRows {
    /// The rows the plan generates in `block`, before any op adds one.
    fn generated_rows(&self, block: u64) -> u64 {
        (self.rows - block * BLOCK_ROWS).min(BLOCK_ROWS)
    }

    /// How many blocks away an `across_files` copy's source lies: half the dataset.
    fn partner_distance(&self) -> u64 {
        self.block_count / 2
    }

    /// The block whose rows `block`'s copies come from, wrapping round.
    fn partner(&self, block: u64) -> u64 {
        (block + self.partner_distance()) % self.block_count
    }
}

impl BlockPerturbation for CopyRows {
    fn added_rows(&self, seed: u64, block: u64, rows: usize) -> usize {
        let expected = rows as f64 * self.per_row;
        let whole = expected.floor();
        let mut count = rng_for(seed, self.count_key, block);
        whole as usize + usize::from(Bernoulli::new(expected - whole).draw(&mut count))
    }

    fn source_reach(&self) -> Option<(&'static str, u64)> {
        match self.scope {
            DuplicateScope::WithinFile => None,
            DuplicateScope::AcrossFiles => Some(("/scope", self.partner_distance() * BLOCK_ROWS)),
        }
    }

    fn apply(&self, stage: &Stage<'_>, block: Block) -> Block {
        let rows = block.batch.num_rows();
        let added = self.added_rows(stage.plan.identity().seed(), stage.block, rows);
        if added == 0 {
            return block;
        }
        let mut sources = stage.stream(self.sources_key);
        let mut homes = block.homes();
        let with_cells = block.cells.is_some();
        let partner = match self.scope {
            DuplicateScope::WithinFile => None,
            DuplicateScope::AcrossFiles => {
                let partner = self.partner(stage.block);
                Some(match partner == stage.block {
                    true => block.clone(),
                    false => stage.other_block(partner, with_cells),
                })
            }
        };
        let (source_rows, copy_homes): (Vec<u32>, Vec<u32>) = match &partner {
            None => (0..added)
                .map(|_| {
                    let source = below(&mut sources, rows as u32);
                    (source, homes[source as usize])
                })
                .unzip(),
            Some(_) => {
                let reach = self.generated_rows(stage.block).min(self.generated_rows(self.partner(stage.block)));
                (0..added)
                    .map(|_| {
                        let offset = below(&mut sources, reach as u32);
                        (offset, offset)
                    })
                    .unzip()
            }
        };
        let source = partner.as_ref().unwrap_or(&block);
        let mut copies = take_record_batch(&source.batch, &arrow_array::UInt32Array::from(source_rows.clone()))
            .expect("a source row lies inside its block");
        let mut copy_cells: Option<Vec<Vec<TextCell>>> = source.cells.as_ref().map(|cells| {
            cells
                .iter()
                .map(|column| array_edits::cells_of_rows(column, &source_rows, rows as u32))
                .collect()
        });
        if let Some(varied) = &self.varied {
            let mut draws = stage.stream(self.varied_key);
            let mut columns = copies.columns().to_vec();
            for copy in 0..added {
                let column = varied[below(&mut draws, varied.len() as u32) as usize];
                let donor = below(&mut draws, rows as u32);
                let value = array_edits::take_rows(block.batch.column(column), &[donor]);
                columns[column] = array_edits::replace_rows(&columns[column], &[copy as u32], &value);
                if let (Some(copy_cells), Some(cells)) = (copy_cells.as_mut(), block.cells.as_ref()) {
                    let row = (rows + copy) as u32;
                    copy_cells[column].retain(|cell| cell.row != row);
                    copy_cells[column].extend(array_edits::cells_of_rows(&cells[column], &[donor], row));
                    copy_cells[column].sort_by_key(|cell| cell.row);
                }
            }
            copies = RecordBatch::try_new(copies.schema(), columns).expect("a varied column keeps its type");
        }
        let batch = concat_batches(stage.schema, [&block.batch, &copies]).expect("copies carry the block's schema");
        let cells = block.cells.map(|mut cells| {
            for (column, copied) in cells.iter_mut().zip(copy_cells.into_iter().flatten()) {
                column.extend(copied);
            }
            cells
        });
        homes.extend(copy_homes);
        Block {
            batch,
            cells,
            homes: Some(homes),
        }
    }
}
