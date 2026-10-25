//! A profile resolved into per-column samplers, and the pure function from a block number to that
//! block's rows.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;

use crate::identity::DatasetIdentity;
use crate::identity::is_sha256_digest;
use crate::profile::FORMAT_VERSION;
use crate::profile::Profile;
use crate::profile::provenance;
use crate::rng::BLOCK_ROWS;
use crate::samplers::BlockPosition;
use crate::samplers::ColumnPlan;
use crate::samplers::PlanContext;
use crate::samplers::TextCell;
use crate::samplers::column_plan;

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("profile {name:?} is a bundle; plan one table at a time")]
    Bundle { name: String },
    #[error("profile {name:?} has no columns to generate")]
    NoColumns { name: String },
    #[error("column {column:?} appears twice, and both would draw the same stream")]
    DuplicateColumn { column: String },
    #[error("profile digest {digest:?} is not `sha256:` and 64 lowercase hex digits")]
    ProfileDigest { digest: String },
    #[error("column {column:?}: {reason}")]
    Column { column: String, reason: String },
}

/// A profile resolved for generation. `generate_block` takes `&self` and the plan is `Send + Sync`,
/// so any thread can produce any block.
#[derive(Debug)]
pub struct Plan {
    identity: DatasetIdentity,
    schema: SchemaRef,
    columns: Vec<ColumnPlan>,
    block_count: u64,
}

const _: () = {
    const fn shared_across_threads<T: Send + Sync>() {}
    shared_across_threads::<Plan>();
};

/// Resolves a table profile into a plan for `rows` rows from `seed`. The library computes the
/// profile's digest, `sha256:` and the hex SHA-256 of its RFC 8785 canonical JSON.
pub fn plan(profile: &Profile, profile_digest: &str, seed: u64, rows: u64) -> Result<Plan, PlanError> {
    let table = match profile {
        Profile::Table(table) => table,
        Profile::Bundle(bundle) => {
            return Err(PlanError::Bundle { name: bundle.name.clone() });
        }
    };
    if !is_sha256_digest(profile_digest) {
        return Err(PlanError::ProfileDigest {
            digest: profile_digest.into(),
        });
    }
    let columns = table.columns.as_deref().unwrap_or_default();
    if columns.is_empty() {
        return Err(PlanError::NoColumns { name: table.name.clone() });
    }
    let mut seen = BTreeSet::new();
    let context = PlanContext { table: &table.name, seed };
    let mut planned = Vec::with_capacity(columns.len());
    for column in columns {
        if !seen.insert(column.name.as_str()) {
            return Err(PlanError::DuplicateColumn {
                column: column.name.clone(),
            });
        }
        let path = column.path.as_deref().unwrap_or(&column.name);
        let plan = column_plan(&context, column, path).map_err(|reason| PlanError::Column {
            column: column.name.clone(),
            reason,
        })?;
        planned.push(plan);
    }
    let identity = DatasetIdentity::new(profile_digest, seed, rows);
    let metadata = HashMap::from([
        (provenance::PROFILE_NAME.to_string(), table.name.clone()),
        (provenance::PROFILE_VERSION.to_string(), FORMAT_VERSION.to_string()),
        (provenance::PROFILE_DIGEST.to_string(), identity.profile_digest.clone()),
        (provenance::SEED.to_string(), seed.to_string()),
        (provenance::STREAM_VERSION.to_string(), identity.stream_version.to_string()),
        (provenance::DATASET_ROWS.to_string(), rows.to_string()),
        (provenance::DATASET_DIGEST.to_string(), identity.digest()),
    ]);
    let fields: Vec<_> = planned.iter().map(|column| column.field.clone()).collect();
    Ok(Plan {
        identity,
        schema: Arc::new(Schema::new_with_metadata(fields, metadata)),
        columns: planned,
        block_count: rows.div_ceil(BLOCK_ROWS),
    })
}

impl Plan {
    /// The Arrow schema every block's batch carries, with the provenance metadata.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn identity(&self) -> &DatasetIdentity {
        &self.identity
    }

    pub fn block_count(&self) -> u64 {
        self.block_count
    }

    fn position(&self, block: u64) -> (BlockPosition, usize) {
        assert!(
            block < self.block_count,
            "block {block} is past the plan's {} blocks",
            self.block_count
        );
        let first_row = block * BLOCK_ROWS;
        let rows = (self.identity.rows - first_row).min(BLOCK_ROWS) as usize;
        let position = BlockPosition {
            seed: self.identity.seed,
            block,
            first_row,
        };
        (position, rows)
    }

    /// The rows of `block`, a pure function of the plan and the block number.
    ///
    /// # Panics
    ///
    /// When `block` is not below [`Plan::block_count`].
    pub fn generate_block(&self, block: u64) -> RecordBatch {
        let (position, rows) = self.position(block);
        let arrays = self.columns.iter().map(|column| column.generate(&position, rows)).collect();
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options).expect("every column's array matches its field")
    }

    /// The cells of `block` only a text format can carry, one list per column in schema order,
    /// each in row order. The batch [`Plan::generate_block`] returns holds a null at each.
    ///
    /// # Panics
    ///
    /// When `block` is not below [`Plan::block_count`].
    pub fn text_cells(&self, block: u64) -> Vec<Vec<TextCell>> {
        let (position, rows) = self.position(block);
        self.columns.iter().map(|column| column.text_cells(&position, rows)).collect()
    }
}
