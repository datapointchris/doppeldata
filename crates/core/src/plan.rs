//! A profile resolved into per-column samplers, and the pure function from a block number to that
//! block's rows.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_schema::DataType;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;

use crate::identity::DatasetIdentity;
use crate::identity::is_sha256_digest;
use crate::profile::DEFAULT_K_MIN;
use crate::profile::FORMAT_VERSION;
use crate::profile::Profile;
use crate::profile::provenance;
use crate::rng::BLOCK_ROWS;
use crate::samplers::BlockPosition;
use crate::samplers::ColumnPlan;
use crate::samplers::ColumnSite;
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
    /// A column the engine cannot generate. `field` is an RFC 6901 JSON Pointer into the profile,
    /// as the library's own issues carry.
    #[error("{field}: {message} [{code}]")]
    Column { code: ColumnFault, field: String, message: String },
}

/// Why a column cannot be generated, as a code a caller can branch on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnFault {
    /// `physical_type` names an Arrow type the engine does not write.
    UnsupportedType,
    /// The column holds a statistic the engine does not generate from yet.
    NotGeneratedYet,
    /// A fit names a family the engine inverts, with a parameter outside its domain.
    InvalidFit,
    /// A pattern that does not parse, or that no string matches.
    InvalidPattern,
    /// Quantiles or histogram edges that fall, or a quantile name with no level.
    InvalidQuantiles,
    /// Rates no alias table can draw from, such as all zero.
    UnusableRates,
    /// A temporal extreme that does not parse as the column's type.
    InvalidExtreme,
    StructWithoutFields,
    /// `missing_tokens` whose rates, each a share of rows, sum above the column's null rate.
    TokensExceedNullRate,
}

impl ColumnFault {
    /// The kebab-case code, in the form of the library's issue codes.
    pub fn code(self) -> &'static str {
        match self {
            Self::UnsupportedType => "unsupported-type",
            Self::NotGeneratedYet => "not-generated-yet",
            Self::InvalidFit => "invalid-fit",
            Self::InvalidPattern => "invalid-pattern",
            Self::InvalidQuantiles => "invalid-quantiles",
            Self::UnusableRates => "unusable-rates",
            Self::InvalidExtreme => "invalid-extreme",
            Self::StructWithoutFields => "struct-without-fields",
            Self::TokensExceedNullRate => "tokens-exceed-null-rate",
        }
    }
}

impl PlanError {
    /// The kebab-case code a caller branches on, in the form of the library's issue codes.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Bundle { .. } => "bundle",
            Self::NoColumns { .. } => "no-columns",
            Self::DuplicateColumn { .. } => "duplicate-column",
            Self::ProfileDigest { .. } => "invalid-profile-digest",
            Self::Column { code, .. } => code.code(),
        }
    }

    /// The RFC 6901 JSON Pointer into the profile that the refusal names.
    pub fn field(&self) -> &str {
        match self {
            Self::Bundle { .. } => "/kind",
            Self::NoColumns { .. } | Self::DuplicateColumn { .. } => "/columns",
            Self::ProfileDigest { .. } => "",
            Self::Column { field, .. } => field,
        }
    }
}

impl std::fmt::Display for ColumnFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

/// How one top-level column is drawn, as a plan reports it before generating anything.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSampler {
    pub name: String,
    pub data_type: DataType,
    /// What draws the values, such as `sequence from 1 by 1` or `50 top values`.
    pub sampler: String,
    /// The share of rows drawn null.
    pub null_share: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("block {block} is past the plan's {block_count} blocks")]
pub struct BlockOutOfRange {
    pub block: u64,
    pub block_count: u64,
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
    let source_rows = table.dataset.as_ref().and_then(|dataset| dataset.row_count);
    let rows_read = table
        .generated
        .as_ref()
        .and_then(|g| g.sample.as_ref())
        .and_then(|sample| sample.rows_read);
    let context = PlanContext {
        table: &table.name,
        seed,
        rows: rows as f64,
        source_rows: source_rows.map(|rows| rows as f64),
        described_rows: rows_read.or(source_rows).map(|rows| rows as f64),
        k_min: table.privacy.as_ref().and_then(|privacy| privacy.k_min).unwrap_or(DEFAULT_K_MIN),
    };
    let mut planned = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        if !seen.insert(column.name.as_str()) {
            return Err(PlanError::DuplicateColumn {
                column: column.name.clone(),
            });
        }
        planned.push(column_plan(&context, column, &ColumnSite::top_level(index, column))?);
    }
    let identity = DatasetIdentity::new(profile_digest, seed, rows);
    let metadata = HashMap::from([
        (provenance::PROFILE_NAME.to_string(), table.name.clone()),
        (provenance::PROFILE_VERSION.to_string(), FORMAT_VERSION.to_string()),
        (provenance::PROFILE_DIGEST.to_string(), profile_digest.to_string()),
        (provenance::SEED.to_string(), seed.to_string()),
        (provenance::STREAM_VERSION.to_string(), identity.stream_version().to_string()),
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

    /// How each top-level column is drawn, in schema order.
    pub fn column_samplers(&self) -> Vec<ColumnSampler> {
        self.columns
            .iter()
            .map(|column| ColumnSampler {
                name: column.field.name().clone(),
                data_type: column.field.data_type().clone(),
                sampler: column.sampler.clone(),
                null_share: column.null_share,
            })
            .collect()
    }

    fn position(&self, block: u64) -> Result<(BlockPosition, usize), BlockOutOfRange> {
        if block >= self.block_count {
            return Err(BlockOutOfRange {
                block,
                block_count: self.block_count,
            });
        }
        let first_row = block * BLOCK_ROWS;
        let rows = (self.identity.rows() - first_row).min(BLOCK_ROWS) as usize;
        let position = BlockPosition {
            seed: self.identity.seed(),
            block,
            first_row,
        };
        Ok((position, rows))
    }

    /// The rows of `block`, a pure function of the plan and the block number.
    pub fn generate_block(&self, block: u64) -> Result<RecordBatch, BlockOutOfRange> {
        let (position, rows) = self.position(block)?;
        let arrays = self.columns.iter().map(|column| column.generate(&position, rows)).collect();
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        Ok(RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options).expect("every column's array matches its field"))
    }

    /// The cells of `block` only a text format can carry, one list per column in schema order,
    /// each in row order. The batch [`Plan::generate_block`] returns holds a null at each.
    pub fn text_cells(&self, block: u64) -> Result<Vec<Vec<TextCell>>, BlockOutOfRange> {
        let (position, rows) = self.position(block)?;
        Ok(self.columns.iter().map(|column| column.text_cells(&position, rows)).collect())
    }
}
