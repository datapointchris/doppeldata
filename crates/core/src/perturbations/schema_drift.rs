//! Files whose first row lies at or past `after_fraction` of the rows carry a new shape: columns
//! dropped, renamed, widened, added and moved to the front, in that order. The writers apply it to
//! each block a drifted file takes rows from, after every op that writes rows.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_cast::can_cast_types;
use arrow_cast::cast;
use arrow_schema::DataType;
use arrow_schema::FieldRef;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;

use super::Block;
use super::Planned;
use super::Planning;
use super::ScenarioFault;
use super::pointer_token;
use crate::physical_type;
use crate::physical_type::PhysicalType;
use crate::physical_type::physical_type_name;
use crate::plan::PlanError;
use crate::profile::Column;
use crate::rng::BLOCK_ROWS;
use crate::samplers::BlockPosition;
use crate::samplers::ColumnPlan;
use crate::samplers::ColumnSite;
use crate::samplers::column_plan;
use crate::scenario::SchemaDrift;

/// Where one column of a drifted file comes from.
#[derive(Debug, Clone)]
enum DriftedColumn {
    /// A column the rows carry, cast where it was widened.
    Kept { source: usize, cast_to: Option<DataType> },
    /// A column generated from its type alone.
    Added(usize),
}

/// A schema drift planned against the rows every other op writes.
#[derive(Debug)]
pub struct SchemaDriftPlan {
    index: usize,
    first_row: u64,
    columns: Vec<DriftedColumn>,
    added: Vec<ColumnPlan>,
    schema: SchemaRef,
    /// Each column the drift drops or renames, with the pointer to the entry that does it.
    moved: Vec<(String, String)>,
}

impl SchemaDriftPlan {
    /// The schema a drifted file carries.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// The first dataset row a file must start at or past to carry the drifted schema.
    pub fn first_row(&self) -> u64 {
        self.first_row
    }

    /// The pointer to the scenario entry that drops or renames `column`, where one does.
    pub fn moves(&self, column: &str) -> Option<String> {
        self.moved
            .iter()
            .find(|(name, _)| name == column)
            .map(|(_, field)| format!("/perturbations/{}{field}", self.index))
    }

    /// `block`'s rows in the drifted shape.
    pub(crate) fn apply(&self, seed: u64, block: u64, rows: Block) -> Block {
        let count = rows.batch.num_rows();
        let position = BlockPosition {
            seed,
            block,
            first_row: block * BLOCK_ROWS,
        };
        let arrays: Vec<ArrayRef> = self
            .columns
            .iter()
            .map(|column| match column {
                DriftedColumn::Kept { source, cast_to: None } => rows.batch.column(*source).clone(),
                DriftedColumn::Kept {
                    source,
                    cast_to: Some(data_type),
                } => cast(rows.batch.column(*source), data_type).expect("planning checked the cast"),
                DriftedColumn::Added(added) => self.added[*added].generate(&position, count),
            })
            .collect();
        let options = RecordBatchOptions::new().with_row_count(Some(count));
        let batch = RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options).expect("a drifted column matches its field");
        let cells = rows.cells.map(|cells| {
            self.columns
                .iter()
                .map(|column| match column {
                    DriftedColumn::Kept { source, .. } => cells[*source].clone(),
                    DriftedColumn::Added(_) => Vec::new(),
                })
                .collect()
        });
        Block {
            batch,
            cells,
            homes: rows.homes,
        }
    }
}

/// A Arrow scalar type from a type string, or why it is not one.
fn scalar_type(text: &str) -> Result<DataType, String> {
    match physical_type::parse(text) {
        Some(PhysicalType::Scalar(data_type)) => Ok(data_type),
        Some(PhysicalType::Container(_)) => Err(format!("{text:?} is a container; a drifted column takes a scalar type")),
        None => Err(format!("{text:?} is not an Arrow type the engine writes")),
    }
}

pub(super) fn plan(op: &SchemaDrift, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    if op.add.is_empty() && op.drop.is_empty() && op.rename.is_empty() && op.widen.is_empty() && op.reorder.is_empty() {
        return Err(planning.refuse(ScenarioFault::InvalidParameter, "", "schema_drift names no change to make"));
    }
    let unknown =
        |field: String, name: &str| planning.refuse(ScenarioFault::UnknownColumn, &field, format!("the rows hold no column {name:?}"));
    let invalid = |field: String, message: String| planning.refuse(ScenarioFault::InvalidParameter, &field, message);
    let fields = planning.schema.fields();
    let mut kept: Vec<Option<(FieldRef, DriftedColumn)>> = fields
        .iter()
        .enumerate()
        .map(|(source, field)| Some((field.clone(), DriftedColumn::Kept { source, cast_to: None })))
        .collect();
    let mut moved: Vec<(String, String)> = Vec::new();
    for (k, name) in op.drop.iter().enumerate() {
        let index = planning.schema.index_of(name).map_err(|_| unknown(format!("/drop/{k}"), name))?;
        kept[index] = None;
        moved.push((name.clone(), format!("/drop/{k}")));
    }
    let present = |name: &str, field: String, kept: &[Option<(FieldRef, DriftedColumn)>]| match planning.schema.index_of(name) {
        Ok(index) if kept[index].is_some() => Ok(index),
        _ => Err(unknown(field, name)),
    };
    for (column, to) in &op.widen {
        let field = format!("/widen/{}", pointer_token(column));
        let index = present(column, field.clone(), &kept)?;
        let data_type = scalar_type(to).map_err(|message| invalid(field.clone(), message))?;
        let (old, _) = kept[index].as_ref().expect("present checked it");
        if !can_cast_types(old.data_type(), &data_type) {
            return Err(invalid(
                field,
                format!("{column:?} is {}, which does not cast to {to}", physical_type_name(old.data_type())),
            ));
        }
        let widened = Arc::new(old.as_ref().clone().with_data_type(data_type.clone()));
        kept[index] = Some((
            widened,
            DriftedColumn::Kept {
                source: index,
                cast_to: Some(data_type),
            },
        ));
    }
    for (from, to) in &op.rename {
        let field = format!("/rename/{}", pointer_token(from));
        let index = present(from, field.clone(), &kept)?;
        let (old, column) = kept[index].take().expect("present checked it");
        kept[index] = Some((Arc::new(old.as_ref().clone().with_name(to)), column));
        moved.push((from.clone(), field));
    }
    let mut drifted: Vec<(FieldRef, DriftedColumn)> = kept.into_iter().flatten().collect();
    let table = planning.sampler_table();
    let context = planning.sampler_context(&table);
    let mut added: Vec<ColumnPlan> = Vec::new();
    for (k, column) in op.add.iter().enumerate() {
        scalar_type(&column.physical_type).map_err(|message| invalid(format!("/add/{k}/type"), message))?;
        let profiled: Column = serde_json::from_value(serde_json::json!({"name": column.name, "physical_type": column.physical_type}))
            .expect("a name and a type make a column");
        let sampler = column_plan(&context, &profiled, &ColumnSite::top_level(k, &profiled))
            .map_err(|error| invalid(format!("/add/{k}/type"), error.to_string()))?;
        drifted.push((sampler.field.clone(), DriftedColumn::Added(added.len())));
        added.push(sampler);
    }
    let mut names: Vec<&str> = Vec::new();
    for (field, _) in &drifted {
        if names.contains(&field.name().as_str()) {
            return Err(invalid(
                String::new(),
                format!("drifted files would hold two columns named {:?}", field.name()),
            ));
        }
        names.push(field.name());
    }
    let mut front: Vec<(FieldRef, DriftedColumn)> = Vec::new();
    for (k, name) in op.reorder.iter().enumerate() {
        let position = drifted
            .iter()
            .position(|(field, _)| field.name() == name)
            .ok_or_else(|| unknown(format!("/reorder/{k}"), name))?;
        front.push(drifted.remove(position));
    }
    front.extend(drifted);
    let (fields, columns): (Vec<FieldRef>, Vec<DriftedColumn>) = front.into_iter().unzip();
    let schema = Arc::new(Schema::new_with_metadata(fields, planning.schema.metadata().clone()));
    Ok(Planned::SchemaDrift(SchemaDriftPlan {
        index: planning.index,
        first_row: (op.after_fraction.0 * planning.rows as f64).ceil() as u64,
        columns,
        added,
        schema,
        moved,
    }))
}
