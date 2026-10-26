//! doppeldata's generation engine. It knows nothing of Python, and leaves where
//! output goes to its caller.

pub mod format_settings;
pub mod generalized_pattern;
mod identity;
pub mod manifest;
mod perturbations;
mod physical_type;
mod plan;
pub mod profile;
pub mod profile_comparison;
pub mod published_schemas;
mod rng;
mod samplers;
pub mod scenario;
mod values_digest;

pub use identity::DatasetIdentity;
pub use perturbations::CdcPlan;
pub use perturbations::CdcStream;
pub use perturbations::FileFault;
pub use perturbations::LineEnd;
pub use perturbations::MalformedPlan;
pub use perturbations::RowFault;
pub use perturbations::RowFaults;
pub use perturbations::ScenarioFault;
pub use perturbations::SchemaDriftPlan;
pub use perturbations::StreamFaultRecord;
pub use physical_type::physical_type_name;
pub use plan::AppliedScenario;
pub use plan::BlockOutOfRange;
pub use plan::ColumnFault;
pub use plan::ColumnSampler;
pub use plan::PlacedBlock;
pub use plan::Plan;
pub use plan::PlanError;
pub use plan::plan;
pub use plan::plan_with_scenario;
pub use rng::BLOCK_ROWS;
pub use rng::STREAM_VERSION;
pub use samplers::FITTED_FAMILIES;
pub use samplers::FittedFamilyKind;
pub use samplers::TextCell;
pub use samplers::check_digit_holds;
pub use samplers::file_rows_draws;
pub use samplers::pattern_problem;
pub use samplers::shape_of;
pub use values_digest::stream_values_digest;
pub use values_digest::values_digest;

/// The engine's release, which is the tag the wheel was built from.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
