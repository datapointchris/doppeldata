//! doppeldata's generation engine. It knows nothing of Python, and leaves where
//! output goes to its caller.

pub mod format_settings;
pub mod generalized_pattern;
mod identity;
pub mod manifest;
mod physical_type;
mod plan;
pub mod profile;
pub mod published_schemas;
mod rng;
mod samplers;

pub use identity::DatasetIdentity;
pub use plan::BlockOutOfRange;
pub use plan::ColumnFault;
pub use plan::Plan;
pub use plan::PlanError;
pub use plan::plan;
pub use rng::BLOCK_ROWS;
pub use rng::STREAM_VERSION;
pub use samplers::TextCell;
pub use samplers::file_rows_draws;
pub use samplers::shape_of;

/// The engine's release, which is the tag the wheel was built from.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
