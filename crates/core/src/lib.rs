//! doppeldata's generation engine. It knows nothing of Python, and leaves where
//! output goes to its caller.

pub mod identity;
mod physical_type;
pub mod plan;
pub mod profile;
pub mod published_schemas;
pub mod rng;
mod samplers;

pub use identity::DatasetIdentity;
pub use plan::Plan;
pub use plan::PlanError;
pub use plan::plan;
pub use samplers::TextCell;

/// The engine's release, which is the tag the wheel was built from.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
