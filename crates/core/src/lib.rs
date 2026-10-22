//! doppeldata's generation engine. It knows nothing of Python, and leaves where
//! output goes to its caller.

pub mod profile;

/// The engine's release, which is the tag the wheel was built from.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
