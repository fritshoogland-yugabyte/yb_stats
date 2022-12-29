//! Module for reading `/varz` on the masters and tablet servers (deprecated: use the vars module).
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;
