//! Module for reading `/api/v1/varz` from the masters and tablet servers.
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;