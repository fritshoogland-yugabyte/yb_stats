//! Module for reading `/api/v1/version` from the masters and tablet servers.
//!
mod structs;
mod versions;

pub use structs::*;
pub use versions::*;