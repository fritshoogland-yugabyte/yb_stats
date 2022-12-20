//! Module for reading `/varz` on the masters and tablet servers (deprecated: use the vars module).
//!
mod structs;
mod gflags;

pub use structs::*;
pub use gflags::*;
