//! Module for reading `/metrics` on node exporter.
//!
mod structs;
mod node_exporter;

pub use structs::*;
pub use node_exporter::*;
