//! Module for reading `/api/v1/cluster-config` on the masters.
//!
mod structs;
mod cluster_config;

pub use structs::*;
pub use cluster_config::*;