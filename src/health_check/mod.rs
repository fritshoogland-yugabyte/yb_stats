//! Module for reading `/api/v1/health-check` on the masters.
//!
//! The `/api/v1/health-check` endpoint contains 3 points of information:
//! - dead_nodes: a list of tablet server UUIDs that are considered 'DEAD'.
//! - most_recent_uptime: ?
//! - under_replicated_tablets: a list of tablet UUIDs that are under replicated, which means the
//!   number of replicas is less than the set replication factor.
//!
//! The `/api/v1/health-check` endpoint is only available on the masters, default port 7000.
//! It is available on all the masters, the leader as well as the followers.
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;