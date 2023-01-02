//! Module for reading `/dump-entities` on the masters.
//!
//! The `/dump-entities` endpoint contains the current known:
//! - Keyspaces / databases
//! - Tables
//! - Tablets
//! - Replicas
//!
//! The `/dump-entities` endpoint is only available on the masters, default port 7000.
//! It is available on all the masters, the leader as well as the followers.
//!
/*
//! The entities functionality is called from:
//! - [crate::snapshot::perform_snapshot] -> [crate::entities::Masters::perform_snapshot] (general snapshot, saves masters data)
//! - [crate::snapshot::snapshot_diff] -> [crate::entities::MastersDiff::snapshot_diff] (general diff, show masters diff based on snapshot data)
//! - [crate] -> [print_entities] (prints adhoc (live) or snapshot masters info)
//! - [crate] -> [entity_diff] (prints entities diff only, based on snapshot data)
//! - [crate::snapshot::adhoc_diff] -> [crate::entities::MastersDiff::adhoc_read_first_snapshot],
//! [crate::masters::MastersDiff::adhoc_read_second_snapshot],
//! [crate::masters::MastersDiff::print] (prints masters diff, based on live data)
//!

 */
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;