//! Module for reading `/api/v1/is-leader` on the masters.
//!
//! The `/api/v1/is-leader` endpoint reports if the master is the leader.
//! This returns: 
//! - `{"STATUS":"OK"}` on the leader.
//! - *Nothing* on the followers.
//!
//! The `/api/v1/is-leader` endpoint is only available on the masters, default port 7000.
//! 
//! The isleader functionality is a helper module, it is not used directly.
//! 
//! The isleader functionality is called from:
//! - [crate::snapshot::perform_snapshot] -> [crate::isleader::AllIsLeader::perform_snapshot]
//! ---
//! - [crate::clocks::AllClocks::print] -> [crate::isleader::AllIsLeader::return_leader_snapshot]
//! - [crate::clocks::AllClocks::print_latency] -> [crate::isleader::AllIsLeader::return_leader_snapshot]
//! - [crate::cluster_config::AllSysClusterConfigEntryPB::print] -> [crate::isleader::AllIsLeader::return_leader_snapshot]
//! - [crate::entities::SnapshotDiffBTreeMapsEntities::snapshot_diff] -> [crate::isleader::AllIsLeader::return_leader_snapshot]
//! - [crate::entities::AllStoredEntities::print] -> [crate::isleader::AllIsLeader::return_leader_snapshot]
//! - [crate::masters::MastersDiff::snapshot_diff] -> [crate::isleader::AllIsLeader::return_leader_snapshot]
//! - [crate::masters::MastersDiff::print] -> [crate::isleader::AllIsLeader::return_leader_snapshot]
//! - [crate::tservers::AllStoredTabletServers::print] -> [crate::isleader::AllIsLeader::return_leader_snapshot]
//! - [crate::tservers::SnapshotDiffBTreeMapsTabletServers::snapshot_diff] -> [crate::isleader::AllIsLeader::return_leader_snapshot]
//! ---
//! - [crate::clocks::AllClocks::print_adhoc] -> [crate::isleader::AllIsLeader::return_leader_http]
//! - [crate::clocks::AllClocks::print_adhoc_latency] -> [crate::isleader::AllIsLeader::return_leader_http]
//! - [crate::cluster_config::AllSysClusterConfigEntryPB::print_adhoc] -> [crate::isleader::AllIsLeader::return_leader_http]
//! - [crate::entities::SnapshotDiffBTreeMapsEntities::adhoc_read_first_snapshot] -> [crate::isleader::AllIsLeader::return_leader_http]
//! - [crate::entities::SnapshotDiffBTreeMapsEntities::adhoc_read_second_snapshot] -> [crate::isleader::AllIsLeader::return_leader_http]
//! - [crate::entities::AllStoredEntities::print_adhoc] -> [crate::isleader::AllIsLeader::return_leader_http]
//! - [crate::masters::Masters::print_adhoc] -> [crate::isleader::AllIsLeader::return_leader_http]
//! - [crate::masters::MastersDiff::adhoc_read_first_snapshot] -> [crate::isleader::AllIsLeader::return_leader_http]
//! - [crate::masters::MastersDiff::adhoc_read_second_snapshot] -> [crate::isleader::AllIsLeader::return_leader_http]
//! - [crate::tservers::AllStoredTabletServers::print_adhoc] -> [crate::isleader::AllIsLeader::return_leader_http]
//! - [crate::tservers::SnapshotDiffBTreeMapsTabletServers::adhoc_read_first_snapshot] -> [crate::isleader::AllIsLeader::return_leader_http]
//! - [crate::tservers::SnapshotDiffBTreeMapsTabletServers::adhoc_read_second_snapshot] -> [crate::isleader::AllIsLeader::return_leader_http]
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;
