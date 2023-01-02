//! Module for reading `/api/v1/cluster-config` on the masters.
//!
//! The `/api/v1/cluster-config` endpoint contains current cluster configuration, including:
//! ([SysClusterConfigEntryPB])
//! - Cluster UUID, which is always set.
//! - Replication info (Optional, [ReplicationInfoPB]).
//! - Server blacklist (Optional, tablet servers, [BlacklistPB]).
//! - Encryption into (Optional, [EncryptionInfoPB]).
//! - Consumer registry (Optional, xcluster, [ConsumerRegistryPB]).
//! - Leader blacklist (Optional, masters, [BlacklistPB]).
//!
//! The `/api/v1/cluster-config` endpoint is only available on the masters, default port 7000.
//! It is available on all the masters, the leader as well as the followers.
//!
//! The cluster-config functionality is called from:
//! - [crate::snapshot::perform_snapshot] -> [crate::cluster_config::AllSysClusterConfigEntryPB::perform_snapshot] (general snapshot, saves cluster-config data)
//! - [crate] -> [print_cluster_config] (prints adhoc (live) or snapshot cluster-config info)
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;