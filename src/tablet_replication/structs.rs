//! The structs
//!
use chrono::{DateTime, Local};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TabletReplication {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the snapshot timestamp.
    pub timestamp: Option<DateTime<Local>>,
    pub leaderless_tablets: Vec<Option<LeaderlessTablet>>,
    pub under_replicated_tablets: Vec<Option<UnderReplicatedTablets>>,
}

#[derive(Debug, Default)]
pub struct AllTabletReplication {
    pub tablet_replication: Vec<TabletReplication>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct LeaderlessTablet {
    pub table_name: String,
    pub table_uuid: String,
    pub tablet_id: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct UnderReplicatedTablets {
    pub table_name: String,
    pub table_uuid: String,
    pub tablet_id: String,
    pub tablet_replication_count: String,
}
