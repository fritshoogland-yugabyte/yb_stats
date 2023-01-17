//! The structs
//!
use chrono::{DateTime, Local};
use std::collections::BTreeMap;

/// The root struct for deserializing `/api/v1/tablet-servers`
///
/// This struct is the begin struct needed to parse the results from master:port/api/v1/tablet-servers:
/// ```text
/// {
///   "": {
///     "yb-3.local:9000": {
///       "time_since_hb": "0.5s",
///       "time_since_hb_sec": 0.536952137,
///       "status": "ALIVE",
///       "uptime_seconds": 45,
///       "ram_used": "26.21 MB",
///       "ram_used_bytes": 26214400,
///       "num_sst_files": 0,
///       "total_sst_file_size": "0 B",
///       "total_sst_file_size_bytes": 0,
///       "uncompressed_sst_file_size": "0 B",
///       "uncompressed_sst_file_size_bytes": 0,
///       "path_metrics": [
///         {
///           "path": "/mnt/d0",
///           "space_used": 334278656,
///           "total_space_size": 10724835328
///         }
///       ],
///       "read_ops_per_sec": 0,
///       "write_ops_per_sec": 0,
///       "user_tablets_total": 1,
///       "user_tablets_leaders": 0,
///       "system_tablets_total": 12,
///       "system_tablets_leaders": 0,
///       "active_tablets": 13,
///       "cloud": "local",
///       "region": "local",
///       "zone": "local3"
///     }
/// }
/// ```
/// Mind the empty object that contains a map of the server objects (not a list!).
///
/// Issue: The master /api/v1/tablet-servers endpoint might display incorrect/incomplete data #15655: <https://github.com/yugabyte/yugabyte-db/issues/15655>
///
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct TabletServers {
    // to make the structure more logical, rename the empty name to "tabletservers".
    // String = tablet server name
    #[serde(rename = "")]
    pub tabletservers: BTreeMap<String, TabletServer>,
}
/// The main struct holding the tablet server information
///
/// This seems to have the majority of the information from `src/yb/master/master_types.proto`
/// But have additional fields?
#[derive(Serialize, Deserialize, Debug)]
pub struct TabletServer {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the timestamp.
    pub timestamp: Option<DateTime<Local>>,
    /// another added field to make this struct function in the deserializing using [TabletServers]
    /// and using it in the struct [AllTabletServers].
    pub tablet_server_hostname_port: Option<String>,
    pub time_since_hb: String,
    pub time_since_hb_sec: f32,
    pub status: String,
    // ---TServerMetricsPB---
    // uptime_seconds uint64
    pub uptime_seconds: u64,
    // total_ram_usage int64
    pub ram_used: String,
    pub ram_used_bytes: i64,
    // num_sst_files uint64
    pub num_sst_files: u64,
    // total_sst_file_size int64
    pub total_sst_file_size: String,
    pub total_sst_file_size_bytes: i64,
    // uncompressed_sst_file_size int64
    pub uncompressed_sst_file_size: String,
    pub uncompressed_sst_file_size_bytes: i64,
    pub path_metrics: Vec<PathMetrics>,
    // read_ops_per_sec double
    pub read_ops_per_sec: f64,
    // write_ops_per_sec double
    pub write_ops_per_sec: f64,
    // never seen this one
    pub disable_tablet_split_if_default_ttl: Option<bool>,
    // ---TServerMetricsPB---
    pub user_tablets_total: i32,
    pub user_tablets_leaders: i32,
    pub system_tablets_total: i32,
    pub system_tablets_leaders: i32,
    pub active_tablets: i32,
    pub cloud: String,
    pub region: String,
    pub zone: String,
}
/// source: `src/yb/master/master_types.proto`
///
/// (not a PB, part of TServerMetricsPB)
#[derive(Serialize, Deserialize, Debug)]
pub struct PathMetrics {
    pub path: String,
    pub space_used: u64,
    pub total_space_size: u64,
}
/// This struct is used by yb_stats for saving and loading the tablet server data.
///
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AllTabletServers {
    // to make the structure more logical, rename the empty name to "tabletservers".
    #[serde(rename = "")]
    pub tabletservers: Vec<TabletServer>,
}

// diff
/// BTreeMap for storing a master diff struct per `tablet_server_hostname_port`
///
/// [TabletServer].tablet_server_hostname_port
type BTreeTabletServersDiff = BTreeMap<String, TabletServersDiffFields>;
/// The wrapper struct for holding the btreemap holding the diff structs.
#[derive(Debug, Default)]
pub struct TabletServersDiff {
    pub btreetabletserversdiff: BTreeTabletServersDiff,
    pub master_found: bool,
}
/// The tablet servers diff struct.
///
/// This performs a very simple way of diffing:
/// For every field that makes sense to see the difference, create a first and a second (snapshot) field.
///
/// What is severely missing is the sequence_id field, to see an actual restart.
/// The code and protobuf definitions say this is available for the tablet sever
/// (and is what is used for the determination of master restarts).
#[derive(Debug, Default)]
pub struct TabletServersDiffFields {
    pub first_status: String,
    pub first_uptime_seconds: u64,
    pub second_status: String,
    pub second_uptime_seconds: u64,
}