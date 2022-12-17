use chrono::{DateTime, Local};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredTabletServers {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub tserver_hostname_port: String,
    pub time_since_hb: String,
    pub time_since_hb_sec: f32,
    pub status: String,
    pub uptime_seconds: i64,
    pub ram_used: String,
    pub ram_used_bytes: i64,
    pub num_sst_files: i32,
    pub total_sst_file_size: String,
    pub total_sst_file_size_bytes: i32,
    pub uncompressed_sst_file_size: String,
    pub uncompressed_sst_file_size_bytes: i32,
    pub read_ops_per_sec: f32,
    pub write_ops_per_sec: f32,
    pub user_tablets_total: i32,
    pub user_tablets_leaders: i32,
    pub system_tablets_total: i32,
    pub system_tablets_leaders: i32,
    pub active_tablets: i32,
    pub cloud: String,
    pub region: String,
    pub zone: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredPathMetrics {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub tserver_hostname_port: String,
    pub path: String,
    pub space_used: i64,
    pub total_space_size: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AllTabletServers {
    #[serde(rename = "")]
    pub tabletservers: HashMap<String, TabletServers>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PathMetrics {
    pub path: String,
    pub space_used: i64,
    pub total_space_size: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TabletServers {
    pub time_since_hb: String,
    pub time_since_hb_sec: f32,
    pub status: String,
    pub uptime_seconds: i64,
    pub ram_used: String,
    pub ram_used_bytes: i64,
    pub num_sst_files: i32,
    pub total_sst_file_size: String,
    pub total_sst_file_size_bytes: i32,
    pub uncompressed_sst_file_size: String,
    pub uncompressed_sst_file_size_bytes: i32,
    pub path_metrics: Vec<PathMetrics>,
    pub read_ops_per_sec: f32,
    pub write_ops_per_sec: f32,
    pub user_tablets_total: i32,
    pub user_tablets_leaders: i32,
    pub system_tablets_total: i32,
    pub system_tablets_leaders: i32,
    pub active_tablets: i32,
    pub cloud: String,
    pub region: String,
    pub zone: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AllStoredTabletServers {
    pub stored_tabletservers: Vec<StoredTabletServers>,
    pub stored_pathmetrics: Vec<StoredPathMetrics>,
}
