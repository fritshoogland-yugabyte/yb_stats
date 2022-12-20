//! The struct definitions
//!
use chrono::{DateTime, Local};

#[derive(Debug)]
pub struct Clocks {
    pub server: String,
    pub time_since_heartbeat: String,
    pub status_uptime: String,
    pub physical_time_utc: String,
    pub hybrid_time_utc: String,
    pub heartbeat_rtt: String,
    pub cloud: String,
    pub region: String,
    pub zone: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredClocks {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub server: String,
    pub time_since_heartbeat: String,
    pub status_uptime: String,
    pub physical_time_utc: String,
    pub hybrid_time_utc: String,
    pub heartbeat_rtt: String,
    pub cloud: String,
    pub region: String,
    pub zone: String,
}

#[derive(Debug, Default)]
pub struct AllStoredClocks {
    pub stored_clocks: Vec<StoredClocks>
}