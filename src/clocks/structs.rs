//! The structs
//!
use chrono::{DateTime, Local};
/// This struct is a wrapper for the Clocks struct.
///
/// In this way, the struct can be used with functions in the impl.
#[derive(Debug, Default)]
pub struct AllClocks {
    pub clocks: Vec<Clocks>
}
/// The main struct containing the clocks information.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Clocks {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the timestamp.
    pub timestamp: Option<DateTime<Local>>,
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
