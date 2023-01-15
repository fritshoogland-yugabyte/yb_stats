//! The structs
//!
#![allow(non_camel_case_types)]

use chrono::{DateTime, Local};

/// This struct is a wrapper for the HealthCheck struct.
///
/// In this way, the struct can be used with functions in impl.
#[derive(Debug, Default)]
pub struct AllHealthCheck {
    pub health_check: Vec<Health_Check>,
}
/// The root struct for deserializing `/api/v1/health-check`.
///
/// This struct is the begin struct needed to parse the results from:
/// - master:port/api/v1/health-check:
/// ```json
/// {
/// "dead_nodes": [],
/// "most_recent_uptime": 2501,
/// "under_replicated_tablets": []
/// }
/// ```
/// - tserver:port/api/v1/health-check:
/// ```json
/// {
/// "failed_tablets": []
/// }
/// ```
/// These are the responses from a master and tablet server in a normal healthy cluster.
///
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Health_Check {
    /// yb_stats added to allow understanding the source host
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the timestamp
    pub timestamp: Option<DateTime<Local>>,
    // dead_nodes, most_recent_uptime and under_replicated_tables are visible on the master
    pub dead_nodes: Option<Vec<String>>,
    pub most_recent_uptime: Option<u64>,
    pub under_replicated_tablets: Option<Vec<String>>,
    // failed_tablets is visible on a tablet server
    pub failed_tablets: Option<Vec<String>>
}
// diff
// we got only a single set of dead nodes and under replicated tablets for the cluster.
// the idea is that the first and second vectors only hold the entries unique to them.
#[derive(Debug, Default)]
pub struct HealthCheckDiff {
    pub first_dead_nodes: Vec<String>,
    pub second_dead_nodes: Vec<String>,
    pub first_under_replicated_tablets: Vec<String>,
    pub second_under_replicated_tablets: Vec<String>,
    pub master_found: bool,
}

