//! The structs
//!
use chrono::{DateTime, Local};

/// The root structure for deserializing the gflags
/// This is taken from `/varz?raw`:
///
/// ```text
/// --log_filename=yb-master
/// --placement_cloud=local
/// --placement_region=local
/// --placement_zone=local
/// --rpc_bind_addresses=0.0.0.0
/// --webserver_interface=
/// --webserver_port=7000
/// ```
///
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct GFlag {
    /// yb_stats added to allow understanding the snapshot timestamp.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the snapshot timestamp.
    pub timestamp: Option<DateTime<Local>>,
    pub name: String,
    pub value: String,
}

/// The wrapper struct for the vector holding the gflags.
#[derive(Debug, Default)]
pub struct AllGFlags {
    pub gflags: Vec<GFlag>,
}
