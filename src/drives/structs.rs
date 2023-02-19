//! The structs
//!
use chrono::{DateTime, Local};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Drives {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the snapshot timestamp.
    pub timestamp: Option<DateTime<Local>>,
    pub drive: Vec<Option<Drive>>,
}

#[derive(Debug, Default)]
pub struct AllDrives {
    pub drives: Vec<Drives>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Drive {
    pub path: String,
    pub used_space: String,
    pub total_space: String,
}