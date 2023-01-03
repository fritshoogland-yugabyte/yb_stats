//! The structs
//!
use chrono::{DateTime, Local};

/// The root struct for deserializing the memtrackers HTML table.
///
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MemTrackers {
    /// yb_stats added to allow understanding the snapshot timestamp.
    pub hostname_port: String,
    /// yb_stats added to allow understanding the snapshot timestamp.
    pub timestamp: DateTime<Local>,
    pub id: String,
    pub current_consumption: String,
    pub peak_consumption: String,
    pub limit: String,
}

#[derive(Debug, Default)]
pub struct AllMemTrackers {
    pub memtrackers: Vec<MemTrackers>,
}
