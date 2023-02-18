//! The structs
//!
use chrono::{DateTime, Local};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Operations {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the snapshot timestamp.
    pub timestamp: Option<DateTime<Local>>,
    pub tasks: Vec<Option<Operation>>,
}

#[derive(Debug, Default)]
pub struct AllOperations {
    pub operations: Vec<Operations>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Operation {
    pub tablet_id: String,
    pub op_id: String,
    pub transaction_type: String,
    pub total_time_in_flight: String,
    pub description: String,
}
