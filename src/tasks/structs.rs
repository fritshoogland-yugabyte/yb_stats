//! The structs
//!
use chrono::{DateTime, Local};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Tasks {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the snapshot timestamp.
    pub timestamp: Option<DateTime<Local>>,
    pub tasks: Vec<Option<TaskDetail>>,
}

#[derive(Debug, Default)]
pub struct AllTasks {
    pub tasks: Vec<Tasks>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TaskDetail {
    pub task_type: String,
    pub status: String,
    pub name: String,
    pub state: String,
    pub start_time: String,
    pub duration: String,
    pub description: String,
}
