//! The structs
//!
use chrono::{DateTime, Local};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Threads {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub thread_name: String,
    pub cumulative_user_cpu_s: String,
    pub cumulative_kernel_cpu_s: String,
    pub cumulative_iowait_cpu_s: String,
    pub stack: String,
}

#[derive(Debug, Default)]
pub struct AllThreads {
    pub threads: Vec<Threads>,
}