use chrono::{DateTime, Local};

#[derive(Debug)]
pub struct Threads {
    pub thread_name: String,
    pub cumulative_user_cpu_s: String,
    pub cumulative_kernel_cpu_s: String,
    pub cumulative_iowait_cpu_s: String,
    pub stack: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredThreads {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub thread_name: String,
    pub cumulative_user_cpu_s: String,
    pub cumulative_kernel_cpu_s: String,
    pub cumulative_iowait_cpu_s: String,
    pub stack: String,
}

#[derive(Debug, Default)]
pub struct AllStoredThreads {
    pub stored_threads: Vec<StoredThreads>,
}