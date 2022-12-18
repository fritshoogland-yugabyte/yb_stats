use chrono::{DateTime, Local};

#[derive(Debug)]
pub struct LogLine {
    pub severity: String,
    pub timestamp: DateTime<Local>,
    pub tid: String,
    pub sourcefile_nr: String,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StoredLogLines {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub severity: String,
    pub tid: String,
    pub sourcefile_nr: String,
    pub message: String,
}

#[derive(Debug, Default)]
pub struct AllStoredLogLines {
    pub stored_loglines: Vec<StoredLogLines>,
}