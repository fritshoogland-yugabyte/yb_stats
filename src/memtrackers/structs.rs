use chrono::{DateTime, Local};

#[derive(Debug)]
pub struct MemTrackers {
    pub id: String,
    pub current_consumption: String,
    pub peak_consumption: String,
    pub limit: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredMemTrackers {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub id: String,
    pub current_consumption: String,
    pub peak_consumption: String,
    pub limit: String,
}

#[derive(Debug, Default)]
pub struct AllStoredMemTrackers {
    pub stored_memtrackers: Vec<StoredMemTrackers>,
}
