use chrono::{DateTime, Local};

#[derive(Debug, Default)]
pub struct GFlag {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredGFlags {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub gflag_name: String,
    pub gflag_value: String,
}

#[derive(Debug, Default)]
pub struct AllStoredGFlags {
    pub stored_gflags: Vec<StoredGFlags>,
}
