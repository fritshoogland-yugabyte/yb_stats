use chrono::{DateTime, Local};

#[derive(Serialize, Deserialize, Debug)]
pub struct AllVars {
    pub flags: Vec<Vars>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Vars {
    pub name: String,
    pub value: String,
    #[serde(rename = "type")]
    pub vars_type: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AllStoredVars {
    pub stored_vars: Vec<StoredVars>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredVars {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub name: String,
    pub value: String,
    pub vars_type: String,
}
