use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;

#[derive(Serialize, Deserialize, Debug)]
pub struct VersionData {
    pub git_hash: String,
    pub build_hostname: String,
    pub build_timestamp: String,
    pub build_username: String,
    pub build_clean_repo: bool,
    pub build_id: String,
    pub build_type: String,
    pub version_number: String,
    pub build_number: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredVersionData {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub git_hash: String,
    pub build_hostname: String,
    pub build_timestamp: String,
    pub build_username: String,
    pub build_clean_repo: String,
    pub build_id: String,
    pub build_type: String,
    pub version_number: String,
    pub build_number: String,
}

pub fn read_version( hostname: &str) -> VersionData {
    if ! scan_port_addr( hostname) {
        println!("Warning hostname:port {} cannot be reached, skipping", hostname.to_string());
        return parse_version(String::from(""))
    }
    if let Ok(data_from_http) = reqwest::blocking::get( format!("http://{}/api/v1/version", hostname.to_string())) {
        parse_version(data_from_http.text().unwrap())
    } else {
        parse_version(String::from(""))
    }
}

pub fn add_to_version_vector(versiondata: VersionData,
                             hostname: &str,
                             snapshot_time: DateTime<Local>,
                             stored_versiondata: &mut Vec<StoredVersionData>
) {
    stored_versiondata.push(StoredVersionData {
        hostname_port: hostname.to_string(),
        timestamp: snapshot_time,
        git_hash: versiondata.git_hash.to_string(),
        build_hostname: versiondata.build_hostname.to_string(),
        build_timestamp: versiondata.build_timestamp.to_string(),
        build_username: versiondata.build_username.to_string(),
        build_clean_repo: versiondata.build_clean_repo.to_string(),
        build_id: versiondata.build_id.to_string(),
        build_type: versiondata.build_type.to_string(),
        version_number: versiondata.version_number.to_string(),
        build_number: versiondata.build_number.to_string(),
    });
}

fn parse_version( version_data: String ) -> VersionData {
    serde_json::from_str( &version_data )
        .unwrap_or_else(|_e| {
            return VersionData { git_hash: "".to_string(), build_hostname: "".to_string(), build_timestamp: "".to_string(), build_username: "".to_string(), build_clean_repo: true, build_id: "".to_string(), build_type: "".to_string(), version_number: "".to_string(), build_number: "".to_string() };
        })
}

