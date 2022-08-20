use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use std::path::PathBuf;
use std::fs;
use std::process;
use regex::Regex;
use serde_derive::{Serialize,Deserialize};
//use rayon;
use std::sync::mpsc::channel;
use log::*;

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

#[allow(dead_code)]
#[allow(clippy::ptr_arg)]
pub fn perform_versions_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    snapshot_number: i32,
    yb_stats_directory: &PathBuf,
    parallel: usize
) {
    info!("perform_versions_snapshot");
    let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
    let (tx, rx) = channel();

    pool.scope(move |s| {
        for host in hosts {
            for port in ports {
                let tx = tx.clone();
                s.spawn(move |_| {
                    let detail_snapshot_time = Local::now();
                    let version = read_version(host, port);
                    tx.send((format!("{}:{}", host, port), detail_snapshot_time, version)).expect("error sending data via tx (versions)");
                });
            }
        }
    });
    let mut stored_versions: Vec<StoredVersionData> = Vec::new();
    for (hostname_port, detail_snapshot_time, version) in rx {
        add_to_version_vector(version, &hostname_port, detail_snapshot_time, &mut stored_versions);
    }

    let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
    let versions_file = &current_snapshot_directory.join("versions");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&versions_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing versions data in snapshot directory {}: {}", &versions_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_versions {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();
}

#[allow(dead_code)]
pub fn read_version(
    host: &str,
    port: &str,
) -> VersionData {
    if ! scan_port_addr( format!("{}:{}", host, port)) {
        warn!("Warning hostname:port {}:{} cannot be reached, skipping (versions)", host, port);
        return parse_version(String::from(""))
    }
    if let Ok(data_from_http) = reqwest::blocking::get( format!("http://{}:{}/api/v1/version", host, port)) {
        parse_version(data_from_http.text().unwrap())
    } else {
        parse_version(String::from(""))
    }
}

#[allow(dead_code)]
#[allow(clippy::ptr_arg)]
fn read_version_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf
) -> Vec<StoredVersionData> {

    let mut stored_versions: Vec<StoredVersionData> = Vec::new();
    let versions_file = &yb_stats_directory.join(snapshot_number).join("versions");
    let file = fs::File::open(&versions_file)
        .unwrap_or_else(|e| {
            error!("Fatal: error reading file: {}: {}", &versions_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredVersionData = row.unwrap();
        let _ = &stored_versions.push(data);
    }
    stored_versions
}


#[allow(dead_code)]
pub fn print_version_data(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
    hostname_filter: &Regex
) {
    info!("print_version");
    let stored_versions: Vec<StoredVersionData> = read_version_snapshot(snapshot_number, yb_stats_directory);
    println!("{:20} {:15} {:10} {:10} {:24} {:10}",
             "hostname_port",
             "version_number",
             "build_nr",
             "build_type",
             "build_timestamp",
             "git_hash"
    );
    for row in stored_versions {
        if hostname_filter.is_match(&row.hostname_port) {
            println!("{:20} {:15} {:10} {:10} {:24} {:10}",
                     row.hostname_port,
                     row.version_number,
                     row.build_number,
                     row.build_type,
                     row.build_timestamp,
                     row.git_hash
            );
        }
    }
}

#[allow(dead_code)]
pub fn add_to_version_vector(versiondata: VersionData,
                             hostname: &str,
                             snapshot_time: DateTime<Local>,
                             stored_versiondata: &mut Vec<StoredVersionData>
) {
    if !versiondata.git_hash.is_empty() {
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
            build_number: versiondata.build_number,
        });
    }
}

#[allow(dead_code)]
fn parse_version( version_data: String ) -> VersionData {
    serde_json::from_str( &version_data )
        .unwrap_or_else(|_e| {
            VersionData { git_hash: "".to_string(), build_hostname: "".to_string(), build_timestamp: "".to_string(), build_username: "".to_string(), build_clean_repo: true, build_id: "".to_string(), build_type: "".to_string(), version_number: "".to_string(), build_number: "".to_string() }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version_data() {
        // This is what /api/v1/version return.
        let version = r#"{
    "git_hash": "d142556567b5e1c83ea5c915ec7b9964492b2321",
    "build_hostname": "centos-gcp-cloud-jenkins-worker-emjsmd",
    "build_timestamp": "25 Jan 2022 17:51:08 UTC",
    "build_username": "jenkins",
    "build_clean_repo": true,
    "build_id": "3801",
    "build_type": "RELEASE",
    "version_number": "2.11.2.0",
    "build_number": "89"
}"#.to_string();
        let result = parse_version(version);
        assert_eq!(result.git_hash, "d142556567b5e1c83ea5c915ec7b9964492b2321");
    }
}