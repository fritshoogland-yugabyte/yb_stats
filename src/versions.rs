use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use std::{fs, process, sync::mpsc::channel, time::Instant, error::Error, env, collections::BTreeMap};
use colored::Colorize;
use regex::Regex;
use serde_derive::{Serialize,Deserialize};
use log::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct Version {
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

impl Version {
    fn empty() -> Self {
        Self {
            git_hash: "".to_string(),
            build_hostname: "".to_string(),
            build_timestamp: "".to_string(),
            build_username: "".to_string(),
            build_clean_repo: true,
            build_id: "".to_string(),
            build_type: "".to_string(),
            version_number: "".to_string(),
            build_number: "".to_string()
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredVersion {
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

impl StoredVersion {
    fn new_from_version(hostname_port: &str, timestamp: DateTime<Local>, version: Version) -> Self {
        Self {
            hostname_port: hostname_port.to_string(),
            timestamp,
            git_hash: version.git_hash.to_string(),
            build_hostname: version.build_hostname.to_string(),
            build_timestamp: version.build_timestamp.to_string(),
            build_username: version.build_hostname.to_string(),
            build_clean_repo: version.build_clean_repo.to_string(),
            build_id: version.build_id.to_string(),
            build_type: version.build_type.to_string(),
            version_number: version.version_number.to_string(),
            build_number: version.build_number,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AllStoredVersions {
   stored_versions: Vec<StoredVersion>,
}

impl AllStoredVersions {
    pub fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    )
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allstoredversions = AllStoredVersions::read_versions(hosts, ports, parallel);

        allstoredversions.save_snapshot(snapshot_number)
            .unwrap_or_else(|e| {
                error!("error saving snapshot: {}", e);
                process::exit(1);
            });

        info!("end snapshot: {:?}", timer.elapsed())
    }
    pub fn read_versions(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllStoredVersions
    {
        info!("begin parallel http read");
        let timer = Instant::now();

        let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
        let (tx, rx) = channel();
        pool.scope(move |s| {
            for host in hosts {
                for port in ports {
                    let tx = tx.clone();
                    s.spawn(move |_| {
                        let detail_snapshot_time = Local::now();
                        let versions = AllStoredVersions::read_http(host, port);
                        tx.send((format!("{}:{}", host, port), detail_snapshot_time, versions)).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allstoredversions = AllStoredVersions {
            stored_versions: Vec::new(),
        };
        for (hostname_port, detail_snapshot_time, versions) in rx {
            AllStoredVersions::split_into_vectors(versions, &hostname_port, detail_snapshot_time, &mut allstoredversions);
        }

        allstoredversions
    }
    fn split_into_vectors(
        version: Version,
        hostname_port: &str,
        detail_snapshot_time: DateTime<Local>,
        allstoredversions: &mut AllStoredVersions,
    )
    {
        if !version.git_hash.is_empty() {
            allstoredversions.stored_versions.push(StoredVersion::new_from_version(hostname_port, detail_snapshot_time, version));
        }
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> Version
    {
        if ! scan_port_addr(format!("{}:{}", host, port)) {
            warn!("hostname: port {}:{} cannot be reached, skipping", host, port);
            return AllStoredVersions::parse_version(String::from(""), "", "")
        };
        let data_from_http = reqwest::blocking::get(format!("http://{}:{}/api/v1/version", host, port))
            .unwrap_or_else(|e| {
                error!("Fatal: error reading from URL: {}", e);
                process::exit(1);
            })
            .text().unwrap();
        AllStoredVersions::parse_version(data_from_http, host, port)
    }
    fn parse_version(
        versions_data: String,
        host: &str,
        port: &str,
    ) -> Version
    {
        serde_json::from_str(&versions_data)
            .unwrap_or_else( |e| {
                info!("({}:{}) cloud not parse /api/v1/versions json data for versions, error: {}", host, port ,e);
                Version::empty()
            })
    }
    fn save_snapshot(
        self,
        snapshot_number: i32
    ) -> Result<(), Box<dyn Error>>
    {
        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number.to_string());

        let versions_file = &current_snapshot_directory.join("versions");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&versions_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_versions {
            writer.serialize(row)?;
        }
        writer.flush()?;

        Ok(())
    }
    pub fn read_snapshot(
        snapshot_number: &String
    ) -> Result<AllStoredVersions, Box<dyn Error>>
    {
        let mut allstoredversions = AllStoredVersions {
            stored_versions: Vec::new(),
        };

        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number);

        let versions_file = &current_snapshot_directory.join("versions");
        let file = fs::File::open(&versions_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredVersion = row?;
            allstoredversions.stored_versions.push(data);
        };

        Ok(allstoredversions)
    }
    pub fn print(
        &self,
        hostname_filter: &Regex,
    )
    {
        info!("print versions");

        println!("{:20} {:15} {:10} {:10} {:24} {:10}",
                 "hostname_port",
                 "version_number",
                 "build_nr",
                 "build_type",
                 "build_timestamp",
                 "git_hash"
        );
        for row in self.stored_versions.iter() {
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
}

#[derive(Debug)]
pub struct SnapshotDiffStoredVersions {
    pub first_git_hash: String,
    pub first_build_hostname: String,
    pub first_build_timestamp: String,
    pub first_build_username: String,
    pub first_build_clean_repo: String,
    pub first_build_id: String,
    pub first_build_type: String,
    pub first_version_number: String,
    pub first_build_number: String,
    pub second_git_hash: String,
    pub second_build_hostname: String,
    pub second_build_timestamp: String,
    pub second_build_username: String,
    pub second_build_clean_repo: String,
    pub second_build_id: String,
    pub second_build_type: String,
    pub second_version_number: String,
    pub second_build_number: String,
}

impl SnapshotDiffStoredVersions {
    fn first_snapshot( storedversion: StoredVersion )  -> Self
    {
        Self {
            first_git_hash: storedversion.git_hash.to_string(),
            first_build_hostname: storedversion.build_hostname.to_string(),
            first_build_timestamp: storedversion.build_timestamp.to_string(),
            first_build_username: storedversion.build_username.to_string(),
            first_build_clean_repo: storedversion.build_clean_repo.to_string(),
            first_build_id: storedversion.build_id.to_string(),
            first_build_type: storedversion.build_type.to_string(),
            first_version_number: storedversion.version_number.to_string(),
            first_build_number: storedversion.build_number.to_string(),
            second_git_hash: "".to_string(),
            second_build_hostname: "".to_string(),
            second_build_timestamp: "".to_string(),
            second_build_username: "".to_string(),
            second_build_clean_repo: "".to_string(),
            second_build_id: "".to_string(),
            second_build_type: "".to_string(),
            second_version_number: "".to_string(),
            second_build_number: "".to_string(),
        }
    }
    fn second_snapshot_new( storedversion: StoredVersion ) -> Self
    {
        Self {
            first_git_hash: "".to_string(),
            first_build_hostname: "".to_string(),
            first_build_timestamp: "".to_string(),
            first_build_username: "".to_string(),
            first_build_clean_repo: "".to_string(),
            first_build_id: "".to_string(),
            first_build_type: "".to_string(),
            first_version_number: "".to_string(),
            first_build_number: "".to_string(),
            second_git_hash: storedversion.git_hash.to_string(),
            second_build_hostname: storedversion.build_hostname.to_string(),
            second_build_timestamp: storedversion.build_timestamp.to_string(),
            second_build_username: storedversion.build_username.to_string(),
            second_build_clean_repo: storedversion.build_clean_repo.to_string(),
            second_build_id: storedversion.build_id.to_string(),
            second_build_type: storedversion.build_type.to_string(),
            second_version_number: storedversion.version_number.to_string(),
            second_build_number: storedversion.build_number.to_string(),
        }
    }
    fn second_snapshot_existing( storedversion_diff_row: &mut SnapshotDiffStoredVersions, storedversion: StoredVersion ) -> Self
    {
        Self {
            first_git_hash: storedversion_diff_row.first_git_hash.to_string(),
            first_build_hostname: storedversion_diff_row.first_build_hostname.to_string(),
            first_build_timestamp: storedversion_diff_row.first_build_timestamp.to_string(),
            first_build_username: storedversion_diff_row.first_build_username.to_string(),
            first_build_clean_repo: storedversion_diff_row.first_build_clean_repo.to_string(),
            first_build_id: storedversion_diff_row.first_build_id.to_string(),
            first_build_type: storedversion_diff_row.first_build_type.to_string(),
            first_version_number: storedversion_diff_row.first_version_number.to_string(),
            first_build_number: storedversion_diff_row.first_build_number.to_string(),
            second_git_hash: storedversion.git_hash.to_string(),
            second_build_hostname: storedversion.build_hostname.to_string(),
            second_build_timestamp: storedversion.build_timestamp.to_string(),
            second_build_username: storedversion.build_username.to_string(),
            second_build_clean_repo: storedversion.build_clean_repo.to_string(),
            second_build_id: storedversion.build_id.to_string(),
            second_build_type: storedversion.build_type.to_string(),
            second_version_number: storedversion.version_number.to_string(),
            second_build_number: storedversion.build_number.to_string(),
        }
    }
}

type BTreeMapSnapshotDiffVersions = BTreeMap<String, SnapshotDiffStoredVersions>;

pub struct SnapshotDiffBTreeMapsVersions {
    pub btreemap_snapshotdiff_versions: BTreeMapSnapshotDiffVersions,
}

impl SnapshotDiffBTreeMapsVersions {
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
    ) -> SnapshotDiffBTreeMapsVersions
    {
        let allstoredversions = AllStoredVersions::read_snapshot(begin_snapshot)
            .unwrap_or_else(|e| {
                error!("Fatal: error reading snapshot: {}", e);
                process::exit(1);
            });
        let mut versions_snapshot_diff = SnapshotDiffBTreeMapsVersions::first_snapshot(allstoredversions);

        let allstoredversions = AllStoredVersions::read_snapshot(end_snapshot)
            .unwrap_or_else(|e| {
                error!("Fatal: error reading snapshot: {}", e);
                process::exit(1);
            });
        versions_snapshot_diff.second_snapshot(allstoredversions);

        versions_snapshot_diff
    }
    fn first_snapshot(
        allstoredversions: AllStoredVersions,
    ) -> SnapshotDiffBTreeMapsVersions
    {
        let mut snapshotdiff_btreemaps = SnapshotDiffBTreeMapsVersions {
            btreemap_snapshotdiff_versions: Default::default(),
        };
        for row in allstoredversions.stored_versions.into_iter() {
            match snapshotdiff_btreemaps.btreemap_snapshotdiff_versions.get_mut( &row.hostname_port.to_string() ) {
                Some( _version_row ) => {
                    error!("Found second entry for first entry of version based on hostname: {}", &row.hostname_port) ;
                },
                None => {
                    snapshotdiff_btreemaps.btreemap_snapshotdiff_versions.insert(
                        row.hostname_port.to_string(),
                        SnapshotDiffStoredVersions::first_snapshot(row)
                    );
                },
            }
        }
        snapshotdiff_btreemaps
    }
    fn second_snapshot(
        &mut self,
        allstoredversions: AllStoredVersions,
    )
    {
        for row in allstoredversions.stored_versions.into_iter() {
            match self.btreemap_snapshotdiff_versions.get_mut( &row.hostname_port.to_string() ) {
                Some( version_row ) => {
                    if version_row.first_version_number == row.version_number
                        && version_row.first_build_number == row.build_number
                        && version_row.first_build_hostname == row.build_hostname
                        && version_row.first_build_type == row.build_type
                        && version_row.first_build_clean_repo == row.build_clean_repo
                        && version_row.first_build_id == row.build_id
                        && version_row.first_build_timestamp == row.build_timestamp
                        && version_row.first_git_hash == row.git_hash
                        && version_row.first_build_username == row.build_username
                    {
                        self.btreemap_snapshotdiff_versions.remove( &row.hostname_port.to_string() );
                    } else {
                        *version_row = SnapshotDiffStoredVersions::second_snapshot_existing(version_row, row)       ;
                    };
                },
                None => {
                    self.btreemap_snapshotdiff_versions.insert(
                        row.hostname_port.to_string(),
                        SnapshotDiffStoredVersions::second_snapshot_new(row)
                    );
                },
            }
        }
    }
    pub fn print(
        &self,
    )
    {
        for (hostname, row) in self.btreemap_snapshotdiff_versions.iter() {
            if row.first_git_hash.is_empty() {
                println!("{} {:20} Versions: {:15} {:10} {:10} {:24} {:10}", "+".to_string().green(), hostname, row.second_version_number, row.second_build_number, row.second_build_type, row.second_build_timestamp, row.second_git_hash);
            } else if row.second_git_hash.is_empty() {
                println!("{} {:20} Versions: {:15} {:10} {:10} {:24} {:10}", "-".to_string().red(), hostname, row.first_version_number, row.first_build_number, row.first_build_type, row.first_build_timestamp, row.first_git_hash);
            } else {
                print!("{} {:20} Versions: ", "*".to_string().yellow(), hostname);
                if row.first_version_number != row.second_version_number {
                    print!("{}->{} ", row.first_version_number.yellow(), row.second_version_number.yellow());
                } else {
                    print!("{} ", row.second_version_number);
                };
                if row.first_build_number != row.second_build_number {
                    print!("{}->{} ", row.first_build_number, row.second_build_number);
                } else {
                    print!("{} ", row.second_build_number);
                };
                if row.first_build_type != row.second_build_type {
                    print!("{}->{} ", row.first_build_type, row.second_build_type);
                } else {
                    print!("{} ", row.second_build_type);
                };
                if row.first_build_timestamp != row.second_build_timestamp {
                    print!("{}->{} ", row.first_build_timestamp, row.second_build_timestamp);
                } else {
                    print!("{} ", row.second_build_timestamp);
                };
                if row.first_git_hash != row.second_git_hash {
                    println!("{}->{} ", row.first_git_hash, row.second_git_hash);
                } else {
                    println!("{} ", row.second_git_hash);
                };
            }
        }
    }
    pub fn adhoc_read_first_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> SnapshotDiffBTreeMapsVersions
    {
        let allstoredversions = AllStoredVersions::read_versions(hosts, ports, parallel);
        SnapshotDiffBTreeMapsVersions::first_snapshot(allstoredversions)
    }
    pub fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allstoredversions = AllStoredVersions::read_versions(hosts, ports, parallel);
        self.second_snapshot(allstoredversions);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_version_data() {
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

    use crate::utility;

    #[test]
    fn integration_parse_versiondata_master() {
        let mut stored_versiondata: Vec<StoredVersionData> = Vec::new();
        let detail_snapshot_time = Local::now();
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let data_parsed_from_json = read_version(hostname.as_str(), port.as_str());
        add_to_version_vector(data_parsed_from_json, format!("{}:{}", hostname, port).as_str(), detail_snapshot_time, &mut stored_versiondata);
        // each daemon should return one row.
        assert!(stored_versiondata.len() == 1);
    }
    #[test]
    fn integration_parse_versiondata_tserver() {
        let mut stored_versiondata: Vec<StoredVersionData> = Vec::new();
        let detail_snapshot_time = Local::now();
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();

        let data_parsed_from_json = read_version(hostname.as_str(), port.as_str());
        add_to_version_vector(data_parsed_from_json, format!("{}:{}", hostname, port).as_str(), detail_snapshot_time, &mut stored_versiondata);
        // each daemon should return one row.
        assert!(stored_versiondata.len() == 1);
    }
}