//! Module for reading the /api/v1/versions output for the master and tablet server.
use chrono::{DateTime, Local};
use std::{sync::mpsc::channel, time::Instant};
use colored::Colorize;
use regex::Regex;
//use serde_derive::{Serialize,Deserialize};
use log::*;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::versions::{Version, StoredVersion, AllStoredVersions, SnapshotDiffStoredVersions, SnapshotDiffBTreeMapsVersions};
use crate::Opts;

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



impl AllStoredVersions {
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allstoredversions = AllStoredVersions::read_versions(hosts, ports, parallel).await;
        snapshot::save_snapshot(snapshot_number, "versions", allstoredversions.stored_versions)?;

        info!("end snapshot: {:?}", timer.elapsed());
        Ok(())
    }
    pub fn new() -> Self {
        Default::default()
    }
    pub async fn read_versions(
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

        let mut allstoredversions = AllStoredVersions::new();

        for (hostname_port, detail_snapshot_time, versions) in rx {
            allstoredversions.split_into_vectors(versions, &hostname_port, detail_snapshot_time);
        }
        allstoredversions
    }
    fn split_into_vectors(
        &mut self,
        version: Version,
        hostname_port: &str,
        detail_snapshot_time: DateTime<Local>,
    )
    {
        if !version.git_hash.is_empty() {
            self.stored_versions.push(StoredVersion::new_from_version(hostname_port, detail_snapshot_time, version));
        }
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> Version
    {
        let data_from_http = if utility::scan_host_port( host, port) {
            utility::http_get(host, port, "api/v1/version")
        } else {
            String::new()
        };
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
                debug!("({}:{}) could not parse /api/v1/versions json data for versions, error: {}", host, port ,e);
                Version::empty()
            })
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
            first_build_number: storedversion.build_number,
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
            second_build_number: storedversion.build_number,
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
            second_build_number: storedversion.build_number,
        }
    }
}


impl SnapshotDiffBTreeMapsVersions {
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
    ) -> Result<SnapshotDiffBTreeMapsVersions>
    {
        // first snapshot
        let mut allstoredversions = AllStoredVersions::new();
        allstoredversions.stored_versions = snapshot::read_snapshot(begin_snapshot, "versions")?;
        let mut versions_snapshot_diff = SnapshotDiffBTreeMapsVersions::new();
        versions_snapshot_diff.first_snapshot(allstoredversions);
        // second snapshot
        let mut allstoredversions = AllStoredVersions::new();
        allstoredversions.stored_versions = snapshot::read_snapshot(end_snapshot, "versions")?;
        versions_snapshot_diff.second_snapshot(allstoredversions);
        // return diff
        Ok(versions_snapshot_diff)
    }
    pub fn new() -> Self {
        Default::default()
    }
    fn first_snapshot(
        &mut self,
        allstoredversions: AllStoredVersions,
    )
    {
        for row in allstoredversions.stored_versions.into_iter() {
            match self.btreemap_snapshotdiff_versions.get_mut( &row.hostname_port.to_string() ) {
                Some( _version_row ) => {
                    error!("Found second entry for first entry of version based on hostname: {}", &row.hostname_port) ;
                },
                None => {
                    self.btreemap_snapshotdiff_versions.insert(
                        row.hostname_port.to_string(),
                        SnapshotDiffStoredVersions::first_snapshot(row)
                    );
                },
            }
        }
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
        hostname_filter: &Regex,
    )
    {
        for (hostname, row) in self.btreemap_snapshotdiff_versions.iter().filter(|(k,_v)| hostname_filter.is_match(k))
        {
            if row.first_git_hash.is_empty()
            {
                print!("{} {:20} Versions: ", "+".to_string().green(), hostname);
                println!("{} b{} {} {} {}", row.second_version_number, row.second_build_number, row.second_build_type, row.second_build_timestamp, row.second_git_hash);
            }
            else if row.second_git_hash.is_empty()
            {
                print!("{} {:20} Versions: ", "-".to_string().red(), hostname);
                println!("{} b{} {} {} {}", row.first_version_number, row.first_build_number, row.first_build_type, row.first_build_timestamp, row.first_git_hash);
            }
            else
            {
                print!("{} {:20} Versions: ", "*".to_string().yellow(), hostname);
                if row.first_version_number != row.second_version_number {
                    print!("{}->{} ", row.first_version_number.yellow(), row.second_version_number.yellow());
                } else {
                    print!("{} ", row.second_version_number);
                };
                if row.first_build_number != row.second_build_number {
                    print!("b{}->b{} ", row.first_build_number.yellow(), row.second_build_number.yellow());
                } else {
                    print!("b{} ", row.second_build_number);
                };
                if row.first_build_type != row.second_build_type {
                    print!("{}->{} ", row.first_build_type.yellow(), row.second_build_type.yellow());
                } else {
                    print!("{} ", row.second_build_type);
                };
                if row.first_build_timestamp != row.second_build_timestamp {
                    print!("{}->{} ", row.first_build_timestamp.yellow(), row.second_build_timestamp.yellow());
                } else {
                    print!("{} ", row.second_build_timestamp);
                };
                if row.first_git_hash != row.second_git_hash {
                    println!("{}->{} ", row.first_git_hash.yellow(), row.second_git_hash.yellow());
                } else {
                    println!("{} ", row.second_git_hash);
                };
            }
        }
    }
    pub async fn adhoc_read_first_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allstoredversions = AllStoredVersions::read_versions(hosts, ports, parallel).await;
        self.first_snapshot(allstoredversions);
    }
    pub async fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allstoredversions = AllStoredVersions::read_versions(hosts, ports, parallel).await;
        self.second_snapshot(allstoredversions);
    }
}

pub async fn versions_diff(
    options: &Opts,
) -> Result<()>
{
    info!("versions diff");

    if options.begin.is_none() || options.end.is_none() {
        snapshot::Snapshot::print()?;
    }
    if options.snapshot_list { return Ok(()) };

    let hostname_filter = utility::set_regex(&options.hostname_match);

    let (begin_snapshot, end_snapshot, _begin_snapshot_row) = snapshot::Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end)?;
    let versions_diff = SnapshotDiffBTreeMapsVersions::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    versions_diff.print(&hostname_filter);

    Ok(())
}

pub async fn print_version(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    let hostname_filter = utility::set_regex(&options.hostname_match);

    match options.print_version.as_ref().unwrap() {
        Some(snapshot_number) => {

            let mut allstoredversions = AllStoredVersions::new();
            allstoredversions.stored_versions = snapshot::read_snapshot(snapshot_number, "versions")?;

            allstoredversions.print(&hostname_filter);
        },
        None => {
            let allstoredversions = AllStoredVersions::read_versions(&hosts, &ports, parallel).await;
            allstoredversions.print(&hostname_filter);
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    //use crate::utility_test::*;

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
        let result = AllStoredVersions::parse_version(version, "", "");
        assert_eq!(result.git_hash, "d142556567b5e1c83ea5c915ec7b9964492b2321");
    }

    #[tokio::test]
    async fn integration_parse_versiondata_master() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let allstoredversions = AllStoredVersions::read_versions(&vec![&hostname], &vec![&port], 1).await;

        // each daemon should return one row.
        assert!(allstoredversions.stored_versions.len() == 1);
    }
    #[tokio::test]
    async fn integration_parse_versiondata_tserver() {
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();

        let allstoredversions = AllStoredVersions::read_versions(&vec![&hostname], &vec![&port], 1).await;

        // each daemon should return one row.
        assert!(allstoredversions.stored_versions.len() == 1);
    }
}
