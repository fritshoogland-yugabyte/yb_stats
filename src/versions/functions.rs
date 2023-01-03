//! The impls and functions
//!
use chrono::Local;
use std::{sync::mpsc::channel, time::Instant};
use colored::Colorize;
use regex::Regex;
use log::*;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::versions::{Version, AllVersions, VersionsDiff, VersionsDiffFields};
use crate::Opts;

impl AllVersions {
    pub fn new() -> Self {
        Default::default()
    }
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allversions = AllVersions::read_versions(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "versions", allversions.versions)?;

        info!("end snapshot: {:?}", timer.elapsed());
        Ok(())
    }
    pub async fn read_versions(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllVersions
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
                        let mut version = AllVersions::read_http(host, port);
                        version.timestamp = Some(detail_snapshot_time);
                        version.hostname_port = Some(format!("{}:{}", host, port));
                        tx.send(version).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allversions = AllVersions::new();

        for version in rx
        {
            allversions.versions.push(version);
        }

        allversions
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> Version
    {
        let data_from_http = utility::http_get(host, port, "api/v1/version");
        AllVersions::parse_version(data_from_http, host, port)
    }
    fn parse_version(
        http_data: String,
        host: &str,
        port: &str,
    ) -> Version
    {
        serde_json::from_str(&http_data)
            .unwrap_or_else( |e| {
                debug!("({}:{}) could not parse /api/v1/versions json data for versions, error: {}", host, port ,e);
                Version::default()
            })
    }
    pub fn print(
        &self,
        hostname_filter: &Regex,
    )
    {
        println!("{:20} {:15} {:10} {:10} {:24} {:10}",
                 "hostname_port",
                 "version_number",
                 "build_nr",
                 "build_type",
                 "build_timestamp",
                 "git_hash"
        );
        for row in self.versions.iter() {
            if hostname_filter.is_match(&row.hostname_port.clone().expect("hostname:port should be set")) {
                println!("{:20} {:15} {:10} {:10} {:24} {:10}",
                         row.hostname_port.as_ref().expect("hostname:port should be set"),
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

impl VersionsDiff {
    pub fn new() -> Self { Default::default() }
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
    ) -> Result<VersionsDiff>
    {
        let mut allversions = AllVersions::new();
        allversions.versions = snapshot::read_snapshot_json(begin_snapshot, "versions")?;
        let mut versionsdiff = VersionsDiff::new();
        versionsdiff.first_snapshot(allversions);

        let mut allversions = AllVersions::new();
        allversions.versions = snapshot::read_snapshot_json(end_snapshot, "versions")?;
        versionsdiff.second_snapshot(allversions);

        Ok(versionsdiff)
    }
    fn first_snapshot(
        &mut self,
        allversions: AllVersions,
    )
    {
        for version in allversions.versions
        {
            self.btreeversionsdiff
                .entry(version.hostname_port.clone().expect("hostname:port should be set"))
                .and_modify(|_| error!("Duplicate hostname:port entry: {}", version.hostname_port.clone().expect("hostname:port should be set")))
                .or_insert( VersionsDiffFields {
                    first_git_hash: version.git_hash,
                    first_build_hostname: version.build_hostname,
                    first_build_timestamp: version.build_timestamp,
                    first_build_username: version.build_username,
                    first_build_clean_repo: version.build_clean_repo.to_string(),
                    first_build_id: version.build_id,
                    first_build_type: version.build_type,
                    first_version_number: version.version_number,
                    first_build_number: version.build_number,
                    ..Default::default()
                });
        }
    }
    fn second_snapshot(
        &mut self,
        allversions: AllVersions,
    )
    {
        for version in allversions.versions
        {
            self.btreeversionsdiff
                .entry(version.hostname_port.clone().expect("hostname:port should be set"))
                .and_modify(|versionsdifffields| {
                    versionsdifffields.second_git_hash = version.git_hash.clone();
                    versionsdifffields.second_build_hostname = version.build_hostname.clone();
                    versionsdifffields.second_build_timestamp = version.build_timestamp.clone();
                    versionsdifffields.second_build_username = version.build_username.clone();
                    versionsdifffields.second_build_clean_repo = version.build_clean_repo.to_string();
                    versionsdifffields.second_build_id = version.build_id.clone();
                    versionsdifffields.second_build_type = version.build_type.clone();
                    versionsdifffields.second_version_number = version.version_number.clone();
                    versionsdifffields.second_build_number = version.build_number.clone();
                })
                .or_insert( VersionsDiffFields {
                    second_git_hash: version.git_hash,
                    second_build_hostname: version.build_hostname,
                    second_build_timestamp: version.build_timestamp,
                    second_build_username: version.build_username,
                    second_build_clean_repo: version.build_clean_repo.to_string(),
                    second_build_id: version.build_id,
                    second_build_type: version.build_type,
                    second_version_number: version.version_number,
                    second_build_number: version.build_number,
                    ..Default::default()
                });
        }
    }
    pub fn print(
        &self,
        hostname_filter: &Regex,
    )
    {
        for (hostname, row) in self.btreeversionsdiff.iter().filter(|(k,_v)| hostname_filter.is_match(k))
        {
            #[allow(clippy::nonminimal_bool)]
            if row.first_git_hash == row.second_git_hash
                && row.first_build_hostname == row.second_build_hostname
                && row.first_build_timestamp == row.second_build_timestamp
                && row.first_build_username == row.second_build_username
                && row.first_build_clean_repo == row.second_build_clean_repo
                && row.first_build_id == row.second_build_id
                && row.first_build_type == row.second_build_type
                && row.first_version_number == row.second_version_number
                && row.first_build_number == row.second_build_number
            {
                // all first and second fields are equal, continue iterator
                debug!("equal, next server");
                continue;
            }
            // okay, first and second fields are not equal
            // is the a "first" entry empty, indicating it appeared between snapshots
            else if row.first_git_hash.is_empty()
            {
                print!("{} {:20} Versions: ", "+".to_string().green(), hostname);
                println!("{} b{} {} {} {}",
                         row.second_version_number,
                         row.second_build_number,
                         row.second_build_type,
                         row.second_build_timestamp,
                         row.second_git_hash
                );
            }
            // is a "second" entry empty, indicating it disappeared between snapshots
            else if row.second_git_hash.is_empty()
            {
                print!("{} {:20} Versions: ", "-".to_string().red(), hostname);
                println!("{} b{} {} {} {}",
                         row.first_version_number,
                         row.first_build_number,
                         row.first_build_type,
                         row.first_build_timestamp,
                         row.first_git_hash
                );
            }
            else
            {
                // first and second fields are set, but not equal: changed versions
                print!("{} {:20} Versions: ", "*".to_string().yellow(), hostname);
                if row.first_version_number != row.second_version_number
                {
                    print!("{}->{} ", row.first_version_number.yellow(), row.second_version_number.yellow());
                }
                else
                {
                    print!("{} ", row.second_version_number);
                };
                if row.first_build_number != row.second_build_number
                {
                    print!("b{}->b{} ", row.first_build_number.yellow(), row.second_build_number.yellow());
                }
                else
                {
                    print!("b{} ", row.second_build_number);
                };
                if row.first_build_type != row.second_build_type
                {
                    print!("{}->{} ", row.first_build_type.yellow(), row.second_build_type.yellow());
                }
                else
                {
                    print!("{} ", row.second_build_type);
                };
                if row.first_build_timestamp != row.second_build_timestamp
                {
                    print!("{}->{} ", row.first_build_timestamp.yellow(), row.second_build_timestamp.yellow());
                }
                else
                {
                    print!("{} ", row.second_build_timestamp);
                };
                if row.first_git_hash != row.second_git_hash
                {
                    println!("{}->{} ", row.first_git_hash.yellow(), row.second_git_hash.yellow());
                }
                else
                {
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
        let allversions = AllVersions::read_versions(hosts, ports, parallel).await;
        self.first_snapshot(allversions);
    }
    pub async fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allversions = AllVersions::read_versions(hosts, ports, parallel).await;
        self.second_snapshot(allversions);
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
    let versions_diff = VersionsDiff::snapshot_diff(&begin_snapshot, &end_snapshot)?;
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

            let mut allversions = AllVersions::new();
            allversions.versions = snapshot::read_snapshot_json(snapshot_number, "versions")?;

            allversions.print(&hostname_filter);
        },
        None => {
            let allversions = AllVersions::read_versions(&hosts, &ports, parallel).await;
            allversions.print(&hostname_filter);
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_version_data() {
        // This is what /api/v1/version returns.
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
        let result = AllVersions::parse_version(version, "", "");
        assert_eq!(result.git_hash, "d142556567b5e1c83ea5c915ec7b9964492b2321");
    }

    #[tokio::test]
    async fn integration_parse_versiondata_master() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let allversions = AllVersions::read_versions(&vec![&hostname], &vec![&port], 1).await;

        // each daemon should return one row.
        assert!(allversions.versions.len() == 1);
    }
    #[tokio::test]
    async fn integration_parse_versiondata_tserver() {
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();

        let allversions = AllVersions::read_versions(&vec![&hostname], &vec![&port], 1).await;

        // each daemon should return one row.
        assert!(allversions.versions.len() == 1);
    }
}
