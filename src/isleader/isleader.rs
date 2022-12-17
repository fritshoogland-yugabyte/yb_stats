//! The module for reading the /api/v1/is-leader to identify the master leader.
//!
//! The functionality for isleader has the following public entries:
//!  1. Snapshot creation: [AllStoredIsLeader::perform_snapshot]
//!  2. Provide the master hostname:port for a given snapshot: [AllStoredIsLeader::return_leader]
//!
//! This function has no public display function, it is only used to store the and retrieve the master leader.
use chrono::Local;
use std::{time::Instant, sync::mpsc::channel};
//use serde_derive::{Serialize,Deserialize};
use log::*;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::isleader::{AllStoredIsLeader, StoredIsLeader, IsLeader};

impl AllStoredIsLeader {
    /// This function reads all the host/port combinations for metrics and saves these in a snapshot indicated by the snapshot_number.
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allstoredisleader = AllStoredIsLeader::read_isleader(hosts, ports, parallel).await;
        snapshot::save_snapshot(snapshot_number, "isleader", allstoredisleader.stored_isleader)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    fn new() -> Self {
        Default::default()
    }
    /// This function requires a snapshot number, and returns the hostname_port of the master leader.
    pub fn return_leader_snapshot (
       snapshot_number: &String
    ) -> Result<String>
    {
        let mut stored_isleader = AllStoredIsLeader::new();
        stored_isleader.stored_isleader = snapshot::read_snapshot(snapshot_number, "isleader")?;
        Ok(stored_isleader.stored_isleader.iter().filter(|r| r.status == "OK").map(|r| r.hostname_port.to_string()).next().unwrap())
    }
    pub async fn return_leader_http (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> String
    {
        let allstoredisleader = AllStoredIsLeader::read_isleader(hosts, ports, parallel).await;
        allstoredisleader.stored_isleader.iter().filter(|r| r.status == "OK").map(|r| r.hostname_port.to_string()).next().unwrap_or_default()
    }
    /// This function takes a vector of hosts and ports, and the allowed parallellism to (try to) read /api/v1/is-leader.
    /// It creates a threadpool based on parallel, and spawns a task for reading and parsing for all host-port combinations.
    /// When all combinations are read, the results are gathered in Vec<AllStoredIsLeader> and returned.
    async fn read_isleader (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize
    ) -> AllStoredIsLeader
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
                        let isleader = AllStoredIsLeader::read_http(host, port);
                        debug!("{:?}",&isleader);
                        tx.send((format!("{}:{}", host, port), detail_snapshot_time, isleader)).expect("error sending data via tx (isleader)");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allstoredisleader = AllStoredIsLeader { stored_isleader: Vec::new() };
        for (hostname_port, detail_snapshot_time, isleader) in rx {
            debug!("hostname_port: {}, timestamp: {}, isleader: {}", &hostname_port, &detail_snapshot_time, &isleader.status);
            allstoredisleader.stored_isleader.push(StoredIsLeader { hostname_port, timestamp: detail_snapshot_time, status: isleader.status.to_string() } );
        }
        allstoredisleader
    }
    /// Using provided host and port, read http://host:port/api/v1/is-leader and parse the result
    /// via [AllStoredIsLeader::parse_isleader], and return struct [IsLeader].
    fn read_http(
        host: &str,
        port: &str,
    ) -> IsLeader
    {
        let data_from_http = if utility::scan_host_port( host, port) {
            utility::http_get(host, port, "api/v1/is-leader")
        } else {
            String::new()
        };
        AllStoredIsLeader::parse_isleader(data_from_http)
    }
    /// This function parses the http output.
    /// This is a separate function in order to allow integration tests to use it.
    fn parse_isleader( http_output: String ) -> IsLeader
    {
        serde_json::from_str( &http_output )
            .unwrap_or_else(|_e| {
                IsLeader { status: "".to_string() }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    //use crate::utility_test::*;

    #[test]
    fn unit_parse_status_ok() {
        // This is what /api/v1/is-leader returns by the master, on the master leader.
        let version = r#"
        {"STATUS":"OK"}
"#.to_string();
        let result = AllStoredIsLeader::parse_isleader(version);
        assert_eq!(result.status, "OK");
    }

    #[test]
    fn unit_parse_status_empty() {
        // This is what /api/v1/is-leader returns by the master, NOT on the master leader.
        let version = r#"
"#.to_string();
        let result = AllStoredIsLeader::parse_isleader(version);
        assert_eq!(result.status, "");
    }

    #[test]
    fn unit_parse_endpoint_does_not_exist() {
        // This is what /api/v1/is-leader returns by requesting not the master.
        // it does return http 404.
        let version = r#"
Error 404: Not Found
File not found
"#.to_string();
        let result = AllStoredIsLeader::parse_isleader(version);
        assert_eq!(result.status, "");
    }

    #[tokio::test]
    async fn integration_find_master_leader() {

        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let leader = AllStoredIsLeader::return_leader_http(&vec![&hostname], &vec![&port], 1_usize).await;
        assert!(leader.is_empty())
    }
}