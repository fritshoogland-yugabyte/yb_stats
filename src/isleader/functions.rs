//! The impls and functions.
//!
use chrono::Local;
use std::{time::Instant, sync::mpsc::channel};
use log::*;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::isleader::{AllIsLeader, IsLeader};

impl IsLeader {
    fn new() -> Self { Default::default() }
}
impl AllIsLeader {
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allisleader = AllIsLeader::read_isleader(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "isleader", allisleader.isleader)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    fn new() -> Self {
        Default::default()
    }
    pub fn return_leader_snapshot (
       snapshot_number: &String
    ) -> Result<String>
    {
        let mut allisleader = AllIsLeader::new();
        allisleader.isleader = snapshot::read_snapshot_json(snapshot_number, "isleader")?;
        // please note the expect() is necessary to unwrap the option/some()
        // unwrap_or_default() allows to obtain data with '--details-enable' even if the master leader cannot be found
        Ok(allisleader.isleader.iter()
            .find(|r| r.status == "OK")
            .map(|r| r.hostname_port.as_ref().expect("None found").to_string())
            .unwrap_or_default()
        )
    }
    pub async fn return_leader_http (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> String
    {
        let allisleader = AllIsLeader::read_isleader(hosts, ports, parallel).await;
        // please note the expect() is necessary to unwrap the option/some()
        // unwrap_or_default() allows to obtain data with '--details-enable' even if the master leader cannot be found
        allisleader.isleader.iter()
            .find(|r| r.status == "OK")
            .map(|r| r.hostname_port.as_ref().expect("None found").to_string())
            .unwrap_or_default()
    }
    async fn read_isleader (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize
    ) -> AllIsLeader
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
                        let mut isleader = AllIsLeader::read_http(host, port);
                        isleader.timestamp = Some(detail_snapshot_time);
                        isleader.hostname_port = Some(format!("{}:{}", host, port));
                        debug!("{:?}",&isleader);
                        tx.send(isleader).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allisleader = AllIsLeader::new();

        // the filter on the mpsc rx channel filter emptiness of the status field,
        // indicating the source was not a master leader or master.
        for isleader in rx.iter().filter(|r| !r.status.is_empty()) {
            allisleader.isleader.push(isleader);
        }
        allisleader
    }
    fn read_http(
        host: &str,
        port: &str,
    ) -> IsLeader
    {
        let data_from_http = utility::http_get(host, port, "api/v1/is-leader");
        AllIsLeader::parse_isleader(data_from_http)
    }
    // This function parses the http output.
    // This is a separate function in order to allow integration tests to use it.
    fn parse_isleader( http_output: String ) -> IsLeader
    {
        serde_json::from_str( &http_output )
            .unwrap_or_else(|_e| {
                IsLeader::new()
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
        let result = AllIsLeader::parse_isleader(version);
        assert_eq!(result.status, "OK");
    }

    #[test]
    fn unit_parse_status_empty() {
        // This is what /api/v1/is-leader returns by the master, NOT on the master leader.
        let version = r#"
"#.to_string();
        let result = AllIsLeader::parse_isleader(version);
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
        let result = AllIsLeader::parse_isleader(version);
        assert_eq!(result.status, "");
    }

    #[tokio::test]
    async fn integration_find_master_leader() {

        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let leader = AllIsLeader::return_leader_http(&vec![&hostname], &vec![&port], 1_usize).await;
        assert!(!leader.is_empty())
    }
}