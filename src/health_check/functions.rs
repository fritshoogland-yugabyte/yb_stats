//! The impls and functions
//!
use chrono::Local;
use std::{sync::mpsc::channel, time::Instant};
use log::*;
use anyhow::{Result, Context};
use crate::isleader::AllIsLeader;
use crate::utility;
use crate::snapshot;
use crate::health_check::{AllHealthCheck, Health_Check};
use crate::Opts;

impl Health_Check {
    pub fn new() -> Self {
        Default::default()
    }
}

impl AllHealthCheck {
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

        let allhealth_check = AllHealthCheck::read_health_check(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "health-check", allhealth_check.health_check)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub async fn read_health_check(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllHealthCheck
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
                        let mut health_check = AllHealthCheck::read_http(host, port);
                        health_check.timestamp = Some(detail_snapshot_time);
                        health_check.hostname_port = Some(format!("{}:{}", host, port));
                        tx.send(health_check).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allhealthcheck = AllHealthCheck::new();

        //.iter().filter(|r| r.most_recent_uptime > Some(0) ) {
        for healthcheck in rx
        {
            allhealthcheck.health_check.push(healthcheck);
        }

        allhealthcheck
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> Health_Check
    {
        let data_from_http = utility::http_get(host, port, "api/v1/health-check");
        AllHealthCheck::parse_health_check(data_from_http, host, port)
    }
    fn parse_health_check(
        http_data: String,
        host: &str,
        port: &str,
    ) -> Health_Check
    {
        serde_json::from_str(&http_data)
            .unwrap_or_else(|e|
            {
                debug!("({}:{}) could not parse /api/v1/health-check json data, error: {}", host, port, e);
                Health_Check::new()
            })
    }
    pub fn print(
        &self,
        leader_hostname: String
    ) -> Result<()>
    {

        println!("{}", serde_json::to_string_pretty( &self.health_check
            .iter()
            .find(|r| r.hostname_port == Some(leader_hostname.clone()))
            .with_context(|| "Unable to find current master leader")?
        )?);
        Ok(())
    }
    /*
    pub fn get_dead_nodes_and_under_replicated_tablets(
        &self,
        leader_hostname: String,
    ) -> Result<(Vec<String>, Vec<String>)>
    {
        self.health_check
            .iter()
            .find(|r| r.hostname_port == Some(leader_hostname.clone()))
            .map(|r| (r.dead_nodes.as_ref().unwrap_or(&Vec::new()).to_owned(), r.under_replicated_tablets.as_ref().unwrap_or(&Vec::new()).to_owned()))
            .with_context(|| "Unable to find current master leader")
    }

     */
    pub async fn return_dead_nodes_and_under_replicated_tablets_http(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
        leader_hostname: &str,
    ) -> Result<(Vec<String>, Vec<String>)>
    {
        let allhealthcheck = AllHealthCheck::read_health_check(hosts, ports, parallel).await;
        allhealthcheck.health_check
            .iter()
            .find(|r| r.hostname_port == Some(leader_hostname.to_owned()))
            .map(|r| (r.dead_nodes.as_ref().unwrap_or(&Vec::new()).to_owned(), r.under_replicated_tablets.as_ref().unwrap_or(&Vec::new()).to_owned()))
            .with_context(|| "Unable to find current master leader")
    }
    pub fn return_dead_nodes_and_under_replicated_tablets_snapshot (
        snapshot_number: &String,
        leader_hostname: &str,
    ) -> Result<(Vec<String>, Vec<String>)>
    {
        let mut allhealthcheck = AllHealthCheck::new();
        allhealthcheck.health_check = snapshot::read_snapshot_json(snapshot_number, "health-check")?;
        allhealthcheck.health_check
            .iter()
            .find(|r| r.hostname_port == Some(leader_hostname.to_owned()))
            .map(|r| (r.dead_nodes.as_ref().unwrap_or(&Vec::new()).to_owned(), r.under_replicated_tablets.as_ref().unwrap_or(&Vec::new()).to_owned()))
            .with_context(|| "Unable to find the master leader")
    }
}

pub async fn print_health_check(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    match options.print_health_check.as_ref().unwrap() {
        Some(snapshot_number) => {

            let mut allhealthcheck = AllHealthCheck::new();
            allhealthcheck.health_check = snapshot::read_snapshot_json(snapshot_number, "health-check")?;
            let leader_hostname = AllIsLeader::return_leader_snapshot(snapshot_number)?;

            allhealthcheck.print(leader_hostname)?;

        }
        None => {
            let allhealthcheck = AllHealthCheck::read_health_check(&hosts, &ports, parallel).await;
            let leader_hostname = AllIsLeader::return_leader_http(&hosts, &ports, parallel).await;
            allhealthcheck.print(leader_hostname)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_simple() {
        let json = r#"
{
    "version":0,
    "cluster_uuid":"6cfdbce0-b98d-4aed-a5ec-372a726258b2"
}
        "#.to_string();
        let result = AllHealthCheck::parse_health_check(json, "", "");
        //println!("{:#?}", result);
        assert_eq!(result.version, 0);
        assert_eq!(result.cluster_uuid, "6cfdbce0-b98d-4aed-a5ec-372a726258b2");
    }

    #[test]
    fn unit_parse_lists_with_data() {
        let json = r#"
{
  "dead_nodes": [
    "b71db83686bb4f22a673875321d2499b",
    "26ab4e25230b462890c878c96c317baf"
  ],
  "most_recent_uptime": 60497,
  "under_replicated_tablets": [
    "3563bc4d087346908b9ac4081449d6bb",
    "c1680a6d943641fe97c14d66406b4080",
    "78bc9ba9274041b989a431f0ff013da3",
    "45ee06bb90bd484b9f373b5d0852ad8c",
    "ec848b92516245b7955bc9234cfa9f43",
    "185027673bec4578ad2a9b97c95ea759",
    "b1bf3f816d414b1da29d74693549c0f2",
    "5520cb4ebf1e4728b4ee2cd2acaf7f66",
    "cf3161152e994bba937d0bcab94b94f9",
    "7c6d7dc9d23445b9862bd6a3485330e3",
    "1b2ccba72b0b455e91df61b3563750b7",
    "d9bb5db8d34648fa86377da76b28227a"
  ]
}
        "#.to_string();
        let _result = AllHealthCheck::parse_cluster_config(json, "", "");
        //println!("{:#?}", result);
        //assert!(result.version, 0);
        //assert!(result.cluster_uuid, "6cfdbce0-b98d-4aed-a5ec-372a726258b2");
    }
    #[tokio::test]
    async fn integration_parse_master_health_check() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let allhealthcheck = AllHealthCheck::read_health_check(&vec![&hostname], &vec![&port], 1).await;

        assert!(!allhealthcheck.health_check.is_empty());
    }
    #[tokio::test]
    async fn integration_parse_tablet_server_health_check() {
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();

        let allhealthcheck = AllHealthCheck::read_health_check(&vec![&hostname], &vec![&port], 1).await;

        assert!(!allhealthcheck.health_check.is_empty());
    }

}
