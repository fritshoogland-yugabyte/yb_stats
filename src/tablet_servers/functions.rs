//! The impls and functions
//!
use chrono::Local;
use std::{sync::mpsc::channel, time::Instant};
use log::*;
use colored::*;
use anyhow::Result;
use crate::isleader::AllIsLeader;
use crate::utility;
use crate::snapshot;
use crate::tablet_servers::{TabletServers, AllTabletServers, TabletServersDiff, TabletServersDiffFields};
use crate::Opts;

impl TabletServers {
    pub fn new() -> Self {
        Default::default()
    }
}

impl AllTabletServers {
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

        let alltabletservers = AllTabletServers::read_tabletservers(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number,"tablet_servers", alltabletservers.tabletservers)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub async fn read_tabletservers(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllTabletServers
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
                        let mut tablet_servers = AllTabletServers::read_http(host, port);
                        tablet_servers.tabletservers.iter_mut().for_each(|(_,v)| v.timestamp = Some(detail_snapshot_time));
                        tablet_servers.tabletservers.iter_mut().for_each(|(_,v)| v.hostname_port = Some(format!("{}:{}", host, port)));
                        tablet_servers.tabletservers.iter_mut().for_each(|(k,v)| v.tablet_server_hostname_port = Some(k.to_string()));
                        tx.send(tablet_servers).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut alltabletservers = AllTabletServers::new();

        for fetched_tabletservers in rx
        {
            for (_, tabletserver_data) in fetched_tabletservers.tabletservers
            {
                alltabletservers.tabletservers.push(tabletserver_data);
            }
        }

        alltabletservers
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> TabletServers
    {
        let data_from_http = utility::http_get(host, port, "api/v1/tablet-servers");
        AllTabletServers::parse_tabletservers(data_from_http, host, port)
    }
    fn parse_tabletservers(
        http_data: String,
        host: &str,
        port: &str,
    ) -> TabletServers {
        serde_json::from_str(&http_data)
            .unwrap_or_else(|e| {
                debug!("({}:{}) could not parse /api/v1/tablet-servers json data for masters, error: {}", host, port, e);
                TabletServers::new()
            })
    }
    pub fn print(
        &self,
        details_enable: &bool,
        leader_hostname: String,
    ) -> Result<()>
    {
        for row in &self.tabletservers {
            // if details_enable is true then always continue
            // if details_enable is false, then hostname_port must be equal to leader_hostname,
            // so only the masters information from the master leader is printed.
            if row.hostname_port != Some(leader_hostname.clone())
                && !*details_enable
            {
                continue;
            }
            if *details_enable
            {
                print!("{} ", row.hostname_port.as_ref().expect("hostname:port should be set"));
            };
            print!("{:20} ", row.tablet_server_hostname_port.clone().expect("tablet-server hostname:port should be set"));
            // this highlights the status row, which makes it easier to spot non-alive (dead) servers.
            if row.status == "ALIVE"
            {
                print!("{} ", row.status.green());
            }
            else
            {
                print!("{} ", row.status.red());
            };
            println!("Placement: {}.{}.{}",
                     row.cloud,
                     row.region,
                     row.zone
            );
            if *details_enable
            {
                print!("{} ", row.hostname_port.as_ref().expect("hostname:port should be set"));
            };
            println!("{} HB time: {}, Uptime: {}, Ram {}",
                     " ".repeat(20),
                     row.time_since_hb,
                     row.uptime_seconds,
                     row.ram_used
            );
            if *details_enable
            {
                print!("{} ", row.hostname_port.as_ref().expect("hostname:port should be set"));
            };
            println!("{} SST files: nr: {}, size: {}, uncompressed: {}",
                     " ".repeat(20),
                     row.num_sst_files,
                     row.total_sst_file_size,
                     row.uncompressed_sst_file_size
            );
            if *details_enable
            {
                print!("{} ", row.hostname_port.as_ref().expect("hostname:port should be set"));
            };
            println!("{} ops read: {}, write: {}",
                     " ".repeat(20),
                     row.read_ops_per_sec,
                     row.write_ops_per_sec
            );
            if *details_enable
            {
                print!("{} ", row.hostname_port.as_ref().expect("hostname:port should be set"));
            };
            println!("{} tablets: active: {}, user (leader/total): {}/{}, system (leader/total): {}/{}",
                     " ".repeat(20),
                     row.active_tablets,
                     row.user_tablets_leaders,
                     row.user_tablets_total,
                     row.system_tablets_leaders,
                     row.system_tablets_total
            );
            for path_metric in row.path_metrics.iter()
            {
                if *details_enable
                {
                    print!("{} ", row.hostname_port.as_ref().expect("hostname:port should be set"));
                };
                println!("{} Path: {}, total: {}, used: {} ({:.2}%)",
                         " ".repeat(20),
                         path_metric.path,
                         path_metric.total_space_size,
                         path_metric.space_used,
                         (path_metric.space_used as f64 / path_metric.total_space_size as f64) * 100.0);
            }
        }
        Ok(())
    }
}

impl TabletServersDiff {
    pub fn new() -> Self { Default::default() }
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
    ) -> Result<TabletServersDiff>
    {
        let mut tabletserversdiff = TabletServersDiff::new();

        let mut tabletservers = AllTabletServers::new();
        tabletservers.tabletservers = snapshot::read_snapshot_json(begin_snapshot, "tablet_servers")?;
        let master_leader = AllIsLeader::return_leader_snapshot(begin_snapshot)?;
        tabletserversdiff.first_snapshot(tabletservers, master_leader);

        let mut tabletservers = AllTabletServers::new();
        tabletservers.tabletservers = snapshot::read_snapshot_json(end_snapshot, "tablet_servers")?;
        let master_leader = AllIsLeader::return_leader_snapshot(end_snapshot)?;
        tabletserversdiff.second_snapshot(tabletservers, master_leader);

        Ok(tabletserversdiff)
    }
    fn first_snapshot(
        &mut self,
        tablet_servers: AllTabletServers,
        master_leader: String,
    )
    {
        if master_leader == *"" {
            self.master_found = false;
            return
        } else {
            self.master_found = true;
        }

        for tablet_server in tablet_servers.tabletservers
            .iter()
            .filter(|r| r.hostname_port == Some(master_leader.clone()))
        {
            self.btreetabletserversdiff
                .entry(tablet_server.tablet_server_hostname_port.clone().expect("tablet server hostname port should be set"))
                .and_modify(|_| error!("Duplicate tablet server hostname port: {}", tablet_server.tablet_server_hostname_port.clone().expect("tablet server hostname port should be set")))
                .or_insert( TabletServersDiffFields {
                    first_status: tablet_server.status.clone(),
                    first_uptime_seconds: tablet_server.uptime_seconds,
                    ..Default::default()
                });
        }
    }
    fn second_snapshot(
        &mut self,
        tablet_servers: AllTabletServers,
        master_leader: String,
    )
    {
        if master_leader == *""
        {
            self.master_found = false;
            return
        }
        else
        {
            self.master_found = true;
        }
        for tablet_server in tablet_servers.tabletservers
            .iter()
            .filter(|r| r.hostname_port == Some(master_leader.clone()))
        {
            self.btreetabletserversdiff
                .entry(tablet_server.tablet_server_hostname_port.clone().expect("tablet server hostname port shoud be set"))
                .and_modify(|tabletserverfields| {
                    tabletserverfields.second_status = tablet_server.status.clone();
                    tabletserverfields.second_uptime_seconds = tablet_server.uptime_seconds;
                })
                .or_insert( TabletServersDiffFields {
                    second_status: tablet_server.status.clone(),
                    second_uptime_seconds: tablet_server.uptime_seconds,
                    ..Default::default()
                });
        }
    }
    pub fn print(
        &self,
    )
    {
        if ! self.master_found
        {
            println!("Master leader was not found in hosts specified, skipping tablet servers diff.");
            return;
        }
        for (hostname, status) in self.btreetabletserversdiff.iter() {
            // If first and second snapshot status fields are identical,
            // and the first_uptime is lesser than or equal to the second uptime,
            // there is no indication anything extraordinary has happened.
            //
            // What we want to be notified of is if the status has changed,
            // or if the first uptime is higher than the second uptime,
            // indicating a tablet server restart.
            if status.first_status == status.second_status
                && status.first_uptime_seconds <= status.second_uptime_seconds
            {
                continue;
            }
            if status.second_status == *""
            {
                println!("{} Tserver:  {}, status: {}, uptime: {} s", "-".to_string().red(), hostname, status.first_status, status.first_uptime_seconds);
            }
            else if status.first_status == *""
            {
                println!("{} Tserver:  {}, status: {}, uptime: {} s", "+".to_string().green(), hostname, status.second_status, status.second_uptime_seconds);
            }
            else
            {
                print!("{} Tserver:  {}, ", "*".to_string().yellow(), hostname);
                if status.first_status != status.second_status
                {
                    print!("status: {}->{}, ", status.first_status.to_string().yellow(), status.second_status.to_string().yellow());
                }
                else
                {
                    print!("status: {}, ", status.first_status);
                };
                if status.second_uptime_seconds < status.first_uptime_seconds
                {
                    println!("uptime: {}->{}", status.first_uptime_seconds.to_string().yellow(), status.second_uptime_seconds.to_string().yellow());
                }
                else
                {
                    println!("uptime: {}", status.second_uptime_seconds);
                };
            };
        }
    }
    pub async fn adhoc_read_first_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let alltabletservers = AllTabletServers::read_tabletservers(hosts, ports, parallel).await;
        let master_leader = AllIsLeader::return_leader_http(hosts, ports, parallel).await;
        self.first_snapshot(alltabletservers, master_leader);
    }
    pub async fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let alltabletservers = AllTabletServers::read_tabletservers(hosts, ports, parallel).await;
        let master_leader = AllIsLeader::return_leader_http(hosts, ports, parallel).await;
        self.second_snapshot(alltabletservers, master_leader);
    }
}

pub async fn print_tablet_servers(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    match options.print_tablet_servers.as_ref().unwrap() {
        Some(snapshot_number) => {

            let mut alltabletservers = AllTabletServers::new();
            alltabletservers.tabletservers = snapshot::read_snapshot_json(snapshot_number, "tablet_servers")?;
            let leader_hostname = AllIsLeader::return_leader_snapshot(snapshot_number)?;

            alltabletservers.print(&options.details_enable, leader_hostname)?;

        }
        None => {

            let alltabletservers = AllTabletServers::read_tabletservers(&hosts, &ports, parallel).await;
            let leader_hostname = AllIsLeader::return_leader_http(&hosts, &ports, parallel).await;

            alltabletservers.print(&options.details_enable, leader_hostname)?;

        }
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_tabletserver_data() {
        let json = r#"
{
    "":
    {
        "yb-2.local:9000":
        {
            "time_since_hb": "0.8s",
            "time_since_hb_sec": 0.82917897,
            "status": "ALIVE",
            "uptime_seconds": 1517,
            "ram_used": "35.00 MB",
            "ram_used_bytes": 34996224,
            "num_sst_files": 3,
            "total_sst_file_size": "199.74 KB",
            "total_sst_file_size_bytes": 199735,
            "uncompressed_sst_file_size": "200.24 KB",
            "uncompressed_sst_file_size_bytes": 200238,
            "path_metrics":
            [
                {
                    "path": "/mnt/d0",
                    "space_used": 760074240,
                    "total_space_size": 10724835328
                }
            ],
            "read_ops_per_sec": 0,
            "write_ops_per_sec": 0,
            "user_tablets_total": 3,
            "user_tablets_leaders": 1,
            "system_tablets_total": 12,
            "system_tablets_leaders": 4,
            "active_tablets": 15,
            "cloud": "local",
            "region": "local",
            "zone": "local2"
        }
    }
}
        "#.to_string();
        let result = AllTabletServers::parse_tabletservers(json, "", "");
        for (_servername, serverstatus) in result.tabletservers.iter() {
            assert_eq!(serverstatus.status, "ALIVE");
        }
    }

    #[tokio::test]
    async fn integration_parse_tabletserver() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let alltabletservers = AllTabletServers::read_tabletservers(&vec![&hostname], &vec![&port], 1).await;

        assert!(!alltabletservers.tabletservers.is_empty());
    }
}