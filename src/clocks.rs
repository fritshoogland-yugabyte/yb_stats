use chrono::{DateTime, Local};
use std::{sync::mpsc::channel, time::Instant};
use serde_derive::{Serialize,Deserialize};
//use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use log::*;
use soup::prelude::*;
use anyhow::Result;
use crate::isleader::AllStoredIsLeader;
use crate::utility::{scan_host_port, http_get};
use crate::snapshot::save_snapshot;

#[derive(Debug)]
pub struct Clocks {
    pub server: String,
    pub time_since_heartbeat: String,
    pub status_uptime: String,
    pub physical_time_utc: String,
    pub hybrid_time_utc: String,
    pub heartbeat_rtt: String,
    pub cloud: String,
    pub region: String,
    pub zone: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredClocks {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub server: String,
    pub time_since_heartbeat: String,
    pub status_uptime: String,
    pub physical_time_utc: String,
    pub hybrid_time_utc: String,
    pub heartbeat_rtt: String,
    pub cloud: String,
    pub region: String,
    pub zone: String,
}

#[derive(Debug, Default)]
pub struct AllStoredClocks {
    pub stored_clocks: Vec<StoredClocks>
}

impl AllStoredClocks {
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize ,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allstoredclocks = AllStoredClocks::read_clocks(hosts, ports, parallel).await?;
        save_snapshot(snapshot_number, "clocks", allstoredclocks.stored_clocks)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub fn new() -> Self { Default::default() }
    pub async fn read_clocks (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize
    ) -> Result<AllStoredClocks>
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
                        let clocks = AllStoredClocks::read_http(host, port);
                        tx.send((format!("{}:{}", host, port), detail_snapshot_time, clocks)).expect("error sending data via tx (clocks)");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allstoredclocks = AllStoredClocks::new();

        for (hostname_port, detail_snapshot_time, clocks) in rx {
            for clock in clocks {
                allstoredclocks.stored_clocks.push(StoredClocks {
                    hostname_port: hostname_port.to_string(),
                    timestamp: detail_snapshot_time,
                    server: clock.server.to_string(),
                    time_since_heartbeat: clock.time_since_heartbeat.to_string(),
                    status_uptime: clock.status_uptime.to_string(),
                    physical_time_utc: clock.physical_time_utc.to_string(),
                    hybrid_time_utc: clock.hybrid_time_utc.to_string(),
                    heartbeat_rtt: clock.heartbeat_rtt.to_string(),
                    cloud: clock.cloud.to_string(),
                    region: clock.region.to_string(),
                    zone: clock.zone.to_string(),
                });
            }
        }
        Ok(allstoredclocks)
    }
    fn read_http(
        host: &str,
        port: &str,
    ) -> Vec<Clocks>
    {
        let data_from_http = if scan_host_port(host, port) {
            http_get(host, port, "tablet-server-clocks?raw")
        } else {
            String::new()
        };
        AllStoredClocks::parse_clocks(data_from_http)
    }
    fn parse_clocks(
        http_data: String,
    ) -> Vec<Clocks>
    {
        let mut clocks: Vec<Clocks> = Vec::new();
        if let Some(table) = AllStoredClocks::find_table(&http_data)
        {
            let (headers, rows) = table;

            let try_find_header = |target| headers.iter().position(|h| h == target);

            let server_pos = try_find_header("Server");
            let time_since_heartbeat_pos = try_find_header("Time since <br>heartbeat");
            let status_uptime_pos = try_find_header("Status &amp; Uptime");
            let physical_time_utc_pos = try_find_header("Physical Time (UTC)");
            let hybrid_time_utc_pos = try_find_header("Hybrid Time (UTC)");
            let heartbeat_rtt_pos = try_find_header("Heartbeat RTT");
            let cloud_pos = try_find_header("Cloud");
            let region_pos = try_find_header("Region");
            let zone_pos = try_find_header("Zone");

            let take_or_missing = |row: &mut [String], pos: Option<usize>|
                match pos.and_then(|pos| row.get_mut(pos))
                {
                    Some(value) => std::mem::take(value),
                    None => "<Missing>".to_string(),
                };

            //let mut stack_from_table = String::from("Initial value: this should not be visible");
            for mut row in rows
            {
                // this is a way to remove some html from the result.
                // not sure if this is the best way, but it fits the purpose.
                let parse = Soup::new(&take_or_missing(&mut row, server_pos));

                clocks.push(Clocks {
                    //server: take_or_missing(&mut row, server_pos),
                    server: parse.text(),
                    time_since_heartbeat: take_or_missing(&mut row, time_since_heartbeat_pos),
                    status_uptime: take_or_missing(&mut row, status_uptime_pos),
                    physical_time_utc: take_or_missing(&mut row, physical_time_utc_pos),
                    hybrid_time_utc: take_or_missing(&mut row, hybrid_time_utc_pos),
                    heartbeat_rtt: take_or_missing(&mut row, heartbeat_rtt_pos),
                    cloud: take_or_missing(&mut row, cloud_pos),
                    region: take_or_missing(&mut row, region_pos),
                    zone: take_or_missing(&mut row, zone_pos),
                });
            }
        }
        clocks
    }
    fn find_table(http_data: &str) -> Option<(Vec<String>, Vec<Vec<String>>)>
    {
        let css = |selector| Selector::parse(selector).unwrap();
        let get_cells = |row: ElementRef, selector| {
            row.select(&css(selector))
                .map(|cell| cell.inner_html().trim().to_string())
                .collect()
        };
        let html = Html::parse_fragment(http_data);
        let table = html.select(&css("table")).next()?;
        let tr = css("tr");
        let mut rows = table.select(&tr);
        let headers = get_cells(rows.next()?, "th");
        let rows: Vec<_> = rows.map(|row| get_cells(row, "td")).collect();
        Some((headers, rows))
    }
    pub fn print(
        &self,
        snapshot_number: &String,
        details_enable: &bool,
    ) -> Result<()>
    {
        info!("print tablet server clocks");

        let leader_hostname = AllStoredIsLeader::return_leader_snapshot(snapshot_number)?;

        for row in &self.stored_clocks {
            if row.hostname_port == leader_hostname
                && !*details_enable
            {
                println!("{} {} {} {} {} {} {} {} {}", row.server, row.time_since_heartbeat, row.status_uptime, row.physical_time_utc, row.hybrid_time_utc, row.heartbeat_rtt, row.cloud, row.region, row.zone);
            }
            if *details_enable
            {
                println!("{}: {} {} {} {} {} {} {} {} {}", row.hostname_port, row.server, row.time_since_heartbeat, row.status_uptime, row.physical_time_utc, row.hybrid_time_utc, row.heartbeat_rtt, row.cloud, row.region, row.zone);
            }
        }
        Ok(())
    }
    pub async fn print_adhoc(
        &self,
        details_enable: &bool,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> Result<()>
    {
        info!("print adhoc tablet servers clocks");

        let leader_hostname = AllStoredIsLeader::return_leader_http(hosts, ports, parallel).await;

        for row in &self.stored_clocks {
            if row.hostname_port == leader_hostname
                && !*details_enable
            {
                println!("{} {} {} {} {} {} {} {} {}", row.server, row.time_since_heartbeat, row.status_uptime, row.physical_time_utc, row.hybrid_time_utc, row.heartbeat_rtt, row.cloud, row.region, row.zone);
            }
            if *details_enable
            {
                println!("{} {} {} {} {} {} {} {} {} {}", row.hostname_port, row.server, row.time_since_heartbeat, row.status_uptime, row.physical_time_utc, row.hybrid_time_utc, row.heartbeat_rtt, row.cloud, row.region, row.zone);
            }
        }
        Ok(())
    }
    pub async fn print_adhoc_latency(
        &self,
        details_enable: &bool,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> Result<()>
    {
        info!("print adhoc tablet servers clocks latency");

        let leader_hostname = AllStoredIsLeader::return_leader_http(hosts, ports, parallel).await;

        for row in &self.stored_clocks {
            if row.hostname_port == leader_hostname
                && !*details_enable
            {
                println!("{} -> {}: {} RTT ({} {} {})", leader_hostname.clone(), row.server.split_whitespace().next().unwrap_or_default(), row.heartbeat_rtt, row.cloud, row.region, row.zone);
            }
            if *details_enable
            {
                println!("{} {} -> {}: {} RTT ({} {} {})", row.hostname_port, leader_hostname.clone(), row.server.split_whitespace().next().unwrap_or_default(), row.heartbeat_rtt, row.cloud, row.region, row.zone);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utility_test::*;

    #[test]
    fn unit_parse_clocks_data() {
        // This is what /tablet-server-clocks?raw returns for an RF3 cluster.
        let threads = r#"<h2>Tablet Servers</h2>
<table class='table table-striped'>
    <tr>
      <th>Server</th>
      <th>Time since </br>heartbeat</th>
      <th>Status & Uptime</th>
      <th>Physical Time (UTC)</th>
      <th>Hybrid Time (UTC)</th>
      <th>Heartbeat RTT</th>
      <th>Cloud</th>
      <th>Region</th>
      <th>Zone</th>
    </tr>
  <tr>
  <td><a href="http://yb-1.local:9000/">yb-1.local:9000</a></br>  fa8b3f29b2a54eadb73ae546454ce1bb</td><td>1.0s</td>    <td style="color:Green">ALIVE: 4:20:46</td>    <td>2022-12-13 15:29:50.817063</td>    <td>2022-12-13 15:29:50.817063</td>    <td>1.56ms</td>    <td>local</td>    <td>local</td>    <td>local1</td>  </tr>
  <tr>
  <td><a href="http://yb-2.local:9000/">yb-2.local:9000</a></br>  15549111cb3448359d4f34a81880eedd</td><td>0.3s</td>    <td style="color:Green">ALIVE: 4:20:46</td>    <td>2022-12-13 15:29:51.481450</td>    <td>2022-12-13 15:29:51.481450</td>    <td>1.43ms</td>    <td>local</td>    <td>local</td>    <td>local2</td>  </tr>
  <tr>
  <td><a href="http://yb-3.local:9000/">yb-3.local:9000</a></br>  f54d6bef7e87407597df67ba7ea59892</td><td>0.8s</td>    <td style="color:Green">ALIVE: 4:20:45</td>    <td>2022-12-13 15:29:50.983270</td>    <td>2022-12-13 15:29:50.983270</td>    <td>0.62ms</td>    <td>local</td>    <td>local</td>    <td>local3</td>  </tr>
</table>
<h3>Tablet-Peers by Availability Zone</h3>
<table class='table table-striped'>
  <tr>
    <th>Cloud</th>
    <th>Region</th>
    <th>Zone</th>
    <th>Total Nodes</th>
    <th>User Tablet-Peers / Leaders</th>
    <th>System Tablet-Peers / Leaders</th>
    <th>Active Tablet-Peers</th>
  </tr>
<tr>
  <td rowspan="3">local</td>
  <td rowspan="3">local</td>
  <td>local1</td>
  <td>1</td>
  <td>8 / 2</td>
  <td>12 / 4</td>
  <td>20</td>
</tr>
<tr>
  <td>local2</td>
  <td>1</td>
  <td>8 / 3</td>
  <td>12 / 4</td>
  <td>20</td>
</tr>
<tr>
  <td>local3</td>
  <td>1</td>
  <td>8 / 3</td>
  <td>12 / 4</td>
  <td>20</td>
</tr>
</table>"#.to_string();
        let result = AllStoredClocks::parse_clocks(threads);
        // this results in 33 Threads
        assert_eq!(result.len(), 3);

        assert_eq!(result[0].server, "yb-1.local:9000  fa8b3f29b2a54eadb73ae546454ce1bb");
        assert_eq!(result[0].time_since_heartbeat, "1.0s");
        assert_eq!(result[0].status_uptime, "ALIVE: 4:20:46");
        assert_eq!(result[0].physical_time_utc, "2022-12-13 15:29:50.817063");
        assert_eq!(result[0].hybrid_time_utc, "2022-12-13 15:29:50.817063");
        assert_eq!(result[0].heartbeat_rtt, "1.56ms");
        assert_eq!(result[0].cloud, "local");
        assert_eq!(result[0].region, "local");
        assert_eq!(result[0].zone, "local1");
    }

    #[tokio::test]
    async fn integration_parse_clocks() -> Result<()> {
        let hostname = get_hostname_master();
        let port = get_port_master();

        let allstoredclocks = AllStoredClocks::read_clocks(&vec![&hostname], &vec![&port], 1_usize).await?;

        assert!(allstoredclocks.stored_clocks.len() > 0);

        Ok(())
    }
}
