//! The impls and functions
//!
use chrono::Local;
use std::{sync::mpsc::channel, time::Instant};
use scraper::{Html, Selector};
use log::*;
use anyhow::Result;
use crate::isleader::AllIsLeader;
use crate::utility;
use crate::snapshot;
use crate::clocks::{AllClocks, Clocks};
use crate::Opts;

impl AllClocks {
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize ,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allstoredclocks = AllClocks::read_clocks(hosts, ports, parallel).await?;
        snapshot::save_snapshot_json(snapshot_number, "clocks", allstoredclocks.clocks)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub fn new() -> Self { Default::default() }
    pub async fn read_clocks (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize
    ) -> Result<AllClocks>
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
                        let mut clocks = AllClocks::read_http(host, port);
                        clocks.iter_mut().for_each(|r| r.timestamp = Some(detail_snapshot_time));
                        clocks.iter_mut().for_each(|r| r.hostname_port = Some(format!("{}:{}", host, port)));
                        tx.send(clocks).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allclocks = AllClocks::new();

        for clocks in rx
        {
            for clock in clocks
            {
                 allclocks.clocks.push(clock);
            }
        }
        Ok(allclocks)
    }
    fn read_http(
        host: &str,
        port: &str,
    ) -> Vec<Clocks>
    {
        let data_from_http = utility::http_get(host, port, "tablet-server-clocks");
        AllClocks::parse_clocks(data_from_http)
    }
    fn parse_clocks(
        http_data: String,
    ) -> Vec<Clocks>
    {
        let table_selector = Selector::parse("table").unwrap();
        let tr_selector = Selector::parse("tr").unwrap();
        let th_selector = Selector::parse("th").unwrap();
        let td_selector = Selector::parse("td").unwrap();

        let mut clocks: Vec<Clocks> = Vec::new();

        let html = Html::parse_document(&http_data);

        // This is how the 'Tablet Servers' table looks like:
        // ---
        // <h2>Tablet Servers</h2>
        // <table class='table table-striped'>
        // <tr>
        // <th>Server</th>
        // <th>Time since </br>heartbeat</th>
        // <th>Status & Uptime</th>
        // <th>Physical Time (UTC)</th>
        // <th>Hybrid Time (UTC)</th>
        // <th>Heartbeat RTT</th>
        // <th>Cloud</th>
        // <th>Region</th>
        // <th>Zone</th>
        // </tr>
        // <tr>
        // <td><a href="http://yb-1.local:9000/">yb-1.local:9000</a></br>  376668f0f8894738878b5cc81bae3be5</td><td>0.0s</td>    <td style="color:Green">ALIVE: 1:48:26</td>    <td>2023-02-02 13:12:21.164655</td>    <td>2023-02-02 13:12:21.164655</td>    <td>0.81ms</td>    <td>local</td>    <td>local</td>    <td>local1</td>  </tr>
        // <tr>
        // ---
        // The table is not enclosed in an html tag, nor has an explicit id set :-(.
        // So what we do is find a table, and match the table header names to find the correct table.
        // This is very exhaustive in checking, an alternative is to simply says we want the nth table;
        // This table is the first one to run into (there are two on this page).
        for table in html.select(&table_selector)
        {
            // table has got the table it found in it.
            match table
            {
                // These are the table column names of the table we want to get the data from.
                // th = table header
                th
                if th.select(&th_selector).next().unwrap().text().collect::<String>() == *"Server"
                    && th.select(&th_selector).nth(1).unwrap().text().collect::<String>() == *"Time since heartbeat"
                    && th.select(&th_selector).nth(2).unwrap().text().collect::<String>() == *"Status & Uptime"
                    && th.select(&th_selector).nth(3).unwrap().text().collect::<String>() == *"Physical Time (UTC)"
                    && th.select(&th_selector).nth(4).unwrap().text().collect::<String>() == *"Hybrid Time (UTC)"
                    && th.select(&th_selector).nth(5).unwrap().text().collect::<String>() == *"Heartbeat RTT"
                    && th.select(&th_selector).nth(6).unwrap().text().collect::<String>() == *"Cloud"
                    && th.select(&th_selector).nth(7).unwrap().text().collect::<String>() == *"Region"
                    && th.select(&th_selector).nth(8).unwrap().text().collect::<String>() == *"Zone" =>
                {
                    // These are the table column data values (td) from the table row (tr).
                    // The first table row is skipped, that is the heading as seen above.
                    for tr in table.select(&tr_selector).skip(1)
                    {
                        clocks.push( Clocks {
                            server: tr.select(&td_selector).next().unwrap().text().collect::<String>(),
                            time_since_heartbeat: tr.select(&td_selector).nth(1).unwrap().text().collect::<String>(),
                            status_uptime: tr.select(&td_selector).nth(2).unwrap().text().collect::<String>(),
                            physical_time_utc: tr.select(&td_selector).nth(3).unwrap().text().collect::<String>(),
                            hybrid_time_utc: tr.select(&td_selector).nth(4).unwrap().text().collect::<String>(),
                            heartbeat_rtt: tr.select(&td_selector).nth(5).unwrap().text().collect::<String>(),
                            cloud: tr.select(&td_selector).nth(6).unwrap().text().collect::<String>(),
                            region: tr.select(&td_selector).nth(7).unwrap().text().collect::<String>(),
                            zone: tr.select(&td_selector).nth(8).unwrap().text().collect::<String>(),
                            ..Default::default()
                        });
                    }
                },
                _ => {
                    info!("Found another table, this shouldn't happen.");
                },
            }
        }
        clocks
    }
    pub fn print(
        &self,
        details_enable: &bool,
        leader_hostname: String,
    ) -> Result<()>
    {
        info!("print tablet server clocks");


        if *details_enable
        {
            println!("{:20} {:20} {:10} {:20} {:26} {:46} {:6} {:10} {:10} {:10}",
                     "hostname",
                     "server",
                     "HB",
                     "status uptime",
                     "physical time UTC",
                     "hybrid time UTC",
                     "HB RTT",
                     "cloud",
                     "region",
                     "zone"
            );
        }
        else
        {
            println!("{:20} {:10} {:20} {:26} {:46} {:6} {:10} {:10} {:10}",
                     "server",
                     "HB",
                     "status uptime",
                     "physical time UTC",
                     "hybrid time UTC",
                     "HB RTT",
                     "cloud",
                     "region",
                     "zone"
            );
        }
        for row in &self.clocks {
            if row.hostname_port == Some(leader_hostname.clone())
                && !*details_enable
            {
                println!("{:20} {:10} {:20} {:26} {:46} {:6} {:10} {:10} {:10}",
                         row.server.split_whitespace().next().unwrap_or_default(),
                         row.time_since_heartbeat,
                         row.status_uptime,
                         row.physical_time_utc,
                         row.hybrid_time_utc,
                         row.heartbeat_rtt,
                         row.cloud,
                         row.region,
                         row.zone
                );
            }
            if *details_enable
            {
                println!("{}: {} {} {} {} {} {} {} {} {}", row.hostname_port.as_ref().unwrap(), row.server, row.time_since_heartbeat, row.status_uptime, row.physical_time_utc, row.hybrid_time_utc, row.heartbeat_rtt, row.cloud, row.region, row.zone);
            }
        }
        Ok(())
    }
    pub async fn print_latency(
        &self,
        details_enable: &bool,
        leader_hostname: String,
    ) -> Result<()>
    {
        info!("print adhoc tablet servers clocks latency");


        for row in &self.clocks {
            if row.hostname_port == Some(leader_hostname.clone())
                && !*details_enable
            {
                println!("{} -> {}: {} RTT ({} {} {})", leader_hostname.clone(), row.server.split_whitespace().next().unwrap_or_default(), row.heartbeat_rtt, row.cloud, row.region, row.zone);
            }
            if *details_enable
            {
                println!("{} {} -> {}: {} RTT ({} {} {})", row.hostname_port.as_ref().unwrap(), leader_hostname.clone(), row.server.split_whitespace().next().unwrap_or_default(), row.heartbeat_rtt, row.cloud, row.region, row.zone);
            }
        }
        Ok(())
    }
}

pub async fn print_clocks(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    match options.print_clocks.as_ref().unwrap() {
        Some(snapshot_number) => {
            let mut allclocks = AllClocks::new();
            allclocks.clocks = snapshot::read_snapshot_json(snapshot_number, "clocks")?;
            let leader_hostname = AllIsLeader::return_leader_snapshot(snapshot_number)?;

            allclocks.print(&options.details_enable, leader_hostname)?;
        },
        None => {
            let allclocks = AllClocks::read_clocks(&hosts, &ports, parallel).await?;
            let leader_hostname = AllIsLeader::return_leader_http(&hosts, &ports, parallel).await;
            allclocks.print(&options.details_enable, leader_hostname)?;
        },
    }
    Ok(())
}

pub async fn print_latencies(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    match options.print_latencies.as_ref().unwrap() {
        Some(snapshot_number) => {
            let mut allclocks = AllClocks::new();
            allclocks.clocks = snapshot::read_snapshot_json(snapshot_number, "clocks")?;
            let leader_hostname = AllIsLeader::return_leader_snapshot(snapshot_number)?;

            allclocks.print_latency(&options.details_enable, leader_hostname).await?;
        },
        None => {
            let allstoredclocks = AllClocks::read_clocks(&hosts, &ports, parallel).await?;
            let leader_hostname = AllIsLeader::return_leader_http(&hosts, &ports, parallel).await;

            allstoredclocks.print_latency(&options.details_enable, leader_hostname).await?;
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let result = AllClocks::parse_clocks(threads);
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
    async fn integration_parse_clocks() -> Result<()>
    {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let allclocks = AllClocks::read_clocks(&vec![&hostname], &vec![&port], 1_usize).await?;
        assert!(!allclocks.clocks.is_empty());

        Ok(())
    }
}
