//! The impls and functions
//!
use chrono::Local;
use regex::Regex;
use std::{sync::mpsc::channel, time::Instant};
use scraper::{Html, Selector};
use log::*;
use anyhow::Result;
use crate::snapshot;
use crate::memtrackers::{MemTrackers, AllMemTrackers};
use crate::Opts;
use crate::utility;

impl AllMemTrackers {
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize ,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allmemtrackers = AllMemTrackers::read_memtrackers(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number,"memtrackers", allmemtrackers.memtrackers)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub fn new() -> Self { Default::default() }
    pub async fn read_memtrackers (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllMemTrackers
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
                        let mut memtrackers = AllMemTrackers::read_http(host, port);
                        memtrackers.iter_mut().for_each(|r| r.timestamp = detail_snapshot_time);
                        memtrackers.iter_mut().for_each(|r| r.hostname_port = format!("{}:{}", host, port));
                        tx.send(memtrackers).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allmemtrackers = AllMemTrackers::new();

        for memtrackers in rx
        {
            for memtracker in memtrackers
            {
                allmemtrackers.memtrackers.push(memtracker);
            }
        }

        allmemtrackers
    }
    fn read_http(
        host: &str,
        port: &str,
    ) -> Vec<MemTrackers>
    {
        let data_from_http = utility::http_get(host, port, "mem-trackers");
        AllMemTrackers::parse_memtrackers(data_from_http)
    }
    fn parse_memtrackers(
        http_data: String
    ) -> Vec<MemTrackers>
    {
        let table_selector = Selector::parse("table").unwrap();
        let tr_selector = Selector::parse("tr").unwrap();
        let th_selector = Selector::parse("th").unwrap();
        let td_selector = Selector::parse("td").unwrap();

        let mut memtrackers: Vec<MemTrackers> = Vec::new();

        let html = Html::parse_document(&http_data);

        // This is how the 'Memory usage by subsystem' table looks like:
        // ---
        //         <table class='table table-striped'>
        //             <tr>
        //                 <th>Id</th>
        //                 <th>Current Consumption</th>
        //                 <th>Peak consumption</th>
        //                 <th>Limit</th>
        //             </tr>
        //             <tr data-depth="0" class="level0">
        //                 <td>root</td>
        //                 <td>25.68M</td>
        //                 <td>74.78M</td>
        //                 <td>241.23M</td>
        //             </tr>
        // --
        for table in html.select(&table_selector)
        {
            match table
            {
                th
                if th.select(&th_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Id"
                    && th.select(&th_selector).nth(1).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Current Consumption"
                    && th.select(&th_selector).nth(2).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Peak consumption"
                    && th.select(&th_selector).nth(3).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Limit" =>
                    {
                        for tr in table.select(&tr_selector).skip(1)
                        {
                            // Data preparation.
                            // The id column.
                            // "->" is fetched as "-&gt;", so translate it back.
                            let id = tr.select(&td_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default().replace("&gt;", ">");
                            // The depth and parent is shown in the page in this way: BlockBasedTable->server->root
                            // I think it's easier to read as root->server->BlockBasedTable, so we reverse the order.
                            let mut id_vec: Vec<_> = id.split("->").collect();
                            id_vec.reverse();

                            memtrackers.push( MemTrackers {
                                id: id_vec.join("->"),
                                current_consumption: tr.select(&td_selector).nth(1).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                peak_consumption: tr.select(&td_selector).nth(2).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                limit: tr.select(&td_selector).nth(3).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                depth: tr.value().attr("data-depth").and_then(|row| Some(row.to_string())).unwrap_or_default(),
                                ..Default::default()
                            });
                        }
                    }
                _ => {
                    info!("Found another table, which shouldn't happen.")
                }
            }
        }


        memtrackers
    }
    pub fn print(
        &self,
        hostname_filter: &Regex,
        stat_name_filter: &Regex,
    ) -> Result<()>
    {
        info!("print_memtrackers");

        let mut previous_hostname_port = String::from("");
        for row in &self.memtrackers
        {
            if hostname_filter.is_match(&row.hostname_port.clone())
                && stat_name_filter.is_match(&row.id)
            {
                if row.hostname_port.clone() != previous_hostname_port
                {
                    println!("{}", "-".repeat(174));
                    println!("Host: {}, Snapshot time: {}", &row.hostname_port.clone(), row.timestamp);
                    println!("{}", "-".repeat(174));
                    println!("{:20} {:90} {:>20} {:>20} {:>20}",
                             "hostname_port",
                             "id",
                             "current_consumption",
                             "peak_consumption",
                             "limit");
                    println!("{}", "-".repeat(174));
                    previous_hostname_port = row.hostname_port.clone();
                }
                let indented_id = " ".repeat(row.depth.parse::<usize>().unwrap()) + &row.id;
                println!("{:20} {:90} {:>20} {:>20} {:>20}", row.hostname_port.clone(), indented_id, row.current_consumption, row.peak_consumption, row.limit)
            }
        }
        Ok(())
    }
}

pub async fn print_memtrackers(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    let hostname_filter = utility::set_regex(&options.hostname_match);
    let stat_name_filter = utility::set_regex(&options.stat_name_match);

    match options.print_memtrackers.as_ref().unwrap() {
        Some(snapshot_number) => {
            let mut allmemtrackers = AllMemTrackers::new();
            allmemtrackers.memtrackers = snapshot::read_snapshot_json(snapshot_number, "memtrackers")?;
            allmemtrackers.print(&hostname_filter, &stat_name_filter)?;
        },
        None => {
            let allmemtrackers = AllMemTrackers::read_memtrackers(&hosts, &ports, parallel).await;
            allmemtrackers.print(&hostname_filter, &stat_name_filter)?;
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_memtrackers_data() {
        // This is what /mem-trackers returns.
        let memtrackers = r#"
<!DOCTYPE html><html>  <head>    <title>YugabyteDB</title>    <link rel='shortcut icon' href='/favicon.ico'>    <link href='/bootstrap/css/bootstrap.min.css' rel='stylesheet' media='screen' />    <link href='/bootstrap/css/bootstrap-theme.min.css' rel='stylesheet' media='screen' />    <link href='/font-awesome/css/font-awesome.min.css' rel='stylesheet' media='screen' />    <link href='/yb.css' rel='stylesheet' media='screen' />  </head>
<body>
  <nav class="navbar navbar-fixed-top navbar-inverse sidebar-wrapper" role="navigation">    <ul class="nav sidebar-nav">      <li><a href='/'><img src='/logo.png' alt='YugabyteDB' class='nav-logo' /></a></li>
<li class='nav-item'><a href='/'><div><i class='fa fa-dashboard'aria-hidden='true'></i></div>Dashboards</a></li>
<li class='nav-item'><a href='/tables'><div><i class='fa fa-table'aria-hidden='true'></i></div>Tables</a></li>
<li class='nav-item'><a href='/tablets'><div><i class='fa fa-server'aria-hidden='true'></i></div>Tablets</a></li>
<li class='nav-item'><a href='/utilz'><div><i class='fa fa-wrench'aria-hidden='true'></i></div>Utilities</a></li>
    </ul>  </nav>

    <div class='yb-main container-fluid'><h1>Memory usage by subsystem</h1>
<table class='table table-striped'>
  <tr><th>Id</th><th>Current Consumption</th><th>Peak consumption</th><th>Limit</th></tr>
  <tr data-depth="0" class="level0">
    <td>root</td><td>99.84M</td><td>111.26M</td><td>485.50M</td>
  </tr>
  <tr data-depth="1" class="level1">
    <td>TCMalloc Central Cache</td><td>2.06M</td><td>2.31M</td><td>none</td>
  </tr>
  <tr data-depth="1" class="level1">
    <td>TCMalloc PageHeap Free</td><td>6.41M</td><td>19.81M</td><td>none</td>
  </tr>
  <tr data-depth="1" class="level1">
    <td>TCMalloc Thread Cache</td><td>3.95M</td><td>4.47M</td><td>none</td>
  </tr>
  <tr data-depth="1" class="level1">
    <td>TCMalloc Transfer Cache</td><td>2.18M</td><td>3.03M</td><td>none</td>
  </tr>
  <tr data-depth="1" class="level1">
    <td>server</td><td>43.15M (58.42M)</td><td>43.17M</td><td>none</td>
  </tr>
  <tr data-depth="2" class="level2">
    <td>BlockBasedTable</td><td>0B</td><td>0B</td><td>48.55M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-16add7b1248a45d2880e5527b2059b54</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-1ccc546756564d0fb2af7bd4800f282c</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-1e336974349049a39195b81c7e3b2b99</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-24f46e6f205c4947ae67990c0a1f2b9c</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-372d9a599da1499e8b7ae0df489e1c90</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-3b48246e33ea4d898a0b01a1e4fc462f</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-3d1a86237e6a4a5ea252c175e98a6adc</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-61b36aececd8430fab92527166d1da9a</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-653aab9b670e42cf8453b131fe67085f</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-7ccbd943aeb34ca6a5dece6492d10409</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-908dbe9c8ac040d68c08a699c639b5e0</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-a4ddf8e1902e447fbee15f01dc5e240e</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-c3a69ba3dad94c9c80d63675b92d5e3b</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-c5ab671d933147adb5b65d2ab8cfd9de</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-c6099b05976f49d9b782ccbe126f9b2d</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-cbefee7ace1743fc97abd0759de58517</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-eb830d7c8c0a4e04b85fae18f92fb811</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-f00af468ba6d49da8e820059667280bd</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>IntentsDB-f085cfb521f74c629bc467a1b0b533ac</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-0e21b2584344415dad9a54371b2d1881</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-11b0cab218c54f168eac3e50bee9a537</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-15e579776886478b87e48580293d3c66</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-16add7b1248a45d2880e5527b2059b54</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-1afc878a9d2b47ff91d4100bc42b1d11</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-1ccc546756564d0fb2af7bd4800f282c</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-1e336974349049a39195b81c7e3b2b99</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-24f46e6f205c4947ae67990c0a1f2b9c</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-372d9a599da1499e8b7ae0df489e1c90</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-3b48246e33ea4d898a0b01a1e4fc462f</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-3d1a86237e6a4a5ea252c175e98a6adc</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-61b36aececd8430fab92527166d1da9a</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-653aab9b670e42cf8453b131fe67085f</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-77271f45481141c3bb2be9c10e73d0ef</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-7ccbd943aeb34ca6a5dece6492d10409</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-908dbe9c8ac040d68c08a699c639b5e0</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-9d97164d0f7c4b6b85918b4cd8a77237</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-a4ddf8e1902e447fbee15f01dc5e240e</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-b770079b94ad430493ba5f729fb1f0e7</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-bd3b8db275034e628f78945810586df2</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-c3a69ba3dad94c9c80d63675b92d5e3b</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-c4e671110ad94326975052da11b3aab6</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-c5ab671d933147adb5b65d2ab8cfd9de</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-c6099b05976f49d9b782ccbe126f9b2d</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-cbefee7ace1743fc97abd0759de58517</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-eb830d7c8c0a4e04b85fae18f92fb811</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-f00af468ba6d49da8e820059667280bd</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>RegularDB-f085cfb521f74c629bc467a1b0b533ac</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="2" class="level2">
    <td>CQL</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>CQL prepared statements</td><td>0B</td><td>0B</td><td>128.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>CQL processors</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>Call</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>Outbound RPC</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>Compressed Read Buffer</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>Read Buffer</td><td>0B</td><td>0B</td><td>24.27M</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>Outbound RPC</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="2" class="level2">
    <td>Call</td><td>0B</td><td>1.8K</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>CQL</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>Inbound RPC</td><td>0B</td><td>1.8K</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>Outbound RPC</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>Redis</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="2" class="level2">
    <td>Compressed Read Buffer</td><td>0B (7.63M)</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>Receive</td><td>7.63M</td><td>15.73M</td><td>none</td>
  </tr>
  <tr data-depth="2" class="level2">
    <td>Read Buffer</td><td>0B (7.63M)</td><td>16.4K</td><td>24.27M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>CQL</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>Inbound RPC</td><td>0B (5.63M)</td><td>901B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>Reading</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>Receive</td><td>5.63M</td><td>13.73M</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>Sending</td><td>0B</td><td>901B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>Outbound RPC</td><td>0B (2.00M)</td><td>16.4K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>Queueing</td><td>0B</td><td>15.7K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>Reading</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>Receive</td><td>2.00M</td><td>2.42M</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>Sending</td><td>0B</td><td>2.8K</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>Redis</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>Allocated</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>Mandatory</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>Used</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="2" class="level2">
    <td>Redis</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>Call</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>Outbound RPC</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>Read Buffer</td><td>0B</td><td>0B</td><td>24.27M</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>Outbound RPC</td><td>0B</td><td>0B</td><td>none</td>
  </tr>
  <tr data-depth="2" class="level2">
    <td>Tablets</td><td>43.15M</td><td>43.15M</td><td>none</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-03f1acfcb82b4ed9bd5f85d14880c480</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-0e21b2584344415dad9a54371b2d1881</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-11b0cab218c54f168eac3e50bee9a537</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-15e579776886478b87e48580293d3c66</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-16add7b1248a45d2880e5527b2059b54</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-1afc878a9d2b47ff91d4100bc42b1d11</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-1ccc546756564d0fb2af7bd4800f282c</td><td>14.25M</td><td>14.25M</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>8.94M</td><td>8.94M</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>8.94M</td><td>8.94M</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>5.31M</td><td>5.31M</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>5.31M</td><td>5.31M</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-1e336974349049a39195b81c7e3b2b99</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-24f46e6f205c4947ae67990c0a1f2b9c</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-372d9a599da1499e8b7ae0df489e1c90</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-3b48246e33ea4d898a0b01a1e4fc462f</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-3d1a86237e6a4a5ea252c175e98a6adc</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-46ceff63657c481f861e91f0f8553c68</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-4c927d4e025d4fa8af01a4b819933266</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-55cf194f15ab40f8ac45b7dfa180ba9c</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-61b36aececd8430fab92527166d1da9a</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-653aab9b670e42cf8453b131fe67085f</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-6dd74b49896840b69cce28d8245f8078</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-77271f45481141c3bb2be9c10e73d0ef</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-7ccbd943aeb34ca6a5dece6492d10409</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-855589b5e2794c718d58dac8dc230e24</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-908dbe9c8ac040d68c08a699c639b5e0</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-983f1d7efd914154b2275f4a0653b34f</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-9d97164d0f7c4b6b85918b4cd8a77237</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-a4ddf8e1902e447fbee15f01dc5e240e</td><td>14.32M</td><td>14.32M</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>9.00M</td><td>9.00M</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>9.00M</td><td>9.00M</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>5.31M</td><td>5.31M</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>5.31M</td><td>5.31M</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-adf4807740c340cea3e85f25cb583104</td><td>0B</td><td>30B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>30B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-b770079b94ad430493ba5f729fb1f0e7</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-bd3b8db275034e628f78945810586df2</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-c3a69ba3dad94c9c80d63675b92d5e3b</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-c3c374a0683f4afd867068403f04cae8</td><td>0B</td><td>30B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>30B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-c4e671110ad94326975052da11b3aab6</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-c5ab671d933147adb5b65d2ab8cfd9de</td><td>14.50M</td><td>14.50M</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>9.13M</td><td>9.13M</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>9.13M</td><td>9.13M</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>5.38M</td><td>5.38M</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>5.38M</td><td>5.38M</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-c6099b05976f49d9b782ccbe126f9b2d</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-ca0ad3898e7b4dfc9e1700e4f7d41e40</td><td>0B</td><td>30B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>30B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-cbefee7ace1743fc97abd0759de58517</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-d4db28847c004f749a28cb343718d6a3</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-eb830d7c8c0a4e04b85fae18f92fb811</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-f00af468ba6d49da8e820059667280bd</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-f085cfb521f74c629bc467a1b0b533ac</td><td>4.0K</td><td>4.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>IntentsDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>28B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>RegularDB</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="5" class="level5">
    <td>MemTable</td><td>2.0K</td><td>2.0K</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>tablet-fdbb065fc42f42babf71bca82cf04c47</td><td>0B</td><td>30B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>OperationsFromDisk</td><td>0B</td><td>30B</td><td>none</td>
  </tr>
  <tr data-depth="4" class="level4">
    <td>operation_tracker</td><td>0B</td><td>0B</td><td>1.00G</td>
  </tr>
  <tr data-depth="2" class="level2">
    <td>log_cache</td><td>0B</td><td>0B</td><td>24.27M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-03f1acfcb82b4ed9bd5f85d14880c480</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-0e21b2584344415dad9a54371b2d1881</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-11b0cab218c54f168eac3e50bee9a537</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-15e579776886478b87e48580293d3c66</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-16add7b1248a45d2880e5527b2059b54</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-1afc878a9d2b47ff91d4100bc42b1d11</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-1ccc546756564d0fb2af7bd4800f282c</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-1e336974349049a39195b81c7e3b2b99</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-24f46e6f205c4947ae67990c0a1f2b9c</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-372d9a599da1499e8b7ae0df489e1c90</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-3b48246e33ea4d898a0b01a1e4fc462f</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-3d1a86237e6a4a5ea252c175e98a6adc</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-46ceff63657c481f861e91f0f8553c68</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-4c927d4e025d4fa8af01a4b819933266</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-55cf194f15ab40f8ac45b7dfa180ba9c</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-61b36aececd8430fab92527166d1da9a</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-653aab9b670e42cf8453b131fe67085f</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-6dd74b49896840b69cce28d8245f8078</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-77271f45481141c3bb2be9c10e73d0ef</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-7ccbd943aeb34ca6a5dece6492d10409</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-855589b5e2794c718d58dac8dc230e24</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-908dbe9c8ac040d68c08a699c639b5e0</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-983f1d7efd914154b2275f4a0653b34f</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-9d97164d0f7c4b6b85918b4cd8a77237</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-a4ddf8e1902e447fbee15f01dc5e240e</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-adf4807740c340cea3e85f25cb583104</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-b770079b94ad430493ba5f729fb1f0e7</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-bd3b8db275034e628f78945810586df2</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-c3a69ba3dad94c9c80d63675b92d5e3b</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-c3c374a0683f4afd867068403f04cae8</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-c4e671110ad94326975052da11b3aab6</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-c5ab671d933147adb5b65d2ab8cfd9de</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-c6099b05976f49d9b782ccbe126f9b2d</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-ca0ad3898e7b4dfc9e1700e4f7d41e40</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-cbefee7ace1743fc97abd0759de58517</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-d4db28847c004f749a28cb343718d6a3</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-eb830d7c8c0a4e04b85fae18f92fb811</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-f00af468ba6d49da8e820059667280bd</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-f085cfb521f74c629bc467a1b0b533ac</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
  <tr data-depth="3" class="level3">
    <td>log_cache-fdbb065fc42f42babf71bca82cf04c47</td><td>0B</td><td>0B</td><td>16.00M</td>
  </tr>
</table>
<div class='yb-bottom-spacer'></div></div>
<footer class='footer'><div class='yb-footer container text-muted'><pre class='message'><i class="fa-lg fa fa-gift" aria-hidden="true"></i> Congratulations on installing YugabyteDB. We'd like to welcome you to the community with a free t-shirt and pack of stickers! Please claim your reward here: <a href='https://www.yugabyte.com/community-rewards/'>https://www.yugabyte.com/community-rewards/</a></pre><pre>version 2.11.2.0 build 89 revision d142556567b5e1c83ea5c915ec7b9964492b2321 build_type RELEASE built at 25 Jan 2022 17:51:08 UTC
server uuid 05b8d17620eb4cd79eddaddb2fbcbb42</pre></div></footer></body></html>
"#.to_string();
        let result = AllMemTrackers::parse_memtrackers(memtrackers);
        assert_eq!(result.len(), 345);
    }

    #[tokio::test]
    async fn integration_parse_memtrackers_master() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let allmemtrackers = AllMemTrackers::read_memtrackers(&vec![&hostname], &vec![&port], 1).await;
        // memtrackers must return some rows
        assert!(!allmemtrackers.memtrackers.is_empty());
    }
    #[tokio::test]
    async fn parse_memtrackers_tserver() {
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();

        let allmemtrackers = AllMemTrackers::read_memtrackers(&vec![&hostname], &vec![&port], 1).await;
        // memtrackers must return some rows
        assert!(!allmemtrackers.memtrackers.is_empty());
    }
}