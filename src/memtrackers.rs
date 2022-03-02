use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use table_extract;
use std::path::PathBuf;
use regex::Regex;
use std::fs;
use std::process;
use serde_derive::{Serialize,Deserialize};

#[derive(Debug)]
pub struct MemTrackers {
    pub id: String,
    pub current_consumption: String,
    pub peak_consumption: String,
    pub limit: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredMemTrackers {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub id: String,
    pub current_consumption: String,
    pub peak_consumption: String,
    pub limit: String,
}

#[allow(dead_code)]
pub fn read_memtrackers(
    hostname: &str
) -> Vec<MemTrackers> {
    if ! scan_port_addr( hostname ) {
        println!("Warning: hostname:port {} cannot be reached, skipping", hostname.to_string());
        return Vec::new();
    }
    if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}/mem-trackers", hostname.to_string())) {
        parse_memtrackers(data_from_http.text().unwrap())
    } else {
        parse_memtrackers(String::from(""))
    }
}

#[allow(dead_code)]
fn parse_memtrackers(
    http_data: String
) -> Vec<MemTrackers> {
    let mut memtrackers: Vec<MemTrackers> = Vec::new();
    if let Some ( table ) = table_extract::Table::find_first(&http_data) {
        for row in &table {
            memtrackers.push(MemTrackers {
                id: row.get("Id").unwrap_or("<Missing>").to_string(),
                current_consumption: row.get("Current Consumption").unwrap_or("<Missing>").to_string(),
                // mind 'consumption': it doesn't start with a capital, which is different from 'Current Consumption' above.
                peak_consumption: row.get("Peak consumption").unwrap_or("<Missing>").to_string(),
                limit: row.get("Limit").unwrap_or("<Missing>").to_string()
            });
        }
    }
    memtrackers
}

#[allow(dead_code)]
pub fn print_memtrackers_data(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
    hostname_filter: &Regex,
    stat_name_filter: &Regex
) {
    let stored_memtrackers: Vec<StoredMemTrackers> = read_memtrackers_snapshot(&snapshot_number, yb_stats_directory);
    let mut previous_hostname_port = String::from("");
    for row in stored_memtrackers {
        if hostname_filter.is_match(&row.hostname_port)
            && stat_name_filter.is_match(&row.id) {
            if row.hostname_port != previous_hostname_port {
                println!("--------------------------------------------------------------------------------------------------------------------------------------");
                println!("Host: {}, Snapshot number: {}, Snapshot time: {}", &row.hostname_port.to_string(), &snapshot_number, row.timestamp);
                println!("--------------------------------------------------------------------------------------------------------------------------------------");
                println!("{:20} {:50} {:>20} {:>20} {:>20}",
                         "hostname_port",
                         "id",
                         "current_consumption",
                         "peak_consumption",
                         "limit");
                println!("--------------------------------------------------------------------------------------------------------------------------------------");
                previous_hostname_port = row.hostname_port.to_string();
            }
            println!("{:20} {:50} {:>20} {:>20} {:>20}", row.hostname_port, row.id, row.current_consumption, row.peak_consumption, row.limit)
        }
    }
}

#[allow(dead_code)]
fn read_memtrackers_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf
) -> Vec<StoredMemTrackers> {
    let mut stored_memtrackers: Vec<StoredMemTrackers> = Vec::new();
    let memtrackers_file = &yb_stats_directory.join(&snapshot_number.to_string()).join("memtrackers");
    let file = fs::File::open(&memtrackers_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error reading file: {}: {}", &memtrackers_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredMemTrackers = row.unwrap();
        let _ = &stored_memtrackers.push(data);
    }
    stored_memtrackers
}

#[allow(dead_code)]
pub fn add_to_memtrackers_vector(memtrackersdata: Vec<MemTrackers>,
                                 hostname: &str,
                                 snapshot_time: DateTime<Local>,
                                 stored_memtrackers: &mut Vec<StoredMemTrackers>
) {
    for line in memtrackersdata {
        stored_memtrackers.push( StoredMemTrackers {
            hostname_port: hostname.to_string(),
            timestamp: snapshot_time,
            id: line.id.to_string(),
            current_consumption: line.current_consumption.to_string(),
            peak_consumption: line.peak_consumption.to_string(),
            limit: line.limit.to_string()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_memtrackers_data() {
        // This is what /mem-trackers return.
        let memtrackers = r#"<!DOCTYPE html><html>  <head>    <title>YugabyteDB</title>    <link rel='shortcut icon' href='/favicon.ico'>    <link href='/bootstrap/css/bootstrap.min.css' rel='stylesheet' media='screen' />    <link href='/bootstrap/css/bootstrap-theme.min.css' rel='stylesheet' media='screen' />    <link href='/font-awesome/css/font-awesome.min.css' rel='stylesheet' media='screen' />    <link href='/yb.css' rel='stylesheet' media='screen' />  </head>
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
server uuid 05b8d17620eb4cd79eddaddb2fbcbb42</pre></div></footer></body></html>"#.to_string();
        let result = parse_memtrackers(memtrackers.clone());
        assert_eq!(result.len(), 345);
    }
}