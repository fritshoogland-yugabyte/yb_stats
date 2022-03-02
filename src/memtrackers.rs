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
