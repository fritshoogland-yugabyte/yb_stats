extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate csv;

mod value_statistic_details;
use value_statistic_details::{ValueStatisticDetails, value_create_hashmap};
mod countsum_statistic_details;
use countsum_statistic_details::{CountSumStatisticDetails, countsum_create_hashmap};

use std::collections::BTreeMap;
use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use std::process;
use std::fs;
use std::path::{Path, PathBuf};
use std::env;
use regex::Regex;
use substring::Substring;
use std::io::{stdin, stdout, Write};
use table_extract;

#[derive(Debug)]
pub struct MemTrackers {
    pub id: String,
    pub current_consumption: String,
    pub peak_consumption: String,
    pub limit: String,
}

#[derive(Debug)]
pub struct LogLine {
    pub severity: String,
    pub timestamp: DateTime<Local>,
    pub tid: String,
    pub sourcefile_nr: String,
    pub message: String,
}

#[derive(Debug)]
pub struct GFlag {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Metrics {
    #[serde(rename = "type")]
    pub metrics_type: String,
    pub id: String,
    pub attributes: Option<Attributes>,
    pub metrics: Vec<NamedMetrics>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Attributes {
    pub namespace_name: Option<String>,
    pub table_name: Option<String>,
    pub table_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VersionData {
    pub git_hash: String,
    pub build_hostname: String,
    pub build_timestamp: String,
    pub build_username: String,
    pub build_clean_repo: bool,
    pub build_id: String,
    pub build_type: String,
    pub version_number: String,
    pub build_number: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Statement {
    pub statements: Vec<Queries>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Queries {
    pub query: String,
    pub calls: i64,
    pub total_time: f64,
    pub min_time: f64,
    pub max_time: f64,
    pub mean_time: f64,
    pub stddev_time: f64,
    pub rows: i64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum NamedMetrics {
    MetricValue {
        name: String,
        value: i64,
    },
    RejectedMetricValue {
        name: String,
        value: u64,
    },
    MetricCountSum {
        name: String,
        total_count: u64,
        min: u64,
        mean: f64,
        percentile_75: u64,
        percentile_95: u64,
        percentile_99: u64,
        percentile_99_9: u64,
        percentile_99_99: u64,
        max: u64,
        total_sum: u64,
    },
    MetricCountSumRows {
        name: String,
        count: i64,
        sum: i64,
        rows: i64,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredStatements {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub query: String,
    pub calls: i64,
    pub total_time: f64,
    pub min_time: f64,
    pub max_time: f64,
    pub mean_time: f64,
    pub stddev_time: f64,
    pub rows: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredVersionData {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub git_hash: String,
    pub build_hostname: String,
    pub build_timestamp: String,
    pub build_username: String,
    pub build_clean_repo: String,
    pub build_id: String,
    pub build_type: String,
    pub version_number: String,
    pub build_number: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredValues {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub metric_type: String,
    pub metric_id: String,
    pub attribute_namespace: String,
    pub attribute_table_name: String,
    pub metric_name: String,
    pub metric_value: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredGFlags {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub gflag_name: String,
    pub gflag_value: String,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredLogLines {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub severity: String,
    pub tid: String,
    pub sourcefile_nr: String,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredCountSum {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub metric_type: String,
    pub metric_id: String,
    pub attribute_namespace: String,
    pub attribute_table_name: String,
    pub metric_name: String,
    pub metric_total_count: u64,
    pub metric_min: u64,
    pub metric_mean: f64,
    pub metric_percentile_75: u64,
    pub metric_percentile_95: u64,
    pub metric_percentile_99: u64,
    pub metric_percentile_99_9: u64,
    pub metric_percentile_99_99: u64,
    pub metric_max: u64,
    pub metric_total_sum: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredCountSumRows {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub metric_type: String,
    pub metric_id: String,
    pub attribute_namespace: String,
    pub attribute_table_name: String,
    pub metric_name: String,
    pub metric_count: i64,
    pub metric_sum: i64,
    pub metric_rows: i64,
}

#[derive(Debug)]
pub struct SnapshotDiffStatements {
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub first_calls: i64,
    pub second_calls: i64,
    pub first_total_time: f64,
    pub second_total_time: f64,
    pub first_rows: i64,
    pub second_rows: i64,
}

#[derive(Debug)]
pub struct SnapshotDiffValues {
    pub table_name: String,
    pub namespace: String,
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub first_snapshot_value: i64,
    pub second_snapshot_value: i64,
}
#[derive(Debug)]
pub struct SnapshotDiffCountSum {
    pub table_name: String,
    pub namespace: String,
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub second_snapshot_total_count: u64,
    pub second_snapshot_min: u64,
    pub second_snapshot_mean: f64,
    pub second_snapshot_percentile_75: u64,
    pub second_snapshot_percentile_95: u64,
    pub second_snapshot_percentile_99: u64,
    pub second_snapshot_percentile_99_9: u64,
    pub second_snapshot_percentile_99_99: u64,
    pub second_snapshot_max: u64,
    pub second_snapshot_total_sum: u64,
    pub first_snapshot_total_count: u64,
    pub first_snapshot_total_sum: u64,
}

#[derive(Debug)]
pub struct SnapshotDiffCountSumRows {
    pub table_name: String,
    pub namespace: String,
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub first_snapshot_count: i64,
    pub first_snapshot_sum: i64,
    pub first_snapshot_rows: i64,
    pub second_snapshot_count: i64,
    pub second_snapshot_sum: i64,
    pub second_snapshot_rows: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Snapshot {
    pub number: i32,
    pub timestamp: DateTime<Local>,
    pub comment: String,
}

pub fn read_memtrackers( hostname: &str) -> Vec<MemTrackers> {
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

pub fn read_loglines( hostname: &str) -> Vec<LogLine> {
    if ! scan_port_addr( hostname ) {
        println!("Warning: hostname:port {} cannot be reached, skipping", hostname.to_string());
        return Vec::new();
    }
    if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}/logs?raw", hostname.to_string())) {
        parse_loglines(data_from_http.text().unwrap())
    } else {
        parse_loglines(String::from(""))
    }
}

pub fn read_gflags( hostname: &str) -> Vec<GFlag> {
    if ! scan_port_addr( hostname ) {
        println!("Warning: hostname:port {} cannot be reached, skipping", hostname.to_string());
        return Vec::new(); //String::from(""); //Vec::new() //parse_statements(String::from(""))
    }
    if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}/varz?raw", hostname.to_string())) {
        parse_gflags(data_from_http.text().unwrap())
    } else {
        parse_gflags(String::from(""))
    }
}

pub fn read_metrics( hostname: &str) -> Vec<Metrics> {
        if ! scan_port_addr(hostname) {
            println!("Warning! hostname:port {} cannot be reached, skipping", hostname.to_string());
            return parse_metrics(String::from(""))
        };
        let data_from_http = reqwest::blocking::get(format!("http://{}/metrics", hostname.to_string()))
            .unwrap_or_else(|e| {
                eprintln!("Fatal: error reading from URL: {}", e);
                process::exit(1);
            })
            .text().unwrap();
        parse_metrics(data_from_http)
}

pub fn read_version( hostname: &str) -> VersionData {
    if ! scan_port_addr( hostname) {
        println!("Warning hostname:port {} cannot be reached, skipping", hostname.to_string());
        return parse_version(String::from(""))
    }
    if let Ok(data_from_http) = reqwest::blocking::get( format!("http://{}/api/v1/version", hostname.to_string())) {
        parse_version(data_from_http.text().unwrap())
    } else {
        parse_version(String::from(""))
    }
}

pub fn read_statements( hostname: &str) -> Statement {
    if ! scan_port_addr( hostname ) {
        println!("Warning: hostname:port {} cannot be reached, skipping", hostname.to_string());
        return parse_statements(String::from(""))
    }
    if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}/statements", hostname.to_string())) {
        parse_statements(data_from_http.text().unwrap())
    } else {
        parse_statements(String::from(""))
    }
}

pub fn read_memtrackers_snapshot(snapshot_number: &String, yb_stats_directory: &PathBuf ) -> Vec<StoredMemTrackers> {

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

pub fn add_to_statements_vector(statementdata: Statement,
    hostname: &str,
    snapshot_time: DateTime<Local>,
    stored_statements: &mut Vec<StoredStatements>
) {
    for statement in statementdata.statements {
        stored_statements.push( StoredStatements {
            hostname_port: hostname.to_string(),
            timestamp: snapshot_time,
            query: statement.query.to_string(),
            calls: statement.calls,
            total_time: statement.total_time,
            min_time: statement.min_time,
            max_time: statement.max_time,
            mean_time: statement.mean_time,
            stddev_time: statement.stddev_time,
            rows: statement.rows
        });
    }
}

pub fn add_to_gflags_vector(gflagdata: Vec<GFlag>,
                            hostname: &str,
                            snapshot_time: DateTime<Local>,
                            stored_gflags: &mut Vec<StoredGFlags>
) {
    for gflag in gflagdata {
        stored_gflags.push( StoredGFlags {
            hostname_port: hostname.to_string(),
            timestamp: snapshot_time,
            gflag_name: gflag.name.to_string(),
            gflag_value: gflag.value.to_string()
        });
    }
}

pub fn add_to_loglines_vector(loglinedata: Vec<LogLine>,
                              hostname: &str,
                              stored_loglines: &mut Vec<StoredLogLines>
) {
    for logline in loglinedata {
        stored_loglines.push( StoredLogLines {
            hostname_port: hostname.to_string(),
            timestamp: logline.timestamp,
            severity: logline.severity.to_string(),
            tid: logline.tid.to_string(),
            sourcefile_nr: logline.sourcefile_nr.to_string(),
            message: logline.message.to_string()
        });
    }
}

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

pub fn add_to_version_vector(versiondata: VersionData,
    hostname: &str,
    snapshot_time: DateTime<Local>,
    stored_versiondata: &mut Vec<StoredVersionData>
) {
    stored_versiondata.push(StoredVersionData {
        hostname_port: hostname.to_string(),
        timestamp: snapshot_time,
        git_hash: versiondata.git_hash.to_string(),
        build_hostname: versiondata.build_hostname.to_string(),
        build_timestamp: versiondata.build_timestamp.to_string(),
        build_username: versiondata.build_username.to_string(),
        build_clean_repo: versiondata.build_clean_repo.to_string(),
        build_id: versiondata.build_id.to_string(),
        build_type: versiondata.build_type.to_string(),
        version_number: versiondata.version_number.to_string(),
        build_number: versiondata.build_number.to_string(),
    });
}

pub fn add_to_metric_vectors(data_parsed_from_json: Vec<Metrics>,
                             hostname: &str,
                             detail_snapshot_time: DateTime<Local>,
                             stored_values: &mut Vec<StoredValues>,
                             stored_countsum: &mut Vec<StoredCountSum>,
                             stored_countsumrows: &mut Vec<StoredCountSumRows>,
) {
    for metric in data_parsed_from_json {
        let metric_type = &metric.metrics_type;
        let metric_id = &metric.id;
        let metric_attribute_namespace_name = match &metric.attributes {
            None => String::from("-"),
            Some(attribute) => {
                match &attribute.namespace_name {
                    Some(namespace_name) => namespace_name.to_string(),
                    None => String::from("-"),
                }
            }
        };
        let metric_attribute_table_name = match &metric.attributes {
            None => String::from("-"),
            Some(attribute) => {
                match &attribute.table_name {
                    Some(table_name) => table_name.to_string(),
                    None => String::from("-"),
                }
            }
        };
        for statistic in &metric.metrics {
            match statistic {
                NamedMetrics::MetricValue { name, value } => {
                    if *value > 0 {
                        stored_values.push(StoredValues {
                            hostname_port: hostname.to_string(),
                            timestamp: detail_snapshot_time,
                            metric_type: metric_type.to_string(),
                            metric_id: metric_id.to_string(),
                            attribute_namespace: metric_attribute_namespace_name.to_string(),
                            attribute_table_name: metric_attribute_table_name.to_string(),
                            metric_name: name.to_string(),
                            metric_value: *value,
                        });
                    }
                },
                NamedMetrics::MetricCountSum { name, total_count, min, mean, percentile_75, percentile_95, percentile_99, percentile_99_9, percentile_99_99, max, total_sum } => {
                    if *total_count > 0 {
                        stored_countsum.push(StoredCountSum {
                            hostname_port: hostname.to_string(),
                            timestamp: detail_snapshot_time,
                            metric_type: metric_type.to_string(),
                            metric_id: metric_id.to_string(),
                            attribute_namespace: metric_attribute_namespace_name.to_string(),
                            attribute_table_name: metric_attribute_table_name.to_string(),
                            metric_name: name.to_string(),
                            metric_total_count: *total_count,
                            metric_min: *min,
                            metric_mean: *mean,
                            metric_percentile_75: *percentile_75,
                            metric_percentile_95: *percentile_95,
                            metric_percentile_99: *percentile_99,
                            metric_percentile_99_9: *percentile_99_9,
                            metric_percentile_99_99: *percentile_99_99,
                            metric_max: *max,
                            metric_total_sum: *total_sum,
                        });
                    }
                },
                NamedMetrics::MetricCountSumRows { name, count, sum, rows} => {
                    if *count > 0 {
                        stored_countsumrows.push( StoredCountSumRows {
                            hostname_port: hostname.to_string(),
                            timestamp: detail_snapshot_time,
                            metric_type: metric_type.to_string(),
                            metric_id: metric_id.to_string(),
                            attribute_namespace: metric_attribute_namespace_name.to_string(),
                            attribute_table_name: metric_attribute_table_name.to_string(),
                            metric_name: name.to_string(),
                            metric_count: *count,
                            metric_sum: *sum,
                            metric_rows: *rows,
                        })
                    }
                }
                // this is to to soak up invalid/rejected values
                NamedMetrics::RejectedMetricValue { name: _, value: _ } => {}
            }
        }
    }
}

pub fn read_snapshots_from_file( yb_stats_directory: &PathBuf ) -> Vec<Snapshot> {

    let mut snapshots: Vec<Snapshot> = Vec::new();
    let snapshot_index = &yb_stats_directory.join("snapshot.index");

    let file = fs::File::open(&snapshot_index)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error opening file {}: {}", &snapshot_index.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: Snapshot = row.unwrap();
        let _ = &snapshots.push(data);
    }
    snapshots

}

pub fn read_begin_end_snapshot_from_user( snapshots: &Vec<Snapshot> ) -> (String, String, Snapshot) {

    let mut begin_snapshot = String::new();
    print!("Enter begin snapshot: ");
    let _ = stdout().flush();
    stdin().read_line(&mut begin_snapshot).expect("Failed to read input.");
    let begin_snapshot: i32 = begin_snapshot.trim().parse().expect("Invalid input");
    let begin_snapshot_row = match snapshots.iter().find(|&row| row.number == begin_snapshot) {
        Some(snapshot_find_result) => snapshot_find_result.clone(),
        None => {
            eprintln!("Fatal: snapshot number {} is not found in the snapshot list", begin_snapshot);
            process::exit(1);
        }
    };

    let mut end_snapshot = String::new();
    print!("Enter end snapshot: ");
    let _ = stdout().flush();
    stdin().read_line(&mut end_snapshot).expect("Failed to read input.");
    let end_snapshot: i32 = end_snapshot.trim().parse().expect("Invalid input");
    let _ = match snapshots.iter().find(|&row| row.number == end_snapshot) {
        Some(snapshot_find_result) => snapshot_find_result.clone(),
        None => {
            eprintln!("Fatal: snapshot number {} is not found in the snapshot list", end_snapshot);
            process::exit(1);
        }
    };

    (begin_snapshot.to_string(), end_snapshot.to_string(), begin_snapshot_row)

}

pub fn read_values_snapshot( snapshot_number: &String, yb_stats_directory: &PathBuf ) -> Vec<StoredValues> {

    let mut stored_values: Vec<StoredValues> = Vec::new();
    let values_file = &yb_stats_directory.join(&snapshot_number.to_string()).join("values");
    let file = fs::File::open(&values_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error reading file: {}: {}", &values_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredValues = row.unwrap();
        let _ = &stored_values.push(data);
    }
    stored_values
}

pub fn read_countsum_snapshot( snapshot_number: &String, yb_stats_directory: &PathBuf ) -> Vec<StoredCountSum> {

    let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
    let countsum_file = &yb_stats_directory.join(&snapshot_number.to_string()).join("countsum");
    let file = fs::File::open(&countsum_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error reading file: {}: {}", &countsum_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredCountSum = row.unwrap();
        let _ = &stored_countsum.push(data);
    }
    stored_countsum
}

pub fn read_countsumrows_snapshot( snapshot_number: &String, yb_stats_directory: &PathBuf ) -> Vec<StoredCountSumRows> {

    let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
    let countsumrows_file = &yb_stats_directory.join(&snapshot_number.to_string()).join("countsumrows");
    let file = fs::File::open(&countsumrows_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error reading file: {}: {}", &countsumrows_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredCountSumRows = row.unwrap();
        let _ = &stored_countsumrows.push(data);
    }
    stored_countsumrows
}

pub fn read_statements_snapshot( snapshot_number: &String, yb_stats_directory: &PathBuf ) -> Vec<StoredStatements> {

    let mut stored_statements: Vec<StoredStatements> = Vec::new();
    let statements_file = &yb_stats_directory.join(&snapshot_number.to_string()).join("statements");
    let file = fs::File::open(&statements_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error reading file: {}: {}", &statements_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for  row in reader.deserialize() {
        let data: StoredStatements = row.unwrap();
        let _ = &stored_statements.push(data);
    }
    stored_statements
}

pub fn perform_snapshot( hostname_port_vec: Vec<&str>,
                         snapshot_comment: String,
                        ) -> i32 {
    let snapshot_time = Local::now();
    let mut snapshot_number: i32 = 0;
    let mut snapshots: Vec<Snapshot> = Vec::new();
    let mut stored_values: Vec<StoredValues> = Vec::new();
    let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
    let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
    let mut stored_versiondata: Vec<StoredVersionData> = Vec::new();
    let mut stored_statements: Vec<StoredStatements> = Vec::new();
    let mut stored_gflags: Vec<StoredGFlags> = Vec::new();
    let mut stored_loglines: Vec<StoredLogLines> = Vec::new();
    let mut stored_memtrackers: Vec<StoredMemTrackers> = Vec::new();

    for hostname in &hostname_port_vec {
        let detail_snapshot_time = Local::now();
        // metrics
        let data_parsed_from_json = read_metrics(&hostname);
        add_to_metric_vectors(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_values, &mut stored_countsum, &mut stored_countsumrows);
        let data_parsed_from_json = read_version(&hostname);
        add_to_version_vector(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_versiondata);
        let data_parsed_from_json = read_statements(&hostname);
        add_to_statements_vector(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_statements);
        let gflags = read_gflags(&hostname);
        add_to_gflags_vector(gflags, hostname, detail_snapshot_time, &mut stored_gflags);
        let loglines = read_loglines(&hostname);
        add_to_loglines_vector(loglines, hostname, &mut stored_loglines);
        let memtrackers: Vec<MemTrackers> = read_memtrackers(&hostname);
        add_to_memtrackers_vector(memtrackers, hostname, detail_snapshot_time, &mut stored_memtrackers);
    }

    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    fs::create_dir_all(&yb_stats_directory)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error creating directory {}: {}", &yb_stats_directory.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });

    let snapshot_index = &yb_stats_directory.join("snapshot.index");
    if Path::new(&snapshot_index).exists() {
        let file = fs::File::open(&snapshot_index)
            .unwrap_or_else(|e| {
                eprintln!("Fatal: error opening file {}: {}", &snapshot_index.clone().into_os_string().into_string().unwrap(), e);
                process::exit(1);
            });
        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: Snapshot = row.unwrap();
            let _ = &snapshots.push(data);
        }
        let record_with_highest_snapshot_number = snapshots.iter().max_by_key(|k| k.number).unwrap();
        snapshot_number = record_with_highest_snapshot_number.number + 1;
    }

    let current_snapshot: Snapshot = Snapshot { number: snapshot_number, timestamp: snapshot_time, comment: snapshot_comment };
    let _ = &snapshots.push(current_snapshot);
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&snapshot_index)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing {}: {}", &snapshot_index.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in snapshots {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
    fs::create_dir_all(&current_snapshot_directory)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error creating directory {}: {}", &current_snapshot_directory.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });

    let values_file = &current_snapshot_directory.join("values");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&values_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing values statistics in snapshot directory {}: {}", &values_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_values {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let countsum_file = &current_snapshot_directory.join("countsum");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&countsum_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing countsum statistics in snapshot directory {}: {}", &countsum_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_countsum {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let countsumrows_file = &current_snapshot_directory.join("countsumrows");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&countsumrows_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing countsumrows statistics in snapshot directory {}: {}", &countsumrows_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_countsumrows {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let versions_file = &current_snapshot_directory.join("versions");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&versions_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing versions data in snapshot directory {}: {}", &versions_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_versiondata {
        if row.git_hash != "" {
            writer.serialize(row).unwrap();
        }
    }
    writer.flush().unwrap();

    let statements_file = &current_snapshot_directory.join("statements");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&statements_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing statements data in snapshot directory {}: {}", &statements_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_statements {
        if row.query != "" {
            writer.serialize(row).unwrap();
        }
    }
    writer.flush().unwrap();

    let gflags_file = &current_snapshot_directory.join("gflags");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&gflags_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing gflags data in snapshot directory {}: {}", &gflags_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_gflags {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let loglines_file = &current_snapshot_directory.join("loglines");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&loglines_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing loglines data in snapshot directory {}: {}", &loglines_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_loglines {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let memtrackers_file = &current_snapshot_directory.join("memtrackers");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&memtrackers_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing memtrackers data in snapshot directory {}: {}", &memtrackers_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_memtrackers {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    snapshot_number
}

fn parse_metrics( metrics_data: String ) -> Vec<Metrics> {
    serde_json::from_str(&metrics_data )
        .unwrap_or_else(|e| {
            println!("Warning: error parsing /metrics json data: {}", e);
            return Vec::<Metrics>::new();
        })
}

fn parse_version( version_data: String ) -> VersionData {
    serde_json::from_str( &version_data )
        .unwrap_or_else(|_e| {
            return VersionData { git_hash: "".to_string(), build_hostname: "".to_string(), build_timestamp: "".to_string(), build_username: "".to_string(), build_clean_repo: true, build_id: "".to_string(), build_type: "".to_string(), version_number: "".to_string(), build_number: "".to_string() };
        })
}

fn parse_statements( statements_data: String ) -> Statement {
    serde_json::from_str( &statements_data )
        .unwrap_or_else(|_e| {
            return Statement { statements: Vec::<Queries>::new() };
        })
}

fn parse_gflags( gflags_data: String ) -> Vec<GFlag> {
    let mut gflags: Vec<GFlag> = Vec::new();
    let re = Regex::new( r"--([A-Za-z_0-9]*)=(.*)\n" ).unwrap();
    for captures in re.captures_iter(&gflags_data) {
        gflags.push(GFlag { name: captures.get(1).unwrap().as_str().to_string(), value: captures.get(2).unwrap().as_str().to_string() });
    }
    gflags
}

fn parse_loglines( logs_data: String ) -> Vec<LogLine> {
    let mut loglines: Vec<LogLine> = Vec::new();
    // This regex does not capture the backtraces
    // I0217 10:19:56.834905 31987 docdb_rocksdb_util.cc:416] FLAGS_rocksdb_base_background_compactions was not set, automatically configuring 1 base background compactions.
    let re = Regex::new( r"([IWFE])(\d{2})(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{6}) (\d{1,6}) ([a-z_A-Z.:0-9]*)] (.*)\n" ).unwrap();

    // Just take the year, it's not in the loglines, however, when the year switches this will lead to error results
    let year= Local::now().format("%Y").to_string();
    let timezone = Local::now().format("%z").to_string();
    for captures in re.captures_iter(&logs_data) {
        let mut timestamp_string: String = year.to_owned();
        timestamp_string.push_str(":");
        timestamp_string.push_str(&captures.get(2).unwrap().as_str());        // month
        timestamp_string.push_str(":");
        timestamp_string.push_str(&captures.get(3).unwrap().as_str());        // day
        timestamp_string.push_str(":");
        timestamp_string.push_str(&captures.get(4).unwrap().as_str());        // hour
        timestamp_string.push_str(":");
        timestamp_string.push_str(&captures.get(5).unwrap().as_str());        // minute
        timestamp_string.push_str(":");
        timestamp_string.push_str(&captures.get(6).unwrap().as_str());        // seconds
        timestamp_string.push_str(":");
        timestamp_string.push_str(&captures.get(7).unwrap().as_str());        // seconds
        timestamp_string.push_str(":");
        timestamp_string.push_str(&timezone.to_owned());
        loglines.push(LogLine {
            severity: captures.get(1).unwrap().as_str().to_string(),
            timestamp: DateTime::parse_from_str(&timestamp_string, "%Y:%m:%d:%H:%M:%S:%6f:%z").unwrap().with_timezone(&Local),
            tid: captures.get(8).unwrap().as_str().to_string(),
            sourcefile_nr: captures.get(9).unwrap().as_str().to_string(),
            message: captures.get(10).unwrap().as_str().to_string()
        });
    }
    loglines
}

fn parse_memtrackers( http_data: String ) -> Vec<MemTrackers> {
    //println!("in parse_memtrackers");
    let mut memtrackers: Vec<MemTrackers> = Vec::new();
    //let table = table_extract::Table::find_first(&http_data).unwrap();
    match table_extract::Table::find_first(&http_data) {
        Some ( table ) => {
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
        None => {}
    }

    memtrackers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_master_2_11_1_0_build_305() {
        let master_metrics = include_str!("master_metrics_2_11_1_0_build_305.json");
        let metrics_parse: serde_json::Result<Vec<Metrics>> = serde_json::from_str(&master_metrics);
        let metrics_parse = metrics_parse.unwrap();
        assert_eq!(metrics_parse.len(),4);
    }
    #[test]
    fn parse_tserver_2_11_1_0_build_305() {
        let tserver_metrics = include_str!("tserver_metrics_2_11_1_0_build_305.json");
        let metrics_parse: serde_json::Result<Vec<Metrics>> = serde_json::from_str(&tserver_metrics);
        let metrics_parse = metrics_parse.unwrap();
        assert_eq!(metrics_parse.len(),6);
    }
}

pub fn build_metrics_btreemaps(
    details_enable: bool,
    stored_values: Vec<StoredValues>,
    stored_countsum: Vec<StoredCountSum>,
    stored_countsumrows: Vec<StoredCountSumRows>
) -> (
    BTreeMap<(String, String, String, String), StoredValues>,
    BTreeMap<(String, String, String, String), StoredCountSum>,
    BTreeMap<(String, String, String, String), StoredCountSumRows>
) {
    let values_btreemap: BTreeMap<(String, String, String, String), StoredValues> = build_metrics_values_btreemap(&details_enable, stored_values);
    let countsum_btreemap: BTreeMap<(String, String, String, String), StoredCountSum> = build_metrics_countsum_btreemap(&details_enable, stored_countsum);
    let countsumrows_btreemap: BTreeMap<(String, String, String, String), StoredCountSumRows> = build_metrics_countsumrows_btreemap(stored_countsumrows);

    (values_btreemap, countsum_btreemap, countsumrows_btreemap)
}
fn build_metrics_values_btreemap(
    details_enable: &bool,
    stored_values: Vec<StoredValues>
) -> BTreeMap<(String, String, String, String), StoredValues>
{
    let mut values_btreemap: BTreeMap<(String, String, String, String), StoredValues> = BTreeMap::new();
    for row in stored_values {
        if row.metric_type == "table" || row.metric_type == "tablet" {
            if *details_enable {
                match values_btreemap.get_mut( &( row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone()) ) {
                    Some( _value_row ) => {
                        panic!("Error: (values_btreemap) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port.clone(), &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                    },
                    None => {
                        values_btreemap.insert( (
                            row.hostname_port.to_string(),
                            row.metric_type.to_string(),
                            row.metric_id.to_string(),
                            row.metric_name.to_string()
                        ), StoredValues {
                            hostname_port: row.hostname_port.to_string(),
                            timestamp: row.timestamp,
                            metric_type: row.metric_type.to_string(),
                            metric_id: row.metric_id.to_string(),
                            attribute_namespace: row.attribute_namespace.to_string(),
                            attribute_table_name: row.attribute_table_name.to_string(),
                            metric_name: row.metric_name.to_string(),
                            metric_value: row.metric_value
                        });
                    }
                }
            } else {
                match values_btreemap.get_mut(&( row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone()) ) {
                    Some(value_row) => {
                        *value_row = StoredValues {
                            hostname_port: value_row.hostname_port.to_string(),
                            timestamp: value_row.timestamp,
                            metric_type: value_row.metric_type.to_string(),
                            metric_id: String::from("-"),
                            attribute_namespace: String::from("-"),
                            attribute_table_name: String::from("-"),
                            metric_name: value_row.metric_name.to_string(),
                            metric_value: value_row.metric_value + row.metric_value
                        }
                    },
                    None => {
                        values_btreemap.insert((
                            row.hostname_port.to_string(),
                            row.metric_type.to_string(),
                            String::from("-"),
                            row.metric_name.to_string()
                        ), StoredValues {
                            hostname_port: row.hostname_port.to_string(),
                            timestamp: row.timestamp,
                            metric_type: row.metric_type.to_string(),
                            metric_id: String::from("-"),
                            attribute_namespace: String::from("-"),
                            attribute_table_name: String::from("-"),
                            metric_name: row.metric_name.to_string(),
                            metric_value: row.metric_value,
                        });
                    }
                }
            }
        } else {
            match values_btreemap.get_mut( &( row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone()) ) {
                Some( _value_row ) => {
                    panic!("Error: (values_btreemap) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port.clone(), &row.metric_type.clone(), String::from("-"), &row.metric_name.clone());
                },
                None => {
                    values_btreemap.insert((
                        row.hostname_port.to_string(),
                        row.metric_type.to_string(),
                        String::from("-"),
                        row.metric_name.to_string()
                    ), StoredValues {
                        hostname_port: row.hostname_port.to_string(),
                        timestamp: row.timestamp,
                        metric_type: row.metric_type.to_string(),
                        metric_id: String::from("-"),
                        attribute_namespace: String::from("-"),
                        attribute_table_name: String::from("-"),
                        metric_name: row.metric_name.to_string(),
                        metric_value: row.metric_value,
                    });
                }
            }
        }
    }
    values_btreemap
}

fn build_metrics_countsum_btreemap(
    details_enable: &bool,
    stored_countsum: Vec<StoredCountSum>
) -> BTreeMap<(String, String, String, String), StoredCountSum>
{
    let mut countsum_btreemap: BTreeMap<(String, String, String, String), StoredCountSum> = BTreeMap::new();
    for row in stored_countsum {
        if row.metric_type == "table" || row.metric_type == "tablet" {
            if *details_enable {
                match countsum_btreemap.get_mut( &( row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone()) ) {
                    Some( _countsum_row ) => {
                        panic!("Error: (countsum_btreemap) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port.clone(), &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                    },
                    None => {
                        countsum_btreemap.insert((
                            row.hostname_port.to_string(),
                            row.metric_type.to_string(),
                            row.metric_id.to_string(),
                            row.metric_name.to_string()
                        ), StoredCountSum {
                            hostname_port: row.hostname_port.to_string(),
                            timestamp: row.timestamp,
                            metric_type: row.metric_type.to_string(),
                            metric_id: row.metric_id.to_string(),
                            attribute_namespace: row.attribute_namespace.to_string(),
                            attribute_table_name: row.attribute_table_name.to_string(),
                            metric_name: row.metric_name.to_string(),
                            metric_total_count: row.metric_total_count,
                            metric_min: 0,
                            metric_mean: 0.0,
                            metric_percentile_75: 0,
                            metric_percentile_95: 0,
                            metric_percentile_99: 0,
                            metric_percentile_99_9: 0,
                            metric_percentile_99_99: 0,
                            metric_max: 0,
                            metric_total_sum: row.metric_total_sum
                        });
                    }
                }
            } else {
                match countsum_btreemap.get_mut(&( row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone()) ) {
                    Some( countsum_row) => {
                        *countsum_row = StoredCountSum {
                            hostname_port: countsum_row.hostname_port.to_string(),
                            timestamp: countsum_row.timestamp,
                            metric_type: countsum_row.metric_type.to_string(),
                            metric_id: String::from("-"),
                            attribute_namespace: String::from("-"),
                            attribute_table_name: String::from("-"),
                            metric_name: countsum_row.metric_name.to_string(),
                            metric_total_count: countsum_row.metric_total_count + row.metric_total_count,
                            metric_min: 0,
                            metric_mean: 0.0,
                            metric_percentile_75: 0,
                            metric_percentile_95: 0,
                            metric_percentile_99: 0,
                            metric_percentile_99_9: 0,
                            metric_percentile_99_99: 0,
                            metric_max: 0,
                            metric_total_sum: countsum_row.metric_total_sum + row.metric_total_sum
                        }
                    },
                    None => {
                        countsum_btreemap.insert((
                            row.hostname_port.to_string(),
                            row.metric_type.to_string(),
                            String::from("-"),
                            row.metric_name.to_string()
                        ), StoredCountSum {
                            hostname_port: row.hostname_port.to_string(),
                            timestamp: row.timestamp,
                            metric_type: row.metric_type.to_string(),
                            metric_id: String::from("-"),
                            attribute_namespace: String::from("-"),
                            attribute_table_name: String::from("-"),
                            metric_name: row.metric_name.to_string(),
                            metric_total_count: row.metric_total_count,
                            metric_min: 0,
                            metric_mean: 0.0,
                            metric_percentile_75: 0,
                            metric_percentile_95: 0,
                            metric_percentile_99: 0,
                            metric_percentile_99_9: 0,
                            metric_percentile_99_99: 0,
                            metric_max: 0,
                            metric_total_sum: row.metric_total_sum
                        });
                    }
                }
            }
        } else {
            match countsum_btreemap.get_mut( &( row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone()) ) {
                Some( _countsum_row ) => {
                    panic!("Error: (countsum_btreemap) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port.clone(), &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                },
                None => {
                    countsum_btreemap.insert((
                        row.hostname_port.to_string(),
                        row.metric_type.to_string(),
                        String::from("-"),
                        row.metric_name.to_string()
                    ), StoredCountSum {
                        hostname_port: row.hostname_port.to_string(),
                        timestamp: row.timestamp,
                        metric_type: row.metric_type.to_string(),
                        metric_id: String::from("-"),
                        attribute_namespace: String::from("-"),
                        attribute_table_name: String::from("-"),
                        metric_name: row.metric_name.to_string(),
                        metric_total_count: row.metric_total_count,
                        metric_min: 0,
                        metric_mean: 0.0,
                        metric_percentile_75: 0,
                        metric_percentile_95: 0,
                        metric_percentile_99: 0,
                        metric_percentile_99_9: 0,
                        metric_percentile_99_99: 0,
                        metric_max: 0,
                        metric_total_sum: row.metric_total_sum
                    });
                }
            }
        }
    }
    countsum_btreemap
}

fn build_metrics_countsumrows_btreemap(
    stored_countsumrows: Vec<StoredCountSumRows>
) -> BTreeMap<(String, String, String, String), StoredCountSumRows>
{
    let mut countsumrows_btreemap: BTreeMap<(String, String, String, String), StoredCountSumRows> = BTreeMap::new();
    for row in stored_countsumrows {
        match countsumrows_btreemap.get_mut( &( row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())  ) {
            Some( _countsumrows_summary_row ) => {
                panic!("Error: (countsumrows_btreemap) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port.clone(), &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
            },
            None => {
                countsumrows_btreemap.insert( (
                    row.hostname_port.to_string(),
                    row.metric_type.to_string(),
                    row.metric_id.to_string(),
                    row.metric_name.to_string()
                ), StoredCountSumRows {
                   hostname_port: row.hostname_port.to_string(),
                   timestamp: row.timestamp,
                   metric_type: row.metric_type.to_string(),
                   metric_id: row.metric_id.to_string(),
                   attribute_namespace: row.attribute_namespace.to_string(),
                   attribute_table_name: row.attribute_table_name.to_string(),
                   metric_name: row.metric_name.to_string(),
                   metric_count: row.metric_count,
                   metric_sum: row.metric_sum,
                   metric_rows: row.metric_rows
                });
            }
        }
    }
    countsumrows_btreemap
}

pub fn insert_first_snapshot_statements(
    stored_statements: Vec<StoredStatements>
) -> BTreeMap<(String, String), SnapshotDiffStatements>
{
    let mut statements_diff: BTreeMap<(String, String), SnapshotDiffStatements> = BTreeMap::new();
    for statement in stored_statements {
        statements_diff.insert( (statement.hostname_port.to_string(), statement.query.to_string()), SnapshotDiffStatements {
            first_snapshot_time: statement.timestamp,
            second_snapshot_time: statement.timestamp,
            first_calls: statement.calls,
            first_total_time: statement.total_time,
            first_rows: statement.rows,
            second_calls: 0,
            second_total_time: 0.0,
            second_rows: 0
        });
    }
    statements_diff
}

pub fn insert_second_snapshot_statements(
    stored_statements: Vec<StoredStatements>,
    statements_diff: &mut BTreeMap<(String, String), SnapshotDiffStatements>,
    first_snapshot_time: &DateTime<Local>
) {
    for statement in stored_statements {
        match statements_diff.get_mut( &(statement.hostname_port.to_string(), statement.query.to_string()) ) {
            Some( statements_diff_row ) => {
                *statements_diff_row = SnapshotDiffStatements {
                    first_snapshot_time: statements_diff_row.first_snapshot_time,
                    second_snapshot_time: statement.timestamp,
                    first_calls: statements_diff_row.first_calls,
                    second_calls: statement.calls,
                    first_total_time: statements_diff_row.first_total_time,
                    second_total_time: statement.total_time,
                    first_rows: statements_diff_row.first_rows,
                    second_rows: statement.rows
                }
            },
            None => {
                statements_diff.insert( (statement.hostname_port.to_string(), statement.query.to_string()), SnapshotDiffStatements {
                    first_snapshot_time: *first_snapshot_time,
                    second_snapshot_time: statement.timestamp,
                    first_calls: 0,
                    second_calls: statement.calls,
                    first_total_time: 0.0,
                    second_total_time: statement.total_time,
                    first_rows: 0,
                    second_rows: statement.rows
                });
            }
        }
    }
}

pub fn insert_first_snapshot_metrics(
    values_map: BTreeMap<(String, String, String, String), StoredValues>,
    countsum_map: BTreeMap<(String, String, String, String), StoredCountSum>,
    countsumrows_map: BTreeMap<(String, String, String, String), StoredCountSumRows>
) -> (
    BTreeMap<(String, String, String, String), SnapshotDiffValues>,
    BTreeMap<(String, String, String, String), SnapshotDiffCountSum>,
    BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows>
) {
    let mut values_diff: BTreeMap<(String, String, String, String), SnapshotDiffValues> = BTreeMap::new();
    for ((hostname_port, metric_type, metric_id, metric_name), storedvalues) in values_map {
        values_diff.insert((hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffValues {
            table_name: storedvalues.attribute_table_name.to_string(),
            namespace: storedvalues.attribute_namespace.to_string(),
            first_snapshot_time: storedvalues.timestamp,
            second_snapshot_time: storedvalues.timestamp,
            first_snapshot_value: storedvalues.metric_value,
            second_snapshot_value: 0
        });
    };

    let mut countsum_diff: BTreeMap<(String, String, String, String), SnapshotDiffCountSum> = BTreeMap::new();
    for ((hostname_port, metric_type, metric_id, metric_name), storedcountsum) in countsum_map {
        countsum_diff.insert( (hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffCountSum {
            table_name: storedcountsum.attribute_table_name.to_string(),
            namespace: storedcountsum.attribute_namespace.to_string(),
            first_snapshot_time: storedcountsum.timestamp,
            second_snapshot_time: storedcountsum.timestamp,
            second_snapshot_total_count: 0,
            second_snapshot_min: 0,
            second_snapshot_mean: 0.0,
            second_snapshot_percentile_75: 0,
            second_snapshot_percentile_95: 0,
            second_snapshot_percentile_99: 0,
            second_snapshot_percentile_99_9: 0,
            second_snapshot_percentile_99_99: 0,
            second_snapshot_max: 0,
            second_snapshot_total_sum: 0,
            first_snapshot_total_count: storedcountsum.metric_total_count,
            first_snapshot_total_sum: storedcountsum.metric_total_sum
        });
    }

    let mut countsumrows_diff: BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows> = BTreeMap::new();
    for ((hostname_port, metric_type, metric_id, metric_name), storedcountsumrows) in countsumrows_map {
        countsumrows_diff.insert( (hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffCountSumRows {
            table_name: storedcountsumrows.attribute_table_name.to_string(),
            namespace: storedcountsumrows.attribute_namespace.to_string(),
            first_snapshot_time: storedcountsumrows.timestamp,
            second_snapshot_time: storedcountsumrows.timestamp,
            first_snapshot_count: storedcountsumrows.metric_count,
            second_snapshot_count: 0,
            first_snapshot_sum: storedcountsumrows.metric_sum,
            second_snapshot_sum: 0,
            first_snapshot_rows: storedcountsumrows.metric_rows,
            second_snapshot_rows: 0
        });
    }

    (values_diff,countsum_diff,countsumrows_diff)
}

pub fn insert_second_snapshot_metrics(
    values_map: BTreeMap<(String, String, String, String), StoredValues>,
    values_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffValues>,
    countsum_map: BTreeMap<(String, String, String, String), StoredCountSum>,
    countsum_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffCountSum>,
    countsumrows_map: BTreeMap<(String, String, String, String), StoredCountSumRows>,
    countsumrows_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows>,
    first_snapshot_time: &DateTime<Local>
) {
    for ((hostname_port, metric_type, metric_id, metric_name), storedvalues) in values_map {
        match values_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string())) {
            Some( values_diff_row ) => {
                *values_diff_row = SnapshotDiffValues {
                    table_name: values_diff_row.table_name.to_string(),
                    namespace: values_diff_row.namespace.to_string(),
                    first_snapshot_time: values_diff_row.first_snapshot_time,
                    second_snapshot_time: storedvalues.timestamp,
                    first_snapshot_value: values_diff_row.first_snapshot_value,
                    second_snapshot_value: storedvalues.metric_value,
                };
            },
            None => {
                values_diff.insert((hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffValues {
                    table_name: storedvalues.attribute_table_name.to_string(),
                    namespace: storedvalues.attribute_namespace.to_string(),
                    first_snapshot_time: *first_snapshot_time,
                    second_snapshot_time: storedvalues.timestamp,
                    first_snapshot_value: 0,
                    second_snapshot_value: storedvalues.metric_value
                });
            }
        }
    }
    for ((hostname_port, metric_type, metric_id, metric_name), storedcountsum) in countsum_map {
        match countsum_diff.get_mut( &(hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string())) {
            Some( countsum_diff_row ) => {
                *countsum_diff_row = SnapshotDiffCountSum {
                    table_name: countsum_diff_row.table_name.to_string(),
                    namespace: countsum_diff_row.namespace.to_string(),
                    first_snapshot_time: countsum_diff_row.first_snapshot_time,
                    second_snapshot_time: storedcountsum.timestamp,
                    second_snapshot_total_count: storedcountsum.metric_total_count,
                    second_snapshot_min: storedcountsum.metric_min,
                    second_snapshot_mean: storedcountsum.metric_mean,
                    second_snapshot_percentile_75: storedcountsum.metric_percentile_75,
                    second_snapshot_percentile_95: storedcountsum.metric_percentile_95,
                    second_snapshot_percentile_99: storedcountsum.metric_percentile_99,
                    second_snapshot_percentile_99_9: storedcountsum.metric_percentile_99_9,
                    second_snapshot_percentile_99_99: storedcountsum.metric_percentile_99_99,
                    second_snapshot_max: storedcountsum.metric_max,
                    second_snapshot_total_sum: storedcountsum.metric_total_sum,
                    first_snapshot_total_count: countsum_diff_row.first_snapshot_total_count,
                    first_snapshot_total_sum: countsum_diff_row.first_snapshot_total_sum
                }
            },
            None => {
                countsum_diff.insert((hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffCountSum {
                    table_name: storedcountsum.attribute_table_name.to_string(),
                    namespace: storedcountsum.attribute_namespace.to_string(),
                    first_snapshot_time: *first_snapshot_time,
                    second_snapshot_time: storedcountsum.timestamp,
                    second_snapshot_total_count: storedcountsum.metric_total_count,
                    second_snapshot_min: storedcountsum.metric_min,
                    second_snapshot_mean: storedcountsum.metric_mean,
                    second_snapshot_percentile_75: storedcountsum.metric_percentile_75,
                    second_snapshot_percentile_95: storedcountsum.metric_percentile_95,
                    second_snapshot_percentile_99: storedcountsum.metric_percentile_99,
                    second_snapshot_percentile_99_9: storedcountsum.metric_percentile_99_9,
                    second_snapshot_percentile_99_99: storedcountsum.metric_percentile_99_99,
                    second_snapshot_max: storedcountsum.metric_max,
                    second_snapshot_total_sum: storedcountsum.metric_total_sum,
                    first_snapshot_total_count: 0,
                    first_snapshot_total_sum: 0
                });
            }
        }
    }
    for ((hostname_port, metric_type, metric_id, metric_name), storedcountsumrows) in countsumrows_map {
        match countsumrows_diff.get_mut( &(hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string())) {
            Some( countsumrows_diff_row ) => {
                *countsumrows_diff_row = SnapshotDiffCountSumRows {
                    table_name: countsumrows_diff_row.table_name.to_string(),
                    namespace: countsumrows_diff_row.namespace.to_string(),
                    first_snapshot_time: countsumrows_diff_row.first_snapshot_time,
                    second_snapshot_time: storedcountsumrows.timestamp,
                    first_snapshot_count: countsumrows_diff_row.first_snapshot_count,
                    first_snapshot_sum: countsumrows_diff_row.first_snapshot_sum,
                    first_snapshot_rows: countsumrows_diff_row.first_snapshot_rows,
                    second_snapshot_count: storedcountsumrows.metric_count,
                    second_snapshot_sum: storedcountsumrows.metric_sum,
                    second_snapshot_rows: storedcountsumrows.metric_rows
                }
            },
            None => {
                countsumrows_diff.insert( (hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffCountSumRows {
                    table_name: storedcountsumrows.attribute_table_name.to_string(),
                    namespace: storedcountsumrows.attribute_namespace.to_string(),
                    first_snapshot_time: *first_snapshot_time,
                    second_snapshot_time: storedcountsumrows.timestamp,
                    first_snapshot_count: 0,
                    first_snapshot_sum: 0,
                    first_snapshot_rows: 0,
                    second_snapshot_count: storedcountsumrows.metric_count,
                    second_snapshot_sum: storedcountsumrows.metric_sum,
                    second_snapshot_rows: storedcountsumrows.metric_rows
                });
            }
        }
    }
}

pub fn print_diff_statements(
    statements_diff: &BTreeMap<(String, String), SnapshotDiffStatements>,
    hostname_filter: &Regex,
) {
    for ((hostname, query), statements_row) in statements_diff {
        if hostname_filter.is_match(&hostname)
        && statements_row.second_calls - statements_row.first_calls != 0 {
            let adaptive_length = if query.len() < 50 { query.len() } else { 50 };
            println!("{:20} {:10} avg.time: {:15.3} ms avg.rows: {:10} : {:50}",
                     hostname,
                     statements_row.second_calls - statements_row.first_calls,
                     (statements_row.second_total_time - statements_row.first_total_time) / (statements_row.second_calls as f64 - statements_row.first_calls as f64),
                     (statements_row.second_rows - statements_row.first_rows) / (statements_row.second_calls - statements_row.first_calls),
                     query.substring(0, adaptive_length).replace("\n", "")
            );
        }
    }
}

pub fn print_diff(value_diff: &BTreeMap<(String, String, String, String), SnapshotDiffValues>,
                  countsum_diff: &BTreeMap<(String, String, String, String), SnapshotDiffCountSum>,
                  countsumrows_diff: &BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows>,
                  hostname_filter: &Regex,
                  stat_name_filter: &Regex,
                  table_name_filter: &Regex,
                  details_enable: &bool,
                  gauges_enable: &bool
) {
    // value_diff
    let value_statistic_details_lookup = value_create_hashmap();
    for ((hostname, metric_type, metric_id, metric_name), value_diff_row) in value_diff {
        if hostname_filter.is_match(&hostname)
        && stat_name_filter.is_match(&metric_name)
        && table_name_filter.is_match(&value_diff_row.table_name) {
            let details = match value_statistic_details_lookup.get(&metric_name.to_string()) {
                Some(x) => { ValueStatisticDetails { unit: x.unit.to_string(), unit_suffix: x.unit_suffix.to_string(), stat_type: x.stat_type.to_string() } },
                None => { ValueStatisticDetails { unit: String::from("?"), unit_suffix: String::from("?"), stat_type: String::from("?") } }
            };
            let adaptive_length = if metric_id.len() < 15 { 0 } else { metric_id.len() - 15 };
            if details.stat_type != "gauge"
            && value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value != 0 {
                if *details_enable {
                    println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:6} {:>15.3}/s",
                             hostname,
                             metric_type,
                             metric_id.substring(adaptive_length, metric_id.len()),
                             value_diff_row.namespace,
                             value_diff_row.table_name,
                             metric_name,
                             value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value,
                             details.unit_suffix,
                             ((value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value) as f64 / (value_diff_row.second_snapshot_time - value_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000 as f64)
                    );
                } else {
                    println!("{:20} {:8} {:70} {:15} {:6} {:>15.3}/s",
                             hostname,
                             metric_type,
                             metric_name,
                             value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value,
                             details.unit_suffix,
                             ((value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value) as f64 / (value_diff_row.second_snapshot_time - value_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000 as f64)
                    );
                }
            }
            if details.stat_type == "gauge"
            && *gauges_enable {
                if *details_enable {
                    println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:6} {:+15}",
                             hostname,
                             metric_type,
                             metric_id.substring(adaptive_length, metric_id.len()),
                             value_diff_row.namespace,
                             value_diff_row.table_name,
                             metric_name,
                             value_diff_row.second_snapshot_value,
                             details.unit_suffix,
                             value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value
                    );
                } else {
                    println!("{:20} {:8} {:70} {:15} {:6} {:+15}",
                             hostname,
                             metric_type,
                             metric_name,
                             value_diff_row.second_snapshot_value,
                             details.unit_suffix,
                             value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value
                    );
                }
            }
        }
    }
    // countsum_diff
    let countsum_statistic_details_lookup = countsum_create_hashmap();
    for ((hostname, metric_type, metric_id, metric_name), countsum_diff_row) in countsum_diff {
        if hostname_filter.is_match(&hostname)
        && stat_name_filter.is_match(&metric_name)
        && table_name_filter.is_match(&countsum_diff_row.table_name) {
            let details = match countsum_statistic_details_lookup.get(&metric_name.to_string()) {
                Some(x) => { CountSumStatisticDetails { unit: x.unit.to_string(), unit_suffix: x.unit_suffix.to_string(), divisor: x.divisor, stat_type: x.stat_type.to_string() } },
                None => { CountSumStatisticDetails { unit: String::from("?"), unit_suffix: String::from("?"), divisor: 0, stat_type: String::from("?") } }
            };
            let adaptive_length = if metric_id.len() < 15 { 0 } else { metric_id.len() - 15 };
            if countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count != 0 {
                if *details_enable {
                    println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:>15.3}/s avg: {:9.0} tot: {:>15.3} {:10}",
                             hostname,
                             metric_type,
                             metric_id.substring(adaptive_length, metric_id.len()),
                             countsum_diff_row.namespace,
                             countsum_diff_row.table_name,
                             metric_name,
                             countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count,
                             (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count) as f64 / (countsum_diff_row.second_snapshot_time - countsum_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000 as f64,
                             ((countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum) / (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count)) as f64,
                             countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum,
                             details.unit_suffix
                    );
                } else {
                    println!("{:20} {:8} {:70} {:15} {:>15.3}/s avg: {:9.0} tot: {:>15.3} {:10}",
                             hostname,
                             metric_type,
                             metric_name,
                             countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count,
                             (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count) as f64 / (countsum_diff_row.second_snapshot_time - countsum_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000 as f64,
                             ((countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum) / (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count)) as f64,
                             countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum,
                             details.unit_suffix
                    );
                }
            }
        }
    }
    // countsumrows_diff
    for ((hostname, _metric_type, _metric_id, metric_name), countsumrows_diff_row) in countsumrows_diff {
        if hostname_filter.is_match( &hostname)
        && stat_name_filter.is_match(&metric_name) {
            if countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count != 0 {
                println!("{:20} {:70} {:>15} avg: {:>15.3} ms, avg.rows: {:>15}",
                    hostname,
                    metric_name,
                    countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count,
                    ((countsumrows_diff_row.second_snapshot_sum as f64 - countsumrows_diff_row.first_snapshot_sum as f64)/1000.0) / (countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count) as f64,
                    countsumrows_diff_row.second_snapshot_rows - countsumrows_diff_row.first_snapshot_rows / (countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count)
                );
            }
        }
    }
}