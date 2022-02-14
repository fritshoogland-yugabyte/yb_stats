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
use std::path::Path;
use std::env;
use regex::Regex;
use substring::Substring;

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

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub number: i32,
    pub timestamp: DateTime<Local>,
    pub comment: String,
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
/*
pub fn read_version( hostname: &str) -> VersionData {
    if ! scan_port_addr(hostname) {
        println!("Warning! hostname:port {} cannot be reached, skipping", hostname.to_string());
        return parse_version(String::from(""))
    };
    let data_from_http = reqwest::blocking::get(format!("http://{}/api/v1/version", hostname.to_string())).unwrap()
        /*.unwrap_or_else(|e| {
            eprintln!("Fatal: error reading from URL: {}", e);
            //process::exit(1);
        })
         */
        .text().unwrap();
    parse_version(data_from_http)
}
 */
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

pub fn add_to_version_vector(versiondata: VersionData,
    hostname: &str,
    snapshot_time: DateTime<Local>,
    stored_versiondata: &mut Vec<StoredVersionData>
) {
   //for versiondata_row in versiondata {
       stored_versiondata.push( StoredVersionData {
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
           build_number: versiondata.build_number.to_string()
       });
   //}
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
        /*
        let metric_attribute_namespace_name = match &metric.attributes.namespace_name {
            Some(namespace_name) => namespace_name.to_string(),
            None => "-".to_string(),
        };
         */
        let metric_attribute_table_name = match &metric.attributes {
            None => String::from("-"),
            Some(attribute) => {
                match &attribute.table_name {
                    Some(table_name) => table_name.to_string(),
                    None => String::from("-"),
                }
            }
        };
        /*
        let metric_attribute_table_name = match &metric.attributes.table_name {
            Some(table_name) => table_name.to_string(),
            None => "-".to_string(),
        };
         */
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

    for hostname in &hostname_port_vec {
        let detail_snapshot_time = Local::now();
        // metrics
        let data_parsed_from_json = read_metrics(&hostname);
        add_to_metric_vectors(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_values, &mut stored_countsum, &mut stored_countsumrows);
        let data_parsed_from_json = read_version(&hostname);
        add_to_version_vector(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_versiondata);
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

    snapshot_number
}

pub fn parse_metrics( metrics_data: String ) -> Vec<Metrics> {
    serde_json::from_str(&metrics_data )
        .unwrap_or_else(|e| {
            println!("Warning: error parsing /metrics json data: {}", e);
            return Vec::<Metrics>::new();
        })
}

pub fn parse_version( version_data: String ) -> VersionData {
    serde_json::from_str( &version_data )
        .unwrap_or_else(|_e| {
            //println!("Warning: issue parsing /api/v1/version json data: {}", e);
            return VersionData { git_hash: "".to_string(), build_hostname: "".to_string(), build_timestamp: "".to_string(), build_username: "".to_string(), build_clean_repo: true, build_id: "".to_string(), build_type: "".to_string(), version_number: "".to_string(), build_number: "".to_string() };
        })
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
pub fn build_summary(values_summary: &mut BTreeMap<(String, String, String, String), StoredValues>,
                     stored_values: &Vec<StoredValues>,
                     countsum_summary: &mut BTreeMap<(String, String, String, String), StoredCountSum>,
                     stored_countsum: &Vec<StoredCountSum>,
                     countsumrows_summary: &mut BTreeMap<(String, String, String, String), StoredCountSumRows>,
                     stored_countsumrows: &Vec<StoredCountSumRows>,
) {
    for row in stored_values {
        if row.metric_type == "table" || row.metric_type == "tablet" {
            match values_summary.get_mut(&(row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone())) {
                Some(values_summary_row) => {
                    *values_summary_row = StoredValues {
                        hostname_port: values_summary_row.hostname_port.to_string(),
                        timestamp: values_summary_row.timestamp,
                        metric_type: values_summary_row.metric_type.to_string(),
                        metric_id: values_summary_row.metric_id.to_string(),
                        attribute_namespace: values_summary_row.attribute_namespace.to_string(),
                        attribute_table_name: values_summary_row.attribute_table_name.to_string(),
                        metric_name: values_summary_row.metric_name.to_string(),
                        metric_value: values_summary_row.metric_value + row.metric_value
                    }
                },
                None => {
                    values_summary.insert((row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone()), StoredValues {
                        hostname_port: row.hostname_port.to_string(),
                        timestamp: row.timestamp,
                        metric_type: row.metric_type.to_string(),
                        metric_id: String::from("-"),
                        attribute_namespace: String::from("-"),
                        attribute_table_name: String::from("-"),
                        metric_name: row.metric_name.to_string(),
                        metric_value: row.metric_value
                    });
                }
            }
        } else {
            match values_summary.get_mut(&(row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone())) {
                Some( _values_summary_row) => {
                    panic!("Error: (values_summary) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port.clone(), &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                    /*
                    *values_summary_row = StoredValues {
                        hostname_port: values_summary_row.hostname_port.to_string(),
                        timestamp: values_summary_row.timestamp,
                        metric_type: values_summary_row.metric_type.to_string(),
                        metric_id: values_summary_row.metric_id.to_string(),
                        attribute_namespace: values_summary_row.attribute_namespace.to_string(),
                        attribute_table_name: values_summary_row.attribute_table_name.to_string(),
                        metric_name: values_summary_row.metric_name.to_string(),
                        metric_value: values_summary_row.metric_value
                    }
                    */
                },
                None => {
                    values_summary.insert((row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone()), StoredValues {
                        hostname_port: row.hostname_port.to_string(),
                        timestamp: row.timestamp,
                        metric_type: row.metric_type.to_string(),
                        metric_id: String::from("-"),
                        attribute_namespace: String::from("-"),
                        attribute_table_name: String::from("-"),
                        metric_name: row.metric_name.to_string(),
                        metric_value: row.metric_value
                    });
                }
            }
        }
    }
    for row in stored_countsum {
        if row.metric_type == "table" || row.metric_type == "tablet" {
            match countsum_summary.get_mut(&(row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone())) {
                Some(countsum_summary_row) => {
                    *countsum_summary_row = StoredCountSum {
                        hostname_port: countsum_summary_row.hostname_port.to_string(),
                        timestamp: countsum_summary_row.timestamp,
                        metric_type: countsum_summary_row.metric_type.to_string(),
                        metric_id: countsum_summary_row.metric_id.to_string(),
                        attribute_namespace: countsum_summary_row.attribute_namespace.to_string(),
                        attribute_table_name: countsum_summary_row.attribute_table_name.to_string(),
                        metric_name: countsum_summary_row.metric_name.to_string(),
                        metric_total_count: countsum_summary_row.metric_total_count + row.metric_total_count,
                        metric_min: 0,
                        metric_mean: 0.0,
                        metric_percentile_75: 0,
                        metric_percentile_95: 0,
                        metric_percentile_99: 0,
                        metric_percentile_99_9: 0,
                        metric_percentile_99_99: 0,
                        metric_max: 0,
                        metric_total_sum: countsum_summary_row.metric_total_sum + row.metric_total_sum
                    }
                },
                None => {
                    countsum_summary.insert((row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone()), StoredCountSum {
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
        } else {
            match countsum_summary.get_mut(&(row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone())) {
                Some(_countsum_summary_row) => {
                    panic!("Error: (countsum_summary) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port.clone(), &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                    /*
                    *countsum_summary_row = StoredCountSum {
                        hostname_port: countsum_summary_row.hostname_port.to_string(),
                        timestamp: countsum_summary_row.timestamp,
                        metric_type: countsum_summary_row.metric_type.to_string(),
                        metric_id: countsum_summary_row.metric_id.to_string(),
                        attribute_namespace: countsum_summary_row.attribute_namespace.to_string(),
                        attribute_table_name: countsum_summary_row.attribute_table_name.to_string(),
                        metric_name: countsum_summary_row.metric_name.to_string(),
                        metric_total_count: countsum_summary_row.metric_total_count + row.metric_total_count,
                        metric_min: 0,
                        metric_mean: 0.0,
                        metric_percentile_75: 0,
                        metric_percentile_95: 0,
                        metric_percentile_99: 0,
                        metric_percentile_99_9: 0,
                        metric_percentile_99_99: 0,
                        metric_max: 0,
                        metric_total_sum: countsum_summary_row.metric_total_sum
                    }
                    */
                },
                None => {
                    countsum_summary.insert((row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone()), StoredCountSum {
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
    for row in stored_countsumrows {
        match countsumrows_summary.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
            Some(_countsumrows_summary_row) => {
                panic!("Error: (countsumrows_detail) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port.clone(), &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                /*
                *countsumrows_summary_row = StoredCountSumRows {
                    hostname_port: countsumrows_summary_row.hostname_port.to_string(),
                    timestamp: countsumrows_summary_row.timestamp,
                    metric_type: countsumrows_summary_row.metric_type.to_string(),
                    metric_id: countsumrows_summary_row.metric_id.to_string(),
                    attribute_namespace: countsumrows_summary_row.attribute_namespace.to_string(),
                    attribute_table_name: countsumrows_summary_row.attribute_table_name.to_string(),
                    metric_name: countsumrows_summary_row.metric_name.to_string(),
                    metric_count: countsumrows_summary_row.metric_count,
                    metric_sum: countsumrows_summary_row.metric_sum,
                    metric_rows: countsumrows_summary_row.metric_rows
                });
                */
            },
            None => {
                countsumrows_summary.insert( (row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone()), StoredCountSumRows {
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
}
pub fn build_detail(values_detail: &mut BTreeMap<(String, String, String, String), StoredValues>,
                    stored_values: &Vec<StoredValues>,
                    countsum_detail: &mut BTreeMap<(String, String, String, String), StoredCountSum>,
                    stored_countsum: &Vec<StoredCountSum>,
                    countsumrows_detail: &mut BTreeMap<(String, String, String, String), StoredCountSumRows>,
                    stored_countsumrows: &Vec<StoredCountSumRows>,
) {
    for row in stored_values {
            match values_detail.get_mut(&(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
                Some(_values_detail_row) => {
                    panic!("Error: (values_detail) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port.clone(), &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                    /*
                    *values_detail_row = StoredValues {
                        hostname_port: values_detail_row.hostname_port.to_string(),
                        timestamp: values_detail_row.timestamp,
                        metric_type: values_detail_row.metric_type.to_string(),
                        metric_id: values_detail_row.metric_id.to_string(),
                        attribute_namespace: values_detail_row.attribute_namespace.to_string(),
                        attribute_table_name: values_detail_row.attribute_table_name.to_string(),
                        metric_name: values_detail_row.metric_name.to_string(),
                        metric_value: values_detail_row.metric_value
                    }
                     */
                },
                None => {
                    values_detail.insert((row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone()), StoredValues {
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
    }
    for row in stored_countsum {
        match countsum_detail.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
            Some(_countsum_detail_row) => {
                panic!("Error: (countsum_detail) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port.clone(), &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                /*
                *countsum_detail_row = StoredCountSum {
                    hostname_port: countsum_detail_row.hostname_port.to_string(),
                    timestamp: countsum_detail_row.timestamp,
                    metric_type: countsum_detail_row.metric_type.to_string(),
                    metric_id: countsum_detail_row.metric_id.to_string(),
                    attribute_namespace: countsum_detail_row.attribute_namespace.to_string(),
                    attribute_table_name: countsum_detail_row.attribute_table_name.to_string(),
                    metric_name: countsum_detail_row.metric_name.to_string(),
                    metric_total_count: countsum_detail_row.metric_total_count,
                    metric_min: countsum_detail_row.metric_min,
                    metric_mean: countsum_detail_row.metric_mean,
                    metric_percentile_75: countsum_detail_row.metric_percentile_75,
                    metric_percentile_95: countsum_detail_row.metric_percentile_95,
                    metric_percentile_99: countsum_detail_row.metric_percentile_99,
                    metric_percentile_99_9: countsum_detail_row.metric_percentile_99_9,
                    metric_percentile_99_99: countsum_detail_row.metric_percentile_99_99,
                    metric_max: countsum_detail_row.metric_max,
                    metric_total_sum: countsum_detail_row.metric_total_sum
                }
                */
            },
            None => {
                countsum_detail.insert( (row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone()), StoredCountSum {
                    hostname_port: row.hostname_port.to_string(),
                    timestamp: row.timestamp,
                    metric_type: row.metric_type.to_string(),
                    metric_id: row.metric_id.to_string(),
                    attribute_namespace: row.attribute_namespace.to_string(),
                    attribute_table_name: row.attribute_table_name.to_string(),
                    metric_name: row.metric_name.to_string(),
                    metric_total_count: row.metric_total_count,
                    metric_min: row.metric_min,
                    metric_mean: row.metric_mean,
                    metric_percentile_75: row.metric_percentile_75,
                    metric_percentile_95: row.metric_percentile_95,
                    metric_percentile_99: row.metric_percentile_99,
                    metric_percentile_99_9: row.metric_percentile_99_9,
                    metric_percentile_99_99: row.metric_percentile_99_99,
                    metric_max: row.metric_max,
                    metric_total_sum: row.metric_total_sum
                });
            }
        }
    }
    for row in stored_countsumrows {
        match countsumrows_detail.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
            Some(_countsumrows_detail_row) => {
                panic!("Error: (countsumrows_detail) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port.clone(), &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                /*
                *countsumrows_detail_row = StoredCountSumRows {
                    hostname_port: countsumrows_detail_row.hostname_port.to_string(),
                    timestamp: countsumrows_detail_row.timestamp,
                    metric_type: countsumrows_detail_row.metric_type.to_string(),
                    metric_id: countsumrows_detail_row.metric_id.to_string(),
                    attribute_namespace: countsumrows_detail_row.attribute_namespace.to_string(),
                    attribute_table_name: countsumrows_detail_row.attribute_table_name.to_string(),
                    metric_name: countsumrows_detail_row.metric_name.to_string(),
                    metric_count: countsumrows_detail_row.metric_count,
                    metric_sum: countsumrows_detail_row.metric_sum,
                    metric_rows: countsumrows_detail_row.metric_rows
                });
                */
            },
            None => {
                countsumrows_detail.insert( (row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone()), StoredCountSumRows {
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
}

pub fn insert_first_snap_into_diff(values_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffValues>,
                                   values_btree: &BTreeMap<(String, String, String, String), StoredValues>,
                                   countsum_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffCountSum>,
                                   countsum_btree: &BTreeMap<(String, String, String, String), StoredCountSum>,
                                   countsumrows_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows>,
                                   countsumrows_btree: &BTreeMap<(String, String, String, String), StoredCountSumRows>
) {
    for ((hostname_port, metric_type, metric_id, metric_name), storedvalues) in values_btree {
        values_diff.insert((hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffValues {
            table_name: storedvalues.attribute_table_name.to_string(),
            namespace: storedvalues.attribute_namespace.to_string(),
            first_snapshot_time: storedvalues.timestamp,
            second_snapshot_time: storedvalues.timestamp,
            first_snapshot_value: storedvalues.metric_value,
            second_snapshot_value: 0
        });
    }
    for ((hostname_port, metric_type, metric_id, metric_name), storedcountsum) in countsum_btree {
        countsum_diff.insert( (hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffCountSum{
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
    for ((hostname_port, metric_type, metric_id, metric_name), storedcountsumrows) in countsumrows_btree {
        countsumrows_diff.insert( (hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffCountSumRows{
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
}
pub fn insert_second_snap_into_diff(values_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffValues>,
                                    values_btree: &BTreeMap<(String, String, String, String), StoredValues>,
                                    countsum_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffCountSum>,
                                    countsum_btree: &BTreeMap<(String, String, String, String), StoredCountSum>,
                                    countsumrows_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows>,
                                    countsumrows_btree: &BTreeMap<(String, String, String, String), StoredCountSumRows>,
                                    first_snapshot_time: &DateTime<Local>
) {
    for ((hostname_port, metric_type, metric_id, metric_name), storedvalues) in values_btree {
        match values_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string())) {
            Some(values_diff_row) => {
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
    for ((hostname_port, metric_type, metric_id, metric_name), storedcountsum) in countsum_btree {
        match countsum_diff.get_mut( &(hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string())) {
            Some(countsum_diff_row) => {
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
    for ((hostname_port, metric_type, metric_id, metric_name), storedcountsumrows) in countsumrows_btree {
        match countsumrows_diff.get_mut( &(hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string())) {
            Some(countsumrows_diff_row) => {
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
            if details.stat_type != "gauge" {
                if value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value != 0 {
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
            }
            if details.stat_type == "gauge" && *gauges_enable {
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
                    println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:>15.3}/s avg.time: {:<9.0} tot: {:>15.3} {:10}",
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
                    println!("{:20} {:8} {:70} {:15} {:>15.3}/s avg.time: {:<9.0} tot: {:>15.3} {:10}",
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
                println!("{:20} {:70} {:>15} avg.time: {:>15} ms, avg.rows: {:>15}",
                    hostname,
                    metric_name,
                    countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count,
                    (countsumrows_diff_row.second_snapshot_sum - countsumrows_diff_row.first_snapshot_sum) / (countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count),
                    countsumrows_diff_row.second_snapshot_rows - countsumrows_diff_row.first_snapshot_rows / (countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count)
                );
            }
        }
    }
}

pub fn build_value_diffs( value_statistics: &mut BTreeMap<(String, String, String, String), StoredValues>,
                          stored_values: &Vec<StoredValues>
                         ) {
    for row in stored_values {
        value_statistics.insert( (row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone()), StoredValues {
            hostname_port: row.hostname_port.clone(),
            timestamp: row.timestamp,
            metric_type: row.metric_type.clone(),
            metric_id: row.metric_id.clone(),
            attribute_namespace: row.attribute_namespace.clone(),
            attribute_table_name: row.attribute_table_name.clone(),
            metric_name: row.metric_name.clone(),
            metric_value: row.metric_value
        });
    }
}