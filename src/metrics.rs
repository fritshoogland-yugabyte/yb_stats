use std::process;
use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use std::collections::BTreeMap;
use serde_derive::{Serialize,Deserialize};
use std::path::PathBuf;
use std::fs;
use regex::Regex;
use std::env;
use substring::Substring;

//mod value_statistic_details;
use crate::value_statistic_details::{ValueStatisticDetails, value_create_hashmap};
//mod countsum_statistic_details;
use crate::countsum_statistic_details::{CountSumStatisticDetails, countsum_create_hashmap};


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
        count: u64,
        sum: u64,
        rows: u64,
    },
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
    pub metric_count: u64,
    pub metric_sum: u64,
    pub metric_rows: u64,
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
    pub first_snapshot_count: u64,
    pub first_snapshot_sum: u64,
    pub first_snapshot_rows: u64,
    pub second_snapshot_count: u64,
    pub second_snapshot_sum: u64,
    pub second_snapshot_rows: u64,
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

pub fn read_metrics_into_vectors(
    hostname_port_vec: &Vec<&str>
) -> (Vec<StoredValues>, Vec<StoredCountSum>, Vec<StoredCountSumRows>) {
    let mut stored_values: Vec<StoredValues> = Vec::new();
    let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
    let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
    for hostname_port in hostname_port_vec {
        let detail_snapshot_time = Local::now();
        let metrics = read_metrics(&hostname_port);
        add_to_metric_vectors(metrics, hostname_port, detail_snapshot_time, &mut stored_values, &mut stored_countsum, &mut stored_countsumrows);
    }
    (stored_values, stored_countsum, stored_countsumrows)
}

#[allow(dead_code)]
pub fn perform_metrics_snapshot(
    hostname_port_vec: &Vec<&str>,
    snapshot_number: i32,
    yb_stats_directory: &PathBuf
) {
    let (stored_values, stored_countsum, stored_countsumrows) = read_metrics_into_vectors(hostname_port_vec);

    let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
    let values_file = &current_snapshot_directory.join("values");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&values_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing values data in snapshot directory {}: {}", &values_file.clone().into_os_string().into_string().unwrap(), e);
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
            eprintln!("Fatal: error writing countsum data in snapshot directory {}: {}", &countsum_file.clone().into_os_string().into_string().unwrap(), e);
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
            eprintln!("Fatal: error writing countsumrows data in snapshot directory {}: {}", &countsumrows_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_countsumrows {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();
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

fn parse_metrics( metrics_data: String ) -> Vec<Metrics> {
    serde_json::from_str(&metrics_data )
        .unwrap_or_else(|e| {
            println!("Warning: error parsing /metrics json data: {}", e);
            return Vec::<Metrics>::new();
        })
}

pub fn build_metrics_btreemaps(
    stored_values: Vec<StoredValues>,
    stored_countsum: Vec<StoredCountSum>,
    stored_countsumrows: Vec<StoredCountSumRows>
) -> (
    BTreeMap<(String, String, String, String), StoredValues>,
    BTreeMap<(String, String, String, String), StoredCountSum>,
    BTreeMap<(String, String, String, String), StoredCountSumRows>
) {
    let values_btreemap: BTreeMap<(String, String, String, String), StoredValues> = build_metrics_values_btreemap(stored_values);
    let countsum_btreemap: BTreeMap<(String, String, String, String), StoredCountSum> = build_metrics_countsum_btreemap(stored_countsum);
    let countsumrows_btreemap: BTreeMap<(String, String, String, String), StoredCountSumRows> = build_metrics_countsumrows_btreemap(stored_countsumrows);

    (values_btreemap, countsum_btreemap, countsumrows_btreemap)
}

fn build_metrics_values_btreemap(
    stored_values: Vec<StoredValues>
) -> BTreeMap<(String, String, String, String), StoredValues>
{
    let mut values_btreemap: BTreeMap<(String, String, String, String), StoredValues> = BTreeMap::new();
    for row in stored_values {
        if row.metric_type == "table" || row.metric_type == "tablet" {
            match values_btreemap.get_mut(&(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
                Some(_value_row) => {
                    panic!("Error: (values_btreemap) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port.clone(), &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                },
                None => {
                    values_btreemap.insert((
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
                        metric_value: row.metric_value,
                    });
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
    stored_countsum: Vec<StoredCountSum>
) -> BTreeMap<(String, String, String, String), StoredCountSum>
{
    let mut countsum_btreemap: BTreeMap<(String, String, String, String), StoredCountSum> = BTreeMap::new();
    for row in stored_countsum {
        if row.metric_type == "table" || row.metric_type == "tablet" {
            match countsum_btreemap.get_mut(&(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
                Some(_countsum_row) => {
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
                        metric_total_sum: row.metric_total_sum,
                    });
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

pub fn print_metrics_diff_for_snapshots(
    begin_snapshot: &String,
    end_snapshot: &String,
    begin_snapshot_timestamp: &DateTime<Local>,
    hostname_filter: &Regex,
    stat_name_filter: &Regex,
    table_name_filter: &Regex,
    details_enable: &bool,
    gauges_enable: &bool
) {
    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    let stored_values: Vec<StoredValues> = read_values_snapshot(&begin_snapshot, &yb_stats_directory);
    let stored_countsum: Vec<StoredCountSum> = read_countsum_snapshot(&begin_snapshot, &yb_stats_directory);
    let stored_countsumrows: Vec<StoredCountSumRows> = read_countsumrows_snapshot(&begin_snapshot, &yb_stats_directory);
    let (values_map, countsum_map, countsumrows_map) = build_metrics_btreemaps(stored_values, stored_countsum, stored_countsumrows);
    let (mut values_diff, mut countsum_diff, mut countsumrows_diff) = insert_first_snapshot_metrics(values_map, countsum_map, countsumrows_map);

    let stored_values: Vec<StoredValues> = read_values_snapshot(&end_snapshot, &yb_stats_directory);
    let stored_countsum: Vec<StoredCountSum> = read_countsum_snapshot(&end_snapshot, &yb_stats_directory);
    let stored_countsumrows: Vec<StoredCountSumRows> = read_countsumrows_snapshot(&end_snapshot, &yb_stats_directory);
    let (values_map, countsum_map, countsumrows_map) = build_metrics_btreemaps(stored_values, stored_countsum, stored_countsumrows);
    insert_second_snapshot_metrics(values_map, &mut values_diff, countsum_map, &mut countsum_diff, countsumrows_map, &mut countsumrows_diff, &begin_snapshot_timestamp);

    print_diff_metrics(&values_diff, &countsum_diff, &countsumrows_diff, &hostname_filter, &stat_name_filter, &table_name_filter, &details_enable, &gauges_enable);
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

pub fn print_diff_metrics(value_diff: &BTreeMap<(String, String, String, String), SnapshotDiffValues>,
                  countsum_diff: &BTreeMap<(String, String, String, String), SnapshotDiffCountSum>,
                  countsumrows_diff: &BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows>,
                  hostname_filter: &Regex,
                  stat_name_filter: &Regex,
                  table_name_filter: &Regex,
                  details_enable: &bool,
                  gauges_enable: &bool
) {
    if *details_enable {
        // value_diff
        let value_statistic_details_lookup = value_create_hashmap();
        for ((hostname, metric_type, metric_id, metric_name), value_diff_row) in value_diff {
            if value_diff_row.second_snapshot_value > 0 {
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
                            println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:6} {:>15.3} /s",
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
                            println!("{:20} {:8} {:70} {:15} {:6} {:>15.3} /s",
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
        }
        // countsum_diff
        let countsum_statistic_details_lookup = countsum_create_hashmap();
        for ((hostname, metric_type, metric_id, metric_name), countsum_diff_row) in countsum_diff {
            if countsum_diff_row.second_snapshot_total_count > 0 {
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
                            println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15}        {:>15.3} /s avg: {:9.0} tot: {:>15.3} {:10}",
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
                            println!("{:20} {:8} {:70} {:15}        {:>15.3} /s avg: {:9.0} tot: {:>15.3} {:10}",
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
        }
    } else {
        // value_diff
        let value_statistic_details_lookup = value_create_hashmap();
        let mut sum_value_diff: BTreeMap<(String, String, String, String), SnapshotDiffValues> = BTreeMap::new();
        for ((hostname_port, metric_type, _metric_id, metric_name), value_diff_row) in value_diff {
            if metric_type == "table" || metric_type == "tablet" {
                // if a table and thus its tablets have been deleted between the first and second snapshot, the second_snapshot_value is 0.
                // however, the first_snapshot_value is > 0, it means it can make the subtraction between the second and the first snapshot get negative, and a summary overview be incorrect.
                // therefore we remove individual statistics where the second snapshot value is set to 0.
                if value_diff_row.second_snapshot_value > 0 {
                    match sum_value_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string())) {
                        Some(sum_value_diff_row) => {
                            *sum_value_diff_row = SnapshotDiffValues {
                                table_name: sum_value_diff_row.table_name.to_string(),
                                namespace: sum_value_diff_row.namespace.to_string(),
                                first_snapshot_time: sum_value_diff_row.first_snapshot_time,
                                second_snapshot_time: sum_value_diff_row.second_snapshot_time,
                                first_snapshot_value: sum_value_diff_row.first_snapshot_value + value_diff_row.first_snapshot_value,
                                second_snapshot_value: sum_value_diff_row.second_snapshot_value + value_diff_row.second_snapshot_value,
                            }
                        },
                        None => {
                            sum_value_diff.insert((
                                                      hostname_port.to_string(),
                                                      metric_type.to_string(),
                                                      String::from("-"),
                                                      metric_name.to_string()
                                                  ), SnapshotDiffValues {
                                table_name: String::from("-"),
                                namespace: String::from("-"),
                                first_snapshot_time: value_diff_row.first_snapshot_time,
                                second_snapshot_time: value_diff_row.second_snapshot_time,
                                first_snapshot_value: value_diff_row.first_snapshot_value,
                                second_snapshot_value: value_diff_row.second_snapshot_value
                            });
                        }
                    }
                }
            } else {
                match sum_value_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string())) {
                    Some(_sum_value_diff) => {
                        panic!("Error: (sum_value_diff) found second entry for hostname: {}, type: {}, id: {}, name: {}", &hostname_port.clone(), &metric_type.clone(), String::from("-"), &metric_name.clone());
                    },
                    None => {
                        sum_value_diff.insert((
                                                  hostname_port.to_string(),
                                                  metric_type.to_string(),
                                                  String::from("-"),
                                                  metric_name.to_string()
                                              ), SnapshotDiffValues {
                            table_name: String::from("-"),
                            namespace: String::from("-"),
                            first_snapshot_time: value_diff_row.first_snapshot_time,
                            second_snapshot_time: value_diff_row.second_snapshot_time,
                            first_snapshot_value: value_diff_row.first_snapshot_value,
                            second_snapshot_value: value_diff_row.second_snapshot_value,
                        });
                    }
                }
            }
        }
        for ((hostname, metric_type, metric_id, metric_name), value_diff_row) in sum_value_diff {
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
                        println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:6} {:>15.3} /s",
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
                        println!("{:20} {:8} {:70} {:15} {:6} {:>15.3} /s",
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
        let mut sum_countsum_diff: BTreeMap<(String, String, String, String), SnapshotDiffCountSum> = BTreeMap::new();
        for ((hostname_port, metric_type, _metric_id, metric_name), countsum_diff_row) in countsum_diff {
            if metric_type == "table" || metric_type == "tablet" {
                if countsum_diff_row.second_snapshot_total_count > 0 {
                    match sum_countsum_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string())) {
                        Some(sum_countsum_diff_row) => {
                            *sum_countsum_diff_row = SnapshotDiffCountSum {
                                table_name: sum_countsum_diff_row.table_name.to_string(),
                                namespace: sum_countsum_diff_row.namespace.to_string(),
                                first_snapshot_time: sum_countsum_diff_row.first_snapshot_time,
                                second_snapshot_time: sum_countsum_diff_row.second_snapshot_time,
                                second_snapshot_total_count: sum_countsum_diff_row.second_snapshot_total_count + countsum_diff_row.second_snapshot_total_count,
                                second_snapshot_min: 0,
                                second_snapshot_mean: 0.0,
                                second_snapshot_percentile_75: 0,
                                second_snapshot_percentile_95: 0,
                                second_snapshot_percentile_99: 0,
                                second_snapshot_percentile_99_9: 0,
                                second_snapshot_percentile_99_99: 0,
                                second_snapshot_max: 0,
                                second_snapshot_total_sum: sum_countsum_diff_row.second_snapshot_total_sum + countsum_diff_row.second_snapshot_total_sum,
                                first_snapshot_total_count: sum_countsum_diff_row.first_snapshot_total_count + countsum_diff_row.first_snapshot_total_count,
                                first_snapshot_total_sum: sum_countsum_diff_row.first_snapshot_total_sum + countsum_diff_row.first_snapshot_total_sum
                            }
                        },
                        None => {
                            sum_countsum_diff.insert((
                                                         hostname_port.to_string(),
                                                         metric_type.to_string(),
                                                         String::from("-"),
                                                         metric_name.to_string()
                                                     ), SnapshotDiffCountSum {
                                table_name: String::from("-"),
                                namespace: String::from("-"),
                                first_snapshot_time: countsum_diff_row.first_snapshot_time,
                                second_snapshot_time: countsum_diff_row.second_snapshot_time,
                                second_snapshot_total_count: countsum_diff_row.second_snapshot_total_count,
                                second_snapshot_min: 0,
                                second_snapshot_mean: 0.0,
                                second_snapshot_percentile_75: 0,
                                second_snapshot_percentile_95: 0,
                                second_snapshot_percentile_99: 0,
                                second_snapshot_percentile_99_9: 0,
                                second_snapshot_percentile_99_99: 0,
                                second_snapshot_max: 0,
                                second_snapshot_total_sum: countsum_diff_row.second_snapshot_total_sum,
                                first_snapshot_total_count: countsum_diff_row.first_snapshot_total_count,
                                first_snapshot_total_sum: countsum_diff_row.first_snapshot_total_sum
                            });
                        }
                    }
                }
            } else {
                match sum_countsum_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string())) {
                    Some(_sum_countsum_diff_row) => {
                        panic!("Error: (sum_countsum_diff) found second entry for hostname: {}, type: {}, id: {}, name: {}", &hostname_port.clone(), &metric_type.clone(), String::from("-"), &metric_name.clone());
                    },
                    None => {
                        sum_countsum_diff.insert((
                                                     hostname_port.to_string(),
                                                     metric_type.to_string(),
                                                     String::from("-"),
                                                     metric_name.to_string()
                                                 ), SnapshotDiffCountSum {
                            table_name: String::from("-"),
                            namespace: String::from("-"),
                            first_snapshot_time: countsum_diff_row.first_snapshot_time,
                            second_snapshot_time: countsum_diff_row.second_snapshot_time,
                            second_snapshot_total_count: countsum_diff_row.second_snapshot_total_count,
                            second_snapshot_min: 0,
                            second_snapshot_mean: 0.0,
                            second_snapshot_percentile_75: 0,
                            second_snapshot_percentile_95: 0,
                            second_snapshot_percentile_99: 0,
                            second_snapshot_percentile_99_9: 0,
                            second_snapshot_percentile_99_99: 0,
                            second_snapshot_max: 0,
                            second_snapshot_total_sum: countsum_diff_row.second_snapshot_total_sum,
                            first_snapshot_total_count: countsum_diff_row.first_snapshot_total_count,
                            first_snapshot_total_sum: countsum_diff_row.first_snapshot_total_sum
                        });
                    }
                }
            }
        }
        for ((hostname, metric_type, metric_id, metric_name), countsum_diff_row) in sum_countsum_diff {
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
                        println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15}        {:>15.3} /s avg: {:9.0} tot: {:>15.3} {:10}",
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
                        println!("{:20} {:8} {:70} {:15}        {:>15.3} /s avg: {:9.0} tot: {:>15.3} {:10}",
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
    }
    // countsumrows_diff
    for ((hostname, _metric_type, _metric_id, metric_name), countsumrows_diff_row) in countsumrows_diff {
        if hostname_filter.is_match( &hostname)
            && stat_name_filter.is_match(&metric_name) {
            if countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count != 0 {
                println!("{:20} {:70} {:>15} avg: {:>15.3} tot: {:>15.3} ms, avg: {:>15} tot: {:>15} rows",
                         hostname,
                         metric_name,
                         countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count,
                         ((countsumrows_diff_row.second_snapshot_sum as f64 - countsumrows_diff_row.first_snapshot_sum as f64)/1000.0) / (countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count) as f64,
                         (countsumrows_diff_row.second_snapshot_sum as f64 - countsumrows_diff_row.first_snapshot_sum as f64)/1000.0,
                         (countsumrows_diff_row.second_snapshot_rows - countsumrows_diff_row.first_snapshot_rows) / (countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count),
                         countsumrows_diff_row.second_snapshot_rows - countsumrows_diff_row.first_snapshot_rows
                );
            }
        }
    }
}

pub fn get_metrics_into_diff_first_snapshot(
    hostname_port_vec: &Vec<&str>
) -> (
    BTreeMap<(String, String, String, String), SnapshotDiffValues>,
    BTreeMap<(String, String, String, String), SnapshotDiffCountSum>,
    BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows>
) {
    let (stored_values, stored_countsum, stored_countsumrows) = read_metrics_into_vectors(&hostname_port_vec);
    let (values_map, countsum_map, countsumrows_map) = build_metrics_btreemaps( stored_values, stored_countsum, stored_countsumrows);
    let (values_diff, countsum_diff, countsumrows_diff) = insert_first_snapshot_metrics(values_map, countsum_map, countsumrows_map);
    (values_diff, countsum_diff, countsumrows_diff)
}

pub fn get_metrics_into_diff_second_snapshot(
    hostname_port_vec: &Vec<&str>,
    values_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffValues>,
    countsum_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffCountSum>,
    countsumrows_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows>,
    first_snapshot_time: &DateTime<Local>
) {
    let (stored_values, stored_countsum, stored_countsumrows) = read_metrics_into_vectors(&hostname_port_vec);
    let (values_map, countsum_map, countsumrows_map) = build_metrics_btreemaps( stored_values, stored_countsum, stored_countsumrows);
    insert_second_snapshot_metrics(values_map, values_diff, countsum_map, countsum_diff, countsumrows_map, countsumrows_diff, &first_snapshot_time);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tablet_metrics_value() {
        let json = r#"[
    {
        "type": "tablet",
        "id": "16add7b1248a45d2880e5527b2059b54",
        "attributes": {
            "namespace_name": "yugabyte",
            "table_name": "config",
            "table_id": "000033e10000300080000000000042d9"
        },
        "metrics": [
            {
                "name": "rocksdb_sequence_number",
                "value": 1125899906842624
            }
        ]
    }
    ]"#.to_string();
        let result = parse_metrics(json.clone());
        assert_eq!(result[0].metrics_type,"tablet");
        let statistic_value = match &result[0].metrics[0] {
            NamedMetrics::MetricValue { name, value} => format!("{}, {}",name, value),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "rocksdb_sequence_number, 1125899906842624");
    }

    #[test]
    fn parse_tablet_metrics_countsum() {
        let json = r#"[
    {
        "type": "table",
        "id": "000033e10000300080000000000042ac",
        "attributes": {
            "namespace_name": "yugabyte",
            "table_name": "benchmark_table",
            "table_id": "000033e10000300080000000000042ac"
        },
        "metrics": [
            {
                "name": "log_sync_latency",
                "total_count": 21,
                "min": 0,
                "mean": 0.0,
                "percentile_75": 0,
                "percentile_95": 0,
                "percentile_99": 0,
                "percentile_99_9": 0,
                "percentile_99_99": 0,
                "max": 0,
                "total_sum": 22349
            }
        ]
    }
    ]"#.to_string();
        let result = parse_metrics(json.clone());
        assert_eq!(result[0].metrics_type,"table");
        let statistic_value = match &result[0].metrics[0] {
            NamedMetrics::MetricCountSum { name, total_count, min: _, mean: _, percentile_75: _, percentile_95: _, percentile_99: _, percentile_99_9: _, percentile_99_99: _, max: _, total_sum} => format!("{}, {}, {}",name, total_count, total_sum),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "log_sync_latency, 21, 22349");
    }

    #[test]
    fn parse_tablet_metrics_countsumrows() {
        let json = r#"[
    {
        "type": "server",
        "id": "yb.ysqlserver",
        "metrics": [
            {
                "name": "handler_latency_yb_ysqlserver_SQLProcessor_CatalogCacheMisses",
                "count": 439,
                "sum": 0,
                "rows": 439
            }
        ]
    }
    ]"#.to_string();
        let result = parse_metrics(json.clone());
        assert_eq!(result[0].metrics_type,"server");
        let statistic_value = match &result[0].metrics[0] {
            NamedMetrics::MetricCountSumRows { name, count, sum, rows} => format!("{}, {}, {}, {}",name, count, sum, rows),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "handler_latency_yb_ysqlserver_SQLProcessor_CatalogCacheMisses, 439, 0, 439");
    }

    #[test]
    fn parse_tablet_metrics_rejectedmetricvalue() {
        // Funny, when I checked with version 2.11.2.0-b89 I could not find the value that only fitted in an unsigned 64 bit integer.
        // Still let's check for it.
        // The id is yb.cqlserver, because that is where I found this value.
        // The value 18446744073709551615 is too big for a signed 64 bit integer (limit = 2^63-1), this value is 2^64-1.
        let json = r#"[
    {
        "type": "server",
        "id": "yb.cqlserver",
        "attributes": {},
        "metrics": [
            {
                "name": "madeup_value",
                "value": 18446744073709551615
            }
        ]
    }
    ]"#.to_string();
        let result = parse_metrics(json.clone());
        assert_eq!(result[0].metrics_type,"server");
        let statistic_value = match &result[0].metrics[0] {
            NamedMetrics::RejectedMetricValue { name, value} => format!("{}, {}",name, value),
            _ => String::from("Not RejectedMetricValue")
        };
        assert_eq!(statistic_value, "madeup_value, 18446744073709551615");
    }
}
