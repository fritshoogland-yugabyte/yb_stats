extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate csv;

mod value_statistic_details;
use value_statistic_details::{ValueStatisticDetails, value_create_hashmap};
mod latency_statistic_details;
use latency_statistic_details::{LatencyStatisticDetails, latency_create_hashmap};

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
    pub attributes: Attributes,
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
    MetricLatency {
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
    }
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
pub struct StoredLatencies {
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
pub struct SnapshotDiffLatencies {
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub number: i32,
    pub timestamp: DateTime<Local>,
    pub comment: String,
}

pub fn read_metrics( hostname: &str) -> Vec<Metrics> {
        if ! scan_port_addr(hostname) {
            println!("Warning! hostname:port {} cannot be reached, skipping", hostname.to_string());
            let data_from_http = String::from("") ;
            return parse_metrics(data_from_http)
        };
        let data_from_http = reqwest::blocking::get(format!("http://{}/metrics", hostname.to_string()))
            .unwrap_or_else(|e| {
                eprintln!("Fatal: error reading from URL: {}", e);
                process::exit(1);
            })
            .text().unwrap();
        parse_metrics(data_from_http)
}

pub fn add_to_metric_vectors(data_parsed_from_json: Vec<Metrics>,
                             hostname: &str,
                             detail_snapshot_time: DateTime<Local>,
                             stored_values: &mut Vec<StoredValues>,
                             stored_latencies: &mut Vec<StoredLatencies>
) {
    for metric in data_parsed_from_json {
        let metric_type = &metric.metrics_type;
        let metric_id = &metric.id;
        let metric_attribute_namespace_name = match &metric.attributes.namespace_name {
            Some(namespace_name) => namespace_name.to_string(),
            None => "-".to_string(),
        };
        let metric_attribute_table_name = match &metric.attributes.table_name {
            Some(table_name) => table_name.to_string(),
            None => "-".to_string(),
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
                NamedMetrics::MetricLatency { name, total_count, min, mean, percentile_75, percentile_95, percentile_99, percentile_99_9, percentile_99_99, max, total_sum } => {
                    if *total_count > 0 {
                        stored_latencies.push(StoredLatencies {
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
    let mut stored_latencies: Vec<StoredLatencies> = Vec::new();
    for hostname in &hostname_port_vec {
        let detail_snapshot_time = Local::now();
        let data_parsed_from_json = read_metrics(&hostname);
        add_to_metric_vectors(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_values, &mut stored_latencies);
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

    let latencies_file = &current_snapshot_directory.join("latencies");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&latencies_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing latencies statistics in snapshot directory {}: {}", &latencies_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_latencies {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    snapshot_number
}

pub fn parse_metrics( metrics_data: String ) -> Vec<Metrics> {
    serde_json::from_str(&metrics_data)
        .unwrap_or_else(|e| {
            println!("Warning: issue parsing response: {}", e);
            return Vec::<Metrics>::new();
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
                     latencies_summary: &mut BTreeMap<(String, String, String, String), StoredLatencies>,
                     stored_latencies: &Vec<StoredLatencies>
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
                Some(values_summary_row) => {
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
    for row in stored_latencies {
        if row.metric_type == "table" || row.metric_type == "tablet" {
            match latencies_summary.get_mut(&(row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone())) {
                Some(latencies_summary_row) => {
                    *latencies_summary_row = StoredLatencies {
                        hostname_port: latencies_summary_row.hostname_port.to_string(),
                        timestamp: latencies_summary_row.timestamp,
                        metric_type: latencies_summary_row.metric_type.to_string(),
                        metric_id: latencies_summary_row.metric_id.to_string(),
                        attribute_namespace: latencies_summary_row.attribute_namespace.to_string(),
                        attribute_table_name: latencies_summary_row.attribute_table_name.to_string(),
                        metric_name: latencies_summary_row.metric_name.to_string(),
                        metric_total_count: latencies_summary_row.metric_total_count + row.metric_total_count,
                        metric_min: 0,
                        metric_mean: 0.0,
                        metric_percentile_75: 0,
                        metric_percentile_95: 0,
                        metric_percentile_99: 0,
                        metric_percentile_99_9: 0,
                        metric_percentile_99_99: 0,
                        metric_max: 0,
                        metric_total_sum: latencies_summary_row.metric_total_sum + row.metric_total_sum
                    }
                },
                None => {
                    latencies_summary.insert((row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone()), StoredLatencies {
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
            match latencies_summary.get_mut(&(row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone())) {
                Some(latencies_summary_row) => {
                    *latencies_summary_row = StoredLatencies {
                        hostname_port: latencies_summary_row.hostname_port.to_string(),
                        timestamp: latencies_summary_row.timestamp,
                        metric_type: latencies_summary_row.metric_type.to_string(),
                        metric_id: latencies_summary_row.metric_id.to_string(),
                        attribute_namespace: latencies_summary_row.attribute_namespace.to_string(),
                        attribute_table_name: latencies_summary_row.attribute_table_name.to_string(),
                        metric_name: latencies_summary_row.metric_name.to_string(),
                        metric_total_count: latencies_summary_row.metric_total_count + row.metric_total_count,
                        metric_min: 0,
                        metric_mean: 0.0,
                        metric_percentile_75: 0,
                        metric_percentile_95: 0,
                        metric_percentile_99: 0,
                        metric_percentile_99_9: 0,
                        metric_percentile_99_99: 0,
                        metric_max: 0,
                        metric_total_sum: latencies_summary_row.metric_total_sum
                    }
                },
                None => {
                    latencies_summary.insert((row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone()), StoredLatencies {
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
}
pub fn build_detail(values_detail: &mut BTreeMap<(String, String, String, String), StoredValues>,
                    stored_values: &Vec<StoredValues>,
                    latencies_detail: &mut BTreeMap<(String, String, String, String), StoredLatencies>,
                    stored_latencies: &Vec<StoredLatencies>
) {
    for row in stored_values {
            match values_detail.get_mut(&(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
                Some(values_detail_row) => {
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
    for row in stored_latencies {
        match latencies_detail.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
            Some(latencies_detail_row) => {
                *latencies_detail_row = StoredLatencies {
                    hostname_port: latencies_detail_row.hostname_port.to_string(),
                    timestamp: latencies_detail_row.timestamp,
                    metric_type: latencies_detail_row.metric_type.to_string(),
                    metric_id: latencies_detail_row.metric_id.to_string(),
                    attribute_namespace: latencies_detail_row.attribute_namespace.to_string(),
                    attribute_table_name: latencies_detail_row.attribute_table_name.to_string(),
                    metric_name: latencies_detail_row.metric_name.to_string(),
                    metric_total_count: latencies_detail_row.metric_total_count,
                    metric_min: latencies_detail_row.metric_min,
                    metric_mean: latencies_detail_row.metric_mean,
                    metric_percentile_75: latencies_detail_row.metric_percentile_75,
                    metric_percentile_95: latencies_detail_row.metric_percentile_95,
                    metric_percentile_99: latencies_detail_row.metric_percentile_99,
                    metric_percentile_99_9: latencies_detail_row.metric_percentile_99_9,
                    metric_percentile_99_99: latencies_detail_row.metric_percentile_99_99,
                    metric_max: latencies_detail_row.metric_max,
                    metric_total_sum: latencies_detail_row.metric_total_sum
                }
            },
            None => {
                latencies_detail.insert( (row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone()), StoredLatencies {
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
}

pub fn insert_first_snap_into_diff(values_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffValues>,
                                   values_btree: &BTreeMap<(String, String, String, String), StoredValues>,
                                   latencies_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffLatencies>,
                                   latencies_btree: &BTreeMap<(String, String, String, String), StoredLatencies>
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
    for ((hostname_port, metric_type, metric_id, metric_name), storedlatendies) in latencies_btree {
        latencies_diff.insert( (hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffLatencies{
            table_name: storedlatendies.attribute_table_name.to_string(),
            namespace: storedlatendies.attribute_namespace.to_string(),
            first_snapshot_time: storedlatendies.timestamp,
            second_snapshot_time: storedlatendies.timestamp,
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
            first_snapshot_total_count: storedlatendies.metric_total_count,
            first_snapshot_total_sum: storedlatendies.metric_total_sum
        });
    }
}
pub fn insert_second_snap_into_diff(values_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffValues>,
                                   values_btree: &BTreeMap<(String, String, String, String), StoredValues>,
                                   latencies_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffLatencies>,
                                   latencies_btree: &BTreeMap<(String, String, String, String), StoredLatencies>,
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
    for ((hostname_port, metric_type, metric_id, metric_name), storedlatencies) in latencies_btree {
        match latencies_diff.get_mut( &(hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string())) {
            Some(latencies_diff_row) => {
               *latencies_diff_row = SnapshotDiffLatencies {
                   table_name: latencies_diff_row.table_name.to_string(),
                   namespace: latencies_diff_row.namespace.to_string(),
                   first_snapshot_time: latencies_diff_row.first_snapshot_time,
                   second_snapshot_time: storedlatencies.timestamp,
                   second_snapshot_total_count: storedlatencies.metric_total_count,
                   second_snapshot_min: storedlatencies.metric_min,
                   second_snapshot_mean: storedlatencies.metric_mean,
                   second_snapshot_percentile_75: storedlatencies.metric_percentile_75,
                   second_snapshot_percentile_95: storedlatencies.metric_percentile_95,
                   second_snapshot_percentile_99: storedlatencies.metric_percentile_99,
                   second_snapshot_percentile_99_9: storedlatencies.metric_percentile_99_9,
                   second_snapshot_percentile_99_99: storedlatencies.metric_percentile_99_99,
                   second_snapshot_max: storedlatencies.metric_max,
                   second_snapshot_total_sum: storedlatencies.metric_total_sum,
                   first_snapshot_total_count: latencies_diff_row.first_snapshot_total_count,
                   first_snapshot_total_sum: latencies_diff_row.first_snapshot_total_sum
               }
            },
            None => {
               latencies_diff.insert((hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffLatencies {
                   table_name: storedlatencies.attribute_table_name.to_string(),
                   namespace: storedlatencies.attribute_namespace.to_string(),
                   first_snapshot_time: *first_snapshot_time,
                   second_snapshot_time: storedlatencies.timestamp,
                   second_snapshot_total_count: storedlatencies.metric_total_count,
                   second_snapshot_min: storedlatencies.metric_min,
                   second_snapshot_mean: storedlatencies.metric_mean,
                   second_snapshot_percentile_75: storedlatencies.metric_percentile_75,
                   second_snapshot_percentile_95: storedlatencies.metric_percentile_95,
                   second_snapshot_percentile_99: storedlatencies.metric_percentile_99,
                   second_snapshot_percentile_99_9: storedlatencies.metric_percentile_99_9,
                   second_snapshot_percentile_99_99: storedlatencies.metric_percentile_99_99,
                   second_snapshot_max: storedlatencies.metric_max,
                   second_snapshot_total_sum: storedlatencies.metric_total_sum,
                   first_snapshot_total_count: 0,
                   first_snapshot_total_sum: 0
               });
            }
        }
    }
}

pub fn print_diff(value_diff: &BTreeMap<(String, String, String, String), SnapshotDiffValues>,
                  latency_diff: &BTreeMap<(String, String, String, String), SnapshotDiffLatencies>,
                  hostname_filter: &Regex,
                  stat_name_filter: &Regex,
                  table_name_filter: &Regex,
                  details_enable: &bool,
                  gauges_enable: &bool
) {
    // value_diff
    for ((hostname, metric_type, metric_id, metric_name), value_diff_row) in value_diff {
        let value_statistic_details_lookup = value_create_hashmap();
        if hostname_filter.is_match(&hostname) {
            if stat_name_filter.is_match(&metric_name) {
                if table_name_filter.is_match(&value_diff_row.table_name) {
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
        }
    }
    // latency_diff
    for ((hostname, metric_type, metric_id, metric_name), latency_diff_row) in latency_diff {
        let latency_statistic_details_lookup = latency_create_hashmap();
        if hostname_filter.is_match(&hostname) {
            if stat_name_filter.is_match(&metric_name) {
                if table_name_filter.is_match(&latency_diff_row.table_name) {
                    let details = match latency_statistic_details_lookup.get(&metric_name.to_string()) {
                        Some(x) => { LatencyStatisticDetails { unit: x.unit.to_string(), unit_suffix: x.unit_suffix.to_string(), divisor: x.divisor, stat_type: x.stat_type.to_string() } },
                        None => { LatencyStatisticDetails { unit: String::from("?"), unit_suffix: String::from("?"), divisor: 0, stat_type: String::from("?") } }
                    };
                    let adaptive_length = if metric_id.len() < 15 { 0 } else { metric_id.len() - 15 };
                    if latency_diff_row.second_snapshot_total_count - latency_diff_row.first_snapshot_total_count != 0 {
                        if *details_enable {
                            println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:>15.3}/s avg.time: {:<9.0} tot: {:>15.3} {:10}",
                                     hostname,
                                     metric_type,
                                     metric_id.substring(adaptive_length, metric_id.len()),
                                     latency_diff_row.namespace,
                                     latency_diff_row.table_name,
                                     metric_name,
                                     latency_diff_row.second_snapshot_total_count - latency_diff_row.first_snapshot_total_count,
                                     (latency_diff_row.second_snapshot_total_count - latency_diff_row.first_snapshot_total_count) as f64 / (latency_diff_row.second_snapshot_time - latency_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000 as f64,
                                     ((latency_diff_row.second_snapshot_total_sum - latency_diff_row.first_snapshot_total_sum) / (latency_diff_row.second_snapshot_total_count - latency_diff_row.first_snapshot_total_count)) as f64,
                                     latency_diff_row.second_snapshot_total_sum - latency_diff_row.first_snapshot_total_sum,
                                     details.unit_suffix
                            );
                        } else {
                            println!("{:20} {:8} {:70} {:15} {:>15.3}/s avg.time: {:<9.0} tot: {:>15.3} {:10}",
                                     hostname,
                                     metric_type,
                                     metric_name,
                                     latency_diff_row.second_snapshot_total_count - latency_diff_row.first_snapshot_total_count,
                                     (latency_diff_row.second_snapshot_total_count - latency_diff_row.first_snapshot_total_count) as f64 / (latency_diff_row.second_snapshot_time - latency_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000 as f64,
                                     ((latency_diff_row.second_snapshot_total_sum - latency_diff_row.first_snapshot_total_sum) / (latency_diff_row.second_snapshot_total_count - latency_diff_row.first_snapshot_total_count)) as f64,
                                     latency_diff_row.second_snapshot_total_sum - latency_diff_row.first_snapshot_total_sum,
                                     details.unit_suffix
                            );
                        }
                    }
                }
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