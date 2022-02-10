extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate csv;

//mod parse_json;

//use std::time::SystemTime;
use std::collections::BTreeMap;
use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use std::process;
use std::fs;
use std::path::Path;

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

#[derive(Debug)]
pub struct LatencyStatisticDetails {
    pub unit: String,
    pub unit_suffix: String,
    pub divisor: i64,
    pub stat_type: String,
}

#[derive(Debug)]
pub struct ValueStatisticDetails {
    pub unit: String,
    pub unit_suffix: String,
    pub stat_type: String,
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

    fs::create_dir_all("yb_stats.snapshots")
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error creating directory yb_stats.snapshots: {}", e);
            process::exit(1);
        });
    if Path::new("yb_stats.snapshots/snapshot.index").exists() {
        let file = fs::File::open("yb_stats.snapshots/snapshot.index").unwrap();
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
        .open("yb_stats.snapshots/snapshot.index")
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing yb_stats.snapshots/snapshot.index file: {}", e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in snapshots {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();
    fs::create_dir_all("yb_stats.snapshots/".to_owned()+&snapshot_number.to_string())
        .unwrap_or_else(|e| {
            eprintln!("Fatel: error creating directory yb_stats.snapshots/{}: {}", &snapshot_number.to_string(), e);
            process::exit(1);
        });
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open("yb_stats.snapshots/".to_owned()+&snapshot_number.to_string()+"/values")
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing values statistics in snapshot directory {}: {}", &snapshot_number.to_string(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_values {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open("yb_stats.snapshots/".to_owned()+&snapshot_number.to_string()+"/latencies")
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing latencies statistics in snapshot directory {}: {}", &snapshot_number.to_string(), e);
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
                                   first_snapshot_time: DateTime<Local>
) {
    for ((hostname_port, metric_type, metric_id, metric_name), storedvalues) in values_btree {
        match values_diff.get_mut(&(hostname_port.clone(), metric_type.clone(), metric_id.clone(), metric_name.clone())) {
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
                    first_snapshot_time: first_snapshot_time,
                    second_snapshot_time: storedvalues.timestamp,
                    first_snapshot_value: 0,
                    second_snapshot_value: storedvalues.metric_value
                });
            }
        }
    }
    for ((hostname_port, metric_type, metric_id, metric_name), storedlatencies) in latencies_btree {
        match latencies_diff.get_mut( &(hostname_port.clone(), metric_type.clone(), metric_id.clone(), metric_name.clone())) {
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
                   first_snapshot_total_sum:latencies_diff_row.first_snapshot_total_sum
               }
            },
            None => {
               latencies_diff.insert((hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffLatencies {
                   table_name: storedlatencies.attribute_table_name.to_string(),
                   namespace: storedlatencies.attribute_namespace.to_string(),
                   first_snapshot_time: first_snapshot_time,
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
        /*
        diff_value_statistics.entry( row.hostname_port.to_string().into()).or_insert(BTreeMap::new());
        match diff_value_statistics.get_mut(row.hostname_port.to_string()) {
            None => { panic!("diff_value_statistics level 1: hostname_port not found, should have been inserted") }
            Some(hostname_port) => {
                hostname_port.entry(row.metric_type.to_string().into()).or_insert(BTreeMap::new());
                match hostname_port.get_mut(row.metric_type.to_string()) {
                    None => { panic!("diff_value_statistics level 2: metric_type not found, should have been inserted") }
                    Some(metric_type) => {
                        metric_type.entry(row.metric_id.to_string().into()).or_insert(BtreeMap::new());
                        match metric_type.get_mut(row.metric_id.to_string()) {
                            None => { panic!("diff_value_statistics level 3: metric_id not found, should have been inserted") }
                            Some(metric_id) => {
                                match metric_id.get_mut(row.metric_name) {
                                    None => {
                                        metric_id.insert(row.metric_name, SnapshotDiffValues {
                                            table_name: row.attribute_table_name.to_string(),
                                            namespace: row.attribute_namespace.to_string(),
                                            first_snapshot_time: *row.timestamp,
                                            second_snapshot_time: *row.timestamp,
                                            first_snapshot_value: 0,
                                            second_snapshot_value: *row.metric_value
                                        })
                                    }
                                    Some (metric_name) => {
                                        *metric_name = SnapshotDiffValues {
                                            table_name: row.attribute_table_name.to_string(),
                                            namespace: row.attribute_namespace.to_string(),
                                            first_snapshot_time: metric_name.second_snapshot_time,
                                            second_snapshot_time: *row.timestamp,
                                            first_snapshot_value: metric_name.second_snapshot_value,
                                            second_snapshot_value: *row.metric_value
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

         */
    }
    /*
    summary_value_statistics: &mut BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, StoredValues>>>>
    for row in stored_values.into_iter() {
        if row.metric_type == "table" || row.metric_type == "tablet" {
            summary_value_statistics.entry( row.hostname_port.to_string().into()).or_insert(BTreeMap::new());
            match summary_value_statistics.get_mut(row.hostname_port.to_string()) {
                None => { panic!("summary_value_statistics level 1: hostname_port not found, should have been inserted") }
                Some(hostname_port) => {
                    hostname_port.entry(row.metric_type.to_string().into()).or_insert(BTreeMap::new());
                    match hostname_port.get_mut(row.metric_type.to_string()) {
                        None => { panic!("summary_value_statistics level 2: metric_type not found, should have been inserted") }
                        Some(metric_type) => {
                            metric_type.entry(String::from("-").into()).or_insert(BtreeMap::new());
                            match metric_type.get_mut(row.metric_id.to_string()) {
                                None => { panic!("summary_value_statistics level 3: metric_id not found, should have been inserted") }
                                Some(metric_id) => {
                                    match metric_id.get_mut(row.metric_name) {
                                        None => {
                                            metric_id.insert(row.metric_name, StoredValues {
                                                hostname_port: row.hostname_port.to_string(),
                                                timestamp: *row.timestamp,
                                                metric_type: row.metric_type.to_string(),
                                                metric_id: String::from("-"),
                                                attribute_namespace: String::from("-"),
                                                attribute_table_name: String::from("-"),

                                                table_name: String::from("-"),
                                                namespace: String::from("-")
                                                first_snapshot_time: *row.timestamp,
                                                second_snapshot_time: *row.timestamp,
                                                first_snapshot_value: 0,
                                                second_snapshot_value: *row.metric_value
                                            })
                                        }
                                        Some(metric_name) => {
                                            *metric_name = SnapshotDiffValues {
                                                table_name: String::from("-"),
                                                namespace: String::from("-")
                                                first_snapshot_time: metric_name.second_snapshot_time,
                                                second_snapshot_time: *row.timestamp,
                                                first_snapshot_value: metric_name.second_snapshot_value,
                                                second_snapshot_value: *row.metric_value
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

     */
}
/*
pub fn build_detail_value_metric( name: &String,
                                  value: &i64,
                                  hostname: &&str,
                                  metrics_type: &String,
                                  metrics_id: &String,
                                  metrics_attribute_table_name: &String,
                                  metrics_attribute_namespace_name: &String,
                                  fetch_time: &DateTime<Local>,
                                  previous_fetch_time: &DateTime<Local>,
                                  value_statistics: &mut BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, Values>>>>,
                                  previous_value_to_return: &mut i64
                                 ) {
    if *value > 0 {
        value_statistics.entry(hostname.to_string().into()).or_insert(BTreeMap::new());
        match value_statistics.get_mut(&hostname.to_string()) {
            None => { panic!("value_statistics 1. hostname not found, should have been inserted") }
            Some(vs_hostname) => {
                vs_hostname.entry(metrics_type.to_string().into()).or_insert(BTreeMap::new());
                match vs_hostname.get_mut(&metrics_type.to_string()) {
                    None => { panic!("value_statistics 2. type not found, should have been inserted")}
                    Some (vs_type) => {
                        vs_type.entry( metrics_id.to_string().into()).or_insert(BTreeMap::new());
                        match vs_type.get_mut( &metrics_id.to_string()) {
                            None => { panic!("value_statistics 3. id not found, should have been inserted")}
                            Some (vs_id) => {
                                match vs_id.get_mut(&name.to_string()) {
                                    None => {
                                        vs_id.insert(name.to_string(), Values { table_name: metrics_attribute_table_name.to_string(), namespace: metrics_attribute_namespace_name.to_string(), current_time: *fetch_time, previous_time: *previous_fetch_time, current_value: *value, previous_value: 0 });
                                        *previous_value_to_return = 0;
                                    }
                                    Some(vs_name) => {
                                        *vs_name = Values { table_name: metrics_attribute_table_name.to_string(), namespace: metrics_attribute_namespace_name.to_string(), current_time: *fetch_time, previous_time: vs_name.current_time, current_value: *value, previous_value: vs_name.current_value };
                                        *previous_value_to_return = vs_name.previous_value;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    };
}

pub fn build_summary_value_metric( name: &String,
                                   value: &i64,
                                   hostname: &&str,
                                   metrics_type: &String,
                                   fetch_time: &DateTime<Local>,
                                   previous_fetch_time: &DateTime<Local>,
                                   summary_value_statistics: &mut BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, Values>>>>,
                                   previous_value_to_return: &i64
                                 ) {
    if metrics_type == "table" || metrics_type == "tablet" {
        if *value > 0 {
            summary_value_statistics.entry(hostname.to_string().into()).or_insert(BTreeMap::new());
            match summary_value_statistics.get_mut(&hostname.to_string()) {
                None => { panic!("summary_value_statistics 1. hostname not found, should have been inserted") }
                Some(vs_hostname) => {
                    vs_hostname.entry(metrics_type.to_string().into()).or_insert(BTreeMap::new());
                    match vs_hostname.get_mut(&metrics_type.to_string()) {
                        None => { panic!("summary_value_statistics 2. type not found, should have been inserted") }
                        Some(vs_type) => {
                            vs_type.entry(String::from("-").into()).or_insert(BTreeMap::new());
                            match vs_type.get_mut(&String::from("-")) {
                                None => { panic!("summary_value_statistics 3. id not found, should have been inserted") }
                                Some(vs_id) => {
                                    match vs_id.get_mut(&name.to_string()) {
                                        None => {
                                            vs_id.insert(name.to_string(), Values { table_name: String::from("-"), namespace: String::from("-"), current_time: *fetch_time, previous_time: *previous_fetch_time, current_value: *value, previous_value: *previous_value_to_return });
                                        }
                                        Some(vs_name) => {
                                            *vs_name = Values { table_name: String::from("-"), namespace: String::from("-"), current_time: *fetch_time, previous_time: *previous_fetch_time, current_value: vs_name.current_value + *value, previous_value: vs_name.previous_value + *previous_value_to_return };
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

pub fn build_detail_latency_metric( name: &String,
                                    total_count: &u64,
                                    min: &u64,
                                    mean: &f64,
                                    percentile_75: &u64,
                                    percentile_95: &u64,
                                    percentile_99: &u64,
                                    percentile_99_9: &u64,
                                    percentile_99_99: &u64,
                                    max: &u64,
                                    total_sum: &u64,
                                    hostname: &&str,
                                    metrics_type: &String,
                                    metrics_id: &String,
                                    metrics_attribute_table_name: &String,
                                    metrics_attribute_namespace_name: &String,
                                    fetch_time: &DateTime<Local>,
                                    previous_fetch_time: &DateTime<Local>,
                                    latency_statistics: &mut BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, Latencies>>>>,
                                    previous_total_count_to_return: &mut u64,
                                    previous_total_sum_to_return: &mut u64
                                  ) {
    if *total_count > 0 {
        latency_statistics.entry(hostname.to_string().into()).or_insert(BTreeMap::new());
        match latency_statistics.get_mut(&hostname.to_string()) {
            None => { panic!("latency_statistics - hostname not found, should have been inserted") }
            Some(ls_hostname) => {
                ls_hostname.entry(metrics_type.to_string().into()).or_insert(BTreeMap::new());
                match ls_hostname.get_mut(&metrics_type.to_string()) {
                    None => { panic!("latency_statistics - hostname.type not found, should have been inserted")}
                    Some (ls_type) => {
                        ls_type.entry( metrics_id.to_string().into()).or_insert(BTreeMap::new());
                        match ls_type.get_mut( &metrics_id.to_string()) {
                            None => { panic!("latency_statistics - hostname.type.id not found, should have been inserted")}
                            Some (ls_id) => {
                                match ls_id.get_mut(&name.to_string()) {
                                    None => {
                                        ls_id.insert(name.to_string(), Latencies { table_name: metrics_attribute_table_name.to_string(), namespace: metrics_attribute_namespace_name.to_string(), current_time: *fetch_time, previous_time: *previous_fetch_time, current_total_count: *total_count, previous_total_count: 0, current_min: *min, current_mean: *mean, current_percentile_75: *percentile_75, current_percentile_95: *percentile_95, current_percentile_99: *percentile_99, current_percentile_99_9: *percentile_99_9, current_percentile_99_99: *percentile_99_99, current_max: *max, current_total_sum: *total_sum, previous_total_sum: 0 });
                                        *previous_total_count_to_return = 0;
                                        *previous_total_sum_to_return = 0;
                                    }
                                    Some(ls_name) => {
                                        *ls_name = Latencies { table_name: metrics_attribute_table_name.to_string(), namespace: metrics_attribute_namespace_name.to_string(), current_time: *fetch_time, previous_time: ls_name.current_time, current_total_count: *total_count, previous_total_count: ls_name.current_total_count, current_min: *min, current_mean: *mean, current_percentile_75: *percentile_75, current_percentile_95: *percentile_95, current_percentile_99: *percentile_99, current_percentile_99_9: *percentile_99_9, current_percentile_99_99: *percentile_99_99, current_max: *max, current_total_sum: *total_sum, previous_total_sum: ls_name.current_total_sum };
                                        *previous_total_count_to_return = ls_name.previous_total_count;
                                        *previous_total_sum_to_return = ls_name.previous_total_sum;
                                    }
                                };
                            }
                        };
                    }
                };
            }
        };
    };
}

pub fn build_summary_latency_metric( name: &String,
                                     total_count: &u64,
                                     total_sum: &u64,
                                     hostname: &&str,
                                     metrics_type: &String,
                                     fetch_time: &DateTime<Local>,
                                     previous_fetch_time: &DateTime<Local>,
                                     summary_latency_statistics: &mut BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, Latencies>>>>,
                                     previous_total_count_to_return: &u64,
                                     previous_total_sum_to_return: &u64
                                   ) {
    if metrics_type == "table" || metrics_type == "tablet" {
        if *total_count > 0 {
            summary_latency_statistics.entry(hostname.to_string().into()).or_insert(BTreeMap::new());
            match summary_latency_statistics.get_mut(&hostname.to_string()) {
                None => { panic!("summary_latency_statistics - hostname not found, should have been inserted") }
                Some(ls_hostname) => {
                    ls_hostname.entry(metrics_type.to_string().into()).or_insert(BTreeMap::new());
                    match ls_hostname.get_mut(&metrics_type.to_string()) {
                        None => { panic!("summary_latency_statistics - hostname.type not found, should have been inserted") }
                        Some(ls_type) => {
                            ls_type.entry(String::from("-").into()).or_insert(BTreeMap::new());
                            match ls_type.get_mut(&String::from("-")) {
                                None => { panic!("summary_latency_statistics - hostname.type.id not found, should have been inserted") }
                                Some(ls_id) => {
                                    match ls_id.get_mut(&name.to_string()) {
                                        None => {
                                            ls_id.insert(name.to_string(), Latencies { table_name: String::from("-"), namespace: String::from("-"), current_time: *fetch_time, previous_time: *previous_fetch_time, current_total_count: *total_count, previous_total_count: *previous_total_count_to_return, current_min: 0, current_mean: 0 as f64, current_percentile_75: 0, current_percentile_95: 0, current_percentile_99: 0, current_percentile_99_9: 0, current_percentile_99_99: 0, current_max: 0, current_total_sum: *total_sum, previous_total_sum: *previous_total_sum_to_return });
                                        }
                                        Some(ls_name) => {
                                            *ls_name = Latencies { table_name: String::from("-"), namespace: String::from("-"), current_time: *fetch_time, previous_time: *previous_fetch_time, current_total_count: ls_name.current_total_count + *total_count, previous_total_count: ls_name.previous_total_count + *previous_total_count_to_return, current_min: 0, current_mean: 0 as f64, current_percentile_75: 0, current_percentile_95: 0, current_percentile_99: 0, current_percentile_99_9: 0, current_percentile_99_99: 0, current_max: 0, current_total_sum: ls_name.current_total_sum + *total_sum, previous_total_sum: ls_name.previous_total_sum + *previous_total_sum_to_return };
                                        }
                                    };
                                }
                            };
                        }
                    };
                }
            };
        };
    }
}


 */