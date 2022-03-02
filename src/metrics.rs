use std::process;
use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use std::collections::BTreeMap;
use serde_derive::{Serialize,Deserialize};

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
        count: i64,
        sum: i64,
        rows: i64,
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
    pub metric_count: i64,
    pub metric_sum: i64,
    pub metric_rows: i64,
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
