extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate csv;

use std::time::SystemTime;
use std::collections::BTreeMap;

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

#[derive(Debug)]
pub struct Values {
    pub table_name: String,
    pub namespace: String,
    pub current_time: SystemTime,
    pub previous_time: SystemTime,
    pub current_value: i64,
    pub previous_value: i64,
}
#[derive(Debug)]
pub struct Latencies {
    pub table_name: String,
    pub namespace: String,
    pub current_time: SystemTime,
    pub previous_time: SystemTime,
    pub current_total_count: u64,
    pub current_min: u64,
    pub current_mean: f64,
    pub current_percentile_75: u64,
    pub current_percentile_95: u64,
    pub current_percentile_99: u64,
    pub current_percentile_99_9: u64,
    pub current_percentile_99_99: u64,
    pub current_max: u64,
    pub current_total_sum: u64,
    pub previous_total_count: u64,
    pub previous_total_sum: u64,
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

#[derive(Debug, Deserialize)]
pub struct Snapshots {
    pub number: i32,
    pub timestamp: SystemTime,
    pub remark: String,
}

pub fn build_detail_value_metric( name: &String,
                                  value: &i64,
                                  hostname: &&str,
                                  metrics_type: &String,
                                  metrics_id: &String,
                                  metrics_attribute_table_name: &String,
                                  metrics_attribute_namespace_name: &String,
                                  fetch_time: &SystemTime,
                                  previous_fetch_time: &SystemTime,
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
                                   fetch_time: &SystemTime,
                                   previous_fetch_time: &SystemTime,
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
                                    fetch_time: &SystemTime,
                                    previous_fetch_time: &SystemTime,
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
                                     fetch_time: &SystemTime,
                                     previous_fetch_time: &SystemTime,
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
