use std::collections::BTreeMap;
use std::path::PathBuf;
use chrono::{DateTime, Local, Utc};
use prometheus_parse::Value;
use serde_derive::{Serialize,Deserialize};
use port_scanner::scan_port_addr;
use std::process;
use rayon;
use std::sync::mpsc::channel;
use std::fs;
use regex::Regex;
use std::env;

#[derive(Debug)]
pub struct NodeExporterValues {
    pub node_exporter_name: String,
    pub node_exporter_type: String,
    pub node_exporter_labels: String,
    pub node_exporter_category: String,
    pub node_exporter_value: f64,
    pub node_exporter_timestamp: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredNodeExporterValues {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub node_exporter_name: String,
    pub node_exporter_type: String,
    pub node_exporter_labels: String,
    pub node_exporter_category: String,
    pub node_exporter_value: f64,
}

#[derive(Debug)]
pub struct SnapshotDiffNodeExporter {
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub node_exporter_type: String,
    pub category: String,
    pub first_value: f64,
    pub second_value: f64,
}

pub fn read_node_exporter(
    host: &str,
    port: &str,
) -> Vec<NodeExporterValues> {
    if ! scan_port_addr(format!("{}:{}", host, port)) {
        println!("Warning! hostname:port {}:{} cannot be reached, skipping (node_exporter)", host, port);
        return parse_node_exporter(String::from(""))
    };
    let data_from_http = reqwest::blocking::get(format!("http://{}:{}/metrics", host, port))
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error reading from URL: {}", e);
            process::exit(1);
        })
        .text().unwrap();
    parse_node_exporter(data_from_http)
}

fn parse_node_exporter( node_exporter_data: String ) -> Vec<NodeExporterValues> {
    let lines: Vec<_> = node_exporter_data.lines().map(|s| Ok(s.to_owned())).collect();
    let node_exporter_rows = prometheus_parse::Scrape::parse(lines.into_iter()).unwrap();
    let mut nodeexportervalues = Vec::new();

    if node_exporter_rows.samples.len() > 0 {
        for sample in node_exporter_rows.samples {
            let mut label_temp = sample.labels.values().cloned().collect::<Vec<String>>();
            label_temp.sort();
            let mut label = label_temp.join("_");
            label = if label.len() > 0 {
                format!("_{}", label)
            } else {
                label
            };

            match sample.value {
                Value::Counter(val) => {
                    nodeexportervalues.push(
                        NodeExporterValues {
                            node_exporter_name: sample.metric.to_string(),
                            node_exporter_type: "counter".to_string(),
                            node_exporter_labels: label,
                            node_exporter_category: "all".to_string(),
                            node_exporter_timestamp: sample.timestamp,
                            node_exporter_value: val,
                        }
                    )
                },
                Value::Gauge(val) => {
                    nodeexportervalues.push(
                        NodeExporterValues {
                            node_exporter_name: sample.metric.to_string(),
                            node_exporter_type: "gauge".to_string(),
                            node_exporter_labels: label,
                            node_exporter_category: "all".to_string(),
                            node_exporter_timestamp: sample.timestamp,
                            node_exporter_value: val,
                        }
                    )
                },
                Value::Untyped(val) => {
                    // it turns out summary type _sum and _count values are untyped values.
                    // so I remove them here.
                    if sample.metric.ends_with("_sum") || sample.metric.ends_with("_count") { continue };
                    // untyped: not sure what it is.
                    // I would say: probably a counter.
                    nodeexportervalues.push(
                        NodeExporterValues {
                            node_exporter_name: sample.metric.to_string(),
                            node_exporter_type: "counter".to_string(),
                            node_exporter_labels: label,
                            node_exporter_category: "all".to_string(),
                            node_exporter_timestamp: sample.timestamp,
                            node_exporter_value: val,

                        }
                    )
                },
                Value::Histogram(_val) => {},
                Value::Summary(_val) => {},
            }
        }
        // post processing.
        // anything that starts with process_ is node_exporter process
        for record in nodeexportervalues.iter_mut().filter(|r| r.node_exporter_name.starts_with("process_")) {
            record.node_exporter_category = "detail".to_string();
        }
        // anything that start with promhttp_ is the node_exporter http server
        for record in nodeexportervalues.iter_mut().filter(|r| r.node_exporter_name.starts_with("promhttp_")) {
            record.node_exporter_category = "detail".to_string();
        }
        // anything that starts with go_ are statistics about the node_exporter process
        for record in nodeexportervalues.iter_mut().filter(|r| r.node_exporter_name.starts_with("go_")) {
            record.node_exporter_category = "detail".to_string();
        }
        // anything that starts with node_scrape_collector is about the node_exporter scraper
        for record in nodeexportervalues.iter_mut().filter(|r| r.node_exporter_name.starts_with("node_scrape_collector_")) {
            record.node_exporter_category = "detail".to_string();
        }
        // any record that contains a label that contains 'dm-' is a specification of a block device, and not the block device itself
        for record in nodeexportervalues.iter_mut().filter(|r| r.node_exporter_labels.contains("dm-")) {
            record.node_exporter_category = "detail".to_string();
        }
        // softnet: node_softnet_processed_total
        if nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_softnet_processed_total").count() > 0 {
            // make current records detail records
            for record in nodeexportervalues.iter_mut().filter(|r| r.node_exporter_name == "node_softnet_processed_total") {
                record.node_exporter_category = "detail".to_string();
            }
            // add a summary record
            nodeexportervalues.push(NodeExporterValues {
                node_exporter_name: "node_softnet_processed_total".to_string(),
                node_exporter_type: "counter".to_string(),
                node_exporter_labels: "".to_string(),
                node_exporter_category: "summary".to_string(),
                node_exporter_timestamp: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_softnet_processed_total").map(|x| x.node_exporter_timestamp).min().unwrap(),
                node_exporter_value: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_softnet_processed_total").map(|x| x.node_exporter_value).sum(),
            });
        }
        // softnet: node_softnet_dropped_total
        if nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_softnet_dropped_total").count() > 0 {
            // make current records detail records
            for record in nodeexportervalues.iter_mut().filter(|r| r.node_exporter_name == "node_softnet_dropped_total") {
                record.node_exporter_category = "detail".to_string();
            }
            // add a summary record
            nodeexportervalues.push(NodeExporterValues {
                node_exporter_name: "node_softnet_dropped_total".to_string(),
                node_exporter_type: "counter".to_string(),
                node_exporter_labels: "".to_string(),
                node_exporter_category: "summary".to_string(),
                node_exporter_timestamp: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_softnet_dropped_total").map(|x| x.node_exporter_timestamp).min().unwrap(),
                node_exporter_value: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_softnet_dropped_total").map(|x| x.node_exporter_value).sum(),
            });
        }
        // softnet: node_softnet_times_squeezed_total
        if nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_softnet_times_squeezed_total").count() > 0 {
            for record in nodeexportervalues.iter_mut().filter(|r| r.node_exporter_name == "node_softnet_times_squeezed_total") {
                record.node_exporter_category = "detail".to_string();
            }
            nodeexportervalues.push(NodeExporterValues {
                node_exporter_name: "node_softnet_times_squeezed_total".to_string(),
                node_exporter_type: "counter".to_string(),
                node_exporter_labels: "".to_string(),
                node_exporter_category: "summary".to_string(),
                node_exporter_timestamp: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_softnet_times_squeezed_total").map(|x| x.node_exporter_timestamp).min().unwrap(),
                node_exporter_value: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_softnet_times_squeezed_total").map(|x| x.node_exporter_value).sum(),
            });
        }
        // schedstat: node_schedstat_waiting_seconds
        if nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_schedstat_waiting_seconds_total").count() > 0 {
            for record in nodeexportervalues.iter_mut().filter(|r| r.node_exporter_name == "node_schedstat_waiting_seconds_total") {
                record.node_exporter_category = "detail".to_string();
            }
            nodeexportervalues.push(NodeExporterValues {
                node_exporter_name: "node_schedstat_waiting_seconds_total".to_string(),
                node_exporter_type: "counter".to_string(),
                node_exporter_labels: "".to_string(),
                node_exporter_category: "summary".to_string(),
                node_exporter_timestamp: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_schedstat_waiting_seconds_total").map(|x| x.node_exporter_timestamp).min().unwrap(),
                node_exporter_value: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_schedstat_waiting_seconds_total").map(|x| x.node_exporter_value).sum(),
            });
        }
        // schedstat: node_schedstat_timeslices
        if nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_schedstat_timeslices_total").count() > 0 {
            for record in nodeexportervalues.iter_mut().filter(|r| r.node_exporter_name == "node_schedstat_timeslices_total") {
                record.node_exporter_category = "detail".to_string();
            }
            nodeexportervalues.push(NodeExporterValues {
                node_exporter_name: "node_schedstat_timeslices_total".to_string(),
                node_exporter_type: "counter".to_string(),
                node_exporter_labels: "".to_string(),
                node_exporter_category: "summary".to_string(),
                node_exporter_timestamp: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_schedstat_timeslices_total").map(|x| x.node_exporter_timestamp).min().unwrap(),
                node_exporter_value: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_schedstat_timeslices_total").map(|x| x.node_exporter_value).sum(),
            });
        }
        // schedstat: node_schedstat_running_seconds
        if nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_schedstat_running_seconds_total").count() > 0 {
            for record in nodeexportervalues.iter_mut().filter(|r| r.node_exporter_name == "node_schedstat_running_seconds_total") {
                record.node_exporter_category = "detail".to_string();
            }
            nodeexportervalues.push(NodeExporterValues {
                node_exporter_name: "node_schedstat_running_seconds_total".to_string(),
                node_exporter_type: "counter".to_string(),
                node_exporter_labels: "".to_string(),
                node_exporter_category: "summary".to_string(),
                node_exporter_timestamp: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_schedstat_running_seconds_total").map(|x| x.node_exporter_timestamp).min().unwrap(),
                node_exporter_value: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_schedstat_running_seconds_total").map(|x| x.node_exporter_value).sum(),
            });
        }
        // cpu_seconds_total:
        if nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").count() > 0 {
            for record in nodeexportervalues.iter_mut().filter(|r| r.node_exporter_name == "node_cpu_seconds_total") {
                record.node_exporter_category = "detail".to_string();
            }
            // idle
            nodeexportervalues.push(NodeExporterValues {
                node_exporter_name: "node_cpu_seconds_total".to_string(),
                node_exporter_type: "counter".to_string(),
                node_exporter_labels: "_idle".to_string(),
                node_exporter_category: "summary".to_string(),
                node_exporter_timestamp: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("idle")).map(|x| x.node_exporter_timestamp).min().unwrap(),
                node_exporter_value: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("idle")).map(|x| x.node_exporter_value).sum(),
            });
            nodeexportervalues.push(NodeExporterValues {
                node_exporter_name: "node_cpu_seconds_total".to_string(),
                node_exporter_type: "counter".to_string(),
                node_exporter_labels: "_irq".to_string(),
                node_exporter_category: "summary".to_string(),
                node_exporter_timestamp: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("_irq")).map(|x| x.node_exporter_timestamp).min().unwrap(),
                node_exporter_value: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("_irq")).map(|x| x.node_exporter_value).sum(),
            });
            nodeexportervalues.push(NodeExporterValues {
                node_exporter_name: "node_cpu_seconds_total".to_string(),
                node_exporter_type: "counter".to_string(),
                node_exporter_labels: "_softirq".to_string(),
                node_exporter_category: "summary".to_string(),
                node_exporter_timestamp: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("_softirq")).map(|x| x.node_exporter_timestamp).min().unwrap(),
                node_exporter_value: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("_softirq")).map(|x| x.node_exporter_value).sum(),
            });
            nodeexportervalues.push(NodeExporterValues {
                node_exporter_name: "node_cpu_seconds_total".to_string(),
                node_exporter_type: "counter".to_string(),
                node_exporter_labels: "_system".to_string(),
                node_exporter_category: "summary".to_string(),
                node_exporter_timestamp: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("system")).map(|x| x.node_exporter_timestamp).min().unwrap(),
                node_exporter_value: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("system")).map(|x| x.node_exporter_value).sum(),
            });
            nodeexportervalues.push(NodeExporterValues {
                node_exporter_name: "node_cpu_seconds_total".to_string(),
                node_exporter_type: "counter".to_string(),
                node_exporter_labels: "_user".to_string(),
                node_exporter_category: "summary".to_string(),
                node_exporter_timestamp: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("user")).map(|x| x.node_exporter_timestamp).min().unwrap(),
                node_exporter_value: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("user")).map(|x| x.node_exporter_value).sum(),
            });
            nodeexportervalues.push(NodeExporterValues {
                node_exporter_name: "node_cpu_seconds_total".to_string(),
                node_exporter_type: "counter".to_string(),
                node_exporter_labels: "_iowait".to_string(),
                node_exporter_category: "summary".to_string(),
                node_exporter_timestamp: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("iowait")).map(|x| x.node_exporter_timestamp).min().unwrap(),
                node_exporter_value: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("iowait")).map(|x| x.node_exporter_value).sum(),
            });
            nodeexportervalues.push(NodeExporterValues {
                node_exporter_name: "node_cpu_seconds_total".to_string(),
                node_exporter_type: "counter".to_string(),
                node_exporter_labels: "_nice".to_string(),
                node_exporter_category: "summary".to_string(),
                node_exporter_timestamp: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("nice")).map(|x| x.node_exporter_timestamp).min().unwrap(),
                node_exporter_value: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("nice")).map(|x| x.node_exporter_value).sum(),
            });
            nodeexportervalues.push(NodeExporterValues {
                node_exporter_name: "node_cpu_seconds_total".to_string(),
                node_exporter_type: "counter".to_string(),
                node_exporter_labels: "_steal".to_string(),
                node_exporter_category: "summary".to_string(),
                node_exporter_timestamp: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("steal")).map(|x| x.node_exporter_timestamp).min().unwrap(),
                node_exporter_value: nodeexportervalues.iter().filter(|r| r.node_exporter_name == "node_cpu_seconds_total").filter(|r| r.node_exporter_labels.contains("steal")).map(|x| x.node_exporter_value).sum(),
            });
        }
    }
    nodeexportervalues
}

pub fn read_node_exporter_into_vectors(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    parallel: usize
) -> Vec<StoredNodeExporterValues> {
    let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
    let (tx, rx) = channel();
    pool.scope(move |s| {
        for host in hosts {
            for port in ports {
                let tx = tx.clone();
                s.spawn(move |_| {
                    let detail_snapshot_time = Local::now();
                    let node_exporter_values = read_node_exporter(&host, &port);
                    tx.send((format!("{}:{}", host, port), detail_snapshot_time, node_exporter_values)).expect("error sending data via tx (node_exporter)");
                });
            }
        }
    });
    let mut stored_node_exporter_values: Vec<StoredNodeExporterValues> = Vec::new();
    for (hostname_port, _detail_snapshot_time, node_exporter_values) in rx {
        add_to_node_exporter_vectors(node_exporter_values, &hostname_port, &mut stored_node_exporter_values);
    }
    stored_node_exporter_values
}

pub fn add_to_node_exporter_vectors(
    node_exporter_values: Vec<NodeExporterValues>,
    hostname: &str,
    stored_node_exporter_values: &mut Vec<StoredNodeExporterValues>,
) {
    for row in node_exporter_values {
        if row.node_exporter_value > 0.0 {
            stored_node_exporter_values.push(
                StoredNodeExporterValues {
                    hostname_port: hostname.to_string(),
                    timestamp: DateTime::from(row.node_exporter_timestamp),
                    node_exporter_name: row.node_exporter_name.to_string(),
                    node_exporter_type: row.node_exporter_type.to_string(),
                    node_exporter_labels: row.node_exporter_labels.to_string(),
                    node_exporter_category: row.node_exporter_category.to_string(),
                    node_exporter_value: row.node_exporter_value,
                }
            );
        }
    }
}

pub fn perform_nodeexporter_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    snapshot_number: i32,
    yb_stats_directory: &PathBuf,
    parallel: usize,
) {
    let stored_nodeexporter = read_node_exporter_into_vectors(hosts, ports, parallel);

    let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
    let nodeexporter_file = &current_snapshot_directory.join("nodeexporter");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&nodeexporter_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing nodeexporter data in snapshot directory {}: {}", &nodeexporter_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_nodeexporter {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();
}

pub fn read_nodeexporter_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
) -> Vec<StoredNodeExporterValues> {
    let mut stored_nodeexporter: Vec<StoredNodeExporterValues> = Vec::new();
    let nodeexporter_file = &yb_stats_directory.join(&snapshot_number.to_string()).join("nodeexporter");
    let file = fs::File::open(&nodeexporter_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error reading file: {}: {}", &nodeexporter_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for  row in reader.deserialize() {
        let data: StoredNodeExporterValues = row.unwrap();
        let _ = &stored_nodeexporter.push(data);
    }
    stored_nodeexporter
}

pub fn insert_first_snapshot_nodeexporter(
    stored_nodeexporter: Vec<StoredNodeExporterValues>
) -> BTreeMap<(String, String), SnapshotDiffNodeExporter> {
    let mut nodeexporter_diff: BTreeMap<(String, String), SnapshotDiffNodeExporter> = BTreeMap::new();
    for row in stored_nodeexporter {
        nodeexporter_diff.insert(
            (row.hostname_port.to_string(), format!("{}{}", row.node_exporter_name.to_string(), row.node_exporter_labels.to_string())),
            SnapshotDiffNodeExporter {
                first_snapshot_time: row.timestamp,
                second_snapshot_time: row.timestamp,
                node_exporter_type: row.node_exporter_type.to_string(),
                category: row.node_exporter_category.to_string(),
                first_value: row.node_exporter_value,
                second_value: 0.0,
            }
        );
    }
    nodeexporter_diff
}

pub fn insert_second_snapshot_nodeexporter(
    stored_nodeexporter: Vec<StoredNodeExporterValues>,
    nodeexporter_diff: &mut BTreeMap<(String, String), SnapshotDiffNodeExporter>,
    first_snapshot_time: &DateTime<Local>,
) {
    for row in stored_nodeexporter {
        match nodeexporter_diff.get_mut( &(row.hostname_port.to_string(), format!("{}{}", row.node_exporter_name.to_string(), row.node_exporter_labels.to_string()) ) ) {
            Some( nodeexporter_diff_row ) => {
               *nodeexporter_diff_row = SnapshotDiffNodeExporter {
                   first_snapshot_time: nodeexporter_diff_row.first_snapshot_time,
                   second_snapshot_time: row.timestamp,
                   node_exporter_type: row.node_exporter_type.to_string(),
                   category: row.node_exporter_category.to_string(),
                   first_value: nodeexporter_diff_row.first_value,
                   second_value: row.node_exporter_value
               }
            },
            None => {
                nodeexporter_diff.insert(
                    (row.hostname_port.to_string(), format!("{}{}", row.node_exporter_name.to_string(), row.node_exporter_labels.to_string()) ),
                    SnapshotDiffNodeExporter {
                              first_snapshot_time: *first_snapshot_time,
                              second_snapshot_time: row.timestamp,
                              node_exporter_type: row.node_exporter_type.to_string(),
                              category: row.node_exporter_category.to_string(),
                              first_value: 0.0,
                              second_value: row.node_exporter_value
                    }
                );
            },
        }
    }
}

pub fn print_diff_nodeexporter(
    nodeexporter_diff: &BTreeMap<(String, String), SnapshotDiffNodeExporter>,
    hostname_filter: &Regex,
    stat_name_filter: &Regex,
    gauges_enable: &bool,
    details_enable: &bool,
) {
    for ((hostname, nodeexporter_name), nodeexporter_row) in nodeexporter_diff {
        if hostname_filter.is_match(&hostname)
            && stat_name_filter.is_match(&nodeexporter_name)
            && nodeexporter_row.second_value - nodeexporter_row.first_value != 0.0
            && nodeexporter_row.node_exporter_type == "counter" {
            if *details_enable && nodeexporter_row.category == "summary" { continue };
            if ! *details_enable && nodeexporter_row.category == "detail" { continue };
            println!("{:20} {:8} {:73} {:19.6} {:15.3} /s",
                     hostname,
                     nodeexporter_row.node_exporter_type,
                     nodeexporter_name,
                     nodeexporter_row.second_value - nodeexporter_row.first_value,
                     (nodeexporter_row.second_value - nodeexporter_row.first_value) / (nodeexporter_row.second_snapshot_time - nodeexporter_row.first_snapshot_time).num_seconds() as f64,
            );
        }
        if hostname_filter.is_match(&hostname)
            && stat_name_filter.is_match(&nodeexporter_name)
            && nodeexporter_row.node_exporter_type == "gauge"
            && *gauges_enable {
            if *details_enable && nodeexporter_row.category == "summary" { continue };
            if ! *details_enable && nodeexporter_row.category == "detail" { continue };
            println!("{:20} {:8} {:73} {:19.6} {:+15}",
                     hostname,
                     nodeexporter_row.node_exporter_type,
                     nodeexporter_name,
                     nodeexporter_row.second_value,
                     nodeexporter_row.second_value - nodeexporter_row.first_value
            );
        }
    }
}

pub fn print_nodeexporter_diff_for_snapshots(
    begin_snapshot: &String,
    end_snapshot: &String,
    begin_snapshot_timestamp: &DateTime<Local>,
    hostname_filter: &Regex,
    stat_name_filter: &Regex,
    gauges_enable: &bool,
    details_enable: &bool,
) {
    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    let stored_nodeexporter: Vec<StoredNodeExporterValues> = read_nodeexporter_snapshot(&begin_snapshot, &yb_stats_directory);
    let mut nodeexporter_diff = insert_first_snapshot_nodeexporter(stored_nodeexporter);
    let stored_nodeexporter: Vec<StoredNodeExporterValues> = read_nodeexporter_snapshot(&end_snapshot, &yb_stats_directory);
    insert_second_snapshot_nodeexporter(stored_nodeexporter, &mut nodeexporter_diff, &begin_snapshot_timestamp);

    print_diff_nodeexporter(&nodeexporter_diff, &hostname_filter, &stat_name_filter, &gauges_enable, &details_enable);
}

pub fn get_nodeexporter_into_diff_first_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    parallel: usize,
) -> BTreeMap<(String, String), SnapshotDiffNodeExporter> {
    let stored_node_exporter = read_node_exporter_into_vectors(&hosts, &ports, parallel);
    let node_exporter_diff = insert_first_snapshot_nodeexporter(stored_node_exporter);
    node_exporter_diff
}

pub fn get_nodeexpoter_into_diff_second_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    node_exporter_diff: &mut BTreeMap<(String, String), SnapshotDiffNodeExporter>,
    first_snapshot_time: &DateTime<Local>,
    parallel: usize,
) {
    let stored_node_exporter = read_node_exporter_into_vectors(&hosts, &ports, parallel);
    insert_second_snapshot_nodeexporter(stored_node_exporter, node_exporter_diff, &first_snapshot_time);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_non_node_exporter_data() {
        let fake_http_data = r#"
        [
    {
        "type": "tablet",
        "id": "3788ca4fb2ab40e2883a6351e7eb3bb7",
        "attributes": {
            "table_name": "sequences_data",
            "namespace_name": "system_postgres",
            "table_id": "0000ffff00003000800000000000ffff"
        },
        "metrics": [
            {
                "name": "in_progress_ops",
                "value": 0
            },
            {
                "name": "log_reader_bytes_read",
                "value": 0
            },
            {
                "name": "truncate_operations_inflight",
                "value": 0
            },
        "#.to_string();
        let result = parse_node_exporter(fake_http_data);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn parse_node_exporter_data_gauge() {
        let fake_http_data = r#"
        # HELP go_memstats_gc_cpu_fraction The fraction of this program's available CPU time used by the GC since the program started.
        # TYPE go_memstats_gc_cpu_fraction gauge
        go_memstats_gc_cpu_fraction 2.4938682471175543e-06
        "#.to_string();
        let result = parse_node_exporter(fake_http_data);
        assert_eq!(&result[0].node_exporter_name, "go_memstats_gc_cpu_fraction");
        assert_eq!(result[0].node_exporter_value, 2.4938682471175543e-6);
    }

    #[test]
    fn parse_node_exporter_data_counter() {
        let fake_http_data = r#"
        # HELP node_network_transmit_packets_total Network device statistic transmit_packets.
        # TYPE node_network_transmit_packets_total counter
        node_network_transmit_packets_total{device="eth0"} 680
        node_network_transmit_packets_total{device="eth1"} 2716
        node_network_transmit_packets_total{device="lo"} 7085
        "#.to_string();
        let result = parse_node_exporter(fake_http_data);
        assert_eq!(&result[0].node_exporter_name, "node_network_transmit_packets_total");
        assert_eq!(result[0].node_exporter_value, 680.0);
    }

    #[test]
    fn parse_node_exporter_data_untyped() {
        let fake_http_data = r#"
        # HELP node_vmstat_pgfault /proc/vmstat information field pgfault.
        # TYPE node_vmstat_pgfault untyped
        node_vmstat_pgfault 718165
        "#.to_string();
        let result = parse_node_exporter(fake_http_data);
        assert_eq!(&result[0].node_exporter_name, "node_vmstat_pgfault");
        assert_eq!(result[0].node_exporter_value, 718165.0);
    }

    #[test]
    fn parse_node_exporter_data_summary() {
        let fake_http_data = r#"
        # HELP go_gc_duration_seconds A summary of the pause duration of garbage collection cycles.
        # TYPE go_gc_duration_seconds summary
        go_gc_duration_seconds{quantile="0"} 1.2047e-05
        go_gc_duration_seconds{quantile="0.25"} 2.7231e-05
        go_gc_duration_seconds{quantile="0.5"} 4.0984e-05
        go_gc_duration_seconds{quantile="0.75"} 5.9209e-05
        go_gc_duration_seconds{quantile="1"} 0.000218416
        go_gc_duration_seconds_sum 0.000609084
        go_gc_duration_seconds_count 11
        "#.to_string();
        let result = parse_node_exporter(fake_http_data);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn parse_node_exporter_data_histogram() {
        let fake_http_data = r#"
# HELP request_duration Time for HTTP request.
# TYPE request_duration histogram
request_duration_bucket{le="0.005",} 0.0
request_duration_bucket{le="0.01",} 0.0
request_duration_bucket{le="0.025",} 0.0
request_duration_bucket{le="0.05",} 0.0
request_duration_bucket{le="0.075",} 0.0
request_duration_bucket{le="0.1",} 0.0
request_duration_bucket{le="0.25",} 0.0
request_duration_bucket{le="0.5",} 0.0
request_duration_bucket{le="0.75",} 0.0
request_duration_bucket{le="1.0",} 0.0
request_duration_bucket{le="2.5",} 0.0
request_duration_bucket{le="5.0",} 1.0
request_duration_bucket{le="7.5",} 1.0
request_duration_bucket{le="10.0",} 3.0
request_duration_bucket{le="+Inf",} 3.0
request_duration_count 3.0
request_duration_sum 22.978489699999997
        "#.to_string();
        let result = parse_node_exporter(fake_http_data);
        assert_eq!(result.len(), 0);
    }
}