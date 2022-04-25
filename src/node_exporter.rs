use std::collections::BTreeMap;
use std::path::PathBuf;
use chrono::{DateTime, Local};
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
    pub node_exporter_value: f64,

}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredNodeExporterValues {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    // sample.metric + labels
    pub node_exporter_name: String,
    // sample.value type : types Counter and Gauge
    pub node_exporter_type: String,
    // sample.value
    pub node_exporter_value: f64,
}

#[derive(Debug)]
pub struct SnapshotDiffNodeExporter {
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub node_exporter_type: String,
    pub first_value: f64,
    pub second_value: f64,
}

pub fn read_node_exporter(
    host: &str,
    port: &str,
) -> Vec<NodeExporterValues> {
    if ! scan_port_addr(format!("{}:{}", host, port)) {
        println!("Warning! hostname:port {}:{} cannot be reached, skipping", host, port);
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
                        node_exporter_name: format!("{}{}", sample.metric.to_string(), &label),
                        node_exporter_type: "counter".to_string(),
                        node_exporter_value: val,
                    }
                )
            },
            Value::Gauge(val) => {
                nodeexportervalues.push(
                    NodeExporterValues {
                        node_exporter_name: format!("{}{}", sample.metric.to_string(), label),
                        node_exporter_type: "gauge".to_string(),
                        node_exporter_value: val,
                    }
                )
            },
            Value::Histogram(_val) => {},
            Value::Summary(_val) => {},
            Value::Untyped(_val) => {},
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
                    tx.send((format!("{}:{}", host, port), detail_snapshot_time, node_exporter_values)).expect("error sending data via tx");
                });
            }
        }
    });
    let mut stored_node_exporter_values: Vec<StoredNodeExporterValues> = Vec::new();
    for (hostname_port, detail_snapshot_time, node_exporter_values) in rx {
        add_to_node_exporter_vectors(node_exporter_values, &hostname_port, detail_snapshot_time, &mut stored_node_exporter_values);
    }
    stored_node_exporter_values
}

pub fn add_to_node_exporter_vectors(
    node_exporter_values: Vec<NodeExporterValues>,
    hostname: &str,
    detail_snapshot_time: DateTime<Local>,
    stored_node_exporter_values: &mut Vec<StoredNodeExporterValues>,
) {
    for row in node_exporter_values {
        if row.node_exporter_value > 0.0 {
            stored_node_exporter_values.push(
                StoredNodeExporterValues {
                    hostname_port: hostname.to_string(),
                    timestamp: detail_snapshot_time,
                    node_exporter_name: row.node_exporter_name.to_string(),
                    node_exporter_type: row.node_exporter_type.to_string(),
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
            (row.hostname_port.to_string(), row.node_exporter_name.to_string()),
            SnapshotDiffNodeExporter {
                first_snapshot_time: row.timestamp,
                second_snapshot_time: row.timestamp,
                node_exporter_type: row.node_exporter_type.to_string(),
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
        match nodeexporter_diff.get_mut( &(row.hostname_port.to_string(), row.node_exporter_name.to_string()) ) {
            Some( nodeexporter_diff_row ) => {
               *nodeexporter_diff_row = SnapshotDiffNodeExporter {
                   first_snapshot_time: nodeexporter_diff_row.first_snapshot_time,
                   second_snapshot_time: row.timestamp,
                   node_exporter_type: row.node_exporter_type.to_string(),
                   first_value: nodeexporter_diff_row.first_value,
                   second_value: row.node_exporter_value
               }
            },
            None => {
                nodeexporter_diff.insert(
                    (row.hostname_port.to_string(), row.node_exporter_name.to_string()),
                    SnapshotDiffNodeExporter {
                              first_snapshot_time: *first_snapshot_time,
                              second_snapshot_time: row.timestamp,
                              node_exporter_type: row.node_exporter_type.to_string(),
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
) {
    for ((hostname, nodeexporter_name), nodeexporter_row) in nodeexporter_diff {
        if hostname_filter.is_match(&hostname)
            && stat_name_filter.is_match(nodeexporter_name)
            && nodeexporter_row.second_value - nodeexporter_row.first_value != 0.0 {
            if nodeexporter_row.node_exporter_type == "counter" {
                println!("{:20} {:8} {:73} {:19.6} {:15.3} /s",
                         hostname,
                         nodeexporter_row.node_exporter_type,
                         nodeexporter_name,
                         nodeexporter_row.second_value - nodeexporter_row.first_value,
                         (nodeexporter_row.second_value - nodeexporter_row.first_value) / (nodeexporter_row.second_snapshot_time - nodeexporter_row.first_snapshot_time).num_seconds() as f64,
                );
            }
        }
        if hostname_filter.is_match(&hostname)
            && stat_name_filter.is_match(&nodeexporter_name)
            && nodeexporter_row.node_exporter_type == "gauge"
            && *gauges_enable {
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
) {
    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    let stored_nodeexporter: Vec<StoredNodeExporterValues> = read_nodeexporter_snapshot(&begin_snapshot, &yb_stats_directory);
    let mut nodeexporter_diff = insert_first_snapshot_nodeexporter(stored_nodeexporter);
    let stored_nodeexporter: Vec<StoredNodeExporterValues> = read_nodeexporter_snapshot(&end_snapshot, &yb_stats_directory);
    insert_second_snapshot_nodeexporter(stored_nodeexporter, &mut nodeexporter_diff, &begin_snapshot_timestamp);

    print_diff_nodeexporter(&nodeexporter_diff, &hostname_filter, &stat_name_filter, &gauges_enable);
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