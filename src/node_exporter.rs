//! The module for prometheus metrics from /metrics endpoint of node-exporter.
use std::{collections::BTreeMap, process, sync::mpsc::channel, fs, env, time::Instant, error::Error};
use chrono::{DateTime, Local, Utc};
use prometheus_parse::Value;
use serde_derive::{Serialize,Deserialize};
use port_scanner::scan_port_addr;
use regex::Regex;
use log::*;

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

impl SnapshotDiffNodeExporter {
    fn first_snapshot(storednodeexportervalues: StoredNodeExporterValues) -> Self {
        Self {
            first_snapshot_time: storednodeexportervalues.timestamp,
            second_snapshot_time: storednodeexportervalues.timestamp,
            node_exporter_type: storednodeexportervalues.node_exporter_type.to_string(),
            category: storednodeexportervalues.node_exporter_category.to_string(),
            first_value: storednodeexportervalues.node_exporter_value,
            second_value: 0.,
        }
    }
    fn second_snapshot_existing(storednodeexportervalues: StoredNodeExporterValues, nodeexporter_diff_row: &mut SnapshotDiffNodeExporter) -> Self
    {
        Self {
            first_snapshot_time: nodeexporter_diff_row.first_snapshot_time,
            second_snapshot_time: storednodeexportervalues.timestamp,
            node_exporter_type: storednodeexportervalues.node_exporter_type.to_string(),
            category: storednodeexportervalues.node_exporter_category.to_string(),
            first_value: nodeexporter_diff_row.first_value,
            second_value: storednodeexportervalues.node_exporter_value,
        }
    }
    fn second_snapshot_new(storednodeexportervalues: StoredNodeExporterValues, first_snapshot_time: DateTime<Local>) -> Self
    {
        Self {
            first_snapshot_time,
            second_snapshot_time: storednodeexportervalues.timestamp,
            node_exporter_type: storednodeexportervalues.node_exporter_type.to_string(),
            category: storednodeexportervalues.node_exporter_category.to_string(),
            first_value: 0.,
            second_value: storednodeexportervalues.node_exporter_value,
        }
    }
}

pub struct AllStoredNodeExporterValues {
    pub stored_nodeexportervalues: Vec<StoredNodeExporterValues>,
}
impl AllStoredNodeExporterValues {
    pub fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) {
        info!("begin snapshot");
        let timer = Instant::now();

        let allstorednodeexportervalues = AllStoredNodeExporterValues::read_nodeexporter(hosts, ports, parallel);
        allstorednodeexportervalues.save_snapshot(snapshot_number)
            .unwrap_or_else(|e| {
                error!("error saving snapshot: {}", e);
                process::exit(1);
            });

        info!("end snapshot: {:?}", timer.elapsed())
    }
    fn save_snapshot(self, snapshot_number: i32) -> Result<(), Box<dyn Error>>
    {
        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number.to_string());

        let nodeexporter_file = &current_snapshot_directory.join("nodeexporter");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&nodeexporter_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_nodeexportervalues {
            writer.serialize(row)?;
        }
        writer.flush()?;

        Ok(())
    }
    fn read_snapshot( snapshot_number: &String, ) -> Result<AllStoredNodeExporterValues, Box<dyn Error>>
    {
        let mut allstorednodeexportervalues = AllStoredNodeExporterValues { stored_nodeexportervalues: Vec::new() };

        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number);

        let nodeexporter_file = &current_snapshot_directory.join("nodeexporter");
        let file = fs::File::open(&nodeexporter_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for  row in reader.deserialize() {
            let data: StoredNodeExporterValues = row?;
            allstorednodeexportervalues.stored_nodeexportervalues.push(data);
        }

        Ok(allstorednodeexportervalues)
    }
    pub fn read_nodeexporter(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize
    ) -> AllStoredNodeExporterValues
    {
        info!("begin parallel http read");
        let timer = Instant::now();

        let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
        let (tx, rx) = channel();
        pool.scope(move |s| {
            for host in hosts {
                for port in ports {
                    let tx = tx.clone();
                    s.spawn(move |_| {
                        let detail_snapshot_time = Local::now();
                        let node_exporter_values = AllStoredNodeExporterValues::read_http(host, port);
                        tx.send((format!("{}:{}", host, port), detail_snapshot_time, node_exporter_values)).expect("error sending data via tx (node_exporter)");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allstorednodeexportervalues = AllStoredNodeExporterValues { stored_nodeexportervalues: Vec::new() };
        for (hostname_port, _detail_snapshot_time, node_exporter_values) in rx {
            AllStoredNodeExporterValues::add_to_vector(node_exporter_values, &hostname_port, &mut allstorednodeexportervalues);
        }
        allstorednodeexportervalues
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> Vec<NodeExporterValues> {
        if ! scan_port_addr(format!("{}:{}", host, port)) {
            warn!("Warning! hostname:port {}:{} cannot be reached, skipping (node_exporter)", host, port);
            return AllStoredNodeExporterValues::parse_nodeexporter(String::from(""))
        };
        let data_from_http = reqwest::blocking::get(format!("http://{}:{}/metrics", host, port))
            .unwrap_or_else(|e| {
                error!("Fatal: error reading from URL: {}", e);
                process::exit(1);
            })
            .text().unwrap();
        AllStoredNodeExporterValues::parse_nodeexporter(data_from_http)
    }
    fn parse_nodeexporter( node_exporter_data: String ) -> Vec<NodeExporterValues>
    {
        // This is the actual parsing
        let node_exporter_rows = prometheus_parse::Scrape::parse(node_exporter_data.lines().map(|s| Ok(s.to_owned()))).unwrap();

        // post processing
        let mut nodeexportervalues: Vec<NodeExporterValues> = Vec::new();
        if !node_exporter_rows.samples.is_empty()
        {
            for sample in node_exporter_rows.samples
            {
                // Build a label of the different labels of a sample
                let mut label_temp = sample.labels.values().cloned().collect::<Vec<String>>();
                label_temp.sort();
                let mut label = label_temp.join("_");
                label = if !label.is_empty() {
                    format!("_{}", label)
                } else {
                    label
                };
                // Insert the sample into the nodeexportervalues vector.
                // Currently, histogram and summary types are not used in YugabyteDB.
                // YugabyteDB uses gauges and counters, but doesn't actually specify it.
                // NodeExporter uses gauges, counters, untyped and one summary.
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
            nodeexporter_statistics_to_detail(&mut nodeexportervalues);
            linux_dm_to_detail(&mut nodeexportervalues);
            linux_softnet_sum(&mut nodeexportervalues);
            linux_schedstat_sum(&mut nodeexportervalues);
            linux_cpu_sum(&mut nodeexportervalues);
        }
        nodeexportervalues
    }
    fn add_to_vector(
        node_exporter_values: Vec<NodeExporterValues>,
        hostname: &str,
        allstorednodeexportervalues: &mut AllStoredNodeExporterValues,
    )
    {
        for row in node_exporter_values {
            if row.node_exporter_value > 0.0 {
                allstorednodeexportervalues.stored_nodeexportervalues.push(
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
}

type BTreeMapSnapshotDiffNodeExporter = BTreeMap<(String, String), SnapshotDiffNodeExporter>;

pub struct SnapshotDiffBTreeMapNodeExporter {
    pub btreemap_snapshotdiff_nodeexporter: BTreeMapSnapshotDiffNodeExporter,
}

impl SnapshotDiffBTreeMapNodeExporter {
    pub fn snapshot_diff (
        begin_snapshot: &String,
        end_snapshot: &String,
        begin_snapshot_time: &DateTime<Local>,
    ) -> SnapshotDiffBTreeMapNodeExporter
    {
        let allstorednodeexportervalues = AllStoredNodeExporterValues::read_snapshot(begin_snapshot)
            .unwrap_or_else(|e| {
                error!("Fatal: error reading snapshot: {}", e);
                process::exit(1);
            });

        let mut nodeexporter_snapshot_diff = SnapshotDiffBTreeMapNodeExporter::first_snapshot(allstorednodeexportervalues);

        let allstorednodeexportervalues = AllStoredNodeExporterValues::read_snapshot(end_snapshot)
            .unwrap_or_else(|e| {
                error!("Fatal: error reading snapshot: {}", e);
                process::exit(1);
            });

        nodeexporter_snapshot_diff.second_snapshot(allstorednodeexportervalues, begin_snapshot_time);

        nodeexporter_snapshot_diff
    }
    pub fn adhoc_read_first_snapshot (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> SnapshotDiffBTreeMapNodeExporter
    {
        let allstorednodeexportervalues = AllStoredNodeExporterValues::read_nodeexporter(hosts, ports, parallel);
        SnapshotDiffBTreeMapNodeExporter::first_snapshot(allstorednodeexportervalues)
    }
    fn first_snapshot(allstorednodeexportervalues: AllStoredNodeExporterValues) -> SnapshotDiffBTreeMapNodeExporter
    {
        let mut nodeexporter_diff_btreemap = SnapshotDiffBTreeMapNodeExporter { btreemap_snapshotdiff_nodeexporter: BTreeMap::new() };
        for row in allstorednodeexportervalues.stored_nodeexportervalues {
            nodeexporter_diff_btreemap.btreemap_snapshotdiff_nodeexporter.insert(
                (row.hostname_port.to_string(), format!("{}{}", row.node_exporter_name, row.node_exporter_labels)),
                SnapshotDiffNodeExporter::first_snapshot(row)
            );
        }
        nodeexporter_diff_btreemap
    }
    pub fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
        first_snapshot_time: &DateTime<Local>,
    )
    {
        let allstorednodeexporter = AllStoredNodeExporterValues::read_nodeexporter(hosts, ports, parallel);
        self.second_snapshot(allstorednodeexporter, first_snapshot_time);
    }
    fn second_snapshot(
        &mut self,
        allstorednodeexporter: AllStoredNodeExporterValues,
        first_snapshot_time: &DateTime<Local>,
    )
    {
        for row in allstorednodeexporter.stored_nodeexportervalues {
            match self.btreemap_snapshotdiff_nodeexporter.get_mut( &(row.hostname_port.to_string(), format!("{}{}",row.node_exporter_name, row.node_exporter_labels)) )
            {
                Some(nodeexporter_diff_row) => {
                   *nodeexporter_diff_row =  SnapshotDiffNodeExporter::second_snapshot_existing(row, nodeexporter_diff_row)
                },
                None => {
                    self.btreemap_snapshotdiff_nodeexporter.insert(
                        (row.hostname_port.to_string(), format!("{}{}", row.node_exporter_name, row.node_exporter_labels)),
                        SnapshotDiffNodeExporter::second_snapshot_new(row, *first_snapshot_time)
                    );
                },
            }
        }
    }
    pub fn print(
        &self,
        hostname_filter: &Regex,
        stat_name_filter: &Regex,
        gauges_enable: &bool,
        details_enable: &bool,
    )
    {
        for ((hostname, nodeexporter_name), nodeexporter_row) in &self.btreemap_snapshotdiff_nodeexporter {
            if hostname_filter.is_match(hostname)
                && stat_name_filter.is_match(nodeexporter_name)
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
            if hostname_filter.is_match(hostname)
                && stat_name_filter.is_match(nodeexporter_name)
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
}

//fn nodeexporter_statistics_to_detail(nodeexportervalues: &mut Vec<NodeExporterValues>)
fn nodeexporter_statistics_to_detail(nodeexportervalues: &mut [NodeExporterValues])
{
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
}

fn linux_dm_to_detail(nodeexportervalues: &mut [NodeExporterValues])
{
    // any record that contains a label that contains 'dm-' is a specification of a block device, and not the block device itself
    for record in nodeexportervalues.iter_mut().filter(|r| r.node_exporter_labels.contains("dm-")) {
        record.node_exporter_category = "detail".to_string();
    }
}

fn linux_softnet_sum(nodeexportervalues: &mut Vec<NodeExporterValues>)
{
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
}

fn linux_schedstat_sum(nodeexportervalues: &mut Vec<NodeExporterValues>)
{
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
}

fn linux_cpu_sum(nodeexportervalues: &mut Vec<NodeExporterValues>)
{
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_node_exporter_non_prometheus_data() {
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
        let result = AllStoredNodeExporterValues::parse_nodeexporter(fake_http_data);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn unit_parse_node_exporter_data_gauge() {
        let fake_http_data = r#"
        # HELP go_memstats_gc_cpu_fraction The fraction of this program's available CPU time used by the GC since the program started.
        # TYPE go_memstats_gc_cpu_fraction gauge
        go_memstats_gc_cpu_fraction 2.4938682471175543e-06
        "#.to_string();
        let result = AllStoredNodeExporterValues::parse_nodeexporter(fake_http_data);
        assert_eq!(&result[0].node_exporter_name, "go_memstats_gc_cpu_fraction");
        assert_eq!(result[0].node_exporter_value, 2.4938682471175543e-6);
    }

    #[test]
    fn unit_parse_node_exporter_data_counter() {
        let fake_http_data = r#"
        # HELP node_network_transmit_packets_total Network device statistic transmit_packets.
        # TYPE node_network_transmit_packets_total counter
        node_network_transmit_packets_total{device="eth0"} 680
        node_network_transmit_packets_total{device="eth1"} 2716
        node_network_transmit_packets_total{device="lo"} 7085
        "#.to_string();
        let result = AllStoredNodeExporterValues::parse_nodeexporter(fake_http_data);
        assert_eq!(&result[0].node_exporter_name, "node_network_transmit_packets_total");
        assert_eq!(result[0].node_exporter_value, 680.0);
    }

    #[test]
    fn unit_parse_node_exporter_data_untyped() {
        let fake_http_data = r#"
        # HELP node_vmstat_pgfault /proc/vmstat information field pgfault.
        # TYPE node_vmstat_pgfault untyped
        node_vmstat_pgfault 718165
        "#.to_string();
        let result = AllStoredNodeExporterValues::parse_nodeexporter(fake_http_data);
        assert_eq!(&result[0].node_exporter_name, "node_vmstat_pgfault");
        assert_eq!(result[0].node_exporter_value, 718165.0);
    }

    #[test]
    fn unit_parse_node_exporter_data_summary() {
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
        let result = AllStoredNodeExporterValues::parse_nodeexporter(fake_http_data);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn unit_parse_node_exporter_data_histogram() {
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
        let result = AllStoredNodeExporterValues::parse_nodeexporter(fake_http_data);
        assert_eq!(result.len(), 0);
    }

    use crate::utility;

    #[test]
    fn integration_parse_node_exporter() {
        let hostname = utility::get_hostname_node_exporter();
        if hostname == *"SKIP" {
            // workaround for allowing integration tests where no node exporter is present.
            return;
        }
        let port = utility::get_port_node_exporter();

        let mut allstorednodeexportervalues = AllStoredNodeExporterValues { stored_nodeexportervalues: Vec::new() };
        let node_exporter_values = AllStoredNodeExporterValues::read_http(hostname.as_str(), port.as_str());
        AllStoredNodeExporterValues::add_to_vector(node_exporter_values, format!("{}:{}",hostname, port).as_ref(), &mut allstorednodeexportervalues);
        // a node exporter endpoint will generate entries in the stored_nodeexportervalues vector.
        assert!(!allstorednodeexportervalues.stored_nodeexportervalues.is_empty());
    }
}