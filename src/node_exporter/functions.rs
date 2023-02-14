//! The impls and functions
//! 
use std::{sync::mpsc::channel, time::Instant};
use chrono::{DateTime, Local};
use prometheus_parse::Value;
use regex::Regex;
use log::*;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::node_exporter::{NodeExporter, AllNodeExporter, NodeExporterDiff, NameCategoryDiff};

impl AllNodeExporter {
    pub fn new() -> Self {
        Default::default()
    }
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    )  -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allnodeexporter = AllNodeExporter::read_nodeexporter(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "nodeexporter", allnodeexporter.nodeexporter)?;

        info!("end snapshot: {:?}", timer.elapsed());
        Ok(())
    }
    pub async fn read_nodeexporter(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize
    ) -> AllNodeExporter
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
                        let mut nodeexporter = AllNodeExporter::read_http(host, port);
                        nodeexporter.iter_mut().for_each(|r| r.timestamp = detail_snapshot_time);
                        nodeexporter.iter_mut().for_each(|r| r.hostname_port = format!("{}:{}", host, port));
                        tx.send(nodeexporter).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allnodeexporter = AllNodeExporter::new();
        for nodeexporters in rx 
        {
            for nodeexporter in nodeexporters 
            {
                allnodeexporter.nodeexporter.push(nodeexporter);
            }
        }
        
        allnodeexporter
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> Vec<NodeExporter>
    {
        let data_from_http = utility::http_get(host, port, "metrics");
        AllNodeExporter::parse_nodeexporter(data_from_http)
    }
    fn parse_nodeexporter( 
        node_exporter_data: String
    ) -> Vec<NodeExporter>
    {
        // This is the actual parsing
        let node_exporter_rows = prometheus_parse::Scrape::parse(node_exporter_data.lines().map(|s| Ok(s.to_owned()))).unwrap();

        // post processing
        let mut nodeexporter = Vec::new();
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
                // Insert the sample into the nodeexporter vector.
                // Currently, histogram and summary types are not used in YugabyteDB.
                // YugabyteDB uses gauges and counters, but doesn't actually specify it.
                // NodeExporter uses gauges, counters, untyped and one summary.
                match sample.value {
                    Value::Counter(val) => {
                        nodeexporter.push(
                            NodeExporter {
                                name: sample.metric.to_string(),
                                exporter_type: "counter".to_string(),
                                labels: label,
                                category: "all".to_string(),
                                exporter_timestamp: sample.timestamp,
                                value: val,
                                ..Default::default()
                            }
                        )
                    },
                    Value::Gauge(val) => {
                        nodeexporter.push(
                            NodeExporter {
                                name: sample.metric.to_string(),
                                exporter_type: "gauge".to_string(),
                                labels: label,
                                category: "all".to_string(),
                                exporter_timestamp: sample.timestamp,
                                value: val,
                                ..Default::default()
                            }
                        )
                    },
                    Value::Untyped(val) => {
                        // it turns out summary type _sum and _count values are untyped values.
                        // so I remove them here.
                        if sample.metric.ends_with("_sum") || sample.metric.ends_with("_count") { continue };
                        // untyped: not sure what it is.
                        // I would say: probably a counter.
                        nodeexporter.push(
                            NodeExporter {
                                name: sample.metric.to_string(),
                                exporter_type: "counter".to_string(),
                                labels: label,
                                category: "all".to_string(),
                                exporter_timestamp: sample.timestamp,
                                value: val,
                                ..Default::default()
                            }
                        )
                    },
                    Value::Histogram(_val) => {},
                    Value::Summary(_val) => {},
                }
            }
            // post processing
            nodeexporter_statistics_to_detail(&mut nodeexporter);
            linux_dm_to_detail(&mut nodeexporter);
            linux_softnet_sum(&mut nodeexporter);
            linux_schedstat_sum(&mut nodeexporter);
            linux_cpu_sum(&mut nodeexporter);
        }
        nodeexporter
    }
}

impl NodeExporterDiff {
    pub fn new() -> Self {
        Default::default()
    }
    pub fn snapshot_diff (
        begin_snapshot: &String,
        end_snapshot: &String,
        begin_snapshot_time: &DateTime<Local>,
    ) -> Result<NodeExporterDiff>
    {
        let mut nodeexporterdiff = NodeExporterDiff::new();

        let mut allnodeexporter = AllNodeExporter::new();
        allnodeexporter.nodeexporter = snapshot::read_snapshot_json(begin_snapshot, "nodeexporter")?;
        nodeexporterdiff.first_snapshot(allnodeexporter);

        let mut allnodeexporter = AllNodeExporter::new();
        allnodeexporter.nodeexporter = snapshot::read_snapshot_json(end_snapshot, "nodeexporter")?;
        nodeexporterdiff.second_snapshot(allnodeexporter, begin_snapshot_time);

        Ok(nodeexporterdiff)
    }
    fn first_snapshot(
        &mut self,
        allnodeexporter: AllNodeExporter,
    )
    {
        for row in &allnodeexporter.nodeexporter
        {
            self.btreemapnodeexporterdiff
                .entry((row.hostname_port.clone(), row.name.clone(), row.labels.clone()))
                .and_modify(|_|
                    error!("Duplicate combination of hostname_port: {}, name: {}, labels: {}",
                       row.hostname_port,
                       row.name.clone(),
                       row.labels.clone(),
                    )
                )
                .or_insert(NameCategoryDiff {
                    first_snapshot_time: row.timestamp,
                    exporter_type: row.exporter_type.clone(),
                    category: row.category.clone(),
                    first_value: row.value,
                    ..Default::default()
                });
        }
    }
    fn second_snapshot(
        &mut self,
        allnodeexporter: AllNodeExporter,
        first_snapshot_time: &DateTime<Local>,
    )
    {
        for row in &allnodeexporter.nodeexporter
        {
            self.btreemapnodeexporterdiff
                .entry((row.hostname_port.clone(), row.name.clone(), row.labels.clone()))
                .and_modify( |namecategorydiff| {
                    namecategorydiff.second_snapshot_time = row.timestamp;
                    namecategorydiff.second_value = row.value;
                })
                .or_insert( NameCategoryDiff {
                    first_snapshot_time: *first_snapshot_time,
                    second_snapshot_time: row.timestamp,
                    exporter_type: row.exporter_type.clone(),
                    category: row.category.clone(),
                    second_value: row.value,
                    ..Default::default()
                });
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
        for ((hostname_port, name, category), diff_row) in &self.btreemapnodeexporterdiff {
            if hostname_filter.is_match(hostname_port)
                && stat_name_filter.is_match(name)
                && diff_row.second_value - diff_row.first_value != 0.0
                && diff_row.exporter_type == "counter"
            {
                if *details_enable && category == "summary" { continue };
                if ! *details_enable && diff_row.category == "detail" { continue };
                println!("{:20} {:8} {:73} {:19.6} {:15.3} /s",
                         hostname_port,
                         diff_row.exporter_type,
                         format!("{}{}", name, category),
                         diff_row.second_value - diff_row.first_value,
                         (diff_row.second_value - diff_row.first_value) / (diff_row.second_snapshot_time - diff_row.first_snapshot_time).num_seconds() as f64,
                );
            }
            if hostname_filter.is_match(hostname_port)
                && stat_name_filter.is_match(hostname_port)
                && diff_row.exporter_type == "gauge"
                && *gauges_enable
            {
                if *details_enable && category == "summary" { continue };
                if ! *details_enable && diff_row.category == "detail" { continue };
                println!("{:20} {:8} {:73} {:19.6} {:+15}",
                         hostname_port,
                         diff_row.exporter_type,
                         format!("{}{}", name, category),
                         diff_row.second_value,
                         diff_row.second_value - diff_row.first_value
                );
            }
        }
    }
    pub async fn adhoc_read_first_snapshot (
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allnodeexporter = AllNodeExporter::read_nodeexporter(hosts, ports, parallel).await;
        self.first_snapshot(allnodeexporter);
    }
    pub async fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
        first_snapshot_time: &DateTime<Local>,
    )
    {
        let allnodeexporter = AllNodeExporter::read_nodeexporter(hosts, ports, parallel).await;
        self.second_snapshot(allnodeexporter, first_snapshot_time);
    }
}

fn nodeexporter_statistics_to_detail(nodeexporter: &mut [NodeExporter])
{
    // anything that starts with process_ is node_exporter process
    for record in nodeexporter.iter_mut().filter(|r| r.name.starts_with("process_")) {
        record.category = "detail".to_string();
    }
    // anything that start with promhttp_ is the node_exporter http server
    for record in nodeexporter.iter_mut().filter(|r| r.name.starts_with("promhttp_")) {
        record.category = "detail".to_string();
    }
    // anything that starts with go_ are statistics about the node_exporter process
    for record in nodeexporter.iter_mut().filter(|r| r.name.starts_with("go_")) {
        record.category = "detail".to_string();
    }
    // anything that starts with node_scrape_collector is about the node_exporter scraper
    for record in nodeexporter.iter_mut().filter(|r| r.name.starts_with("node_scrape_collector_")) {
        record.category = "detail".to_string();
    }
    // any record that contains a label that contains 'dm-' is a specification of a block device, and not the block device itself
    for record in nodeexporter.iter_mut().filter(|r| r.name.contains("dm-")) {
        record.category = "detail".to_string();
    }
}

fn linux_dm_to_detail(nodeexporter: &mut [NodeExporter])
{
    // any record that contains a label that contains 'dm-' is a specification of a block device, and not the block device itself
    for record in nodeexporter.iter_mut().filter(|r| r.labels.contains("dm-")) {
        record.category = "detail".to_string();
    }
}

fn linux_softnet_sum(nodeexporter: &mut Vec<NodeExporter>)
{
    // softnet: node_softnet_processed_total
    if nodeexporter.iter().filter(|r| r.name == "node_softnet_processed_total").count() > 0 {
        // make current records detail records
        for record in nodeexporter.iter_mut().filter(|r| r.name == "node_softnet_processed_total") {
            record.category = "detail".to_string();
        }
        // add a summary record
        nodeexporter.push(NodeExporter{
            name: "node_softnet_processed_total".to_string(),
            exporter_type: "counter".to_string(),
            labels: "".to_string(),
            category: "summary".to_string(),
            exporter_timestamp: nodeexporter.iter().filter(|r| r.name == "node_softnet_processed_total").map(|x| x.exporter_timestamp).min().unwrap(),
            value: nodeexporter.iter().filter(|r| r.name == "node_softnet_processed_total").map(|x| x.value).sum(),
            ..Default::default()
        });
    }
    // softnet: node_softnet_dropped_total
    if nodeexporter.iter().filter(|r| r.name == "node_softnet_dropped_total").count() > 0 {
        // make current records detail records
        for record in nodeexporter.iter_mut().filter(|r| r.name == "node_softnet_dropped_total") {
            record.category = "detail".to_string();
        }
        // add a summary record
        nodeexporter.push(NodeExporter {
            name: "node_softnet_dropped_total".to_string(),
            exporter_type: "counter".to_string(),
            labels: "".to_string(),
            category: "summary".to_string(),
            exporter_timestamp: nodeexporter.iter().filter(|r| r.name == "node_softnet_dropped_total").map(|x| x.exporter_timestamp).min().unwrap(),
            value: nodeexporter.iter().filter(|r| r.name == "node_softnet_dropped_total").map(|x| x.value).sum(),
            ..Default::default()
        });
    }
    // softnet: node_softnet_times_squeezed_total
    if nodeexporter.iter().filter(|r| r.name == "node_softnet_times_squeezed_total").count() > 0 {
        for record in nodeexporter.iter_mut().filter(|r| r.name == "node_softnet_times_squeezed_total") {
            record.category = "detail".to_string();
        }
        nodeexporter.push(NodeExporter {
            name: "node_softnet_times_squeezed_total".to_string(),
            exporter_type: "counter".to_string(),
            labels: "".to_string(),
            category: "summary".to_string(),
            exporter_timestamp: nodeexporter.iter().filter(|r| r.name == "node_softnet_times_squeezed_total").map(|x| x.exporter_timestamp).min().unwrap(),
            value: nodeexporter.iter().filter(|r| r.name == "node_softnet_times_squeezed_total").map(|x| x.value).sum(),
            ..Default::default()
        });
    }
}
/// Scheduler statistics summarization.
///
/// Scheduler statistics are provided per logical CPU. That is the most correct way to show these statistics.
/// However: that is not very easy for a human to read.
/// For that reason, this function sums up the total amounts of the schedstat statistics:
/// - schedstat_waiting (task runnable, but not running on CPU, waiting for runtime)
/// - schedstat_running (task runnable and running on CPU)
/// - schedstat_timeslices (the number of timeslices executed)
/// The original values are kept, but put in category 'detail'.
/// The summarized values are put in a category 'summary'.
fn linux_schedstat_sum(nodeexporter: &mut Vec<NodeExporter>)
{
    // schedstat: node_schedstat_waiting_seconds
    if nodeexporter.iter().filter(|r| r.name == "node_schedstat_waiting_seconds_total").count() > 0 {
        for record in nodeexporter.iter_mut().filter(|r| r.name == "node_schedstat_waiting_seconds_total") {
            record.category = "detail".to_string();
        }
        nodeexporter.push(NodeExporter {
            name: "node_schedstat_waiting_seconds_total".to_string(),
            exporter_type: "counter".to_string(),
            labels: "".to_string(),
            category: "summary".to_string(),
            exporter_timestamp: nodeexporter.iter().filter(|r| r.name == "node_schedstat_waiting_seconds_total").map(|x| x.exporter_timestamp).min().unwrap(),
            value: nodeexporter.iter().filter(|r| r.name == "node_schedstat_waiting_seconds_total").map(|x| x.value).sum(),
            ..Default::default()
        });
    }
    // schedstat: node_schedstat_timeslices
    if nodeexporter.iter().filter(|r| r.name == "node_schedstat_timeslices_total").count() > 0 {
        for record in nodeexporter.iter_mut().filter(|r| r.name == "node_schedstat_timeslices_total") {
            record.category = "detail".to_string();
        }
        nodeexporter.push(NodeExporter {
            name: "node_schedstat_timeslices_total".to_string(),
            exporter_type: "counter".to_string(),
            labels: "".to_string(),
            category: "summary".to_string(),
            exporter_timestamp: nodeexporter.iter().filter(|r| r.name == "node_schedstat_timeslices_total").map(|x| x.exporter_timestamp).min().unwrap(),
            value: nodeexporter.iter().filter(|r| r.name == "node_schedstat_timeslices_total").map(|x| x.value).sum(),
            ..Default::default()
        });
    }
    // schedstat: node_schedstat_running_seconds
    if nodeexporter.iter().filter(|r| r.name == "node_schedstat_running_seconds_total").count() > 0 {
        for record in nodeexporter.iter_mut().filter(|r| r.name == "node_schedstat_running_seconds_total") {
            record.category = "detail".to_string();
        }
        nodeexporter.push(NodeExporter {
            name: "node_schedstat_running_seconds_total".to_string(),
            exporter_type: "counter".to_string(),
            labels: "".to_string(),
            category: "summary".to_string(),
            exporter_timestamp: nodeexporter.iter().filter(|r| r.name == "node_schedstat_running_seconds_total").map(|x| x.exporter_timestamp).min().unwrap(),
            value: nodeexporter.iter().filter(|r| r.name == "node_schedstat_running_seconds_total").map(|x| x.value).sum(),
            ..Default::default()
        });
    }
}
/// CPU statistics summarization.
///
/// CPU statistics are provided per logical CPU. That is the most correct way to show these statistics.
/// However: that is not very easy for a human to read.
/// For that reason, this function sums up the total amounts of the cpu statistics:
/// - user      : amount of time running in normal (user) mode.
/// - system    : amount of time running in kernel (system) mode.
/// - nice      : amount of time running in normal mode with a changed priority.
/// - irq       : amount of time running for interrupt CPU usage.
/// - softirq   : amount of time running deferrable functions (non-urgent interruptable kernel functions).
/// - idle      : amount of time NOT running.
/// - iowait    : this is a special case. There is no IO wait time in the kernel. Instead for regular, buffered, IO, linux keeps a counter of outstanding IOs, and tries to map idle time to these, based on idle time availability.
///               More advanced IO interfaces mostly do not increase this counter, such as io_submit/io_getevents.
/// - steal     : this too is a special case: this is the time the hypervisor did not get CPU slices. Higher values for this could indicate CPU oversubscription by the hypervisor.
///
/// The original values are kept, but put in category 'detail'.
/// The summarized values are put in a category 'summary'.
/// TBD: category node_cpu_guest_seconds
fn linux_cpu_sum(nodeexporter: &mut Vec<NodeExporter>)
{
    // cpu_seconds_total:
    if nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").count() > 0 {
        for record in nodeexporter.iter_mut().filter(|r| r.name == "node_cpu_seconds_total") {
            record.category = "detail".to_string();
        }
        // idle
        nodeexporter.push(NodeExporter {
            name: "node_cpu_seconds_total".to_string(),
            exporter_type: "counter".to_string(),
            labels: "_idle".to_string(),
            category: "summary".to_string(),
            exporter_timestamp: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("idle")).map(|x| x.exporter_timestamp).min().unwrap(),
            value: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("idle")).map(|x| x.value).sum(),
            ..Default::default()
        });
        nodeexporter.push(NodeExporter {
            name: "node_cpu_seconds_total".to_string(),
            exporter_type: "counter".to_string(),
            labels: "_irq".to_string(),
            category: "summary".to_string(),
            exporter_timestamp: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("_irq")).map(|x| x.exporter_timestamp).min().unwrap(),
            value: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("_irq")).map(|x| x.value).sum(),
            ..Default::default()
        });
        nodeexporter.push(NodeExporter {
            name: "node_cpu_seconds_total".to_string(),
            exporter_type: "counter".to_string(),
            labels: "_softirq".to_string(),
            category: "summary".to_string(),
            exporter_timestamp: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("_softirq")).map(|x| x.exporter_timestamp).min().unwrap(),
            value: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("_softirq")).map(|x| x.value).sum(),
            ..Default::default()
        });
        nodeexporter.push(NodeExporter {
            name: "node_cpu_seconds_total".to_string(),
            exporter_type: "counter".to_string(),
            labels: "_system".to_string(),
            category: "summary".to_string(),
            exporter_timestamp: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("system")).map(|x| x.exporter_timestamp).min().unwrap(),
            value: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("system")).map(|x| x.value).sum(),
            ..Default::default()
        });
        nodeexporter.push(NodeExporter {
            name: "node_cpu_seconds_total".to_string(),
            exporter_type: "counter".to_string(),
            labels: "_user".to_string(),
            category: "summary".to_string(),
            exporter_timestamp: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("user")).map(|x| x.exporter_timestamp).min().unwrap(),
            value: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("user")).map(|x| x.value).sum(),
            ..Default::default()
        });
        nodeexporter.push(NodeExporter {
            name: "node_cpu_seconds_total".to_string(),
            exporter_type: "counter".to_string(),
            labels: "_iowait".to_string(),
            category: "summary".to_string(),
            exporter_timestamp: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("iowait")).map(|x| x.exporter_timestamp).min().unwrap(),
            value: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("iowait")).map(|x| x.value).sum(),
            ..Default::default()
        });
        nodeexporter.push(NodeExporter {
            name: "node_cpu_seconds_total".to_string(),
            exporter_type: "counter".to_string(),
            labels: "_nice".to_string(),
            category: "summary".to_string(),
            exporter_timestamp: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("nice")).map(|x| x.exporter_timestamp).min().unwrap(),
            value: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("nice")).map(|x| x.value).sum(),
            ..Default::default()
        });
        nodeexporter.push(NodeExporter {
            name: "node_cpu_seconds_total".to_string(),
            exporter_type: "counter".to_string(),
            labels: "_steal".to_string(),
            category: "summary".to_string(),
            exporter_timestamp: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("steal")).map(|x| x.exporter_timestamp).min().unwrap(),
            value: nodeexporter.iter().filter(|r| r.name == "node_cpu_seconds_total").filter(|r| r.labels.contains("steal")).map(|x| x.value).sum(),
            ..Default::default()
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
        let result = AllNodeExporter::parse_nodeexporter(fake_http_data);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn unit_parse_node_exporter_data_gauge() {
        let fake_http_data = r#"
        # HELP go_memstats_gc_cpu_fraction The fraction of this program's available CPU time used by the GC since the program started.
        # TYPE go_memstats_gc_cpu_fraction gauge
        go_memstats_gc_cpu_fraction 2.4938682471175543e-06
        "#.to_string();
        let result = AllNodeExporter::parse_nodeexporter(fake_http_data);
        assert_eq!(&result[0].name, "go_memstats_gc_cpu_fraction");
        assert_eq!(result[0].value, 2.4938682471175543e-6);
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
        let result = AllNodeExporter::parse_nodeexporter(fake_http_data);
        assert_eq!(&result[0].name, "node_network_transmit_packets_total");
        assert_eq!(result[0].value, 680.0);
    }

    #[test]
    fn unit_parse_node_exporter_data_untyped() {
        let fake_http_data = r#"
        # HELP node_vmstat_pgfault /proc/vmstat information field pgfault.
        # TYPE node_vmstat_pgfault untyped
        node_vmstat_pgfault 718165
        "#.to_string();
        let result = AllNodeExporter::parse_nodeexporter(fake_http_data);
        assert_eq!(&result[0].name, "node_vmstat_pgfault");
        assert_eq!(result[0].value, 718165.0);
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
        let result = AllNodeExporter::parse_nodeexporter(fake_http_data);
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
        let result = AllNodeExporter::parse_nodeexporter(fake_http_data);
        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn integration_parse_node_exporter() {
        let hostname = utility::get_hostname_node_exporter();
        if hostname == *"SKIP" {
            // workaround for allowing integration tests where no node exporter is present.
            return;
        }
        let port = utility::get_port_node_exporter();

        let allnodeexporter = AllNodeExporter::read_nodeexporter(&vec![&hostname], &vec![&port], 1).await;

        assert!(!allnodeexporter.nodeexporter.is_empty());
    }
}