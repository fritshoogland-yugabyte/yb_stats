//! This is the main crate of the yb_stats executable: a utility to extract all possible data from a YugabyteDB cluster.
//!
#![allow(rustdoc::private_intra_doc_links)]

extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate csv;

use clap::Parser;
use std::{process, env, collections::HashMap, io::stdin, sync::Arc, time::Instant};
use regex::Regex;
use chrono::Local;
use dotenv::dotenv;
use log::*;
use anyhow::Result;
use tokio::{fs, io::AsyncWriteExt, sync::Mutex};
use crate::entities::{AllStoredEntities, SnapshotDiffBTreeMapsEntities};
use crate::masters::{AllStoredMasters, SnapshotDiffBTreeMapsMasters};
use crate::metrics::SnapshotDiffBTreeMapsMetrics;
use crate::node_exporter::SnapshotDiffBTreeMapNodeExporter;
use crate::statements::SnapshotDiffBTreeMapStatements;
use crate::tservers::{AllStoredTabletServers, SnapshotDiffBTreeMapsTabletServers};
use crate::vars::{AllStoredVars, SnapshotDiffBTreeMapsVars};
use crate::versions::{AllStoredVersions, SnapshotDiffBTreeMapsVersions};
use crate::snapshot::read_snapshot;
use crate::threads::AllStoredThreads;
use crate::clocks::AllStoredClocks;
use crate::memtrackers::AllStoredMemTrackers;

mod snapshot;
mod value_statistic_details;
mod countsum_statistic_details;

mod statements;

mod threads;
mod memtrackers;
mod gflags;
mod loglines;
mod versions;
mod node_exporter;
mod entities;
mod masters;
mod rpcs;
mod pprof;
mod mems;
mod metrics;
mod utility;
mod isleader;
mod tservers;
mod vars;
mod clocks;
#[cfg(test)]
mod utility_test;

const DEFAULT_HOSTS: &str = "192.168.66.80,192.168.66.81,192.168.66.82";
const DEFAULT_PORTS: &str = "7000,9000,12000,13000,9300";
const DEFAULT_PARALLEL: &str = "1";
const WRITE_DOTENV: bool = true;

const ACCEPT_INVALID_CERTS: bool = true;

/// yb_stats switches
#[derive(Debug, Parser)]
#[clap(version, about, long_about = None)]
struct Opts {
    /// Snapshot input hostnames (comma separated)
    #[arg(short = 'H', long, value_name = "hostname,hostname")]
    hosts: Option<String>,
    /// Snapshot input port numbers (comma separated)
    #[arg(short, long, value_name = "port,port")]
    ports: Option<String>,
    /// Snapshot capture parallelism (default 1)
    #[arg(long, value_name = "nr")]
    parallel: Option<String>,
    /// Output filter for statistic names as regex
    #[arg(short, long, value_name = "regex")]
    stat_name_match: Option<String>,
    /// Output filter for table names as regex (requires --details-enable)
    #[arg(short, long, value_name = "regex")]
    table_name_match: Option<String>,
    /// Output filter for hostname or ports as regex
    #[arg(long, value_name = "regex")]
    hostname_match: Option<String>,
    /// Output setting to add statistics that are not counters
    #[arg(short, long)]
    gauges_enable: bool,
    /// Output setting to increase detail, such as report each table and tablet individually
    #[arg(short, long)]
    details_enable: bool,
    /// Snapshot setting to be as silent as possible, only errors are printed
    #[arg(long)]
    silent: bool,
    /// Perform a snapshot (creates stored CSV files)
    #[arg(long)]
    snapshot: bool,
    /// Snapshot add comment in snapshot overview
    #[arg(long, value_name = "\"comment\"")]
    snapshot_comment: Option<String>,
    /// Create a performance diff report using a begin and an end snapshot number.
    #[arg(long)]
    snapshot_diff: bool,
    /// Create an entity diff report using a begin and end snapshot number.
    #[arg(long)]
    entity_diff: bool,
    /// Create a masters diff report using a begin and end snapshot number.
    #[arg(long)]
    masters_diff: bool,
    /// Create a versions diff report using a begin and end snapshot number.
    #[arg(long)]
    versions_diff: bool,
    /// Create an adhoc diff report only for metrics
    #[arg(long)]
    adhoc_metrics_diff: bool,
    /// Lists the snapshots in the yb_stats.snapshots in the current directory.
    #[arg(short = 'l', long)]
    snapshot_list: bool,
    /// Output setting to specify the begin snapshot number for diff report.
    #[arg(short = 'b', long, value_name = "snapshot nr")]
    begin: Option<i32>,
    /// Output setting to specify the end snapshot number for diff report.
    #[arg(short = 'e', long, value_name = "snapshot nr")]
    end: Option<i32>,
    /// Print memtrackers data for the given snapshot number
    #[arg(long, value_name = "snapshot number")]
    print_memtrackers: Option<Option<String>>,
    /// Print log data for the given snapshot number
    #[arg(long, value_name = "snapshot number")]
    print_log: Option<String>,
    /// Output log data severity to include: optional: I (use with --print_log)
    #[arg(long, default_value = "WEF")]
    log_severity: String,
    /// Print entity data for snapshot number, or get current.
    #[arg(long, value_name = "snapshot number")]
    print_entities: Option<Option<String>>,
    /// Print master server data for snapshot number, or get current.
    #[arg(long, value_name = "snapshot number")]
    print_masters: Option<Option<String>>,
    /// Print tablet server data for snapshot number, or get current.
    #[arg(long, value_name = "snapshot number")]
    print_tablet_servers: Option<Option<String>>,
    /// Print vars for snapshot number, or get current
    #[arg(long, value_name = "snapshot number")]
    print_vars: Option<Option<String>>,
    /// Print version data for snapshot number, or get current.
    #[arg(long, value_name = "snapshot number")]
    print_version: Option<Option<String>>,
    /// Print rpcs for the given snapshot number
    #[arg(long, value_name = "snapshot number")]
    print_rpcs: Option<String>,
    /// print clocks
    #[arg(long)]
    print_clocks: Option<Option<String>>,
    /// print master leader tablet server latencies
    #[arg(long)]
    print_latencies: bool,
    /// Print threads data for the given snapshot number
    #[arg(long, value_name = "snapshot number")]
    print_threads: Option<Option<String>>,
    /// Print gflags for the given snapshot number
    #[arg(long, value_name = "snapshot number")]
    print_gflags: Option<String>,
    /// Snapshot disable gathering of thread stacks from /threadz
    #[arg(long)]
    disable_threads: bool,
    /// Output setting for the length of the SQL text to display
    #[arg(long, value_name = "nr", default_value = "80")]
    sql_length: usize,
}

/// The entrypoint of the executable.
#[tokio::main]
async fn main() -> Result<()>
{
    env_logger::init();
    let mut changed_options = HashMap::new();
    dotenv().ok();
    let options = Opts::parse();

     // Hosts
     //  - if hosts is set, it's detected by is_some() and we take the set value, and set the changed_options HashMap for later write.
     //  - if hosts is not set, we can detect if it's set via .env by looking at YBSTATS_HOSTS.
     //  - If YBSTATS_HOSTS is set, it's detected by Ok(), we set the changed_options HashMap for later write and return the set value.
     //  - if YBSTATS_HOSTS is not set, it will trigger Err(), and DEFAULT_HOSTS is used.
    let hosts_string = if options.hosts.is_some() {
        info!("hosts argument set: using: {}", &options.hosts.as_ref().unwrap());
        changed_options.insert("YBSTATS_HOSTS", options.hosts.as_ref().unwrap().to_string());
        options.hosts.unwrap()
    } else {
        match env::var("YBSTATS_HOSTS") {
            Ok(set_var) => {
                info!("hosts not set: set via .env: YBSTATS_HOSTS: {}", set_var);
                changed_options.insert("YBSTATS_HOSTS", set_var.to_owned());
                set_var
            }
            Err(_e) => {
                info!("hosts not set: and not set via .env: using DEFAULT_HOSTS: {}", DEFAULT_HOSTS.to_string());
                DEFAULT_HOSTS.to_string()
            }
        }
    };
    let static_hosts: &'static str = Box::leak(hosts_string.into_boxed_str());
    let hosts: Vec<&'static str> = static_hosts.split(',').collect();

    // Ports
    // - if ports is set, it's detected by is_some() and we take the set value, and set the changed_options HashMap for later write.
    // - if ports is not set, then we can detect if it's set via .env by looking at YBSTATS_PORTS.
    //   - If YBSTATS_PORTS is set, it's detected by Ok(), we set the changed_options HashMap for later write and return the set value.
    //   - if YBSTATS_PORTS is not set, it will trigger Err(), and DEFAULT_PORTS is used.
    let ports_string = if options.ports.is_some() {
        info!("ports argument set: using: {}", &options.ports.as_ref().unwrap());
        changed_options.insert("YBSTATS_PORTS", options.ports.as_ref().unwrap().to_string());
        options.ports.unwrap()
    } else {
        match env::var("YBSTATS_PORTS") {
            Ok(set_var) => {
                info!("ports not set: set via .env: YBSTATS_PORTS: {}", set_var);
                changed_options.insert("YBSTATS_PORTS", set_var.to_owned());
                set_var
            }
            Err(_e) => {
                info!("ports not set: and not set via .env: using DEFAULT_PORTS: {}", DEFAULT_PORTS.to_string());
                DEFAULT_PORTS.to_string()
            }
        }
    };
    let static_ports: &'static str = Box::leak(ports_string.into_boxed_str());
    let ports: Vec<&'static str> = static_ports.split(',').collect();

     // Parallel
     // - if parallel is set, it's detected by is_some() and we take the set value, and set the changed_options HashMap for later write.
     // - if parallel is not set, then we can detect if it's set via .env by looking at YBSTATS_PARALLEL.
     //   - If YBSTATS_PARALLEL is set, it's detected by Ok(), we set the changed_options HashMap for later write and return the set value.
     //   - if YBSTATS_PARALLEL is not set, it will trigger Err(), and DEFAULT_PARALLEL is used.
    let parallel_string = if options.parallel.is_some() {
        info!("parallel argument set: using: {}", &options.parallel.as_ref().unwrap());
        changed_options.insert("YBSTATS_PARALLEL", options.parallel.as_ref().unwrap().to_string());
        options.parallel.unwrap()
    } else {
        match env::var("YBSTATS_PARALLEL") {
            Ok(set_var) => {
                info!("parallel not set: set via .env: YBSTATS_PARALLEL: {}", set_var);
                changed_options.insert("YBSTATS_PARALLEL", set_var.to_owned());
                set_var
            }
            Err(_e) => {
                info!("parallel not set: and not set via .env: using DEFAULT_PARALLEL: {}", DEFAULT_PARALLEL.to_string());
                DEFAULT_PARALLEL.to_string()
            }
        }
    };
    let parallel: usize = parallel_string.parse().unwrap();

    let stat_name_filter = match options.stat_name_match {
        Some(stat_name_match) => Regex::new(stat_name_match.as_str()).unwrap(),
        None => Regex::new(".*").unwrap()
    };
    let hostname_filter = match options.hostname_match {
        Some(hostname_match) => Regex::new(hostname_match.as_str()).unwrap(),
        None => Regex::new(".*").unwrap()
    };
    let table_name_filter = match options.table_name_match {
        Some(table_name_match) => Regex::new(table_name_match.as_str()).unwrap(),
        None => Regex::new(".*").unwrap()
    };

    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    if options.snapshot {

        info!("snapshot option");
        let snapshot_number: i32 = perform_snapshot(hosts, ports, options.snapshot_comment, parallel, options.disable_threads).await?;
        if !options.silent {
            println!("snapshot number {}", snapshot_number);
        }

    } else if options.snapshot_diff || options.snapshot_list {

        info!("snapshot_diff");
        if options.begin.is_none() || options.end.is_none() {
            snapshot::Snapshot::print()?;
        }
        if options.snapshot_list { process::exit(0) };

        let (begin_snapshot, end_snapshot, begin_snapshot_row) = snapshot::Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end)?;

        let metrics_diff = metrics::SnapshotDiffBTreeMapsMetrics::snapshot_diff(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp)?;
        metrics_diff.print(&hostname_filter, &stat_name_filter, &table_name_filter, &options.details_enable, &options.gauges_enable).await;
        let statements_diff = statements::SnapshotDiffBTreeMapStatements::snapshot_diff(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp)?;
        statements_diff.print(&hostname_filter, options.sql_length).await;
        let nodeexporter_diff = node_exporter::SnapshotDiffBTreeMapNodeExporter::snapshot_diff(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp)?;
        nodeexporter_diff.print(&hostname_filter, &stat_name_filter, &options.gauges_enable, &options.details_enable);
        let entities_diff = entities::SnapshotDiffBTreeMapsEntities::snapshot_diff(&begin_snapshot, &end_snapshot, &options.details_enable)?;
        entities_diff.print();
        let masters_diff = masters::SnapshotDiffBTreeMapsMasters::snapshot_diff(&begin_snapshot, &end_snapshot)?;
        masters_diff.print();
        let tabletservers_diff = tservers::SnapshotDiffBTreeMapsTabletServers::snapshot_diff(&begin_snapshot, &end_snapshot)?;
        tabletservers_diff.print();
        let vars_diff = vars::SnapshotDiffBTreeMapsVars::snapshot_diff(&begin_snapshot, &end_snapshot)?;
        vars_diff.print();
        let versions_diff = versions::SnapshotDiffBTreeMapsVersions::snapshot_diff(&begin_snapshot, &end_snapshot)?;
        versions_diff.print(&hostname_filter);

    } else if options.entity_diff {

        info!("entity_diff");

        if options.begin.is_none() || options.end.is_none() {
            snapshot::Snapshot::print()?;
        }
        if options.snapshot_list { process::exit(0) };

        let (begin_snapshot, end_snapshot, _begin_snapshot_row) = snapshot::Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end)?;
        let entity_diff = entities::SnapshotDiffBTreeMapsEntities::snapshot_diff(&begin_snapshot, &end_snapshot, &options.details_enable)?;
        entity_diff.print();
    } else if options.masters_diff {

        info!("masters_diff");

        if options.begin.is_none() || options.end.is_none() {
            snapshot::Snapshot::print()?;
        }
        if options.snapshot_list { process::exit(0) };

        let (begin_snapshot, end_snapshot, _begin_snapshot_row) = snapshot::Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end)?;
        let masters_diff = masters::SnapshotDiffBTreeMapsMasters::snapshot_diff(&begin_snapshot, &end_snapshot)?;
        masters_diff.print();

    } else if options.versions_diff {

        info!("versions_diff");

        if options.begin.is_none() || options.end.is_none() {
            snapshot::Snapshot::print()?;
        }
        if options.snapshot_list { process::exit(0) };

        let (begin_snapshot, end_snapshot, _begin_snapshot_row) = snapshot::Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end)?;
        let versions_diff = versions::SnapshotDiffBTreeMapsVersions::snapshot_diff(&begin_snapshot, &end_snapshot)?;
        versions_diff.print(&hostname_filter);

    } else if options.print_memtrackers.is_some() {

        match options.print_memtrackers.unwrap() {

            Some(snapshot_number) => {

              let mut allstoredmemtrackers = AllStoredMemTrackers::new();
              allstoredmemtrackers.stored_memtrackers = read_snapshot(&snapshot_number, "memtrackers")?;

            },
            None => {

               let allstoredmemtrackers = AllStoredMemTrackers::read_memtrackers(&hosts, &ports, parallel).await;
               allstoredmemtrackers.print(&hostname_filter, &stat_name_filter)?;

            },

        }

    } else if options.print_log.is_some() {

        loglines::print_loglines(&options.print_log.unwrap(), &hostname_filter, &options.log_severity)?;

    } else if options.print_version.is_some() {

        match options.print_version.unwrap() {
            Some(snapshot_number) => {

                let mut allstoredversions = AllStoredVersions::new();
                allstoredversions.stored_versions = read_snapshot(&snapshot_number, "versions")?;

                allstoredversions.print(&hostname_filter);
            },
            None => {
                let allstoredversions = AllStoredVersions::read_versions(&hosts, &ports, parallel).await;
                allstoredversions.print(&hostname_filter);
            },
        }

    } else if options.print_threads.is_some() {

        match options.print_threads.unwrap() {
            Some(snapshot_number) => {
                let mut allstoredthreads = AllStoredThreads::new();
                allstoredthreads.stored_threads = read_snapshot(&snapshot_number, "threads")?;
                allstoredthreads.print(&hostname_filter)?;
            },
            None => {
                let allstoredthreads = AllStoredThreads::read_threads(&hosts, &ports, parallel).await;
                allstoredthreads.print(&hostname_filter)?;
            }
        }

    } else if options.print_gflags.is_some() {

        gflags::print_gflags_data(&options.print_gflags.unwrap(), &yb_stats_directory, &hostname_filter, &stat_name_filter);

    } else if options.print_entities.is_some() {

        match options.print_entities.unwrap() {
            Some(snapshot_number) => {

                let mut allstoredentities = AllStoredEntities::new();
                allstoredentities.stored_keyspaces = read_snapshot(&snapshot_number, "keyspaces")?;
                allstoredentities.stored_tables = read_snapshot(&snapshot_number, "tables")?;
                allstoredentities.stored_tablets = read_snapshot(&snapshot_number, "tablets")?;
                allstoredentities.stored_replicas = read_snapshot(&snapshot_number, "replicas")?;

                allstoredentities.print(&snapshot_number, &table_name_filter, &options.details_enable)?;
            },
            None => {
                let allstoredentities = AllStoredEntities::read_entities(&hosts, &ports, parallel).await;
                allstoredentities.print_adhoc(&table_name_filter, &options.details_enable, &hosts, &ports, parallel).await;
            },
        }
    } else if options.print_masters.is_some() {

        match options.print_masters.unwrap() {
            Some(snapshot_number) => {

                let mut allstoredmasters = AllStoredMasters::new();
                allstoredmasters.stored_masters = read_snapshot(&snapshot_number, "masters")?;
                allstoredmasters.stored_rpc_addresses = read_snapshot(&snapshot_number, "master_rpc_addresses")?;
                allstoredmasters.stored_http_addresses = read_snapshot(&snapshot_number, "master_http_addresses")?;
                allstoredmasters.stored_master_error = read_snapshot(&snapshot_number, "master_errors")?;

                allstoredmasters.print(&snapshot_number, &options.details_enable)?;

            }
            None => {
                let allstoredmasters = AllStoredMasters::read_masters(&hosts, &ports, parallel).await;
                allstoredmasters.print_adhoc(&options.details_enable, &hosts, &ports, parallel).await;
            }
        }

    } else if options.print_tablet_servers.is_some() {

        match options.print_tablet_servers.unwrap() {
            Some(snapshot_number) => {

                let mut allstoredtabletservers = AllStoredTabletServers::new();
                allstoredtabletservers.stored_tabletservers = read_snapshot(&snapshot_number, "tablet_servers")?;
                allstoredtabletservers.stored_pathmetrics = read_snapshot(&snapshot_number, "tablet_servers_pathmetrics")?;

                allstoredtabletservers.print(&snapshot_number, &options.details_enable)?;

            }
            None => {
                let allstoredtabletservers = AllStoredTabletServers::read_tabletservers(&hosts, &ports, parallel).await;
                allstoredtabletservers.print_adhoc(&options.details_enable, &hosts, &ports, parallel).await?;
            }
        }

    } else if options.print_vars.is_some() {

        match options.print_vars.unwrap() {
            Some(snapshot_number) => {
                let mut allstoredvars = AllStoredVars::new();
                allstoredvars.stored_vars = read_snapshot(&snapshot_number, "vars")?;

                allstoredvars.print(&options.details_enable, &hostname_filter, &stat_name_filter).await;
            }
            None => {
                let allstoredvars = AllStoredVars::read_vars(&hosts, &ports, parallel).await;
                allstoredvars.print(&options.details_enable, &hostname_filter, &stat_name_filter).await;
            }
        }

    } else if options.print_clocks.is_some() {

        match options.print_clocks.unwrap() {
            Some(snapshot_number) => {
                let mut allstoredclocks = AllStoredClocks::new();
                allstoredclocks.stored_clocks = read_snapshot(&snapshot_number, "clocks")?;

                allstoredclocks.print(&snapshot_number, &options.details_enable)?;
            },
            None => {
                let allstoredclocks = AllStoredClocks::read_clocks(&hosts, &ports, parallel).await?;
                allstoredclocks.print_adhoc(&options.details_enable, &hosts, &ports, parallel).await?;
            },
        }

    } else if options.print_latencies {

        let allstoredclocks = AllStoredClocks::read_clocks(&hosts, &ports, parallel).await?;
        allstoredclocks.print_adhoc_latency(&options.details_enable, &hosts, &ports, parallel).await?;

    } else if options.print_rpcs.is_some() {

        rpcs::print_rpcs(&options.print_rpcs.unwrap(), &hostname_filter, &options.details_enable)?;

    } else if options.adhoc_metrics_diff {

        info!("ad-hoc metrics diff first snapshot begin");
        let timer = Instant::now();

        let first_snapshot_time = Local::now();

        let metrics = Arc::new(Mutex::new(SnapshotDiffBTreeMapsMetrics::new()));
        let statements = Arc::new(Mutex::new(SnapshotDiffBTreeMapStatements::new()));
        let node_exporter = Arc::new(Mutex::new(SnapshotDiffBTreeMapNodeExporter::new()));

        let hosts = Arc::new(Mutex::new(hosts));
        let ports = Arc::new(Mutex::new(ports));
        let mut handles = vec![];

        let clone_metrics = metrics.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_metrics.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        let clone_statements = statements.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_statements.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        let clone_node_exporter = node_exporter.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_node_exporter.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        for handle in handles {
            handle.await.unwrap();
        }
        info!("ad-hoc metrics diff first snapshot end: {:?}", timer.elapsed());

        println!("Begin ad-hoc in-memory snapshot created, press enter to create end snapshot for difference calculation.");
        let mut input = String::new();
        stdin().read_line(&mut input).expect("failed");

        info!("ad-hoc metrics diff second snapshot begin");
        let timer = Instant::now();

        let second_snapshot_time = Local::now();

        let mut handles = vec![];

        let clone_metrics = metrics.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_metrics.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel, &first_snapshot_time).await;
        });
        handles.push(handle);

        let clone_statements = statements.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_statements.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel, &first_snapshot_time).await;
        });
        handles.push(handle);

        let clone_node_exporter = node_exporter.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_node_exporter.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel, &first_snapshot_time).await;
        });
        handles.push(handle);

        for handle in handles {
            handle.await.unwrap();
        }

        info!("ad-hoc metrics diff second snapshot end: {:?}", timer.elapsed());

        println!("Time between snapshots: {:8.3} seconds", (second_snapshot_time - first_snapshot_time).num_milliseconds() as f64 / 1000_f64);
        metrics.lock().await.print(&hostname_filter, &stat_name_filter, &table_name_filter, &options.details_enable, &options.gauges_enable).await;
        statements.lock().await.print(&hostname_filter, options.sql_length).await;
        node_exporter.lock().await.print(&hostname_filter, &stat_name_filter, &options.gauges_enable, &options.details_enable);

    } else {

        info!("ad-hoc mode first snapshot begin");

        let timer = Instant::now();

        let first_snapshot_time = Local::now();

        let metrics = Arc::new(Mutex::new(SnapshotDiffBTreeMapsMetrics::new()));
        let statements = Arc::new(Mutex::new(SnapshotDiffBTreeMapStatements::new()));
        let node_exporter = Arc::new(Mutex::new(SnapshotDiffBTreeMapNodeExporter::new()));
        let entities = Arc::new(Mutex::new(SnapshotDiffBTreeMapsEntities::new()));
        let masters = Arc::new(Mutex::new(SnapshotDiffBTreeMapsMasters::new()));
        let tablet_servers = Arc::new(Mutex::new(SnapshotDiffBTreeMapsTabletServers::new()));
        let versions = Arc::new(Mutex::new(SnapshotDiffBTreeMapsVersions::new()));
        let vars = Arc::new(Mutex::new(SnapshotDiffBTreeMapsVars::new()));

        let hosts = Arc::new(Mutex::new(hosts));
        let ports = Arc::new(Mutex::new(ports));
        let mut handles = vec![];

        let clone_metrics = metrics.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_metrics.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        let clone_statements = statements.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_statements.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        let clone_node_exporter = node_exporter.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_node_exporter.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        let clone_entities = entities.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_entities.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        let clone_masters = masters.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_masters.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        let clone_tablet_servers = tablet_servers.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_tablet_servers.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        let clone_vars = vars.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_vars.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        let clone_versions = versions.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_versions.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        for handle in handles {
            handle.await.unwrap();
        }
        info!("ad-hoc metrics diff first snapshot end: {:?}", timer.elapsed());

        println!("Begin ad-hoc in-memory snapshot created, press enter to create end snapshot for difference calculation.");
       let mut input = String::new();
        stdin().read_line(&mut input).expect("failed");

        info!("ad-hoc metrics diff second snapshot begin");
        let timer = Instant::now();

        let second_snapshot_time = Local::now();
        let mut handles = vec![];

        let clone_metrics = metrics.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_metrics.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel, &first_snapshot_time).await;
        });
        handles.push(handle);

        let clone_statements = statements.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_statements.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel, &first_snapshot_time).await;
        });
        handles.push(handle);

        let clone_node_exporter = node_exporter.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_node_exporter.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel, &first_snapshot_time).await;
        });
        handles.push(handle);

        let clone_entities = entities.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_entities.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        let clone_masters = masters.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_masters.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        let clone_tablet_servers = tablet_servers.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_tablet_servers.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        let clone_vars = vars.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_vars.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        let clone_versions = versions.clone();
        let clone_hosts = hosts.clone();
        let clone_ports = ports.clone();
        let handle = tokio::spawn(async move {
            clone_versions.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
        });
        handles.push(handle);

        for handle in handles {
            handle.await.unwrap();
        }
        info!("ad-hoc metrics diff second snapshot end: {:?}", timer.elapsed());

        println!("Time between snapshots: {:8.3} seconds", (second_snapshot_time - first_snapshot_time).num_milliseconds() as f64 / 1000_f64);
        metrics.lock().await.print(&hostname_filter, &stat_name_filter, &table_name_filter, &options.details_enable, &options.gauges_enable).await;
        statements.lock().await.print(&hostname_filter, options.sql_length).await;
        node_exporter.lock().await.print(&hostname_filter, &stat_name_filter, &options.gauges_enable, &options.details_enable);
        entities.lock().await.print();
        masters.lock().await.print();
        tablet_servers.lock().await.print();
        vars.lock().await.print();
        versions.lock().await.print(&hostname_filter);

    }

    if !changed_options.is_empty() && WRITE_DOTENV {
        info!("Writing .env file");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(".env")
            .await
            .unwrap_or_else(|e| {
                error!("error writing .env file into current working directory: {}", e);
                process::exit(1);
            });
        for (key, value) in changed_options {
            //file.write_all(format!("{}={}\n", key, value).as_bytes()).unwrap();
            file.write_all(format!("{}={}\n", key, value).as_bytes()).await.unwrap();
            info!("{}={}", key, value);
        }
        file.flush().await.unwrap();
    }
    Ok(())
}

/// The function to perform a snapshot resulting in CSV files.
async fn perform_snapshot(
    hosts: Vec<&'static str>,
    ports: Vec<&'static str>,
    snapshot_comment: Option<String>,
    parallel: usize,
    disable_threads: bool,
) -> Result<i32> {
    info!("begin snapshot");
    let timer = Instant::now();

    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    let snapshot_number = snapshot::Snapshot::insert_new_snapshot_number(snapshot_comment)?;
    info!("using snapshot number: {}", snapshot_number);

    let arc_hosts = Arc::new(hosts);
    let arc_ports = Arc::new(ports);
    let arc_yb_stats_directory = Arc::new(yb_stats_directory);

    let mut handles = vec![];

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        metrics::AllStoredMetrics::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        statements::AllStoredStatements::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        node_exporter::AllStoredNodeExporterValues::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        isleader::AllStoredIsLeader::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        entities::AllStoredEntities::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        masters::AllStoredMasters::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        tservers::AllStoredTabletServers::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        vars::AllStoredVars::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        versions::AllStoredVersions::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
    let handle = tokio::spawn(async move {
        gflags::perform_gflags_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel).await;
    });
    handles.push(handle);

    if !disable_threads {
        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let handle = tokio::spawn(async move {
            AllStoredThreads::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
        });
        handles.push(handle);
    };

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        AllStoredMemTrackers::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        loglines::perform_loglines_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        rpcs::perform_rpcs_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
    let handle = tokio::spawn(async move {
        pprof::perform_pprof_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
    let handle = tokio::spawn(async move {
        mems::perform_mems_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        AllStoredClocks::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    for handle in handles {
        handle.await.unwrap();
    }

    info!("end snapshot: {:?}", timer.elapsed());
    Ok(snapshot_number)
}