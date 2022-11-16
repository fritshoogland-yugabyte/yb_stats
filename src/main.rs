//! This is the main crate of the yb_stats executable: a utility to extract all possible data from a YugabyteDB cluster.
//!
#![allow(rustdoc::private_intra_doc_links)]

extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate csv;

use structopt::StructOpt;
use std::{process, env, fs, collections::HashMap, io::{stdin, Write}, sync::Arc, time::Instant};
use regex::Regex;
use chrono::Local;
use dotenv::dotenv;
use log::*;
use crate::entities::AllStoredEntities;

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

const DEFAULT_HOSTS: &str = "192.168.66.80,192.168.66.81,192.168.66.82";
const DEFAULT_PORTS: &str = "7000,9000,12000,13000,9300";
const DEFAULT_PARALLEL: &str = "1";
const WRITE_DOTENV: bool = true;

/// Struct that holds the commandline options.
#[derive(Debug, StructOpt)]
struct Opts {
    /// Snapshot input hostnames (comma separated)
    #[structopt(short, long, value_name = "hostname,hostname")]
    hosts: Option<String>,
    /// Snapshot input port numbers (comma separated)
    #[structopt(short, long, value_name = "port,port")]
    ports: Option<String>,
    /// Output filter for statistic names as regex
    #[structopt(short, long, value_name = "regex")]
    stat_name_match: Option<String>,
    /// Output filter for table names as regex (requires --details-enable)
    #[structopt(short, long, value_name = "regex")]
    table_name_match: Option<String>,
    /// Output filter for hostname or ports as regex
    #[structopt(long, value_name = "regex")]
    hostname_match: Option<String>,
    /// Output setting to add statistics that are not counters
    #[structopt(short, long)]
    gauges_enable: bool,
    /// Output setting to increase detail, such as report each table and tablet individually
    #[structopt(short, long)]
    details_enable: bool,
    /// Snapshot setting to be as silent as possible, only errors are printed
    #[structopt(long)]
    silent: bool,
    /// Perform a snapshot (creates stored CSV files)
    #[structopt(long)]
    snapshot: bool,
    /// Snapshot add comment in snapshot overview
    #[structopt(long, value_name = "\"comment\"")]
    snapshot_comment: Option<String>,
    /// Create a performance diff report using a begin and an end snapshot number.
    #[structopt(long)]
    snapshot_diff: bool,
    /// Create an entity diff report using a begin and end snapshot number.
    #[structopt(long)]
    entity_diff: bool,
    /// Lists the snapshots in the yb_stats.snapshots in the current directory.
    #[structopt(short = "l", long)]
    snapshot_list: bool,
    /// Output setting to specify the begin snapshot number for diff report.
    #[structopt(short = "b", long)]
    begin: Option<i32>,
    /// Output setting to specify the end snapshot number for diff report.
    #[structopt(short = "e", long)]
    end: Option<i32>,
    /// Print memtrackers data for the given snapshot number
    #[structopt(long, value_name = "snapshot number")]
    print_memtrackers: Option<String>,
    /// Print log data for the given snapshot number
    #[structopt(long, value_name = "snapshot number")]
    print_log: Option<String>,
    /// Print entity data for the given snapshot number
    #[structopt(long, value_name = "snapshot number")]
    print_entities: Option<String>,
    /// Print master info for the given snapshot number
    #[structopt(long, value_name = "snapshot number")]
    print_masters: Option<String>,
    /// Print version data for the given snapshot number
    #[structopt(long, value_name = "snapshot number")]
    print_version: Option<String>,
    /// Print rpcs for the given snapshot number
    #[structopt(long, value_name = "snapshot number")]
    print_rpcs: Option<String>,
    /// Print threads data for the given snapshot number
    #[structopt(long, value_name = "snapshot number")]
    print_threads: Option<String>,
    /// Print gflags for the given snapshot number
    #[structopt(long, value_name = "snapshot number")]
    print_gflags: Option<String>,
    /// Output log data severity to include: optional: I (use with --print_log)
    #[structopt(long, default_value = "WEF")]
    log_severity: String,
    /// Snapshot capture parallelism (default 1)
    #[structopt(long, value_name = "nr")]
    parallel: Option<String>,
    /// Snapshot disable gathering of thread stacks from /threadz
    #[structopt(long)]
    disable_threads: bool,
    /// Output setting for the length of the SQL text to display
    #[structopt(long, value_name = "nr", default_value = "80")]
    sql_length: usize,
}

/// The entrypoint of the executable.
fn main() {
    env_logger::init();
    let mut changed_options = HashMap::new();
    dotenv().ok();
    let options = Opts::from_args();

    /*
     * Hosts
     * - if hosts is set, it's detected by is_some() and we take the set value, and set the changed_options HashMap for later write.
     * - if hosts is not set, we can detect if it's set via .env by looking at YBSTATS_HOSTS.
     *   - If YBSTATS_HOSTS is set, it's detected by Ok(), we set the changed_options HashMap for later write and return the set value.
     *   - if YBSTATS_HOSTS is not set, it will trigger Err(), and DEFAULT_HOSTS is used.
     */
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
            },
            Err(_e) => {
                info!("hosts not set: and not set via .env: using DEFAULT_HOSTS: {}", DEFAULT_HOSTS.to_string());
                DEFAULT_HOSTS.to_string()
            },
        }
    };
    let static_hosts: &'static str = Box::leak(hosts_string.into_boxed_str());
    let hosts: Vec<&'static str> = static_hosts.split(',').collect();

    /*
     * Ports
     * - if ports is set, it's detected by is_some() and we take the set value, and set the changed_options HashMap for later write.
     * - if ports is not set, then we can detect if it's set via .env by looking at YBSTATS_PORTS.
     *   - If YBSTATS_PORTS is set, it's detected by Ok(), we set the changed_options HashMap for later write and return the set value.
     *   - if YBSTATS_PORTS is not set, it will trigger Err(), and DEFAULT_PORTS is used.
     */
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
            },
            Err(_e) => {
                info!("ports not set: and not set via .env: using DEFAULT_PORTS: {}", DEFAULT_PORTS.to_string());
                DEFAULT_PORTS.to_string()
            },
        }
    };
    let static_ports: &'static str = Box::leak(ports_string.into_boxed_str());
    let ports: Vec<&'static str> = static_ports.split(',').collect();

    /*
     * Parallel
     * - if parallel is set, it's detected by is_some() and we take the set value, and set the changed_options HashMap for later write.
     * - if parallel is not set, then we can detect if it's set via .env by looking at YBSTATS_PARALLEL.
     *   - If YBSTATS_PARALLEL is set, it's detected by Ok(), we set the changed_options HashMap for later write and return the set value.
     *   - if YBSTATS_PARALLEL is not set, it will trigger Err(), and DEFAULT_PARALLEL is used.
     */
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
            },
            Err(_e) => {
                info!("parallel not set: and not set via .env: using DEFAULT_PARALLEL: {}", DEFAULT_PARALLEL.to_string());
                DEFAULT_PARALLEL.to_string()
            },
        }
    };
    let parallel: usize = parallel_string.parse().unwrap();

    let stat_name_filter = match options.stat_name_match {
        Some(stat_name_match) => Regex::new( stat_name_match.as_str() ).unwrap(),
        None => Regex::new( ".*" ).unwrap()
    };
    let hostname_filter = match options.hostname_match {
        Some(hostname_match) => Regex::new( hostname_match.as_str() ).unwrap(),
        None => Regex::new( ".*" ).unwrap()
    };
    let table_name_filter = match options.table_name_match {
        Some(table_name_match) => Regex::new( table_name_match.as_str() ).unwrap(),
        None => Regex::new( ".*" ).unwrap()
    };

    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    if options.snapshot {

        info!("snapshot option");
        let snapshot_number: i32 = perform_snapshot(hosts, ports, options.snapshot_comment, parallel, options.disable_threads);
        if ! options.silent {
            println!("snapshot number {}", snapshot_number);
        }

    } else if options.snapshot_diff || options.snapshot_list {
        info!("snapshot_diff");
        if options.begin.is_none() || options.end.is_none() {
            snapshot::Snapshot::print();
        }
        if options.snapshot_list { process::exit(0) };

        let (begin_snapshot, end_snapshot, begin_snapshot_row) = snapshot::Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end);

        let metrics_diff = metrics::SnapshotDiffBTreeMapsMetrics::snapshot_diff(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp);
        metrics_diff.print(&hostname_filter, &stat_name_filter, &table_name_filter, &options.details_enable, &options.gauges_enable);
        let statements_diff = statements::SnapshotDiffBTreeMapStatements::snapshot_diff(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp);
        statements_diff.print(&hostname_filter, options.sql_length);
        let nodeexporter_diff = node_exporter::SnapshotDiffBTreeMapNodeExporter::snapshot_diff(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp);
        nodeexporter_diff.print(&hostname_filter, &stat_name_filter, &options.gauges_enable, &options.details_enable);

    } else if options.entity_diff {
        info!("entity_diff");

        if options.begin.is_none() || options.end.is_none() {
            snapshot::Snapshot::print();
        }
        if options.snapshot_list { process::exit(0) };

        let (begin_snapshot, end_snapshot, _begin_snapshot_row) = snapshot::Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end);
        let entity_diff = entities::SnapshotDiffBTreeMapsEntities::snapshot_diff(&begin_snapshot, &end_snapshot, &options.details_enable);
        entity_diff.print();

    } else if options.print_memtrackers.is_some() {

        memtrackers::print_memtrackers_data(&options.print_memtrackers.unwrap(), &yb_stats_directory, &hostname_filter, &stat_name_filter);

    } else if options.print_log.is_some() {

        loglines::print_loglines(&options.print_log.unwrap(), &yb_stats_directory, &hostname_filter, &options.log_severity);

    } else if options.print_version.is_some() {

        versions::print_version_data(&options.print_version.unwrap(), &yb_stats_directory, &hostname_filter);

    } else if options.print_threads.is_some() {

        threads::print_threads_data(&options.print_threads.unwrap(), &yb_stats_directory, &hostname_filter);

    } else if options.print_gflags.is_some() {

        gflags::print_gflags_data(&options.print_gflags.unwrap(), &yb_stats_directory, &hostname_filter, &stat_name_filter);

    } else if options.print_entities.is_some() {

        let snapshot_number = &options.print_entities.unwrap();

        //let entities = AllStoredEntities::read_snapshot(&options.print_entities.unwrap())
        let entities = AllStoredEntities::read_snapshot(snapshot_number)
            .unwrap_or_else(|e| {
                error!("Error loading snapshot: {}", e);
                process::exit(1);
            });

        entities.print(snapshot_number, &table_name_filter, &options.details_enable);

    } else if options.print_masters.is_some() {

        masters::print_masters(&options.print_masters.unwrap(), &yb_stats_directory, &hostname_filter);

    } else if options.print_rpcs.is_some() {

        rpcs::print_rpcs(&options.print_rpcs.unwrap(), &yb_stats_directory, &hostname_filter, &options.details_enable);

    } else {

        info!("ad-hoc mode");
        let first_snapshot_time = Local::now();
        let mut metrics_diff = metrics::SnapshotDiffBTreeMapsMetrics::adhoc_read_first_snapshot(&hosts, &ports, parallel);
        let mut statements_diff = statements::SnapshotDiffBTreeMapStatements::adhoc_read_first_snapshot(&hosts, &ports, parallel);
        let mut node_exporter_diff = node_exporter::SnapshotDiffBTreeMapNodeExporter::adhoc_read_first_snapshot(&hosts, &ports, parallel);

        println!("Begin metrics snapshot created, press enter to create end snapshot for difference calculation.");
        let mut input = String::new();
        stdin().read_line(&mut input).expect("failed");

        let second_snapshot_time = Local::now();
        metrics_diff.adhoc_read_second_snapshot(&hosts, &ports, parallel, &first_snapshot_time);
        statements_diff.adhoc_read_second_snapshot(&hosts, &ports, parallel, &first_snapshot_time);
        node_exporter_diff.adhoc_read_second_snapshot(&hosts, &ports, parallel, &first_snapshot_time);

        println!("Time between snapshots: {:8.3} seconds", (second_snapshot_time-first_snapshot_time).num_milliseconds() as f64/1000_f64);
        metrics_diff.print(&hostname_filter, &stat_name_filter, &table_name_filter, &options.details_enable, &options.gauges_enable);
        statements_diff.print(&hostname_filter, options.sql_length);
        node_exporter_diff.print(&hostname_filter, &stat_name_filter, &options.gauges_enable, &options.details_enable);

    }

    if !changed_options.is_empty() && WRITE_DOTENV {
            info!("Writing .env file");
            let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(".env")
            .unwrap_or_else(| e | {
                            error!("error writing .env file into current working directory: {}", e);
                            process::exit(1);
                            });
            for (key, value) in changed_options {
                file.write_all(format!("{}={}\n", key, value).as_bytes()).unwrap();
                info!("{}={}", key, value);
            }
            file.flush().unwrap();
    }
}

/// The function to perform a snapshot resulting in CSV files.
fn perform_snapshot(
    hosts: Vec<&'static str>,
    ports: Vec<&'static str>,
    snapshot_comment: Option<String>,
    parallel: usize,
    disable_threads: bool,
) -> i32 {
    info!("begin snapshot");
    let timer = Instant::now();

    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    let snapshot_number= snapshot::Snapshot::insert_new_snapshot_number(snapshot_comment);
    info!("using snapshot number: {}", snapshot_number);

    /*
     * Snapshot creation is done using a threadpool using rayon.
     * Every different snapshot type is executing in its own thread.
     * The maximum number of threads for each snapshot type is set by the user using the --parallel switch or via .env.
     * The default value is 1 to be conservative.
     *
     * Inside the thread for the specific snapshot type, that thread also uses --parallel.
     * The reason being that most of the work is sending and waiting for a remote server to respond.
     * This might not be really intuitive, but it should not overallocate CPU very much.
     *
     * If it does: set parallel to 1 again using --parallel 1, which will also set it in .env.
     *
     *
     * Using a threadpool
     */
    let main_pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
    main_pool.scope(|mps| {
        let arc_hosts = Arc::new(hosts);
        let arc_ports = Arc::new(ports);
        let arc_yb_stats_directory = Arc::new(yb_stats_directory);

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        mps.spawn(move |_| {
            metrics::AllStoredMetrics::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel);
        });

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        mps.spawn(move |_| {
            statements::AllStoredStatements::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel);
        });

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        mps.spawn(move |_| {
            node_exporter::AllStoredNodeExporterValues::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel);
        });

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        mps.spawn(move |_| {
            isleader::AllStoredIsLeader::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel);
        });

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        mps.spawn(move |_| {
            entities::AllStoredEntities::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel);
        });

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
        mps.spawn(move |_| {
            gflags::perform_gflags_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);
        });

        if !disable_threads {
            let arc_hosts_clone = arc_hosts.clone();
            let arc_ports_clone = arc_ports.clone();
            let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
            mps.spawn(move |_| {
                threads::perform_threads_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel)
            });
        };

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
        mps.spawn(move |_| {
            memtrackers::perform_memtrackers_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);
        });

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
        mps.spawn(move |_| {
            loglines::perform_loglines_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);
        });

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
        mps.spawn(move |_| {
            versions::perform_versions_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);
        });

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
        mps.spawn(move |_| {
            masters::perform_masters_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);
        });

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
        mps.spawn(move |_| {
            rpcs::perform_rpcs_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);
        });

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
        mps.spawn(move |_| {
            pprof::perform_pprof_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);
        });

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory;
        mps.spawn(move |_| {
            mems::perform_mems_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);
        });
    });

    /*
     * No threadpool
     * The rpcs function didn't seem to work reliably with threads, but it turned out to be a JSON parsing problem.
     * probably this should be removed in the future.
     *
        let arc_hosts = Arc::new(hosts);
        let arc_ports = Arc::new(ports);
        let arc_yb_stats_directory = Arc::new(yb_stats_directory);

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
            metrics::perform_metrics_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
            gflags::perform_gflags_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);

        if !disable_threads {
            let arc_hosts_clone = arc_hosts.clone();
            let arc_ports_clone = arc_ports.clone();
            let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
                threads::perform_threads_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel)
        };

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
            memtrackers::perform_memtrackers_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
            loglines::perform_loglines_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
            versions::perform_versions_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
            statements::perform_statements_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
            node_exporter::perform_nodeexporter_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
            entities::perform_entities_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
            masters::perform_masters_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
            rpcs::perform_rpcs_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory;
            pprof::perform_pprof_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);
     */

    info!("end snapshot: {:?}", timer.elapsed());
    snapshot_number
}