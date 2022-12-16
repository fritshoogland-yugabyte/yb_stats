//! yb_stats: a utility to extract all possible data from a YugabyteDB cluster.
//!
//! This utility can:
//! - Read YugabyteDB http endpoints and write the distinct groups of data into CSV files (`--snapshot`)
//! - Read YugabyteDB http endpoints and report results back directory (`--print-*` without a snapshot number),
//!   and the adhoc snapshot mode.
//! - Read yb_stats snapshots (CSV), and report the difference (`--*-diff`).
//! - Read yb_stats snapshots (CSV), and report the snapshot data (`--print-* <NR>`).
//!
#![allow(rustdoc::private_intra_doc_links)]
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate csv;

use clap::Parser;
use std::{env, collections::HashMap};
use dotenv::dotenv;
use anyhow::Result;

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

// constants
const DEFAULT_HOSTS: &str = "192.168.66.80,192.168.66.81,192.168.66.82";
const DEFAULT_PORTS: &str = "7000,9000,12000,13000,9300";
const DEFAULT_PARALLEL: &str = "1";
const WRITE_DOTENV: bool = true;
const ACCEPT_INVALID_CERTS: bool = true;

/// yb_stats switches
#[derive(Debug, Parser)]
#[clap(version, about, long_about = None)]
pub struct Opts {
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
    print_latencies: Option<Option<String>>,
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

    let hosts = utility::set_hosts(&options.hosts, &mut changed_options);
    let ports = utility::set_ports(&options.ports, &mut changed_options);
    let parallel = utility::set_parallel(&options.parallel, &mut changed_options);
    let stat_name_filter = utility::set_regex(&options.stat_name_match);
    let hostname_filter = utility::set_regex(&options.hostname_match);
    let table_name_filter = utility::set_regex(&options.table_name_match);

    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    // The actual functions that perform the yb_stats functionality
    if      options.snapshot                               { utility::perform_snapshot(hosts, ports, options.snapshot_comment, parallel, options.disable_threads, options.silent).await?; }
    else if options.snapshot_diff || options.snapshot_list { utility::snapshot_diff(&options, &hostname_filter, &stat_name_filter, &table_name_filter).await?; }
    else if options.entity_diff                            { utility::entity_diff(&options).await?; }
    else if options.masters_diff                           { utility::masters_diff(&options).await?; }
    else if options.versions_diff                          { utility::versions_diff(&options, &hostname_filter).await?; }
    else if options.print_memtrackers.is_some()            { utility::print_memtrackers(hosts, ports, parallel, &options, &hostname_filter, &stat_name_filter).await?; }
    else if options.print_log.is_some()                    { loglines::print_loglines(&options.print_log.unwrap(), &hostname_filter, &options.log_severity)?; }
    else if options.print_version.is_some()                { utility::print_version(hosts, ports, parallel, &options, &hostname_filter).await?; }
    else if options.print_threads.is_some()                { utility::print_threads(hosts, ports, parallel, &options, &hostname_filter).await?; }
    else if options.print_gflags.is_some()                 { gflags::print_gflags_data(&options.print_gflags.unwrap(), &yb_stats_directory, &hostname_filter, &stat_name_filter); }
    else if options.print_entities.is_some()               { utility::print_entities(hosts, ports, parallel, &options, &table_name_filter).await?; }
    else if options.print_masters.is_some()                { utility::print_masters(hosts, ports, parallel, &options).await?; }
    else if options.print_tablet_servers.is_some()         { utility::print_tablet_servers(hosts, ports, parallel, &options).await?; }
    else if options.print_vars.is_some()                   { utility::print_vars(hosts, ports, parallel, &options, &hostname_filter, &stat_name_filter).await?; }
    else if options.print_clocks.is_some()                 { utility::print_clocks(hosts, ports, parallel, &options).await?; }
    else if options.print_latencies.is_some()              { utility::print_latencies(hosts, ports, parallel, &options).await?; }
    else if options.print_rpcs.is_some()                   { rpcs::print_rpcs(&options.print_rpcs.unwrap(), &hostname_filter, &options.details_enable)?; }
    else if options.adhoc_metrics_diff                     { utility::adhoc_metrics_diff(hosts, ports, parallel, &options, &hostname_filter, &stat_name_filter, &table_name_filter).await?; }
    else                                                   { utility::adhoc_diff(hosts, ports, parallel, &options, &hostname_filter, &stat_name_filter, &table_name_filter).await?; }

    // if we are allowed to write, and changed_options does contain values, write them to '.env'
    utility::dotenv_writer(WRITE_DOTENV, changed_options)?;

    Ok(())
}