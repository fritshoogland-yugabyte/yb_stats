//! yb_stats: a utility to extract all possible data from a YugabyteDB cluster.
//!
//! This utility can:
//! - Read YugabyteDB http endpoints and write the distinct groups of data into CSV files (`--snapshot`)
//! - Read YugabyteDB http endpoints and report results back directory (`--print-*` without a snapshot number),
//!   and the adhoc snapshot mode.
//! - Read yb_stats snapshots (CSV), and report the difference (`--*-diff`).
//! - Read yb_stats snapshots (CSV), and report the snapshot data (`--print-* <NR>`).
//!
//! This main file contains the [Opts] struct for commandline options via clap.
//! It then calls the tasks using the opts structure.
//!
#![allow(rustdoc::private_intra_doc_links)]
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate csv;

use clap::Parser;
use std::collections::HashMap;
use dotenv::dotenv;
use anyhow::Result;

mod snapshot;
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
mod cluster_config;

// constants
const DEFAULT_HOSTS: &str = "192.168.66.80,192.168.66.81,192.168.66.82";
const DEFAULT_PORTS: &str = "7000,9000,12000,13000,9300";
const DEFAULT_PARALLEL: &str = "1";
/// Write the `.env` in the current working directory?
const WRITE_DOTENV: bool = true;
/// Accept certificates not signed by an official CA?
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
    /// tail log data
    #[arg(long)]
    tail_log: bool,
    /// Print log data for the given snapshot number
    #[arg(long, value_name = "snapshot number")]
    print_log: Option<Option<String>>,
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
    /// Print rpcs for the given snapshot number, or get current.
    #[arg(long, value_name = "snapshot number")]
    print_rpcs: Option<Option<String>>,
    /// print clocks for the given snapshot number, or get current.
    #[arg(long)]
    print_clocks: Option<Option<String>>,
    /// print master leader tablet server latencies
    #[arg(long)]
    print_latencies: Option<Option<String>>,
    /// Print threads data for the given snapshot number, or get current.
    #[arg(long, value_name = "snapshot number")]
    print_threads: Option<Option<String>>,
    /// Print gflags for the given snapshot number, or get current.
    #[arg(long, value_name = "snapshot number")]
    print_gflags: Option<Option<String>>,
    /// Print cluster-config for the given snapshot number, or get current.
    #[arg(long, value_name = "snapshot number")]
    print_cluster_config: Option<Option<String>>,
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

    match &options {
        Opts { snapshot, ..               } if *snapshot                       => snapshot::perform_snapshot(hosts, ports, parallel, &options).await?,
        Opts { snapshot_diff, ..          } if *snapshot_diff                  => snapshot::snapshot_diff(&options).await?,
        Opts { snapshot_list, ..          } if *snapshot_list                  => snapshot::snapshot_diff(&options).await?,
        Opts { entity_diff, ..            } if *entity_diff                    => entities::entity_diff(&options).await?,
        Opts { masters_diff, ..           } if *masters_diff                   => masters::masters_diff(&options).await?,
        Opts { versions_diff, ..          } if *versions_diff                  => versions::versions_diff(&options).await?,
        Opts { print_memtrackers, ..      } if print_memtrackers.is_some()     => memtrackers::print_memtrackers(hosts, ports, parallel, &options).await?,
        Opts { print_version, ..          } if print_version.is_some()         => versions::print_version(hosts, ports, parallel, &options).await?,
        Opts { print_threads, ..          } if print_threads.is_some()         => threads::print_threads(hosts, ports, parallel, &options).await?,
        Opts { print_entities, ..         } if print_entities.is_some()        => entities::print_entities(hosts, ports, parallel, &options).await?,
        Opts { print_masters, ..          } if print_masters.is_some()         => masters::print_masters(hosts, ports, parallel, &options).await?,
        Opts { print_tablet_servers, ..   } if print_tablet_servers.is_some()  => tservers::print_tablet_servers(hosts, ports, parallel, &options).await?,
        Opts { print_vars, ..             } if print_vars.is_some()            => vars::print_vars(hosts, ports, parallel, &options).await?,
        Opts { print_clocks, ..           } if print_clocks.is_some()          => clocks::print_clocks(hosts, ports, parallel, &options).await?,
        Opts { print_latencies, ..        } if print_latencies.is_some()       => clocks::print_latencies(hosts, ports, parallel, &options).await?,
        Opts { print_rpcs, ..             } if print_rpcs.is_some()            => rpcs::print_rpcs(hosts, ports, parallel, &options).await?,
        Opts { print_log, ..              } if print_log.is_some()             => loglines::print_loglines(hosts, ports, parallel, &options).await?,
        Opts { tail_log, ..               } if *tail_log                       => loglines::tail_loglines(hosts, ports, parallel, &options).await?,
        Opts { adhoc_metrics_diff, ..     } if *adhoc_metrics_diff             => snapshot::adhoc_metrics_diff(hosts, ports, parallel, &options).await?,
        Opts { print_gflags, ..           } if print_gflags.is_some()          => gflags::print_gflags(hosts, ports, parallel, &options).await?,
        Opts { print_cluster_config, ..           } if print_cluster_config.is_some()          => cluster_config::print_cluster_config(hosts, ports, parallel, &options).await?,
        _                                                                      => snapshot::adhoc_diff(hosts, ports, parallel, &options).await?,
    };
    // if we are allowed to write, and changed_options does contain values, write them to '.env'
    utility::dotenv_writer(WRITE_DOTENV, changed_options)?;

    Ok(())
}
