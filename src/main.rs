use structopt::StructOpt;
use std::process;
use regex::Regex;
use chrono::Local;
use std::io::{stdin, Write};
use std::env;
use dotenv::dotenv;
use std::collections::HashMap;
use std::fs;

//mod yb_statsmetrics;
use yb_stats::metrics::{print_metrics_diff_for_snapshots, print_diff_metrics, get_metrics_into_diff_first_snapshot, get_metrics_into_diff_second_snapshot};


// structs from lib
use yb_stats::Snapshot;

// functions from lib
use yb_stats::{perform_snapshot,
               read_snapshots_from_file,
               read_begin_end_snapshot_from_user};

mod memtrackers;
use memtrackers::print_memtrackers_data;
mod loglines;
use loglines::print_loglines;
mod versions;
use versions::print_version_data;
mod threads;
use threads::print_threads_data;
mod gflags;
use gflags::print_gflags_data;
use yb_stats::node_exporter::{get_nodeexporter_into_diff_first_snapshot, get_nodeexpoter_into_diff_second_snapshot, print_diff_nodeexporter, print_nodeexporter_diff_for_snapshots};

mod statements;
use yb_stats::statements::{print_diff_statements, print_statements_diff_for_snapshots, get_statements_into_diff_first_snapshot, get_statements_into_diff_second_snapshot};
//mod node_exporter;
//use node_exporter::{}

const DEFAULT_HOSTNAMES: &str = "192.168.66.80,192.168.66.81,192.168.66.82";
const DEFAULT_PORTS: &str = "7000,9000,12000,13000,9300";
const DEFAULT_PARALLEL: &str = "1";
const WRITE_DOTENV: bool = true;

#[derive(Debug, StructOpt)]
struct Opts {
    /// hostnames (comma separated)
    #[structopt(short, long, default_value = DEFAULT_HOSTNAMES)]
    hosts: String,
    /// port numbers (comma separated)
    #[structopt(short, long, default_value = DEFAULT_PORTS)]
    ports: String,
    /// regex to filter statistic names
    #[structopt(short, long)]
    stat_name_match: Option<String>,
    /// regex to filter table names (requires --details-enable)
    #[structopt(short, long)]
    table_name_match: Option<String>,
    /// regex to select hostnames or ports
    #[structopt(long)]
    hostname_match: Option<String>,
    /// add statistics that are not counters
    #[structopt(short, long)]
    gauges_enable: bool,
    /// report each table and tablet individually
    #[structopt(short, long)]
    details_enable: bool,
    /// perform a CSV (stored) snapshot
    #[structopt(long)]
    snapshot: bool,
    /// comment to be added with the snapshot
    #[structopt(long)]
    snapshot_comment: Option<String>,
    /// this lists the snapshots, and allows you to select a begin and end snapshot for a diff report
    #[structopt(long)]
    snapshot_diff: bool,
    /// print memtrackers data for the given snapshot
    #[structopt(long)]
    print_memtrackers: Option<String>,
    /// print log data for the given snapshot
    #[structopt(long)]
    print_log: Option<String>,
    /// print version data for the given snapshot
    #[structopt(long)]
    print_version: Option<String>,
    /// print threads data for the given snapshot
    #[structopt(long)]
    print_threads: Option<String>,
    /// print gflags for the given snapshot
    #[structopt(long)]
    print_gflags: Option<String>,
    /// log data severity to include: optional: I
    #[structopt(long, default_value = "WEF")]
    log_severity: String,
    /// how much threads to use in parallel for fetching data
    #[structopt(long, default_value = DEFAULT_PARALLEL)]
    parallel: String,
    /// disable gathering of thread stacks from /threadz
    #[structopt(long)]
    disable_threads: bool,
    /// the length of the SQL text display
    #[structopt(long, default_value = "80")]
    sql_length: i32,
}
///// begin snapshot number
//#[structopt(short, long)]
//begin: String,
///// end snapshot number
//#[structopt(short, long)]
//end: String,

fn main() {
    let mut changed_options = HashMap::new();
    dotenv().ok();
    let options = Opts::from_args();

    let hosts_string = if options.hosts == DEFAULT_HOSTNAMES {
        match env::var("YBSTATS_HOSTS") {
            Ok(var) => {
                changed_options.insert("YBSTATS_HOSTS", var.to_owned());
                var
            },
            Err(_e)        => DEFAULT_HOSTNAMES.to_string(),
        }
    } else {
        changed_options.insert("YBSTATS_HOSTS", options.hosts.to_owned());
        options.hosts
    };
    let hosts = hosts_string.split(",").collect();

    let ports_string= if options.ports == DEFAULT_PORTS {
        match env::var("YBSTATS_PORTS") {
            Ok(var) => {
                changed_options.insert("YBSTATS_PORTS", var.to_owned());
                var
            },
            Err(_e)        => DEFAULT_PORTS.to_string(),
        }
    } else {
        changed_options.insert("YBSTATS_PORTS", options.ports.to_owned());
        options.ports
    };
    let ports = ports_string.split(",").collect();

    let parallel_string = if options.parallel == DEFAULT_PARALLEL {
        match env::var("YBSTATS_PARALLEL") {
            Ok(var) => {
                changed_options.insert("YBSTATS_PARALLEL", var.to_owned());
                var
            },
            Err(_e) => DEFAULT_PARALLEL.to_string(),
        }
    } else {
        changed_options.insert("YBSTATS_PARALLEL", options.parallel.to_owned());
        options.parallel
    };
    let parallel: usize = parallel_string.parse().unwrap();

    let snapshot: bool = options.snapshot as bool;
    let gauges_enable: bool = options.gauges_enable as bool;
    let details_enable: bool = options.details_enable as bool;
    let snapshot_diff: bool = options.snapshot_diff as bool;
    let disable_threads: bool = options.disable_threads as bool;
    let sql_length: usize = options.sql_length as usize;
    //let parallel: usize = options.parallel;
    let log_severity: String = options.log_severity;
    let snapshot_comment = match options.snapshot_comment {
        Some(comment) => comment,
        None => String::from("")
    };
    let stat_name_filter = match options.stat_name_match {
        Some(stat_name_match) => Regex::new( &stat_name_match.as_str() ).unwrap(),
        None => Regex::new( ".*" ).unwrap()
    };
    let hostname_filter = match options.hostname_match {
        Some(hostname_match) => Regex::new( &hostname_match.as_str() ).unwrap(),
        None => Regex::new( ".*" ).unwrap()
    };
    let table_name_filter = match options.table_name_match {
        Some(table_name_match) => Regex::new( &table_name_match.as_str() ).unwrap(),
        None => Regex::new( ".*" ).unwrap()
    };

    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    if snapshot {

        let snapshot_number: i32 = perform_snapshot(hosts, ports, snapshot_comment, parallel, disable_threads);
        println!("snapshot number {}", snapshot_number);

    } else if snapshot_diff {

        let current_directory = env::current_dir().unwrap();
        let yb_stats_directory = current_directory.join("yb_stats.snapshots");
        let snapshots: Vec<Snapshot> = read_snapshots_from_file(&yb_stats_directory);

        for row in &snapshots {
            println!("{:>3} {:30} {:50}", row.number, row.timestamp, row.comment);
        }

        let (begin_snapshot, end_snapshot, begin_snapshot_row) = read_begin_end_snapshot_from_user(&snapshots);

        print_metrics_diff_for_snapshots(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp, &hostname_filter, &stat_name_filter, &table_name_filter, &details_enable, &gauges_enable);
        print_statements_diff_for_snapshots(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp, &hostname_filter, sql_length);
        print_nodeexporter_diff_for_snapshots(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp, &hostname_filter, &stat_name_filter, &gauges_enable, &details_enable);

    } else if options.print_memtrackers.is_some() {

        print_memtrackers_data(&options.print_memtrackers.unwrap(), &yb_stats_directory, &hostname_filter, &stat_name_filter);

    } else if options.print_log.is_some() {

        print_loglines(&options.print_log.unwrap(), &yb_stats_directory, &hostname_filter, &log_severity);

    } else if options.print_version.is_some() {

        print_version_data(&options.print_version.unwrap(), &yb_stats_directory, &hostname_filter);

    } else if options.print_threads.is_some() {

        print_threads_data(&options.print_threads.unwrap(), &yb_stats_directory, &hostname_filter);

    } else if options.print_gflags.is_some() {

        print_gflags_data(&options.print_gflags.unwrap(), &yb_stats_directory, &hostname_filter);

    } else {

        let first_snapshot_time = Local::now();
        let (mut values_diff, mut countsum_diff, mut countsumrows_diff) = get_metrics_into_diff_first_snapshot(&hosts, &ports, parallel);
        let mut statements_diff = get_statements_into_diff_first_snapshot(&hosts, &ports, parallel);
        let mut node_exporter_diff = get_nodeexporter_into_diff_first_snapshot(&hosts, &ports, parallel);

        println!("Begin metrics snapshot created, press enter to create end snapshot for difference calculation.");
        let mut input = String::new();
        stdin().read_line(&mut input).ok().expect("failed");

        let second_snapshot_time = Local::now();
        get_metrics_into_diff_second_snapshot(&hosts, &ports, &mut values_diff, &mut countsum_diff, &mut countsumrows_diff, &first_snapshot_time, parallel);
        get_statements_into_diff_second_snapshot(&hosts, &ports, &mut statements_diff, &first_snapshot_time, parallel);
        get_nodeexpoter_into_diff_second_snapshot(&hosts, &ports, &mut node_exporter_diff, &first_snapshot_time, parallel);

        println!("Time between snapshots: {:8.3} seconds", (second_snapshot_time-first_snapshot_time).num_milliseconds() as f64/1000 as f64);
        print_diff_metrics(&values_diff, &countsum_diff, &countsumrows_diff, &hostname_filter, &stat_name_filter, &table_name_filter, &details_enable, &gauges_enable);
        print_diff_statements(&statements_diff, &hostname_filter, sql_length);
        print_diff_nodeexporter(&node_exporter_diff, &hostname_filter, &stat_name_filter, &gauges_enable, &details_enable);

    }

    if changed_options.len() > 0 && WRITE_DOTENV {
            let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(".env")
            .unwrap_or_else(| e | {
                            eprintln!("error writing .env file into current working directory: {}", e);
                            process::exit(1);
                            });
            for (key, value) in changed_options {
                file.write(format!("{}={}\n", key, value).as_bytes()).unwrap();
            }
            file.flush().unwrap();
    }
}
