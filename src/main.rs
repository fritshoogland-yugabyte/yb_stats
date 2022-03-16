use structopt::StructOpt;
use std::process;
use regex::Regex;
use chrono::Local;
use std::io::stdin;
use std::env;

//mod yb_statsmetrics};
use yb_stats::metrics::{
    StoredValues,
    StoredCountSum,
    StoredCountSumRows,
    read_metrics,
    add_to_metric_vectors,
    build_metrics_btreemaps
};


// structs from lib
use yb_stats::{
    Snapshot,
};

// functions from lib
use yb_stats::{perform_snapshot,
               print_diff,
               print_diff_statements,
               read_snapshots_from_file,
               read_begin_end_snapshot_from_user,
               read_values_snapshot,
               read_countsum_snapshot,
               read_countsumrows_snapshot,
               read_statements_snapshot,
               insert_first_snapshot_metrics,
               insert_first_snapshot_statements,
               insert_second_snapshot_metrics,
               insert_second_snapshot_statements};

mod memtrackers;
use memtrackers::print_memtrackers_data;
mod loglines;
use loglines::print_loglines;
mod versions;
use versions::print_version_data;
use yb_stats::statements::{StoredStatements, read_statements, add_to_statements_vector};
mod threads;
use threads::print_threads_data;

#[derive(Debug, StructOpt)]
struct Opts {
    /// all metric endpoints to be used, a metric endpoint is a hostname or ip address with colon and port number, comma separated.
    #[structopt(short, long, default_value = "192.168.66.80:7000,192.168.66.81:7000,192.168.66.82:7000")]
    metric_sources: String,
    /// regex to select specific statistic names
    #[structopt(short, long, default_value = ".*")]
    stat_name_match: String,
    /// regex to select specific table names (only sensible with --details-enable, default mode adds the statistics for all tables)
    #[structopt(short, long, default_value = ".*")]
    table_name_match: String,
    /// regex to select hostnames or ports (so you can select master or tserver by port number)
    #[structopt(long, default_value = ".*")]
    hostname_match:String,
    /// boolean (set to enable) to add statistics that are not counters
    #[structopt(short, long)]
    gauges_enable: bool,
    /// boolean (set to enable) to report for each table or tablet individually
    #[structopt(short, long)]
    details_enable: bool,
    /// boolean (set to enable) to perform a snapshot of the statistics, stored as CSV files in yb_stats.snapshots
    #[structopt(long)]
    snapshot: bool,
    /// comment to be added with the snapshot, to make review or use more easy
    #[structopt(long, default_value = "")]
    snapshot_comment: String,
    /// this lists the snapshots, and allows you to select a begin and end snapshot for a diff report
    #[structopt(long)]
    snapshot_diff: bool,
    /// print memtrackers data for the given snapshot
    #[structopt(long, default_value = "-1")]
    print_memtrackers: String,
    /// print log data for the given snapshot
    #[structopt(long, default_value = "-1")]
    print_log: String,
    /// print version data for the given snapshot
    #[structopt(long, default_value = "-1")]
    print_version: String,
    /// print threads data for the given snapshot
    #[structopt(long, default_value = "-1")]
    print_threads: String,
    /// log data severity to include: optional: I
    #[structopt(long, default_value = "WEF")]
    log_severity: String,
}

fn main() {

    // create variables based on StructOpt values
    let options = Opts::from_args();
    let hostname_port_vec: Vec<&str> = options.metric_sources.split(",").collect();
    let stat_name_match = &options.stat_name_match.as_str();
    let stat_name_filter = Regex::new(stat_name_match).unwrap();
    let table_name_match = &options.table_name_match.as_str();
    let table_name_filter = Regex::new(table_name_match).unwrap();
    let hostname_match = &options.hostname_match.as_str();
    let hostname_filter = Regex::new(hostname_match).unwrap();
    let gauges_enable = options.gauges_enable as bool;
    let details_enable = options.details_enable as bool;
    let snapshot: bool = options.snapshot as bool;
    let snapshot_comment: String = options.snapshot_comment;
    let snapshot_diff: bool = options.snapshot_diff as bool;
    let print_memtrackers: String = options.print_memtrackers;
    let print_log: String = options.print_log;
    let print_version: String = options.print_version;
    let print_threads: String = options.print_threads;
    let log_severity: String = options.log_severity;

    if snapshot {

        let snapshot_number: i32 = perform_snapshot(hostname_port_vec, snapshot_comment);
        println!("snapshot number {}", snapshot_number);
        process::exit(0);

    } else if snapshot_diff {

        let current_directory = env::current_dir().unwrap();
        let yb_stats_directory = current_directory.join("yb_stats.snapshots");
        let snapshots: Vec<Snapshot> = read_snapshots_from_file(&yb_stats_directory);

        for row in &snapshots {
            println!("{:>3} {:30} {:50}", row.number, row.timestamp, row.comment);
        }

        let (begin_snapshot, end_snapshot, begin_snapshot_row) = read_begin_end_snapshot_from_user(&snapshots);

        // first snapshot
        let stored_values: Vec<StoredValues> = read_values_snapshot(&begin_snapshot, &yb_stats_directory);
        let stored_countsum: Vec<StoredCountSum> = read_countsum_snapshot(&begin_snapshot, &yb_stats_directory);
        let stored_countsumrows: Vec<StoredCountSumRows> = read_countsumrows_snapshot(&begin_snapshot, &yb_stats_directory);
        let stored_statements: Vec<StoredStatements> = read_statements_snapshot(&begin_snapshot, &yb_stats_directory);
        // process first snapshot results
        let (values_map, countsum_map, countsumrows_map) = build_metrics_btreemaps(stored_values, stored_countsum, stored_countsumrows);
        let (mut values_diff, mut countsum_diff, mut countsumrows_diff) = insert_first_snapshot_metrics(values_map, countsum_map, countsumrows_map);
        let mut statements_diff = insert_first_snapshot_statements(stored_statements);

        // second snapshot
        let stored_values: Vec<StoredValues> = read_values_snapshot(&end_snapshot, &yb_stats_directory);
        let stored_countsum: Vec<StoredCountSum> = read_countsum_snapshot(&end_snapshot, &yb_stats_directory);
        let stored_countsumrows: Vec<StoredCountSumRows> = read_countsumrows_snapshot(&end_snapshot, &yb_stats_directory);
        let stored_statements: Vec<StoredStatements> = read_statements_snapshot(&end_snapshot, &yb_stats_directory);
        // process second snapshot results
        let (values_map, countsum_map, countsumrows_map) = build_metrics_btreemaps(stored_values, stored_countsum, stored_countsumrows);
        insert_second_snapshot_metrics(values_map, &mut values_diff, countsum_map, &mut countsum_diff, countsumrows_map, &mut countsumrows_diff, &begin_snapshot_row.timestamp);
        insert_second_snapshot_statements(stored_statements, &mut statements_diff, &begin_snapshot_row.timestamp);

        // print difference
        print_diff(&values_diff, &countsum_diff, &countsumrows_diff, &hostname_filter, &stat_name_filter, &table_name_filter, &details_enable, &gauges_enable);
        print_diff_statements(&statements_diff, &hostname_filter);

    } else if print_memtrackers != "-1" {

        let current_directory = env::current_dir().unwrap();
        let yb_stats_directory = current_directory.join("yb_stats.snapshots");
        print_memtrackers_data(&print_memtrackers, &yb_stats_directory, &hostname_filter, &stat_name_filter);

    } else if print_log != "-1" {

        let current_directory = env::current_dir().unwrap();
        let yb_stats_directory = current_directory.join("yb_stats.snapshots");
        print_loglines(&print_log, &yb_stats_directory, &hostname_filter, &log_severity);

    } else if print_version != "-1" {

        let current_directory = env::current_dir().unwrap();
        let yb_stats_directory = current_directory.join("yb_stats.snapshots");
        print_version_data(&print_version, &yb_stats_directory, &hostname_filter);

    } else if print_threads != "-1" {

        let current_directory = env::current_dir().unwrap();
        let yb_stats_directory = current_directory.join("yb_stats.snapshots");
        print_threads_data(&print_threads, &yb_stats_directory, &hostname_filter);

    } else {

        // first snapshot
        let mut stored_values: Vec<StoredValues> = Vec::new();
        let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
        let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
        let mut stored_statements: Vec<StoredStatements> = Vec::new();

        let first_snapshot_time = Local::now();
        for hostname in &hostname_port_vec {
            let detail_snapshot_time = Local::now();
            let data_parsed_from_json = read_metrics(&hostname);
            add_to_metric_vectors(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_values, &mut stored_countsum, &mut stored_countsumrows);
            let data_parsed_from_json = read_statements(&hostname);
            add_to_statements_vector(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_statements);
        }
        // process first snapshot results
        let (values_map, countsum_map, countsumrows_map) = build_metrics_btreemaps( stored_values, stored_countsum, stored_countsumrows);
        let (mut values_diff, mut countsum_diff, mut countsumrows_diff) = insert_first_snapshot_metrics(values_map, countsum_map, countsumrows_map);
        let mut statements_diff = insert_first_snapshot_statements(stored_statements);

        println!("Begin metrics snapshot created, press enter to create end snapshot for difference calculation.");
        let mut input = String::new();
        stdin().read_line(&mut input).ok().expect("failed");

        // second snapshot
        let mut stored_values: Vec<StoredValues> = Vec::new();
        let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
        let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
        let mut stored_statements: Vec<StoredStatements> = Vec::new();

        let second_snapshot_time = Local::now();
        for hostname in &hostname_port_vec {
            let detail_snapshot_time = Local::now();
            let data_parsed_from_json = read_metrics(&hostname);
            add_to_metric_vectors(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_values, &mut stored_countsum, &mut stored_countsumrows);
            let data_parsed_from_json = read_statements(&hostname);
            add_to_statements_vector(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_statements);
        }
        // process second snapshot results
        let (values_map, countsum_map, countsumrows_map) = build_metrics_btreemaps( stored_values, stored_countsum, stored_countsumrows);
        insert_second_snapshot_metrics(values_map, &mut values_diff, countsum_map, &mut countsum_diff, countsumrows_map, &mut countsumrows_diff, &first_snapshot_time);
        insert_second_snapshot_statements(stored_statements, &mut statements_diff, &first_snapshot_time);

        // print difference
        //println!("Time between snapshots: {:5.3}", (second_snapshot_time-first_snapshot_time).num_milliseconds()/1000);
        println!("Time between snapshots: {:8.3} seconds", (second_snapshot_time-first_snapshot_time).num_milliseconds() as f64/1000 as f64);
        print_diff(&values_diff, &countsum_diff, &countsumrows_diff, &hostname_filter, &stat_name_filter, &table_name_filter, &details_enable, &gauges_enable);
        print_diff_statements(&statements_diff, &hostname_filter);

    }
}
