//use value_statistic_details::ValueStatisticDetails;
mod value_statistic_details;
//use countsum_statistic_details::CountSumStatisticDetails;
mod countsum_statistic_details;

use structopt::StructOpt;
use std::process;
use std::collections::BTreeMap;
use regex::Regex;
use chrono::Local;
use std::io::{stdin, stdout, Write};
use std::fs;
use std::env;

use yb_stats::{SnapshotDiffStatements, SnapshotDiffValues, SnapshotDiffCountSum, StoredValues, StoredCountSum, perform_snapshot, read_metrics, add_to_metric_vectors, insert_first_snap_into_diff, insert_second_snap_into_diff, build_summary, build_detail, Snapshot, print_diff, StoredCountSumRows, SnapshotDiffCountSumRows, StoredStatements, insert_first_snap_into_statements_diff, insert_second_snap_into_statements_diff, print_diff_statements, read_statements, add_to_statements_vector};

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

    if snapshot {

        let snapshot_number: i32 = perform_snapshot(hostname_port_vec, snapshot_comment);
        println!("snapshot number {}", snapshot_number);
        process::exit(0);

    }

    if snapshot_diff {

        let mut snapshots: Vec<Snapshot> = Vec::new();
        let current_directory = env::current_dir().unwrap();
        let yb_stats_directory = current_directory.join("yb_stats.snapshots");
        let snapshot_index = &yb_stats_directory.join("snapshot.index");

        let file = fs::File::open(&snapshot_index)
            .unwrap_or_else(|e| {
                eprintln!("Fatal: error opening file {}: {}", &snapshot_index.clone().into_os_string().into_string().unwrap(), e);
                process::exit(1);
            });
        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: Snapshot = row.unwrap();
            let _ = &snapshots.push(data);
        }

        for row in &snapshots {
            println!("{} {} {}", row.number, row.timestamp, row.comment);
        }
        let mut begin_snapshot = String::new();
        let mut end_snapshot = String::new();
        print!("Enter begin snapshot: ");
        let _ = stdout().flush();
        stdin().read_line(&mut begin_snapshot).expect("Failed to read input.");
        let begin_snapshot: i32 = begin_snapshot.trim().parse().expect("Invalid input");
        let begin_snapshot_row = snapshots.iter().find(|&row| row.number == begin_snapshot).unwrap();

        print!("Enter end snapshot: ");
        let _ = stdout().flush();
        stdin().read_line(&mut end_snapshot).expect("Failed to read input.");
        let end_snapshot: i32 = end_snapshot.trim().parse().expect("Invalid input");

        let mut stored_values: Vec<StoredValues> = Vec::new();
        let mut values_diff: BTreeMap<(String, String, String, String), SnapshotDiffValues> = BTreeMap::new();
        let mut values_detail: BTreeMap<(String, String, String, String), StoredValues> = BTreeMap::new();
        let mut values_summary: BTreeMap<(String, String, String, String), StoredValues> = BTreeMap::new();
        let values_file = &yb_stats_directory.join(&begin_snapshot.to_string()).join("values");
        let file = fs::File::open(&values_file)
            .unwrap_or_else(|e| {
                eprintln!("Fatal: error reading file: {}: {}", &values_file.clone().into_os_string().into_string().unwrap(), e);
                process::exit(1);
            });
        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredValues = row.unwrap();
            let _ = &stored_values.push(data);
        }

        let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
        let mut countsum_diff: BTreeMap<(String, String, String, String), SnapshotDiffCountSum> = BTreeMap::new();
        let mut countsum_detail: BTreeMap<(String, String, String, String), StoredCountSum> = BTreeMap::new();
        let mut countsum_summary: BTreeMap<(String, String, String, String), StoredCountSum> = BTreeMap::new();
        let countsum_file = &yb_stats_directory.join(&begin_snapshot.to_string()).join("countsum");
        let file = fs::File::open(&countsum_file)
            .unwrap_or_else(|e| {
                eprintln!("Fatal: error reading file: {}: {}", &countsum_file.clone().into_os_string().into_string().unwrap(), e);
                process::exit(1);
            });
        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredCountSum = row.unwrap();
            let _ = &stored_countsum.push(data);
        }

        let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
        let mut countsumrows_diff: BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows> = BTreeMap::new();
        let mut countsumrows_detail: BTreeMap<(String, String, String, String), StoredCountSumRows> = BTreeMap::new();
        let mut countsumrows_summary: BTreeMap<(String, String, String, String), StoredCountSumRows> = BTreeMap::new();
        let countsumrows_file = &yb_stats_directory.join(&begin_snapshot.to_string()).join("countsumrows");
        let file = fs::File::open(&countsumrows_file)
            .unwrap_or_else(|e| {
                eprintln!("Fatal: error reading file: {}: {}", &countsumrows_file.clone().into_os_string().into_string().unwrap(), e);
                process::exit(1);
            });
        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredCountSumRows = row.unwrap();
            let _ = &stored_countsumrows.push(data);
        }

        let mut stored_statements: Vec<StoredStatements> = Vec::new();
        let mut statements_diff: BTreeMap<(String, String), SnapshotDiffStatements> = BTreeMap::new();
        let statements_file = &yb_stats_directory.join(&begin_snapshot.to_string()).join("statements");
        let file = fs::File::open(&statements_file)
            .unwrap_or_else(|e| {
                eprintln!("Fatal: error reading file: {}: {}", &statements_file.clone().into_os_string().into_string().unwrap(), e);
                process::exit(1);
            });
        let mut reader = csv::Reader::from_reader(file);
        for  row in reader.deserialize() {
            let data: StoredStatements = row.unwrap();
            let _ = &stored_statements.push(data);
        }

        if details_enable {
            build_detail(&mut values_detail, &stored_values, &mut countsum_detail, &stored_countsum, &mut countsumrows_detail, &stored_countsumrows);
            insert_first_snap_into_diff(&mut values_diff, &values_detail, &mut countsum_diff, &countsum_detail, &mut countsumrows_diff, &countsumrows_detail);
        } else {
            build_summary(&mut values_summary, &stored_values, &mut countsum_summary, &stored_countsum, &mut countsumrows_summary, &stored_countsumrows);
            insert_first_snap_into_diff(&mut values_diff, &values_summary, &mut countsum_diff, &countsum_summary, &mut countsumrows_diff, &countsumrows_summary);
        }
        insert_first_snap_into_statements_diff(&mut statements_diff, &stored_statements);

        let mut stored_values: Vec<StoredValues> = Vec::new();
        let mut values_detail: BTreeMap<(String, String, String, String), StoredValues> = BTreeMap::new();
        let mut values_summary: BTreeMap<(String, String, String, String), StoredValues> = BTreeMap::new();
        let values_file = &yb_stats_directory.join(&end_snapshot.to_string()).join("values");
        let file = fs::File::open(&values_file)
            .unwrap_or_else(|e| {
                eprintln!("Fatal: error reading file: {}: {}", &values_file.clone().into_os_string().into_string().unwrap(), e);
                process::exit(1);
            });
        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredValues = row.unwrap();
            let _ = &stored_values.push(data);
        }

        let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
        let mut countsum_detail: BTreeMap<(String, String, String, String), StoredCountSum> = BTreeMap::new();
        let mut countsum_summary: BTreeMap<(String, String, String, String), StoredCountSum> = BTreeMap::new();

        let countsum_file = &yb_stats_directory.join(&end_snapshot.to_string()).join("countsum");
        let file = fs::File::open(&countsum_file)
            .unwrap_or_else(|e| {
                eprintln!("Fatal: error reading file: {}: {}", &countsum_file.clone().into_os_string().into_string().unwrap(), e);
                process::exit(1);
            });
        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredCountSum = row.unwrap();
            let _ = &stored_countsum.push(data);
        }

        let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
        let mut countsumrows_detail: BTreeMap<(String, String, String, String), StoredCountSumRows> = BTreeMap::new();
        let mut countsumrows_summary: BTreeMap<(String, String, String, String), StoredCountSumRows> = BTreeMap::new();

        let countsumrows_file = &yb_stats_directory.join(&end_snapshot.to_string()).join("countsumrows");
        let file = fs::File::open(&countsumrows_file)
            .unwrap_or_else(|e| {
                eprintln!("Fatal: error reading file: {}: {}", &countsumrows_file.clone().into_os_string().into_string().unwrap(), e);
                process::exit(1);
            });
        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredCountSumRows = row.unwrap();
            let _ = &stored_countsumrows.push(data);
        }

        let mut stored_statements: Vec<StoredStatements> = Vec::new();
        let statements_file = &yb_stats_directory.join(&end_snapshot.to_string()).join("statements");
        let file = fs::File::open(&statements_file)
            .unwrap_or_else(|e| {
                eprintln!("Fatal: error reading file: {}: {}", &statements_file.clone().into_os_string().into_string().unwrap(), e);
                process::exit(1);
            });
        let mut reader = csv::Reader::from_reader(file);
        for  row in reader.deserialize() {
            let data: StoredStatements = row.unwrap();
            let _ = &stored_statements.push(data);
        }

        if details_enable {
            build_detail(&mut values_detail, &stored_values, &mut countsum_detail, &stored_countsum, &mut countsumrows_detail, &stored_countsumrows);
            insert_second_snap_into_diff(&mut values_diff, &values_detail, &mut countsum_diff, &countsum_detail, &mut countsumrows_diff, &countsumrows_detail, &begin_snapshot_row.timestamp);
        } else {
            build_summary(&mut values_summary, &stored_values, &mut countsum_summary, &stored_countsum, &mut countsumrows_summary, &stored_countsumrows);
            insert_second_snap_into_diff(&mut values_diff, &values_summary, &mut countsum_diff, &countsum_summary, &mut countsumrows_diff, &countsumrows_summary, &begin_snapshot_row.timestamp);
        }
        insert_second_snap_into_statements_diff(&mut statements_diff, &stored_statements, &begin_snapshot_row.timestamp);

        print_diff(&values_diff, &countsum_diff, &countsumrows_diff, &hostname_filter, &stat_name_filter, &table_name_filter, &details_enable, &gauges_enable);

        print_diff_statements(&statements_diff, &hostname_filter);

    } else {

        let mut stored_values: Vec<StoredValues> = Vec::new();
        let mut values_diff: BTreeMap<(String, String, String, String), SnapshotDiffValues> = BTreeMap::new();
        let mut values_detail: BTreeMap<(String, String, String, String), StoredValues> = BTreeMap::new();
        let mut values_summary: BTreeMap<(String, String, String, String), StoredValues> = BTreeMap::new();
        let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
        let mut countsum_diff: BTreeMap<(String, String, String, String), SnapshotDiffCountSum> = BTreeMap::new();
        let mut countsum_detail: BTreeMap<(String, String, String, String), StoredCountSum> = BTreeMap::new();
        let mut countsum_summary: BTreeMap<(String, String, String, String), StoredCountSum> = BTreeMap::new();
        let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
        let mut countsumrows_diff: BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows> = BTreeMap::new();
        let mut countsumrows_detail: BTreeMap<(String, String, String, String), StoredCountSumRows> = BTreeMap::new();
        let mut countsumrows_summary: BTreeMap<(String, String, String, String), StoredCountSumRows> = BTreeMap::new();
        let mut stored_statements: Vec<StoredStatements> = Vec::new();
        let mut statements_diff: BTreeMap<(String, String), SnapshotDiffStatements> = BTreeMap::new();

        let first_snapshot_time = Local::now();
        for hostname in &hostname_port_vec {
            let detail_snapshot_time = Local::now();
            let data_parsed_from_json = read_metrics(&hostname);
            add_to_metric_vectors(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_values, &mut stored_countsum, &mut stored_countsumrows);
            let data_parsed_from_json = read_statements(&hostname);
            add_to_statements_vector(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_statements);
        }
        if details_enable {
            build_detail(&mut values_detail, &stored_values, &mut countsum_detail, &stored_countsum, &mut countsumrows_detail, &stored_countsumrows);
            insert_first_snap_into_diff(&mut values_diff, &values_detail, &mut countsum_diff, &countsum_detail, &mut countsumrows_diff, &countsumrows_detail);
        } else {
            build_summary(&mut values_summary, &stored_values, &mut countsum_summary, &stored_countsum, &mut countsumrows_summary, &stored_countsumrows);
            insert_first_snap_into_diff(&mut values_diff, &values_summary, &mut countsum_diff, &countsum_summary, &mut countsumrows_diff, &countsumrows_summary);
        }
        insert_first_snap_into_statements_diff(&mut statements_diff, &stored_statements);

        println!("Begin metrics snapshot created, press enter to create end snapshot for difference calculation.");
        let mut input = String::new();
        stdin().read_line(&mut input).ok().expect("failed");

        let mut stored_values: Vec<StoredValues> = Vec::new();
        let mut values_detail: BTreeMap<(String, String, String, String), StoredValues> = BTreeMap::new();
        let mut values_summary: BTreeMap<(String, String, String, String), StoredValues> = BTreeMap::new();
        let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
        let mut countsum_detail: BTreeMap<(String, String, String, String), StoredCountSum> = BTreeMap::new();
        let mut countsum_summary: BTreeMap<(String, String, String, String), StoredCountSum> = BTreeMap::new();
        let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
        let mut countsumrows_detail: BTreeMap<(String, String, String, String), StoredCountSumRows> = BTreeMap::new();
        let mut countsumrows_summary: BTreeMap<(String, String, String, String), StoredCountSumRows> = BTreeMap::new();
        let mut stored_statements: Vec<StoredStatements> = Vec::new();

        for hostname in &hostname_port_vec {
            let detail_snapshot_time = Local::now();
            let data_parsed_from_json = read_metrics(&hostname);
            add_to_metric_vectors(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_values, &mut stored_countsum, &mut stored_countsumrows);
            let data_parsed_from_json = read_statements(&hostname);
            add_to_statements_vector(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_statements);
        }
        if details_enable {
            build_detail(&mut values_detail, &stored_values, &mut countsum_detail, &stored_countsum, &mut countsumrows_detail, &stored_countsumrows);
            insert_second_snap_into_diff(&mut values_diff, &values_detail, &mut countsum_diff, &countsum_detail, &mut countsumrows_diff, &countsumrows_detail, &first_snapshot_time);
        } else {
            build_summary(&mut values_summary, &stored_values, &mut countsum_summary, &stored_countsum, &mut countsumrows_summary, &stored_countsumrows);
            insert_second_snap_into_diff(&mut values_diff, &values_summary, &mut countsum_diff, &countsum_summary, &mut countsumrows_diff, &countsumrows_summary, &first_snapshot_time);
        }
        insert_second_snap_into_statements_diff(&mut statements_diff, &stored_statements, &first_snapshot_time);

        print_diff(&values_diff, &countsum_diff, &countsumrows_diff, &hostname_filter, &stat_name_filter, &table_name_filter, &details_enable, &gauges_enable);

        print_diff_statements(&statements_diff, &hostname_filter);

    }
}