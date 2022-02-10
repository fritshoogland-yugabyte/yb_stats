mod latency_statistic_details;
mod value_statistic_details;

use structopt::StructOpt;
use std::process;
use std::collections::BTreeMap;
use regex::Regex;
use chrono::Local;

use yb_stats::{SnapshotDiffValues, SnapshotDiffLatencies, StoredValues, StoredLatencies, perform_snapshot, read_metrics, add_to_metric_vectors, insert_first_snap_into_diff, insert_second_snap_into_diff, build_summary, build_detail};

#[derive(Debug, StructOpt)]
struct Opts {
    #[structopt(short, long, default_value = "192.168.66.80:7000,192.168.66.81:7000,192.168.66.82:7000")]
    metric_sources: String,
    #[structopt(short, long, default_value = ".*")]
    stat_name_match: String,
    #[structopt(short, long, default_value = ".*")]
    table_name_match: String,
    #[structopt(short, long, default_value = "2")]
    wait_time: i32,
    #[structopt(short, long)]
    begin_end_mode: bool,
    #[structopt(short, long)]
    gauges_enable: bool,
    #[structopt(short, long)]
    details_enable: bool,
    #[structopt(long)]
    snapshot: bool,
    #[structopt(long, default_value = "")]
    snapshot_comment: String,
}

fn main()
{
    // create variables based on StructOpt values
    let options = Opts::from_args();
    let hostname_port_vec: Vec<&str> = options.metric_sources.split(",").collect();
    let stat_name_match = &options.stat_name_match.as_str();
    let stat_name_filter = Regex::new(stat_name_match).unwrap();
    let table_name_match = &options.table_name_match.as_str();
    let table_name_filter = Regex::new(table_name_match).unwrap();
    let wait_time = options.wait_time as u64;
    let begin_end_mode = options.begin_end_mode as bool;
    let gauges_enable = options.gauges_enable as bool;
    let details_enable = options.details_enable as bool;
    let snapshot: bool = options.snapshot as bool;
    let snapshot_comment: String = options.snapshot_comment;


    if snapshot {
        let snapshot_number: i32 = perform_snapshot(hostname_port_vec, snapshot_comment);
        println!("snapshot number {}", snapshot_number);
        process::exit(0);
    }

    let mut stored_values: Vec<StoredValues> = Vec::new();
    let mut values_diff: BTreeMap<(String, String, String, String), SnapshotDiffValues> = BTreeMap::new();
    let mut values_detail: BTreeMap<(String, String, String, String), StoredValues> = BTreeMap::new();
    let mut values_summary: BTreeMap<(String, String, String, String), StoredValues> = BTreeMap::new();
    let mut stored_latencies: Vec<StoredLatencies> = Vec::new();
    let mut latencies_diff: BTreeMap<(String, String, String, String), SnapshotDiffLatencies> = BTreeMap::new();
    let mut latencies_detail: BTreeMap<(String, String, String, String), StoredLatencies> = BTreeMap::new();
    let mut latencies_summary: BTreeMap<(String, String, String, String), StoredLatencies> = BTreeMap::new();
    let first_snapshot_time = Local::now();
    for hostname in &hostname_port_vec {
        let detail_snapshot_time = Local::now();
        let data_parsed_from_json = read_metrics(&hostname);
        add_to_metric_vectors(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_values, &mut stored_latencies);
    }
    if details_enable {
        build_detail(&mut values_detail, &stored_values, &mut latencies_detail, &stored_latencies);
        insert_first_snap_into_diff(&mut values_diff, &values_detail, &mut latencies_diff, &latencies_detail);
    } else {
        build_summary(&mut values_summary, &stored_values, &mut latencies_summary, &stored_latencies);
        insert_first_snap_into_diff(&mut values_diff, &values_summary, &mut latencies_diff, &latencies_summary);
    }

    let mut stored_values: Vec<StoredValues> = Vec::new();
    let mut values_detail: BTreeMap<(String, String, String, String), StoredValues> = BTreeMap::new();
    let mut values_summary: BTreeMap<(String, String, String, String), StoredValues> = BTreeMap::new();
    let mut stored_latencies: Vec<StoredLatencies> = Vec::new();
    let mut latencies_diff: BTreeMap<(String, String, String, String), SnapshotDiffLatencies> = BTreeMap::new();
    let mut latencies_detail: BTreeMap<(String, String, String, String), StoredLatencies> = BTreeMap::new();
    for hostname in &hostname_port_vec {
        let detail_snapshot_time = Local::now();
        let data_parsed_from_json = read_metrics(&hostname);
        add_to_metric_vectors(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_values, &mut stored_latencies);
    }
    if details_enable {
        build_detail(&mut values_detail, &stored_values, &mut latencies_detail, &stored_latencies);
        insert_second_snap_into_diff(&mut values_diff, &values_detail, &mut latencies_diff, &latencies_detail, first_snapshot_time);
    } else {
        build_summary(&mut values_summary, &stored_values, &mut latencies_summary, &stored_latencies);
        insert_second_snap_into_diff(&mut values_diff, &values_summary, &mut latencies_diff, &latencies_summary, first_snapshot_time);
    }

    for ((hostname_port, metric_type, metric_id, metric_name), diff_values) in values_diff {
       if diff_values.second_snapshot_value - diff_values.first_snapshot_value > 0 {
           println!("{} {} {} {} {} {}", hostname_port, metric_id,  metric_name, diff_values.table_name, diff_values.second_snapshot_value, diff_values.first_snapshot_value );
       }
    }
    for ((hostname_port, metric_type, metric_id, metric_name), diff_latencies) in latencies_diff {
        if diff_latencies.second_snapshot_total_count - diff_latencies.first_snapshot_total_count > 0 {
            println!("{} {} {} {} {} {}", hostname_port, metric_id, metric_name, diff_latencies.table_name, diff_latencies.second_snapshot_total_count, diff_latencies.first_snapshot_total_count );
        }
    }
}