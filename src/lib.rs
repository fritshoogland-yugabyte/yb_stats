extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate csv;

mod value_statistic_details;
use value_statistic_details::{ValueStatisticDetails, value_create_hashmap};
mod countsum_statistic_details;
use countsum_statistic_details::{CountSumStatisticDetails, countsum_create_hashmap};

pub mod memtrackers;
use memtrackers::{MemTrackers, StoredMemTrackers, read_memtrackers, add_to_memtrackers_vector};

pub mod loglines;
use loglines::{StoredLogLines, read_loglines, add_to_loglines_vector};

pub mod gflags;
use gflags::{StoredGFlags, read_gflags, add_to_gflags_vector};

pub mod versions;
use versions::{StoredVersionData, read_version, add_to_version_vector};

pub mod metrics;
use metrics::{StoredValues,StoredCountSum, StoredCountSumRows, read_metrics, add_to_metric_vectors};

pub mod statements;
use statements::{StoredStatements, read_statements, add_to_statements_vector};

use std::collections::BTreeMap;
use chrono::{DateTime, Local};
use std::process;
use std::fs;
use std::path::{Path, PathBuf};
use std::env;
use regex::Regex;
use substring::Substring;
use std::io::{stdin, stdout, Write};

#[derive(Debug)]
pub struct SnapshotDiffStatements {
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub first_calls: i64,
    pub second_calls: i64,
    pub first_total_time: f64,
    pub second_total_time: f64,
    pub first_rows: i64,
    pub second_rows: i64,
}

#[derive(Debug)]
pub struct SnapshotDiffValues {
    pub table_name: String,
    pub namespace: String,
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub first_snapshot_value: i64,
    pub second_snapshot_value: i64,
}
#[derive(Debug)]
pub struct SnapshotDiffCountSum {
    pub table_name: String,
    pub namespace: String,
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub second_snapshot_total_count: u64,
    pub second_snapshot_min: u64,
    pub second_snapshot_mean: f64,
    pub second_snapshot_percentile_75: u64,
    pub second_snapshot_percentile_95: u64,
    pub second_snapshot_percentile_99: u64,
    pub second_snapshot_percentile_99_9: u64,
    pub second_snapshot_percentile_99_99: u64,
    pub second_snapshot_max: u64,
    pub second_snapshot_total_sum: u64,
    pub first_snapshot_total_count: u64,
    pub first_snapshot_total_sum: u64,
}

#[derive(Debug)]
pub struct SnapshotDiffCountSumRows {
    pub table_name: String,
    pub namespace: String,
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub first_snapshot_count: u64,
    pub first_snapshot_sum: u64,
    pub first_snapshot_rows: u64,
    pub second_snapshot_count: u64,
    pub second_snapshot_sum: u64,
    pub second_snapshot_rows: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Snapshot {
    pub number: i32,
    pub timestamp: DateTime<Local>,
    pub comment: String,
}

pub fn read_snapshots_from_file( yb_stats_directory: &PathBuf ) -> Vec<Snapshot> {

    let mut snapshots: Vec<Snapshot> = Vec::new();
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
    snapshots
}

pub fn read_begin_end_snapshot_from_user( snapshots: &Vec<Snapshot> ) -> (String, String, Snapshot) {

    let mut begin_snapshot = String::new();
    print!("Enter begin snapshot: ");
    let _ = stdout().flush();
    stdin().read_line(&mut begin_snapshot).expect("Failed to read input.");
    let begin_snapshot: i32 = begin_snapshot.trim().parse().expect("Invalid input");
    let begin_snapshot_row = match snapshots.iter().find(|&row| row.number == begin_snapshot) {
        Some(snapshot_find_result) => snapshot_find_result.clone(),
        None => {
            eprintln!("Fatal: snapshot number {} is not found in the snapshot list", begin_snapshot);
            process::exit(1);
        }
    };

    let mut end_snapshot = String::new();
    print!("Enter end snapshot: ");
    let _ = stdout().flush();
    stdin().read_line(&mut end_snapshot).expect("Failed to read input.");
    let end_snapshot: i32 = end_snapshot.trim().parse().expect("Invalid input");
    let _ = match snapshots.iter().find(|&row| row.number == end_snapshot) {
        Some(snapshot_find_result) => snapshot_find_result.clone(),
        None => {
            eprintln!("Fatal: snapshot number {} is not found in the snapshot list", end_snapshot);
            process::exit(1);
        }
    };

    (begin_snapshot.to_string(), end_snapshot.to_string(), begin_snapshot_row)

}

pub fn read_values_snapshot( snapshot_number: &String, yb_stats_directory: &PathBuf ) -> Vec<StoredValues> {

    let mut stored_values: Vec<StoredValues> = Vec::new();
    let values_file = &yb_stats_directory.join(&snapshot_number.to_string()).join("values");
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
    stored_values
}

pub fn read_countsum_snapshot( snapshot_number: &String, yb_stats_directory: &PathBuf ) -> Vec<StoredCountSum> {

    let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
    let countsum_file = &yb_stats_directory.join(&snapshot_number.to_string()).join("countsum");
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
    stored_countsum
}

pub fn read_countsumrows_snapshot( snapshot_number: &String, yb_stats_directory: &PathBuf ) -> Vec<StoredCountSumRows> {

    let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
    let countsumrows_file = &yb_stats_directory.join(&snapshot_number.to_string()).join("countsumrows");
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
    stored_countsumrows
}

pub fn read_statements_snapshot( snapshot_number: &String, yb_stats_directory: &PathBuf ) -> Vec<StoredStatements> {

    let mut stored_statements: Vec<StoredStatements> = Vec::new();
    let statements_file = &yb_stats_directory.join(&snapshot_number.to_string()).join("statements");
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
    stored_statements
}

pub fn perform_snapshot( hostname_port_vec: Vec<&str>,
                         snapshot_comment: String,
                        ) -> i32 {
    let snapshot_time = Local::now();
    let mut snapshot_number: i32 = 0;
    let mut snapshots: Vec<Snapshot> = Vec::new();
    let mut stored_values: Vec<StoredValues> = Vec::new();
    let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
    let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
    let mut stored_versiondata: Vec<StoredVersionData> = Vec::new();
    let mut stored_statements: Vec<StoredStatements> = Vec::new();
    let mut stored_gflags: Vec<StoredGFlags> = Vec::new();
    let mut stored_loglines: Vec<StoredLogLines> = Vec::new();
    let mut stored_memtrackers: Vec<StoredMemTrackers> = Vec::new();

    for hostname in &hostname_port_vec {
        let detail_snapshot_time = Local::now();
        // metrics
        let data_parsed_from_json = read_metrics(&hostname);
        add_to_metric_vectors(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_values, &mut stored_countsum, &mut stored_countsumrows);
        let data_parsed_from_json = read_version(&hostname);
        add_to_version_vector(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_versiondata);
        let data_parsed_from_json = read_statements(&hostname);
        add_to_statements_vector(data_parsed_from_json, hostname, detail_snapshot_time, &mut stored_statements);
        let gflags = read_gflags(&hostname);
        add_to_gflags_vector(gflags, hostname, detail_snapshot_time, &mut stored_gflags);
        let loglines = read_loglines(&hostname);
        add_to_loglines_vector(loglines, hostname, &mut stored_loglines);
        let memtrackers: Vec<MemTrackers> = read_memtrackers(&hostname);
        add_to_memtrackers_vector(memtrackers, hostname, detail_snapshot_time, &mut stored_memtrackers);
    }

    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    fs::create_dir_all(&yb_stats_directory)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error creating directory {}: {}", &yb_stats_directory.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });

    let snapshot_index = &yb_stats_directory.join("snapshot.index");
    if Path::new(&snapshot_index).exists() {
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
        let record_with_highest_snapshot_number = snapshots.iter().max_by_key(|k| k.number).unwrap();
        snapshot_number = record_with_highest_snapshot_number.number + 1;
    }

    let current_snapshot: Snapshot = Snapshot { number: snapshot_number, timestamp: snapshot_time, comment: snapshot_comment };
    let _ = &snapshots.push(current_snapshot);
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&snapshot_index)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing {}: {}", &snapshot_index.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in snapshots {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
    fs::create_dir_all(&current_snapshot_directory)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error creating directory {}: {}", &current_snapshot_directory.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });

    let values_file = &current_snapshot_directory.join("values");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&values_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing values statistics in snapshot directory {}: {}", &values_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_values {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let countsum_file = &current_snapshot_directory.join("countsum");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&countsum_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing countsum statistics in snapshot directory {}: {}", &countsum_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_countsum {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let countsumrows_file = &current_snapshot_directory.join("countsumrows");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&countsumrows_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing countsumrows statistics in snapshot directory {}: {}", &countsumrows_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_countsumrows {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let versions_file = &current_snapshot_directory.join("versions");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&versions_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing versions data in snapshot directory {}: {}", &versions_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_versiondata {
        if row.git_hash != "" {
            writer.serialize(row).unwrap();
        }
    }
    writer.flush().unwrap();

    let statements_file = &current_snapshot_directory.join("statements");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&statements_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing statements data in snapshot directory {}: {}", &statements_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_statements {
        if row.query != "" {
            writer.serialize(row).unwrap();
        }
    }
    writer.flush().unwrap();

    let gflags_file = &current_snapshot_directory.join("gflags");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&gflags_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing gflags data in snapshot directory {}: {}", &gflags_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_gflags {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let loglines_file = &current_snapshot_directory.join("loglines");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&loglines_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing loglines data in snapshot directory {}: {}", &loglines_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_loglines {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let memtrackers_file = &current_snapshot_directory.join("memtrackers");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&memtrackers_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing memtrackers data in snapshot directory {}: {}", &memtrackers_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_memtrackers {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    snapshot_number
}

pub fn insert_first_snapshot_statements(
    stored_statements: Vec<StoredStatements>
) -> BTreeMap<(String, String), SnapshotDiffStatements>
{
    let mut statements_diff: BTreeMap<(String, String), SnapshotDiffStatements> = BTreeMap::new();
    for statement in stored_statements {
        statements_diff.insert( (statement.hostname_port.to_string(), statement.query.to_string()), SnapshotDiffStatements {
            first_snapshot_time: statement.timestamp,
            second_snapshot_time: statement.timestamp,
            first_calls: statement.calls,
            first_total_time: statement.total_time,
            first_rows: statement.rows,
            second_calls: 0,
            second_total_time: 0.0,
            second_rows: 0
        });
    }
    statements_diff
}

pub fn insert_second_snapshot_statements(
    stored_statements: Vec<StoredStatements>,
    statements_diff: &mut BTreeMap<(String, String), SnapshotDiffStatements>,
    first_snapshot_time: &DateTime<Local>
) {
    for statement in stored_statements {
        match statements_diff.get_mut( &(statement.hostname_port.to_string(), statement.query.to_string()) ) {
            Some( statements_diff_row ) => {
                *statements_diff_row = SnapshotDiffStatements {
                    first_snapshot_time: statements_diff_row.first_snapshot_time,
                    second_snapshot_time: statement.timestamp,
                    first_calls: statements_diff_row.first_calls,
                    second_calls: statement.calls,
                    first_total_time: statements_diff_row.first_total_time,
                    second_total_time: statement.total_time,
                    first_rows: statements_diff_row.first_rows,
                    second_rows: statement.rows
                }
            },
            None => {
                statements_diff.insert( (statement.hostname_port.to_string(), statement.query.to_string()), SnapshotDiffStatements {
                    first_snapshot_time: *first_snapshot_time,
                    second_snapshot_time: statement.timestamp,
                    first_calls: 0,
                    second_calls: statement.calls,
                    first_total_time: 0.0,
                    second_total_time: statement.total_time,
                    first_rows: 0,
                    second_rows: statement.rows
                });
            }
        }
    }
}

pub fn insert_first_snapshot_metrics(
    values_map: BTreeMap<(String, String, String, String), StoredValues>,
    countsum_map: BTreeMap<(String, String, String, String), StoredCountSum>,
    countsumrows_map: BTreeMap<(String, String, String, String), StoredCountSumRows>
) -> (
    BTreeMap<(String, String, String, String), SnapshotDiffValues>,
    BTreeMap<(String, String, String, String), SnapshotDiffCountSum>,
    BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows>
) {
    let mut values_diff: BTreeMap<(String, String, String, String), SnapshotDiffValues> = BTreeMap::new();
    for ((hostname_port, metric_type, metric_id, metric_name), storedvalues) in values_map {
        values_diff.insert((hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffValues {
            table_name: storedvalues.attribute_table_name.to_string(),
            namespace: storedvalues.attribute_namespace.to_string(),
            first_snapshot_time: storedvalues.timestamp,
            second_snapshot_time: storedvalues.timestamp,
            first_snapshot_value: storedvalues.metric_value,
            second_snapshot_value: 0
        });
    };

    let mut countsum_diff: BTreeMap<(String, String, String, String), SnapshotDiffCountSum> = BTreeMap::new();
    for ((hostname_port, metric_type, metric_id, metric_name), storedcountsum) in countsum_map {
        countsum_diff.insert( (hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffCountSum {
            table_name: storedcountsum.attribute_table_name.to_string(),
            namespace: storedcountsum.attribute_namespace.to_string(),
            first_snapshot_time: storedcountsum.timestamp,
            second_snapshot_time: storedcountsum.timestamp,
            second_snapshot_total_count: 0,
            second_snapshot_min: 0,
            second_snapshot_mean: 0.0,
            second_snapshot_percentile_75: 0,
            second_snapshot_percentile_95: 0,
            second_snapshot_percentile_99: 0,
            second_snapshot_percentile_99_9: 0,
            second_snapshot_percentile_99_99: 0,
            second_snapshot_max: 0,
            second_snapshot_total_sum: 0,
            first_snapshot_total_count: storedcountsum.metric_total_count,
            first_snapshot_total_sum: storedcountsum.metric_total_sum
        });
    }

    let mut countsumrows_diff: BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows> = BTreeMap::new();
    for ((hostname_port, metric_type, metric_id, metric_name), storedcountsumrows) in countsumrows_map {
        countsumrows_diff.insert( (hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffCountSumRows {
            table_name: storedcountsumrows.attribute_table_name.to_string(),
            namespace: storedcountsumrows.attribute_namespace.to_string(),
            first_snapshot_time: storedcountsumrows.timestamp,
            second_snapshot_time: storedcountsumrows.timestamp,
            first_snapshot_count: storedcountsumrows.metric_count,
            second_snapshot_count: 0,
            first_snapshot_sum: storedcountsumrows.metric_sum,
            second_snapshot_sum: 0,
            first_snapshot_rows: storedcountsumrows.metric_rows,
            second_snapshot_rows: 0
        });
    }

    (values_diff,countsum_diff,countsumrows_diff)
}

pub fn insert_second_snapshot_metrics(
    values_map: BTreeMap<(String, String, String, String), StoredValues>,
    values_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffValues>,
    countsum_map: BTreeMap<(String, String, String, String), StoredCountSum>,
    countsum_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffCountSum>,
    countsumrows_map: BTreeMap<(String, String, String, String), StoredCountSumRows>,
    countsumrows_diff: &mut BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows>,
    first_snapshot_time: &DateTime<Local>
) {
    for ((hostname_port, metric_type, metric_id, metric_name), storedvalues) in values_map {
        match values_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string())) {
            Some( values_diff_row ) => {
                *values_diff_row = SnapshotDiffValues {
                    table_name: values_diff_row.table_name.to_string(),
                    namespace: values_diff_row.namespace.to_string(),
                    first_snapshot_time: values_diff_row.first_snapshot_time,
                    second_snapshot_time: storedvalues.timestamp,
                    first_snapshot_value: values_diff_row.first_snapshot_value,
                    second_snapshot_value: storedvalues.metric_value,
                };
            },
            None => {
                values_diff.insert((hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffValues {
                    table_name: storedvalues.attribute_table_name.to_string(),
                    namespace: storedvalues.attribute_namespace.to_string(),
                    first_snapshot_time: *first_snapshot_time,
                    second_snapshot_time: storedvalues.timestamp,
                    first_snapshot_value: 0,
                    second_snapshot_value: storedvalues.metric_value
                });
            }
        }
    }
    for ((hostname_port, metric_type, metric_id, metric_name), storedcountsum) in countsum_map {
        match countsum_diff.get_mut( &(hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string())) {
            Some( countsum_diff_row ) => {
                *countsum_diff_row = SnapshotDiffCountSum {
                    table_name: countsum_diff_row.table_name.to_string(),
                    namespace: countsum_diff_row.namespace.to_string(),
                    first_snapshot_time: countsum_diff_row.first_snapshot_time,
                    second_snapshot_time: storedcountsum.timestamp,
                    second_snapshot_total_count: storedcountsum.metric_total_count,
                    second_snapshot_min: storedcountsum.metric_min,
                    second_snapshot_mean: storedcountsum.metric_mean,
                    second_snapshot_percentile_75: storedcountsum.metric_percentile_75,
                    second_snapshot_percentile_95: storedcountsum.metric_percentile_95,
                    second_snapshot_percentile_99: storedcountsum.metric_percentile_99,
                    second_snapshot_percentile_99_9: storedcountsum.metric_percentile_99_9,
                    second_snapshot_percentile_99_99: storedcountsum.metric_percentile_99_99,
                    second_snapshot_max: storedcountsum.metric_max,
                    second_snapshot_total_sum: storedcountsum.metric_total_sum,
                    first_snapshot_total_count: countsum_diff_row.first_snapshot_total_count,
                    first_snapshot_total_sum: countsum_diff_row.first_snapshot_total_sum
                }
            },
            None => {
                countsum_diff.insert((hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffCountSum {
                    table_name: storedcountsum.attribute_table_name.to_string(),
                    namespace: storedcountsum.attribute_namespace.to_string(),
                    first_snapshot_time: *first_snapshot_time,
                    second_snapshot_time: storedcountsum.timestamp,
                    second_snapshot_total_count: storedcountsum.metric_total_count,
                    second_snapshot_min: storedcountsum.metric_min,
                    second_snapshot_mean: storedcountsum.metric_mean,
                    second_snapshot_percentile_75: storedcountsum.metric_percentile_75,
                    second_snapshot_percentile_95: storedcountsum.metric_percentile_95,
                    second_snapshot_percentile_99: storedcountsum.metric_percentile_99,
                    second_snapshot_percentile_99_9: storedcountsum.metric_percentile_99_9,
                    second_snapshot_percentile_99_99: storedcountsum.metric_percentile_99_99,
                    second_snapshot_max: storedcountsum.metric_max,
                    second_snapshot_total_sum: storedcountsum.metric_total_sum,
                    first_snapshot_total_count: 0,
                    first_snapshot_total_sum: 0
                });
            }
        }
    }
    for ((hostname_port, metric_type, metric_id, metric_name), storedcountsumrows) in countsumrows_map {
        match countsumrows_diff.get_mut( &(hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string())) {
            Some( countsumrows_diff_row ) => {
                *countsumrows_diff_row = SnapshotDiffCountSumRows {
                    table_name: countsumrows_diff_row.table_name.to_string(),
                    namespace: countsumrows_diff_row.namespace.to_string(),
                    first_snapshot_time: countsumrows_diff_row.first_snapshot_time,
                    second_snapshot_time: storedcountsumrows.timestamp,
                    first_snapshot_count: countsumrows_diff_row.first_snapshot_count,
                    first_snapshot_sum: countsumrows_diff_row.first_snapshot_sum,
                    first_snapshot_rows: countsumrows_diff_row.first_snapshot_rows,
                    second_snapshot_count: storedcountsumrows.metric_count,
                    second_snapshot_sum: storedcountsumrows.metric_sum,
                    second_snapshot_rows: storedcountsumrows.metric_rows
                }
            },
            None => {
                countsumrows_diff.insert( (hostname_port.to_string(), metric_type.to_string(), metric_id.to_string(), metric_name.to_string()), SnapshotDiffCountSumRows {
                    table_name: storedcountsumrows.attribute_table_name.to_string(),
                    namespace: storedcountsumrows.attribute_namespace.to_string(),
                    first_snapshot_time: *first_snapshot_time,
                    second_snapshot_time: storedcountsumrows.timestamp,
                    first_snapshot_count: 0,
                    first_snapshot_sum: 0,
                    first_snapshot_rows: 0,
                    second_snapshot_count: storedcountsumrows.metric_count,
                    second_snapshot_sum: storedcountsumrows.metric_sum,
                    second_snapshot_rows: storedcountsumrows.metric_rows
                });
            }
        }
    }
}

pub fn print_diff_statements(
    statements_diff: &BTreeMap<(String, String), SnapshotDiffStatements>,
    hostname_filter: &Regex,
) {
    for ((hostname, query), statements_row) in statements_diff {
        if hostname_filter.is_match(&hostname)
        && statements_row.second_calls - statements_row.first_calls != 0 {
            let adaptive_length = if query.len() < 50 { query.len() } else { 50 };
            println!("{:20} {:10} avg: {:15.3} tot: {:15.3} ms avg: {:10} tot: {:10} rows: {:50}",
                     hostname,
                     statements_row.second_calls - statements_row.first_calls,
                     (statements_row.second_total_time - statements_row.first_total_time) / (statements_row.second_calls as f64 - statements_row.first_calls as f64),
                     statements_row.second_total_time - statements_row.first_total_time as f64,
                     (statements_row.second_rows - statements_row.first_rows) / (statements_row.second_calls - statements_row.first_calls),
                     statements_row.second_rows - statements_row.first_rows,
                     query.substring(0, adaptive_length).replace("\n", "")
            );
        }
    }
}

pub fn print_diff(value_diff: &BTreeMap<(String, String, String, String), SnapshotDiffValues>,
                  countsum_diff: &BTreeMap<(String, String, String, String), SnapshotDiffCountSum>,
                  countsumrows_diff: &BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows>,
                  hostname_filter: &Regex,
                  stat_name_filter: &Regex,
                  table_name_filter: &Regex,
                  details_enable: &bool,
                  gauges_enable: &bool
) {
    if *details_enable {
        // value_diff
        let value_statistic_details_lookup = value_create_hashmap();
        for ((hostname, metric_type, metric_id, metric_name), value_diff_row) in value_diff {
            if value_diff_row.second_snapshot_value > 0 {
                if hostname_filter.is_match(&hostname)
                    && stat_name_filter.is_match(&metric_name)
                    && table_name_filter.is_match(&value_diff_row.table_name) {
                    let details = match value_statistic_details_lookup.get(&metric_name.to_string()) {
                        Some(x) => { ValueStatisticDetails { unit: x.unit.to_string(), unit_suffix: x.unit_suffix.to_string(), stat_type: x.stat_type.to_string() } },
                        None => { ValueStatisticDetails { unit: String::from("?"), unit_suffix: String::from("?"), stat_type: String::from("?") } }
                    };
                    let adaptive_length = if metric_id.len() < 15 { 0 } else { metric_id.len() - 15 };
                    if details.stat_type != "gauge"
                        && value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value != 0 {
                        if *details_enable {
                            println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:6} {:>15.3} /s",
                                     hostname,
                                     metric_type,
                                     metric_id.substring(adaptive_length, metric_id.len()),
                                     value_diff_row.namespace,
                                     value_diff_row.table_name,
                                     metric_name,
                                     value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value,
                                     details.unit_suffix,
                                     ((value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value) as f64 / (value_diff_row.second_snapshot_time - value_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000 as f64)
                            );
                        } else {
                            println!("{:20} {:8} {:70} {:15} {:6} {:>15.3} /s",
                                     hostname,
                                     metric_type,
                                     metric_name,
                                     value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value,
                                     details.unit_suffix,
                                     ((value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value) as f64 / (value_diff_row.second_snapshot_time - value_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000 as f64)
                            );
                        }
                    }
                    if details.stat_type == "gauge"
                        && *gauges_enable {
                        if *details_enable {
                            println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:6} {:+15}",
                                     hostname,
                                     metric_type,
                                     metric_id.substring(adaptive_length, metric_id.len()),
                                     value_diff_row.namespace,
                                     value_diff_row.table_name,
                                     metric_name,
                                     value_diff_row.second_snapshot_value,
                                     details.unit_suffix,
                                     value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value
                            );
                        } else {
                            println!("{:20} {:8} {:70} {:15} {:6} {:+15}",
                                     hostname,
                                     metric_type,
                                     metric_name,
                                     value_diff_row.second_snapshot_value,
                                     details.unit_suffix,
                                     value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value
                            );
                        }
                    }
                }
            }
        }
        // countsum_diff
        let countsum_statistic_details_lookup = countsum_create_hashmap();
        for ((hostname, metric_type, metric_id, metric_name), countsum_diff_row) in countsum_diff {
            if countsum_diff_row.second_snapshot_total_count > 0 {
                if hostname_filter.is_match(&hostname)
                    && stat_name_filter.is_match(&metric_name)
                    && table_name_filter.is_match(&countsum_diff_row.table_name) {
                    let details = match countsum_statistic_details_lookup.get(&metric_name.to_string()) {
                        Some(x) => { CountSumStatisticDetails { unit: x.unit.to_string(), unit_suffix: x.unit_suffix.to_string(), divisor: x.divisor, stat_type: x.stat_type.to_string() } },
                        None => { CountSumStatisticDetails { unit: String::from("?"), unit_suffix: String::from("?"), divisor: 0, stat_type: String::from("?") } }
                    };
                    let adaptive_length = if metric_id.len() < 15 { 0 } else { metric_id.len() - 15 };
                    if countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count != 0 {
                        if *details_enable {
                            println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15}        {:>15.3} /s avg: {:9.0} tot: {:>15.3} {:10}",
                                     hostname,
                                     metric_type,
                                     metric_id.substring(adaptive_length, metric_id.len()),
                                     countsum_diff_row.namespace,
                                     countsum_diff_row.table_name,
                                     metric_name,
                                     countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count,
                                     (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count) as f64 / (countsum_diff_row.second_snapshot_time - countsum_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000 as f64,
                                     ((countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum) / (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count)) as f64,
                                     countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum,
                                     details.unit_suffix
                            );
                        } else {
                            println!("{:20} {:8} {:70} {:15}        {:>15.3} /s avg: {:9.0} tot: {:>15.3} {:10}",
                                     hostname,
                                     metric_type,
                                     metric_name,
                                     countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count,
                                     (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count) as f64 / (countsum_diff_row.second_snapshot_time - countsum_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000 as f64,
                                     ((countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum) / (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count)) as f64,
                                     countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum,
                                     details.unit_suffix
                            );
                        }
                    }
                }
            }
        }
    } else {
        // value_diff
        let value_statistic_details_lookup = value_create_hashmap();
        let mut sum_value_diff: BTreeMap<(String, String, String, String), SnapshotDiffValues> = BTreeMap::new();
        for ((hostname_port, metric_type, _metric_id, metric_name), value_diff_row) in value_diff {
            if metric_type == "table" || metric_type == "tablet" {
                // if a table and thus its tablets have been deleted between the first and second snapshot, the second_snapshot_value is 0.
                // however, the first_snapshot_value is > 0, it means it can make the subtraction between the second and the first snapshot get negative, and a summary overview be incorrect.
                // therefore we remove individual statistics where the second snapshot value is set to 0.
                if value_diff_row.second_snapshot_value > 0 {
                    match sum_value_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string())) {
                        Some(sum_value_diff_row) => {
                            *sum_value_diff_row = SnapshotDiffValues {
                                table_name: sum_value_diff_row.table_name.to_string(),
                                namespace: sum_value_diff_row.namespace.to_string(),
                                first_snapshot_time: sum_value_diff_row.first_snapshot_time,
                                second_snapshot_time: sum_value_diff_row.second_snapshot_time,
                                first_snapshot_value: sum_value_diff_row.first_snapshot_value + value_diff_row.first_snapshot_value,
                                second_snapshot_value: sum_value_diff_row.second_snapshot_value + value_diff_row.second_snapshot_value,
                            }
                        },
                        None => {
                            sum_value_diff.insert((
                                                      hostname_port.to_string(),
                                                      metric_type.to_string(),
                                                      String::from("-"),
                                                      metric_name.to_string()
                                                  ), SnapshotDiffValues {
                                table_name: String::from("-"),
                                namespace: String::from("-"),
                                first_snapshot_time: value_diff_row.first_snapshot_time,
                                second_snapshot_time: value_diff_row.second_snapshot_time,
                                first_snapshot_value: value_diff_row.first_snapshot_value,
                                second_snapshot_value: value_diff_row.second_snapshot_value
                            });
                        }
                    }
                }
            } else {
                match sum_value_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string())) {
                    Some(_sum_value_diff) => {
                        panic!("Error: (sum_value_diff) found second entry for hostname: {}, type: {}, id: {}, name: {}", &hostname_port.clone(), &metric_type.clone(), String::from("-"), &metric_name.clone());
                    },
                    None => {
                        sum_value_diff.insert((
                                                  hostname_port.to_string(),
                                                  metric_type.to_string(),
                                                  String::from("-"),
                                                  metric_name.to_string()
                                              ), SnapshotDiffValues {
                            table_name: String::from("-"),
                            namespace: String::from("-"),
                            first_snapshot_time: value_diff_row.first_snapshot_time,
                            second_snapshot_time: value_diff_row.second_snapshot_time,
                            first_snapshot_value: value_diff_row.first_snapshot_value,
                            second_snapshot_value: value_diff_row.second_snapshot_value,
                        });
                    }
                }
            }
        }
        for ((hostname, metric_type, metric_id, metric_name), value_diff_row) in sum_value_diff {
            if hostname_filter.is_match(&hostname)
                && stat_name_filter.is_match(&metric_name)
                && table_name_filter.is_match(&value_diff_row.table_name) {
                let details = match value_statistic_details_lookup.get(&metric_name.to_string()) {
                    Some(x) => { ValueStatisticDetails { unit: x.unit.to_string(), unit_suffix: x.unit_suffix.to_string(), stat_type: x.stat_type.to_string() } },
                    None => { ValueStatisticDetails { unit: String::from("?"), unit_suffix: String::from("?"), stat_type: String::from("?") } }
                };
                let adaptive_length = if metric_id.len() < 15 { 0 } else { metric_id.len() - 15 };
                if details.stat_type != "gauge"
                    && value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value != 0 {
                    if *details_enable {
                        println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:6} {:>15.3} /s",
                                 hostname,
                                 metric_type,
                                 metric_id.substring(adaptive_length, metric_id.len()),
                                 value_diff_row.namespace,
                                 value_diff_row.table_name,
                                 metric_name,
                                 value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value,
                                 details.unit_suffix,
                                 ((value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value) as f64 / (value_diff_row.second_snapshot_time - value_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000 as f64)
                        );
                    } else {
                        println!("{:20} {:8} {:70} {:15} {:6} {:>15.3} /s",
                                 hostname,
                                 metric_type,
                                 metric_name,
                                 value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value,
                                 details.unit_suffix,
                                 ((value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value) as f64 / (value_diff_row.second_snapshot_time - value_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000 as f64)
                        );
                    }
                }
                if details.stat_type == "gauge"
                    && *gauges_enable {
                    if *details_enable {
                        println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:6} {:+15}",
                                 hostname,
                                 metric_type,
                                 metric_id.substring(adaptive_length, metric_id.len()),
                                 value_diff_row.namespace,
                                 value_diff_row.table_name,
                                 metric_name,
                                 value_diff_row.second_snapshot_value,
                                 details.unit_suffix,
                                 value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value
                        );
                    } else {
                        println!("{:20} {:8} {:70} {:15} {:6} {:+15}",
                                 hostname,
                                 metric_type,
                                 metric_name,
                                 value_diff_row.second_snapshot_value,
                                 details.unit_suffix,
                                 value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value
                        );
                    }
                }
            }
        }
        // countsum_diff
        let countsum_statistic_details_lookup = countsum_create_hashmap();
        let mut sum_countsum_diff: BTreeMap<(String, String, String, String), SnapshotDiffCountSum> = BTreeMap::new();
        for ((hostname_port, metric_type, _metric_id, metric_name), countsum_diff_row) in countsum_diff {
            if metric_type == "table" || metric_type == "tablet" {
                if countsum_diff_row.second_snapshot_total_count > 0 {
                    match sum_countsum_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string())) {
                        Some(sum_countsum_diff_row) => {
                            *sum_countsum_diff_row = SnapshotDiffCountSum {
                                table_name: sum_countsum_diff_row.table_name.to_string(),
                                namespace: sum_countsum_diff_row.namespace.to_string(),
                                first_snapshot_time: sum_countsum_diff_row.first_snapshot_time,
                                second_snapshot_time: sum_countsum_diff_row.second_snapshot_time,
                                second_snapshot_total_count: sum_countsum_diff_row.second_snapshot_total_count + countsum_diff_row.second_snapshot_total_count,
                                second_snapshot_min: 0,
                                second_snapshot_mean: 0.0,
                                second_snapshot_percentile_75: 0,
                                second_snapshot_percentile_95: 0,
                                second_snapshot_percentile_99: 0,
                                second_snapshot_percentile_99_9: 0,
                                second_snapshot_percentile_99_99: 0,
                                second_snapshot_max: 0,
                                second_snapshot_total_sum: sum_countsum_diff_row.second_snapshot_total_sum + countsum_diff_row.second_snapshot_total_sum,
                                first_snapshot_total_count: sum_countsum_diff_row.first_snapshot_total_count + countsum_diff_row.first_snapshot_total_count,
                                first_snapshot_total_sum: sum_countsum_diff_row.first_snapshot_total_sum + countsum_diff_row.first_snapshot_total_sum
                            }
                        },
                        None => {
                            sum_countsum_diff.insert((
                                                      hostname_port.to_string(),
                                                      metric_type.to_string(),
                                                      String::from("-"),
                                                      metric_name.to_string()
                                                  ), SnapshotDiffCountSum {
                                table_name: String::from("-"),
                                namespace: String::from("-"),
                                first_snapshot_time: countsum_diff_row.first_snapshot_time,
                                second_snapshot_time: countsum_diff_row.second_snapshot_time,
                                second_snapshot_total_count: countsum_diff_row.second_snapshot_total_count,
                                second_snapshot_min: 0,
                                second_snapshot_mean: 0.0,
                                second_snapshot_percentile_75: 0,
                                second_snapshot_percentile_95: 0,
                                second_snapshot_percentile_99: 0,
                                second_snapshot_percentile_99_9: 0,
                                second_snapshot_percentile_99_99: 0,
                                second_snapshot_max: 0,
                                second_snapshot_total_sum: countsum_diff_row.second_snapshot_total_sum,
                                first_snapshot_total_count: countsum_diff_row.first_snapshot_total_count,
                                first_snapshot_total_sum: countsum_diff_row.first_snapshot_total_sum
                            });
                        }
                    }
                }
            } else {
                match sum_countsum_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string())) {
                    Some(_sum_countsum_diff_row) => {
                        panic!("Error: (sum_countsum_diff) found second entry for hostname: {}, type: {}, id: {}, name: {}", &hostname_port.clone(), &metric_type.clone(), String::from("-"), &metric_name.clone());
                    },
                    None => {
                        sum_countsum_diff.insert((
                                                     hostname_port.to_string(),
                                                     metric_type.to_string(),
                                                     String::from("-"),
                                                     metric_name.to_string()
                                                 ), SnapshotDiffCountSum {
                            table_name: String::from("-"),
                            namespace: String::from("-"),
                            first_snapshot_time: countsum_diff_row.first_snapshot_time,
                            second_snapshot_time: countsum_diff_row.second_snapshot_time,
                            second_snapshot_total_count: countsum_diff_row.second_snapshot_total_count,
                            second_snapshot_min: 0,
                            second_snapshot_mean: 0.0,
                            second_snapshot_percentile_75: 0,
                            second_snapshot_percentile_95: 0,
                            second_snapshot_percentile_99: 0,
                            second_snapshot_percentile_99_9: 0,
                            second_snapshot_percentile_99_99: 0,
                            second_snapshot_max: 0,
                            second_snapshot_total_sum: countsum_diff_row.second_snapshot_total_sum,
                            first_snapshot_total_count: countsum_diff_row.first_snapshot_total_count,
                            first_snapshot_total_sum: countsum_diff_row.first_snapshot_total_sum
                        });
                    }
                }
            }
        }
        for ((hostname, metric_type, metric_id, metric_name), countsum_diff_row) in sum_countsum_diff {
            if hostname_filter.is_match(&hostname)
                && stat_name_filter.is_match(&metric_name)
                && table_name_filter.is_match(&countsum_diff_row.table_name) {
                let details = match countsum_statistic_details_lookup.get(&metric_name.to_string()) {
                    Some(x) => { CountSumStatisticDetails { unit: x.unit.to_string(), unit_suffix: x.unit_suffix.to_string(), divisor: x.divisor, stat_type: x.stat_type.to_string() } },
                    None => { CountSumStatisticDetails { unit: String::from("?"), unit_suffix: String::from("?"), divisor: 0, stat_type: String::from("?") } }
                };
                let adaptive_length = if metric_id.len() < 15 { 0 } else { metric_id.len() - 15 };
                if countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count != 0 {
                    if *details_enable {
                        println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15}        {:>15.3} /s avg: {:9.0} tot: {:>15.3} {:10}",
                                 hostname,
                                 metric_type,
                                 metric_id.substring(adaptive_length, metric_id.len()),
                                 countsum_diff_row.namespace,
                                 countsum_diff_row.table_name,
                                 metric_name,
                                 countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count,
                                 (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count) as f64 / (countsum_diff_row.second_snapshot_time - countsum_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000 as f64,
                                 ((countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum) / (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count)) as f64,
                                 countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum,
                                 details.unit_suffix
                        );
                    } else {
                        println!("{:20} {:8} {:70} {:15}        {:>15.3} /s avg: {:9.0} tot: {:>15.3} {:10}",
                                 hostname,
                                 metric_type,
                                 metric_name,
                                 countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count,
                                 (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count) as f64 / (countsum_diff_row.second_snapshot_time - countsum_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000 as f64,
                                 ((countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum) / (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count)) as f64,
                                 countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum,
                                 details.unit_suffix
                        );
                    }
                }
            }
        }
    }
    // countsumrows_diff
    for ((hostname, _metric_type, _metric_id, metric_name), countsumrows_diff_row) in countsumrows_diff {
        if hostname_filter.is_match( &hostname)
        && stat_name_filter.is_match(&metric_name) {
            if countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count != 0 {
                println!("{:20} {:70} {:>15} avg: {:>15.3} tot: {:>15.3} ms, avg: {:>15} tot: {:>15} rows",
                    hostname,
                    metric_name,
                    countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count,
                    ((countsumrows_diff_row.second_snapshot_sum as f64 - countsumrows_diff_row.first_snapshot_sum as f64)/1000.0) / (countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count) as f64,
                    (countsumrows_diff_row.second_snapshot_sum as f64 - countsumrows_diff_row.first_snapshot_sum as f64)/1000.0,
                    (countsumrows_diff_row.second_snapshot_rows - countsumrows_diff_row.first_snapshot_rows) / (countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count),
                    countsumrows_diff_row.second_snapshot_rows - countsumrows_diff_row.first_snapshot_rows
                );
            }
        }
    }
}