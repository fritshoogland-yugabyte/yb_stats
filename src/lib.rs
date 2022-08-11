extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate csv;

pub mod value_statistic_details;
//use value_statistic_details::{ValueStatisticDetails, value_create_hashmap};
pub mod countsum_statistic_details;
//use countsum_statistic_details::{CountSumStatisticDetails, countsum_create_hashmap};

pub mod threads;
pub mod memtrackers;
pub mod gflags;
pub mod loglines;
pub mod versions;
pub mod statements;
pub mod metrics;
pub mod node_exporter;
pub mod entities;
pub mod masters;
//pub mod rpcs;

use chrono::{DateTime, Local};
use std::process;
use std::fs;
use std::path::{Path, PathBuf};
use std::env;
use std::io::{stdin, stdout, Write};
use log::*;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Snapshot {
    pub number: i32,
    pub timestamp: DateTime<Local>,
    pub comment: String,
}

#[allow(clippy::ptr_arg)]
pub fn read_snapshots_from_file( yb_stats_directory: &PathBuf ) -> Vec<Snapshot> {

    let mut snapshots: Vec<Snapshot> = Vec::new();
    let snapshot_index = &yb_stats_directory.join("snapshot.index");

    let file = fs::File::open(&snapshot_index)
        .unwrap_or_else(|e| {
            error!("Fatal: error opening file {}: {}", &snapshot_index.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: Snapshot = row.unwrap();
        let _ = &snapshots.push(data);
    }
    snapshots
}

pub fn read_begin_end_snapshot_from_user( snapshots: &[Snapshot] ) -> (String, String, Snapshot) {

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

fn read_snapshot_number(
    yb_stats_directory: &PathBuf,
    snapshots: &mut Vec<Snapshot>
) -> i32 {
    info!("read_snapshot_number");
    let mut snapshot_number: i32 = 0;
    fs::create_dir_all(&yb_stats_directory)
        .unwrap_or_else(|e| {
            error!("Fatal: error creating directory {}: {}", &yb_stats_directory.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });

    let snapshot_index = &yb_stats_directory.join("snapshot.index");
    if Path::new(&snapshot_index).exists() {
        let file = fs::File::open(&snapshot_index)
            .unwrap_or_else(|e| {
                error!("Fatal: error opening file {}: {}", &snapshot_index.clone().into_os_string().into_string().unwrap(), e);
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
    snapshot_number
}

#[allow(clippy::ptr_arg)]
fn create_new_snapshot_directory(
    yb_stats_directory: &PathBuf,
    snapshot_number: i32,
    snapshot_comment: String,
    snapshots: &mut Vec<Snapshot>,
) {
    info!("create_new_snapshot_directory");
    let snapshot_time = Local::now();
    let snapshot_index = &yb_stats_directory.join("snapshot.index");
    let current_snapshot: Snapshot = Snapshot { number: snapshot_number, timestamp: snapshot_time, comment: snapshot_comment };
    let _ = &snapshots.push(current_snapshot);
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&snapshot_index)
        .unwrap_or_else(|e| {
            error!("Fatal: error writing {}: {}", &snapshot_index.clone().into_os_string().into_string().unwrap(), e);
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
            error!("Fatal: error creating directory {}: {}", &current_snapshot_directory.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
}

pub fn perform_snapshot(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    snapshot_comment: String,
    parallel: usize,
    disable_threads: bool,
) -> i32 {
    let mut snapshots: Vec<Snapshot> = Vec::new();

    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    let snapshot_number = read_snapshot_number(&yb_stats_directory, &mut snapshots);
    info!("using snapshot number: {}", snapshot_number);
    create_new_snapshot_directory(&yb_stats_directory, snapshot_number, snapshot_comment, &mut snapshots);

    metrics::perform_metrics_snapshot(&hosts, &ports, snapshot_number, &yb_stats_directory, parallel);
    gflags::perform_gflags_snapshot(&hosts, &ports, snapshot_number, &yb_stats_directory, parallel);
    if ! disable_threads { threads::perform_threads_snapshot(&hosts, &ports, snapshot_number, &yb_stats_directory, parallel) };
    memtrackers::perform_memtrackers_snapshot(&hosts, &ports, snapshot_number, &yb_stats_directory, parallel);
    loglines::perform_loglines_snapshot(&hosts, &ports, snapshot_number, &yb_stats_directory, parallel);
    versions::perform_versions_snapshot(&hosts, &ports, snapshot_number, &yb_stats_directory, parallel);
    statements::perform_statements_snapshot(&hosts, &ports, snapshot_number, &yb_stats_directory, parallel);
    node_exporter::perform_nodeexporter_snapshot(&hosts, &ports, snapshot_number, &yb_stats_directory, parallel);
    entities::perform_entities_snapshot(&hosts, &ports, snapshot_number, &yb_stats_directory, parallel);
    masters::perform_masters_snapshot(&hosts, &ports, snapshot_number, &yb_stats_directory, parallel);
    //rpcs::perform_rpcs_snapshot(&hosts, &ports, snapshot_number, &yb_stats_directory, parallel);

    snapshot_number
}



