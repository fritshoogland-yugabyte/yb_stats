extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate csv;

pub mod value_statistic_details;
pub mod countsum_statistic_details;

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
pub mod rpcs;
pub mod pprof;
pub mod mems;

use chrono::{DateTime, Local};
use std::process;
use std::fs;
use std::path::{Path, PathBuf};
use std::env;
use std::io::{stdin, stdout, Write};
use log::*;
use std::sync::Arc;
use crate::metrics::AllStoredMetrics;

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

pub fn read_begin_end_snapshot_from_user( snapshots: &[Snapshot], option_begin: Option<i32>, option_end: Option<i32> ) -> (String, String, Snapshot) {

    //let mut begin_snapshot = String::new();
    let begin_snapshot= match option_begin {
        Some(nr) => nr,
        None => {
            print!("Enter begin snapshot: ");
            let mut snap= String::new();
            let _ = stdout().flush();
            stdin().read_line(&mut snap).expect("Failed to read input.");
            let snap: i32 = snap.trim().parse().expect("Invalid input");
            snap
        }
    };
    let begin_snapshot_row = match snapshots.iter().find(|&row| row.number == begin_snapshot) {
        Some(snapshot_find_result) => snapshot_find_result.clone(),
        None => {
            eprintln!("Fatal: snapshot number {} is not found in the snapshot list", begin_snapshot);
            process::exit(1);
        }
    };

    let end_snapshot = match option_end {
        Some(nr) => nr,
        None => {
            print!("Enter end snapshot: ");
            let mut snap = String::new();
            let _ = stdout().flush();
            stdin().read_line(&mut snap).expect("Failed to read input.");
            let snap: i32 = snap.trim().parse().expect("Invalid input");
            snap
        }
    };
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
    snapshot_comment: Option<String>,
    snapshots: &mut Vec<Snapshot>,
) {
    info!("create_new_snapshot_directory");
    let snapshot_time = Local::now();
    let snapshot_index = &yb_stats_directory.join("snapshot.index");
    let current_snapshot: Snapshot = Snapshot { number: snapshot_number, timestamp: snapshot_time, comment: snapshot_comment.unwrap_or_default() };
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
    hosts: Vec<&'static str>,
    ports: Vec<&'static str>,
    snapshot_comment: Option<String>,
    parallel: usize,
    disable_threads: bool,
) -> i32 {
    let mut snapshots: Vec<Snapshot> = Vec::new();

    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    let snapshot_number = read_snapshot_number(&yb_stats_directory, &mut snapshots);
    info!("using snapshot number: {}", snapshot_number);
    create_new_snapshot_directory(&yb_stats_directory, snapshot_number, snapshot_comment, &mut snapshots);

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
        //let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
        mps.spawn(move |_| {
            //metrics::perform_metrics_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);
            AllStoredMetrics::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel);
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
            statements::perform_statements_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);
        });

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
        mps.spawn(move |_| {
            node_exporter::perform_nodeexporter_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);
        });

        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
        mps.spawn(move |_| {
            entities::perform_entities_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel);
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

    snapshot_number
}



