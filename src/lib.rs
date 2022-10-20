//! This is the main crate of yb_stats: a utility to extract all possible data from a YugabyteDB cluster.
#![allow(rustdoc::private_intra_doc_links)]

extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate csv;

pub mod value_statistic_details;
pub mod countsum_statistic_details;
pub mod statements;

pub mod threads;
pub mod memtrackers;
pub mod gflags;
pub mod loglines;
pub mod versions;
pub mod node_exporter;
pub mod entities;
pub mod masters;
pub mod rpcs;
pub mod pprof;
pub mod mems;
pub mod snapshot;
pub mod metrics;

use std::env;
use log::*;
use std::sync::Arc;
pub use metrics::{AllStoredMetrics, SnapshotDiffBtreeMaps, read_metrics, add_to_metric_vectors};
pub use snapshot::Snapshot;

/// This is the function that performs all snapshots for all different sources of information, and returns the snapshot number.
pub fn perform_snapshot(
    hosts: Vec<&'static str>,
    ports: Vec<&'static str>,
    snapshot_comment: Option<String>,
    parallel: usize,
    disable_threads: bool,
) -> i32 {

    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    let snapshot_number= Snapshot::insert_new_snapshot_number(snapshot_comment);
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



