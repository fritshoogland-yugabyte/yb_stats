//! Snapshot functions
//!
use log::*;
use std::{env, fs, io::{stdin, stdout, Write}, path::Path, sync::Arc, time::Instant};
use chrono::Local;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use crate::{Opts, tablet_server_operations};
use crate::{clocks, entities, gflags, isleader, loglines, masters, mems, memtrackers, metrics, node_exporter, pprof, rpcs, statements, threads, tablet_servers, utility, vars, versions, cluster_config, health_check, table_detail, tablet_detail, tasks, tablet_replication, drives};
use crate::snapshot::Snapshot;

impl Snapshot {
    /// This is a public function to:
    /// - create the yb_stats.snapshots directory (if it exists, it does nothing).
    /// - open the yb_stats.snapshots/snapshot.index if it exists, and read it into a vec of Snapshot.
    /// - if it exists, get the highest snapshot number, otherwise snapshot_number remains 0.
    /// - save a new Snapshot into the vec of Snapshot.
    /// - write the vec of Snapshot to yb_stats.snapshots/snapshot.index.
    /// - create the snapshot directory for the data as yb_stats.snapshots/<snapshot_number>.
    /// - return snapshot_number.
    pub fn insert_new_snapshot_number(
        snapshot_comment: &Option<String>
    ) -> Result<i32>
    {
        info!("read_snapshot_number");
        let mut snapshots: Vec<Snapshot> = Vec::new();
        let mut snapshot_number: i32 = 0;

        // setup yb_stats.snapshots if necessary
        let current_directory = env::current_dir()
            .with_context(|| "Cannot evaluate current working directory" )?;
        let yb_stats_directory = current_directory.join("yb_stats.snapshots");
        // If the &yb_stats_directory does not exist, create it.
        // If it does exist already, nothing happens and continue.
        fs::create_dir_all(&yb_stats_directory)
            .with_context(|| format!("Cannot create directory: {}",&yb_stats_directory.clone().into_os_string().into_string().unwrap()))?;
        // If &yb_stats_directory/snapshot.index exists, read snapshots into snapshots vector,
        // and determine the highest snapshot number, add one and assign it to snapshot_number.
        // If it doesn't exist, snapshot_number 0 is okay.
        let snapshot_index = &yb_stats_directory.join("snapshot.index");
        if Path::new(&snapshot_index).exists() {
            snapshots = Snapshot::read_snapshot_index()?;
            let record_with_highest_snapshot_number = snapshots.iter().max_by_key(|k| k.number).unwrap();
            snapshot_number = record_with_highest_snapshot_number.number + 1;
        }
        // create a new snapshot vector and assign it the new_snapshot, and add it to the snapshots vector.
        let new_snapshot: Snapshot = Snapshot { number: snapshot_number, timestamp: Local::now(), comment: snapshot_comment.clone().unwrap_or_default() };
        snapshots.push(new_snapshot);
        Snapshot::write_snapshot_index(snapshots)?;
        // Create the snapshot number directory in the &yb_stats_directory
        let current_snapshot_directory = &yb_stats_directory.join(snapshot_number.to_string());
        fs::create_dir_all(current_snapshot_directory)
            .with_context(|| format!("Cannot create directory: {}",&current_snapshot_directory.clone().into_os_string().into_string().unwrap()))?;
        Ok(snapshot_number)
    }
    /// This is a private function to read the snapshots index file, and return a vector with the snapshots.
    fn read_snapshot_index(
    ) -> Result<Vec<Snapshot>>
    {
        let mut snapshots: Vec<Snapshot> = Vec::new();
        let current_directory = env::current_dir()
            .with_context(|| "Cannot evaluate current working directory" )?;
        let yb_stats_directory = current_directory.join("yb_stats.snapshots");
        let snapshot_index = &yb_stats_directory.join("snapshot.index");

        let file = fs::File::open(snapshot_index)
            .with_context(|| format!("Error opening file: {}", snapshot_index.display()))?;
        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: Snapshot = row
                .with_context(|| "Error deserialize row")?;
            snapshots.push(data);
        }
        Ok(snapshots)
    }
    /// This is a private function to write the vector to the snapshots index file.
    /// The file gets truncated and overwritten upon write.
    fn write_snapshot_index(
        snapshots: Vec<Snapshot>
    ) -> Result<()>
    {
        let current_directory = env::current_dir()
            .with_context(|| "Cannot evaluate current working directory" )?;

        let yb_stats_directory = current_directory.join("yb_stats.snapshots");
        let snapshot_index = &yb_stats_directory.join("snapshot.index");

        // Open the snapshot.index file, but truncate it and write the new snapshots vector to it.
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(snapshot_index)
            .with_context(|| format!("Cannot create file: {}", snapshot_index.display()))?;
        let mut writer = csv::Writer::from_writer(file);
        for row in snapshots {
            writer.serialize(&row)
                .with_context(|| format!("Unable to serialize: {} {} {}", row.number, row.timestamp, row.comment))?;
        }
        writer.flush()
            .with_context(|| "Error flushing buffer")?;
        Ok(())
    }
    /// This is a public function that reads the snapshots file into a vector and print the contents of it.
    /// The main use is to display the current snapshots to the user.
    pub fn print(
    ) -> Result<()>
    {
        let snapshots = Snapshot::read_snapshot_index()?;
        for row in &snapshots {
            println!("{:>3} {:30} {:50}", row.number, row.timestamp, row.comment);
        }
        Ok(())
    }
    /// This is a public function that validates begin and end provided values,
    /// and if these are not specified are requested interactively,
    /// after which the begin and end snapshot numbers and the struct with the begin snapshot are returned as record.
    ///
    /// If the begin or end value is provided (using the switches `-b`/`--begin` and `-e`/`--end`),
    /// it will take that value and not ask for it.
    ///
    /// Both begin and end snapshots are validated for their existence in the [Snapshot] vector.
    /// Besides the begin and end snapshot values, the struct with the begin [Snapshot] is returned.
    /// The begin [Snapshot] struct is needed for the timestamp.
    pub fn read_begin_end_snapshot_from_user(
        option_begin: Option<i32>,
        option_end: Option<i32>
    ) -> Result<(String, String, Snapshot)>
    {
        let snapshots = Snapshot::read_snapshot_index()?;
        let begin_snapshot= match option_begin {
            Some(nr) => nr,
            None => {
                print!("Enter begin snapshot: ");
                let mut snap= String::new();
                stdout().flush()?;
                stdin().read_line(&mut snap).expect("Failed to read input.");
                let snap: i32 = snap.trim().parse().expect("Invalid input");
                snap
            }
        };
        // begin_snapshot has to exists as row.number.
        // if it does, assign to begin_snapshot_row, otherwise error out.
        let begin_snapshot_row = snapshots.iter()
            .find(|&row| row.number == begin_snapshot)
            .with_context(|| format!("Unable to find begin snapshot number: {}", begin_snapshot))?;
        // if option_end is filled out (Some), use it.
        // if option_end is not filled out (None), read from stdin.
        let end_snapshot = match option_end {
            Some(nr) => nr,
            None => {
                print!("Enter end snapshot: ");
                let mut snap = String::new();
                stdout().flush()?;
                stdin().read_line(&mut snap).expect("Failed to read input.");
                let snap: i32 = snap.trim().parse().expect("Invalid input");
                snap
            }
        };
        // end_snapshot has to exists as row.number, otherwise error out.
        snapshots.iter()
            .find(|&row| row.number == end_snapshot)
            .with_context(|| format!("Unable to find end snapshot number: {}", end_snapshot))?;
        // all information is found and collected, return it to the caller.
        Ok((begin_snapshot.to_string(), end_snapshot.to_string(), begin_snapshot_row.clone()))
    }
}
/// This is the general yb_stat wide save_snapshot_json function.
pub fn save_snapshot_json<T: Serialize>(
    snapshot_number: i32,
    filename: &str,
    vector: Vec<T>,
) -> Result<()>
{
    let current_directory = env::current_dir()?;
    let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(snapshot_number.to_string());

    let filepath = &current_snapshot_directory.join(format!("{}.{}",filename, "json"));
    fs::write(filepath, serde_json::to_string(&vector)
        .with_context(|| "Json serialization error")?
    ).with_context(|| format!("Error saving snapshot: {}", filepath.display()))?;
    Ok(())
}
/// This is the general yb_stat wide read_snapshot_json function.
pub fn read_snapshot_json<T: for<'de> Deserialize<'de>>(
    snapshot_number: &String,
    filename: &str,
) -> Result<Vec<T>>
{
    let current_directory = env::current_dir()?;
    let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(snapshot_number);
    let filepath = &current_snapshot_directory.join(format!("{}.{}", filename, "json"));

    let vector = {
        let read_from_file = fs::read_to_string(filepath)
            .with_context(|| format!("Error reading snapshot: {}", &filepath.display()))?;
        serde_json::from_str(&read_from_file).with_context(|| "Json deserialization error")?
    };
    Ok(vector)
}
/// The function to perform a general snapshot resulting in JSON files.
pub async fn perform_snapshot(
    hosts: Vec<&'static str>,
    ports: Vec<&'static str>,
    parallel: usize,
    options: &Opts,
) -> Result<()> {
    info!("begin snapshot");
    let timer = Instant::now();

    let snapshot_number = Snapshot::insert_new_snapshot_number(&options.snapshot_comment)?;
    info!("using snapshot number: {}", snapshot_number);

    let arc_hosts = Arc::new(hosts);
    let arc_ports = Arc::new(ports);
    let arc_extra_data = Arc::new(options.extra_data);

    let mut handles = vec![];

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        metrics::AllMetricEntity::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        statements::AllStatements::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        node_exporter::AllNodeExporter::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        isleader::AllIsLeader::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        entities::AllEntities::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        masters::Masters::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        tablet_servers::AllTabletServers::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        vars::AllVars::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        versions::AllVersions::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        gflags::AllGFlags::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    if !options.disable_threads {
        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let handle = tokio::spawn(async move {
            threads::AllThreads::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
        });
        handles.push(handle);
    };

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        memtrackers::AllMemTrackers::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        loglines::AllLogLines::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        rpcs::AllRpcs::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        pprof::Pprof::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        mems::Mems::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        clocks::AllClocks::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        cluster_config::AllSysClusterConfigEntryPB::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        health_check::AllHealthCheck::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let arc_extra_data_clone = arc_extra_data.clone();
    let handle = tokio::spawn(async move {
        table_detail::AllTables::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel, &arc_extra_data_clone).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let arc_extra_data_clone = arc_extra_data.clone();
    let handle = tokio::spawn(async move {
        tablet_detail::AllTablets::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel, &arc_extra_data_clone).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        tasks::AllTasks::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        tablet_replication::AllTabletReplication::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        tablet_server_operations::AllOperations::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        drives::AllDrives::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    for handle in handles {
        handle.await.unwrap();
    }

    if !options.silent {
        println!("snapshot number {}", snapshot_number);
    }

    info!("end snapshot: {:?}", timer.elapsed());
    Ok(())
}

/// This function shows the difference report for the snapshot data that allows to show a difference:
/// - metrics (value, coarse_histogram/countsum, ysql/countsumrows)
/// - statements (ysql)
/// - node_exporter
/// - entities (read via master leader)
/// - masters (read via master leader)
/// - tablet servers (read via master leader)
/// - vars
/// - versions
/// - health check (read via master leader)
pub async fn snapshot_diff(
    options: &Opts,
) -> Result<()>
{
    info!("snapshot diff");
    if options.begin.is_none() || options.end.is_none() {
        Snapshot::print()?;
    }
    if options.snapshot_list { return Ok(()) };

    let hostname_filter = utility::set_regex(&options.hostname_match);
    let stat_name_filter = utility::set_regex(&options.stat_name_match);
    let table_name_filter = utility::set_regex(&options.table_name_match);

    let (begin_snapshot, end_snapshot, begin_snapshot_row) = Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end)?;

    let metrics_diff = metrics::MetricEntityDiff::snapshot_diff(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp, &options.details_enable)?;
    metrics_diff.print(&hostname_filter, &stat_name_filter, &table_name_filter, &options.details_enable, &options.gauges_enable).await;

    let statements_diff = statements::StatementsDiff::snapshot_diff(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp)?;
    statements_diff.print(&hostname_filter, options.sql_length).await;

    let nodeexporter_diff = node_exporter::NodeExporterDiff::snapshot_diff(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp)?;
    nodeexporter_diff.print(&hostname_filter, &stat_name_filter, &options.gauges_enable, &options.details_enable);

    let entities_diff = entities::EntitiesDiff::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    entities_diff.print();

    let masters_diff = masters::MastersDiff::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    masters_diff.print();

    let tabletservers_diff = tablet_servers::TabletServersDiff::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    tabletservers_diff.print();

    let vars_diff = vars::VarsDiff::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    vars_diff.print();

    let versions_diff = versions::VersionsDiff::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    versions_diff.print(&hostname_filter);

    let healthcheck_diff = health_check::HealthCheckDiff::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    healthcheck_diff.print();

    Ok(())
}


/// This function shows the difference report for the snapshot data that allows to show a difference:
/// - entities (read via master leader)
/// - masters (read via master leader)
/// - tablet servers (read via master leader)
/// - vars
/// - versions
/// - health check (read via master leader)
///
/// The purpose of this function is to quickly determine significant changes for a cluster, not to
/// look into performance.
pub async fn snapshot_nonmetrics_diff(
    options: &Opts,
) -> Result<()>
{
    info!("snapshot diff");
    if options.begin.is_none() || options.end.is_none() {
        Snapshot::print()?;
    }
    if options.snapshot_list { return Ok(()) };

    let hostname_filter = utility::set_regex(&options.hostname_match);

    let (begin_snapshot, end_snapshot, _) = Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end)?;

    let entities_diff = entities::EntitiesDiff::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    entities_diff.print();

    let masters_diff = masters::MastersDiff::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    masters_diff.print();

    let tabletservers_diff = tablet_servers::TabletServersDiff::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    tabletservers_diff.print();

    let vars_diff = vars::VarsDiff::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    vars_diff.print();

    let versions_diff = versions::VersionsDiff::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    versions_diff.print(&hostname_filter);

    let healthcheck_diff = health_check::HealthCheckDiff::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    healthcheck_diff.print();

    Ok(())
}

/// Take "adhoc" (in memory) snapshots of metrics only:
/// - metrics (value, coarse_histogram/countsum, ysql/countsumrows)
///
/// The idea here to reduce output when it's know lots of 'entities'/objects are created, or other
/// changed would give too much output to be useful.
pub async fn adhoc_metrics_diff(
    hosts: Vec<&'static str>,
    ports: Vec<&'static str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    info!("ad-hoc metrics diff first snapshot begin");
    let timer = Instant::now();

    let stat_name_filter = utility::set_regex(&options.stat_name_match);
    let hostname_filter = utility::set_regex(&options.hostname_match);
    let table_name_filter = utility::set_regex(&options.table_name_match);

    let first_snapshot_time = Local::now();

    let metrics = Arc::new(Mutex::new(metrics::MetricEntityDiff::new()));

    let hosts = Arc::new(hosts);
    let ports = Arc::new(ports);

    let mut handles = vec![];

    let clone_metrics = metrics.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let details_enable = options.details_enable;

    let handle = tokio::spawn(async move {
        clone_metrics.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel, details_enable).await;
    });
    handles.push(handle);

    for handle in handles {
        handle.await.unwrap();
    }
    info!("ad-hoc metrics diff first snapshot end: {:?}", timer.elapsed());

    println!("Begin ad-hoc in-memory snapshot created, press enter to create end snapshot for difference calculation.");
    let mut input = String::new();
    stdin().read_line(&mut input).expect("failed");

    info!("ad-hoc metrics diff second snapshot begin");
    let timer = Instant::now();

    let second_snapshot_time = Local::now();

    let mut handles = vec![];

    let clone_metrics = metrics.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let details_enable = options.details_enable;

    let handle = tokio::spawn(async move {
        clone_metrics.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel, &first_snapshot_time, details_enable).await;
    });
    handles.push(handle);

    for handle in handles {
        handle.await.unwrap();
    }

    info!("ad-hoc metrics diff second snapshot end: {:?}", timer.elapsed());

    println!("Time between snapshots: {:8.3} seconds", (second_snapshot_time - first_snapshot_time).num_milliseconds() as f64 / 1000_f64);
    metrics.lock().await.print(&hostname_filter, &stat_name_filter, &table_name_filter, &options.details_enable, &options.gauges_enable).await;

    Ok(())
}

/// Take "adhoc" (in memory) snapshots of node_exporter only:
///
/// The idea here to reduce output when it's know lots of 'entities'/objects are created, or other
/// changed would give too much output to be useful.
pub async fn adhoc_node_exporter_diff(
    hosts: Vec<&'static str>,
    ports: Vec<&'static str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    info!("ad-hoc node_exporter diff first snapshot begin");
    let timer = Instant::now();

    let stat_name_filter = utility::set_regex(&options.stat_name_match);
    let hostname_filter = utility::set_regex(&options.hostname_match);

    let first_snapshot_time = Local::now();

    let node_exporter = Arc::new(Mutex::new(node_exporter::NodeExporterDiff::new()));

    let hosts = Arc::new(hosts);
    let ports = Arc::new(ports);

    let mut handles = vec![];

    let clone_node_exporter = node_exporter.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_node_exporter.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    for handle in handles {
        handle.await.unwrap();
    }
    info!("ad-hoc node_exporter diff first snapshot end: {:?}", timer.elapsed());

    println!("Begin ad-hoc in-memory snapshot created, press enter to create end snapshot for difference calculation.");
    let mut input = String::new();
    stdin().read_line(&mut input).expect("failed");

    info!("ad-hoc node_exporter diff second snapshot begin");
    let timer = Instant::now();

    let second_snapshot_time = Local::now();

    let mut handles = vec![];

    let clone_node_exporter = node_exporter.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_node_exporter.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel, &first_snapshot_time).await;
    });
    handles.push(handle);

    for handle in handles {
        handle.await.unwrap();
    }

    info!("ad-hoc node_exporter diff second snapshot end: {:?}", timer.elapsed());

    println!("Time between snapshots: {:8.3} seconds", (second_snapshot_time - first_snapshot_time).num_milliseconds() as f64 / 1000_f64);
    node_exporter.lock().await.print(&hostname_filter, &stat_name_filter, &options.gauges_enable, &options.details_enable);

    Ok(())
}

pub async fn adhoc_nonmetrics_diff(
    hosts: Vec<&'static str>,
    ports: Vec<&'static str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    let timer = Instant::now();

    let _stat_name_filter = utility::set_regex(&options.stat_name_match);
    let hostname_filter = utility::set_regex(&options.hostname_match);
    let _table_name_filter = utility::set_regex(&options.table_name_match);

    let first_snapshot_time = Local::now();

    let entities = Arc::new(Mutex::new(entities::EntitiesDiff::new()));
    let masters = Arc::new(Mutex::new(masters::MastersDiff::new()));
    let tablet_servers = Arc::new(Mutex::new(tablet_servers::TabletServersDiff::new()));
    let versions = Arc::new(Mutex::new(versions::VersionsDiff::new()));
    let vars = Arc::new(Mutex::new(vars::VarsDiff::new()));
    let health_check = Arc::new(Mutex::new(health_check::HealthCheckDiff::new()));

    let hosts = Arc::new(hosts);
    let ports = Arc::new(ports);
    let _details_enable = options.details_enable;

    let mut handles = vec![];

    let clone_entities = entities.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_entities.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_masters = masters.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_masters.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_tablet_servers = tablet_servers.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_tablet_servers.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_vars = vars.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_vars.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_versions = versions.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_versions.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_health_check = health_check.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_health_check.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);


    for handle in handles {
        handle.await.unwrap();
    }
    info!("ad-hoc metrics diff first snapshot end: {:?}", timer.elapsed());

    println!("Begin ad-hoc in-memory snapshot created, press enter to create end snapshot for difference calculation.");
    let mut input = String::new();
    stdin().read_line(&mut input).expect("failed");

    let timer = Instant::now();

    let second_snapshot_time = Local::now();
    let mut handles = vec![];

    let clone_entities = entities.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_entities.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_masters = masters.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_masters.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_tablet_servers = tablet_servers.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_tablet_servers.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_vars = vars.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_vars.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_versions = versions.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_versions.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_health_check = health_check.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_health_check.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    for handle in handles {
        handle.await.unwrap();
    }
    info!("ad-hoc metrics diff second snapshot end: {:?}", timer.elapsed());

    println!("Time between snapshots: {:8.3} seconds", (second_snapshot_time - first_snapshot_time).num_milliseconds() as f64 / 1000_f64);
    entities.lock().await.print();
    masters.lock().await.print();
    tablet_servers.lock().await.print();
    vars.lock().await.print();
    versions.lock().await.print(&hostname_filter);
    health_check.lock().await.print();

    Ok(())
}
/// This function shows the difference report for the adhoc (in memory) snapshot data that allows to show a difference:
/// - metrics (value, coarse_histogram/countsum, ysql/countsumrows)
/// - statements (ysql)
/// - node_exporter
/// - entities (read via master leader)
/// - masters (read via master leader)
/// - tablet servers (read via master leader)
/// - vars
/// - versions
pub async fn adhoc_diff(
    hosts: Vec<&'static str>,
    ports: Vec<&'static str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    info!("ad-hoc mode first snapshot begin");
    let timer = Instant::now();

    let stat_name_filter = utility::set_regex(&options.stat_name_match);
    let hostname_filter = utility::set_regex(&options.hostname_match);
    let table_name_filter = utility::set_regex(&options.table_name_match);

    let first_snapshot_time = Local::now();

    let metrics = Arc::new(Mutex::new(metrics::MetricEntityDiff::new()));
    let statements = Arc::new(Mutex::new(statements::StatementsDiff::new()));
    let node_exporter = Arc::new(Mutex::new(node_exporter::NodeExporterDiff::new()));
    let entities = Arc::new(Mutex::new(entities::EntitiesDiff::new()));
    let masters = Arc::new(Mutex::new(masters::MastersDiff::new()));
    let tablet_servers = Arc::new(Mutex::new(tablet_servers::TabletServersDiff::new()));
    let versions = Arc::new(Mutex::new(versions::VersionsDiff::new()));
    let vars = Arc::new(Mutex::new(vars::VarsDiff::new()));
    let health_check = Arc::new(Mutex::new(health_check::HealthCheckDiff::new()));

    let hosts = Arc::new(hosts);
    let ports = Arc::new(ports);
    let details_enable = options.details_enable;

    let mut handles = vec![];

    let clone_metrics = metrics.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_metrics.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel, details_enable).await;
    });
    handles.push(handle);

    let clone_statements = statements.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_statements.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_node_exporter = node_exporter.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_node_exporter.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_entities = entities.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_entities.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_masters = masters.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_masters.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_tablet_servers = tablet_servers.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_tablet_servers.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_vars = vars.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_vars.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_versions = versions.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_versions.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_health_check = health_check.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_health_check.lock().await.adhoc_read_first_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    for handle in handles {
        handle.await.unwrap();
    }
    info!("ad-hoc metrics diff first snapshot end: {:?}", timer.elapsed());

    println!("Begin ad-hoc in-memory snapshot created, press enter to create end snapshot for difference calculation.");
    let mut input = String::new();
    stdin().read_line(&mut input).expect("failed");

    info!("ad-hoc metrics diff second snapshot begin");
    let timer = Instant::now();

    let second_snapshot_time = Local::now();
    let mut handles = vec![];

    let clone_metrics = metrics.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let details_enable = options.details_enable;

    let handle = tokio::spawn(async move {
        clone_metrics.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel, &first_snapshot_time, details_enable).await;
    });
    handles.push(handle);

    let clone_statements = statements.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_statements.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel, &first_snapshot_time).await;
    });
    handles.push(handle);

    let clone_node_exporter = node_exporter.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_node_exporter.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel, &first_snapshot_time).await;
    });
    handles.push(handle);

    let clone_entities = entities.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_entities.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_masters = masters.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_masters.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_tablet_servers = tablet_servers.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_tablet_servers.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_vars = vars.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_vars.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_versions = versions.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_versions.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    let clone_health_check = health_check.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_health_check.lock().await.adhoc_read_second_snapshot(&clone_hosts, &clone_ports, parallel).await;
    });
    handles.push(handle);

    for handle in handles {
        handle.await.unwrap();
    }
    info!("ad-hoc metrics diff second snapshot end: {:?}", timer.elapsed());

    println!("Time between snapshots: {:8.3} seconds", (second_snapshot_time - first_snapshot_time).num_milliseconds() as f64 / 1000_f64);
    metrics.lock().await.print(&hostname_filter, &stat_name_filter, &table_name_filter, &options.details_enable, &options.gauges_enable).await;
    statements.lock().await.print(&hostname_filter, options.sql_length).await;
    node_exporter.lock().await.print(&hostname_filter, &stat_name_filter, &options.gauges_enable, &options.details_enable);
    entities.lock().await.print();
    masters.lock().await.print();
    tablet_servers.lock().await.print();
    vars.lock().await.print();
    versions.lock().await.print(&hostname_filter);
    health_check.lock().await.print();

    Ok(())
}
