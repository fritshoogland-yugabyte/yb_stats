//! Utilities
use port_scanner::scan_port_addr;
use log::*;
use std::{time::Instant, env, sync::Arc, fs, process, collections::HashMap, io::{Write, stdin}};
use chrono::Local;
use anyhow::{Result, Context};
use regex::Regex;
use tokio::sync::Mutex;
use crate::snapshot;
use crate::{metrics, statements, node_exporter, isleader, entities, masters, tservers, vars, versions, gflags, memtrackers, loglines, rpcs, pprof, mems, clocks, threads};


// This reads the constant set in main.rs.
// This probably needs to be made better, and user settable.
use crate::ACCEPT_INVALID_CERTS;

// the scan routine for an existing host:port combination.
// this currently uses port_scanner, but can be slow (3s).
// qscan crate?
pub fn scan_host_port(
    host: &str,
    port: &str,
) -> bool
{
    if ! scan_port_addr( format!("{}:{}", host, port)) {
        warn!("Port scanner: hostname:port {}:{} cannot be reached, skipping",host ,port);
        false
    } else {
        true
    }
}

pub fn http_get(
    host: &str,
    port: &str,
    url: &str,
) -> String
{
    if let Ok(data_from_web_request) = reqwest::blocking::Client::builder()
        .danger_accept_invalid_certs(ACCEPT_INVALID_CERTS)
        .build()
        .unwrap()
        .get(format!("http://{}:{}/{}", host, port, url))
        .send()
    {
        if ! &data_from_web_request.status().is_success()
        {
            debug!("Non success response: {}:{}/{} = {}", host, port, url, &data_from_web_request.status());
        }
        else
        {
           debug!("Success response: {}:{}/{} = {}", host, port, url, &data_from_web_request.status());
        }
        data_from_web_request.text().unwrap()
    } else {
        debug!("Non-Ok success response: {}:{}/{}", host, port, url);
        String::new()
    }
}

/// The function to perform a snapshot resulting in CSV files.
pub async fn perform_snapshot(
    hosts: Vec<&'static str>,
    ports: Vec<&'static str>,
    snapshot_comment: Option<String>,
    parallel: usize,
    disable_threads: bool,
    silent: bool,
) -> Result<()> {
    info!("begin snapshot");
    let timer = Instant::now();

    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    let snapshot_number = snapshot::Snapshot::insert_new_snapshot_number(snapshot_comment)?;
    info!("using snapshot number: {}", snapshot_number);

    let arc_hosts = Arc::new(hosts);
    let arc_ports = Arc::new(ports);
    let arc_yb_stats_directory = Arc::new(yb_stats_directory);

    let mut handles = vec![];

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        metrics::AllStoredMetrics::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        statements::AllStoredStatements::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        node_exporter::AllStoredNodeExporterValues::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        isleader::AllStoredIsLeader::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        entities::AllStoredEntities::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        masters::AllStoredMasters::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        tservers::AllStoredTabletServers::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        vars::AllStoredVars::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        versions::AllStoredVersions::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
    let handle = tokio::spawn(async move {
        gflags::perform_gflags_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel).await;
    });
    handles.push(handle);

    if !disable_threads {
        let arc_hosts_clone = arc_hosts.clone();
        let arc_ports_clone = arc_ports.clone();
        let handle = tokio::spawn(async move {
            threads::AllStoredThreads::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
        });
        handles.push(handle);
    };

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        memtrackers::AllStoredMemTrackers::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        loglines::perform_loglines_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        rpcs::perform_rpcs_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
    let handle = tokio::spawn(async move {
        pprof::perform_pprof_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let arc_yb_stats_directory_clone = arc_yb_stats_directory.clone();
    let handle = tokio::spawn(async move {
        mems::perform_mems_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, &arc_yb_stats_directory_clone, parallel).await.unwrap();
    });
    handles.push(handle);

    let arc_hosts_clone = arc_hosts.clone();
    let arc_ports_clone = arc_ports.clone();
    let handle = tokio::spawn(async move {
        clocks::AllStoredClocks::perform_snapshot(&arc_hosts_clone, &arc_ports_clone, snapshot_number, parallel).await.unwrap();
    });
    handles.push(handle);

    for handle in handles {
        handle.await.unwrap();
    }

    if !silent {
        println!("snapshot number {}", snapshot_number);
    }

    info!("end snapshot: {:?}", timer.elapsed());
    Ok(())
}

use crate::DEFAULT_HOSTS;

pub fn set_hosts(
    option: &Option<String>,
    changed_options: &mut HashMap<&str, String>,
) -> Vec<&'static str>
{
    // is --hosts/-H set?
    let hosts_string = if option.is_some() {
        info!("hosts argument set: using: {}", &option.as_ref().unwrap());
        // insert into changed_options to be written later on.
        changed_options.insert("YBSTATS_HOSTS", option.as_ref().unwrap().to_string());
        // set hosts_string to the set hosts.
        option.clone().unwrap()
    } else {
        // is the environment variable YBSTATS_HOSTS set (via dotenv().ok())?
        match env::var("YBSTATS_HOSTS") {
            Ok(set_var) => {
                info!("hosts not set: set via .env: YBSTATS_HOSTS: {}", set_var);
                changed_options.insert("YBSTATS_HOSTS", set_var.to_owned());
                // return the hosts set in YBSTATS_HOSTS in .env
                set_var
            }
            Err(_e) => {
                info!("hosts not set: and not set via .env: using DEFAULT_HOSTS: {}", DEFAULT_HOSTS.to_string());
                // return the default set ones.
                DEFAULT_HOSTS.to_string()
            }
        }
    };
    let static_hosts: &'static str = Box::leak(hosts_string.into_boxed_str());
    let hosts: Vec<&'static str> = static_hosts.split(',').collect();
    hosts
}

use crate::DEFAULT_PORTS;

pub fn set_ports(
    option: &Option<String>,
    changed_options: &mut HashMap<&str, String>,
) -> Vec<&'static str>
{
    // is --ports/-p set?
    let ports_string = if option.is_some() {
        info!("ports argument set: using: {}", &option.as_ref().unwrap());
        // insert into changed_options to be written later on.
        changed_options.insert("YBSTATS_PORTS", option.as_ref().unwrap().to_string());
        // set ports_string to the set ports.
        option.clone().unwrap()
    } else {
        // is the environment variable YBSTSTATS_PORTS set (via dotenv().ok())?
        match env::var("YBSTATS_PORTS") {
            Ok(set_var) => {
                info!("ports not set: set via .env: YBSTATS_PORTS: {}", set_var);
                changed_options.insert("YBSTATS_PORTS", set_var.to_owned());
                // return the ports in YBSTATS_PORTS in .env
                set_var
            }
            Err(_e) => {
                info!("ports not set: and not set via .env: using DEFAULT_PORTS: {}", DEFAULT_PORTS.to_string());
                // return the default set ones.
                DEFAULT_PORTS.to_string()
            }
        }
    };
    let static_ports: &'static str = Box::leak(ports_string.into_boxed_str());
    let ports: Vec<&'static str> = static_ports.split(',').collect();
    ports
}

use crate::DEFAULT_PARALLEL;

pub fn set_parallel(
    option: &Option<String>,
    changed_options: &mut HashMap<&str, String>,
) -> usize
{
    // is --parallel set?
    let parallel_string = if option.is_some() {
        info!("parallel argument set: using: {}", &option.as_ref().unwrap());
        // insert into changed_options to be written later on.
        changed_options.insert("YBSTATS_PARALLEL", option.as_ref().unwrap().to_string());
        // set parallel_string to the set parallel
        option.clone().unwrap()
    } else {
        // is the environment variable YBSTATS_PARALLEL set (via dotenv().ok())?
        match env::var("YBSTATS_PARALLEL") {
            Ok(set_var) => {
                info!("parallel not set: set via .env: YBSTATS_PARALLEL: {}", set_var);
                changed_options.insert("YBSTATS_PARALLEL", set_var.to_owned());
                // return parallel setting in YBSTATS_PARALLEL in .env
                set_var
            }
            Err(_e) => {
                info!("parallel not set: and not set via .env: using DEFAULT_PARALLEL: {}", DEFAULT_PARALLEL.to_string());
                // return the default setting.
                DEFAULT_PARALLEL.to_string()
            }
        }
    };
    let parallel: usize = parallel_string.parse().unwrap();
    parallel
}

pub fn set_regex(
    regex: &Option<String>,
) -> Regex
{
    match regex {
        Some(regex) => Regex::new(regex.as_str()).unwrap(),
        None => Regex::new(".*").unwrap(),
    }
}

pub fn dotenv_writer(
    write_dotenv: bool,
    changed_options: HashMap<&str, String>,
) -> Result<()>
{
    if !changed_options.is_empty() && write_dotenv {
        info!("Writing .env file");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(".env")
            .with_context(|| format!("Error writing .env file: .env"))?;

        //let writer = BufWriter::new(file);
        for (key, value) in changed_options {
            //file.write_all(format!("{}={}\n", key, value).as_bytes()).unwrap();
            file.write_all(format!("{}={}\n", key, value).as_bytes())?;
            info!("{}={}", key, value);
        }
        //writer.flush().unwrap();
    }
    Ok(())
}

use crate::Opts;

pub async fn snapshot_diff(
    options: &Opts,
    hostname_filter: &Regex,
    stat_name_filter: &Regex,
    table_name_filter: &Regex,
) -> Result<()>
{
    info!("snapshot diff");
    if options.begin.is_none() || options.end.is_none() {
        snapshot::Snapshot::print()?;
    }
    if options.snapshot_list { process::exit(0) };

    let (begin_snapshot, end_snapshot, begin_snapshot_row) = snapshot::Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end)?;

    let metrics_diff = metrics::SnapshotDiffBTreeMapsMetrics::snapshot_diff(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp)?;
    metrics_diff.print(&hostname_filter, &stat_name_filter, &table_name_filter, &options.details_enable, &options.gauges_enable).await;

    let statements_diff = statements::SnapshotDiffBTreeMapStatements::snapshot_diff(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp)?;
    statements_diff.print(&hostname_filter, options.sql_length).await;

    let nodeexporter_diff = node_exporter::SnapshotDiffBTreeMapNodeExporter::snapshot_diff(&begin_snapshot, &end_snapshot, &begin_snapshot_row.timestamp)?;
    nodeexporter_diff.print(&hostname_filter, &stat_name_filter, &options.gauges_enable, &options.details_enable);

    let entities_diff = entities::SnapshotDiffBTreeMapsEntities::snapshot_diff(&begin_snapshot, &end_snapshot, &options.details_enable)?;
    entities_diff.print();

    let masters_diff = masters::SnapshotDiffBTreeMapsMasters::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    masters_diff.print();

    let tabletservers_diff = tservers::SnapshotDiffBTreeMapsTabletServers::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    tabletservers_diff.print();

    let vars_diff = vars::SnapshotDiffBTreeMapsVars::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    vars_diff.print();

    let versions_diff = versions::SnapshotDiffBTreeMapsVersions::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    versions_diff.print(&hostname_filter);

    Ok(())
}

pub async fn entity_diff(
    options: &Opts,
) -> Result<()>
{
    info!("entity diff");
    if options.begin.is_none() || options.end.is_none() {
        snapshot::Snapshot::print()?;
    }
    if options.snapshot_list { process::exit(0) };

    let (begin_snapshot, end_snapshot, _begin_snapshot_row) = snapshot::Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end)?;
    let entity_diff = entities::SnapshotDiffBTreeMapsEntities::snapshot_diff(&begin_snapshot, &end_snapshot, &options.details_enable)?;
    entity_diff.print();

    Ok(())
}

pub async fn masters_diff(
    options: &Opts,
) -> Result<()>
{
    info!("masters diff");

    if options.begin.is_none() || options.end.is_none() {
        snapshot::Snapshot::print()?;
    }
    if options.snapshot_list { process::exit(0) };

    let (begin_snapshot, end_snapshot, _begin_snapshot_row) = snapshot::Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end)?;
    let masters_diff = masters::SnapshotDiffBTreeMapsMasters::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    masters_diff.print();

    Ok(())
}

pub async fn versions_diff(
    options: &Opts,
    hostname_filter: &Regex,
) -> Result<()>
{
    info!("versions diff");

    if options.begin.is_none() || options.end.is_none() {
        snapshot::Snapshot::print()?;
    }
    if options.snapshot_list { process::exit(0) };

    let (begin_snapshot, end_snapshot, _begin_snapshot_row) = snapshot::Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end)?;
    let versions_diff = versions::SnapshotDiffBTreeMapsVersions::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    versions_diff.print(&hostname_filter);

    Ok(())
}

pub async fn print_memtrackers(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
    hostname_filter: &Regex,
    stat_name_filter: &Regex,
) -> Result<()>
{
    match options.print_memtrackers.as_ref().unwrap() {

        Some(snapshot_number) => {

            let mut allstoredmemtrackers = memtrackers::AllStoredMemTrackers::new();
            allstoredmemtrackers.stored_memtrackers = snapshot::read_snapshot(&snapshot_number, "memtrackers")?;

            allstoredmemtrackers.print(&hostname_filter, &stat_name_filter)?;

        },
        None => {

            let allstoredmemtrackers = memtrackers::AllStoredMemTrackers::read_memtrackers(&hosts, &ports, parallel).await;
            allstoredmemtrackers.print(&hostname_filter, &stat_name_filter)?;

        },

    }
    Ok(())
}

pub async fn print_version(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
    hostname_filter: &Regex,
) -> Result<()>
{
    match options.print_version.as_ref().unwrap() {
        Some(snapshot_number) => {

            let mut allstoredversions = versions::AllStoredVersions::new();
            allstoredversions.stored_versions = snapshot::read_snapshot(&snapshot_number, "versions")?;

            allstoredversions.print(&hostname_filter);
        },
        None => {
            let allstoredversions = versions::AllStoredVersions::read_versions(&hosts, &ports, parallel).await;
            allstoredversions.print(&hostname_filter);
        },
    }
    Ok(())
}

pub async fn print_threads(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
    hostname_filter: &Regex,
) -> Result<()>
{
    match options.print_threads.as_ref().unwrap() {
        Some(snapshot_number) => {
            let mut allstoredthreads = threads::AllStoredThreads::new();
            allstoredthreads.stored_threads = snapshot::read_snapshot(&snapshot_number, "threads")?;
            allstoredthreads.print(&hostname_filter)?;
        },
        None => {
            let allstoredthreads = threads::AllStoredThreads::read_threads(&hosts, &ports, parallel).await;
            allstoredthreads.print(&hostname_filter)?;
        }
    }
    Ok(())
}

pub async fn print_entities(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
    table_name_filter: &Regex,
) -> Result<()>
{
    match options.print_entities.as_ref().unwrap() {
        Some(snapshot_number) => {

            let mut allstoredentities = entities::AllStoredEntities::new();
            allstoredentities.stored_keyspaces = snapshot::read_snapshot(&snapshot_number, "keyspaces")?;
            allstoredentities.stored_tables = snapshot::read_snapshot(&snapshot_number, "tables")?;
            allstoredentities.stored_tablets = snapshot::read_snapshot(&snapshot_number, "tablets")?;
            allstoredentities.stored_replicas = snapshot::read_snapshot(&snapshot_number, "replicas")?;

            allstoredentities.print(&snapshot_number, &table_name_filter, &options.details_enable)?;
        },
        None => {
            let allstoredentities = entities::AllStoredEntities::read_entities(&hosts, &ports, parallel).await;
            allstoredentities.print_adhoc(&table_name_filter, &options.details_enable, &hosts, &ports, parallel).await;
        },
    }
    Ok(())
}

pub async fn print_masters(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    match options.print_masters.as_ref().unwrap() {
        Some(snapshot_number) => {

            let mut allstoredmasters = masters::AllStoredMasters::new();
            allstoredmasters.stored_masters = snapshot::read_snapshot(&snapshot_number, "masters")?;
            allstoredmasters.stored_rpc_addresses = snapshot::read_snapshot(&snapshot_number, "master_rpc_addresses")?;
            allstoredmasters.stored_http_addresses = snapshot::read_snapshot(&snapshot_number, "master_http_addresses")?;
            allstoredmasters.stored_master_error = snapshot::read_snapshot(&snapshot_number, "master_errors")?;

            allstoredmasters.print(&snapshot_number, &options.details_enable)?;

        }
        None => {
            let allstoredmasters = masters::AllStoredMasters::read_masters(&hosts, &ports, parallel).await;
            allstoredmasters.print_adhoc(&options.details_enable, &hosts, &ports, parallel).await;
        }
    }
    Ok(())
}

pub async fn print_tablet_servers(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    match options.print_tablet_servers.as_ref().unwrap() {
        Some(snapshot_number) => {

            let mut allstoredtabletservers = tservers::AllStoredTabletServers::new();
            allstoredtabletservers.stored_tabletservers = snapshot::read_snapshot(&snapshot_number, "tablet_servers")?;
            allstoredtabletservers.stored_pathmetrics = snapshot::read_snapshot(&snapshot_number, "tablet_servers_pathmetrics")?;

            allstoredtabletservers.print(&snapshot_number, &options.details_enable)?;

        }
        None => {
            let allstoredtabletservers = tservers::AllStoredTabletServers::read_tabletservers(&hosts, &ports, parallel).await;
            allstoredtabletservers.print_adhoc(&options.details_enable, &hosts, &ports, parallel).await?;
        }
    }
    Ok(())
}

pub async fn print_vars(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
    hostname_filter: &Regex,
    stat_name_filter: &Regex,
) -> Result<()>
{
    match options.print_vars.as_ref().unwrap() {
        Some(snapshot_number) => {
            let mut allstoredvars = vars::AllStoredVars::new();
            allstoredvars.stored_vars = snapshot::read_snapshot(&snapshot_number, "vars")?;

            allstoredvars.print(&options.details_enable, &hostname_filter, &stat_name_filter).await;
        }
        None => {
            let allstoredvars = vars::AllStoredVars::read_vars(&hosts, &ports, parallel).await;
            allstoredvars.print(&options.details_enable, &hostname_filter, &stat_name_filter).await;
        }
    }
    Ok(())
}

pub async fn print_clocks(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    match options.print_clocks.as_ref().unwrap() {
        Some(snapshot_number) => {
            let mut allstoredclocks = clocks::AllStoredClocks::new();
            allstoredclocks.stored_clocks = snapshot::read_snapshot(&snapshot_number, "clocks")?;

            allstoredclocks.print(&snapshot_number, &options.details_enable)?;
        },
        None => {
            let allstoredclocks = clocks::AllStoredClocks::read_clocks(&hosts, &ports, parallel).await?;
            allstoredclocks.print_adhoc(&options.details_enable, &hosts, &ports, parallel).await?;
        },
    }
    Ok(())
}

pub async fn print_latencies(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    match options.print_latencies.as_ref().unwrap() {
        Some(snapshot_number) => {
            let mut allstoredclocks = clocks::AllStoredClocks::new();
            allstoredclocks.stored_clocks = snapshot::read_snapshot(&snapshot_number, "clocks")?;

            allstoredclocks.print_latency(&snapshot_number, &options.details_enable).await?;
        },
        None => {
            let allstoredclocks = clocks::AllStoredClocks::read_clocks(&hosts, &ports, parallel).await?;
            allstoredclocks.print_adhoc_latency(&options.details_enable, &hosts, &ports, parallel).await?;
        },
    }
    Ok(())
}

pub async fn adhoc_metrics_diff(
    hosts: Vec<&'static str>,
    ports: Vec<&'static str>,
    parallel: usize,
    options: &Opts,
    hostname_filter: &Regex,
    stat_name_filter: &Regex,
    table_name_filter: &Regex,
) -> Result<()>
{
    info!("ad-hoc metrics diff first snapshot begin");
    let timer = Instant::now();

    let first_snapshot_time = Local::now();

    let metrics = Arc::new(Mutex::new(metrics::SnapshotDiffBTreeMapsMetrics::new()));
    let statements = Arc::new(Mutex::new(statements::SnapshotDiffBTreeMapStatements::new()));
    let node_exporter = Arc::new(Mutex::new(node_exporter::SnapshotDiffBTreeMapNodeExporter::new()));

    let hosts = Arc::new(Mutex::new(hosts));
    let ports = Arc::new(Mutex::new(ports));
    let mut handles = vec![];

    let clone_metrics = metrics.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_metrics.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
    });
    handles.push(handle);

    let clone_statements = statements.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_statements.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
    });
    handles.push(handle);

    let clone_node_exporter = node_exporter.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_node_exporter.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
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
    let handle = tokio::spawn(async move {
        clone_metrics.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel, &first_snapshot_time).await;
    });
    handles.push(handle);

    let clone_statements = statements.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_statements.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel, &first_snapshot_time).await;
    });
    handles.push(handle);

    let clone_node_exporter = node_exporter.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_node_exporter.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel, &first_snapshot_time).await;
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

    Ok(())
}

pub async fn adhoc_diff(
    hosts: Vec<&'static str>,
    ports: Vec<&'static str>,
    parallel: usize,
    options: &Opts,
    hostname_filter: &Regex,
    stat_name_filter: &Regex,
    table_name_filter: &Regex,
) -> Result<()>
{
    info!("ad-hoc mode first snapshot begin");

    let timer = Instant::now();

    let first_snapshot_time = Local::now();

    let metrics = Arc::new(Mutex::new(metrics::SnapshotDiffBTreeMapsMetrics::new()));
    let statements = Arc::new(Mutex::new(statements::SnapshotDiffBTreeMapStatements::new()));
    let node_exporter = Arc::new(Mutex::new(node_exporter::SnapshotDiffBTreeMapNodeExporter::new()));
    let entities = Arc::new(Mutex::new(entities::SnapshotDiffBTreeMapsEntities::new()));
    let masters = Arc::new(Mutex::new(masters::SnapshotDiffBTreeMapsMasters::new()));
    let tablet_servers = Arc::new(Mutex::new(tservers::SnapshotDiffBTreeMapsTabletServers::new()));
    let versions = Arc::new(Mutex::new(versions::SnapshotDiffBTreeMapsVersions::new()));
    let vars = Arc::new(Mutex::new(vars::SnapshotDiffBTreeMapsVars::new()));

    let hosts = Arc::new(Mutex::new(hosts));
    let ports = Arc::new(Mutex::new(ports));
    let mut handles = vec![];

    let clone_metrics = metrics.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_metrics.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
    });
    handles.push(handle);

    let clone_statements = statements.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_statements.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
    });
    handles.push(handle);

    let clone_node_exporter = node_exporter.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_node_exporter.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
    });
    handles.push(handle);

    let clone_entities = entities.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_entities.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
    });
    handles.push(handle);

    let clone_masters = masters.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_masters.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
    });
    handles.push(handle);

    let clone_tablet_servers = tablet_servers.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_tablet_servers.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
    });
    handles.push(handle);

    let clone_vars = vars.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_vars.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
    });
    handles.push(handle);

    let clone_versions = versions.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_versions.lock().await.adhoc_read_first_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
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
    let handle = tokio::spawn(async move {
        clone_metrics.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel, &first_snapshot_time).await;
    });
    handles.push(handle);

    let clone_statements = statements.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_statements.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel, &first_snapshot_time).await;
    });
    handles.push(handle);

    let clone_node_exporter = node_exporter.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_node_exporter.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel, &first_snapshot_time).await;
    });
    handles.push(handle);

    let clone_entities = entities.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_entities.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
    });
    handles.push(handle);

    let clone_masters = masters.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_masters.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
    });
    handles.push(handle);

    let clone_tablet_servers = tablet_servers.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_tablet_servers.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
    });
    handles.push(handle);

    let clone_vars = vars.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_vars.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
    });
    handles.push(handle);

    let clone_versions = versions.clone();
    let clone_hosts = hosts.clone();
    let clone_ports = ports.clone();
    let handle = tokio::spawn(async move {
        clone_versions.lock().await.adhoc_read_second_snapshot(clone_hosts.lock().await.as_ref(), clone_ports.lock().await.as_ref(), parallel).await;
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

    Ok(())
}