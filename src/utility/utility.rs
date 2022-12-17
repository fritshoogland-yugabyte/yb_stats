//! Utilities
use port_scanner::scan_port_addr;
use log::*;
use std::{time::Instant, env, sync::Arc, fs, collections::HashMap, io::{Write, stdin}};
use chrono::Local;
use anyhow::{Result, Context};
use regex::Regex;
use tokio::sync::Mutex;
use crate::{metrics, statements, node_exporter, entities, masters, tservers, vars, versions};
use crate::Opts;

// This reads the constant set in main.rs.
// This probably needs to be made better, and user settable.
use crate::ACCEPT_INVALID_CERTS;

use crate::DEFAULT_HOSTS;
use crate::DEFAULT_PORTS;
use crate::DEFAULT_PARALLEL;

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
            .with_context(|| "Error writing .env file: .env")?;

        for (key, value) in changed_options {
            file.write_all(format!("{}={}\n", key, value).as_bytes())?;
            info!("{}={}", key, value);
        }
    }
    Ok(())
}

pub async fn adhoc_metrics_diff(
    hosts: Vec<&'static str>,
    ports: Vec<&'static str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    info!("ad-hoc metrics diff first snapshot begin");
    let timer = Instant::now();

    let stat_name_filter = set_regex(&options.stat_name_match);
    let hostname_filter = set_regex(&options.hostname_match);
    let table_name_filter = set_regex(&options.table_name_match);

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
) -> Result<()>
{
    info!("ad-hoc mode first snapshot begin");
    let timer = Instant::now();

    let stat_name_filter = set_regex(&options.stat_name_match);
    let hostname_filter = set_regex(&options.hostname_match);
    let table_name_filter = set_regex(&options.table_name_match);

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

#[cfg(test)]
pub fn get_hostname_master() -> String {
    match env::var("HOSTNAME_MASTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_MASTER should be set") },
    }
}
#[cfg(test)]
pub fn get_port_master() -> String {
    match env::var("PORT_MASTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_MASTER should be set") },
    }
}
#[cfg(test)]
pub fn get_hostname_tserver() -> String {
    match env::var("HOSTNAME_TSERVER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_TSERVER should be set") },
    }
}
#[cfg(test)]
pub fn get_port_tserver() -> String {
    match env::var("PORT_TSERVER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_TSERVER should be set") },
    }
}
#[cfg(test)]
pub fn get_hostname_ysql() -> String {
    match env::var("HOSTNAME_YSQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_YSQL should be set") },
    }
}
#[cfg(test)]
pub fn get_port_ysql() -> String {
    match env::var("PORT_YSQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_YSQL should be set") },
    }
}
#[cfg(test)]
pub fn get_hostname_ycql() -> String {
    match env::var("HOSTNAME_YCQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_YCQL should be set") },
    }
}
#[cfg(test)]
pub fn get_port_ycql() -> String {
    match env::var("PORT_YCQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_YCQL should be set") },
    }
}
#[cfg(test)]
pub fn get_hostname_yedis() -> String {
    match env::var("HOSTNAME_YEDIS") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_YEDIS should be set") },
    }
}
#[cfg(test)]
pub fn get_port_yedis() -> String {
    match env::var("PORT_YEDIS") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_YEDIS should be set") },
    }
}
#[cfg(test)]
pub fn get_hostname_node_exporter() -> String {
    match env::var("HOSTNAME_NODE_EXPORTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_NODE_EXPORTER should be set") },
    }
}
#[cfg(test)]
pub fn get_port_node_exporter() -> String {
    match env::var("PORT_NODE_EXPORTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_NODE_EXPORTER should be set") },
    }
}