//! Utilities
//use port_scanner::scan_port_addr;
use log::*;
use std::{collections::HashMap, env, fs, io::Write, time::Duration};
use anyhow::{Context, Result};
use regex::Regex;
//use qscan::{QScanner, QScanResult, QScanType, QscanTcpConnectState, QScanTcpConnectState};
//use tokio::runtime::Runtime;

// This reads the constant set in main.rs.
// This probably needs to be made better, and user settable.
use crate::ACCEPT_INVALID_CERTS;

use crate::DEFAULT_HOSTS;
use crate::DEFAULT_PORTS;
use crate::DEFAULT_PARALLEL;

/*
/// Scan the given host and port combination to see if it's reachable.
pub fn scan_host_port(
    host: &str,
    port: &str,
) -> bool
{
    // this currently uses port_scanner, but can be slow (3s).
    /*
    if ! scan_port_addr( format!("{}:{}", host, port)) {
        warn!("Port scanner: hostname:port {}:{} cannot be reached, skipping",host ,port);
        false
    } else {
        true
    }

     */
    true
    /* try with qscan crate
    let mut scanner = QScanner::new(host, port);
    scanner.set_timeout_ms(100);
    scanner.set_ntries(1);
    scanner.set_scan_type(QScanType::TcpConnect);

    let results: &Vec<QScanResult> = Runtime::new().unwrap().block_on(scanner.scan_tcp_connect());

    for result in resuts {
        if let QScanResult::TcpConnect(sa) = result {
            if sa.state == QScanTcpConnectState::Open {
                true
            } else {
                false
            }
        }
    }

     */
}

 */

/// Reads the http endpoint as specified by the caller, and returns the result as String.
pub fn http_get(
    host: &str,
    port: &str,
    url: &str,
) -> String
{
    if let Ok(data_from_web_request) = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
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

/// Take the hosts from the [Option] struct, and:
/// - adds it to the changed_options hashmap if necessary.
/// - returns a Vec<&str>.
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

/// Take the ports from the [Option] struct, and:
/// - adds it to the changed_options hashmap if necessary.
/// - returns a Vec<&str>.
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

/// Take the parallel from the [Option] struct, and:
/// - adds it to the changed_options hashmap if necessary.
/// - returns a usize.
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

/// Simple helper routine to create a regex from an `&Option<String>`.
pub fn set_regex(
    regex: &Option<String>,
) -> Regex
{
    match regex {
        Some(regex) => Regex::new(regex.as_str()).unwrap(),
        None => Regex::new(".*").unwrap(),
    }
}

/// If writing the '.env' file is allowed via write_dotenv,
/// take the changed_options hashmap, and write it.
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
            .with_context(|| "Error writing .env file in current directory")?;

        for (key, value) in changed_options {
            file.write_all(format!("{}={}\n", key, value).as_bytes())?;
            info!("{}={}", key, value);
        }
    }
    Ok(())
}

/// Read environment variable for integration tests
#[cfg(test)]
pub fn get_hostname_master() -> String {
    match env::var("HOSTNAME_MASTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_MASTER should be set") },
    }
}
/// Read environment variable for integration tests
#[cfg(test)]
pub fn get_port_master() -> String {
    match env::var("PORT_MASTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_MASTER should be set") },
    }
}
/// Read environment variable for integration tests
#[cfg(test)]
pub fn get_hostname_tserver() -> String {
    match env::var("HOSTNAME_TSERVER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_TSERVER should be set") },
    }
}
/// Read environment variable for integration tests
#[cfg(test)]
pub fn get_port_tserver() -> String {
    match env::var("PORT_TSERVER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_TSERVER should be set") },
    }
}
/// Read environment variable for integration tests
#[cfg(test)]
pub fn get_hostname_ysql() -> String {
    match env::var("HOSTNAME_YSQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_YSQL should be set") },
    }
}
/// Read environment variable for integration tests
#[cfg(test)]
pub fn get_port_ysql() -> String {
    match env::var("PORT_YSQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_YSQL should be set") },
    }
}
/// Read environment variable for integration tests
#[cfg(test)]
pub fn get_hostname_ycql() -> String {
    match env::var("HOSTNAME_YCQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_YCQL should be set") },
    }
}
/// Read environment variable for integration tests
#[cfg(test)]
pub fn get_port_ycql() -> String {
    match env::var("PORT_YCQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_YCQL should be set") },
    }
}
/// Read environment variable for integration tests
#[cfg(test)]
pub fn get_hostname_yedis() -> String {
    match env::var("HOSTNAME_YEDIS") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_YEDIS should be set") },
    }
}
/// Read environment variable for integration tests
#[cfg(test)]
pub fn get_port_yedis() -> String {
    match env::var("PORT_YEDIS") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_YEDIS should be set") },
    }
}
/// Read environment variable for integration tests
#[cfg(test)]
pub fn get_hostname_node_exporter() -> String {
    match env::var("HOSTNAME_NODE_EXPORTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_NODE_EXPORTER should be set") },
    }
}
/// Read environment variable for integration tests
#[cfg(test)]
pub fn get_port_node_exporter() -> String {
    match env::var("PORT_NODE_EXPORTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_NODE_EXPORTER should be set") },
    }
}