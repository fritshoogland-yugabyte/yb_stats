use std::path::PathBuf;
//use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use std::fs;
use std::io::Write;
use std::process;
use std::sync::mpsc::channel;
use log::*;

pub fn read_mems(
    host: &str,
    port: &str,
) -> String {
    if ! scan_port_addr( format!("{}:{}", host, port)) {
        warn!("hostname:port {}:{} cannot be reached, skipping (mems)",host ,port);
        return String::from("");
    }
    if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}:{}/memz?raw=true", host, port)) {
        data_from_http.text().unwrap()
    } else {
        String::from("")
    }
}

#[allow(clippy::ptr_arg)]
pub async fn perform_mems_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    snapshot_number: i32,
    yb_stats_directory: &PathBuf,
    parallel: usize
) {
    info!("perform_mems_snapshot");
    let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
    let (tx, rx) = channel();
    pool.scope(move |s| {
        for host in hosts {
            for port in ports {
                let tx = tx.clone();
                s.spawn(move |_| {
                    let mems_data = read_mems(host, port);
                    tx.send((format!("{}:{}", host, port), mems_data)).expect("error sending data via tx (mems)");
                });
            }}
    });
    for (hostname_port, mems) in rx {

        if mems.starts_with("------------------------------------------------") {
            let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
            let mems_file = &current_snapshot_directory.join(format!("mems_{}", hostname_port));
            let mut file = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(mems_file)
                .unwrap_or_else(|e| {
                    error!("Fatal: error writing mems data in snapshot directory {}: {}", &mems_file.clone().into_os_string().into_string().unwrap(), e);
                    process::exit(1);
                });
            file.write_all(mems.as_bytes()).unwrap_or_else(|e| {
                error!("Fatal: error mems data in snapshot directory {:?}: {}", &file, e);
                process::exit(1);
            });
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::utility;

    #[test]
    fn parse_mems_tserver() {
        // currently, the mems "parsing" is not much parsing.
        // What currently is done, is that the hostname:port/memz output is stored in a file in the snapshot directory named <hostname>:<port>_mems.
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();
        read_mems(&hostname, &port);
    }
    #[test]
    fn parse_mems_master() {
        // currently, the mems "parsing" is not much parsing.
        // What currently is done, is that the hostname:port/memz output is stored in a file in the snapshot directory named <hostname>:<port>_mems.
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();
        read_mems(&hostname, &port);
    }
}