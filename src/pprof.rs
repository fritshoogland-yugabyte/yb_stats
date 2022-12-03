use std::path::PathBuf;
//use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use std::fs;
use std::io::Write;
use std::process;
use std::sync::mpsc::channel;
use log::*;

pub fn read_pprof(
    host: &str,
    port: &str,
) -> String {
    if ! scan_port_addr( format!("{}:{}", host, port)) {
        warn!("hostname:port {}:{} cannot be reached, skipping (pprof)",host ,port);
        return String::from("");
    }
    if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}:{}/pprof/growth", host, port)) {
        data_from_http.text().unwrap()
    } else {
        String::from("")
    }
}

#[allow(clippy::ptr_arg)]
pub fn perform_pprof_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    snapshot_number: i32,
    yb_stats_directory: &PathBuf,
    parallel: usize
) {
    info!("perform_pprof_snapshot");
    let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
    let (tx, rx) = channel();
    pool.scope(move |s| {
        for host in hosts {
            for port in ports {
                let tx = tx.clone();
                s.spawn(move |_| {
                    let pprof_data = read_pprof(host, port);
                    tx.send((format!("{}:{}", host, port), pprof_data)).expect("error sending data via tx (pprof)");
                });
            }}
    });
    for (hostname_port, pprof) in rx {

        if pprof.starts_with("heap profile") {
            let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
            let pprof_file = &current_snapshot_directory.join(format!("pprof_growth_{}", hostname_port));
            let mut file = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(pprof_file)
                .unwrap_or_else(|e| {
                    error!("Fatal: error writing pprof growth data in snapshot directory {}: {}", &pprof_file.clone().into_os_string().into_string().unwrap(), e);
                    process::exit(1);
                });
            //let mut writer = csv::Writer::from_writer(file);
            file.write_all(pprof.as_bytes()).unwrap_or_else(|e| {
                error!("Fatal: error pprof growth data in snapshot directory {:?}: {}", &file, e);
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
    fn integration_parse_pprof_growth_tserver() {
        // currently, the pprof "parsing" is not much parsing.
        // What currently is done, is that the hostname:port/pprof/growth output is stored in a file in the snapshot directory named <hostname>:<port>_pprof_growth.
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();
        read_pprof(&hostname, &port);
    }
    #[test]
    fn integration_parse_pprof_growth_master() {
        // currently, the pprof "parsing" is not much parsing.
        // What currently is done, is that the hostname:port/pprof/growth output is stored in a file in the snapshot directory named <hostname>:<port>_pprof_growth.
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();
        read_pprof(&hostname, &port);
    }
}
