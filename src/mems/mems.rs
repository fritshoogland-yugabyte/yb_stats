use std::{path::PathBuf, fs, io::Write, sync::mpsc::channel};
use log::*;
use anyhow::{Result, Context};
use crate::utility;

pub fn read_mems(
    host: &str,
    port: &str,
) -> String
{
    if utility::scan_host_port( host, port) {
        utility::http_get(host, port, "memz?raw=true")
    } else {
        String::new()
    }
}

#[allow(clippy::ptr_arg)]
pub async fn perform_mems_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    snapshot_number: i32,
    yb_stats_directory: &PathBuf,
    parallel: usize
) -> Result<()>
{
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
            let current_snapshot_directory = &yb_stats_directory.join(snapshot_number.to_string());
            let mems_file = &current_snapshot_directory.join(format!("mems_{}", hostname_port));
            let mut file = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(mems_file)
                .with_context(|| format!("Cannot create file: {}", mems_file.display()))?;

            file.write_all(mems.as_bytes())
                .with_context(|| format!("Error writing file: {}", mems_file.display()))?;
        };

    }
    Ok(())

}

#[cfg(test)]
mod tests {
    use super::*;
    //use crate::utility_test::*;

    #[test]
    fn integration_parse_mems_tserver() {
        // currently, the mems "parsing" is not much parsing.
        // What currently is done, is that the hostname:port/memz output is stored in a file in the snapshot directory named <hostname>:<port>_mems.
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();
        read_mems(&hostname, &port);
    }
    #[test]
    fn integration_parse_mems_master() {
        // currently, the mems "parsing" is not much parsing.
        // What currently is done, is that the hostname:port/memz output is stored in a file in the snapshot directory named <hostname>:<port>_mems.
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();
        read_mems(&hostname, &port);
    }
}