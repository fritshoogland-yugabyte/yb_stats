use std::{path::PathBuf, fs, io::Write, sync::mpsc::channel};
use log::*;
use anyhow::{Result, Context};
use crate::utility;

pub fn read_pprof(
    host: &str,
    port: &str,
) -> String {
    if utility::scan_host_port( host, port) {
        utility::http_get(host, port, "pprof/growth")
    } else {
        String::new()
    }
}

#[allow(clippy::ptr_arg)]
pub async fn perform_pprof_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    snapshot_number: i32,
    yb_stats_directory: &PathBuf,
    parallel: usize
)  -> Result<()>
{
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
            let current_snapshot_directory = &yb_stats_directory.join(snapshot_number.to_string());
            let pprof_file = &current_snapshot_directory.join(format!("pprof_growth_{}", hostname_port));
            let mut file = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(pprof_file)
                .with_context(|| format!("Cannot create file: {}", pprof_file.display()))?;

            file.write_all(pprof.as_bytes())
                .with_context(|| format!("Error writing file: {}", pprof_file.display()))?;
            //let mut writer = csv::Writer::from_writer(file);
            /*
            file.write_all(pprof.as_bytes()).unwrap_or_else(|e| {
                error!("Fatal: error write pprof growth data in snapshot directory {:?}: {}", &file, e);
                process::exit(1);
            });

             */
        };

    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    //use crate::utility_test::*;

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
