//! Module for reading the /prof/growth output for the master and tablet server.
//!
use std::{fs, io::Write, sync::mpsc::channel, time::Instant, env};
use log::*;
use anyhow::{Result, Context};
use crate::utility;

pub struct Pprof;

impl Pprof {
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        Pprof::read_and_write_pprof(hosts, ports, snapshot_number, parallel).await?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub async fn read_and_write_pprof(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin parallel http read");
        let timer = Instant::now();

        let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
        let (tx, rx) = channel();

        pool.scope(move |s| {
            for host in hosts {
                for port in ports {
                    let tx = tx.clone();
                    s.spawn(move |_| {
                        let pprof_data = Pprof::read_http(host, port);
                        tx.send((format!("{}:{}", host, port), pprof_data)).expect("error sending data via tx (pprof)");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        for (hostname_port, pprof_data) in rx {
            if pprof_data.starts_with("heap profile")
                && snapshot_number >= 0
            {
                let current_directory = env::current_dir()?;
                let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(snapshot_number.to_string());

                let pprof_file = &current_snapshot_directory.join(format!("pprof_growth_{}", hostname_port));
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(pprof_file)
                    .with_context(|| format!("Cannot create file: {}", pprof_file.display()))?;

                file.write_all(pprof_data.as_bytes())
                    .with_context(|| format!("Error writing file: {}", pprof_file.display()))?;
            };
        }
        Ok(())
    }
    fn read_http(
        host: &str,
        port: &str,
    ) -> String
    {
        let data_from_http = utility::http_get(host, port, "pprof/growth");
        data_from_http
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    //use crate::utility_test::*;

    #[tokio::test]
    async fn integration_parse_pprof_growth_tserver() {
        // currently, the pprof "parsing" is not much parsing.
        // What currently is done, is that the hostname:port/pprof/growth output is stored in a file in the snapshot directory named <hostname>:<port>_pprof_growth.
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();
        Pprof::read_and_write_pprof(&vec![&hostname], &vec![&port], -1, 1).await.unwrap();
    }
    #[tokio::test]
    async fn integration_parse_pprof_growth_master() {
        // currently, the pprof "parsing" is not much parsing.
        // What currently is done, is that the hostname:port/pprof/growth output is stored in a file in the snapshot directory named <hostname>:<port>_pprof_growth.
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();
        Pprof::read_and_write_pprof(&vec![&hostname], &vec![&port], -1, 1).await.unwrap();
    }
}
