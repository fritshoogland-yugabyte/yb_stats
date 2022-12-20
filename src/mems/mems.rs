//! Module for reading the /memz output for the master and tablet server.
//!
use std::{fs, io::Write, sync::mpsc::channel, time::Instant, env};
use log::*;
use anyhow::{Result, Context};
use crate::utility;

pub struct Mems;

impl Mems {
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        Mems::read_and_write_mems(hosts, ports, snapshot_number, parallel).await?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub async fn read_and_write_mems(
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
                        let mems_data = Mems::read_http(host, port);
                        tx.send((format!("{}:{}", host, port), mems_data)).expect("error sending data via tx (mems)");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        for (hostname_port, mems_data) in rx {
            if mems_data.starts_with("------------------------------------------------") {
                let current_directory = env::current_dir()?;
                let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(snapshot_number.to_string());

                let mems_file = &current_snapshot_directory.join(format!("mems_{}", hostname_port));
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(mems_file)
                    .with_context(|| format!("Cannot create file: {}", mems_file.display()))?;

                file.write_all(mems_data.as_bytes())
                    .with_context(|| format!("Error writing file: {}", mems_file.display()))?;
            }
        }
        Ok(())
    }
    fn read_http(
        host: &str,
        port: &str,
    ) -> String
    {
        let data_from_http = if utility::scan_host_port(host, port)
        {
            utility::http_get(host, port, "memz?raw=true")
        }
        else
        {
            String::new()
        };
        data_from_http
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    //use crate::utility_test::*;

    #[tokio::test]
    async fn integration_parse_mems_tserver() {
        // currently, the mems "parsing" is not much parsing.
        // What currently is done, is that the hostname:port/memz output is stored in a file in the snapshot directory named <hostname>:<port>_mems.
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();
        Mems::read_and_write_mems(&vec![&hostname], &vec![&port], 1).await;;
    }
    #[tokio::test]
    async fn integration_parse_mems_master() {
        // currently, the mems "parsing" is not much parsing.
        // What currently is done, is that the hostname:port/memz output is stored in a file in the snapshot directory named <hostname>:<port>_mems.
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();
        Mems::read_and_write_mems(&vec![&hostname], &vec![&port], 1).await;;
    }
}