//! The module for reading the /api/v1/is-leader to identify the master leader.
//!
//! The functionality for isleader has the following public entries:
//!  1. Snapshot creation: [AllStoredIsLeader::perform_snapshot]
//!  2. Provide the master hostname:port for a given snapshot: [AllStoredIsLeader::return_leader]
//!
//! This function has no public display function, it is only used to store the and retrieve the master leader.
use chrono::{DateTime, Local};
use std::{env, fs, error::Error, process, time::Instant, sync::mpsc::channel};
use serde_derive::{Serialize,Deserialize};
use log::*;
use crate::utility::{scan_host_port, http_get};
/// The struct that is used to parse the JSON returned from /api/v1/is-leader using serde.
///
/// Please mind that only the leader shows:
/// ```
/// {"STATUS":"OK"}
/// ```
/// The master followers do not return anything after being parsed.
#[derive(Serialize, Deserialize, Debug)]
pub struct IsLeader {
    /// The key for the status is in capitals, in this way it's renamed to 'status'.
    #[serde(rename = "STATUS")]
    pub status: String,
}
/// The struct that is used to store and retrieve the fetched and parsed data in CSV using serde.
///
/// The hostname_port and timestamp fields are filled out.
/// One of all the servers will have status field reading 'OK', indicating being the master leader.
#[derive(Serialize, Deserialize, Debug)]
pub struct StoredIsLeader {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub status: String,
}
/// This struct is used to handle the [StoredIsLeader] struct.
///
/// In this way, the struct can be using the impl functions.
#[derive(Debug)]
pub struct AllStoredIsLeader {
    pub stored_isleader: Vec<StoredIsLeader>
}

impl AllStoredIsLeader {
    /// This function reads all the host/port combinations for metrics and saves these in a snapshot indicated by the snapshot_number.
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize
    )
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allstoredisleader = AllStoredIsLeader::read_isleader(hosts, ports, parallel);
        allstoredisleader.await.save_snapshot(snapshot_number)
            .unwrap_or_else(|e| {
                error!("error saving snapshot: {}", e);
                process::exit(1);
            });

        info!("end snapshot: {:?}", timer.elapsed())
    }
    /// This function requires a snapshot number, and returns the hostname_port of the master leader.
    pub fn return_leader_snapshot (
       snapshot_number: &String
    ) -> String
    {
       let stored_isleader = AllStoredIsLeader::read_snapshot(snapshot_number)
           .unwrap_or_else(|e| {
               error!("error reading snapshot: {}", e);
               process::exit(1);
           });
        stored_isleader.stored_isleader.iter().filter(|r| r.status == "OK").map(|r| r.hostname_port.to_string()).next().unwrap()
    }
    pub async fn return_leader_http (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> String
    {
        let allstoredisleader = AllStoredIsLeader::read_isleader(hosts, ports, parallel);
        allstoredisleader.await.stored_isleader.iter().filter(|r| r.status == "OK").map(|r| r.hostname_port.to_string()).next().unwrap_or_default()
        //Ok(result)
    }
    /// This function takes a vector of hosts and ports, and the allowed parallellism to (try to) read /api/v1/is-leader.
    /// It creates a threadpool based on parallel, and spawns a task for reading and parsing for all host-port combinations.
    /// When all combinations are read, the results are gathered in Vec<AllStoredIsLeader> and returned.
    async fn read_isleader (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize
    ) -> AllStoredIsLeader
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
                        let detail_snapshot_time = Local::now();
                        let isleader = AllStoredIsLeader::read_http(host, port);
                        debug!("{:?}",&isleader);
                        tx.send((format!("{}:{}", host, port), detail_snapshot_time, isleader)).expect("error sending data via tx (isleader)");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allstoredisleader = AllStoredIsLeader { stored_isleader: Vec::new() };
        for (hostname_port, detail_snapshot_time, isleader) in rx {
            debug!("hostname_port: {}, timestamp: {}, isleader: {}", &hostname_port, &detail_snapshot_time, &isleader.status);
            allstoredisleader.stored_isleader.push(StoredIsLeader { hostname_port, timestamp: detail_snapshot_time, status: isleader.status.to_string() } );
        }
        allstoredisleader
    }
    /// Using provided host and port, read http://host:port/api/v1/is-leader and parse the result
    /// via [AllStoredIsLeader::parse_isleader], and return struct [IsLeader].
    fn read_http(
        host: &str,
        port: &str,
    ) -> IsLeader
    {
        let data_from_http = if scan_host_port( host, port) {
            http_get(host, port, "api/v1/is-leader")
        } else {
            String::new()
        };
        AllStoredIsLeader::parse_isleader(data_from_http)
        /*
        if ! scan_port_addr( format!("{}:{}", host, port) ) {
            warn!("Warning: hostname:port {}:{} cannot be reached, skipping", host, port);
            return AllStoredIsLeader::parse_isleader(String::from(""))
        }
        let data_from_http = reqwest::blocking::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap()
            .get(format!("http://{}:{}/api/v1/is-leader", host, port))
            .send()
            .unwrap()
            .text()
            .unwrap();
        if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}:{}/api/v1/is-leader", host, port)) {
            AllStoredIsLeader::parse_isleader(data_from_http.text().unwrap())
        } else {
            AllStoredIsLeader::parse_isleader(String::from(""))
        }
         */
    }
    /// This function parses the http output.
    /// This is a separate function in order to allow integration tests to use it.
    fn parse_isleader( http_output: String ) -> IsLeader
    {
        serde_json::from_str( &http_output )
            .unwrap_or_else(|_e| {
                IsLeader { status: "".to_string() }
            })
    }
    /// This function takes the rows in the vector StoredIsLeader, and saves it as CSV in the snapshot directory indicated by the snapshot number.
    fn save_snapshot ( self, snapshot_number: i32 ) -> Result<(), Box<dyn Error>>
    {
        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number.to_string());

        let isleader_file = &current_snapshot_directory.join("isleader");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(isleader_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_isleader {
            writer.serialize(row)?;
        }
        writer.flush()?;

        Ok(())
    }
    /// This function takes a snapshot number and reads the isleader CSV and loads it into the vector stored_isleader in [AllStoredIsLeader].
    fn read_snapshot( snapshot_number: &String, ) -> Result<AllStoredIsLeader, Box<dyn Error>>
    {
        let mut allstoredisleader = AllStoredIsLeader { stored_isleader: Vec::new() };

        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(snapshot_number);

        let isleader_file = &current_snapshot_directory.join("isleader");
        let file = fs::File::open(isleader_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredIsLeader = row?;
            allstoredisleader.stored_isleader.push(data);
        };

        Ok(allstoredisleader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_status_ok() {
        // This is what /api/v1/is-leader returns by the master, on the master leader.
        let version = r#"
        {"STATUS":"OK"}
"#.to_string();
        let result = AllStoredIsLeader::parse_isleader(version);
        assert_eq!(result.status, "OK");
    }

    #[test]
    fn unit_parse_status_empty() {
        // This is what /api/v1/is-leader returns by the master, NOT on the master leader.
        let version = r#"
"#.to_string();
        let result = AllStoredIsLeader::parse_isleader(version);
        assert_eq!(result.status, "");
    }

    #[test]
    fn unit_parse_endpoint_does_not_exist() {
        // This is what /api/v1/is-leader returns by requesting not the master.
        // it does return http 404.
        let version = r#"
Error 404: Not Found
File not found
"#.to_string();
        let result = AllStoredIsLeader::parse_isleader(version);
        assert_eq!(result.status, "");
    }

    use crate::utility;

    #[tokio::test]
    async fn integration_find_master_leader() {

        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let leader = AllStoredIsLeader::return_leader_http(&vec![&hostname], &vec![&port], 1_usize).await;
        //let hostname_port = AllStoredIsLeader::return_leader_snapshot(&"22".to_string());
        println!("{}", leader);
        //let hostname_port = String::from("haha");
        //let stored_isleader = AllStoredIsLeader::read_snapshot(&"22".to_string())
        //    .unwrap_or_else(|e| {
        //        error!("error reading snapshot: {}", e);
        //        process::exit(1);
        //    });
        //println!("{:?}", stored_isleader);
        //let leader = stored_isleader.stored_isleader.iter().filter(|r| r.status == "OK").map(|r| r.hostname_port.to_string()).next().unwrap();
        //let h = stored_isleader.stored_isleader.iter().filter(|r| r.status == "OK").map(|r| r.hostname_port.to_string()).next().unwrap();
        //println!("{:?}", hostname_port);
        //assert_eq!(hostname_port, "192.168.66.82:7000");
        /*
        let snapshot_number = "22".to_string();
        let mut allstoredisleader = AllStoredIsLeader { stored_isleader: Vec::new() };

        let current_directory = env::current_dir().unwrap();
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number);

        let isleader_file = &current_snapshot_directory.join("isleader");
        let file = fs::File::open(&isleader_file).unwrap();

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredIsLeader = row.unwrap();
            allstoredisleader.stored_isleader.push(data);
        };

         */
    }
}