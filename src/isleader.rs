//! the module for reading the /api/v1/is-leader to identify the master leader
use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use std::{env, fs, error::Error, process, time::Instant, sync::mpsc::channel};
use serde_derive::{Serialize,Deserialize};
use log::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct IsLeader {
    #[serde(rename = "STATUS")]
    pub status: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredIsLeader {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub status: String,
}

#[derive(Debug)]
pub struct AllStoredIsLeader {
    pub stored_isleader: Vec<StoredIsLeader>
}

impl AllStoredIsLeader {
    pub fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize
    )
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allstoredisleader = AllStoredIsLeader::read_isleader(hosts, ports, parallel);
        allstoredisleader.save_snapshot(snapshot_number)
            .unwrap_or_else(|e| {
                error!("error saving snapshot: {}", e);
                process::exit(1);
            });

        info!("end snapshot: {:?}", timer.elapsed())
    }
    pub fn return_leader (
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
    pub fn read_isleader (
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
                        tx.send((format!("{}:{}", host, port), detail_snapshot_time, isleader)).expect("error sending data via tx (isleader)");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allstoredisleader = AllStoredIsLeader { stored_isleader: Vec::new() };
        for (hostname_port, detail_snapshot_time, isleader) in rx {
            allstoredisleader.stored_isleader.push(StoredIsLeader { hostname_port: hostname_port, timestamp: detail_snapshot_time, status: isleader.status.to_string() } );
        }
        allstoredisleader
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> IsLeader
    {
        if ! scan_port_addr( format!("{}:{}", host, port) ) {
            warn!("Warning: hostname:port {}:{} cannot be reached, skipping", host, port);
            return AllStoredIsLeader::parse_isleader(String::from(""))
        }
        if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}:{}/api/v1/is-leader", host, port)) {
            AllStoredIsLeader::parse_isleader(data_from_http.text().unwrap())
        } else {
            AllStoredIsLeader::parse_isleader(String::from(""))
        }
    }
    fn parse_isleader( http_output: String ) -> IsLeader
    {
        serde_json::from_str( &http_output )
            .unwrap_or_else(|_e| {
                IsLeader { status: "".to_string() }
            })
    }
    fn save_snapshot ( self, snapshot_number: i32 ) -> Result<(), Box<dyn Error>>
    {
        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number.to_string());

        let isleader_file = &current_snapshot_directory.join("isleader");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&isleader_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_isleader {
            writer.serialize(row)?;
        }
        writer.flush()?;

        Ok(())
    }
    fn read_snapshot( snapshot_number: &String, ) -> Result<AllStoredIsLeader, Box<dyn Error>>
    {
        let mut allstoredisleader = AllStoredIsLeader { stored_isleader: Vec::new() };

        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number);

        let isleader_file = &current_snapshot_directory.join("isleader");
        let file = fs::File::open(&isleader_file)?;

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

    #[test]
    fn unit_fetch_master_from_snapshot() {
        let hostname_port = AllStoredIsLeader::return_leader(&"22".to_string());
        //let hostname_port = String::from("haha");
        //let stored_isleader = AllStoredIsLeader::read_snapshot(&"22".to_string())
        //    .unwrap_or_else(|e| {
        //        error!("error reading snapshot: {}", e);
        //        process::exit(1);
        //    });
        //println!("{:?}", stored_isleader);
        //let leader = stored_isleader.stored_isleader.iter().filter(|r| r.status == "OK").map(|r| r.hostname_port.to_string()).next().unwrap();
        //let h = stored_isleader.stored_isleader.iter().filter(|r| r.status == "OK").map(|r| r.hostname_port.to_string()).next().unwrap();
        println!("{:?}", hostname_port);
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