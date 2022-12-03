use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use regex::Regex;
use std::{fs, process, sync::mpsc::channel, time::Instant, error::Error, env, collections::BTreeMap};
use serde_derive::{Serialize,Deserialize};
use log::*;
use colored::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct AllVars {
    pub flags: Vec<Vars>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Vars {
    pub name: String,
    pub value: String,
    #[serde(rename = "type")]
    pub vars_type: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AllStoredVars {
    stored_vars: Vec<StoredVars>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredVars {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub name: String,
    pub value: String,
    pub vars_type: String,
}

impl AllStoredVars {
    pub fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    )
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allvars = AllStoredVars::read_vars(hosts, ports, parallel);

        allvars.save_snapshot(snapshot_number)
            .unwrap_or_else(|e| {
                error!("error saving snapshot: {}", e);
                process::exit(1);
            });

        info!("end snapshot: {:?}", timer.elapsed())
    }
    pub fn read_vars(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllStoredVars
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
                        let vars = AllStoredVars::read_http(host, port);
                        tx.send((format!("{}:{}", host, port), detail_snapshot_time, vars)).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allstoredvars = AllStoredVars {
            stored_vars: Vec::new(),
        };
        for (hostname_port, detail_snapshot_time, vars) in rx {
            AllStoredVars::split_into_vectors(vars, &hostname_port, detail_snapshot_time, &mut allstoredvars);
        }

        allstoredvars
    }
    fn split_into_vectors(
        allvars: AllVars,
        hostname_port: &str,
        detail_snapshot_time: DateTime<Local>,
        allstoredvars: &mut AllStoredVars,
    )
    {
        for var in allvars.flags {
            allstoredvars.stored_vars.push( StoredVars {
                hostname_port: hostname_port.to_string(),
                timestamp: detail_snapshot_time,
                name: var.name.to_string(),
                value: var.value.to_string(),
                vars_type: var.vars_type.to_string(),
            });
        }
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> AllVars {
        if ! scan_port_addr(format!("{}:{}", host, port)) {
            warn!("hostname: port {}:{} cannot be reached, skipping", host, port);
            return AllStoredVars::parse_vars(String::from(""), "", "")
        };
        let data_from_http = reqwest::blocking::get(format!("http://{}:{}/api/v1/varz", host, port))
            .unwrap_or_else(|e| {
                error!("Fatal: error reading from URL: {}", e);
                process::exit(1);
            })
            .text().unwrap();
        AllStoredVars::parse_vars(data_from_http, host, port)
    }
    fn parse_vars(
        vars_data: String,
        host: &str,
        port: &str,
    ) -> AllVars
    {
        serde_json::from_str( &vars_data )
            .unwrap_or_else( |e| {
                info!("({}:{}) could not parse /api/v1/varz json data for varz, error: {}", host, port, e);
                AllVars { flags: Vec::new() }
            })
    }
    fn save_snapshot ( self, snapshot_number: i32 ) -> Result<(), Box<dyn Error>>
    {
        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number.to_string());

        let vars_file = &current_snapshot_directory.join("vars");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(vars_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_vars {
            writer.serialize(row)?;
        }
        writer.flush()?;

        Ok(())
    }
    pub fn read_snapshot( snapshot_number: &String ) -> Result<AllStoredVars, Box<dyn Error>>
    {
        let mut allstoredvars = AllStoredVars {
            stored_vars: Vec::new(),
        };

        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(snapshot_number);

        let vars_file = &current_snapshot_directory.join("vars");
        let file = fs::File::open(vars_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredVars = row?;
            allstoredvars.stored_vars.push(data);
        };

        Ok(allstoredvars)
    }
    pub fn print(
        &self,
        details_enable: &bool,
        hostname_filter: &Regex,
        stat_name_filter: &Regex,
    )
    {
        info!("print vars");

        for row in self.stored_vars.iter() {
            if hostname_filter.is_match(&row.hostname_port.clone())
                && stat_name_filter.is_match( &row.name.clone())
            {
                if row.vars_type == *"Default" && ! *details_enable {
                    continue;
                };
                println!("{:20} {:50} {:40} {}", row.hostname_port, row.name, row.value, row.vars_type);
            };
        };
    }
}

#[derive(Debug)]
pub struct SnapshotDiffStoredVars {
    pub first_value: String,
    pub first_vars_type: String,
    pub second_value: String,
    pub second_vars_type: String,
}

impl SnapshotDiffStoredVars {
    fn first_snapshot( storedvars: StoredVars ) -> Self
    {
        Self
        {
            first_value: storedvars.value.to_string(),
            first_vars_type: storedvars.vars_type,
            second_value: "".to_string(),
            second_vars_type: "".to_string(),
        }
    }
    fn second_snapshot_new( storedvars: StoredVars ) -> Self
    {
        Self
        {
            first_value: "".to_string(),
            first_vars_type: "".to_string(),
            second_value: storedvars.value.to_string(),
            second_vars_type: storedvars.vars_type,
        }
    }
    fn second_snapshot_existing( storedvars_diff_row: &mut SnapshotDiffStoredVars, storedvars: StoredVars ) -> Self
    {
        Self
        {
            first_value: storedvars_diff_row.first_value.to_string(),
            first_vars_type: storedvars_diff_row.first_vars_type.to_string(),
            second_value: storedvars.value.to_string(),
            second_vars_type: storedvars.vars_type,
        }
    }
}
type BTreeMapSnapshotDiffVars = BTreeMap<(String, String), SnapshotDiffStoredVars>;

pub struct SnapshotDiffBTreeMapsVars {
    pub btreemap_snapshotdiff_vars: BTreeMapSnapshotDiffVars,
}
impl SnapshotDiffBTreeMapsVars {
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
    ) -> SnapshotDiffBTreeMapsVars
    {
        let allstoredvars = AllStoredVars::read_snapshot(begin_snapshot)
            .unwrap_or_else(|e| {
                error!("Fatal: error reading snapshot: {}", e);
                process::exit(1);
            });
        let mut vars_snapshot_diff = SnapshotDiffBTreeMapsVars::first_snapshot(allstoredvars);

        let allstoredvars = AllStoredVars::read_snapshot(end_snapshot)
            .unwrap_or_else(|e| {
                error!("Fatal: error reading snapshot: {}", e);
                process::exit(1);
            });
        vars_snapshot_diff.second_snapshot(allstoredvars);

        vars_snapshot_diff
    }
    fn first_snapshot(
        allstoredvars: AllStoredVars,
    ) -> SnapshotDiffBTreeMapsVars
    {
        let mut snapshotdiff_btreemaps = SnapshotDiffBTreeMapsVars {
            btreemap_snapshotdiff_vars: Default::default(),
        };
        for row in allstoredvars.stored_vars.into_iter() {
            match snapshotdiff_btreemaps.btreemap_snapshotdiff_vars.get_mut(&(row.hostname_port.to_string(), row.name.to_string()) ) {
                Some( _vars_row ) => {
                    error!("Found second entry for first entry of vars based on hostname, name: {}, {}", &row.hostname_port, &row.name);
                },
                None => {
                    snapshotdiff_btreemaps.btreemap_snapshotdiff_vars.insert(
                        (row.hostname_port.to_string(), row.name.to_string()),
                        SnapshotDiffStoredVars::first_snapshot(row)
                    );
                },
            };
        }
        snapshotdiff_btreemaps
    }
    fn second_snapshot(
        &mut self,
        allstoredvars: AllStoredVars,
    )
    {
        for row in allstoredvars.stored_vars.into_iter() {
            match self.btreemap_snapshotdiff_vars.get_mut( &(row.hostname_port.to_string(), row.name.to_string()) ) {
                Some( vars_row ) => {
                    if vars_row.first_value == row.value
                    && vars_row.first_vars_type == row.vars_type
                    {
                        self.btreemap_snapshotdiff_vars.remove( &(row.hostname_port.to_string(), row.name.to_string()) );
                    } else {
                        *vars_row = SnapshotDiffStoredVars::second_snapshot_existing(vars_row, row);
                    }
                },
                None => {
                    self.btreemap_snapshotdiff_vars.insert(
                        (row.hostname_port.to_string(), row.name.to_string()),
                        SnapshotDiffStoredVars::second_snapshot_new(row)
                    );
                },

            }
        }
    }
    pub fn print(
        &self,
    )
    {
        for ((hostname_port, name), row) in self.btreemap_snapshotdiff_vars.iter() {
            // first value empty means a server started/became available during the snapshot. Do not report
            if row.first_value.is_empty() || row.second_value.is_empty() {
                //println!("{} {:20} Vars: {:50} {:40} {}", "+".to_string().green(), hostname_port, name, row.second_value, row.second_vars_type);
                continue;
            // second value empty means a server stopped during the snapshot. Do not report
            //} else if row.second_value.is_empty() {
            //   //println!("{} {:20} Vars: {:50} {:40} {}", "-".to_string().red(), hostname_port, name, row.first_value, row.first_vars_type);
            //    continue;
            } else {
                print!("{} {:20} Vars: {:50} ", "*".to_string().yellow(), hostname_port, name);
                if row.first_value != row.second_value {
                    print!("{}->{} ", row.first_value.yellow(), row.second_value.yellow());
                } else {
                    print!("{} ", row.second_value);
                };
                if row.first_vars_type != row.second_vars_type {
                    println!("{}->{}", row.first_vars_type.yellow(), row.second_vars_type.yellow());
                } else {
                    println!("{}", row.second_vars_type);
                };
            };
        }
    }
    pub fn adhoc_read_first_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> SnapshotDiffBTreeMapsVars
    {
        let allstoredvars = AllStoredVars::read_vars(hosts, ports, parallel);
        SnapshotDiffBTreeMapsVars::first_snapshot(allstoredvars)
    }
    pub fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allstoredvars = AllStoredVars::read_vars(hosts, ports, parallel);
        self.second_snapshot(allstoredvars);
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_regular_vars() {
        let gflags = r#"
{
    "flags":
    [
        {
          "name": "log_filename",
          "value": "yb-master",
          "type": "NodeInfo"
        },
        {
          "name": "placement_cloud",
          "value": "local",
          "type": "NodeInfo"
        },
        {
          "name": "placement_region",
          "value": "local",
          "type": "NodeInfo"
        },
        {
          "name": "placement_zone",
          "value": "local",
          "type": "NodeInfo"
        },
        {
          "name": "rpc_bind_addresses",
          "value": "0.0.0.0",
          "type": "NodeInfo"
        },
        {
          "name": "webserver_interface",
          "value": "",
          "type": "NodeInfo"
        },
        {
          "name": "webserver_port",
          "value": "7000",
          "type": "NodeInfo"
        },
        {
          "name": "db_block_cache_size_percentage",
          "value": "10",
          "type": "Custom"
        },
        {
          "name": "default_memory_limit_to_ram_ratio",
          "value": "0.29999999999999999",
          "type": "Custom"
        },
        {
          "name": "flagfile",
          "value": "/opt/yugabyte/conf/master.conf",
          "type": "Custom"
        }
    ]
}
"#.to_string();
        let result = AllStoredVars::parse_vars(gflags, "", "");
        assert_eq!(result.flags[0].name, "log_filename");
        assert_eq!(result.flags[0].value, "yb-master");
        assert_eq!(result.flags[0].vars_type, "NodeInfo");
    }

    /*
    use crate::utility;
    #[test]
    fn integration_parse_gflags_master() {
        let mut stored_gflags: Vec<StoredGFlags> = Vec::new();
        let detail_snapshot_time = Local::now();
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let gflags = read_gflags(hostname.as_str(), port.as_str());
        add_to_gflags_vector(gflags, format!("{}:{}", hostname, port).as_str(), detail_snapshot_time, &mut stored_gflags);
        // the master must have gflags
        assert!(!stored_gflags.is_empty());
    }
    #[test]
    fn integration_parse_gflags_tserver() {
        let mut stored_gflags: Vec<StoredGFlags> = Vec::new();
        let detail_snapshot_time = Local::now();
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();

        let gflags = read_gflags(hostname.as_str(), port.as_str());
        add_to_gflags_vector(gflags, format!("{}:{}", hostname, port).as_str(), detail_snapshot_time, &mut stored_gflags);
        // the tserver must have gflags
        assert!(!stored_gflags.is_empty());
    }

     */
}