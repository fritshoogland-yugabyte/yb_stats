use chrono::{DateTime, Local};
use regex::Regex;
use std::{sync::mpsc::channel, time::Instant, collections::BTreeMap};
use serde_derive::{Serialize,Deserialize};
use log::*;
use colored::*;
use anyhow::Result;
use crate::utility::{scan_host_port, http_get};
use crate::snapshot::{read_snapshot, save_snapshot};

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

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AllStoredVars {
    pub stored_vars: Vec<StoredVars>,
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
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allvars = AllStoredVars::read_vars(hosts, ports, parallel).await;
        save_snapshot(snapshot_number, "vars", allvars.stored_vars)?;

        info!("end snapshot: {:?}", timer.elapsed());
        Ok(())
    }
    pub fn new() -> Self {
        Default::default()
    }
    pub async fn read_vars(
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

        let mut allstoredvars = AllStoredVars::new();

        for (hostname_port, detail_snapshot_time, vars) in rx {
            allstoredvars.split_into_vectors(vars, &hostname_port, detail_snapshot_time);
        }

        allstoredvars
    }
    fn split_into_vectors(
        &mut self,
        allvars: AllVars,
        hostname_port: &str,
        detail_snapshot_time: DateTime<Local>,
    )
    {
        for var in allvars.flags {
            self.stored_vars.push( StoredVars {
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
    ) -> AllVars
    {
        let data_from_http = if scan_host_port( host, port) {
            http_get(host, port, "api/v1/varz")
        } else {
            String::new()
        };
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
                debug!("({}:{}) could not parse /api/v1/varz json data for varz, error: {}", host, port, e);
                AllVars { flags: Vec::new() }
            })
    }
    pub async fn print(
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

#[derive(Default)]
pub struct SnapshotDiffBTreeMapsVars {
    pub btreemap_snapshotdiff_vars: BTreeMapSnapshotDiffVars,
}

impl SnapshotDiffBTreeMapsVars {
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
    ) -> Result<SnapshotDiffBTreeMapsVars>
    {
        let mut allstoredvars = AllStoredVars::new();
        allstoredvars.stored_vars = read_snapshot(begin_snapshot, "vars")?;

        let mut vars_snapshot_diff = SnapshotDiffBTreeMapsVars::first_snapshot(allstoredvars);

        let mut allstoredvars = AllStoredVars::new();
        allstoredvars.stored_vars = read_snapshot(end_snapshot, "vars")?;
        vars_snapshot_diff.second_snapshot(allstoredvars);

        Ok(vars_snapshot_diff)
    }
    pub fn new() -> Self {
        Default::default()
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
    pub async fn adhoc_read_first_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allstoredvars = AllStoredVars::read_vars(hosts, ports, parallel).await;
        SnapshotDiffBTreeMapsVars::first_snapshot(allstoredvars);
    }
    pub async fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allstoredvars = AllStoredVars::read_vars(hosts, ports, parallel).await;
        self.second_snapshot(allstoredvars);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::utility_test::*;

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

    #[tokio::test]
    async fn integration_parse_vars_master()
    {
        let hostname = get_hostname_master();
        let port = get_port_master();

        let allstoredvars = AllStoredVars::read_vars(&vec![&hostname], &vec![&port], 1).await;

        // the master must have gflags
        assert!(!allstoredvars.stored_vars.is_empty());
    }
    #[tokio::test]
    async fn integration_parse_vars_tserver()
    {
        let hostname = get_hostname_tserver();
        let port = get_port_tserver();

        let allstoredvars = AllStoredVars::read_vars(&vec![&hostname], &vec![&port], 1).await;

        // the master must have gflags
        assert!(!allstoredvars.stored_vars.is_empty());
    }
}