//! The impls and functions
//!
use chrono::Local;
use regex::Regex;
use std::{sync::mpsc::channel, time::Instant};
use log::*;
use colored::*;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::vars::{AllVars, Vars, VarsDiff, VarsDiffFields};
use crate::Opts;

impl AllVars {
    pub fn new() -> Self { Default::default() }
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allvars = AllVars::read_vars(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "vars", allvars.vars)?;

        info!("end snapshot: {:?}", timer.elapsed());
        Ok(())
    }
    pub async fn read_vars(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllVars
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
                        let mut vars = AllVars::read_http(host, port);
                        vars.timestamp = Some(detail_snapshot_time);
                        vars.hostname_port = Some(format!("{}:{}", host, port));
                        tx.send(vars).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allvars = AllVars::new();

        for vars in rx
        {
            allvars.vars.push(vars);
        }

        allvars
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> Vars
    {
        let data_from_http = utility::http_get(host, port, "api/v1/varz");
        AllVars::parse_vars(data_from_http, host, port)
    }
    fn parse_vars(
        http_data: String,
        host: &str,
        port: &str,
    ) -> Vars
    {
        serde_json::from_str( &http_data )
            .unwrap_or_else( |e| {
                debug!("({}:{}) could not parse /api/v1/varz json data for varz, error: {}", host, port, e);
                Vars::default()
            })
    }
    pub async fn print(
        &self,
        details_enable: &bool,
        hostname_filter: &Regex,
        stat_name_filter: &Regex,
    )
    {
        for host_entry in self.vars.iter()
        {
            if hostname_filter.is_match(&host_entry.hostname_port.clone().expect("hostname:port should be set"))
            {
                for flag in &host_entry.flags
                {
                    if stat_name_filter.is_match(&flag.name) {
                        if flag.vars_type == *"Default"
                            && !*details_enable
                        {
                            continue;
                        };
                        println!("{:20} {:50} {:40} {}",
                                 &host_entry.hostname_port.clone().expect("hostname:port should be set"),
                                 flag.name,
                                 flag.value,
                                 flag.vars_type
                        );
                    }
                };
            };
        };
    }
}

impl VarsDiff {
    pub fn new() -> Self { Default::default() }
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
    ) -> Result<VarsDiff>
    {
        let mut varsdiff = VarsDiff::new();

        let mut allvars = AllVars::new();
        allvars.vars = snapshot::read_snapshot_json(begin_snapshot, "vars")?;
        varsdiff.first_snapshot(allvars);

        let mut allvars = AllVars::new();
        allvars.vars = snapshot::read_snapshot_json(end_snapshot, "vars")?;
        varsdiff.second_snapshot(allvars);

        Ok(varsdiff)
    }
    fn first_snapshot(
        &mut self,
        allvars: AllVars,
    )
    {
        for vars in allvars.vars
        {
            for var in vars.flags
            {
                self.btreevarsdiff
                    .entry((vars.hostname_port.clone().expect("hostname:port should be set"), var.name.clone()) )
                    .and_modify(|_| error!("Duplicate hostname:port + var name entry: {}, {}", vars.hostname_port.clone().expect("hostname:port should be set"), var.name))
                    .or_insert( VarsDiffFields {
                        first_value: var.value,
                        first_vars_type: var.vars_type,
                        ..Default::default()
                    });
            }
        }
    }
    fn second_snapshot(
        &mut self,
        allvars: AllVars,
    )
    {
        for vars in allvars.vars
        {
            for var in vars.flags
            {
                self.btreevarsdiff
                    .entry((vars.hostname_port.clone().expect("hostname:port should be set"), var.name.clone()) )
                    .and_modify(|varsdifffields| {
                        varsdifffields.second_value = var.value.clone();
                        varsdifffields.second_vars_type = var.vars_type.clone();
                    })
                    .or_insert( VarsDiffFields {
                        second_value: var.value,
                        second_vars_type: var.vars_type,
                        ..Default::default()
                    });
            }
        }
    }
    pub fn print(
        &self,
    )
    {
        for ((hostname_port, name), row) in self.btreevarsdiff.iter() {
            if row.first_value == row.second_value
                && row.first_vars_type == row.second_vars_type
                || row.first_value.is_empty()
                || row.second_value.is_empty()
            {
                continue;
            }
            else
            {
                print!("{} {:20} Vars: {:50} ", "*".to_string().yellow(), hostname_port, name);
                if row.first_value != row.second_value
                {
                    print!("{}->{} ", row.first_value.yellow(), row.second_value.yellow());
                }
                else
                {
                    print!("{} ", row.second_value);
                };
                if row.first_vars_type != row.second_vars_type
                {
                    println!("{}->{}", row.first_vars_type.yellow(), row.second_vars_type.yellow());
                }
                else
                {
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
        let allvars = AllVars::read_vars(hosts, ports, parallel).await;
        self.first_snapshot(allvars);
    }
    pub async fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allvars = AllVars::read_vars(hosts, ports, parallel).await;
        self.second_snapshot(allvars);
    }
}

pub async fn print_vars(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    let hostname_filter = utility::set_regex(&options.hostname_match);
    let stat_name_filter = utility::set_regex(&options.stat_name_match);
    match options.print_vars.as_ref().unwrap()
    {
        Some(snapshot_number) => {

            let mut allvars = AllVars::new();
            allvars.vars = snapshot::read_snapshot_json(snapshot_number, "vars")?;
            allvars.print(&options.details_enable, &hostname_filter, &stat_name_filter).await;
        }
        None => {
            let allvars = AllVars::read_vars(&hosts, &ports, parallel).await;
            allvars.print(&options.details_enable, &hostname_filter, &stat_name_filter).await;
        }
    }
    Ok(())
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
        let result = AllVars::parse_vars(gflags, "", "");
        assert_eq!(result.flags[0].name, "log_filename");
        assert_eq!(result.flags[0].value, "yb-master");
        assert_eq!(result.flags[0].vars_type, "NodeInfo");
    }

    #[tokio::test]
    async fn integration_parse_vars_master()
    {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let allvars = AllVars::read_vars(&vec![&hostname], &vec![&port], 1).await;

        // the master must have gflags
        assert!(!allvars.vars.is_empty());
    }
    #[tokio::test]
    async fn integration_parse_vars_tserver()
    {
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();

        let allvars = AllVars::read_vars(&vec![&hostname], &vec![&port], 1).await;

        // the master must have gflags
        assert!(!allvars.vars.is_empty());
    }
}