//! The impls and functions
//!
use chrono::Local;
use std::{fmt, sync::mpsc::channel, time::Instant};
use log::*;
use colored::*;
use anyhow::Result;

use crate::isleader::AllIsLeader;
use crate::utility;
use crate::snapshot;
use crate::masters::{Masters, MastersDiff, PeerRole, MastersDiffFields};
use crate::Opts;
use crate::snapshot::read_snapshot_json;

impl fmt::Display for PeerRole {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Masters {
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let masters = Masters::read_masters(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "masters", masters.masters)?;

        info!("end snapshot: {:?}", timer.elapsed());
        Ok(())
    }
    pub fn new() -> Self {
        Default::default()
    }
    pub async fn read_masters(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> Masters
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
                        let mut masters = Masters::read_http(host, port);
                        masters.masters.iter_mut().for_each(|r| r.timestamp = Some(detail_snapshot_time));
                        masters.masters.iter_mut().for_each(|r| r.hostname_port = Some(format!("{}:{}", host, port)));
                        tx.send(masters).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut masters = Masters::new();

        for fetched_masters in rx {
            for master in fetched_masters.masters.into_iter().filter(|r| !r.instance_id.permanent_uuid.is_empty()) {
                masters.masters.push(master);
            }
        }

        masters
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> Masters
    {
        let data_from_http = utility::http_get(host, port, "api/v1/masters");
        Masters::parse_masters(data_from_http, host, port)
    }
    fn parse_masters(
        http_data: String,
        host: &str,
        port: &str,
    ) -> Masters {
        serde_json::from_str(&http_data )
            .unwrap_or_else(|e| {
                debug!("({}:{}) could not parse /api/v1/masters json data for masters, error: {}", host, port, e);
                Masters::new()
            })
    }
    pub fn print(
        &self,
        details_enable: &bool,
        leader_hostname: String,
    ) -> Result<()>
    {
        for row in &self.masters {
            // if details_enable is true then always continue
            // if details_enable is false, then hostname_port must equal to leader_hostname,
            // so only the masters information from the master leader is printed.
            if row.hostname_port != Some(leader_hostname.clone())
                && !*details_enable
            {
                continue;
            }
            // first row
            if *details_enable {
                print!("{} ", row.hostname_port.as_ref().unwrap());
            };
            print!("{} ", row.instance_id.permanent_uuid);
            // highlighted role
            match row.role.as_ref().unwrap_or(&PeerRole::default())
            {
                &PeerRole::LEADER => { print!("{} ", "LEADER".to_string().green().bold()) }
                &PeerRole::FOLLOWER => { print!("{} ", "FOLLOWER".to_string().green()) }
                &PeerRole::UNKNOWN_ROLE => { print!("{} ", "UNKNOWN_ROLE".to_string().red()) }
                others => { print!("{} ", others.to_string().yellow())}
            }
            println!("Placement: {}.{}.{}",
                     row.registration
                         .as_ref()
                         .and_then(|registration| registration.cloud_info.as_ref())
                         .and_then(|cloud_info| cloud_info.placement_cloud.as_ref())
                         .unwrap_or(&"-".to_string()),
                     row.registration
                         .as_ref()
                         .and_then(|registration| registration.cloud_info.as_ref())
                         .and_then(|cloud_info| cloud_info.placement_region.as_ref())
                         .unwrap_or(&"-".to_string()),
                     row.registration
                         .as_ref()
                         .and_then(|registration| registration.cloud_info.as_ref())
                         .and_then(|cloud_info| cloud_info.placement_zone.as_ref())
                         .unwrap_or(&"-".to_string())
            );
            // second row
            if *details_enable {
                print!("{} ", row.hostname_port
                    .as_ref()
                    .unwrap()
                );
            };
            // blank space, sequence_no, start_time_us
            println!("{} Seqno: {} Start time: {}",
                     " ".repeat(32),
                     row.instance_id.instance_seqno,
                     row.instance_id.start_time_us.unwrap_or_default()
            );
            // third row
            if *details_enable {
                print!("{} ", row.hostname_port
                    .as_ref()
                    .unwrap()
                );
            };
            // blank space, list of rpc addresses
            print!("{} RPC addresses: ( ", " ".repeat(32));
            for addresses in row.registration
                .as_ref()
                .and_then(|registration| registration.private_rpc_addresses.as_ref())
                .iter() {
                for address in *addresses {
                    print!("{}:{} ", address.host, address.port);
                }
            };
            println!(")");
            // fourth row
            if *details_enable {
                print!("{} ", row.hostname_port
                    .as_ref()
                    .unwrap()
                );
            };
            // blank space, list of http addresses
            print!("{} HTTP addresses: ( ", " ".repeat(32));
            for addresses in row.registration
                .as_ref()
                .and_then(|registration| registration.http_addresses.as_ref())
                .iter() {
                for address in *addresses {
                    print!("{}:{} ", address.host, address.port);
                }
            };
            println!(")");
            // fifth row: only if errors are reported
            if row.error.is_some() {
                if *details_enable {
                    print!("{} ", row.hostname_port
                        .as_ref()
                        .unwrap()
                    );
                };
                println!("{:#?}", row.error
                    .as_ref()
                    .unwrap()
                );
            };
        }
        Ok(())
    }
}

impl MastersDiff {
    pub fn new() -> Self { Default::default() }
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
    ) -> Result<MastersDiff>
    {
        let mut mastersdiff = MastersDiff::new();

        let mut masters = Masters::new();
        masters.masters = snapshot::read_snapshot_json(begin_snapshot, "masters")?;
        let master_leader = AllIsLeader::return_leader_snapshot(begin_snapshot)?;
        mastersdiff.first_snapshot(masters, master_leader);

        let mut masters = Masters::new();
        masters.masters = read_snapshot_json(end_snapshot, "masters")?;
        let master_leader = AllIsLeader::return_leader_snapshot(end_snapshot)?;
        mastersdiff.second_snapshot(masters, master_leader);

        Ok(mastersdiff)
    }
    fn first_snapshot(
        &mut self,
        masters: Masters,
        master_leader: String,
    )
    {
        if master_leader == *"" {
            self.master_found = false;
            return
        } else {
            self.master_found = true;
        }
        trace!("First snapshot: master_leader:{}, found:{}", master_leader, self.master_found);

        for master in masters.masters
            .iter()
            .filter(|r| r.hostname_port.as_ref().unwrap().clone() == master_leader.clone())
        {
            //println!("{}", master.instance_id.permanent_uuid.clone());

            self.btreemastersdiff
                .entry(master.instance_id.permanent_uuid.clone())
                .and_modify(|_| error!("Duplicate permanent_uuid entry: {}", master.instance_id.permanent_uuid))
                .or_insert(MastersDiffFields {
                    first_instance_seqno: master.instance_id.instance_seqno,
                    first_start_time_us: master.instance_id.start_time_us.unwrap_or_default(),
                    first_placement_cloud: master.registration
                        .as_ref()
                        .and_then(|registration| registration.cloud_info.as_ref())
                        .and_then(|cloud_info| cloud_info.placement_cloud.as_ref())
                        .unwrap_or(&"-".to_string())
                        .to_string(),
                    first_placement_region: master.registration
                        .as_ref()
                        .and_then(|registration| registration.cloud_info.as_ref())
                        .and_then(|cloud_info| cloud_info.placement_region.as_ref())
                        .unwrap_or(&"-".to_string())
                        .to_string(),
                    first_placement_zone: master.registration
                        .as_ref()
                        .and_then(|registration| registration.cloud_info.as_ref())
                        .and_then(|cloud_info| cloud_info.placement_zone.as_ref())
                        .unwrap_or(&"-".to_string())
                        .to_string(),
                    first_placement_uuid: master.registration
                        .as_ref()
                        .and_then(|registration| registration.placement_uuid.as_ref())
                        .unwrap_or(&"-".to_string())
                        .to_string(),
                    first_role: master.role
                        .as_ref()
                        .unwrap_or(&PeerRole::UNKNOWN_ROLE)
                        .clone(),
                    first_private_rpc_addresses: master.registration
                        .as_ref()
                        .and_then(|registration| registration.private_rpc_addresses.as_ref())
                        .unwrap_or(&Vec::new())
                        .iter()
                        .map(|r| format!("{}:{},", r.host, r.port))
                        .collect::<String>(),
                    first_http_addresses: master.registration
                        .as_ref()
                        .and_then(|registration| registration.http_addresses.as_ref())
                        .unwrap_or(&Vec::new())
                        .iter()
                        .map(|r| format!("{}:{},", r.host, r.port))
                        .collect::<String>(),
                    ..Default::default()
                    }
                );
        }
    }
    fn second_snapshot(
        &mut self,
        masters: Masters,
        master_leader: String,
    )
    {
        if master_leader == *"" {
            self.master_found = false;
            return
        } else {
            self.master_found = true;
        }
        trace!("Second snapshot: master_leader:{}, found:{}", master_leader, self.master_found);

        for master in masters.masters
            .iter()
            .filter(|r| r.hostname_port.as_ref().unwrap().clone() == master_leader.clone())
        {
            self.btreemastersdiff
                .entry(master.instance_id.permanent_uuid.clone())
                .and_modify(|mastersdifffields| {
                    mastersdifffields.second_instance_seqno = master.instance_id.instance_seqno;
                    mastersdifffields.second_start_time_us = master.instance_id.start_time_us.unwrap_or_default();
                    mastersdifffields.second_placement_cloud = master.registration
                        .as_ref()
                        .and_then(|registration| registration.cloud_info.as_ref())
                        .and_then(|cloud_info| cloud_info.placement_cloud.as_ref())
                        .unwrap_or(&"-".to_string())
                        .to_string();
                    mastersdifffields.second_placement_region = master.registration
                        .as_ref()
                        .and_then(|registration| registration.cloud_info.as_ref())
                        .and_then(|cloud_info| cloud_info.placement_region.as_ref())
                        .unwrap_or(&"-".to_string())
                        .to_string();
                    mastersdifffields.second_placement_zone = master.registration
                        .as_ref()
                        .and_then(|registration| registration.cloud_info.as_ref())
                        .and_then(|cloud_info| cloud_info.placement_zone.as_ref())
                        .unwrap_or(&"-".to_string())
                        .to_string();
                    mastersdifffields.second_placement_uuid = master.registration
                        .as_ref()
                        .and_then(|registration| registration.placement_uuid.as_ref())
                        .unwrap_or(&"-".to_string())
                        .to_string();
                    mastersdifffields.second_private_rpc_addresses = master.registration
                        .as_ref()
                        .and_then(|registration| registration.private_rpc_addresses.as_ref())
                        .unwrap_or(&Vec::new())
                        .iter()
                        .map(|r| format!("{}:{},", r.host, r.port))
                        .collect::<String>();
                    mastersdifffields.second_http_addresses = master.registration
                        .as_ref()
                        .and_then(|registration| registration.http_addresses.as_ref())
                        .unwrap_or(&Vec::new())
                        .iter()
                        .map(|r| format!("{}:{},", r.host, r.port))
                        .collect::<String>();
                    mastersdifffields.second_role = master.role
                        .as_ref()
                        .unwrap_or(&PeerRole::UNKNOWN_ROLE)
                        .clone();
                    }
                )
                .or_insert(MastersDiffFields {
                    second_instance_seqno: master.instance_id.instance_seqno,
                    second_start_time_us: master.instance_id.start_time_us.unwrap_or_default(),
                    second_placement_cloud: master.registration
                        .as_ref()
                        .and_then(|registration| registration.cloud_info.as_ref())
                        .and_then(|cloud_info| cloud_info.placement_cloud.as_ref())
                        .unwrap_or(&"-".to_string())
                        .to_string(),
                    second_placement_region: master.registration
                        .as_ref()
                        .and_then(|registration| registration.cloud_info.as_ref())
                        .and_then(|cloud_info| cloud_info.placement_region.as_ref())
                        .unwrap_or(&"-".to_string())
                        .to_string(),
                    second_placement_zone: master.registration
                        .as_ref()
                        .and_then(|registration| registration.cloud_info.as_ref())
                        .and_then(|cloud_info| cloud_info.placement_zone.as_ref())
                        .unwrap_or(&"-".to_string())
                        .to_string(),
                    second_placement_uuid: master.registration
                        .as_ref()
                        .and_then(|registration| registration.placement_uuid.as_ref())
                        .unwrap_or(&"-".to_string())
                        .to_string(),
                    second_role: master.role
                        .as_ref()
                        .unwrap_or(&PeerRole::UNKNOWN_ROLE)
                        .clone(),
                    second_private_rpc_addresses: master.registration
                        .as_ref()
                        .and_then(|registration| registration.private_rpc_addresses.as_ref())
                        .unwrap_or(&Vec::new())
                        .iter()
                        .map(|r| format!("{}:{},", r.host, r.port))
                        .collect::<String>(),
                    second_http_addresses: master.registration
                        .as_ref()
                        .and_then(|registration| registration.http_addresses.as_ref())
                        .unwrap_or(&Vec::new())
                        .iter()
                        .map(|r| format!("{}:{},", r.host, r.port))
                        .collect::<String>(),
                    ..Default::default()
                });
        }
    }
    pub fn print(
        &self,
    )
    {
        if ! self.master_found {
            println!("Master leader was not found in the hosts specified, skipping masters diff.");
            return;
        }
        for (permanent_uuid, row) in &self.btreemastersdiff {
            debug!("uuid: {}", permanent_uuid);
            // first check: are both situations equal?
            // if so, don't display anything: this is a diff.
            #[allow(clippy::nonminimal_bool)]
            if row.first_instance_seqno == row.second_instance_seqno
                && row.first_start_time_us == row.second_start_time_us
                && row.first_placement_cloud == row.second_placement_cloud
                && row.first_placement_region == row.second_placement_region
                && row.first_placement_cloud == row.second_placement_cloud
                && row.first_role == row.second_role
                && row.first_private_rpc_addresses == row.second_private_rpc_addresses
                && row.first_http_addresses == row.second_http_addresses
            {
                // rows are equal, continue loop
                debug!("equal, next master");
                continue;
            }
            else if row.second_instance_seqno == 0
            {
                // if the second_instance_seqno is zero, then the first_instance_seqno likely is
                // non-zero, indicating that this master was alive in the first snapshot, and gone
                // away in the second.
                debug!("role: {}->{} placement: {}.{}.{}->{}.{}.{}", row.first_role, row.second_role, row.first_placement_cloud, row.first_placement_region, row.first_placement_cloud, row.second_placement_cloud, row.second_placement_region, row.second_placement_cloud);
                // if the second instance_seqno is zero, it means the permanent_uuid is gone in the second snapshot.
                // this means the masters is gone.
                println!("{} Masters:  {} Role: {}, Previous placement: {}.{}.{}",
                    "-".to_string().red(),
                    permanent_uuid,
                    row.first_role,
                    row.first_placement_cloud,
                    row.first_placement_region,
                    row.first_placement_zone,
                );
                debug!("Seq#:{}->{} Start time:{}->{}", row.first_instance_seqno, row.second_instance_seqno, row.first_start_time_us, row.second_start_time_us);
                println!("{} Seq#: {}, Start time: {}",
                    " ".repeat(44),
                    row.first_instance_seqno,
                    row.first_start_time_us,
                );
                debug!("rpc: {}->{}", row.first_private_rpc_addresses, row.second_private_rpc_addresses);
                println!("{} RPC ( {} )",
                    " ".repeat(44),
                    row.first_private_rpc_addresses,
                );
                debug!("http: {}->{}", row.first_http_addresses, row.second_http_addresses);
                println!("{} HTTP ( {} )",
                    " ".repeat(44),
                    row.first_http_addresses,
                );
            }
            else if row.first_instance_seqno == 0
            {
                // if the first instance_seqno is zero, then the second_instance_seqno likely is
                // non-zero, indicating that this master only was alive in the second snapshot.
                // this means it's a new master.
                debug!("role: {}->{} placement: {}.{}.{}->{}.{}.{}", row.first_role, row.second_role, row.first_placement_cloud, row.first_placement_region, row.first_placement_cloud, row.second_placement_cloud, row.second_placement_region, row.second_placement_cloud);
                println!("{} Masters:  {} Role: {}, Placement: {}.{}.{}",
                    "+".to_string().green(),
                    permanent_uuid,
                    row.second_role,
                    row.second_placement_cloud,
                    row.second_placement_region,
                    row.second_placement_zone,
                );
                debug!("Seq#:{}->{} Start time:{}->{}", row.first_instance_seqno, row.second_instance_seqno, row.first_start_time_us, row.second_start_time_us);
                println!("{} Seq#: {}, Start time: {}",
                    " ".repeat(44),
                    row.second_instance_seqno,
                    row.second_start_time_us,
                );
                debug!("rpc: {}->{}", row.first_private_rpc_addresses, row.second_private_rpc_addresses);
                println!("{} RPC ( {} )",
                    " ".repeat(44),
                    row.second_private_rpc_addresses,
                );
                debug!("http: {}->{}", row.first_http_addresses, row.second_http_addresses);
                println!("{} HTTP ( {} )",
                    " ".repeat(44),
                    row.second_http_addresses,
                );
            }
            else
            {
                    // okay, so they are not equal.
                    // print out the master, and highlight the changes

                    // first row
                    print!("{} Masters:  {} ",
                        "=".to_string().yellow(),
                        permanent_uuid,
                    );
                    debug!("Role: {}->{} Placement: {}.{}.{}->{}.{}.{}", row.first_role, row.second_role, row.first_placement_cloud, row.first_placement_region, row.first_placement_cloud, row.second_placement_cloud, row.second_placement_region, row.second_placement_cloud);
                    if row.first_role != row.second_role
                    {
                        print!("Role: {}->{} ",
                            row.first_role.to_string().yellow(),
                            row.second_role.to_string().yellow(),
                        );
                    }
                    else
                    {
                        print!("Role: {} ", row.second_role);
                    };
                    if row.first_placement_cloud != row.second_placement_cloud
                    {
                        print!("{}->{}.",
                            row.first_placement_cloud.to_string().yellow(),
                            row.second_placement_cloud.to_string().yellow(),
                        );
                    }
                    else
                    {
                        print!("Placement: {}.", row.second_placement_cloud);
                    };
                    if row.first_placement_region != row.second_placement_region
                    {
                        print!("{}->{}.",
                            row.first_placement_region.to_string().yellow(),
                            row.second_placement_region.to_string().yellow(),
                        );
                    }
                    else
                    {
                        print!("{}.", row.second_placement_region);
                    };
                    if row.first_placement_zone != row.second_placement_zone
                    {
                        println!("{}->{}",
                            row.first_placement_zone.to_string().yellow(),
                            row.second_placement_zone.to_string().yellow(),
                        );
                    }
                    else
                    {
                        println!("{}", row.second_placement_zone);
                    };
                    // second row
                    debug!("Seq#: {}->{} Start time: {}->{}", row.first_instance_seqno, row.second_instance_seqno, row.first_start_time_us, row.second_start_time_us);
                    if row.first_instance_seqno != row.second_instance_seqno
                    {
                        print!("{} Seq#: {}->{} ",
                            " ".repeat(44),
                            row.first_instance_seqno.to_string().yellow(),
                            row.second_instance_seqno.to_string().yellow(),
                        );
                    }
                    else
                    {
                        print!("{} Seq#: {} ",
                            " ".repeat(44),
                            row.second_instance_seqno,
                        );
                    }
                    if row.first_start_time_us != row.second_start_time_us
                    {
                        println!("Start time: {}->{}",
                            row.first_start_time_us.to_string().yellow(),
                            row.second_start_time_us.to_string().yellow()
                        );
                    }
                    else
                    {
                        println!("Start time: {}", row.second_start_time_us);
                    }
                    // third row
                    debug!("rpc: {} - {}", row.first_http_addresses.clone(), row.second_http_addresses.clone());
                    if row.first_private_rpc_addresses != row.second_private_rpc_addresses
                    {
                        println!("{} RPC: {}->{}",
                            " ".repeat(44),
                            row.first_private_rpc_addresses.clone().yellow(),
                            row.second_private_rpc_addresses.clone().yellow(),
                        );
                    }
                    else
                    {
                        println!("{} RPC: {}",
                            " ".repeat(44),
                            row.second_private_rpc_addresses,
                        );
                    }
                    // fourth row
                    debug!("http: {} - {}", row.first_http_addresses.clone(), row.second_http_addresses.clone());
                    if row.first_http_addresses != row.second_http_addresses
                    {
                        println!("{} HTTP: {}->{}",
                            " ".repeat(44),
                            row.first_http_addresses.clone().yellow(),
                            row.second_http_addresses.clone().yellow(),
                        );
                    }
                    else
                    {
                        println!("{} HTTP: {}",
                            " ".repeat(44),
                            row.second_http_addresses,
                        );
                    }
            }
        }
    }
    pub async fn adhoc_read_first_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let masters = Masters::read_masters(hosts, ports, parallel).await;
        let master_leader = AllIsLeader::return_leader_http(hosts, ports, parallel).await;
        self.first_snapshot(masters, master_leader);
    }
    pub async fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let masters = Masters::read_masters(hosts, ports, parallel).await;
        let master_leader = AllIsLeader::return_leader_http(hosts, ports, parallel).await;
        self.second_snapshot(masters, master_leader);
    }
}

pub async fn masters_diff(
    options: &Opts,
) -> Result<()>
{
    if options.begin.is_none() || options.end.is_none() {
        snapshot::Snapshot::print()?;
    }
    if options.snapshot_list { return Ok(()) };

    let (begin_snapshot, end_snapshot, _begin_snapshot_row) = snapshot::Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end)?;

    let mastersdiff = MastersDiff::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    mastersdiff.print();

    Ok(())
}

pub async fn print_masters(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    match options.print_masters.as_ref().unwrap() {
        Some(snapshot_number) => {

            let mut masters = Masters::new();
            masters.masters = snapshot::read_snapshot_json(snapshot_number, "masters")?;
            let leader_hostname = AllIsLeader::return_leader_snapshot(snapshot_number)?;
            masters.print(&options.details_enable, leader_hostname)?;

        }
        None => {
            let masters = Masters::read_masters(&hosts, &ports, parallel).await;
            let leader_hostname = AllIsLeader::return_leader_http(&hosts, &ports, parallel).await;
            masters.print(&options.details_enable, leader_hostname)?;
        }
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_master_data() {
        let json = r#"
{
  "masters": [
    {
      "instance_id": {
        "permanent_uuid": "3fc1141619304cffa2f0a345d37a51c2",
        "instance_seqno": 1657972299220554,
        "start_time_us": 1657972299220554
      },
      "registration": {
        "private_rpc_addresses": [
          {
            "host": "yb-1.local",
            "port": 7100
          }
        ],
        "http_addresses": [
          {
            "host": "yb-1.local",
            "port": 7000
          }
        ],
        "cloud_info": {
          "placement_cloud": "local",
          "placement_region": "local",
          "placement_zone": "local"
        },
        "placement_uuid": ""
      },
      "role": "LEADER"
    },
    {
      "instance_id": {
        "permanent_uuid": "f32d67fbf54545b18d3aef17fee4032b",
        "instance_seqno": 1657972325360336,
        "start_time_us": 1657972325360336
      },
      "registration": {
        "private_rpc_addresses": [
          {
            "host": "yb-2.local",
            "port": 7100
          }
        ],
        "http_addresses": [
          {
            "host": "yb-2.local",
            "port": 7000
          }
        ],
        "cloud_info": {
          "placement_cloud": "local",
          "placement_region": "local",
          "placement_zone": "local"
        },
        "placement_uuid": ""
      },
      "role": "FOLLOWER"
    },
    {
      "instance_id": {
        "permanent_uuid": "b44e60f6a7f54aae98de54ee2e00736d",
        "instance_seqno": 1657972347087226,
        "start_time_us": 1657972347087226
      },
      "registration": {
        "private_rpc_addresses": [
          {
            "host": "yb-3.local",
            "port": 7100
          }
        ],
        "http_addresses": [
          {
            "host": "yb-3.local",
            "port": 7000
          }
        ],
        "cloud_info": {
          "placement_cloud": "local",
          "placement_region": "local",
          "placement_zone": "local"
        },
        "placement_uuid": ""
      },
      "role": "FOLLOWER"
    }
  ]
}
        "#.to_string();
        let result = Masters::parse_masters(json, "", "");
        assert!(result.masters[0].error.is_none());
    }
/*
    #[tokio::test]
    async fn integration_parse_masters() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();
        let result = AllGetMasterRegistrationRequestPB::read_masters(&vec![&hostname], &vec![&port], 1).await;

        // a MASTER only will generate entities on each master (!)
        assert!(!result.getmasterregistrationrequestpb[0].instance_id.permanent_uuid.is_empty());
        assert!(!result.getmasterregistrationrequestpb[0].registration.unwrap().private_rpc_addresses[0].unwrap().is_empty());
        assert!(!result.getmasterregistrationrequestpb[0].registration.unwrap().private_rpc_addresses[0].host[0].is_empty());
    }

 */
}