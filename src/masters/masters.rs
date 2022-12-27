//use serde_derive::{Serialize,Deserialize};
use chrono::Local;
use std::{sync::mpsc::channel, time::Instant};
//use std::collections::BTreeMap;
use log::*;
//use colored::*;
use anyhow::Result;
use crate::isleader::AllIsLeader;
use crate::utility;
use crate::snapshot;
//use crate::masters::{AllStoredMasters, AllMasters, StoredMasters, StoredMasterError, StoredRpcAddresses, StoredHttpAddresses, Masters, SnapshotDiffStoredMasters, SnapshotDiffBTreeMapsMasters, PermanentUuidHttpAddress, PermanentUuidRpcAddress, AllGetMasterRegistrationRequestPB};
use crate::masters::{Masters, PeerRole};
use crate::Opts;

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
                        for master in masters.masters.iter_mut() {
                            master.hostname_port = Some(format!("{}:{}", host.clone(), port.clone()));
                            master.timestamp = Some(detail_snapshot_time);
                        }
                        tx.send(masters).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut masters = Masters::new();

        for fetched_masters in rx {
            for master in fetched_masters.masters.into_iter().filter(|r| r.instance_id.permanent_uuid != "") {
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
        snapshot_number: &String,
        details_enable: &bool,
    ) -> Result<()>
    {
        let leader_hostname = AllIsLeader::return_leader_snapshot(snapshot_number)?;

        for row in &self.masters {
            if row.hostname_port.as_ref().unwrap() != &leader_hostname
                && !*details_enable {
                continue
            }
            // first row
            if *details_enable {
                print!("{} ", row.hostname_port.as_ref().unwrap());
            };
            println!("{} {:?} Cloud: {}, Region: {}, Zone: {}",
                     row.instance_id.permanent_uuid,
                     row.role
                         .as_ref()
                         .unwrap_or(&PeerRole::UNKNOWN_ROLE),
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
            print!("{} RPC addresses: ( ", " ".repeat(32));
            for addresses in row.registration
                .as_ref()
                .and_then(|registration| registration.private_rpc_addresses.as_ref())
                .iter() {
                for address in *addresses {
                    print!("{}:{} ", address.host, address.port);
                }
            };
            println!(" )");
            // fourth row
            if *details_enable {
                print!("{} ", row.hostname_port
                    .as_ref()
                    .unwrap()
                );
            };
            print!("{} HTTP addresses: ( ", " ".repeat(32));
            for addresses in row.registration
                .as_ref()
                .and_then(|registration| registration.http_addresses.as_ref())
                .iter() {
                for address in *addresses {
                    print!("{}:{} ", address.host, address.port);
                }
            };
            println!(" )");
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
    pub async fn print_adhoc(
        &self,
        details_enable: &bool,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let leader_hostname = AllIsLeader::return_leader_http(hosts, ports, parallel).await;

        for row in &self.masters {
            if row.hostname_port.as_ref().unwrap() != &leader_hostname
                && !*details_enable {
                continue
            }
            //if row.hostname_port.as_ref().unwrap() == &leader_hostname {
            // first row
            if *details_enable {
                print!("{} ", row.hostname_port.as_ref().unwrap());
            };
            println!("{} {:?} Cloud: {}, Region: {}, Zone: {}",
                     row.instance_id.permanent_uuid,
                     row.role
                         .as_ref()
                         .unwrap_or(&PeerRole::UNKNOWN_ROLE),
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
            print!("{} RPC addresses: ( ", " ".repeat(32));
            for addresses in row.registration
                .as_ref()
                .and_then(|registration| registration.private_rpc_addresses.as_ref())
                .iter() {
                for address in *addresses {
                    print!("{}:{} ", address.host, address.port);
                }
            };
            println!(" )");
            // fourth row
            if *details_enable {
                print!("{} ", row.hostname_port
                    .as_ref()
                    .unwrap()
                );
            };
            print!("{} HTTP addresses: ( ", " ".repeat(32));
            for addresses in row.registration
                .as_ref()
                .and_then(|registration| registration.http_addresses.as_ref())
                .iter() {
                for address in *addresses {
                    print!("{}:{} ", address.host, address.port);
                }
            };
            println!(" )");
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
    }
}


/*
impl SnapshotDiffStoredMasters {
    fn first_snapshot( storedmasters: StoredMasters ) -> Self
    {
        Self {
            first_instance_seqno: storedmasters.instance_instance_seqno,
            first_start_time_us: storedmasters.start_time_us,
            first_registration_cloud_placement_cloud: storedmasters.registration_cloud_placement_cloud.to_string(),
            first_registration_cloud_placement_region: storedmasters.registration_cloud_placement_region.to_string(),
            first_registration_cloud_placement_zone: storedmasters.registration_cloud_placement_zone.to_string(),
            first_registration_placement_uuid: storedmasters.registration_placement_uuid.to_string(),
            first_role: storedmasters.role,
            second_instance_seqno: 0,
            second_start_time_us: 0,
            second_registration_cloud_placement_cloud: "".to_string(),
            second_registration_cloud_placement_region: "".to_string(),
            second_registration_cloud_placement_zone: "".to_string(),
            second_registration_placement_uuid: "".to_string(),
            second_role: "".to_string(),
        }
    }
    fn second_snapshot_new( storedmasters: StoredMasters ) -> Self
    {
        Self {
            first_instance_seqno: 0,
            first_start_time_us: 0,
            first_registration_cloud_placement_cloud: "".to_string(),
            first_registration_cloud_placement_region: "".to_string(),
            first_registration_cloud_placement_zone: "".to_string(),
            first_registration_placement_uuid: "".to_string(),
            first_role: "".to_string(),
            second_instance_seqno: storedmasters.instance_instance_seqno,
            second_start_time_us: storedmasters.start_time_us,
            second_registration_cloud_placement_cloud: storedmasters.registration_cloud_placement_cloud.to_string(),
            second_registration_cloud_placement_region: storedmasters.registration_cloud_placement_region.to_string(),
            second_registration_cloud_placement_zone: storedmasters.registration_cloud_placement_zone.to_string(),
            second_registration_placement_uuid: storedmasters.registration_placement_uuid.to_string(),
            second_role: storedmasters.role,
        }
    }
    fn second_snapshot_existing( storedmasters_diff_row: &mut SnapshotDiffStoredMasters, storedmasters: StoredMasters ) -> Self
    {
        Self {
            first_instance_seqno: storedmasters_diff_row.first_instance_seqno,
            first_start_time_us: storedmasters_diff_row.first_start_time_us,
            first_registration_cloud_placement_cloud: storedmasters_diff_row.first_registration_cloud_placement_cloud.to_string(),
            first_registration_cloud_placement_region: storedmasters_diff_row.first_registration_cloud_placement_region.to_string(),
            first_registration_cloud_placement_zone: storedmasters_diff_row.first_registration_cloud_placement_zone.to_string(),
            first_registration_placement_uuid: storedmasters_diff_row.first_registration_placement_uuid.to_string(),
            first_role: storedmasters_diff_row.first_role.to_string(),
            second_instance_seqno: storedmasters.instance_instance_seqno,
            second_start_time_us: storedmasters.start_time_us,
            second_registration_cloud_placement_cloud: storedmasters.registration_cloud_placement_cloud.to_string(),
            second_registration_cloud_placement_region: storedmasters.registration_cloud_placement_region.to_string(),
            second_registration_cloud_placement_zone: storedmasters.registration_cloud_placement_zone.to_string(),
            second_registration_placement_uuid: storedmasters.registration_placement_uuid.to_string(),
            second_role: storedmasters.role,
        }
    }
}

 */


/*
impl SnapshotDiffBTreeMapsMasters {
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
    ) -> Result<SnapshotDiffBTreeMapsMasters>
    {
        let mut allstoredmasters = AllStoredMasters::new();
        allstoredmasters.stored_masters = snapshot::read_snapshot(begin_snapshot, "masters")?;
        allstoredmasters.stored_rpc_addresses = snapshot::read_snapshot(begin_snapshot, "master_rpc_addresses")?;
        allstoredmasters.stored_http_addresses = snapshot::read_snapshot(begin_snapshot, "master_http_addresses")?;
        allstoredmasters.stored_master_error = snapshot::read_snapshot(begin_snapshot, "master_errors")?;

        let master_leader = AllIsLeader::return_leader_snapshot(begin_snapshot)?;
        let mut masters_snapshot_diff = SnapshotDiffBTreeMapsMasters::new();
        masters_snapshot_diff.first_snapshot(allstoredmasters, master_leader);

        let mut allstoredmasters = AllStoredMasters::new();
        allstoredmasters.stored_masters = snapshot::read_snapshot(end_snapshot, "masters")?;
        allstoredmasters.stored_rpc_addresses = snapshot::read_snapshot(end_snapshot, "master_rpc_addresses")?;
        allstoredmasters.stored_http_addresses = snapshot::read_snapshot(end_snapshot, "master_http_addresses")?;
        allstoredmasters.stored_master_error = snapshot::read_snapshot(end_snapshot, "master_errors")?;

        let master_leader = AllIsLeader::return_leader_snapshot(begin_snapshot)?;
        masters_snapshot_diff.second_snapshot(allstoredmasters, master_leader);

        Ok(masters_snapshot_diff)
    }
    pub fn new() -> Self {
        Default::default()
    }
    fn first_snapshot(
        &mut self,
        allstoredmasters: AllStoredMasters,
        master_leader: String,
    )
    {
        if master_leader == *"" {
            self.master_found = false;
            return;
        } else {
            self.master_found = true;
        };
        trace!("first snapshot: master_leader: {}, found: {}", master_leader, self.master_found);

        for row in allstoredmasters.stored_masters.into_iter().filter(|r| r.hostname_port == master_leader.clone()) {
            match self.btreemap_snapshotdiff_masters.get_mut( &row.instance_permanent_uuid ) {
                Some( _master_row ) => {
                    error!("Found second entry for first entry of masters based on instance permanent uuid: {}", &row.instance_permanent_uuid);
                },
                None => {
                    trace!("first snapshot: add master permanent uuid: {}", row.instance_permanent_uuid.to_string() );
                    self.btreemap_snapshotdiff_masters.insert(
                        row.instance_permanent_uuid.to_string(),
                        SnapshotDiffStoredMasters::first_snapshot(row)
                    );
                },
            };
        }
        for row in allstoredmasters.stored_http_addresses.into_iter().filter(|r| r.hostname_port == master_leader.clone()) {
            if self.first_http_addresses.iter().filter(|r| r.permanent_uuid == row.instance_permanent_uuid && r.hostname_port == format!("{}:{}", row.host, row.port)).count() == 0 {
                trace!("first snapshot: add http address: {}:{}", row.host.to_string(), row.port.to_string() );
                self.first_http_addresses.push( PermanentUuidHttpAddress { permanent_uuid: row.instance_permanent_uuid.to_string(), hostname_port: format!("{}:{}", row.host, row.port) });
            }
        }
        for row in allstoredmasters.stored_rpc_addresses.into_iter().filter(|r| r.hostname_port == master_leader.clone()) {
            if self.first_rpc_addresses.iter().filter(|r| r.permanent_uuid == row.instance_permanent_uuid && r.hostname_port == format!("{}:{}", row.host, row.port)).count() == 0 {
                trace!("first snapshot: add rpc address: {}:{}", row.host.to_string(), row.port.to_string() );
                self.first_rpc_addresses.push( PermanentUuidRpcAddress { permanent_uuid: row.instance_permanent_uuid.to_string(), hostname_port: format!("{}:{}", row.host, row.port) });
            }
        }
    }
    fn second_snapshot(
        &mut self,
        allstoredmasters: AllStoredMasters,
        master_leader: String,
    )
    {
        if master_leader == *"" {
            self.master_found = false;
            return;
        } else {
            self.master_found = true;
        };
        trace!("second snapshot: master_leader: {}, found: {}", master_leader, self.master_found);

        for row in allstoredmasters.stored_masters.into_iter().filter(|r| r.hostname_port == master_leader.clone()) {
            match self.btreemap_snapshotdiff_masters.get_mut( &row.instance_permanent_uuid.clone() ) {
                Some( master_row) => {
                    if master_row.first_instance_seqno == row.instance_instance_seqno
                        && master_row.first_start_time_us == row.start_time_us
                        && master_row.first_registration_cloud_placement_cloud == row.registration_cloud_placement_cloud
                        && master_row.first_registration_cloud_placement_region == row.registration_cloud_placement_region
                        && master_row.first_registration_cloud_placement_zone == row.registration_cloud_placement_zone
                        && master_row.first_role == row.role
                    {
                        // the second snapshot contains identicial values, so we remove it.
                        trace!("second snapshot: idential:remove: {}", row.instance_permanent_uuid.to_string() );
                        self.btreemap_snapshotdiff_masters.remove( &row.instance_permanent_uuid.clone() );
                    }
                    else {
                        trace!("second snapshot: CHANGED: {}", row.instance_permanent_uuid.to_string() );
                        *master_row = SnapshotDiffStoredMasters::second_snapshot_existing(master_row, row);
                    }
                },
                None => {
                    trace!("second snapshot: new: {}", row.instance_permanent_uuid.to_string() );
                    self.btreemap_snapshotdiff_masters.insert( row.instance_permanent_uuid.clone(), SnapshotDiffStoredMasters::second_snapshot_new(row));
                }
            }
        }
        for row in allstoredmasters.stored_http_addresses.into_iter().filter(|r| r.hostname_port == master_leader.clone()) {
            if self.second_http_addresses.iter().filter(|r| r.permanent_uuid == row.instance_permanent_uuid && r.hostname_port == format!("{}:{}", row.host, row.port)).count() == 0 {
                trace!("second snapshot: new http address: {}:{}", row.host.to_string(), row.port.to_string() );
                self.second_http_addresses.push( PermanentUuidHttpAddress { permanent_uuid: row.instance_permanent_uuid.to_string(), hostname_port: format!("{}:{}", row.host, row.port) });
            }
        }
        for row in allstoredmasters.stored_rpc_addresses.into_iter().filter(|r| r.hostname_port == master_leader.clone()) {
            if self.second_rpc_addresses.iter().filter(|r| r.permanent_uuid == row.instance_permanent_uuid && r.hostname_port == format!("{}:{}", row.host, row.port)).count() == 0 {
                trace!("second snapshot: new rpc address: {}:{}", row.host.to_string(), row.port.to_string() );
                self.second_rpc_addresses.push( PermanentUuidRpcAddress { permanent_uuid: row.instance_permanent_uuid.to_string(), hostname_port: format!("{}:{}", row.host, row.port) });
            }
        }
    }
    pub fn print(
        &self,
    )
    {
        if ! self.master_found {
            println!("Master leader was not found in hosts specified, skipping masters diff.");
            return;
        }
        for (permanent_uuid, row) in self.btreemap_snapshotdiff_masters.iter() {
            if row.second_instance_seqno == 0 {
                // If the second instance_seqno is zero, it means the permanent_uuid is gone. This means the master is gone.
                println!("{} Master {} {:8} Cloud: {}, Region: {}, Zone: {}", "-".to_string().red(), permanent_uuid, row.first_role, row.first_registration_cloud_placement_cloud, row.first_registration_cloud_placement_region, row.first_registration_cloud_placement_zone);
                println!("         {} Seqno: {} Start time: {}", " ".repeat(32), row.first_instance_seqno, row.first_start_time_us);
                print!("         {} Http ( ", " ".repeat(32));
                for http_address in self.first_http_addresses.iter().filter(|r| r.permanent_uuid == permanent_uuid.clone()) {
                    print!("{} ", http_address.hostname_port);
                }
                println!(")");
                print!("         {} Rpc ( ", " ".repeat(32));
                for http_address in self.first_rpc_addresses.iter().filter(|r| r.permanent_uuid == permanent_uuid.clone()) {
                    print!("{} ", http_address.hostname_port);
                }
                println!(")");
            } else if row.first_instance_seqno == 0 {
                // if the first instance_seqno is zero, it means the permanent_uuid has appeared after the first snapshot. This means it's a new master.
                println!("{} Master {} {:8} Cloud: {}, Region: {}, Zone: {}", "+".to_string().green(), permanent_uuid, row.second_role, row.second_registration_cloud_placement_cloud, row.second_registration_cloud_placement_region, row.second_registration_cloud_placement_zone);
                println!("         {} Seqno: {} Start time: {}", " ".repeat(32), row.second_instance_seqno, row.second_start_time_us);
                print!("         {} Http ( ", " ".repeat(32));
                for http_address in self.second_http_addresses.iter().filter(|r| r.permanent_uuid == permanent_uuid.clone()) {
                    print!("{} ", http_address.hostname_port);
                }
                println!(")");
                print!("         {} Rpc ( ", " ".repeat(32));
                for http_address in self.second_rpc_addresses.iter().filter(|r| r.permanent_uuid == permanent_uuid.clone()) {
                    print!("{} ", http_address.hostname_port);
                }
                println!(")");
            } else {
                // If both instance_seqno's have a number for the same permanent_uuid, it means we found something changed for a master.
                print!("{} Master {} ", "*".to_string().yellow(), permanent_uuid);
                if row.first_role != row.second_role {
                    print!("{}->{} ",row.first_role.to_string().yellow(), row.second_role.to_string().yellow());
                } else {
                    print!("{} ", row.second_role)
                };
                if row.first_registration_cloud_placement_cloud != row.second_registration_cloud_placement_cloud {
                    print!("Cloud: {}->{}, ",row.first_registration_cloud_placement_cloud.to_string().yellow(), row.second_registration_cloud_placement_cloud.to_string().yellow());
                } else {
                    print!("Cloud: {}, ", row.second_registration_cloud_placement_cloud)
                };
                if row.first_registration_cloud_placement_region != row.second_registration_cloud_placement_region {
                    print!("Region: {}->{}, ",row.first_registration_cloud_placement_region.to_string().yellow(), row.second_registration_cloud_placement_region.to_string().yellow());
                } else {
                    print!("Region: {}, ", row.second_registration_cloud_placement_region)
                };
                if row.first_registration_cloud_placement_zone != row.second_registration_cloud_placement_zone {
                    println!("Zone: {}->{}, ",row.first_registration_cloud_placement_zone.to_string().yellow(), row.second_registration_cloud_placement_zone.to_string().yellow());
                } else {
                    println!("Zone: {}, ", row.second_registration_cloud_placement_zone)
                };
                print!("         {} ", " ".repeat(32));
                if row.first_instance_seqno != row.second_instance_seqno {
                    print!("Seqno: {}->{}, ", row.first_instance_seqno.to_string().yellow(), row.second_instance_seqno.to_string().yellow());
                } else {
                    print!("Seqno: {}, ", row.second_instance_seqno);
                };
                if row.first_start_time_us != row.second_start_time_us {
                    println!("Start time: {}->{} ", row.first_start_time_us.to_string().yellow(), row.second_start_time_us.to_string().yellow());
                } else {
                    println!("Start time: {} ", row.second_start_time_us);
                };
                print!("         {} Http ( ", " ".repeat(32));
                for http_address in self.second_http_addresses.iter().filter(|r| r.permanent_uuid == permanent_uuid.clone()) {
                    print!("{} ", http_address.hostname_port);
                }
                println!(")");
                print!("         {} Rpc ( ", " ".repeat(32));
                for http_address in self.second_rpc_addresses.iter().filter(|r| r.permanent_uuid == permanent_uuid.clone()) {
                    print!("{} ", http_address.hostname_port);
                }
                println!(")");
            };
        };
    }
    pub async fn adhoc_read_first_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allstoredmasters = AllStoredMasters::read_masters(hosts, ports, parallel).await;
        let master_leader= AllIsLeader::return_leader_http(hosts, ports, parallel).await;
        self.first_snapshot(allstoredmasters, master_leader);
    }
    pub async fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allstoredmasters = AllStoredMasters::read_masters(hosts, ports, parallel).await;
        let master_leader= AllIsLeader::return_leader_http(hosts, ports, parallel).await;
        self.second_snapshot(allstoredmasters, master_leader);
    }
}


pub async fn masters_diff(
    options: &Opts,
) -> Result<()>
{
    info!("masters diff");

    if options.begin.is_none() || options.end.is_none() {
        snapshot::Snapshot::print()?;
    }
    if options.snapshot_list { return Ok(()) };

    let (begin_snapshot, end_snapshot, _begin_snapshot_row) = snapshot::Snapshot::read_begin_end_snapshot_from_user(options.begin, options.end)?;
    let masters_diff = SnapshotDiffBTreeMapsMasters::snapshot_diff(&begin_snapshot, &end_snapshot)?;
    masters_diff.print();

    Ok(())
}
*/

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
            //allstoredmasters.stored_rpc_addresses = snapshot::read_snapshot(snapshot_number, "master_rpc_addresses")?;
            //allstoredmasters.stored_http_addresses = snapshot::read_snapshot(snapshot_number, "master_http_addresses")?;
            //allstoredmasters.stored_master_error = snapshot::read_snapshot(snapshot_number, "master_errors")?;

            masters.print(snapshot_number, &options.details_enable)?;

        }
        None => {
            let masters = Masters::read_masters(&hosts, &ports, parallel).await;
            masters.print_adhoc(&options.details_enable, &hosts, &ports, parallel).await;
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