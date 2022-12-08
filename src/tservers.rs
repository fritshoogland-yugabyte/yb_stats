use serde_derive::{Serialize,Deserialize};
use chrono::{DateTime, Local};
use std::{sync::mpsc::channel, time::Instant, collections::{HashMap, BTreeMap}};
use log::*;
use colored::*;
use anyhow::Result;
use crate::isleader::AllStoredIsLeader;
use crate::utility::{scan_host_port, http_get};
use crate::snapshot::{read_snapshot, save_snapshot};

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredTabletServers {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub tserver_hostname_port: String,
    pub time_since_hb: String,
    pub time_since_hb_sec: f32,
    pub status: String,
    pub uptime_seconds: i64,
    pub ram_used: String,
    pub ram_used_bytes: i64,
    pub num_sst_files: i32,
    pub total_sst_file_size: String,
    pub total_sst_file_size_bytes: i32,
    pub uncompressed_sst_file_size: String,
    pub uncompressed_sst_file_size_bytes: i32,
    pub read_ops_per_sec: f32,
    pub write_ops_per_sec: f32,
    pub user_tablets_total: i32,
    pub user_tablets_leaders: i32,
    pub system_tablets_total: i32,
    pub system_tablets_leaders: i32,
    pub active_tablets: i32,
    pub cloud: String,
    pub region: String,
    pub zone: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredPathMetrics {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub tserver_hostname_port: String,
    pub path: String,
    pub space_used: i64,
    pub total_space_size: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AllTabletServers {
    #[serde(rename = "")]
    pub tabletservers: HashMap<String, TabletServers>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PathMetrics {
    pub path: String,
    pub space_used: i64,
    pub total_space_size: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TabletServers {
    pub time_since_hb: String,
    pub time_since_hb_sec: f32,
    pub status: String,
    pub uptime_seconds: i64,
    pub ram_used: String,
    pub ram_used_bytes: i64,
    pub num_sst_files: i32,
    pub total_sst_file_size: String,
    pub total_sst_file_size_bytes: i32,
    pub uncompressed_sst_file_size: String,
    pub uncompressed_sst_file_size_bytes: i32,
    pub path_metrics: Vec<PathMetrics>,
    pub read_ops_per_sec: f32,
    pub write_ops_per_sec: f32,
    pub user_tablets_total: i32,
    pub user_tablets_leaders: i32,
    pub system_tablets_total: i32,
    pub system_tablets_leaders: i32,
    pub active_tablets: i32,
    pub cloud: String,
    pub region: String,
    pub zone: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AllStoredTabletServers {
    pub stored_tabletservers: Vec<StoredTabletServers>,
    pub stored_pathmetrics: Vec<StoredPathMetrics>,
}

impl AllStoredTabletServers {
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let alltabletservers = AllStoredTabletServers::read_tabletservers(hosts, ports, parallel).await;
        save_snapshot(snapshot_number,"tablet_servers", alltabletservers.stored_tabletservers)?;
        save_snapshot(snapshot_number,"tablet_servers_pathmetrics", alltabletservers.stored_pathmetrics)?;
        /*
        alltabletservers.await.save_snapshot(snapshot_number)
            .unwrap_or_else(|e| {
                error!("error saving snapshot: {}", e);
                process::exit(1);
            });

         */

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub fn new() -> Self {
        Default::default()
    }
    pub async fn read_tabletservers(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllStoredTabletServers
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
                        let tablet_servers = AllStoredTabletServers::read_http(host, port);
                        tx.send((format!("{}:{}", host, port), detail_snapshot_time, tablet_servers)).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allstoredtabletservers = AllStoredTabletServers::new();

        for (hostname_port, detail_snapshot_time, tablet_servers) in rx {
            allstoredtabletservers.split_into_vectors(tablet_servers, &hostname_port, detail_snapshot_time);
        }

        allstoredtabletservers
    }
    fn split_into_vectors(
        &mut self,
        alltabletservers: AllTabletServers,
        hostname_port: &str,
        detail_snapshot_time: DateTime<Local>,
    )
    {
        for (servername, serverstatus) in alltabletservers.tabletservers.iter() {
            self.stored_tabletservers.push( StoredTabletServers {
                hostname_port: hostname_port.to_string(),
                timestamp: detail_snapshot_time,
                tserver_hostname_port: servername.to_string(),
                time_since_hb: serverstatus.time_since_hb.to_string(),
                time_since_hb_sec: serverstatus.time_since_hb_sec,
                status: serverstatus.status.to_string(),
                uptime_seconds: serverstatus.uptime_seconds,
                ram_used: serverstatus.ram_used.to_string(),
                ram_used_bytes: serverstatus.ram_used_bytes,
                num_sst_files: serverstatus.num_sst_files,
                total_sst_file_size: serverstatus.total_sst_file_size.to_string(),
                total_sst_file_size_bytes: serverstatus.total_sst_file_size_bytes,
                uncompressed_sst_file_size: serverstatus.uncompressed_sst_file_size.to_string(),
                uncompressed_sst_file_size_bytes: serverstatus.uncompressed_sst_file_size_bytes,
                read_ops_per_sec: serverstatus.read_ops_per_sec,
                write_ops_per_sec: serverstatus.write_ops_per_sec,
                user_tablets_total: serverstatus.user_tablets_total,
                user_tablets_leaders: serverstatus.user_tablets_leaders,
                system_tablets_total: serverstatus.system_tablets_total,
                system_tablets_leaders: serverstatus.system_tablets_leaders,
                active_tablets: serverstatus.active_tablets,
                cloud: serverstatus.cloud.to_string(),
                region: serverstatus.region.to_string(),
                zone: serverstatus.zone.to_string(),
            });
            for pathmetrics in serverstatus.path_metrics.iter() {
                self.stored_pathmetrics.push( StoredPathMetrics {
                    hostname_port: hostname_port.to_string(),
                    timestamp: detail_snapshot_time,
                    tserver_hostname_port: servername.to_string(),
                    path: pathmetrics.path.to_string(),
                    space_used: pathmetrics.space_used,
                    total_space_size: pathmetrics.total_space_size,
                });
            }
        }
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> AllTabletServers
    {
        let data_from_http = if scan_host_port( host, port) {
            http_get(host, port, "api/v1/tablet-servers")
        } else {
            String::new()
        };
        AllStoredTabletServers::parse_tabletservers(data_from_http, host, port)

/*
        if ! scan_port_addr(format!("{}:{}", host, port)) {
            warn!("hostname: port {}:{} cannot be reached, skipping", host, port);
            return AllStoredTabletServers::parse_tabletservers(String::from(""), "", "")
        };

        let data_from_http = reqwest::blocking::get(format!("http://{}:{}/api/v1/tablet-servers", host, port))
            .unwrap_or_else(|e| {
                error!("Fatal: error reading from URL: {}", e);
                process::exit(1);
            })
            .text().unwrap();
        AllStoredTabletServers::parse_tabletservers(data_from_http, host, port)

 */
    }
    fn parse_tabletservers(
        tabletservers_data: String,
        host: &str,
        port: &str,
    ) -> AllTabletServers {
        serde_json::from_str(&tabletservers_data)
            .unwrap_or_else(|e| {
                debug!("({}:{}) could not parse /api/v1/tablet-servers json data for masters, error: {}", host, port, e);
                AllTabletServers { tabletservers: HashMap::new() }
            })
    }
    /*
    fn save_snapshot ( self, snapshot_number: i32 ) -> Result<(), Box<dyn Error>>
    {
        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number.to_string());

        let tablet_servers_file = &current_snapshot_directory.join("tablet_servers");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(tablet_servers_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_tabletservers {
            writer.serialize(row)?;
        }
        writer.flush()?;

        let tablet_servers_pathmetrics_file = &current_snapshot_directory.join("tablet_servers_pathmetrics");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(tablet_servers_pathmetrics_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_pathmetrics {
            writer.serialize(row)?;
        }
        writer.flush()?;

        Ok(())
    }

    pub fn read_snapshot( snapshot_number: &String, ) -> Result<AllStoredTabletServers, Box<dyn Error>>
    {
        let mut allstoredtabletservers = AllStoredTabletServers {
            stored_tabletservers: Default::default(),
            stored_pathmetrics: Default::default(),
        };

        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(snapshot_number);

        let tablet_servers_file = &current_snapshot_directory.join("tablet_servers");
        let file = fs::File::open(tablet_servers_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredTabletServers = row?;
            allstoredtabletservers.stored_tabletservers.push(data);
        };

        let tablet_servers_pathmetrics_file = &current_snapshot_directory.join("tablet_servers_pathmetrics");
        let file = fs::File::open(tablet_servers_pathmetrics_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredPathMetrics = row?;
            allstoredtabletservers.stored_pathmetrics.push(data);
        };

        Ok(allstoredtabletservers)
    }
     */
    pub fn print(
        &self,
        snapshot_number: &String,
        details_enable: &bool,
    ) -> Result<()>
    {
        info!("print tablet servers");

        let leader_hostname = AllStoredIsLeader::return_leader_snapshot(snapshot_number)?;

        for row in &self.stored_tabletservers {
            if row.hostname_port == leader_hostname
            && !*details_enable {
                println!("{:20} {} Cloud: {}, Region: {}, Zone: {}", row.tserver_hostname_port, row.status, row.cloud, row.region, row.zone);
                println!("{} HB time: {}, Uptime: {}, Ram {}", " ".repeat(20), row.time_since_hb, row.uptime_seconds, row.ram_used);
                println!("{} SST files: nr: {}, size: {}, uncompressed: {}", " ".repeat(20), row.num_sst_files, row.total_sst_file_size, row.uncompressed_sst_file_size);
                println!("{} ops read: {}, write: {}", " ".repeat(20), row.read_ops_per_sec, row.write_ops_per_sec);
                println!("{} tablets: active: {}, user (leader/total): {}/{}, system (leader/total): {}/{}", " ".repeat(20), row.active_tablets, row.user_tablets_leaders, row.user_tablets_total, row.system_tablets_leaders, row.system_tablets_total);
                for pathmetric in self.stored_pathmetrics.iter()
                    .filter(|x| x.hostname_port == row.hostname_port)
                    .filter(|x| x.tserver_hostname_port == row.tserver_hostname_port) {
                    println!("{} Path: {}, total: {}, used: {} ({:.2}%)", " ".repeat(20), pathmetric.path, pathmetric.total_space_size, pathmetric.space_used, (pathmetric.space_used as f64/pathmetric.total_space_size as f64)*100.0);
                }
            }
            if *details_enable {
                println!("{} {:20} {} Cloud: {}, Region: {}, Zone: {}", row.hostname_port, row.tserver_hostname_port, row.status, row.cloud, row.region, row.zone);
                println!("{} {} HB time: {}, Uptime: {}, Ram {}", row.hostname_port, " ".repeat(20), row.time_since_hb, row.uptime_seconds, row.ram_used);
                println!("{} {} SST files: nr: {}, size: {}, uncompressed: {}", row.hostname_port, " ".repeat(20), row.num_sst_files, row.total_sst_file_size, row.uncompressed_sst_file_size);
                println!("{} {} ops read: {}, write: {}", row.hostname_port, " ".repeat(20), row.read_ops_per_sec, row.write_ops_per_sec);
                println!("{} {} tablets: active: {}, user (leader/total): {}/{}, system (leader/total): {}/{}", row.hostname_port, " ".repeat(20), row.active_tablets, row.user_tablets_leaders, row.user_tablets_total, row.system_tablets_leaders, row.system_tablets_total);
                for pathmetric in self.stored_pathmetrics.iter()
                    .filter(|x| x.hostname_port == row.hostname_port)
                    .filter(|x| x.tserver_hostname_port == row.tserver_hostname_port) {
                    println!("{} {} Path: {}, total: {}, used: {} ({:.2}%)", row.hostname_port, " ".repeat(20), pathmetric.path, pathmetric.total_space_size, pathmetric.space_used, (pathmetric.space_used as f64/pathmetric.total_space_size as f64)*100.0);
                }
            }
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
        info!("print adhoc tablet servers");

        let leader_hostname = AllStoredIsLeader::return_leader_http(hosts, ports, parallel).await;

        for row in &self.stored_tabletservers {
            if row.hostname_port == leader_hostname
                && !*details_enable {
                println!("{:20} {} Cloud: {}, Region: {}, Zone: {}", row.tserver_hostname_port, row.status, row.cloud, row.region, row.zone);
                println!("{} HB time: {}, Uptime: {}, Ram {}", " ".repeat(20), row.time_since_hb, row.uptime_seconds, row.ram_used);
                println!("{} SST files: nr: {}, size: {}, uncompressed: {}", " ".repeat(20), row.num_sst_files, row.total_sst_file_size, row.uncompressed_sst_file_size);
                println!("{} ops read: {}, write: {}", " ".repeat(20), row.read_ops_per_sec, row.write_ops_per_sec);
                println!("{} tablets: active: {}, user (leader/total): {}/{}, system (leader/total): {}/{}", " ".repeat(20), row.active_tablets, row.user_tablets_leaders, row.user_tablets_total, row.system_tablets_leaders, row.system_tablets_total);
                for pathmetric in self.stored_pathmetrics.iter()
                    .filter(|x| x.hostname_port == row.hostname_port)
                    .filter(|x| x.tserver_hostname_port == row.tserver_hostname_port) {
                    println!("{} Path: {}, total: {}, used: {} ({:.2}%)", " ".repeat(20), pathmetric.path, pathmetric.total_space_size, pathmetric.space_used, (pathmetric.space_used as f64/pathmetric.total_space_size as f64)*100.0);
                }
            }
            if *details_enable {
                println!("{} {:20} {} Cloud: {}, Region: {}, Zone: {}", row.hostname_port, row.tserver_hostname_port, row.status, row.cloud, row.region, row.zone);
                println!("{} {} HB time: {}, Uptime: {}, Ram {}", row.hostname_port, " ".repeat(20), row.time_since_hb, row.uptime_seconds, row.ram_used);
                println!("{} {} SST files: nr: {}, size: {}, uncompressed: {}", row.hostname_port, " ".repeat(20), row.num_sst_files, row.total_sst_file_size, row.uncompressed_sst_file_size);
                println!("{} {} ops read: {}, write: {}", " ".repeat(20), row.hostname_port, row.read_ops_per_sec, row.write_ops_per_sec);
                println!("{} {} tablets: active: {}, user (leader/total): {}/{}, system (leader/total): {}/{}", row.hostname_port, " ".repeat(20), row.active_tablets, row.user_tablets_leaders, row.user_tablets_total, row.system_tablets_leaders, row.system_tablets_total);
                for pathmetric in self.stored_pathmetrics.iter()
                    .filter(|x| x.hostname_port == row.hostname_port)
                    .filter(|x| x.tserver_hostname_port == row.tserver_hostname_port) {
                    println!("{} {} Path: {}, total: {}, used: {} ({:.2}%)", row.hostname_port, " ".repeat(20), pathmetric.path, pathmetric.total_space_size, pathmetric.space_used, (pathmetric.space_used as f64/pathmetric.total_space_size as f64)*100.0);
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct SnapshotDiffStoredTabletServers {
    pub first_status: String,
    pub first_uptime_seconds: i64,
    pub second_status: String,
    pub second_uptime_seconds: i64,
}

impl SnapshotDiffStoredTabletServers {
    fn first_snapshot( storedtabletservers: StoredTabletServers ) -> Self
    {
        Self {
            first_status: storedtabletservers.status,
            first_uptime_seconds: storedtabletservers.uptime_seconds,
            second_status: "".to_string(),
            second_uptime_seconds: 0,
        }
    }
    fn second_snapshot_new( storedtabletservers: StoredTabletServers ) -> Self
    {
        Self {
            first_status: "".to_string(),
            first_uptime_seconds: 0,
            second_status: storedtabletservers.status,
            second_uptime_seconds: storedtabletservers.uptime_seconds,
        }
    }
    fn second_snapshot_existing( storedtabletservers_diff_row: &mut SnapshotDiffStoredTabletServers, storedtabletservers: StoredTabletServers ) -> Self
    {
        Self {
            first_status: storedtabletservers_diff_row.first_status.to_string(),
            first_uptime_seconds: storedtabletservers_diff_row.first_uptime_seconds,
            second_status: storedtabletservers.status,
            second_uptime_seconds: storedtabletservers.uptime_seconds,
        }
    }
}

type BTreeMapSnapshotDiffTabletServers = BTreeMap<String, SnapshotDiffStoredTabletServers>;

#[derive(Default)]
pub struct SnapshotDiffBTreeMapsTabletServers {
    pub btreemap_snapshotdiff_tabletservers: BTreeMapSnapshotDiffTabletServers,
    pub master_found: bool,
}

impl SnapshotDiffBTreeMapsTabletServers {
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
    ) -> Result<SnapshotDiffBTreeMapsTabletServers>
    {
        let mut allstoredtabletservers = AllStoredTabletServers::new();
        allstoredtabletservers.stored_tabletservers = read_snapshot(begin_snapshot, "tablet_servers")?;
        allstoredtabletservers.stored_pathmetrics = read_snapshot(begin_snapshot, "tablet_servers_pathmetrics")?;
        /*
        let allstoredtabletservers = AllStoredTabletServers::read_snapshot(begin_snapshot)
            .unwrap_or_else(|e| {
                error!("Fatal: error reading snapshot: {}", e);
                process::exit(1);
            });

         */
        let master_leader = AllStoredIsLeader::return_leader_snapshot(begin_snapshot)?;
        let mut tabletservers_snapshot_diff = SnapshotDiffBTreeMapsTabletServers::new();
        tabletservers_snapshot_diff.first_snapshot(allstoredtabletservers, master_leader);


        let mut allstoredtabletservers = AllStoredTabletServers::new();
        allstoredtabletservers.stored_tabletservers = read_snapshot(end_snapshot, "tablet_servers")?;
        allstoredtabletservers.stored_pathmetrics = read_snapshot(end_snapshot, "tablet_servers_pathmetrics")?;
        /*
        let allstoredtabletservers = AllStoredTabletServers::read_snapshot(end_snapshot)
            .unwrap_or_else(|e| {
                error!("Fatal: error reading snapshot: {}", e);
                process::exit(1);
            });

         */
        let master_leader = AllStoredIsLeader::return_leader_snapshot(end_snapshot)?;
        tabletservers_snapshot_diff.second_snapshot(allstoredtabletservers, master_leader);

        Ok(tabletservers_snapshot_diff)
    }
    pub fn new() -> Self {
        Default::default()
    }
    fn first_snapshot(
        &mut self,
        allstoredtabletservers: AllStoredTabletServers,
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

        for row in allstoredtabletservers.stored_tabletservers.into_iter().filter(|r| r.hostname_port == master_leader.clone()) {
            match self.btreemap_snapshotdiff_tabletservers.get_mut( &row.tserver_hostname_port ) {
                Some( _tabletserver_row ) => {
                    error!("Found second entry for first entry of tablet server, based on tablet server hostname:port: {}", &row.tserver_hostname_port);
                },
                None => {
                    trace!("first snapshot: add tablet server: {}", row.tserver_hostname_port.to_string() );
                    self.btreemap_snapshotdiff_tabletservers.insert(
                        row.tserver_hostname_port.to_string(),
                        SnapshotDiffStoredTabletServers::first_snapshot(row)
                    );
                },
            }
        };
    }
    fn second_snapshot(
        &mut self,
        allstoredtabletserver: AllStoredTabletServers,
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

        for row in allstoredtabletserver.stored_tabletservers.into_iter().filter(|r| r.hostname_port == master_leader.clone()) {
            match self.btreemap_snapshotdiff_tabletservers.get_mut( &row.tserver_hostname_port ) {
                Some( tabletserver_row ) => {
                    if tabletserver_row.first_status == row.status
                        && tabletserver_row.first_uptime_seconds <= row.uptime_seconds
                    {
                        trace!("second snapshot: tablet server idential:remove: {}", row.tserver_hostname_port.to_string() );
                        self.btreemap_snapshotdiff_tabletservers.remove( &row.tserver_hostname_port.clone() );
                    } else  {
                        trace!("second snapshot: tablet server CHANGED: {}", row.tserver_hostname_port.to_string() );
                        *tabletserver_row = SnapshotDiffStoredTabletServers::second_snapshot_existing(tabletserver_row, row);
                    }
                },
                None => {
                    trace!("second snapshot: new tablet server: {}", row.tserver_hostname_port.to_string() );
                    self.btreemap_snapshotdiff_tabletservers.insert( row.tserver_hostname_port.clone(), SnapshotDiffStoredTabletServers::second_snapshot_new(row));
                },
            }
        }
    }
    pub fn print(
        &self,
    )
    {
       if ! self.master_found {
           println!("Master leader was not found in hosts specified, skipping tablet servers diff.");
           return;
       }
        for (hostname, status) in self.btreemap_snapshotdiff_tabletservers.iter() {
            if status.second_status == *"" {
                println!("{} Tserver:  {}, status: {}, uptime: {} s", "-".to_string().red(), hostname, status.first_status, status.first_uptime_seconds);
            } else if status.first_status == *"" {
                println!("{} Tserver:  {}, status: {}, uptime: {} s", "+".to_string().green(), hostname, status.second_status, status.second_uptime_seconds);
            } else {
                print!("{} Tserver:  {}, ", "*".to_string().yellow(), hostname);
                if status.first_status != status.second_status {
                    print!("status: {}->{}, ", status.first_status.to_string().yellow(), status.second_status.to_string().yellow());
                } else {
                    print!("status: {}, ", status.first_status);
                };
                if status.second_uptime_seconds < status.first_uptime_seconds {
                    println!("uptime: {}->{} (reboot)", status.first_uptime_seconds.to_string().yellow(), status.second_uptime_seconds.to_string().yellow());
                } else {
                    println!("uptime: {}", status.second_uptime_seconds);
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
        let allstoredtabletservers = AllStoredTabletServers::read_tabletservers(hosts, ports, parallel).await;
        let master_leader = AllStoredIsLeader::return_leader_http(hosts, ports, parallel).await;
        self.first_snapshot(allstoredtabletservers, master_leader);
    }
    pub async fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allstoredtabletservers = AllStoredTabletServers::read_tabletservers(hosts, ports, parallel).await;
        let master_leader = AllStoredIsLeader::return_leader_http(hosts, ports, parallel).await;
        self.second_snapshot(allstoredtabletservers, master_leader);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_tabletserver_data() {
        let json = r#"
{
    "":
    {
        "yb-2.local:9000":
        {
            "time_since_hb": "0.8s",
            "time_since_hb_sec": 0.82917897,
            "status": "ALIVE",
            "uptime_seconds": 1517,
            "ram_used": "35.00 MB",
            "ram_used_bytes": 34996224,
            "num_sst_files": 3,
            "total_sst_file_size": "199.74 KB",
            "total_sst_file_size_bytes": 199735,
            "uncompressed_sst_file_size": "200.24 KB",
            "uncompressed_sst_file_size_bytes": 200238,
            "path_metrics":
            [
                {
                    "path": "/mnt/d0",
                    "space_used": 760074240,
                    "total_space_size": 10724835328
                }
            ],
            "read_ops_per_sec": 0,
            "write_ops_per_sec": 0,
            "user_tablets_total": 3,
            "user_tablets_leaders": 1,
            "system_tablets_total": 12,
            "system_tablets_leaders": 4,
            "active_tablets": 15,
            "cloud": "local",
            "region": "local",
            "zone": "local2"
        }
    }
}
        "#.to_string();
        let result = AllStoredTabletServers::parse_tabletservers(json, "", "");
        for (_servername, serverstatus) in result.tabletservers.iter() {
            assert_eq!(serverstatus.status, "ALIVE");
        }
    }

    use crate::utility;

    #[test]
    fn integration_parse_tabletserver() {
        let mut allstoredtabletservers = AllStoredTabletServers::new();

        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let data_parsed_from_json = AllStoredTabletServers::read_http(&hostname, &port);
        allstoredtabletservers.split_into_vectors(data_parsed_from_json, format!("{}:{}",hostname, port).as_ref(), Local::now());

        println!("{:?}", allstoredtabletservers);

        assert!(!allstoredtabletservers.stored_tabletservers.is_empty());
        assert!(!allstoredtabletservers.stored_pathmetrics.is_empty());

    }
}