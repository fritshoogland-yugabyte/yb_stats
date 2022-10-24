use serde_derive::{Serialize,Deserialize};
use port_scanner::scan_port_addr;
use chrono::{DateTime, Local};
use std::{fs, process};
use log::*;
use std::sync::mpsc::channel;
use std::path::PathBuf;
use regex::Regex;
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct AllMasters {
    pub masters: Vec<Masters>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Masters {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Error>,
    pub instance_id: InstanceId,
    pub registration: Registration,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Error {
    pub code: String,
    pub message: String,
    pub posix_code: i32,
    pub source_file: String,
    pub source_line: i32,
    pub errors: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InstanceId {
    pub instance_seqno: i64,
    pub permanent_uuid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time_us: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Registration {
    pub private_rpc_addresses: Vec<PrivateRpcAddresses>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_addresses: Option<Vec<HttpAddresses>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloud_info: Option<CloudInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placement_uuid: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PrivateRpcAddresses {
    pub host: String,
    pub port: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HttpAddresses {
    pub host: String,
    pub port: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CloudInfo {
    pub placement_cloud: String,
    pub placement_region: String,
    pub placement_zone: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredMasters {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub instance_permanent_uuid: String,
    pub instance_instance_seqno: i64,
    pub start_time_us: i64,
    pub registration_cloud_placement_cloud: String,
    pub registration_cloud_placement_region: String,
    pub registration_cloud_placement_zone: String,
    pub registration_placement_uuid: String,
    pub role: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredRpcAddresses {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub instance_permanent_uuid: String,
    pub host: String,
    pub port: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredHttpAddresses {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub instance_permanent_uuid: String,
    pub host: String,
    pub port: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredMasterError {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub instance_permanent_uuid: String,
    pub code: String,
    pub message: String,
    pub posix_code: i32,
    pub source_file: String,
    pub source_line: i32,
    pub errors: String,
}

#[allow(dead_code)]
pub fn read_masters(
    host: &str,
    port: &str,
) -> AllMasters {
    if ! scan_port_addr(format!("{}:{}", host, port)) {
        warn!("hostname: port {}:{} cannot be reached, skipping", host, port);
        return parse_masters(String::from(""), "", "")
    };
    let data_from_http = reqwest::blocking::get(format!("http://{}:{}/api/v1/masters", host, port))
        .unwrap_or_else(|e| {
            error!("Fatal: error reading from URL: {}", e);
            process::exit(1);
        })
        .text().unwrap();
    parse_masters(data_from_http, host, port)
}

#[allow(dead_code)]
fn parse_masters(
    masters_data: String,
    host: &str,
    port: &str,
) -> AllMasters {
    serde_json::from_str(&masters_data )
        .unwrap_or_else(|e| {
            info!("({}:{}) could not parse /api/v1/masters json data for masters, error: {}", host, port, e);
            AllMasters { masters: Vec::<Masters>::new() }
        })
}

#[allow(dead_code)]
fn read_masters_into_vectors(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    parallel: usize,
) -> (
    Vec<StoredMasters>,
    Vec<StoredRpcAddresses>,
    Vec<StoredHttpAddresses>,
    Vec<StoredMasterError>,
) {
    let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
    let (tx, rx) = channel();
    pool.scope(move |s| {
        for host in hosts {
            for port in ports {
                let tx = tx.clone();
                s.spawn(move |_| {
                    let detail_snapshot_time = Local::now();
                    let masters = read_masters(host, port);
                    tx.send((format!("{}:{}", host, port), detail_snapshot_time, masters)).expect("error sending data via tx (masters)");
                });
            }
        }
    });
    let mut stored_masters: Vec<StoredMasters> = Vec::new();
    let mut stored_rpcaddresses: Vec<StoredRpcAddresses> = Vec::new();
    let mut stored_httpaddresses: Vec<StoredHttpAddresses> = Vec::new();
    let mut stored_mastererror: Vec<StoredMasterError> = Vec::new();
    for (hostname_port, detail_snapshot_time, masters) in rx {
        add_to_master_vectors(masters, &hostname_port, detail_snapshot_time, &mut stored_masters, &mut stored_rpcaddresses, &mut stored_httpaddresses, &mut stored_mastererror);
    }
    (stored_masters, stored_rpcaddresses, stored_httpaddresses, stored_mastererror)
}

#[allow(dead_code)]
pub fn add_to_master_vectors(
    masters: AllMasters,
    hostname: &str,
    detail_snapshot_time: DateTime<Local>,
    stored_masters: &mut Vec<StoredMasters>,
    stored_rpcaddresses: &mut Vec<StoredRpcAddresses>,
    stored_httpaddresses: &mut Vec<StoredHttpAddresses>,
    stored_mastererror: &mut Vec<StoredMasterError>,
) {

    for master in masters.masters {
        let mut placement_cloud = String::from("Unset");
        let mut placement_region = String::from("Unset");
        let mut placement_zone = String::from("Unset");
        if let Some(cloud_info) = master.registration.cloud_info {
            placement_cloud = cloud_info.placement_cloud;
            placement_region = cloud_info.placement_region;
            placement_zone = cloud_info.placement_zone;
        };
        /*
        match master.registration.cloud_info {
            Some(cloud_info) => {
                placement_cloud = cloud_info.placement_cloud;
                placement_region = cloud_info.placement_region;
                placement_zone = cloud_info.placement_zone;
            },
            None => {},
        };

         */
        stored_masters.push(StoredMasters {
            hostname_port: hostname.to_string(),
            timestamp: detail_snapshot_time,
            instance_permanent_uuid: master.instance_id.permanent_uuid.to_string(),
            instance_instance_seqno: master.instance_id.instance_seqno,
            start_time_us: master.instance_id.start_time_us.unwrap_or_default(),
            registration_cloud_placement_cloud: placement_cloud.to_string(),
            registration_cloud_placement_region: placement_region.to_string(),
            registration_cloud_placement_zone: placement_zone.to_string(),
            registration_placement_uuid: master.registration.placement_uuid.unwrap_or_else(|| "Unset".to_string()).to_string(),
            role: master.role.unwrap_or_else(|| "Unknown".to_string()).to_string(),
        });
        if let Some(error) = master.error {
            stored_mastererror.push(StoredMasterError {
                hostname_port: hostname.to_string(),
                timestamp: detail_snapshot_time,
                instance_permanent_uuid: master.instance_id.permanent_uuid.to_string(),
                code: error.code.to_string(),
                message: error.message.to_string(),
                posix_code: error.posix_code,
                source_file: error.source_file.to_string(),
                source_line: error.source_line,
                errors: error.errors.to_string(),
            });
        }
        /*
        match master.error {
            Some(error) => {
                stored_mastererror.push(StoredMasterError {
                    hostname_port: hostname.to_string(),
                    timestamp: detail_snapshot_time,
                    instance_permanent_uuid: master.instance_id.permanent_uuid.to_string(),
                    code: error.code.to_string(),
                    message: error.message.to_string(),
                    posix_code: error.posix_code,
                    source_file: error.source_file.to_string(),
                    source_line: error.source_line,
                    errors: error.errors.to_string(),
                });
            },
            None => {},
        };

         */
        for rpc_address in master.registration.private_rpc_addresses {
            stored_rpcaddresses.push( StoredRpcAddresses {
                hostname_port: hostname.to_string(),
                timestamp: detail_snapshot_time,
                instance_permanent_uuid: master.instance_id.permanent_uuid.to_string(),
                host: rpc_address.host.to_string(),
                port: rpc_address.port.to_string(),
            });
        };
        if let Some(http_addresses) = master.registration.http_addresses {
            for http_address in http_addresses {
                stored_httpaddresses.push(StoredHttpAddresses {
                    hostname_port: hostname.to_string(),
                    timestamp: detail_snapshot_time,
                    instance_permanent_uuid: master.instance_id.permanent_uuid.to_string(),
                    host: http_address.host.to_string(),
                    port: http_address.port.to_string(),
                });
            };
        };
        /*
        match master.registration.http_addresses {
            Some(http_addresses) => {
                for http_address in http_addresses {
                    stored_httpaddresses.push(StoredHttpAddresses {
                        hostname_port: hostname.to_string(),
                        timestamp: detail_snapshot_time,
                        instance_permanent_uuid: master.instance_id.permanent_uuid.to_string(),
                        host: http_address.host.to_string(),
                        port: http_address.port.to_string(),
                    });
                };
            },
            None => {},
        };

         */
    }
}

#[allow(dead_code)]
#[allow(clippy::ptr_arg)]
pub fn perform_masters_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    snapshot_number: i32,
    yb_stats_directory: &PathBuf,
    parallel: usize,
) {
    info!("perform_masters_snapshot");

    let (stored_masters, stored_rpcaddresses, stored_httpaddresses, stored_mastererrors) = read_masters_into_vectors(hosts, ports, parallel);

    let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
    let masters_file = &current_snapshot_directory.join("masters");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&masters_file)
        .unwrap_or_else(|e| {
            error!("Fatal: error writing masters data in snapshot directory {}: {}", &masters_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_masters {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let master_rpc_addresses_file = &current_snapshot_directory.join("master_rpc_addresses");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&master_rpc_addresses_file)
        .unwrap_or_else(|e| {
            error!("Fatal: error writing master_rpc_addresses data in snapshot directory {}: {}", &master_rpc_addresses_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_rpcaddresses {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let master_http_addresses_file = &current_snapshot_directory.join("master_http_addresses");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&master_http_addresses_file)
        .unwrap_or_else(|e| {
            error!("Fatal: error writing masters_http_addresses data in snapshot directory {}: {}", &master_http_addresses_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_httpaddresses {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let master_errors_file = &current_snapshot_directory.join("master_errors");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&master_errors_file)
        .unwrap_or_else(|e| {
            error!("Fatal: error writing masters_errors data in snapshot directory {}: {}", &master_errors_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_mastererrors {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();
}

#[allow(clippy::ptr_arg)]
pub fn read_masters_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
) -> Vec<StoredMasters>
{
    let mut stored_masters: Vec<StoredMasters> = Vec::new();
    let masters_file = &yb_stats_directory.join(snapshot_number).join("masters");
    let file = fs::File::open( &masters_file )
    .unwrap_or_else(|e| {
        error!("Fatal: error reading file: {}: {}", &masters_file.clone().into_os_string().into_string().unwrap(), e);
        process::exit(1);
    });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredMasters = row.unwrap();
        let _ = &stored_masters.push(data);
    }
    stored_masters
}

#[allow(clippy::ptr_arg)]
pub fn read_master_rpc_addresses_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
) -> Vec<StoredRpcAddresses>
{
    let mut stored_master_rpc_addresses: Vec<StoredRpcAddresses> = Vec::new();
    let master_rpc_addresses_file = &yb_stats_directory.join(snapshot_number).join("master_rpc_addresses");
    let file = fs::File::open( &master_rpc_addresses_file )
    .unwrap_or_else(|e| {
        error!("Fatal: error reading file: {}: {}", &master_rpc_addresses_file.clone().into_os_string().into_string().unwrap(), e);
        process::exit(1);
    });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredRpcAddresses = row.unwrap();
        let _ = &stored_master_rpc_addresses.push(data);
    }
    stored_master_rpc_addresses
}

#[allow(clippy::ptr_arg)]
pub fn read_master_http_addresses_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
) -> Vec<StoredHttpAddresses>
{
    let mut stored_master_http_addresses: Vec<StoredHttpAddresses> = Vec::new();
    let master_http_addresses_file = &yb_stats_directory.join(snapshot_number).join("master_http_addresses");
    let file = fs::File::open( &master_http_addresses_file )
    .unwrap_or_else(|e| {
        error!("Fatal: error reading file: {}: {}", &master_http_addresses_file.clone().into_os_string().into_string().unwrap(), e);
        process::exit(1);
    });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredHttpAddresses = row.unwrap();
        let _ = &stored_master_http_addresses.push(data);
    }
    stored_master_http_addresses
}

#[allow(clippy::ptr_arg)]
pub fn read_master_errors_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
) -> Vec<StoredMasterError>
{
    let mut stored_master_errors: Vec<StoredMasterError> = Vec::new();
    let master_errors_file = &yb_stats_directory.join(snapshot_number).join("master_errors");
    let file = fs::File::open( &master_errors_file )
        .unwrap_or_else(|e| {
            error!("Fatal: error reading file: {}: {}", &master_errors_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredMasterError = row.unwrap();
        let _ = &stored_master_errors.push(data);
    }
    stored_master_errors
}

pub fn print_masters(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
    hostname_filter: &Regex,
) {
    info!("print_masters");
    let stored_masters: Vec<StoredMasters>  = read_masters_snapshot(snapshot_number, yb_stats_directory);
    let stored_master_rpc_addresses: Vec<StoredRpcAddresses> = read_master_rpc_addresses_snapshot(snapshot_number, yb_stats_directory);
    let stored_master_http_addresses: Vec<StoredHttpAddresses> = read_master_http_addresses_snapshot(snapshot_number, yb_stats_directory);
    let stored_master_errors: Vec<StoredMasterError> = read_master_errors_snapshot(snapshot_number, yb_stats_directory);

    let mut masters_btreemap: BTreeMap<(String, String), StoredMasters> = BTreeMap::new();
    for row in stored_masters {
        masters_btreemap.insert( (row.hostname_port.to_string(),
                                      row.instance_permanent_uuid.to_string()), StoredMasters {
            hostname_port: row.hostname_port,
            timestamp: row.timestamp,
            instance_permanent_uuid: row.instance_permanent_uuid,
            instance_instance_seqno: row.instance_instance_seqno,
            start_time_us: row.start_time_us,
            registration_cloud_placement_cloud: row.registration_cloud_placement_cloud,
            registration_cloud_placement_region: row.registration_cloud_placement_region,
            registration_cloud_placement_zone: row.registration_cloud_placement_zone,
            registration_placement_uuid: row.registration_placement_uuid,
            role: row.role,
        });
    }
    let mut previous_hostname = String::from("");
    for ((hostname, instance_permanent_uuid), row) in masters_btreemap {
        if hostname_filter.is_match(&hostname) {
            if previous_hostname != hostname {
                println!("{}", "-".repeat(80));
                println!("Source host: {}, snapshot time: {}", hostname, row.timestamp);
                println!("{}", "-".repeat(80));
            }
            // table data
            println!("Permanent UUID:{}, Instance Seqno:{}, Start time:{}us, Cloud:{}, Region:{}, Zone:{}, Role:{}", instance_permanent_uuid, row.instance_instance_seqno, row.start_time_us, row.registration_cloud_placement_cloud, row.registration_cloud_placement_region, row.registration_cloud_placement_zone, row.role);
            //let mut tablet_data: Vec<(String, String, String)> = Vec::new();
            print!("RPC addresses: ( ");
            for rpc_address in stored_master_rpc_addresses.iter()
                .filter(|x| x.hostname_port == hostname)
                .filter(|x| x.instance_permanent_uuid == instance_permanent_uuid) {
                print!("{}:{} ", rpc_address.host, rpc_address.port);
            };
            println!(" )");
            print!("HTTP addresses: ( ");
            for http_address in stored_master_http_addresses.iter()
                .filter(|x| x.hostname_port == hostname)
                .filter(|x| x.instance_permanent_uuid == instance_permanent_uuid) {
                print!("{}:{} ", http_address.host, http_address.port);
            };
            println!(" )");
            for error in stored_master_errors.iter()
                .filter(|x| x.hostname_port == hostname)
                .filter(|x| x.instance_permanent_uuid == instance_permanent_uuid) {
                println!("{:#?}", error);
            };
            previous_hostname = hostname;
        }
    }
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
        let result = parse_masters(json, "", "");
        assert!(result.masters[0].error.is_none());
    }

    use crate::utility;

    #[test]
    fn integration_parse_masters() {
        let mut stored_masters: Vec<StoredMasters> = Vec::new();
        let mut stored_rpc_addresses: Vec<StoredRpcAddresses> = Vec::new();
        let mut stored_http_addresses: Vec<StoredHttpAddresses> = Vec::new();
        let mut stored_master_errors: Vec<StoredMasterError> = Vec::new();
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let data_parsed_from_json = read_masters(hostname.as_str(), port.as_str());
        add_to_master_vectors(data_parsed_from_json, format!("{}:{}", hostname, port).as_str(), Local::now(), &mut stored_masters, &mut stored_rpc_addresses, &mut stored_http_addresses, &mut stored_master_errors);
        // a MASTER only will generate entities on each master (!)
        assert!(!stored_masters.is_empty());
        assert!(!stored_rpc_addresses.is_empty());
        assert!(!stored_http_addresses.is_empty());
    }

}