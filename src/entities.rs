use serde_derive::{Serialize,Deserialize};
use port_scanner::scan_port_addr;
use chrono::{DateTime, Local};
use std::{fs, process};
use log::*;
use std::sync::mpsc::channel;
use std::collections::HashMap;
use std::path::PathBuf;
use regex::Regex;
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct Entities {
    pub keyspaces: Vec<Keyspaces>,
    pub tables: Vec<Tables>,
    pub tablets: Vec<Tablets>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Keyspaces {
    pub keyspace_id: String,
    pub keyspace_name: String,
    pub keyspace_type: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Tables {
    pub table_id: String,
    pub keyspace_id: String,
    pub table_name: String,
    pub state: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Tablets {
    pub table_id: String,
    pub tablet_id: String,
    pub state: String,
    pub replicas: Option<Vec<Replicas>>,
    pub leader: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Replicas {
    #[serde(rename = "type")]
    pub replica_type: String,
    pub server_uuid: String,
    pub addr: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredTables {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub table_id: String,
    pub table_name: String,
    pub table_state: String,
    pub keyspace_id: String,
    pub keyspace_name: String,
    pub keyspace_type: String,
}

impl StoredTables {
    fn new(hostname_port: &str, timestamp: DateTime<Local>, table: Tables, keyspace_name: &str, keyspace_type: &str) -> Self {
        Self {
            hostname_port: hostname_port.to_string(),
            timestamp,
            table_id: table.table_id.to_string(),
            table_name: table.table_name.to_string(),
            table_state: table.state.to_string(),
            keyspace_id: table.keyspace_id,
            keyspace_name: keyspace_name.to_string(),
            keyspace_type: keyspace_type.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredTablets {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub table_id: String,
    pub tablet_id: String,
    pub tablet_state: String,
    pub leader: String,
}

impl StoredTablets {
    fn new(hostname_port: &str, timestamp: DateTime<Local>, tablet: &Tablets) -> Self {
        Self {
            hostname_port: hostname_port.to_string(),
            timestamp,
            table_id: tablet.table_id.to_string(),
            tablet_id: tablet.tablet_id.to_string(),
            tablet_state: tablet.state.to_string(),
            leader: tablet.leader.as_ref().unwrap_or(&"-".to_string()).to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredReplicas {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub tablet_id: String,
    pub replica_type: String,
    pub server_uuid: String,
    pub addr: String,
}

impl StoredReplicas {
    fn new(hostname_port: &str, timestamp: DateTime<Local>, tablet_id: &str, replica: Replicas) -> Self {
        Self {
            hostname_port: hostname_port.to_string(),
            timestamp,
            tablet_id: tablet_id.to_string(),
            replica_type: replica.replica_type.to_string(),
            server_uuid: replica.server_uuid.to_string(),
            addr: replica.addr
        }
    }
}

#[derive(Debug)]
pub struct KeyspaceLookup {
    pub keyspace_name: String,
    pub keyspace_type: String,
}

impl KeyspaceLookup {
    fn new(keyspace_name: &str, keyspace_type: &str) -> Self {
        Self {
            keyspace_name: keyspace_name.to_string(),
            keyspace_type: keyspace_type.to_string(),
        }
    }
}

#[allow(dead_code)]
pub fn read_entities(
    host: &str,
    port: &str,
) -> Entities {
    if ! scan_port_addr(format!("{}:{}", host, port)) {
        warn!("hostname: port {}:{} cannot be reached, skipping", host, port);
        return parse_entities(String::from(""), "", "")
    };
    let data_from_http = reqwest::blocking::get(format!("http://{}:{}/dump-entities", host, port))
        .unwrap_or_else(|e| {
            error!("Fatal: error reading from URL: {}", e);
            process::exit(1);
        })
        .text().unwrap();
    parse_entities(data_from_http, host, port)
}

#[allow(dead_code)]
fn parse_entities( entities_data: String, host: &str, port: &str ) -> Entities {
    serde_json::from_str(&entities_data )
        .unwrap_or_else(|e| {
            info!("({}:{}) could not parse /dump-entities json data for entities, error: {}", host, port, e);
            Entities { keyspaces: Vec::<Keyspaces>::new(), tables: Vec::<Tables>::new(), tablets: Vec::<Tablets>::new() }
        })
}

#[allow(dead_code)]
fn read_entities_into_vectors(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    parallel: usize,
) -> (
    Vec<StoredTables>,
    Vec<StoredTablets>,
    Vec<StoredReplicas>,
) {
    let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
    let (tx, rx) = channel();
    pool.scope(move |s| {
        for host in hosts {
            for port in ports {
                let tx = tx.clone();
                s.spawn(move |_| {
                    let detail_snapshot_time = Local::now();
                    let entities = read_entities(host, port);
                    tx.send((format!("{}:{}", host, port), detail_snapshot_time, entities)).expect("error sending data via tx (entities)");
                });
            }
        }
    });
    let mut stored_tables: Vec<StoredTables> = Vec::new();
    let mut stored_tablets: Vec<StoredTablets> = Vec::new();
    let mut stored_replicas: Vec<StoredReplicas> = Vec::new();
    for (hostname_port, detail_snapshot_time, entities) in rx {
        add_to_entity_vectors(entities, &hostname_port, detail_snapshot_time, &mut stored_tables, &mut stored_tablets, &mut stored_replicas);
    }
    (stored_tables, stored_tablets, stored_replicas)
}

#[allow(dead_code)]
pub fn add_to_entity_vectors(
    entities: Entities,
    hostname: &str,
    detail_snapshot_time: DateTime<Local>,
    stored_tables: &mut Vec<StoredTables>,
    stored_tablets: &mut Vec<StoredTablets>,
    stored_replicas: &mut Vec<StoredReplicas>,
) {

    // build a lookup table for keyspaces
    let mut keyspace_lookup: HashMap<String, KeyspaceLookup> = HashMap::new();
    for keyspace in entities.keyspaces {
        let keyspace_id = &keyspace.keyspace_id;
        let keyspace_name= &keyspace.keyspace_name;
        let keyspace_type = &keyspace.keyspace_type;
        keyspace_lookup.insert(keyspace_id.to_string(), KeyspaceLookup::new(keyspace_name, keyspace_type));
    }

    // build a vector for tables which includes the keyspaces data
    for table in entities.tables {
        let keyspace_name = match keyspace_lookup.get(&table.keyspace_id ) {
            Some(x) => &x.keyspace_name,
            None => {
                error!("table keyspace_id: {} not found in keyspaces for keyspace_name", &table.keyspace_id);
                "????"
            },
        };
        let keyspace_type = match keyspace_lookup.get(&table.keyspace_id ) {
            Some(x) => &x.keyspace_type,
            None => {
                error!("table keyspace_id: {} not found in keyspaces for keyspace_type", &table.keyspace_id);
                "????"
            },
        };
        stored_tables.push( StoredTables::new(hostname, detail_snapshot_time, table, keyspace_name, keyspace_type));
    }
    // build a vector for tablets
    for tablet in entities.tablets {
        stored_tablets.push( StoredTablets::new( hostname, detail_snapshot_time, &tablet));
        if let Some(replicas) = tablet.replicas {
            for replica in replicas {
                stored_replicas.push(StoredReplicas::new(hostname, detail_snapshot_time, &tablet.tablet_id, replica));
            }
        }
        /*
        match tablet.replicas {
            Some(replicas) => {
                for replica in replicas {
                    stored_replicas.push(StoredReplicas::new(hostname, detail_snapshot_time, &tablet.tablet_id, replica));
                }
            },
            None => {},
        }

         */
    }
}

#[allow(dead_code)]
#[allow(clippy::ptr_arg)]
pub fn perform_entities_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    snapshot_number: i32,
    yb_stats_directory: &PathBuf,
    parallel: usize,
) {
    info!("perform_entities_snapshot");
    let (stored_tables, stored_tablets, stored_replicas) = read_entities_into_vectors(hosts, ports, parallel);

    let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
    let tables_file = &current_snapshot_directory.join("tables");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&tables_file)
        .unwrap_or_else(|e| {
            error!("Fatal: error writing tables data in snapshot directory {}: {}", &tables_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_tables {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let tablets_file = &current_snapshot_directory.join("tablets");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&tablets_file)
        .unwrap_or_else(|e| {
            error!("Fatal: error writing tablets data in snapshot directory {}: {}", &tablets_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_tablets {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let replicas_file = &current_snapshot_directory.join("replicas");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&replicas_file)
        .unwrap_or_else(|e| {
            error!("Fatal: error writing replicas data in snapshot directory {}: {}", &replicas_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_replicas {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();
}

#[allow(clippy::ptr_arg)]
pub fn read_tables_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
) -> Vec<StoredTables>
{
    let mut stored_tables: Vec<StoredTables> = Vec::new();
    let tables_file = &yb_stats_directory.join(snapshot_number).join("tables");
    let file = fs::File::open( &tables_file )
    .unwrap_or_else(|e| {
        error!("Fatal: error reading file: {}: {}", &tables_file.clone().into_os_string().into_string().unwrap(), e);
        process::exit(1);
    });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredTables = row.unwrap();
        let _ = &stored_tables.push(data);
    }
    stored_tables
}

#[allow(clippy::ptr_arg)]
pub fn read_tablets_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
) -> Vec<StoredTablets>
{
    let mut stored_tablets: Vec<StoredTablets> = Vec::new();
    let tablets_file = &yb_stats_directory.join(snapshot_number).join("tablets");
    let file = fs::File::open( &tablets_file )
    .unwrap_or_else(|e| {
        error!("Fatal: error reading file: {}: {}", &tablets_file.clone().into_os_string().into_string().unwrap(), e);
        process::exit(1);
    });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredTablets = row.unwrap();
        let _ = &stored_tablets.push(data);
    }
    stored_tablets
}

#[allow(clippy::ptr_arg)]
pub fn read_replicas_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
) -> Vec<StoredReplicas>
{
    let mut stored_replicas: Vec<StoredReplicas> = Vec::new();
    let replicas_file = &yb_stats_directory.join(snapshot_number).join("replicas");
    let file = fs::File::open( &replicas_file )
    .unwrap_or_else(|e| {
        error!("Fatal: error reading file: {}: {}", &replicas_file.clone().into_os_string().into_string().unwrap(), e);
        process::exit(1);
    });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredReplicas = row.unwrap();
        let _ = &stored_replicas.push(data);
    }
    stored_replicas
}

pub fn print_entities(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
    hostname_filter: &Regex,
    table_name_filter: &Regex,
) {
    info!("print_entities");
    let stored_tables: Vec<StoredTables>  = read_tables_snapshot(snapshot_number, yb_stats_directory);
    let stored_tablets: Vec<StoredTablets> = read_tablets_snapshot(snapshot_number, yb_stats_directory);
    let stored_replicas: Vec<StoredReplicas> = read_replicas_snapshot(snapshot_number, yb_stats_directory);

    let mut tables_btreemap: BTreeMap<(String, String, String, String, String), StoredTables> = BTreeMap::new();
    for row in stored_tables {
        tables_btreemap.insert( (row.hostname_port.to_string(),
                                      row.keyspace_type.to_string(),
                                      row.keyspace_name.to_string(),
                                      row.table_name.to_string(),
                                      row.table_id.to_string()),
                                StoredTables { ..row }
        );
    }
    for ((hostname, keyspace_type, keyspace_name, table_name, table_id), row) in tables_btreemap {
        if hostname_filter.is_match(&hostname)
            &&table_name_filter.is_match(&table_name) {
            // table data
            println!("{} {} {} {} {} {}", hostname, keyspace_type, keyspace_name, table_name, table_id, row.table_state);
            // build a vector of records for the tablets
            let mut tablet_data: Vec<(String, String, String)> = Vec::new();
            for tablet in stored_tablets.iter()
                .filter(|x| x.hostname_port == hostname)
                .filter(|x| x.table_id == table_id) {
                tablet_data.push((tablet.leader.to_string(), tablet.tablet_id.to_string(), tablet.tablet_state.to_string()));
            };
            // iterate over the tablet vector
            for (leader, tablet_id, tablet_state) in tablet_data {
                print!("tablet:{} {} : ( ", tablet_id, tablet_state);
                // iterate over the replicas of the tablet
                for replica in stored_replicas.iter()
                    .filter(|x| x.hostname_port == hostname)
                    .filter(|x| x.tablet_id == tablet_id) {
                    let replica_state = if leader == replica.server_uuid { "LEADER" } else { "FOLLOWER" };
                    print!("{},{},{} ", replica.replica_type, replica.addr, replica_state);
                }
                println!(")");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_simple_entities_dump() {
        let json = r#"
{
  "keyspaces": [
    {
      "keyspace_id": "00000000000000000000000000000001",
      "keyspace_name": "system",
      "keyspace_type": "ycql"
    }
  ],
  "tables": [
    {
      "table_id": "000000010000300080000000000000af",
      "keyspace_id": "00000001000030008000000000000000",
      "table_name": "pg_user_mapping_user_server_index",
      "state": "RUNNING"
    }
  ],
  "tablets": [
    {
      "table_id": "sys.catalog.uuid",
      "tablet_id": "00000000000000000000000000000000",
      "state": "RUNNING"
    },
    {
      "table_id": "a1da3fb4b3be4bd4860253e723d11b97",
      "tablet_id": "235b5b031f094ec3bf6be2a023abebba",
      "state": "RUNNING",
      "replicas": [
        {
          "type": "VOTER",
          "server_uuid": "5b6fd994d7e34504ac48a5e653456704",
          "addr": "yb-3.local:9100"
        },
        {
          "type": "VOTER",
          "server_uuid": "a3f5a16532bb4ed4a061e794831168f8",
          "addr": "yb-1.local:9100"
        },
        {
          "type": "VOTER",
          "server_uuid": "e7a4a66ae7f94eb6a75b0ce3a90ab5ba",
          "addr": "yb-2.local:9100"
        }
      ],
      "leader": "a3f5a16532bb4ed4a061e794831168f8"
    }
  ]
}
        "#.to_string();
        let result = parse_entities(json, "", "");
        assert_eq!(result.keyspaces[0].keyspace_type,"ycql");
        assert_eq!(result.tables[0].table_name,"pg_user_mapping_user_server_index");
        assert_eq!(result.tablets[0].table_id,"sys.catalog.uuid");
        assert_eq!(result.tablets[1].table_id,"a1da3fb4b3be4bd4860253e723d11b97");
        assert_eq!(result.tablets[1].replicas.as_ref().unwrap()[0].server_uuid,"5b6fd994d7e34504ac48a5e653456704");
        assert_eq!(result.tablets[1].leader.as_ref().unwrap(),"a3f5a16532bb4ed4a061e794831168f8");
    }

    use crate::utility;

    #[test]
    fn integration_parse_entities() {
        let mut stored_tables: Vec<StoredTables> = Vec::new();
        let mut stored_tablets: Vec<StoredTablets> = Vec::new();
        let mut stored_replicas: Vec<StoredReplicas> = Vec::new();
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let data_parsed_from_json = read_entities(hostname.as_str(), port.as_str());
        add_to_entity_vectors(data_parsed_from_json, format!("{}:{}", hostname, port).as_str(), Local::now(), &mut stored_tables, &mut stored_tablets, &mut stored_replicas);
        // a MASTER only will generate entities on each master (!)
        assert!(!stored_tables.is_empty());
        assert!(!stored_tablets.is_empty());
        assert!(!stored_replicas.is_empty());
    }

}