//! The module for reading /dump-entities available on the masters.
//!
//! The /dump-entities endpoint contains a number of independent JSON arrays:
//! 1. keyspaces: "keyspaces":[{"keyspace_id":"00000000000000000000000000000001","keyspace_name":"system","keyspace_type":"ycql"},..]
//! 2. tables: "tables":[{"table_id":"000000010000300080000000000000af","keyspace_id":"00000001000030008000000000000000","table_name":"pg_user_mapping_user_server_index","state":"RUNNING"},..]
//! 3. tablets: "tablets":[{"table_id":"sys.catalog.uuid","tablet_id":"00000000000000000000000000000000","state":"RUNNING"},..]
//! 3.1 replicas: "replicas":[{"type":"VOTER","server_uuid":"047856aaf11547749694ca7d7941fb31","addr":"yb-2.local:9100"},..]
//! This is 3.1 because replicas are arrays nested in tablets.
//!
//! The way these link together is:
//! tables.keyspace_id -> keyspaces.keyspace_id (keyspaces.keyspace_id must exist for tables.keyspace_id)
//! tables.table_id -> tablets.table_id, which contains the replicas as a nested array.
//! tablets.table_id might not exist for tables.table_id, because some tables do not have tablets, such as the postgres catalog entries.
//!
//! Special keyspaces:
//! - system: contains the local, partitions, roles, transactions, peers, size_estimates, transactions-<UUID> tables.
//! - system_schema: contains the YCQL indexes, views, aggregates, keyspaces, tables, types, functions, triggers, columns, sys.catalog tables.
//! - system_auth: contains the roles, role_permissions, resource_role_permissions_index tables.
//! - template1: postgres template1 database template, contains catalog
//! - template0: postgres template0 database template, contains catalog
//! - postgres: postgres standard database, not commonly used with YugabyteDB.
//! - yugabyte: postgres standard database, default database.
//! - system_platform: postgres database, contains a few extra catalog tables starting with 'sql'.
//!
//! YCQL requires a keyspace to be defined before user objects can be created and loaded, and
//! keyspace, table and tablet will get a random UUID as id.
//!
//! YSQL keyspaces do get an id in a specific way.
//! The id of the YSQL keyspace is in a format that later is used by the objects too.
//! This is how a YSQL keyspace id looks like:
//! 000033e5000030008000000000000000
//! |------|xxxx||xx||xxxxxx|------|
//! database    ver var     object
//! oid         (3) (8)     oid
//! (hex)                   (hex)
//! A YSQL keyspace has the object id set to all 0.
//! This is described in the YugabyteDB sourcecode: src/yb/common/entity_ids.cc
//! Version 3 is for ISO4122 UUID version.
//! Variant 8 means DCE 1.1, ISO/IEC 11578:1996
//! Version and variant are static currently.
//!
//! The object OID number indicates whether an object is a catalog object or a user object.
//! Any object OID lower than 16384 (0x4000) is a catalog object. This is why a user table_id always starts from ..4000.
//!
//! YSQL colocated databases.
//! If a database is created with colocation turned on, it will generate a special table entry:
//!     {
//!       "table_id": "0000400f000030008000000000000000.colocated.parent.uuid",
//!       "keyspace_id": "0000400f000030008000000000000000",
//!       "table_name": "0000400f000030008000000000000000.colocated.parent.tablename",
//!       "state": "RUNNING"
//!     }
//! This indicates the keyspace/database is colocated, and thus any object not explicitly defined using its own tablets,
//! will be stored in the tablets that are part of the database.
//! 
use serde_derive::{Serialize,Deserialize};
use port_scanner::scan_port_addr;
use chrono::{DateTime, Local};
use std::{fs, process, collections::{BTreeMap, HashMap}, sync::mpsc::channel, time::Instant, env, error::Error};
use log::*;
use regex::Regex;
use colored::*;
use crate::isleader::AllStoredIsLeader;

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

impl StoredTables
{
    fn new(hostname_port: &str, timestamp: DateTime<Local>, table: Tables) -> Self
    {
        Self
        {
            hostname_port: hostname_port.to_string(),
            timestamp,
            table_id: table.table_id.to_string(),
            table_name: table.table_name.to_string(),
            table_state: table.state.to_string(),
            keyspace_id: table.keyspace_id,
            keyspace_name: "".to_string(),
            keyspace_type: "".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredKeyspaces
{
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub keyspace_id: String,
    pub keyspace_name: String,
    pub keyspace_type: String,
}

impl StoredKeyspaces
{
    fn new(hostname_port: &str, timestamp: DateTime<Local>, keyspaces: &Keyspaces) -> Self
    {
        Self
        {
            hostname_port: hostname_port.to_string(),
            timestamp,
            keyspace_id: keyspaces.keyspace_id.to_string(),
            keyspace_name: keyspaces.keyspace_name.to_string(),
            keyspace_type: keyspaces.keyspace_type.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredTablets
{
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub table_id: String,
    pub tablet_id: String,
    pub tablet_state: String,
    pub leader: String,
}

impl StoredTablets
{
    fn new(hostname_port: &str, timestamp: DateTime<Local>, tablet: &Tablets) -> Self
    {
        Self
        {
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
pub struct StoredReplicas
{
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub tablet_id: String,
    pub replica_type: String,
    pub server_uuid: String,
    pub addr: String,
}

impl StoredReplicas
{
    fn new(hostname_port: &str, timestamp: DateTime<Local>, tablet_id: &str, replica: Replicas) -> Self
    {
        Self
        {
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
pub struct AllStoredEntities {
    pub stored_keyspaces: Vec<StoredKeyspaces>,
    pub stored_tables: Vec<StoredTables>,
    pub stored_tablets: Vec<StoredTablets>,
    pub stored_replicas: Vec<StoredReplicas>,

}

impl AllStoredEntities
{
    pub fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    )
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allstoredentities = AllStoredEntities::read_entities(hosts, ports, parallel);
        allstoredentities.save_snapshot(snapshot_number)
            .unwrap_or_else(|e| {
                error!("error saving snasphot: {}", e);
                process::exit(1);
            });

        info!("end snapshot: {:?}", timer.elapsed());
    }
    fn read_entities (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllStoredEntities
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
                        let entities = AllStoredEntities::read_http(host, port);
                        tx.send((format!("{}:{}", host, port), detail_snapshot_time, entities)).expect("error sending data via tx (entities)");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allstoredentities = AllStoredEntities { stored_keyspaces: Vec::new(), stored_tables: Vec::new(), stored_tablets: Vec::new(), stored_replicas: Vec::new() };
        for (hostname_port, detail_snapshot_time, entities) in rx {
            AllStoredEntities::split_into_vectors(entities, &hostname_port, detail_snapshot_time, &mut allstoredentities);
        }

        allstoredentities
    }
    fn read_http(
        host: &str,
        port: &str,
    ) -> Entities
    {
        if ! scan_port_addr(format!("{}:{}", host, port)) {
            warn!("hostname: port {}:{} cannot be reached, skipping", host, port);
            return AllStoredEntities::parse_entities(String::from(""), "", "")
        };
        let data_from_http = reqwest::blocking::get(format!("http://{}:{}/dump-entities", host, port))
            .unwrap_or_else(|e| {
                error!("Fatal: error reading from URL: {}", e);
                process::exit(1);
            })
            .text().unwrap();
        AllStoredEntities::parse_entities(data_from_http, host, port)
    }
    fn parse_entities( entities_data: String, host: &str, port: &str ) -> Entities {
        serde_json::from_str(&entities_data )
            .unwrap_or_else(|e| {
                info!("({}:{}) could not parse /dump-entities json data for entities, error: {}", host, port, e);
                Entities { keyspaces: Vec::<Keyspaces>::new(), tables: Vec::<Tables>::new(), tablets: Vec::<Tablets>::new() }
            })
    }
    fn split_into_vectors(
        entities: Entities,
        hostname_port: &str,
        detail_snapshot_time: DateTime<Local>,
        allstoredentities: &mut AllStoredEntities,
    )
    {
        for keyspace in entities.keyspaces {
            allstoredentities.stored_keyspaces.push(StoredKeyspaces::new(hostname_port, detail_snapshot_time, &keyspace) );
        }
        for table in entities.tables {
            allstoredentities.stored_tables.push( StoredTables::new(hostname_port, detail_snapshot_time, table) );
        }
        for tablet in entities.tablets {
            allstoredentities.stored_tablets.push( StoredTablets::new(hostname_port, detail_snapshot_time, &tablet) );
            if let Some(replicas) = tablet.replicas {
                for replica in replicas {
                    allstoredentities.stored_replicas.push(StoredReplicas::new(hostname_port, detail_snapshot_time, &tablet.tablet_id, replica));
                }
            }
        }
    }
    fn save_snapshot ( self, snapshot_number: i32 ) -> Result<(), Box<dyn Error>>
    {
        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number.to_string());

        let tables_file = &current_snapshot_directory.join("tables");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&tables_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_tables {
            writer.serialize(row)?;
        }
        writer.flush()?;

        let tablets_file = &current_snapshot_directory.join("tablets");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&tablets_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_tablets {
            writer.serialize(row)?;
        }
        writer.flush()?;

        let replicas_file = &current_snapshot_directory.join("replicas");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&replicas_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_replicas {
            writer.serialize(row)?;
        }
        writer.flush()?;

        let keyspaces_file = &current_snapshot_directory.join("keyspaces");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&keyspaces_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_keyspaces {
            writer.serialize(row)?;
        }
        writer.flush()?;

        Ok(())
    }
    pub fn read_snapshot( snapshot_number: &String, ) -> Result<AllStoredEntities, Box<dyn Error>>
    {
        let mut allstoredentities = AllStoredEntities { stored_keyspaces: Vec::new(), stored_tables: Vec::new(), stored_tablets: Vec::new(), stored_replicas: Vec::new() };

        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number);

        let keyspaces_file = &current_snapshot_directory.join("keyspaces");
        let file = fs::File::open(&keyspaces_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredKeyspaces = row?;
            allstoredentities.stored_keyspaces.push(data);
        };

        let tables_file = &current_snapshot_directory.join("tables");
        let file = fs::File::open(&tables_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredTables = row?;
            allstoredentities.stored_tables.push(data);
        };

        let tablets_file = &current_snapshot_directory.join("tablets");
        let file = fs::File::open(&tablets_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredTablets = row?;
            allstoredentities.stored_tablets.push(data);
        };

        let replicas_file = &current_snapshot_directory.join("replicas");
        let file = fs::File::open(&replicas_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredReplicas = row?;
            allstoredentities.stored_replicas.push(data);
        };

        Ok(allstoredentities)
    }
    pub fn print(
        &self,
        snapshot_number: &String,
        table_name_filter: &Regex,
        details_enable: &bool,
    )
    {
        info!("print_entities");

        let leader_hostname = AllStoredIsLeader::return_leader_snapshot(snapshot_number);

        let mut tables_btreemap: BTreeMap<(String, String, String), StoredTables> = BTreeMap::new();

        let is_system_keyspace = |keyspace: &str| -> bool {
            matches!(keyspace, "00000000000000000000000000000001" |   // ycql system
                               "00000000000000000000000000000002" |   // ycql system_schema
                               "00000000000000000000000000000003" |   // ycql system_auth
                               "00000001000030008000000000000000" |   // ysql template1
                               "000033e5000030008000000000000000")    // ysql template0
        };

        let object_oid_number = |oid: &str| -> u32 {
            // The oid entry normally is a 32 byte UUID for both ycql and ysql, which only contains hexadecimal numbers.
            // However, there is a single entry in system_schema that has a table_id that is 'sys.catalog.uuid'
            if oid.len() == 32_usize {
                let true_oid = &oid[24..];
                u32::from_str_radix(true_oid, 16).unwrap()
            } else {
                0
            }
        };

        for row in self.stored_tables.iter() {
            if !*details_enable && row.hostname_port.ne(&leader_hostname) {
               continue
            }
            if is_system_keyspace(row.keyspace_id.as_str()) && !*details_enable {
                continue
            }
            if object_oid_number(row.table_id.as_str()) < 16384
            && !*details_enable
            // this takes the keyspace_id from the stored_tables vector, and filters the contents of stored_keyspaces vector for the keyspace_id,
            // and then maps the keyspace_name, which is tested for equality with "ysql" to make sure it's a ysql row for which the filter is applied.
            //&& self.stored_keyspaces.iter().filter(|r| r.keyspace_id == row.keyspace_id).next().map(|r| &r.keyspace_type).unwrap() == "ysql" {
            && self.stored_keyspaces.iter().find(|r| r.keyspace_id == row.keyspace_id).map(|r| &r.keyspace_type).unwrap() == "ysql" {
                continue
            }
            if !table_name_filter.is_match(&row.table_name) {
                continue
            }
            let keyspace_type = self.stored_keyspaces
                .iter()
                .filter(|r| r.keyspace_id == row.keyspace_id.clone())
                .map(|r| r.keyspace_type.clone())
                .next()
                .unwrap();
            tables_btreemap.insert( (keyspace_type.clone(), row.keyspace_id.clone(), row.table_id.clone()), StoredTables {
                hostname_port: row.hostname_port.to_string(),
                timestamp: row.timestamp,
                table_id: row.table_id.to_string(),
                table_name: row.table_name.to_string(),
                table_state: row.table_state.to_string(),
                keyspace_id: row.keyspace_id.to_string(),
                keyspace_name: "".to_string(),
                keyspace_type
            } );
        }

        for ((keyspace_type, keyspace_id, table_id), row) in tables_btreemap {
            let keyspace_name = self.stored_keyspaces
                .iter()
                .filter(|r| r.keyspace_id == keyspace_id)
                .map(|r| r.keyspace_name.clone())
                .next()
                .unwrap();

            if *details_enable {
                print!("{} ", &row.hostname_port);
            }
            println!("{} {} {} {}", keyspace_type, keyspace_name, row.table_name, row.table_state);

            for tablet in self.stored_tablets
                .iter()
                .filter(|r| r.hostname_port == leader_hostname)
                .filter(|r| r.table_id == table_id)
            {
                if *details_enable {
                    print!("{} ", &row.hostname_port);
                }

                print!(" {} {} ( ", tablet.tablet_id, tablet.tablet_state);

                for replica in self.stored_replicas
                    .iter()
                    .filter(|r| r.hostname_port == leader_hostname)
                    .filter(|r| r.tablet_id == tablet.tablet_id)
                {
                    let replica_state = if replica.server_uuid == tablet.leader { "LEADER" } else { "FOLLOWER" };
                    print!("{},{},{} ", replica.replica_type, replica.addr, replica_state);
                };
                println!(")");
            }
        }
    }
}

// replica_id, server_uuid is the unique key
//pub server_uuid: String,
//pub tablet_id: String,
#[derive(Debug)]
pub struct SnapshotDiffReplica {
    pub first_replica_type: String,
    pub first_addr: String,
    pub second_replica_type: String,
    pub second_addr: String,
}

impl SnapshotDiffReplica {
    fn first_snapshot ( storedreplicas: StoredReplicas ) -> Self
    {
        Self {
            first_replica_type: storedreplicas.replica_type.to_string(),
            first_addr: storedreplicas.addr,
            second_replica_type: "".to_string(),
            second_addr: "".to_string()
        }
    }
    fn second_snapshot_new ( storedreplicas: StoredReplicas ) -> Self
    {
        Self {
            first_replica_type: "".to_string(),
            first_addr: "".to_string(),
            second_replica_type: storedreplicas.replica_type.to_string(),
            second_addr: storedreplicas.addr,
        }
    }
    fn second_snapshot_existing ( replica_diff_row: &mut SnapshotDiffReplica, storedreplicas: StoredReplicas ) -> Self
    {
        Self {
            first_replica_type: replica_diff_row.first_replica_type.to_string(),
            first_addr: replica_diff_row.first_addr.to_string(),
            second_replica_type: storedreplicas.replica_type.to_string(),
            second_addr: storedreplicas.addr
        }
    }
}

// tablet_id is the unique key
// pub tablet_id: String,
#[derive(Debug)]
pub struct SnapshotDiffTablets
{
    pub first_table_id: String,
    pub first_tablet_state: String,
    pub first_leader: String,
    pub second_table_id: String,
    pub second_tablet_state: String,
    pub second_leader: String,
}

impl SnapshotDiffTablets {
    fn first_snapshot ( storedtablets: StoredTablets ) -> Self
    {
        Self {
            first_table_id: storedtablets.table_id.to_string(),
            first_tablet_state: storedtablets.tablet_state.to_string(),
            first_leader: storedtablets.leader,
            second_table_id: "".to_string(),
            second_tablet_state: "".to_string(),
            second_leader: "".to_string(),
        }
    }
    fn second_snapshot_new ( storedtablets: StoredTablets ) -> Self
    {
        Self {
            first_table_id: "".to_string(),
            first_tablet_state: "".to_string(),
            first_leader: "".to_string(),
            second_table_id: storedtablets.table_id.to_string(),
            second_tablet_state: storedtablets.tablet_state.to_string(),
            second_leader: storedtablets.leader,
        }
    }
    fn second_snapshot_existing ( tablet_diff_row: &mut SnapshotDiffTablets, storedtablets: StoredTablets ) -> Self
    {
        Self {
            first_table_id: tablet_diff_row.first_table_id.to_string(),
            first_tablet_state: tablet_diff_row.first_tablet_state.to_string(),
            first_leader: tablet_diff_row.first_leader.to_string(),
            second_table_id: storedtablets.table_id.to_string(),
            second_tablet_state: storedtablets.tablet_state.to_string(),
            second_leader: storedtablets.leader
        }
    }
}

// table_id is the unique key
// pub table_id: String,
#[derive(Debug)]
pub struct SnapshotDiffTables {
    pub first_table_name: String,
    pub first_table_state: String,
    pub first_keyspace_id: String,
    pub second_table_name: String,
    pub second_table_state: String,
    pub second_keyspace_id: String,
}

impl SnapshotDiffTables {
    fn first_snapshot ( storedtables: StoredTables ) -> Self
    {
        Self {
            first_table_name: storedtables.table_name.to_string(),
            first_table_state: storedtables.table_state.to_string(),
            first_keyspace_id: storedtables.keyspace_id,
            second_table_name: "".to_string(),
            second_table_state: "".to_string(),
            second_keyspace_id: "".to_string()
        }
    }
    fn second_snapshot_new ( storedtables: StoredTables ) -> Self
    {
        Self {
            first_table_name: "".to_string(),
            first_table_state: "".to_string(),
            first_keyspace_id: "".to_string(),
            second_table_name: storedtables.table_name.to_string(),
            second_table_state: storedtables.table_state.to_string(),
            second_keyspace_id: storedtables.keyspace_id,
        }
    }
    fn second_snapshot_existing ( table_diff_row: &mut SnapshotDiffTables, storedtables: StoredTables ) -> Self
    {
        Self {
            first_table_name: table_diff_row.first_table_name.to_string(),
            first_table_state: table_diff_row.first_table_state.to_string(),
            first_keyspace_id: table_diff_row.first_keyspace_id.to_string(),
            second_table_name: storedtables.table_name.to_string(),
            second_table_state: storedtables.table_state.to_string(),
            second_keyspace_id: storedtables.keyspace_id,
        }
    }
}

// keyspace_id is the unique key
//pub keyspace_id: String,
#[derive(Debug)]
pub struct SnapshotDiffKeyspaces
{
    pub first_keyspace_name: String,
    pub first_keyspace_type: String,
    pub second_keyspace_name: String,
    pub second_keyspace_type: String,
}

impl SnapshotDiffKeyspaces {
    fn first_snapshot ( storedkeyspaces: StoredKeyspaces ) -> Self
    {
        Self {
            first_keyspace_name: storedkeyspaces.keyspace_name.to_string(),
            first_keyspace_type: storedkeyspaces.keyspace_type,
            second_keyspace_name: "".to_string(),
            second_keyspace_type: "".to_string(),
        }
    }
    fn second_snapshot_new ( storedkeyspaces: StoredKeyspaces ) -> Self
    {
        Self {
            first_keyspace_name: "".to_string(),
            first_keyspace_type: "".to_string(),
            second_keyspace_name: storedkeyspaces.keyspace_name.to_string(),
            second_keyspace_type: storedkeyspaces.keyspace_type,
        }
    }
    fn second_snapshot_existing ( keyspaces_diff_row: &mut SnapshotDiffKeyspaces, storedkeyspaces: StoredKeyspaces ) -> Self
    {
        Self {
            first_keyspace_name: keyspaces_diff_row.first_keyspace_name.to_string(),
            first_keyspace_type: keyspaces_diff_row.first_keyspace_type.to_string(),
            second_keyspace_name: storedkeyspaces.keyspace_name.to_string(),
            second_keyspace_type: storedkeyspaces.keyspace_type,
        }
    }
}

// String, String = tablet_id, server_uuid
type BTreeMapSnapshotDiffReplicas = BTreeMap<(String, String), SnapshotDiffReplica>;
// String = tablet_id
type BTreeMapSnapshotDiffTablets = BTreeMap<String, SnapshotDiffTablets>;
// String = table_id
type BTreeMapSnapshotDiffTables = BTreeMap<String, SnapshotDiffTables>;
// String = keyspace_id
type BTreeMapSnapshotDiffKeyspaces = BTreeMap<String, SnapshotDiffKeyspaces>;

pub struct SnapshotDiffBTreeMapsEntities {
    pub btreemap_snapshotdiff_replicas: BTreeMapSnapshotDiffReplicas,
    pub btreemap_snapshotdiff_tablets: BTreeMapSnapshotDiffTablets,
    pub btreemap_snapshotdiff_tables: BTreeMapSnapshotDiffTables,
    pub btreemap_snapshotdiff_keyspaces: BTreeMapSnapshotDiffKeyspaces,
    pub keyspace_id_lookup: HashMap<String, (String, String)>,
    pub table_keyspace_lookup: HashMap<String, String>,
    pub table_id_lookup: HashMap<String, String>,
    pub server_id_lookup: HashMap<String, String>,
    pub tablet_table_lookup: HashMap<String, String>,
    pub master_found: bool,
}

impl SnapshotDiffBTreeMapsEntities {
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
        details_enable: &bool,
    ) -> SnapshotDiffBTreeMapsEntities
    {
        let allstoredentities = AllStoredEntities::read_snapshot(begin_snapshot)
            .unwrap_or_else(|e| {
                error!("Fatal: error reading snapshot: {}", e);
                process::exit(1);
            });
        let master_leader = AllStoredIsLeader::return_leader_snapshot(begin_snapshot);
        let mut entities_snapshot_diff = SnapshotDiffBTreeMapsEntities::first_snapshot(allstoredentities, master_leader, details_enable);

        let allstoredentities = AllStoredEntities::read_snapshot(end_snapshot)
            .unwrap_or_else(|e| {
                error!("Fatal: error reading snapshot: {}", e);
                process::exit(1);
            });
        let master_leader = AllStoredIsLeader::return_leader_snapshot(end_snapshot);
        entities_snapshot_diff.second_snapshot(allstoredentities, master_leader, details_enable);

        entities_snapshot_diff
    }
    fn first_snapshot(
        allstoredentities: AllStoredEntities,
        master_leader: String,
        details_enable: &bool,
    ) -> SnapshotDiffBTreeMapsEntities {
        let mut snapshotdiff_btreemaps = SnapshotDiffBTreeMapsEntities {
            btreemap_snapshotdiff_replicas: Default::default(),
            btreemap_snapshotdiff_tablets: Default::default(),
            btreemap_snapshotdiff_tables: Default::default(),
            btreemap_snapshotdiff_keyspaces: Default::default(),
            keyspace_id_lookup: HashMap::new(),
            table_keyspace_lookup: HashMap::new(),
            table_id_lookup: HashMap::new(),
            server_id_lookup: HashMap::new(),
            tablet_table_lookup: HashMap::new(),
            master_found: true,
        };
        if master_leader == *"" {
            snapshotdiff_btreemaps.master_found = false;
            return snapshotdiff_btreemaps;
        };
        let object_oid_number = |oid: &str| -> u32 {
            // The oid entry normally is a 32 byte UUID for both ycql and ysql, which only contains hexadecimal numbers.
            // However, there is a single entry in system_schema that has a table_id that is 'sys.catalog.uuid'
            if oid.len() == 32_usize {
                let true_oid = &oid[24..];
                u32::from_str_radix(true_oid, 16).unwrap()
            } else {
                0
            }
        };
        //replicas
        for row in allstoredentities.stored_replicas.into_iter().filter(|r| r.hostname_port == master_leader.clone() ) {
            snapshotdiff_btreemaps.server_id_lookup.entry(row.server_uuid.to_string()).or_insert_with(|| row.addr.to_string());

            match snapshotdiff_btreemaps.btreemap_snapshotdiff_replicas.get_mut( &(row.tablet_id.clone(), row.server_uuid.clone())) {
                Some( _replica_row ) => {
                    error!("Found second entry for first entry of replica, hostname: {}, tablet_id: {}, server_uuid: {}", &row.hostname_port.clone(), &row.tablet_id.clone(), &row.server_uuid.clone());
                },
                None => {
                    snapshotdiff_btreemaps.btreemap_snapshotdiff_replicas.insert(
                        (row.tablet_id.to_string(), row.server_uuid.to_string()),
                        SnapshotDiffReplica::first_snapshot(row)
                    );
                },
            }
        };
        //tablets
        for row in allstoredentities.stored_tablets.into_iter().filter(|r| r.hostname_port == master_leader.clone() ) {
            snapshotdiff_btreemaps.tablet_table_lookup.entry(row.tablet_id.to_string()).or_insert_with(|| row.table_id.to_string());
            match snapshotdiff_btreemaps.btreemap_snapshotdiff_tablets.get_mut( &(row.tablet_id.clone())) {
                Some( _tablet_row ) => {
                    error!("Found second entry for first entry of tablet, hostname: {}, tablet_id: {}", &row.hostname_port.clone(), &row.tablet_id.clone());
                },
                None => {
                    snapshotdiff_btreemaps.btreemap_snapshotdiff_tablets.insert(
                        row.tablet_id.to_string(),
                        SnapshotDiffTablets::first_snapshot(row)
                    );
                },
            }
        };
        // table counts for ysql keyspaces
        let mut table_counts_hashmap = HashMap::new();
        for keyspace in allstoredentities.stored_keyspaces.iter().filter(|r| r.hostname_port == master_leader.clone() ) {
            let count = allstoredentities.stored_tables.iter()
                .filter(|r| r.keyspace_id == keyspace.keyspace_id && keyspace.keyspace_type == "ysql" )
                .count();
            if keyspace.keyspace_type == "ysql"
            {
                table_counts_hashmap.insert(keyspace.keyspace_id.to_string(), count);
            };
        }
        // tables
        for row in allstoredentities.stored_tables.into_iter().filter(|r| r.hostname_port == master_leader.clone() ) {
            snapshotdiff_btreemaps.table_id_lookup.entry(row.table_id.to_string()).or_insert_with(|| row.table_name.to_string());
            snapshotdiff_btreemaps.table_keyspace_lookup.entry(row.table_id.to_string()).or_insert_with(|| row.keyspace_id.to_string());
            if allstoredentities.stored_keyspaces.iter().find(|r| r.keyspace_id == row.keyspace_id).map(|r| &r.keyspace_type).unwrap() == "ysql"
            && object_oid_number(row.table_id.as_str()) < 16384
            && !*details_enable
            {
                continue
            }
            match snapshotdiff_btreemaps.btreemap_snapshotdiff_tables.get_mut( &row.table_id.clone() ) {
                Some( _table_row ) => {
                    error!("Found second entry for first entry of table, hostname: {}, table_id: {}", &row.hostname_port.clone(), &row.table_id.clone());
                },
                None => {
                    snapshotdiff_btreemaps.btreemap_snapshotdiff_tables.insert(
                        row.table_id.to_string(),
                        SnapshotDiffTables::first_snapshot(row)
                    );
                },
            }
        };
        // keyspaces
        for row in allstoredentities.stored_keyspaces.into_iter().filter(|r| r.hostname_port == master_leader.clone() ) {
            snapshotdiff_btreemaps.keyspace_id_lookup.entry(row.keyspace_id.to_string()).or_insert_with(|| (row.keyspace_type.to_string(), row.keyspace_name.to_string()));

            // if the keyspace is not in table_counts_hashmap it's not a ysql keyspace, and then gets the value 1 to pass.
            // if the keyspace is in the table_counts_hashmap it must have more than one table to be a non-dropped database.
            if table_counts_hashmap.get(&row.keyspace_id.to_string()).unwrap_or( &1_usize ) > &0_usize {
                match snapshotdiff_btreemaps.btreemap_snapshotdiff_keyspaces.get_mut(&row.keyspace_id.clone()) {
                    Some(_keyspace_row) => {
                        error!("Found second entry for first entry of keyspace, hostname: {}, keyspace_id: {}", &row.hostname_port.clone(), &row.keyspace_id.clone());
                    },
                    None => {
                        snapshotdiff_btreemaps.btreemap_snapshotdiff_keyspaces.insert(
                            row.keyspace_id.to_string(),
                            SnapshotDiffKeyspaces::first_snapshot(row)
                        );
                    },
                }
            }
        };
        snapshotdiff_btreemaps
    }
    /// The second snapshot function checks the entries in
    fn second_snapshot(
        &mut self,
        allstoredentities: AllStoredEntities,
        master_leader: String,
        details_enable: &bool,
    )
    {
        if master_leader == *"" {
            self.master_found = false;
            return;
        };
        let object_oid_number = |oid: &str| -> u32 {
            // The oid entry normally is a 32 byte UUID for both ycql and ysql, which only contains hexadecimal numbers.
            // However, there is a single entry in system_schema that has a table_id that is 'sys.catalog.uuid'
            if oid.len() == 32_usize {
                let true_oid = &oid[24..];
                u32::from_str_radix(true_oid, 16).unwrap()
            } else {
                0
            }
        };
        // replicas
        for row in allstoredentities.stored_replicas.into_iter().filter(|r| r.hostname_port == master_leader.clone() ) {
            self.server_id_lookup.entry(row.server_uuid.to_string()).or_insert_with(|| row.addr.to_string());
            match self.btreemap_snapshotdiff_replicas.get_mut( &(row.tablet_id.clone(), row.server_uuid.clone())) {
                Some( replica_row ) => {
                    if replica_row.first_addr == row.addr
                    && replica_row.first_replica_type == row.replica_type
                    {
                        // second snapshot contains identical values, so we remove it.
                        self.btreemap_snapshotdiff_replicas.remove( &(row.tablet_id.clone(), row.server_uuid.clone()) );
                    }
                    else
                    {
                        // second snapshot has the same key, but contains different values (?)
                        *replica_row = SnapshotDiffReplica::second_snapshot_existing(replica_row, row);

                    };
                },
                None => {
                    self.btreemap_snapshotdiff_replicas.insert(
                        (row.tablet_id.to_string(), row.server_uuid.to_string()),
                        SnapshotDiffReplica::second_snapshot_new(row)
                    );
                },
            }
        }
        // tablets
        for row in allstoredentities.stored_tablets.into_iter().filter(|r| r.hostname_port == master_leader.clone() ) {
            self.tablet_table_lookup.entry(row.tablet_id.to_string()).or_insert_with(|| row.table_id.to_string());
            match self.btreemap_snapshotdiff_tablets.get_mut( &(row.tablet_id.clone())) {
                Some( tablet_row ) => {
                    if tablet_row.first_table_id == row.table_id
                    && tablet_row.first_tablet_state == row.tablet_state
                    && tablet_row.first_leader == row.leader
                    {
                        // second snapshot contains identical values, so we remove it.
                        self.btreemap_snapshotdiff_tablets.remove( &(row.tablet_id.clone()) );
                    }
                    else
                    {
                        // second snapshot has the same key, but contains different values (?)
                        *tablet_row = SnapshotDiffTablets::second_snapshot_existing(tablet_row, row);
                    };
                },
                None => {
                    self.btreemap_snapshotdiff_tablets.insert(
                        row.tablet_id.to_string(),
                        SnapshotDiffTablets::second_snapshot_new(row)
                    );
                },
            }
        }
        // table counts for ysql keyspaces
        let mut table_counts_hashmap = HashMap::new();
        for keyspace in allstoredentities.stored_keyspaces.iter().filter(|r| r.hostname_port == master_leader.clone() ) {
            let count = allstoredentities.stored_tables.iter()
                .filter(|r| r.keyspace_id == keyspace.keyspace_id && keyspace.keyspace_type == "ysql" )
                .count();
            if keyspace.keyspace_type == "ysql"
            {
                table_counts_hashmap.insert(keyspace.keyspace_id.to_string(), count);
            };
        }
        // tables
        for row in allstoredentities.stored_tables.into_iter().filter(|r| r.hostname_port == master_leader.clone() ) {
            self.table_id_lookup.entry(row.table_id.to_string()).or_insert_with(|| row.table_name.to_string());
            self.table_keyspace_lookup.entry(row.table_id.to_string()).or_insert_with(|| row.keyspace_id.to_string());
            if allstoredentities.stored_keyspaces.iter().find(|r| r.keyspace_id == row.keyspace_id).map(|r| &r.keyspace_type).unwrap() == "ysql"
            && object_oid_number(row.table_id.as_str()) < 16384
            && !*details_enable
            {
                continue;
            }
            match self.btreemap_snapshotdiff_tables.get_mut( &(row.table_id.clone())) {
                Some( table_row ) => {
                    if table_row.first_table_name == row.table_name
                    && table_row.first_table_state == row.table_state
                    && table_row.first_keyspace_id == row.keyspace_id
                    {
                        // second snapshot contains identical values, so we remove it.
                        self.btreemap_snapshotdiff_tables.remove( &(row.table_id.clone()) );
                    }
                    else {
                        // second snapshot has the same key, but contains different values.
                        *table_row = SnapshotDiffTables::second_snapshot_existing(table_row, row);
                    };
                },
                None => {
                    self.btreemap_snapshotdiff_tables.insert(
                        row.table_id.to_string(),
                        SnapshotDiffTables::second_snapshot_new(row)
                    );
                },
            }
        }
        // keyspaces
        for row in allstoredentities.stored_keyspaces.into_iter().filter(|r| r.hostname_port == master_leader.clone() ) {
            self.keyspace_id_lookup.entry(row.keyspace_id.to_string()).or_insert((row.keyspace_type.to_string(), row.keyspace_name.to_string()));
            // if the keyspace is not in table_counts_hashmap it's not a ysql keyspace, and then gets the value 1 to pass.
            // if the keyspace is in the table_counts_hashmap it must have more than one table to be a non-dropped database.
            if table_counts_hashmap.get(&row.keyspace_id.to_string()).unwrap_or( &1_usize ) > &0_usize {
                match self.btreemap_snapshotdiff_keyspaces.get_mut(&(row.keyspace_id.clone())) {
                    Some(keyspace_row) => {
                        if keyspace_row.first_keyspace_name == row.keyspace_name
                            && keyspace_row.first_keyspace_type == row.keyspace_type
                        {
                            // second snapshot contains identical values, so we remove it.
                            self.btreemap_snapshotdiff_keyspaces.remove(&(row.keyspace_id.clone()));
                        } else {
                            // second snapshot has the same key, but contains different values.
                            *keyspace_row = SnapshotDiffKeyspaces::second_snapshot_existing(keyspace_row, row);
                        };
                    },
                    None => {
                        self.btreemap_snapshotdiff_keyspaces.insert(
                            row.keyspace_id.to_string(),
                            SnapshotDiffKeyspaces::second_snapshot_new(row)
                        );
                    },
                }
            }
        }
    }
    pub fn adhoc_read_first_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> SnapshotDiffBTreeMapsEntities
    {
        let allstoredentities = AllStoredEntities::read_entities(hosts, ports, parallel);
        let master_leader= AllStoredIsLeader::return_leader_http(hosts, ports, parallel);
        SnapshotDiffBTreeMapsEntities::first_snapshot(allstoredentities, master_leader, &false )
    }
    pub fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allstoredentities = AllStoredEntities::read_entities(hosts, ports, parallel);
        let master_leader= AllStoredIsLeader::return_leader_http(hosts, ports, parallel);
        self.second_snapshot(allstoredentities, master_leader, &false);
    }
    pub fn print(
        &self,
    )
    {
        if ! self.master_found {
            println!("Master leader was not found in hosts specified, skipping entity diff.");
            return;
        }
        for (keyspace_id, keyspace_row) in self.btreemap_snapshotdiff_keyspaces.iter() {
            if keyspace_row.first_keyspace_name.is_empty()
            && keyspace_row.first_keyspace_type.is_empty()
            {
                let colocation = if self.btreemap_snapshotdiff_tablets.iter()
                    .any(|(_k,v)| v.second_table_id == format!("{}.colocated.parent.uuid", &keyspace_id)) {
                    "[colocated]"
                } else {
                    ""
                };
                println!("{} Database: {}.{}, id: {} {}", "+".to_string().green(), keyspace_row.second_keyspace_type, keyspace_row.second_keyspace_name, keyspace_id, colocation);
            } else if keyspace_row.second_keyspace_name.is_empty()
            && keyspace_row.second_keyspace_type.is_empty()
            {
                let colocation = if self.btreemap_snapshotdiff_tablets.iter()
                    .any(|(_k,v)| v.first_table_id == format!("{}.colocated.parent.uuid", &keyspace_id)) {
                    "[colocated]"
                } else {
                    ""
                };
                println!("{} Database: {}.{}, id: {} {}", "-".to_string().red(), keyspace_row.first_keyspace_type, keyspace_row.first_keyspace_name, keyspace_id, colocation);
            }
        }
        for (table_id, table_row) in &self.btreemap_snapshotdiff_tables {
            if table_row.first_table_name.is_empty()
            && table_row.first_table_state.is_empty()
            && table_row.first_keyspace_id.is_empty()
            {
                //println!("{} Table id: {}, name: {}, state: {}, keyspace: {}", format!("+").green(), table_id, table_row.second_table_name, table_row.second_table_state, table_row.second_keyspace_id);
                println!("{} Table:    {}.{}.{}, state: {}, id: {}",
                         "+".to_string().green(),
                         &self.keyspace_id_lookup.get(&table_row.second_keyspace_id).unwrap().0,
                         &self.keyspace_id_lookup.get(&table_row.second_keyspace_id).unwrap().1,
                         table_row.second_table_name,
                         table_row.second_table_state,
                         table_id,
                );
            }
            if table_row.second_table_name.is_empty()
            && table_row.second_table_state.is_empty()
            && table_row.second_keyspace_id.is_empty()
            {
                //println!("{} Table id: {}, name: {}, state: {}, keyspace: {}", format!("-").red(), table_id, table_row.first_table_name, table_row.first_table_state, table_row.first_keyspace_id);
                println!("{} Table:    {}.{}.{}, state: {}, id: {}",
                         "-".to_string().red(),
                         &self.keyspace_id_lookup.get(&table_row.first_keyspace_id).unwrap().0,
                         &self.keyspace_id_lookup.get(&table_row.first_keyspace_id).unwrap().1,
                         table_row.first_table_name,
                         table_row.first_table_state,
                         table_id,
                );
            }
        }
        for (tablet_id, tablet_row) in &self.btreemap_snapshotdiff_tablets {
            if tablet_row.first_table_id.is_empty()
            && tablet_row.first_tablet_state.is_empty()
            && tablet_row.first_leader.is_empty()
            {
                //println!("{} Tablet id: {}, state: {}, leader: {}, table_id: {}", format!("+").green(), tablet_id, tablet_row.second_tablet_state, tablet_row.second_leader, tablet_row.second_table_id);
                println!("{} Tablet:   {}.{}.{}.{} state: {}, leader: {}",
                         "+".to_string().green(),
                         self.keyspace_id_lookup.get(self.table_keyspace_lookup.get(&tablet_row.second_table_id).unwrap()).unwrap().0,
                         self.keyspace_id_lookup.get(self.table_keyspace_lookup.get(&tablet_row.second_table_id).unwrap()).unwrap().1,
                         self.table_id_lookup.get(&tablet_row.second_table_id).unwrap(),
                         tablet_id,
                         tablet_row.second_tablet_state,
                         self.server_id_lookup.get(&tablet_row.second_leader).unwrap(),
                         //tablet_row.second_leader
                );
            }
            else if tablet_row.second_table_id.is_empty()
            && tablet_row.second_tablet_state.is_empty()
            && tablet_row.second_leader.is_empty()
            {
                //println!("{} Tablet id: {}, state: {}, leader: {}, table_id: {}", format!("-").red(), tablet_id, tablet_row.first_tablet_state, tablet_row.first_leader, tablet_row.first_table_id);
                println!("{} Tablet:   {}.{}.{}.{} state: {}, leader: {}",
                         "-".to_string().red(),
                         self.keyspace_id_lookup.get(self.table_keyspace_lookup.get(&tablet_row.first_table_id).unwrap()).unwrap().0,
                         self.keyspace_id_lookup.get(self.table_keyspace_lookup.get(&tablet_row.first_table_id).unwrap()).unwrap().1,
                         self.table_id_lookup.get(&tablet_row.first_table_id).unwrap(),
                         tablet_id,
                         tablet_row.first_tablet_state,
                         self.server_id_lookup.get(&tablet_row.first_leader).unwrap(),
                         //tablet_row.first_leader
                );
            }
            else
            {
                //println!("{} Tablet id: {}, state: {} > {}, leader: {} > {}, table_id: {}", format!("*").yellow(), tablet_id, tablet_row.first_tablet_state, tablet_row.second_tablet_state, tablet_row.first_leader, tablet_row.second_leader, tablet_row.first_table_id);
                println!("{} Tablet:   {}.{}.{}.{} state: {}->{}, leader: {}->{}",
                         "*".to_string().yellow(),
                         self.keyspace_id_lookup.get(self.table_keyspace_lookup.get(&tablet_row.first_table_id).unwrap()).unwrap().0,
                         self.keyspace_id_lookup.get(self.table_keyspace_lookup.get(&tablet_row.first_table_id).unwrap()).unwrap().1,
                         self.table_id_lookup.get(&tablet_row.first_table_id).unwrap(),
                         tablet_id,
                         tablet_row.first_tablet_state,
                         tablet_row.second_tablet_state,
                         self.server_id_lookup.get(&tablet_row.first_leader).unwrap(),
                         self.server_id_lookup.get(&tablet_row.second_leader).unwrap(),
                         //tablet_row.first_leader,
                         //tablet_row.second_leader,
                );
            }
        }
        for ((tablet_id, _server_uuid), replica_row) in &self.btreemap_snapshotdiff_replicas {
            if replica_row.first_addr.is_empty()
            && replica_row.first_replica_type.is_empty()
            {
                println!("{} Replica:  {}.{}.{}.{} server: {}, type: {}",
                    "+".to_string().green(),
                    self.keyspace_id_lookup.get(self.table_keyspace_lookup.get(self.tablet_table_lookup.get(tablet_id).unwrap()).unwrap()).unwrap().0,
                    self.keyspace_id_lookup.get(self.table_keyspace_lookup.get(self.tablet_table_lookup.get(tablet_id).unwrap()).unwrap()).unwrap().1,
                    self.table_id_lookup.get(self.tablet_table_lookup.get(tablet_id).unwrap()).unwrap(),
                    tablet_id,
                    replica_row.second_addr,
                    replica_row.second_replica_type,
                );
            }
            else if replica_row.second_addr.is_empty()
            && replica_row.second_replica_type.is_empty()
            {
                println!("{} Replica:  {}.{}.{}.{} server: {}, type: {}",
                         "-".to_string().red(),
                         self.keyspace_id_lookup.get(self.table_keyspace_lookup.get(self.tablet_table_lookup.get(tablet_id).unwrap()).unwrap()).unwrap().0,
                         self.keyspace_id_lookup.get(self.table_keyspace_lookup.get(self.tablet_table_lookup.get(tablet_id).unwrap()).unwrap()).unwrap().1,
                         self.table_id_lookup.get(self.tablet_table_lookup.get(tablet_id).unwrap()).unwrap(),
                         tablet_id,
                         replica_row.first_addr,
                         replica_row.first_replica_type,
                );
            }
            else
            {
                println!("{} Replica:  {}.{}.{}.{} server: {}, type: {}->{}",
                         "*".to_string().yellow(),
                         self.keyspace_id_lookup.get(self.table_keyspace_lookup.get(self.tablet_table_lookup.get(tablet_id).unwrap()).unwrap()).unwrap().0,
                         self.keyspace_id_lookup.get(self.table_keyspace_lookup.get(self.tablet_table_lookup.get(tablet_id).unwrap()).unwrap()).unwrap().1,
                         self.table_id_lookup.get(self.tablet_table_lookup.get(tablet_id).unwrap()).unwrap(),
                         tablet_id,
                         replica_row.first_addr,
                         replica_row.first_replica_type,
                         replica_row.second_replica_type,
                );
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
        let result = AllStoredEntities::parse_entities(json, "", "");
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
        let mut allstoredentities = AllStoredEntities { stored_keyspaces: Vec::new(), stored_tables: Vec::new(), stored_tablets: Vec::new(), stored_replicas: Vec::new() };

        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let json = AllStoredEntities::read_http(hostname.as_str(), port.as_str());
        AllStoredEntities::split_into_vectors(json, format!("{}:{}", hostname, port).as_str(), Local::now(), &mut allstoredentities);

        assert!(!allstoredentities.stored_tables.is_empty());
        assert!(!allstoredentities.stored_tablets.is_empty());
        assert!(!allstoredentities.stored_replicas.is_empty());
    }

}