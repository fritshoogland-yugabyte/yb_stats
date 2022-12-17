use chrono::{DateTime, Local};
use std::collections::{BTreeMap, HashMap};

type BTreeMapSnapshotDiffReplicas = BTreeMap<(String, String), SnapshotDiffReplica>;
type BTreeMapSnapshotDiffTablets = BTreeMap<String, SnapshotDiffTablets>;
type BTreeMapSnapshotDiffTables = BTreeMap<String, SnapshotDiffTables>;
type BTreeMapSnapshotDiffKeyspaces = BTreeMap<String, SnapshotDiffKeyspaces>;

#[derive(Serialize, Deserialize, Debug, Default)]
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

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredKeyspaces
{
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub keyspace_id: String,
    pub keyspace_name: String,
    pub keyspace_type: String,
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

#[derive(Debug)]
pub struct SnapshotDiffReplica {
    pub first_replica_type: String,
    pub first_addr: String,
    pub second_replica_type: String,
    pub second_addr: String,
}

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

#[derive(Debug)]
pub struct SnapshotDiffTables {
    pub first_table_name: String,
    pub first_table_state: String,
    pub first_keyspace_id: String,
    pub second_table_name: String,
    pub second_table_state: String,
    pub second_keyspace_id: String,
}

#[derive(Debug)]
pub struct SnapshotDiffKeyspaces
{
    pub first_keyspace_name: String,
    pub first_keyspace_type: String,
    pub second_keyspace_name: String,
    pub second_keyspace_type: String,
}

#[derive(Default)]
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