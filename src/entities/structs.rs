//! The structs
use chrono::{DateTime, Local};
use std::collections::BTreeMap;

//type BTreeMapSnapshotDiffReplicas = BTreeMap<(String, String), SnapshotDiffReplica>;
//type BTreeMapSnapshotDiffTablets = BTreeMap<String, SnapshotDiffTablets>;
//type BTreeMapSnapshotDiffTables = BTreeMap<String, SnapshotDiffTables>;
//type BTreeMapSnapshotDiffKeyspaces = BTreeMap<String, SnapshotDiffKeyspaces>;

/// The root struct for deserializing `/dump-entities`
///
/// This struct is the begin struct needed to parse the results from master:port/dump-entities:
/// ```text
/// {
///   "keyspaces": [
///     {
///       "keyspace_id": "00000000000000000000000000000001",
///       "keyspace_name": "system",
///       "keyspace_type": "ycql"
///     },
///    ...
///   "tables": [
///     {
///       "table_id": "000000010000300080000000000000af",
///       "keyspace_id": "00000001000030008000000000000000",
///       "table_name": "pg_user_mapping_user_server_index",
///       "state": "RUNNING"
///     },
///    ...
///  "tablets": [
///     {
///       "table_id": "8df210c6ccf442bf8b324ab827478505",
///       "tablet_id": "08b1f1b9fd47407db33c696ebf10d847",
///       "state": "RUNNING",
///       "replicas": [
///         {
///           "type": "VOTER",
///           "server_uuid": "0ec4306a3fe2421c8e891acde43f4276",
///           "addr": "yb-3.local:9100"
///         },
///         {
///           "type": "VOTER",
///           "server_uuid": "a9f3342741564167824c25b3303ac7c1",
///           "addr": "yb-1.local:9100"
///         },
///         {
///           "type": "VOTER",
///           "server_uuid": "eda624cb7e864ff6aa2b25dfc27e64ea",
///           "addr": "yb-2.local:9100"
///         }
///       ],
///       "leader": "eda624cb7e864ff6aa2b25dfc27e64ea"
///     },
/// ```
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Entities {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the timestamp.
    pub timestamp: Option<DateTime<Local>>,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Replicas {
    #[serde(rename = "type")]
    pub replica_type: String,
    pub server_uuid: String,
    pub addr: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AllEntities {
    pub entities: Vec<Entities>,
}

#[derive(Debug, Default)]
pub struct EntitiesDiff {
    pub btreekeyspacediff: BTreeKeyspaceDiff,
    pub btreetablesdiff: BTreeTablesDiff,
    pub btreetabletsdiff: BTreeTabletsDiff,
    pub btreereplicasdiff: BTreeReplicasDiff,
    pub master_found: bool,
}
// String = keyspace_id
type BTreeKeyspaceDiff = BTreeMap<String, KeyspaceDiff>;
#[derive(Debug, Default)]
pub struct KeyspaceDiff {
    pub first_keyspace_name: String,
    pub first_keyspace_type: String,
    pub second_keyspace_name: String,
    pub second_keyspace_type: String,
}
// String = table_id
type BTreeTablesDiff = BTreeMap<String, TablesDiff>;
#[derive(Debug, Default)]
pub struct TablesDiff {
    pub first_keyspace_id: String,
    pub first_table_name: String,
    pub first_state: String,
    pub second_keyspace_id: String,
    pub second_table_name: String,
    pub second_state: String,
}
// String = tablet_id
type BTreeTabletsDiff = BTreeMap<String, TabletsDiff>;
#[derive(Debug, Default)]
pub struct TabletsDiff {
    pub first_table_id: String,
    pub first_state: String,
    pub first_leader: String,
    pub second_table_id: String,
    pub second_state: String,
    pub second_leader: String,
}
// (String, String) = (tablet_id, server_uuid)
type  BTreeReplicasDiff = BTreeMap<(String, String), ReplicasDiff>;
#[derive(Debug, Default)]
pub struct ReplicasDiff {
    pub first_replica_type: String,
    pub first_addr: String,
    pub second_replica_type: String,
    pub second_addr: String,
}
/*

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
 */