use std::collections::BTreeMap;
use chrono::{DateTime, Local};

#[derive(Serialize, Deserialize, Debug)]
pub struct AllMasters {
    pub masters: Vec<Masters>,
}

#[derive(Default)]
pub struct AllStoredMasters {
    pub stored_masters: Vec<StoredMasters>,
    pub stored_rpc_addresses: Vec<StoredRpcAddresses>,
    pub stored_http_addresses: Vec<StoredHttpAddresses>,
    pub stored_master_error: Vec<StoredMasterError>,
}

#[derive(Debug)]
pub struct SnapshotDiffStoredMasters {
    pub first_instance_seqno: i64,
    pub first_start_time_us: i64,
    pub first_registration_cloud_placement_cloud: String,
    pub first_registration_cloud_placement_region: String,
    pub first_registration_cloud_placement_zone: String,
    pub first_registration_placement_uuid: String,
    pub first_role: String,
    pub second_instance_seqno: i64,
    pub second_start_time_us: i64,
    pub second_registration_cloud_placement_cloud: String,
    pub second_registration_cloud_placement_region: String,
    pub second_registration_cloud_placement_zone: String,
    pub second_registration_placement_uuid: String,
    pub second_role: String,
}

type BTreeMapSnapshotDiffMasters = BTreeMap<String, SnapshotDiffStoredMasters>;
type BTreeMapSnapshotDiffHttpAddresses = BTreeMap<(String, String), SnapshotDiffHttpAddresses>;
type BTreeMapSnapshotDiffRpcAddresses = BTreeMap<(String, String), SnapshotDiffRpcAddresses>;
#[derive(Debug)]
pub struct PermanentUuidHttpAddress {
    pub permanent_uuid: String,
    pub hostname_port: String,
}
#[derive(Debug)]
pub struct PermanentUuidRpcAddress {
    pub permanent_uuid: String,
    pub hostname_port: String,
}

#[derive(Default)]
pub struct SnapshotDiffBTreeMapsMasters {
    pub btreemap_snapshotdiff_masters: BTreeMapSnapshotDiffMasters,
    pub btreemap_snapshotdiff_httpaddresses: BTreeMapSnapshotDiffHttpAddresses, // currently no diff for http addresses
    pub btreemap_snapshotdiff_rpcaddresses: BTreeMapSnapshotDiffRpcAddresses, // currently no diff for rpc addresses
    pub first_http_addresses: Vec<PermanentUuidHttpAddress>,
    pub second_http_addresses: Vec<PermanentUuidHttpAddress>,
    pub first_rpc_addresses: Vec<PermanentUuidRpcAddress>,
    pub second_rpc_addresses: Vec<PermanentUuidRpcAddress>,
    pub master_found: bool,
}

#[derive(Debug)]
pub struct SnapshotDiffRpcAddresses {
    pub first_port: String,
    pub second_port: String,
}
#[derive(Debug)]
pub struct SnapshotDiffHttpAddresses {
    pub first_port: String,
    pub second_port: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Masters {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<MasterError>,
    pub instance_id: InstanceId,
    pub registration: Registration,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MasterError {
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
