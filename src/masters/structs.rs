#![allow(non_camel_case_types)]

//use std::collections::BTreeMap;
use chrono::{DateTime, Local};

// src/yb/common/wire_protocol.proto
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct NodeInstancePB {
    pub permanent_uuid: String,
    pub instance_seqno: i64,
    pub start_time_us: Option<u64>,
}
// src/yb/common/wire_protocol.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct ServerRegistrationPB {
    pub private_rpc_addresses: Option<Vec<HostPortPB>>,
    pub http_addresses: Option<Vec<HostPortPB>>,
    pub cloud_info: Option<CloudInfoPB>,
    pub placement_uuid: Option<String>,
    pub broadcast_addresses: Option<Vec<HostPortPB>>,
    pub pg_port: Option<u64>,
}
// src/yb/common/common_net.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct HostPortPB {
    pub host: String,
    pub port: u32,
}
// src/yb/common/common_net.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct CloudInfoPB {
   pub placement_cloud: Option<String>,
   pub placement_region: Option<String>,
   pub placement_zone: Option<String>,
}
// src/yb/master/master_cluster.proto
// code is ErrorCode, not Code.
/*
#[derive(Serialize, Deserialize, Debug)]
pub struct MasterErrorPB {
    //pub code: Code,
    pub code: ErrorCode,
    pub status: AppStatusPB,
}
 */
// src/yb/master/master_cluster.proto
#[derive(Serialize, Deserialize, Debug)]
pub enum Code {
    // An error which has no more specific error code.
    // The code and message in 'status' may reveal more details.
    //
    // RPCs should avoid returning this, since callers will not be
    // able to easily parse the error.
    UNKNOWN_ERROR = 1,
    // The schema provided for a request was not well-formed.
    INVALID_SCHEMA = 2,
    // The requested table or index does not exist
    OBJECT_NOT_FOUND = 3,
    // The name requested for the table or index is already in use
    OBJECT_ALREADY_PRESENT = 4,
    // The number of tablets requested for a new table is over the per TS limit.
    TOO_MANY_TABLETS = 5,
    // Catalog manager is not yet initialized.
    CATALOG_MANAGER_NOT_INITIALIZED = 6,
    // The operation attempted can only be invoked against either the
    // leader or a single non-distributed master, which this node
    // isn't.
    NOT_THE_LEADER = 7,
    // The number of replicas requested is greater than the number of live servers
    // in the cluster.
    REPLICATION_FACTOR_TOO_HIGH = 8,
    // Change config should always be issued with the latest config version set.
    // If the client fails to do so, or there is a concurrent change, we will
    // set this error code.
    CONFIG_VERSION_MISMATCH = 9,
    // If there is an operation in progress causing the current rpc to be in an indeterminate state,
    // we return this error code. Client can retry on a case by case basis as needed.
    IN_TRANSITION_CAN_RETRY = 10,
    // Invalid namespace name or id for the namespace operation.
    NAMESPACE_NOT_FOUND = 11,
    NAMESPACE_ALREADY_PRESENT = 12,
    NO_NAMESPACE_USED = 13,
    NAMESPACE_IS_NOT_EMPTY = 14,
    // Client set some fields incorrectly in the cluster config proto.
    INVALID_CLUSTER_CONFIG = 15,
    // Indicator to client that the load balance operation can be retried.
    CAN_RETRY_LOAD_BALANCE_CHECK = 16,
    // Invalid (User-Defined) Type operation
    TYPE_NOT_FOUND = 17,
    INVALID_TYPE = 18,
    TYPE_ALREADY_PRESENT = 19,
    // Snapshot related errors.
    INVALID_TABLE_TYPE = 20,
    TABLE_CREATION_IS_IN_PROGRESS = 21,
    SNAPSHOT_NOT_FOUND = 22,
    SNAPSHOT_FAILED = 23,
    SNAPSHOT_CANCELLED = 24,
    PARALLEL_SNAPSHOT_OPERATION = 25,
    SNAPSHOT_IS_NOT_READY = 26,
    // Roles and permissions errors.
    ROLE_ALREADY_PRESENT = 27,
    ROLE_NOT_FOUND = 28,
    INVALID_REQUEST = 29,
    NOT_AUTHORIZED = 32,
    // Indicator to client that the are leaders on preferred only operation can be retried.
    CAN_RETRY_ARE_LEADERS_ON_PREFERRED_ONLY_CHECK = 30,
    REDIS_CONFIG_NOT_FOUND = 31,
    // Indicator to client that load balancer was recently active.
    LOAD_BALANCER_RECENTLY_ACTIVE = 33,
    INTERNAL_ERROR = 34,
    // Client set some fields in the table replication info incorrectly.
    INVALID_TABLE_REPLICATION_INFO = 35,
    REACHED_SPLIT_LIMIT = 36,
    SPLIT_OR_BACKFILL_IN_PROGRESS = 37,
    // Error in case a tablet-level operation was attempted on a tablet which is not running.
    TABLET_NOT_RUNNING = 38,
    TABLE_NOT_RUNNING = 39,
}
// src/yb/common/wire_protocol.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct AppStatusPB {
    pub code: ErrorCode,
    pub message: Option<String>,
    pub error_codes: Option<ErrorCodes>,
    pub source_file: Option<String>,
    pub source_line: Option<i32>,
    pub errors: Option<String>,
}
// src/yb/common/wire_protocol.proto, in AppStatusPB
#[derive(Serialize, Deserialize, Debug)]
pub enum ErrorCode {
    UNKNOWN_ERROR = 999,
    OK = 0,
    NOT_FOUND = 1,
    CORRUPTION = 2,
    NOT_SUPPORTED = 3,
    INVALID_ARGUMENT = 4,
    IO_ERROR = 5,
    ALREADY_PRESENT = 6,
    RUNTIME_ERROR = 7,
    NETWORK_ERROR = 8,
    ILLEGAL_STATE = 9,
    NOT_AUTHORIZED = 10,
    ABORTED = 11,
    REMOTE_ERROR = 12,
    SERVICE_UNAVAILABLE = 13,
    TIMED_OUT = 14,
    UNINITIALIZED = 15,
    CONFIGURATION_ERROR = 16,
    INCOMPLETE = 17,
    END_OF_FILE = 18,
    INVALID_COMMAND = 19,
    QL_ERROR = 20,
    INTERNAL_ERROR = 21,
    EXPIRED = 22,
    LEADER_NOT_READY_TO_SERVE = 23,
    LEADER_HAS_NO_LEASE = 24,
    TRY_AGAIN_CODE = 25,
    BUSY = 26,
    SHUTDOWN_IN_PROGRESS = 27,
    MERGE_IN_PROGRESS = 28,
    COMBINED_ERROR = 29,
    SNAPSHOT_TOO_OLD = 30,
    DEPRECATED_HOST_UNREACHABLE = 31,
    CACHE_MISS_ERROR = 32,
    TABLET_SPLIT = 33,
}
// src/yb/common/wire_protocol.proto, in AppStatusPB
#[derive(Serialize, Deserialize, Debug)]
pub enum ErrorCodes {
   posix_code(i32),
   ql_error_code(i64),
}
// src/yb/common/common_types.proto
#[derive(Serialize, Deserialize, Debug)]
pub enum PeerRole {
  // Indicates this node is a follower in the configuration, i.e. that it participates
  // in majorities and accepts Consensus::Update() calls.
  FOLLOWER = 0,
  // Indicates this node is the current leader of the configuration, i.e. that it
  // participates in majorities and accepts Consensus::Append() calls.
  LEADER = 1,
  // New peers joining a quorum will be in this role for both PRE_VOTER and PRE_OBSERVER
  // while the tablet data is being remote bootstrapped. The peer does not participate
  // in starting elections or majorities.
  LEARNER = 2,
  // Indicates that this node is not a participant of the configuration, i.e. does
  // not accept Consensus::Update() or Consensus::Update() and cannot
  // participate in elections or majorities. This is usually the role of a node
  // that leaves the configuration.
  NON_PARTICIPANT = 3,
  // This peer is a read (async) replica and gets informed of the quorum write
  // activity and provides time-line consistent reads.
  READ_REPLICA = 4,
  UNKNOWN_ROLE = 7,
}
// src/yb/master/master_cluster.proto
// hostname_port + timestamp: added.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct GetMasterRegistrationRequestPB {
    pub hostname_port: Option<String>,
    pub timestamp: Option<DateTime<Local>>,
    pub instance_id: NodeInstancePB,
    pub registration: Option<ServerRegistrationPB>,
    pub role: Option<PeerRole>,
    //pub error: Option<MasterErrorPB>,
    pub error: Option<AppStatusPB>,
}

/*
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AllGetMasterRegistrationRequestPB {
    pub getmasterregistrationrequestpb: Vec<GetMasterRegistrationRequestPB>,
}

 */

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Masters {
    pub masters: Vec<GetMasterRegistrationRequestPB>,
}

/*
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

 */
