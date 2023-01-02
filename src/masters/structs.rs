//! The structs
#![allow(non_camel_case_types)]
#![allow(clippy::upper_case_acronyms)]

use std::collections::BTreeMap;
use chrono::{DateTime, Local};

/// The root struct for deserializing `/api/v1/masters`.
///
/// This struct is the begin struct needed to parse the results from master:port/api/v1/masters:
/// ```text
/// {
///   "masters": [
///     {
///       "instance_id": {
///         "permanent_uuid": "ca7914fb53bf4d8e992ba8af6daf886c",
/// ..etc..
/// ```
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Masters {
    pub masters: Vec<GetMasterRegistrationRequestPB>,
}
/// The main struct holding the master information.
///
/// source: `src/yb/master/master_cluster.proto`
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct GetMasterRegistrationRequestPB {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the timestamp.
    pub timestamp: Option<DateTime<Local>>,
    pub instance_id: NodeInstancePB,
    pub registration: Option<ServerRegistrationPB>,
    pub role: Option<PeerRole>,
    pub error: Option<AppStatusPB>,
}
/// source: `src/yb/common/wire_protocol.proto`
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct NodeInstancePB {
    pub permanent_uuid: String,
    pub instance_seqno: i64,
    pub start_time_us: Option<u64>,
}
/// source: `src/yb/common/wire_protocol.proto`
#[derive(Serialize, Deserialize, Debug)]
pub struct ServerRegistrationPB {
    pub private_rpc_addresses: Option<Vec<HostPortPB>>,
    pub http_addresses: Option<Vec<HostPortPB>>,
    pub cloud_info: Option<CloudInfoPB>,
    pub placement_uuid: Option<String>,
    pub broadcast_addresses: Option<Vec<HostPortPB>>,
    pub pg_port: Option<u64>,
}
/// source: `src/yb/common/common_net.proto`
#[derive(Serialize, Deserialize, Debug)]
pub struct HostPortPB {
    pub host: String,
    pub port: u32,
}
/// source: `src/yb/common/common_net.proto`
#[derive(Serialize, Deserialize, Debug)]
pub struct CloudInfoPB {
   pub placement_cloud: Option<String>,
   pub placement_region: Option<String>,
   pub placement_zone: Option<String>,
}
/// source: `src/yb/common/wire_protocol.proto`
#[derive(Serialize, Deserialize, Debug)]
pub struct AppStatusPB {
    pub code: ErrorCode,
    pub message: Option<String>,
    pub error_codes: Option<ErrorCodes>,
    pub source_file: Option<String>,
    pub source_line: Option<i32>,
    pub errors: Option<String>,
}
/// source: `src/yb/common/wire_protocol.proto`
///
/// Defined in `AppStatusPB`
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
/// source: `src/yb/common/wire_protocol.proto`
///
/// Defined in `AppStatusPB`
#[derive(Serialize, Deserialize, Debug)]
pub enum ErrorCodes {
   posix_code(i32),
   ql_error_code(i64),
}
/// source: `src/yb/common/common_types.proto`
///
/// Default value set to `UNKNOWN_ROLE`.
/// The derive macro `PartialEq` is set to allow checking for equality.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
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
  #[default]
  UNKNOWN_ROLE = 7,
}

// diff
/// BTreeMap for storing a master diff struct per `permanent_uuid`.
type BTreeMastersDiff = BTreeMap<String, MastersDiffFields>;
/// The wrapper struct for holding the btreemap holding the diff structs.
#[derive(Debug, Default)]
pub struct MastersDiff {
    pub btreemastersdiff: BTreeMastersDiff,
    pub master_found: bool,
}
/// The masters diff struct.
///
/// This performs a very simple way of diffing:
/// For every field that makes sense to see the difference, create a first and second (snapshot) field.
#[derive(Debug, Default)]
pub struct MastersDiffFields {
    pub first_instance_seqno: i64,
    pub first_start_time_us: u64,
    pub first_placement_cloud: String,
    pub first_placement_region: String,
    pub first_placement_zone: String,
    pub first_placement_uuid: String,
    pub first_role: PeerRole,
    pub first_private_rpc_addresses: String,
    pub first_http_addresses: String,
    pub second_instance_seqno: i64,
    pub second_start_time_us: u64,
    pub second_placement_cloud: String,
    pub second_placement_region: String,
    pub second_placement_zone: String,
    pub second_placement_uuid: String,
    pub second_role: PeerRole,
    pub second_private_rpc_addresses: String,
    pub second_http_addresses: String,
}