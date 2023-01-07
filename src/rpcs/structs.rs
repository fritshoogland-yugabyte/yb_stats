//! The structs
//!
#![allow(non_camel_case_types)]
use chrono::{DateTime, Local};
/// The root struct for deserializing `/rpcz`.
///
/// This struct deserializes a number of actually different structs:
/// 1. YSQL:
/// ```json
/// {
///     "connections": [
///         {
///             "process_start_time": "2023-01-04 09:30:03.017119+00",
///             "application_name": "",
///             "backend_type": "checkpointer",
///             "backend_status": ""
///         }
///     ]
/// }
/// ```
/// The `connections` array is the externalisation of the `pg_stat_activity` view in YSQL.
///
/// 2. tablet server, master, YCQL and YEDIS inbound connections:
/// ```json
/// {
///     "inbound_connections": [
///         {
///             "remote_ip": "192.168.66.81:36048",
///             "state": "OPEN",
///             "processed_call_count": 36
///         },
///        ...
/// ```
///
/// 3. tablet server, master outbound connections:
/// ```json
/// "outbound_connections": [
///         {
///             "remote_ip": "192.168.66.82:9100",
///             "state": "OPEN",
///             "processed_call_count": 261,
///             "sending_bytes": 0
///         },
/// ```
///
/// 4. YCQL and YEDIS not serving any connections:
/// ```text
/// {}
/// ```
///    If these serve connections, these become inbound_connections.
///
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum Rpcs {
    Ysql {
        /// yb_stats added to allow understanding the source host.
        hostname_port: Option<String>,
        /// yb_stats added to allow understanding the timestamp.
        timestamp: Option<DateTime<Local>>,
        connections: Vec<YsqlConnection>,
    },
    Rpc {
        /// yb_stats added to allow understanding the source host.
        hostname_port: Option<String>,
        /// yb_stats added to allow understanding the timestamp.
        timestamp: Option<DateTime<Local>>,
        inbound_connections: Option<Vec<InboundConnection>>,
        outbound_connections: Option<Vec<OutboundConnection>>,
    },
    Empty {},
}
/// This struct is used by yb_stats for saving and loading the Rpcs data.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AllRpcs {
    pub rpcs: Vec<Rpcs>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct YsqlConnection {
    pub process_start_time: String,
    pub application_name: String,
    pub backend_type: String,
    pub backend_status: String,
    pub db_oid: Option<u32>,
    pub db_name: Option<String>,
    pub host: Option<String>,
    pub port: Option<String>,
    pub query: Option<String>,
    pub query_start_time: Option<String>,
    pub transaction_start_time: Option<String>,
    pub process_running_for_ms: Option<u32>,
    pub transaction_running_for_ms: Option<u32>,
    pub query_running_for_ms: Option<u32>,
}
/// `src/yb/rpc/rpc_introspection.proto`
///
/// RpcConnectionPB
#[derive(Serialize, Deserialize, Debug)]
pub struct InboundConnection {
    pub remote_ip: String,
    pub state: StateType,
    pub processed_call_count: Option<u64>,
    pub connection_details: Option<RpcConnectionDetailsPB>,
    pub calls_in_flight: Option<Vec<RpcCallInProgressPB>>,
}
/// `src/yb/rpc/rpc_introspection.proto`
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RpcConnectionDetailsPB {
    pub cql_connection_details: Option<CqlConnectionDetails>,
}
/// `src/yb/rpc/rpc_introspection.proto`
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct CqlConnectionDetails {
    pub keyspace: Option<String>,
}
/// `src/yb/rpc/rpc_introspection.proto`
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RpcCallInProgressPB {
    pub header: Option<RequestHeader>,
    pub trace_buffer: Option<String>,
    pub elapsed_millis: Option<u64>,
    pub cql_details: Option<CQLCallDetailsPB>,
    pub redis_details: Option<RedisCallDetailsPB>,
    pub state: Option<RpcCallState>,
}
/// `src/yb/rpc/rpc_header.proto`
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RequestHeader {
    pub call_id: Option<u32>,
    pub remote_method: Option<RemoteMethodPB>,
    pub timeout_millis: Option<u32>,
}
/// `src/yb/rpc/rpc_header.proto`
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RemoteMethodPB {
    pub service_name: String,
    pub method_name: String,
}
/// `src/yb/rpc/rpc_introspection.proto`
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub enum RpcCallState {
    #[default]
    READY = 0,
    ON_OUTBOUND_QUEUE = 1,
    SENT = 2,
    TIMED_OUT = 3,
    FINISHED_ERROR = 4,
    FINISHED_SUCCESS = 5,
}
/// `src/yb/rpc/rpc_introspection.proto`
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct CQLCallDetailsPB {
    #[serde(rename = "type")]
    pub call_type: Option<String>,
    pub call_details: Vec<CQLStatementsDetailsPB>,
}
/// `src/yb/rpc/rpc_introspection.proto`
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RedisCallDetailsPB {
    pub call_details: Vec<RedisStatementsDetailsPB>,
}
/// `src/yb/rpc/rpc_introspection.proto`
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CQLStatementsDetailsPB {
    pub sql_id: Option<String>,
    pub sql_string: Option<String>,
    pub params: Option<String>,
}
/// `src/yb/rpc/rpc_introspection.proto`
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RedisStatementsDetailsPB {
    pub redis_string: Option<String>,
}
/// `src/yb/rpc/rpc_introspection.proto`
///
/// RpcConnectionPB
#[derive(Serialize, Deserialize, Debug)]
pub struct OutboundConnection {
    pub remote_ip: String,
    pub state: StateType,
    pub processed_call_count: Option<u64>,
    pub sending_bytes: Option<u64>,
    pub calls_in_flight: Option<Vec<RpcCallInProgressPB>>,
}
/// `src/yb/rpc/rpc_introspection.proto`
///
/// Inside `RpcConnectionPB` definition.
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum StateType {
    UNKNOWN = 999,
    NEGOTIATING = 0,  // Connection is still being negotiated.
    OPEN = 1,         // Connection is active.
}
