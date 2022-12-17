use chrono::{DateTime, Local};

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum AllConnections {
    /*
     * Connections is unique to YSQL
     */
    Connections {
        connections: Vec<YsqlConnection>,
    },
    /*
     * InAndOutboundConnections are used by: YCQL, YEDIS, tablet server and master.
     * YCQL and YEDIS can have zero connections, which then use Empty, see below.
     * YCQL and YEDIS also only have inbound_connections, which is why outbound_connections is set to Optional.
     * Please mind that YEDIS is not verified at this moment!
     */
    InAndOutboundConnections {
        inbound_connections: Option<Vec<InboundConnection>>,
        outbound_connections: Option<Vec<OutboundConnection>>,
    },
    /*
     * An empty object ({}) is shown for YCQL and YEDIS if these have no connections.
     * YSQL always has at least the checkpointer as an active connection.
     */
    Empty {},
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

#[derive(Serialize, Deserialize, Debug)]
pub struct InboundConnection {
    pub remote_ip: String,
    pub state: String,
    pub processed_call_count: Option<u32>,
    pub connection_details: Option<ConnectionDetails>,
    pub calls_in_flight: Option<Vec<CallsInFlight>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConnectionDetails {
    pub cql_connection_details: CqlConnectionDetails,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CqlConnectionDetails {
    pub keyspace: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CallsInFlight {
    pub elapsed_millis: u32,
    pub cql_details: Option<CqlDetails>,
    pub header: Option<Header>,
    pub state: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CqlDetails {
    #[serde(rename = "type")]
    pub call_type: String,
    pub call_details: Vec<CallDetails>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CallDetails {
    pub sql_id: Option<String>,
    pub sql_string: String,
    pub params: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OutboundConnection {
    pub remote_ip: String,
    pub state: String,
    pub processed_call_count: Option<u32>,
    pub sending_bytes: Option<u32>,
    pub calls_in_flight: Option<Vec<CallsInFlight>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Header {
    pub call_id: u32,
    pub remote_method: RemoteMethod,
    pub timeout_millis: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RemoteMethod {
    pub service_name: String,
    pub method_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredYsqlConnection {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub process_start_time: String,
    pub application_name: String,
    pub backend_type: String,
    pub backend_status: String,
    pub db_oid: u32,
    pub db_name: String,
    pub host: String,
    pub port: String,
    pub query: String,
    pub query_start_time: String,
    pub transaction_start_time: String,
    pub process_running_for_ms: u32,
    pub transaction_running_for_ms: u32,
    pub query_running_for_ms: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredInboundRpc {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub remote_ip: String,
    pub state: String,
    pub processed_call_count: u32,
    pub serial_nr: u32
}


#[derive(Serialize, Deserialize, Debug)]
pub struct StoredOutboundRpc {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub remote_ip: String,
    pub state: String,
    pub processed_call_count: u32,
    pub sending_bytes: u32,
    pub serial_nr: u32
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredCqlDetails {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub remote_ip: String,
    pub keyspace: String,
    pub elapsed_millis: u32,
    pub cql_details_type: String,
    pub sql_id: String,
    pub sql_string: String,
    pub params: String,
    pub serial_nr: u32
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredHeaders {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub remote_ip: String,
    pub call_id: u32,
    pub remote_method_service_name: String,
    pub remote_method_method_name: String,
    pub timeout_millis: u32,
    pub elapsed_millis: u32,
    pub state: String,
    pub serial_nr: u32
}

#[derive(Default)]
pub struct AllStoredConnections {
    pub stored_ysqlconnections: Vec<StoredYsqlConnection>,
    pub stored_inboundrpcs: Vec<StoredInboundRpc>,
    pub stored_outboundrpcs: Vec<StoredOutboundRpc>,
    pub stored_cqldetails: Vec<StoredCqlDetails>,
    pub stored_headers: Vec<StoredHeaders>,
}

