use serde_derive::{Serialize,Deserialize};
use port_scanner::scan_port_addr;
use chrono::{DateTime, Local};
use std::{fs, process};
use log::*;
use std::sync::mpsc::channel;
use std::path::PathBuf;
use regex::Regex;
use std::collections::BTreeMap;
use crate::rpcs::AllConnections::{Connections, InAndOutboundConnections};

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum AllConnections {
    /*
     * Connections is unique to YSQL
     */
    Connections {
        connections: Vec<Connection>,
    },
    /*
     * InAndOutboundConnections are used by: YCQL, YEDIS, tablet server and master.
     * YCQL and YEDIS can have zero connections, which then use Empty, see below.
     * YCQL and YEDIS also only have inbound_connections, which is why outbound_connections is set to Optional.
     * Please mind that YEDIS is not verified at this moment!
     */
    InAndOutboundConnections {
            inbound_connections: Vec<InboundConnection>,
            outbound_connections: Option<Vec<OutboundConnection>>,
    },
    /*
     * An empty object ({}) is shown for YCQL and YEDIS if these have no connections.
     * YSQL always has at least the checkpointer as an active connection.
     */
    Empty {},
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Connection {
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
pub struct StoredYsqlRpc {
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

impl StoredYsqlRpc {
    fn new(hostname_port: &str,
           timestamp: DateTime<Local>,
           connection: Connection,
    ) -> Self {
        Self {
            hostname_port: hostname_port.to_string(),
            timestamp,
            process_start_time: connection.process_start_time.to_string(),
            application_name: connection.application_name.to_string(),
            backend_type: connection.backend_type.to_string(),
            backend_status: connection.backend_status.to_string(),
            db_oid: connection.db_oid.unwrap_or_default(),
            db_name: connection.db_name.unwrap_or_default(),
            host: connection.host.unwrap_or_default(),
            port: connection.port.unwrap_or_default(),
            query: connection.query.unwrap_or_default(),
            query_start_time: connection.query_start_time.unwrap_or_default(),
            transaction_start_time: connection.transaction_start_time.unwrap_or_default(),
            process_running_for_ms: connection.process_running_for_ms.unwrap_or_default(),
            transaction_running_for_ms: connection.transaction_running_for_ms.unwrap_or_default(),
            query_running_for_ms: connection.query_running_for_ms.unwrap_or_default(),
        }
    }
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

impl StoredInboundRpc {
    fn new(hostname_port: &str, timestamp: DateTime<Local>, serial_number: u32, inboundrpc: &InboundConnection) -> Self {
        Self {
            hostname_port: hostname_port.to_string(),
            timestamp,
            remote_ip: inboundrpc.remote_ip.to_string(),
            state: inboundrpc.state.to_string(),
            processed_call_count: inboundrpc.processed_call_count.unwrap_or_default(),
            serial_nr: serial_number,
        }
    }
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

type AllStoredRpcs = (Vec<StoredYsqlRpc>, Vec<StoredInboundRpc>, Vec<StoredOutboundRpc>, Vec<StoredCqlDetails>, Vec<StoredHeaders>);

fn read_rpcs_into_vectors(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    parallel: usize,
) -> AllStoredRpcs {
    let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
    let (tx, rx) = channel();
    pool.scope(move |s| {
        for host in hosts {
            for port in ports {
                let tx = tx.clone();
                s.spawn(move |_| {
                    let detail_snapshot_time = Local::now();
                    let rpcs = read_rpcs(host, port);
                    debug!("allconnections: host: {}:{}, {:?}", host, port, &rpcs);
                    tx.send((format!("{}:{}", host, port), detail_snapshot_time, rpcs)).expect("error sending data via tx (rpcs)");
                });
            }
        }
    });
    let mut stored_ysqlrpc: Vec<StoredYsqlRpc> = Vec::new();
    let mut stored_inboundrpc: Vec<StoredInboundRpc> = Vec::new();
    let mut stored_outboundrpc: Vec<StoredOutboundRpc> = Vec::new();
    let mut stored_cqldetails: Vec<StoredCqlDetails> = Vec::new();
    let mut stored_header: Vec<StoredHeaders> = Vec::new();
    for (hostname_port, detail_snapshot_time, rpcs) in rx {
        trace!("read_rpcs_into_vectors host: {}, {:?}", &hostname_port, rpcs);
        add_to_rpcs_vectors(rpcs, &hostname_port, detail_snapshot_time, &mut stored_ysqlrpc, &mut stored_inboundrpc, &mut stored_outboundrpc, &mut stored_cqldetails, &mut stored_header);
    }
    (stored_ysqlrpc, stored_inboundrpc, stored_outboundrpc, stored_cqldetails, stored_header)
}

pub fn read_rpcs(
    host: &str,
    port: &str,
) -> AllConnections
{
    if scan_port_addr(format!("{}:{}", host, port)) {
        let http_data = reqwest::blocking::get(format!("http://{}:{}/rpcz", host, port))
            .unwrap()
            .text()
            .unwrap();
        debug!("host: {}:{} http_data: {}", host, port, &http_data);
        parse_rpcs(http_data, host, port)
    } else {
        warn!("hostname: port {}:{} cannot be reached, skipping", host, port);
        parse_rpcs( String::from(""), "", "")
    }
}

// this function is split from the read_rpcs function so we can use it with the tests.
fn parse_rpcs(
    rpcs_data: String,
    host: &str,
    port: &str,
) -> AllConnections {
    serde_json::from_str(&rpcs_data).unwrap_or_else(|e| {
        info!("Could not parse {}:{}/rpcz json data for rpcs, error: {}", host, port, e);
        AllConnections::Empty {}
    })
}

#[allow(clippy::too_many_arguments)]
pub fn add_to_rpcs_vectors(
    allconnections: AllConnections,
    hostname: &str,
    detail_snapshot_time: DateTime<Local>,
    stored_ysqlrpc: &mut Vec<StoredYsqlRpc>,
    stored_inboundrpc: &mut Vec<StoredInboundRpc>,
    stored_outboundrpc: &mut Vec<StoredOutboundRpc>,
    stored_cqldetails: &mut Vec<StoredCqlDetails>,
    stored_headers: &mut Vec<StoredHeaders>,
)
{
    match allconnections {
        Connections { connections } => {
            for ysqlrpc in connections {
                trace!("add_to_rpcs_vectors ysqlrpc {:?}", &ysqlrpc);
                stored_ysqlrpc.push( StoredYsqlRpc::new( hostname, detail_snapshot_time, ysqlrpc) );
            }
        },
        InAndOutboundConnections { inbound_connections, outbound_connections } => {
            for (serial_number, inboundrpc) in (0_u32..).zip(inbound_connections.into_iter()) {
                trace!("add_to_rpcs_vectors inboundrpc, hostname: {}, {:?}", hostname, inboundrpc);
                stored_inboundrpc.push(StoredInboundRpc::new(hostname, detail_snapshot_time, serial_number, &inboundrpc));
                let keyspace = match inboundrpc.connection_details.as_ref() {
                    Some( connection_details )  => {
                        connection_details.cql_connection_details.keyspace.clone()
                    },
                    None => String::from("")
                };
                for calls_in_flight in inboundrpc.calls_in_flight.unwrap_or_default() {
                    if let Some ( cql_details ) = calls_in_flight.cql_details {
                        for call_detail in cql_details.call_details {
                            stored_cqldetails.push(StoredCqlDetails {
                                hostname_port: hostname.to_string(),
                                timestamp: detail_snapshot_time,
                                remote_ip: inboundrpc.remote_ip.to_string(),
                                keyspace: keyspace.to_owned(),
                                elapsed_millis: calls_in_flight.elapsed_millis,
                                cql_details_type: cql_details.call_type.clone(),
                                sql_id: call_detail.sql_id.unwrap_or_default(),
                                sql_string: call_detail.sql_string,
                                params: call_detail.params.unwrap_or_default(),
                                serial_nr: serial_number,
                            });
                        }
                    };
                    /*
                    match calls_in_flight.cql_details {
                        Some ( cql_details ) => {
                            for call_detail in cql_details.call_details {
                                stored_cqldetails.push(StoredCqlDetails {
                                    hostname_port: hostname.to_string(),
                                    timestamp: detail_snapshot_time,
                                    remote_ip: inboundrpc.remote_ip.to_string(),
                                    keyspace: keyspace.to_owned(),
                                    elapsed_millis: calls_in_flight.elapsed_millis,
                                    cql_details_type: cql_details.call_type.clone(),
                                    sql_id: call_detail.sql_id.unwrap_or_default(),
                                    sql_string: call_detail.sql_string,
                                    params: call_detail.params.unwrap_or_default(),
                                    serial_nr: serial_number,
                                });
                            }
                        },
                        None => {},
                    }
                     */
                    if let Some ( header) = calls_in_flight.header {
                        stored_headers.push( StoredHeaders {
                            hostname_port: hostname.to_string(),
                            timestamp: detail_snapshot_time,
                            remote_ip: inboundrpc.remote_ip.to_string(),
                            call_id: header.call_id,
                            remote_method_service_name: header.remote_method.service_name.to_string(),
                            remote_method_method_name: header.remote_method.method_name.to_string(),
                            timeout_millis: header.timeout_millis,
                            elapsed_millis: calls_in_flight.elapsed_millis,
                            state: calls_in_flight.state.unwrap_or_default(),
                            serial_nr: serial_number,
                        });
                    };
                    /*
                    match calls_in_flight.header {
                        Some ( header) => {
                            stored_headers.push( StoredHeaders {
                                hostname_port: hostname.to_string(),
                                timestamp: detail_snapshot_time,
                                remote_ip: inboundrpc.remote_ip.to_string(),
                                call_id: header.call_id,
                                remote_method_service_name: header.remote_method.service_name.to_string(),
                                remote_method_method_name: header.remote_method.method_name.to_string(),
                                timeout_millis: header.timeout_millis,
                                elapsed_millis: calls_in_flight.elapsed_millis,
                                state: calls_in_flight.state.unwrap_or_default(),
                                serial_nr: serial_number,
                            });
                        },
                        None => {},
                    }
                     */
                }
            };

            for (serial_number, outboundrpc) in (0_u32..).zip(outbound_connections.unwrap_or_default().into_iter()) {
                trace!("add_to_rpcs_vectors outboundrpc, hostname: {}, {:?}", hostname, outboundrpc);
                stored_outboundrpc.push(StoredOutboundRpc {
                    hostname_port: hostname.to_string(),
                    timestamp: detail_snapshot_time,
                    remote_ip: outboundrpc.remote_ip.to_string(),
                    state: outboundrpc.state.to_string(),
                    processed_call_count: outboundrpc.processed_call_count.unwrap_or_default(),
                    sending_bytes: outboundrpc.sending_bytes.unwrap_or_default(),
                    serial_nr: serial_number,
                });
                for calls_in_flight in outboundrpc.calls_in_flight.unwrap_or_default() {
                    if let Some ( header) = calls_in_flight.header {
                        stored_headers.push( StoredHeaders {
                            hostname_port: hostname.to_string(),
                            timestamp: detail_snapshot_time,
                            remote_ip: outboundrpc.remote_ip.to_string(),
                            call_id: header.call_id,
                            remote_method_service_name: header.remote_method.service_name.to_string(),
                            remote_method_method_name: header.remote_method.method_name.to_string(),
                            timeout_millis: header.timeout_millis,
                            elapsed_millis: calls_in_flight.elapsed_millis,
                            state: calls_in_flight.state.unwrap_or_default(),
                            serial_nr: serial_number,
                        });
                    };
                    /*
                    match calls_in_flight.header {
                        Some ( header) => {
                            stored_headers.push( StoredHeaders {
                                hostname_port: hostname.to_string(),
                                timestamp: detail_snapshot_time,
                                remote_ip: outboundrpc.remote_ip.to_string(),
                                call_id: header.call_id,
                                remote_method_service_name: header.remote_method.service_name.to_string(),
                                remote_method_method_name: header.remote_method.method_name.to_string(),
                                timeout_millis: header.timeout_millis,
                                elapsed_millis: calls_in_flight.elapsed_millis,
                                state: calls_in_flight.state.unwrap_or_default(),
                                serial_nr: serial_number,
                            });
                        },
                        None => {},
                    }

                     */
                }
            }
        },
        _ => {
            info!("No match: hostname: {}; {:?}", hostname, allconnections);
        },
    }
}

#[allow(dead_code)]
#[allow(clippy::ptr_arg)]
pub fn perform_rpcs_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    snapshot_number: i32,
    yb_stats_directory: &PathBuf,
    parallel: usize,
) {
    info!("perform_rpcs_snapshot");

    let (stored_ysqlrpc, stored_inboundrpc, stored_outboundrpc, stored_cqldetails, stored_headers) = read_rpcs_into_vectors(hosts, ports, parallel);

    let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
    let ysqlrpc_file = &current_snapshot_directory.join("ysqlrpc");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&ysqlrpc_file)
        .unwrap_or_else(|e| {
            error!("Fatal: error writing ysqlrpc data in snapshot directory {}: {}", &ysqlrpc_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_ysqlrpc {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
    let inboundrpc_file = &current_snapshot_directory.join("inboundrpc");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&inboundrpc_file)
        .unwrap_or_else(|e| {
            error!("Fatal: error writing inboundrpc data in snapshot directory {}: {}", &inboundrpc_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_inboundrpc {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
    let outboundrpc_file = &current_snapshot_directory.join("outboundrpc");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&outboundrpc_file)
        .unwrap_or_else(|e| {
            error!("Fatal: error writing outboundrpc data in snapshot directory {}: {}", &outboundrpc_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_outboundrpc {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
    let cqldetails_file = &current_snapshot_directory.join("cqldetails");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&cqldetails_file)
        .unwrap_or_else(|e| {
            error!("Fatal: error writing cqldetails data in snapshot directory {}: {}", &cqldetails_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_cqldetails {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();

    let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
    let headers_file = &current_snapshot_directory.join("headers");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&headers_file)
        .unwrap_or_else(|e| {
            error!("Fatal: error writing headers data in snapshot directory {}: {}", &headers_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_headers {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();
}

#[allow(clippy::ptr_arg)]
pub fn read_ysqlrpc_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
) -> Vec<StoredYsqlRpc>
{
    let mut stored_ysqlrpc: Vec<StoredYsqlRpc> = Vec::new();
    let ysqlrpc_file = &yb_stats_directory.join(snapshot_number).join("ysqlrpc");
    let file = fs::File::open( &ysqlrpc_file )
        .unwrap_or_else(|e| {
            error!("Fatal: error reading file: {}: {}", &ysqlrpc_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredYsqlRpc = row.unwrap();
        let _ = &stored_ysqlrpc.push(data);
    }
    stored_ysqlrpc
}

#[allow(clippy::ptr_arg)]
pub fn read_inboundrpc_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
) -> Vec<StoredInboundRpc>
{
    let mut stored_inboundrpc: Vec<StoredInboundRpc> = Vec::new();
    let inboundrpc_file = &yb_stats_directory.join(snapshot_number).join("inboundrpc");
    let file = fs::File::open( &inboundrpc_file )
        .unwrap_or_else(|e| {
            error!("Fatal: error reading file: {}: {}", &inboundrpc_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredInboundRpc = row.unwrap();
        let _ = &stored_inboundrpc.push(data);
    }
    stored_inboundrpc
}

#[allow(clippy::ptr_arg)]
pub fn read_outboundrpc_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
) -> Vec<StoredOutboundRpc>
{
    let mut stored_outboundrpc: Vec<StoredOutboundRpc> = Vec::new();
    let outboundrpc_file = &yb_stats_directory.join(snapshot_number).join("outboundrpc");
    let file = fs::File::open( &outboundrpc_file )
        .unwrap_or_else(|e| {
            error!("Fatal: error reading file: {}: {}", &outboundrpc_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredOutboundRpc = row.unwrap();
        let _ = &stored_outboundrpc.push(data);
    }
    stored_outboundrpc
}

#[allow(clippy::ptr_arg)]
pub fn read_cqldetails_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
) -> Vec<StoredCqlDetails>
{
    let mut stored_cqldetails: Vec<StoredCqlDetails> = Vec::new();
    let cqldetails_file = &yb_stats_directory.join(snapshot_number).join("cqldetails");
    let file = fs::File::open( &cqldetails_file )
        .unwrap_or_else(|e| {
            error!("Fatal: error reading file: {}: {}", &cqldetails_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredCqlDetails = row.unwrap();
        let _ = &stored_cqldetails.push(data);
    }
    stored_cqldetails
}

#[allow(clippy::ptr_arg)]
pub fn read_headers_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
) -> Vec<StoredHeaders>
{
    let mut stored_headers: Vec<StoredHeaders> = Vec::new();
    let headers_file = &yb_stats_directory.join(snapshot_number).join("headers");
    let file = fs::File::open( &headers_file )
        .unwrap_or_else(|e| {
            error!("Fatal: error reading file: {}: {}", &headers_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredHeaders = row.unwrap();
        let _ = &stored_headers.push(data);
    }
    stored_headers
}

pub fn print_rpcs(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
    hostname_filter: &Regex,
    details_enable: &bool,
) {
    info!("print_rpcs");
    let stored_ysqlrpc: Vec<StoredYsqlRpc> = read_ysqlrpc_snapshot(snapshot_number, yb_stats_directory);
    let stored_inboundrpcs: Vec<StoredInboundRpc> = read_inboundrpc_snapshot(snapshot_number, yb_stats_directory);
    let stored_outboundrpcs: Vec<StoredOutboundRpc> = read_outboundrpc_snapshot(snapshot_number, yb_stats_directory);
    let stored_cqldetails: Vec<StoredCqlDetails> = read_cqldetails_snapshot(snapshot_number, yb_stats_directory);
    let stored_headers: Vec<StoredHeaders> = read_headers_snapshot(snapshot_number, yb_stats_directory);
    let mut endpoint_count: BTreeMap<String, usize> = BTreeMap::new();
    for row in &stored_ysqlrpc {
        *endpoint_count.entry(row.hostname_port.clone()).or_default() += 1;
    }
    for row in &stored_inboundrpcs {
        *endpoint_count.entry(row.hostname_port.clone()).or_default() += 1;
    }
    for row in &stored_outboundrpcs {
        *endpoint_count.entry(row.hostname_port.clone()).or_default() += 1;
    }
    debug!("{:#?}", endpoint_count);
    let mut previous_hostname = String::from("");
    let mut endpoints_collector: Vec<String> = Vec::new();
    for (endpoint, count) in endpoint_count {
        if hostname_filter.is_match(&endpoint) {
            let current_hostname = endpoint.split(':').next().unwrap();
            let current_port = endpoint.split(':').nth(1).unwrap();
            if current_hostname != previous_hostname {
                println!("\n{}", "-".repeat(100));
                print_details(details_enable, &mut endpoints_collector, &stored_ysqlrpc, &stored_inboundrpcs, &stored_outboundrpcs, &stored_cqldetails, &stored_headers);
                print!("Host: {}", current_hostname);
                previous_hostname = current_hostname.to_string();
            }
            endpoints_collector.push(endpoint.clone());
            print!("; port: {}, count: {}", current_port, count);
        };
    }
    println!("\n{}", "-".repeat(100));
    print_details(details_enable, &mut endpoints_collector, &stored_ysqlrpc, &stored_inboundrpcs, &stored_outboundrpcs, &stored_cqldetails, &stored_headers);
}

fn print_details(
    details_enable: &bool,
    endpoints_collector: &mut Vec<String>,
    stored_ysqlrpc: &[StoredYsqlRpc],
    stored_inboundrpcs: &[StoredInboundRpc],
    stored_outboundrpcs: &[StoredOutboundRpc],
    stored_cqldetails: &[StoredCqlDetails],
    stored_headers: &[StoredHeaders],
) {
    if *details_enable {
        for hostname_port in endpoints_collector.drain(..) {
            for ysql in stored_ysqlrpc.iter().filter(|x| x.hostname_port == hostname_port) {
                println!("{} {} {} {} {} {} {}", ysql.hostname_port, ysql.backend_status, ysql.db_name, ysql.backend_type, ysql.application_name, ysql.host, ysql.query);
            }
            for inbound in stored_inboundrpcs.iter().filter(|x| x.hostname_port == hostname_port) {
                println!("{} {} {} {}", inbound.hostname_port, inbound.state, inbound.remote_ip, inbound.processed_call_count);
                for cqldetail in stored_cqldetails.iter().filter(|x| x.hostname_port == hostname_port && x.remote_ip == inbound.remote_ip && x.serial_nr == inbound.serial_nr) {
                    println!(" Key:{} ela_ms:{} type:{} sql_id:{} sql_str:{}", cqldetail.keyspace, cqldetail.elapsed_millis, cqldetail.cql_details_type, cqldetail.sql_id, cqldetail.sql_string);
                }
                for header in stored_headers.iter().filter(|x| x.hostname_port == hostname_port && x.remote_ip == inbound.remote_ip && x.serial_nr == inbound.serial_nr) {
                    println!(" State:{} ela_ms:{} tmout_ms:{} method:{} service:{}", header.state, header.elapsed_millis, header.timeout_millis, header.remote_method_method_name, header.remote_method_service_name);
                }
            }
            for outbound in stored_outboundrpcs.iter().filter(|x| x.hostname_port == hostname_port) {
                println!("{} {} {} {} {}", outbound.hostname_port, outbound.state, outbound.remote_ip, outbound.processed_call_count, outbound.sending_bytes);
                for header in stored_headers.iter().filter(|x| x.hostname_port == hostname_port && x.remote_ip == outbound.remote_ip && x.serial_nr == outbound.serial_nr) {
                    println!(" State:{} ela_ms:{} tmout_ms:{} method:{} service:{}", header.state, header.elapsed_millis, header.timeout_millis, header.remote_method_method_name, header.remote_method_service_name);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ysqlrpc_only_checkpointer() {
        let json = r#"
{
    "connections": [
        {
            "process_start_time": "2022-08-11 10:06:23.639902+00",
            "application_name": "",
            "backend_type": "checkpointer",
            "backend_status": ""
        }
    ]
}
        "#.to_string();
        let result = parse_rpcs(json, "", "");
        let mut stored_ysqlrpc: Vec<StoredYsqlRpc> = Vec::new();
        let mut stored_inboundrpc: Vec<StoredInboundRpc> = Vec::new();
        let mut stored_outboundrpc: Vec<StoredOutboundRpc> = Vec::new();
        let mut stored_cqldetails: Vec<StoredCqlDetails> = Vec::new();
        let mut stored_headers: Vec<StoredHeaders> = Vec::new();
        add_to_rpcs_vectors(result, "", Local::now(), &mut stored_ysqlrpc, &mut stored_inboundrpc, &mut stored_outboundrpc, &mut stored_cqldetails, &mut stored_headers);
        assert_eq!(stored_ysqlrpc[0].process_start_time,"2022-08-11 10:06:23.639902+00");
        assert_eq!(stored_ysqlrpc[0].application_name,"");
        assert_eq!(stored_ysqlrpc[0].backend_type,"checkpointer");
    }

    #[test]
    fn parse_ysqlrpc_active_transaction() {
        let json = r#"
{
    "connections":
    [
        {
            "db_oid": 13285,
            "db_name": "yugabyte",
            "query": "insert into t select x from generate_series(1,100000000) x;",
            "process_start_time": "2022-08-12 14:17:25.144833+00",
            "process_running_for_ms": 195045,
            "transaction_start_time": "2022-08-12 14:20:38.366499+00",
            "transaction_running_for_ms": 1824,
            "query_start_time": "2022-08-12 14:20:38.366499+00",
            "query_running_for_ms": 1824,
            "application_name": "ysqlsh",
            "backend_type": "client backend",
            "backend_status": "active",
            "host": "127.0.0.1",
            "port": "37648"
        }
    ]
}
        "#.to_string();
        let result = parse_rpcs(json, "", "");
        let mut stored_ysqlrpc: Vec<StoredYsqlRpc> = Vec::new();
        let mut stored_inboundrpc: Vec<StoredInboundRpc> = Vec::new();
        let mut stored_outboundrpc: Vec<StoredOutboundRpc> = Vec::new();
        let mut stored_cqldetails: Vec<StoredCqlDetails> = Vec::new();
        let mut stored_headers: Vec<StoredHeaders> = Vec::new();
        add_to_rpcs_vectors(result, "", Local::now(), &mut stored_ysqlrpc, &mut stored_inboundrpc, &mut stored_outboundrpc, &mut stored_cqldetails, &mut stored_headers);
        assert_eq!(stored_ysqlrpc[0].db_oid,13285);
        assert_eq!(stored_ysqlrpc[0].db_name,"yugabyte");
        assert_eq!(stored_ysqlrpc[0].query,"insert into t select x from generate_series(1,100000000) x;");
        assert_eq!(stored_ysqlrpc[0].query_start_time,"2022-08-12 14:20:38.366499+00");
        assert_eq!(stored_ysqlrpc[0].query_running_for_ms,1824);
    }

    #[test]
    fn parse_inboundrpc_idle_ycql() {
        /*
         * This is how a simple, inactive simple connection via ycqlsh looks like.
         */
        let json = r#"
{
    "inbound_connections": [
        {
            "remote_ip": "127.0.0.1:33086",
            "state": "OPEN",
            "processed_call_count": 2
        },
        {
            "remote_ip": "127.0.0.1:33084",
            "state": "OPEN",
            "processed_call_count": 13
        }
    ]
}
        "#.to_string();
        let result = parse_rpcs(json, "", "");
        let mut stored_ysqlrpc: Vec<StoredYsqlRpc> = Vec::new();
        let mut stored_inboundrpc: Vec<StoredInboundRpc> = Vec::new();
        let mut stored_outboundrpc: Vec<StoredOutboundRpc> = Vec::new();
        let mut stored_cqldetails: Vec<StoredCqlDetails> = Vec::new();
        let mut stored_headers: Vec<StoredHeaders> = Vec::new();
        add_to_rpcs_vectors(result, "", Local::now(), &mut stored_ysqlrpc, &mut stored_inboundrpc, &mut stored_outboundrpc, &mut stored_cqldetails, &mut stored_headers);
        assert_eq!(stored_inboundrpc[0].remote_ip,"127.0.0.1:33086");
        assert_eq!(stored_inboundrpc[0].state,"OPEN");
        assert_eq!(stored_inboundrpc[0].processed_call_count,2);
        assert_eq!(stored_inboundrpc[1].remote_ip,"127.0.0.1:33084");
        assert_eq!(stored_inboundrpc[1].state,"OPEN");
        assert_eq!(stored_inboundrpc[1].processed_call_count,13);
    }

    #[test]
    fn parse_inboundrpc_active_query_ycql() {
        /*
         * This is how an active query via ycqlsh looks like.
         */
        let json = r#"
{
    "inbound_connections": [
        {
            "remote_ip": "127.0.0.1:35518",
            "state": "OPEN",
            "processed_call_count": 20,
            "connection_details": {
                "cql_connection_details": {
                    "keyspace": "cr"
                }
            },
            "calls_in_flight": [
                {
                    "elapsed_millis": 252,
                    "cql_details": {
                        "type": "QUERY",
                        "call_details": [
                            {
                                "sql_string": "select avg(permit), avg(permit_recheck), avg( handgun), avg( long_gun), avg( other), avg( multiple), avg( admin), avg( prepawn_handgun), avg( prepawn_long_gun), avg( prepawn_other), avg( redemption_handgun), avg( redemption_long_gun), avg( redemption_other), avg( returned_handgun), avg( returned_long_gun), avg( returned_other), avg( rentals_handgun), avg( rentals_long_gun), avg( private_sale_handgun), avg( private_sale_long_gun), avg( private_sale_other), avg( return_to_seller_handgun), avg( return_to_seller_long_gun), avg( return_to_seller_other), avg( totals) from fa_bg_checks;"
                            }
                        ]
                    }
                }
            ]
        }
    ]
}
        "#.to_string();
        let result = parse_rpcs(json, "", "");
        let mut stored_ysqlrpc: Vec<StoredYsqlRpc> = Vec::new();
        let mut stored_inboundrpc: Vec<StoredInboundRpc> = Vec::new();
        let mut stored_outboundrpc: Vec<StoredOutboundRpc> = Vec::new();
        let mut stored_cqldetails: Vec<StoredCqlDetails> = Vec::new();
        let mut stored_headers: Vec<StoredHeaders> = Vec::new();
        add_to_rpcs_vectors(result, "", Local::now(), &mut stored_ysqlrpc, &mut stored_inboundrpc, &mut stored_outboundrpc, &mut stored_cqldetails, &mut stored_headers);
        assert_eq!(stored_inboundrpc[0].remote_ip,"127.0.0.1:35518");
        assert_eq!(stored_inboundrpc[0].state,"OPEN");
        assert_eq!(stored_inboundrpc[0].processed_call_count,20);
        assert_eq!(stored_cqldetails[0].keyspace,"cr");
        assert_eq!(stored_cqldetails[0].cql_details_type,"QUERY");
        assert_eq!(stored_cqldetails[0].sql_string,"select avg(permit), avg(permit_recheck), avg( handgun), avg( long_gun), avg( other), avg( multiple), avg( admin), avg( prepawn_handgun), avg( prepawn_long_gun), avg( prepawn_other), avg( redemption_handgun), avg( redemption_long_gun), avg( redemption_other), avg( returned_handgun), avg( returned_long_gun), avg( returned_other), avg( rentals_handgun), avg( rentals_long_gun), avg( private_sale_handgun), avg( private_sale_long_gun), avg( private_sale_other), avg( return_to_seller_handgun), avg( return_to_seller_long_gun), avg( return_to_seller_other), avg( totals) from fa_bg_checks;");
    }

    /*
     * Please mind clippy remains on complaining about invisible characters despite  #[allow(clippy::invisible_characters)]
     * I know there are invisible characters in the params, but this is an actual sample of the params.
     */
    #[test]
    #[allow(clippy::invisible_characters)]
    fn parse_inboundrpc_active_batch_query_ycql() {
        /*
         * This is how an active batch query via ycqlsh looks like.
         */
        #[allow(clippy::invisible_characters)]
        let json = r#"
{
    "inbound_connections": [
        {
            "remote_ip": "127.0.0.1:35692",
            "state": "OPEN",
            "processed_call_count": 135,
            "connection_details": {
                "cql_connection_details": {
                    "keyspace": "cr"
                }
            },
            "calls_in_flight": [
                {
                    "elapsed_millis": 6,
                    "cql_details": {
                        "type": "BATCH",
                        "call_details": [
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Alabama, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u001C,, \u0000\u0000\u001C\u001C, n/a, \u0000\u0000\u0001B, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\t, \u0000\u0000\u0000\u000B, n/a, \u0000\u0000\u0005\u000F, \u0000\u0000\u0005\u0081, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000D.]"

                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Alaska, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0005ó, \u0000\u0000\u0007l, n/a, \u0000\u0000\u0000L, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0006, \u0000\u0000\u0000\f, n/a, \u0000\u0000\u0000|, \u0000\u0000\u0000£, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000\u000EÜ]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Arizona, \u0000\u0000\nG, n/a, \u0000\u0000\u0015_, \u0000\u0000\u0010Í, n/a, \u0000\u0000\u0000å, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0004, \u0000\u0000\u0000\u0002, n/a, \u0000\u0000\u0002-, \u0000\u0000\u0001Á, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u00005L]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Arkansas, \u0000\u0000\tÖ, n/a, \u0000\u0000\f­, \u0000\u0000\r\", n/a, \u0000\u0000\u0000Î, \u0000\u0000\u0000\u0002, \u0000\u0000\u0000\u0006, \u0000\u0000\u0000\u0013, n/a, \u0000\u0000\u00022, \u0000\u0000\u0005j, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000,*]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, California, \u0000\u0000^ö, n/a, \u0000\u0000F‘, \u0000\u0000:c, n/a, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000ßê]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Colorado, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u001FQ, \u0000\u0000\u001E‘, n/a, \u0000\u0000\fM, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000J/]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Connecticut, \u0000\u0000\u0014ñ, n/a, \u0000\u0000\tú, \u0000\u0000\bK, n/a, \u0000\u0000\u00009, \u0000\u0000\u00023, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000)¢]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Delaware, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0002\u000B, \u0000\u0000\u0001Ü, n/a, \u0000\u0000\u0000\u000E, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000\u0003õ]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, District of Columbia, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0010, n/a, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000\u0000\u0010]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Florida, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000SE, \u0000\u0000*G, n/a, \u0000\u0000\u0002Ù, \u0000\u0000\u0001`, \u0000\u0000\u0000\u0005, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0002\u0004, \u0000\u0000\u0001“, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000…a]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Georgia, \u0000\u0000!D, n/a, \u0000\u0000\u001A), \u0000\u0000\u0012q, n/a, \u0000\u0000\u00010, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0019, \u0000\u0000\u0000\u001F, n/a, \u0000\u0000\u0003Œ, \u0000\u0000\u0005\u001F, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000Wñ]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Guam, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0000\n, \u0000\u0000\u0000\u0019, n/a, \u0000\u0000\u0000\u0001, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000\u0000$]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Hawaii, \u0000\u0000\u0002`, n/a, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000\u0002`]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Idaho, \u0000\u0000\u0005„, n/a, \u0000\u0000\u0006[, \u0000\u0000\u000Bk, n/a, \u0000\u0000\u0000c, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0001, \u0000\u0000\u0000\u0007, n/a, \u0000\u0000\u0000Ç, \u0000\u0000\u0001þ, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000\u001Az]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Illinois, \u0000\u0000d;, n/a, \u0000\u0000\u0014l, \u0000\u0000\u0014l, n/a, \u0000\u0000\u0001\u0007, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000Ž\u001A]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Indiana, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0016E, \u0000\u0000\u0016:, n/a, \u0000\u0000\u0000É, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0001, n/a, \u0000\u0000\u0000\u0001, \u0000\u0000\u0001„, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000.Î]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Iowa, \u0000\u0000\r\r, n/a, \u0000\u0000\u0000\u0011, \u0000\u0000\u0006, n/a, \u0000\u0000\u0000\u0001, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0001, n/a, \u0000\u0000\u0000\u0002, \u0000\u0000\u0000N, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000\u0013ï]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Kansas, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\fÜ, \u0000\u0000\u000B#, n/a, \u0000\u0000\u0000¬, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0001, \u0000\u0000\u0000\u0002, n/a, \u0000\u0000\u0001), \u0000\u0000\u0001\b, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0000\u001Aß]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Kentucky, \u0000\u0002\u0006K, n/a, \u0000\u0000\u0016@, \u0000\u0000\u0013ª, n/a, \u0000\u0000\u0001u, \u0000\u0000\u0000\u0002, \u0000\u0000\u0000\u0010, \u0000\u0000\u0000\f, n/a, \u0000\u0000\u0004`, \u0000\u0000\u0006o, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u0002<—]"
                            },
                            {
                                "sql_id": "344cf13216c84b621b82d4c212f04b0a",
                                "sql_string": "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                "params": "[2008-06, Louisiana, \u0000\u0000\u0000\u0000, n/a, \u0000\u0000\u0019\u00cd, \u0000\u0000\u0014L, n/a, \u0000\u0000\u0000\u00da, \u0000\u0000\u0000\u0000, \u0000\u0000\u0000\u0005, \u0000\u0000\u0000\u0003, n/a, \u0000\u0000\u0002\u0192, \u0000\u0000\u0003@, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \u0000\u00004\u00be]"

                            }
                        ]
                    }
                }
            ]
        }
    ]
}
        "#.to_string();
        let result = parse_rpcs(json, "", "");
        let mut stored_ysqlrpc: Vec<StoredYsqlRpc> = Vec::new();
        let mut stored_inboundrpc: Vec<StoredInboundRpc> = Vec::new();
        let mut stored_outboundrpc: Vec<StoredOutboundRpc> = Vec::new();
        let mut stored_cqldetails: Vec<StoredCqlDetails> = Vec::new();
        let mut stored_headers: Vec<StoredHeaders> = Vec::new();
        add_to_rpcs_vectors(result, "", Local::now(), &mut stored_ysqlrpc, &mut stored_inboundrpc, &mut stored_outboundrpc, &mut stored_cqldetails, &mut stored_headers);
        assert_eq!(stored_inboundrpc[0].remote_ip,"127.0.0.1:35692");
        assert_eq!(stored_inboundrpc[0].state,"OPEN");
        assert_eq!(stored_inboundrpc[0].processed_call_count,135);
        assert_eq!(stored_cqldetails[0].keyspace,"cr");
        assert_eq!(stored_cqldetails[0].cql_details_type,"BATCH");
        assert_eq!(stored_cqldetails[0].sql_id,"344cf13216c84b621b82d4c212f04b0a");
        assert_eq!(stored_cqldetails[0].sql_string,"INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        assert_eq!(stored_cqldetails[0].params,"[2008-06, Alabama, \0\0\0\0, n/a, \0\0\u{1c},, \0\0\u{1c}\u{1c}, n/a, \0\0\u{1}B, \0\0\0\0, \0\0\0\t, \0\0\0\u{b}, n/a, \0\0\u{5}\u{f}, \0\0\u{5}\u{81}, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \0\0D.]");
        assert_eq!(stored_cqldetails[0].cql_details_type,"BATCH");
        assert_eq!(stored_cqldetails[19].sql_id,"344cf13216c84b621b82d4c212f04b0a");
        assert_eq!(stored_cqldetails[19].sql_string,"INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        assert_eq!(stored_cqldetails[19].params,"[2008-06, Louisiana, \0\0\0\0, n/a, \0\0\u{19}\u{00cd}, \0\0\u{14}L, n/a, \0\0\0\u{00da}, \0\0\0\0, \0\0\0\u{5}, \0\0\0\u{3}, n/a, \0\0\u{2}\u{0192}, \0\0\u{3}@, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \0\04\u{00be}]");
    }

    #[test]
    fn parse_inboundrpc_and_outboundrpc_simple_tabletserver() {
        /*
         * This is the simple, most usual form of inbound and outbound connections for tablet server and master.
         * The entries can have a more verbose form when they are active.
         */
        let json = r#"
{
    "inbound_connections":
    [
        {
            "remote_ip": "172.158.40.206:38776",
            "state": "OPEN",
            "processed_call_count": 314238
        }
    ],
    "outbound_connections":
    [
        {
            "remote_ip": "172.158.40.206:9100",
            "state": "OPEN",
            "processed_call_count": 316390,
            "sending_bytes": 0
        }
    ]
}
        "#.to_string();
        let result = parse_rpcs(json, "", "");
        let mut stored_ysqlrpc: Vec<StoredYsqlRpc> = Vec::new();
        let mut stored_inboundrpc: Vec<StoredInboundRpc> = Vec::new();
        let mut stored_outboundrpc: Vec<StoredOutboundRpc> = Vec::new();
        let mut stored_cqldetails: Vec<StoredCqlDetails> = Vec::new();
        let mut stored_headers: Vec<StoredHeaders> = Vec::new();
        add_to_rpcs_vectors(result, "", Local::now(), &mut stored_ysqlrpc, &mut stored_inboundrpc, &mut stored_outboundrpc, &mut stored_cqldetails, &mut stored_headers);
        // inbound
        assert_eq!(stored_inboundrpc[0].remote_ip, "172.158.40.206:38776");
        assert_eq!(stored_inboundrpc[0].state, "OPEN");
        assert_eq!(stored_inboundrpc[0].processed_call_count, 314238);
        // outbound
        assert_eq!(stored_outboundrpc[0].remote_ip, "172.158.40.206:9100");
        assert_eq!(stored_outboundrpc[0].state, "OPEN");
        assert_eq!(stored_outboundrpc[0].processed_call_count, 316390);
        assert_eq!(stored_outboundrpc[0].sending_bytes, 0);
    }

    #[test]
    fn parse_inboundrpc_only_simple_tabletserver() {
        /*
         * This is the simple, most usual form of inbound connections for tablet server and master.
         * The entries can have a more verbose form when they are active.
         */
        let json = r#"
{
    "inbound_connections":
    [
        {
            "remote_ip": "172.158.40.206:38776",
            "state": "OPEN",
            "processed_call_count": 314238
        }
    ]
}
        "#.to_string();
        let result = parse_rpcs(json, "", "");
        let mut stored_ysqlrpc: Vec<StoredYsqlRpc> = Vec::new();
        let mut stored_inboundrpc: Vec<StoredInboundRpc> = Vec::new();
        let mut stored_outboundrpc: Vec<StoredOutboundRpc> = Vec::new();
        let mut stored_cqldetails: Vec<StoredCqlDetails> = Vec::new();
        let mut stored_headers: Vec<StoredHeaders> = Vec::new();
        add_to_rpcs_vectors(result, "", Local::now(), &mut stored_ysqlrpc, &mut stored_inboundrpc, &mut stored_outboundrpc, &mut stored_cqldetails, &mut stored_headers);
        // inbound
        assert_eq!(stored_inboundrpc[0].remote_ip, "172.158.40.206:38776");
        assert_eq!(stored_inboundrpc[0].state, "OPEN");
        assert_eq!(stored_inboundrpc[0].processed_call_count, 314238);
    }

    #[test]
    fn parse_inboundrpc_and_outboundrpc_advanced_tabletserver() {
        /*
         * This is the "advanced" form of inbound and outbound connections for tablet server and master.
         * "Advanced" means an in-progress tablet server transaction.
         */
        let json = r#"
{
    "inbound_connections":
    [
        {
            "remote_ip": "192.168.66.82:51316",
            "state": "OPEN",
            "processed_call_count": 74,
            "calls_in_flight": [
                {
                    "header": {
                        "call_id": 3160,
                        "remote_method": {
                            "service_name": "yb.consensus.ConsensusService",
                            "method_name": "UpdateConsensus"
                        },
                        "timeout_millis": 3000
                    },
                    "elapsed_millis": 6
                }
            ]
        }
    ],
    "outbound_connections":
    [
        {
            "remote_ip": "172.158.40.206:9100",
            "state": "OPEN",
            "processed_call_count": 316390,
            "calls_in_flight": [
                {
                    "header": {
                        "call_id": 4595,
                        "remote_method": {
                            "service_name": "yb.tserver.TabletServerService",
                            "method_name": "Write"
                        },
                        "timeout_millis": 119994
                    },
                    "elapsed_millis": 84,
                    "state": "SENT"
                },
                {
                    "header": {
                        "call_id": 4615,
                        "remote_method": {
                            "service_name": "yb.tserver.TabletServerService",
                            "method_name": "Write"
                        },
                        "timeout_millis": 119991
                    },
                    "elapsed_millis": 7,
                    "state": "SENT"
                }
            ],
            "sending_bytes": 0
        }
    ]
}
        "#.to_string();
        let result = parse_rpcs(json, "", "");
        let mut stored_ysqlrpc: Vec<StoredYsqlRpc> = Vec::new();
        let mut stored_inboundrpc: Vec<StoredInboundRpc> = Vec::new();
        let mut stored_outboundrpc: Vec<StoredOutboundRpc> = Vec::new();
        let mut stored_cqldetails: Vec<StoredCqlDetails> = Vec::new();
        let mut stored_headers: Vec<StoredHeaders> = Vec::new();
        add_to_rpcs_vectors(result, "", Local::now(), &mut stored_ysqlrpc, &mut stored_inboundrpc, &mut stored_outboundrpc, &mut stored_cqldetails, &mut stored_headers);
        // inbound
        assert_eq!(stored_inboundrpc[0].remote_ip,"192.168.66.82:51316");
        assert_eq!(stored_inboundrpc[0].state,"OPEN");
        assert_eq!(stored_inboundrpc[0].processed_call_count,74);
        assert_eq!(stored_headers[0].call_id,3160);
        assert_eq!(stored_headers[0].remote_method_service_name,"yb.consensus.ConsensusService");
        assert_eq!(stored_headers[0].remote_method_method_name,"UpdateConsensus");
        assert_eq!(stored_headers[0].timeout_millis,3000);
        assert_eq!(stored_headers[0].elapsed_millis,6);
        // outbound
        assert_eq!(stored_outboundrpc[0].remote_ip,"172.158.40.206:9100");
        assert_eq!(stored_outboundrpc[0].state,"OPEN");
        assert_eq!(stored_outboundrpc[0].processed_call_count,316390);
        assert_eq!(stored_outboundrpc[0].sending_bytes,0);
        assert_eq!(stored_headers[1].call_id,4595);
        assert_eq!(stored_headers[1].remote_method_service_name,"yb.tserver.TabletServerService");
        assert_eq!(stored_headers[1].remote_method_method_name,"Write");
        assert_eq!(stored_headers[1].timeout_millis,119994);
        assert_eq!(stored_headers[1].elapsed_millis,84);
        assert_eq!(stored_headers[1].state,"SENT");
        assert_eq!(stored_headers[2].call_id,4615);
        assert_eq!(stored_headers[2].remote_method_service_name,"yb.tserver.TabletServerService");
        assert_eq!(stored_headers[2].remote_method_method_name,"Write");
        assert_eq!(stored_headers[2].timeout_millis,119991);
        assert_eq!(stored_headers[2].elapsed_millis,7);
        assert_eq!(stored_headers[2].state,"SENT");
    }

    #[test]
    fn parse_inboundrpc_and_outboundrpc_master_server() {
        /*
         * It turns out the master can have a different format with lesser JSON fields used.
         * The below outbound_connections JSON document is an example of that.
         */
        let json = r#"
{
        "inbound_connections": [
            {
                "remote_ip": "192.168.66.80:53856",
                "state": "OPEN",
                "processed_call_count": 186
            },
            {
                "remote_ip": "192.168.66.80:53858",
                "state": "OPEN",
                "processed_call_count": 186
            },
            {
                "remote_ip": "192.168.66.80:53860",
                "state": "OPEN",
                "processed_call_count": 185
            },
            {
                "remote_ip": "192.168.66.80:53864",
                "state": "OPEN",
                "processed_call_count": 184
            },
            {
                "remote_ip": "192.168.66.80:53866",
                "state": "OPEN",
                "processed_call_count": 184
            },
            {
                "remote_ip": "192.168.66.80:53868",
                "state": "OPEN",
                "processed_call_count": 184
            },
            {
                "remote_ip": "192.168.66.80:53874",
                "state": "OPEN",
                "processed_call_count": 184
            },
            {
                "remote_ip": "192.168.66.80:53876",
                "state": "OPEN",
                "processed_call_count": 184
            },
            {
                "remote_ip": "192.168.66.82:33168",
                "state": "OPEN",
                "processed_call_count": 1
            },
            {
                "remote_ip": "192.168.66.82:33170",
                "state": "OPEN",
                "processed_call_count": 1
            }
        ],
        "outbound_connections": [
            {
                "remote_ip": "192.168.66.80:7100",
                "state": "OPEN",
                "calls_in_flight": [
                    {
                        "header": {
                            "call_id": 7993,
                            "remote_method": {
                                "service_name": "yb.master.MasterService",
                                "method_name": "GetMasterRegistration"
                            },
                            "timeout_millis": 1500
                        },
                        "elapsed_millis": 3,
                        "state": "SENT"
                    }
                ],
                "sending_bytes": 0
            }
        ]
    }
        "#.to_string();
        let result = parse_rpcs(json, "", "");
        let mut stored_ysqlrpc: Vec<StoredYsqlRpc> = Vec::new();
        let mut stored_inboundrpc: Vec<StoredInboundRpc> = Vec::new();
        let mut stored_outboundrpc: Vec<StoredOutboundRpc> = Vec::new();
        let mut stored_cqldetails: Vec<StoredCqlDetails> = Vec::new();
        let mut stored_headers: Vec<StoredHeaders> = Vec::new();
        add_to_rpcs_vectors(result, "", Local::now(), &mut stored_ysqlrpc, &mut stored_inboundrpc, &mut stored_outboundrpc, &mut stored_cqldetails, &mut stored_headers);
        // inbound
        assert_eq!(stored_inboundrpc[0].remote_ip, "192.168.66.80:53856");
        assert_eq!(stored_inboundrpc[0].state, "OPEN");
        assert_eq!(stored_inboundrpc[0].processed_call_count, 186);
        // outbound
        assert_eq!(stored_outboundrpc[0].remote_ip, "192.168.66.80:7100");
    }

}