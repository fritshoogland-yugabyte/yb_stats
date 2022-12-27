//use serde_derive::{Serialize,Deserialize};
use chrono::{DateTime, Local};
use std::{sync::mpsc::channel, collections::BTreeMap, time::Instant};
use log::*;
use regex::Regex;
use anyhow::Result;
use crate::rpcs::AllConnections::{Connections, InAndOutboundConnections};
use crate::utility;
use crate::snapshot;
use crate::rpcs::{AllConnections, YsqlConnection, StoredYsqlConnection, StoredInboundRpc, InboundConnection, AllStoredConnections, StoredCqlDetails, StoredOutboundRpc, StoredHeaders};
use crate::Opts;

impl StoredYsqlConnection {
    fn new(hostname_port: &str,
           timestamp: DateTime<Local>,
           connection: YsqlConnection,
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

impl AllStoredConnections {
    // called from snapshot::perform_snapshot
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allconnections = AllStoredConnections::read_connections(hosts, ports, parallel).await;
        snapshot::save_snapshot(snapshot_number, "ysqlrpc", allconnections.stored_ysqlconnections)?;
        snapshot::save_snapshot(snapshot_number, "inboundrpc", allconnections.stored_inboundrpcs)?;
        snapshot::save_snapshot(snapshot_number, "outboundrpc", allconnections.stored_outboundrpcs)?;
        snapshot::save_snapshot(snapshot_number, "cqldetails", allconnections.stored_cqldetails)?;
        snapshot::save_snapshot(snapshot_number, "headers", allconnections.stored_headers)?;

        info!("end snapshot: {:?}", timer.elapsed());
        Ok(())
    }
    pub fn new() -> Self { Default::default() }
    pub async fn read_connections(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllStoredConnections
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
                        let connections = AllStoredConnections::read_http(host, port);
                        tx.send((format!("{}:{}", host, port), detail_snapshot_time, connections)).expect("error sending data via tx (masters)");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allstoredconnections = AllStoredConnections::new();

        for (hostname_port, detail_snapshot_time, connections) in rx {
            allstoredconnections.split_into_vectors(connections, &hostname_port, detail_snapshot_time);
        }

        allstoredconnections
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> AllConnections
    {
        let data_from_http = utility::http_get(host, port, "rpcz");
        AllStoredConnections::parse_connections(data_from_http, host, port)
    }
    fn parse_connections(
        connections: String,
        host: &str,
        port: &str,
    ) -> AllConnections {
        serde_json::from_str(&connections).unwrap_or_else(|e| {
            debug!("Could not parse {}:{}/rpcz json data for rpcs, error: {}", host, port, e);
            AllConnections::Empty {}
        })
    }
    pub fn split_into_vectors(
        &mut self,
        allconnections: AllConnections,
        hostname: &str,
        detail_snapshot_time: DateTime<Local>,
    )
    {
        match allconnections {
            Connections { connections } => {
                for ysqlconnection in connections {
                    trace!("add ysqlconnection, hostname: {}, {:?}", hostname, &ysqlconnection);
                    self.stored_ysqlconnections.push( StoredYsqlConnection::new( hostname, detail_snapshot_time, ysqlconnection) );
                }
            },
            InAndOutboundConnections { inbound_connections, outbound_connections } => {
                for (serial_number, inboundrpc) in (0_u32..).zip(inbound_connections.unwrap_or_default().into_iter()) {
                    trace!("add inboundrpc, hostname: {}, {:?}", hostname, inboundrpc);
                    self.stored_inboundrpcs.push(StoredInboundRpc::new(hostname, detail_snapshot_time, serial_number, &inboundrpc));
                    let keyspace = match inboundrpc.connection_details.as_ref() {
                        Some( connection_details )  => {
                            connection_details.cql_connection_details.keyspace.clone()
                        },
                        None => String::from("")
                    };
                    for calls_in_flight in inboundrpc.calls_in_flight.unwrap_or_default() {
                        if let Some ( cql_details ) = calls_in_flight.cql_details {
                            for call_detail in cql_details.call_details {
                                self.stored_cqldetails.push(StoredCqlDetails {
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
                        if let Some ( header) = calls_in_flight.header {
                            self.stored_headers.push( StoredHeaders {
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
                    }
                };

                for (serial_number, outboundrpc) in (0_u32..).zip(outbound_connections.unwrap_or_default().into_iter()) {
                    trace!("add outboundrpc, hostname: {}, {:?}", hostname, outboundrpc);
                    self.stored_outboundrpcs.push(StoredOutboundRpc {
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
                            self.stored_headers.push( StoredHeaders {
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
                    }
                }
            },
            _ => {
                info!("No match: hostname: {}; {:?}", hostname, allconnections);
            },
        }
    }
    pub fn print(
        &self,
        details_enable: &bool,
        hostname_filter: &Regex,
    ) -> Result<()>
    {
        info!("print connections");

        let mut endpoint_count: BTreeMap<String, (usize, usize)> = BTreeMap::new();
        for row in &self.stored_ysqlconnections
        {
            if row.backend_status == "active"
            {
                endpoint_count.entry(row.hostname_port.clone()).and_modify(|(active, total)| { *active +=1; *total += 1} ).or_insert((1,1));
            }
            else
            {
                endpoint_count.entry(row.hostname_port.clone()).and_modify(|(_active, total)| *total +=1).or_insert((0,1));
            }
        }
        for row in &self.stored_inboundrpcs
        {
            // active cql
            if self.stored_cqldetails.iter().any(|r| r.hostname_port == row.hostname_port && r.remote_ip == row.remote_ip && r.serial_nr == row.serial_nr)
            {
                endpoint_count.entry(row.hostname_port.clone()).and_modify(|(active, total)| { *active +=1; *total += 1} ).or_insert((1,1));
            }
            // active rpc
            else if self.stored_headers.iter().any(|r| r.hostname_port == row.hostname_port && r.remote_ip == row.remote_ip && r.serial_nr == row.serial_nr)
            {
                endpoint_count.entry(row.hostname_port.clone()).and_modify(|(active, total)| { *active +=1; *total += 1} ).or_insert((1,1));
            }
            // inactive rpc
            else
            {
                endpoint_count.entry(row.hostname_port.clone()).and_modify(|(_active, total)| *total +=1).or_insert((0,1));
            }
        }
        for row in &self.stored_outboundrpcs
        {
            // active rpc
            if self.stored_headers.iter().any(|r| r.hostname_port == row.hostname_port && r.remote_ip == row.remote_ip && r.serial_nr == row.serial_nr)
            {
                endpoint_count.entry(row.hostname_port.clone()).and_modify(|(active, total)| { *active +=1; *total += 1} ).or_insert((1,1));
            }
            // inactive rpc
            else
            {
                endpoint_count.entry(row.hostname_port.clone()).and_modify(|(_active, total)| *total +=1).or_insert((0,1));
            }
        }
        let mut previous_hostname = String::new();
        for (endpoint, (active, inactive)) in &endpoint_count {
            if hostname_filter.is_match(&endpoint) {
                let current_hostname = &endpoint.split(':').next().unwrap();
                let current_port = &endpoint.split(':').nth(1).unwrap();
                if current_hostname.to_string() != previous_hostname {
                    println!("\n{}", "-".repeat(120));
                    if current_port.to_string() != String::from("") {
                        self.print_details(previous_hostname, details_enable, &endpoint_count);
                    }

                    print!("{}", current_hostname);
                    previous_hostname = current_hostname.to_string();
                }
                print!("; port: {}, {}/{} act/tot", current_port, active, inactive);
            };
        }
        println!("\n{}", "-".repeat(120));
        self.print_details(previous_hostname, details_enable, &endpoint_count);

        Ok(())
    }
    fn print_details(
        &self,
        hostname: String,
        details_enable: &bool,
        endpoint_count: &BTreeMap<String,(usize, usize)>,
    )
    {
        let mut activity_counter = 0;
        for (endpoint, (_active, _inactive)) in endpoint_count {
            if endpoint.split(':').nth(0).unwrap() == hostname {
                // YSQL connections have a 1:1 relationship
                for row in self.stored_ysqlconnections.iter().filter(|r| r.hostname_port == endpoint.to_string()) {
                    if row.backend_status == "active" || *details_enable {
                        println!("{}<-{:30} {:6} {:>6} ms db:{}, q:{}", row.hostname_port, format!("{}:{}", row.host, row.port), row.backend_status, row.query_running_for_ms, row.db_name, row.query);
                        activity_counter+=1;
                    }
                }
                for row in self.stored_inboundrpcs.iter().filter(|r| r.hostname_port == endpoint.to_string()) {
                    // YCQL connections have a 1:1 relationship
                    if let Some(cql) = self.stored_cqldetails.iter().find(|r| r.hostname_port == row.hostname_port && r.remote_ip == row.remote_ip && r.serial_nr == row.serial_nr) {
                        println!("{}<-{:30} {:6}  {:>6} ms ks:{}, q: {}", cql.hostname_port, cql.remote_ip, cql.cql_details_type, cql.elapsed_millis, cql.keyspace, cql.sql_string);
                        activity_counter+=1;
                    }
                    // inbound RPCs can have multiple requests
                    let mut counter = 0;
                    for header in self.stored_headers.iter().filter(|r| r.hostname_port == row.hostname_port && r.remote_ip == row.remote_ip && r.serial_nr == row.serial_nr) {
                        if counter == 0
                        {
                            println!("{}<-{:30}  {:6} {:>6} ms {}:{}", header.hostname_port, header.remote_ip, header.state, header.elapsed_millis, header.remote_method_service_name, header.remote_method_method_name);
                            activity_counter += 1;
                        }
                        else
                        {
                            println!("{:50}  {:6} {:>6} ms {}:{}", "", header.state, header.elapsed_millis, header.remote_method_service_name, header.remote_method_method_name);
                            activity_counter += 1;
                        };
                        counter += 1;
                    }
                    // idle inbound connections have a 1:1 relationship, because it's just an open connection
                    if self.stored_headers.iter().filter(|r| r.hostname_port == row.hostname_port && r.remote_ip == row.remote_ip && r.serial_nr == row.serial_nr).count() == 0 && *details_enable {
                        println!("{}<-{:30} {:6}", row.hostname_port, row.remote_ip, row.state);
                        activity_counter+=1;
                    }
                }
                for row in self.stored_outboundrpcs.iter().filter(|r| r.hostname_port == endpoint.to_string()) {
                    // outbound RPCs can have multiple requests
                    let mut counter = 0;
                    for header in self.stored_headers.iter().filter(|r| r.hostname_port == row.hostname_port && r.remote_ip == row.remote_ip && r.serial_nr == row.serial_nr) {
                        if counter == 0
                        {
                            println!("{}->{:30}  {:6} {:>6} ms {}:{}", header.hostname_port, header.remote_ip, header.state, header.elapsed_millis, header.remote_method_service_name, header.remote_method_method_name);
                            activity_counter+=1;
                        }
                        else
                        {
                            println!("{:50}  {:6} {:>6} ms {}:{}", "", header.state, header.elapsed_millis, header.remote_method_service_name, header.remote_method_method_name);
                            activity_counter += 1;
                        }
                        counter += 1;
                    }
                    // idle outbound connections have a 1:1 relationship, because it's just en open connection
                    if self.stored_headers.iter().filter(|r| r.hostname_port == row.hostname_port && r.remote_ip == row.remote_ip && r.serial_nr == row.serial_nr).count() == 0 && *details_enable {
                        println!("{}->{:30}  {:6}", row.hostname_port, row.remote_ip, row.state);
                        activity_counter+=1;
                    }
                }
            }
        }
        if activity_counter > 0 {
            println!("{}", "-".repeat(120));
        }
    }
}

// called from main
pub async fn print_rpcs(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    let hostname_filter = utility::set_regex(&options.hostname_match);
    // unwrap() removes/evaluates the first Option<>, match evaluates the the second Option<>.
    match options.print_rpcs.as_ref().unwrap() {
        // a snapshot_number provided, read snapshots and print data.
        Some(snapshot_number) => {
            let mut allstoredconnections = AllStoredConnections::new();
            allstoredconnections.stored_ysqlconnections = snapshot::read_snapshot(snapshot_number, "ysqlrpc")?;
            allstoredconnections.stored_inboundrpcs = snapshot::read_snapshot(snapshot_number, "inboundrpc")?;
            allstoredconnections.stored_outboundrpcs = snapshot::read_snapshot(snapshot_number, "outboundrpc")?;
            allstoredconnections.stored_cqldetails = snapshot::read_snapshot(snapshot_number, "cqldetails")?;
            allstoredconnections.stored_headers = snapshot::read_snapshot(snapshot_number, "headers")?;

            allstoredconnections.print(&options.details_enable, &hostname_filter)?;
        },
        // no snapshot number was provided, obtain data from the endpoints
        None => {
            let allstoredconnections = AllStoredConnections::read_connections(&hosts, &ports, parallel).await;
            allstoredconnections.print(&options.details_enable, &hostname_filter)?;
        },
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_ysqlconnection_only_checkpointer()
    {
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
        let result = AllStoredConnections::parse_connections(json,"","");
        let mut allstoredconnections = AllStoredConnections::new();
        allstoredconnections.split_into_vectors(result,"", Local::now());
        assert_eq!(allstoredconnections.stored_ysqlconnections[0].process_start_time,"2022-08-11 10:06:23.639902+00");
        assert_eq!(allstoredconnections.stored_ysqlconnections[0].application_name,"");
        assert_eq!(allstoredconnections.stored_ysqlconnections[0].backend_type,"checkpointer");
        assert_eq!(allstoredconnections.stored_ysqlconnections[0].backend_status,"");
    }

    #[test]
    fn unit_parse_ysqlconnection_active_transaction()
    {
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
        let result = AllStoredConnections::parse_connections(json,"","");
        let mut allstoredconnections = AllStoredConnections::new();
        allstoredconnections.split_into_vectors(result,"", Local::now());
        assert_eq!(allstoredconnections.stored_ysqlconnections[0].db_oid,13285);
        assert_eq!(allstoredconnections.stored_ysqlconnections[0].db_name,"yugabyte");
        assert_eq!(allstoredconnections.stored_ysqlconnections[0].query,"insert into t select x from generate_series(1,100000000) x;");
        assert_eq!(allstoredconnections.stored_ysqlconnections[0].query_start_time,"2022-08-12 14:20:38.366499+00");
        assert_eq!(allstoredconnections.stored_ysqlconnections[0].query_running_for_ms,1824);
    }

    #[test]
    fn unit_parse_inboundrpc_idle_ycql() {
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
        let result = AllStoredConnections::parse_connections(json,"","");
        let mut allstoredconnections = AllStoredConnections::new();
        allstoredconnections.split_into_vectors(result,"", Local::now());
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].remote_ip,"127.0.0.1:33086");
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].state,"OPEN");
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].processed_call_count,2);
        assert_eq!(allstoredconnections.stored_inboundrpcs[1].remote_ip,"127.0.0.1:33084");
        assert_eq!(allstoredconnections.stored_inboundrpcs[1].state,"OPEN");
        assert_eq!(allstoredconnections.stored_inboundrpcs[1].processed_call_count,13);
    }

    #[test]
    fn unit_parse_inboundrpc_active_query_ycql() {
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
        let result = AllStoredConnections::parse_connections(json,"","");
        let mut allstoredconnections = AllStoredConnections::new();
        allstoredconnections.split_into_vectors(result,"", Local::now());
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].remote_ip,"127.0.0.1:35518");
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].state,"OPEN");
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].processed_call_count,20);
        assert_eq!(allstoredconnections.stored_cqldetails[0].keyspace,"cr");
        assert_eq!(allstoredconnections.stored_cqldetails[0].cql_details_type,"QUERY");
        assert_eq!(allstoredconnections.stored_cqldetails[0].sql_string,"select avg(permit), avg(permit_recheck), avg( handgun), avg( long_gun), avg( other), avg( multiple), avg( admin), avg( prepawn_handgun), avg( prepawn_long_gun), avg( prepawn_other), avg( redemption_handgun), avg( redemption_long_gun), avg( redemption_other), avg( returned_handgun), avg( returned_long_gun), avg( returned_other), avg( rentals_handgun), avg( rentals_long_gun), avg( private_sale_handgun), avg( private_sale_long_gun), avg( private_sale_other), avg( return_to_seller_handgun), avg( return_to_seller_long_gun), avg( return_to_seller_other), avg( totals) from fa_bg_checks;");
    }

    /*
     * Please mind clippy remains on complaining about invisible characters despite  #[allow(clippy::invisible_characters)]
     * I know there are invisible characters in the params, but this is an actual sample of the params.
     */
    #[test]
    #[allow(clippy::invisible_characters)]
    fn unit_parse_inboundrpc_active_batch_query_ycql() {
        /*
         * This is how an active batch query via ycqlsh looks like.
         */
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
        let result = AllStoredConnections::parse_connections(json,"","");
        let mut allstoredconnections = AllStoredConnections::new();
        allstoredconnections.split_into_vectors(result,"", Local::now());
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].remote_ip,"127.0.0.1:35692");
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].state,"OPEN");
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].processed_call_count,135);
        assert_eq!(allstoredconnections.stored_cqldetails[0].keyspace,"cr");
        assert_eq!(allstoredconnections.stored_cqldetails[0].cql_details_type,"BATCH");
        assert_eq!(allstoredconnections.stored_cqldetails[0].sql_id,"344cf13216c84b621b82d4c212f04b0a");
        assert_eq!(allstoredconnections.stored_cqldetails[0].sql_string,"INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        assert_eq!(allstoredconnections.stored_cqldetails[0].params,"[2008-06, Alabama, \0\0\0\0, n/a, \0\0\u{1c},, \0\0\u{1c}\u{1c}, n/a, \0\0\u{1}B, \0\0\0\0, \0\0\0\t, \0\0\0\u{b}, n/a, \0\0\u{5}\u{f}, \0\0\u{5}\u{81}, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \0\0D.]");
        assert_eq!(allstoredconnections.stored_cqldetails[0].cql_details_type,"BATCH");
        assert_eq!(allstoredconnections.stored_cqldetails[19].sql_id,"344cf13216c84b621b82d4c212f04b0a");
        assert_eq!(allstoredconnections.stored_cqldetails[19].sql_string,"INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        assert_eq!(allstoredconnections.stored_cqldetails[19].params,"[2008-06, Louisiana, \0\0\0\0, n/a, \0\0\u{19}\u{00cd}, \0\0\u{14}L, n/a, \0\0\0\u{00da}, \0\0\0\0, \0\0\0\u{5}, \0\0\0\u{3}, n/a, \0\0\u{2}\u{0192}, \0\0\u{3}@, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \0\04\u{00be}]");
    }

    #[test]
    fn unit_parse_inboundrpc_and_outboundrpc_simple_tabletserver() {
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
        let result = AllStoredConnections::parse_connections(json,"","");
        let mut allstoredconnections = AllStoredConnections::new();
        allstoredconnections.split_into_vectors(result,"", Local::now());
        // inbound
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].remote_ip, "172.158.40.206:38776");
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].state, "OPEN");
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].processed_call_count, 314238);
        // outbound
        assert_eq!(allstoredconnections.stored_outboundrpcs[0].remote_ip, "172.158.40.206:9100");
        assert_eq!(allstoredconnections.stored_outboundrpcs[0].state, "OPEN");
        assert_eq!(allstoredconnections.stored_outboundrpcs[0].processed_call_count, 316390);
        assert_eq!(allstoredconnections.stored_outboundrpcs[0].sending_bytes, 0);
    }

    #[test]
    fn unit_parse_outboundrpc_simple_tabletserver() {
        /*
         * This is the simple, most usual form of outbound connections for tablet server and master.
         * The entries can have a more verbose form when they are active.
         */
        let json = r#"
{
    "outbound_connections": [
        {
            "remote_ip": "192.168.66.80:7100",
            "state": "OPEN",
            "processed_call_count": 3526,
            "sending_bytes": 0
        },
        {
            "remote_ip": "192.168.66.80:7100",
            "state": "OPEN",
            "processed_call_count": 3527,
            "sending_bytes": 0
        },
        {
            "remote_ip": "192.168.66.80:7100",
            "state": "OPEN",
            "processed_call_count": 3526,
            "sending_bytes": 0
        },
        {
            "remote_ip": "192.168.66.80:7100",
            "state": "OPEN",
            "processed_call_count": 3527,
            "sending_bytes": 0
        },
        {
            "remote_ip": "192.168.66.80:7100",
            "state": "OPEN",
            "processed_call_count": 3527,
            "sending_bytes": 0
        },
        {
            "remote_ip": "192.168.66.80:7100",
            "state": "OPEN",
            "processed_call_count": 3527,
            "sending_bytes": 0
        },
        {
            "remote_ip": "192.168.66.80:7100",
            "state": "OPEN",
            "processed_call_count": 3527,
            "sending_bytes": 0
        },
        {
            "remote_ip": "192.168.66.80:7100",
            "state": "OPEN",
            "processed_call_count": 3527,
            "sending_bytes": 0
        }
    ]
}
        "#.to_string();
        let result = AllStoredConnections::parse_connections(json,"","");
        let mut allstoredconnections = AllStoredConnections::new();
        allstoredconnections.split_into_vectors(result,"", Local::now());
        // outbound
        assert_eq!(allstoredconnections.stored_outboundrpcs[0].remote_ip, "192.168.66.80:7100");
        assert_eq!(allstoredconnections.stored_outboundrpcs[0].state, "OPEN");
        assert_eq!(allstoredconnections.stored_outboundrpcs[0].processed_call_count, 3526);
        assert_eq!(allstoredconnections.stored_outboundrpcs[0].sending_bytes, 0);
    }

    #[test]
    fn unit_parse_inboundrpc_only_simple_tabletserver() {
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
        let result = AllStoredConnections::parse_connections(json,"","");
        let mut allstoredconnections = AllStoredConnections::new();
        allstoredconnections.split_into_vectors(result,"", Local::now());
        // inbound
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].remote_ip, "172.158.40.206:38776");
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].state, "OPEN");
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].processed_call_count, 314238);
    }

    #[test]
    fn unit_parse_inboundrpc_and_outboundrpc_advanced_tabletserver() {
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
        let result = AllStoredConnections::parse_connections(json,"","");
        let mut allstoredconnections = AllStoredConnections::new();
        allstoredconnections.split_into_vectors(result,"", Local::now());
        // inbound
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].remote_ip,"192.168.66.82:51316");
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].state,"OPEN");
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].processed_call_count,74);
        assert_eq!(allstoredconnections.stored_headers[0].call_id,3160);
        assert_eq!(allstoredconnections.stored_headers[0].remote_method_service_name,"yb.consensus.ConsensusService");
        assert_eq!(allstoredconnections.stored_headers[0].remote_method_method_name,"UpdateConsensus");
        assert_eq!(allstoredconnections.stored_headers[0].timeout_millis,3000);
        assert_eq!(allstoredconnections.stored_headers[0].elapsed_millis,6);
        // outbound
        assert_eq!(allstoredconnections.stored_outboundrpcs[0].remote_ip,"172.158.40.206:9100");
        assert_eq!(allstoredconnections.stored_outboundrpcs[0].state,"OPEN");
        assert_eq!(allstoredconnections.stored_outboundrpcs[0].processed_call_count,316390);
        assert_eq!(allstoredconnections.stored_outboundrpcs[0].sending_bytes,0);
        assert_eq!(allstoredconnections.stored_headers[1].call_id,4595);
        assert_eq!(allstoredconnections.stored_headers[1].remote_method_service_name,"yb.tserver.TabletServerService");
        assert_eq!(allstoredconnections.stored_headers[1].remote_method_method_name,"Write");
        assert_eq!(allstoredconnections.stored_headers[1].timeout_millis,119994);
        assert_eq!(allstoredconnections.stored_headers[1].elapsed_millis,84);
        assert_eq!(allstoredconnections.stored_headers[1].state,"SENT");
        assert_eq!(allstoredconnections.stored_headers[2].call_id,4615);
        assert_eq!(allstoredconnections.stored_headers[2].remote_method_service_name,"yb.tserver.TabletServerService");
        assert_eq!(allstoredconnections.stored_headers[2].remote_method_method_name,"Write");
        assert_eq!(allstoredconnections.stored_headers[2].timeout_millis,119991);
        assert_eq!(allstoredconnections.stored_headers[2].elapsed_millis,7);
        assert_eq!(allstoredconnections.stored_headers[2].state,"SENT");
    }

    #[test]
    fn unit_parse_inboundrpc_and_outboundrpc_master_server() {
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
        let result = AllStoredConnections::parse_connections(json,"","");
        let mut allstoredconnections = AllStoredConnections::new();
        allstoredconnections.split_into_vectors(result,"", Local::now());
        // inbound
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].remote_ip, "192.168.66.80:53856");
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].state, "OPEN");
        assert_eq!(allstoredconnections.stored_inboundrpcs[0].processed_call_count, 186);
        // outbound
        assert_eq!(allstoredconnections.stored_outboundrpcs[0].remote_ip, "192.168.66.80:7100");
    }

    #[tokio::test]
    async fn integration_parse_rpcs_tserver() {
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();
        let allstoredconnections = AllStoredConnections::read_connections(&vec![&hostname], &vec![&port], 1_usize).await;
        // a tserver / port 9000 does not have YSQL rpcs, port 13000 has.
        assert!(allstoredconnections.stored_ysqlconnections.is_empty());
        // a tserver will have inbound RPCs, even RF=1 / 1 tserver. NOPE, no inbound RPCS for tserver with RF1
        //assert!(!stored_inboundrpc.is_empty());
        // a tserver will have outbound RPCs, even RF=1 / 1 tserver.
        assert!(!allstoredconnections.stored_outboundrpcs.is_empty());
    }
    #[tokio::test]
    async fn integration_parse_rpcs_master() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();
        let allstoredconnections = AllStoredConnections::read_connections(&vec![&hostname], &vec![&port], 1_usize).await;
        // a master / port 7000 does not have YSQL rpcs, port 13000 has.
        assert!(allstoredconnections.stored_ysqlconnections.is_empty());
        // a master will have inbound RPCs, even RF=1 / 1 tserver.
        assert!(!allstoredconnections.stored_inboundrpcs.is_empty());
    }
    #[tokio::test]
    async fn integration_parse_rpcs_ysql() {
        let hostname = utility::get_hostname_ysql();
        let port = utility::get_port_ysql();
        let allstoredconnections = AllStoredConnections::read_connections(&vec![&hostname], &vec![&port], 1_usize).await;

        // ysql does have a single RPC connection by default after startup, which is the checkpointer process
        assert!(!allstoredconnections.stored_ysqlconnections.is_empty());
        // ysql does not have inbound RPCs
        assert!(allstoredconnections.stored_inboundrpcs.is_empty());
        // ysql does not have outbound RPCs
        assert!(allstoredconnections.stored_outboundrpcs.is_empty());
    }
}