//! The impls and functions
//!
use chrono::Local;
use std::{fmt, sync::mpsc::channel, collections::BTreeMap, time::Instant};
use log::*;
use regex::Regex;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::rpcs::{Rpcs, AllRpcs, CQLCallDetailsPB, RpcConnectionDetailsPB, RpcCallState, RequestHeader, StateType, CqlConnectionDetails, RemoteMethodPB};
use crate::rpcs::Rpcs::{Ysql, Rpc};
use crate::Opts;

impl fmt::Display for RpcCallState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl fmt::Display for StateType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl AllRpcs {
    pub fn new() -> Self { Default::default() }
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allrpcs = AllRpcs::read_rpcs(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "rpcs", allrpcs.rpcs)?;

        info!("end snapshot: {:?}", timer.elapsed());
        Ok(())
    }
    pub async fn read_rpcs(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllRpcs
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
                        let mut rpcs = AllRpcs::read_http(host, port);
                        match rpcs
                        {
                            Ysql { ref mut hostname_port, ref mut timestamp, .. } => {
                                *hostname_port = Some(format!("{}:{}", host, port));
                                *timestamp = Some(detail_snapshot_time);
                            }
                            Rpc { ref mut hostname_port, ref mut timestamp, .. } => {
                                *hostname_port = Some(format!("{}:{}", host, port));
                                *timestamp = Some(detail_snapshot_time);
                            }
                            _ => {}
                        }
                        tx.send(rpcs).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allrpcs = AllRpcs::new();

        for rpcs in rx
        {
            allrpcs.rpcs.push(rpcs);
        }

        allrpcs
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> Rpcs
    {
        let data_from_http = utility::http_get(host, port, "rpcz");
        AllRpcs::parse_rpcs(data_from_http, host, port)
    }
    fn parse_rpcs(
        http_data: String,
        host: &str,
        port: &str,
    ) -> Rpcs {
        serde_json::from_str(&http_data).unwrap_or_else(|e| {
            debug!("Could not parse {}:{}/rpcz, error: {}", host, port, e);
            Rpcs::Empty {}
        })
    }
    pub fn print(
        &self,
        details_enable: &bool,
        hostname_filter: &Regex,
    ) -> Result<()>
    {
        let mut endpoint_count: BTreeMap<String, (usize, usize)> = BTreeMap::new();
        for rpcs in &self.rpcs {
            match rpcs
            {
                Ysql { connections, hostname_port, .. } =>
                    {
                        for connection in connections
                        {
                            // active inbound ysql
                            if connection.backend_status == "active"
                            {
                                endpoint_count
                                    .entry(hostname_port.clone().expect("hostname:port should be set"))
                                    .and_modify(|(active, total)| {
                                        *active += 1;
                                        *total += 1
                                    })
                                    .or_insert((1, 1));
                            }
                            // inactive inbound ysql
                            else {
                                endpoint_count
                                    .entry(hostname_port.clone().expect("hostname:port should be set"))
                                    .and_modify(|(_, total)| { *total += 1 })
                                    .or_insert((0, 1));
                            }
                        }
                    }
                Rpc { inbound_connections, outbound_connections, hostname_port, .. } =>
                    {
                        for inbound in inbound_connections.as_ref().unwrap_or(&Vec::new())
                        {
                            // active cql, always inbound
                            if inbound.calls_in_flight
                                .as_ref()
                                .unwrap_or(&Vec::new())
                                .iter()
                                .any(|calls_in_flight| calls_in_flight.cql_details.is_some())
                            {
                                endpoint_count
                                    .entry(hostname_port.clone().expect("hostname:port should be set"))
                                    .and_modify(|(active, total)| {
                                        *active += 1;
                                        *total += 1
                                    })
                                    .or_insert((1, 1));
                            }
                            // active inbound rpc
                            else if inbound.calls_in_flight
                                .as_ref()
                                .unwrap_or(&Vec::new())
                                .iter()
                                .any(|calls_in_flight| calls_in_flight.header.is_some())
                            {
                                endpoint_count
                                    .entry(hostname_port.clone().expect("hostname:port should be set"))
                                    .and_modify(|(active, total)| {
                                        *active += 1;
                                        *total += 1
                                    })
                                    .or_insert((1, 1));
                            }
                            // inactive inbound rpc
                            else {
                                endpoint_count
                                    .entry(hostname_port.clone().expect("hostname:port should be set"))
                                    .and_modify(|(_, total)| { *total += 1 })
                                    .or_insert((0, 1));
                            };
                        };
                        for outbound in outbound_connections.as_ref().unwrap_or(&Vec::new())
                        {
                            // active outbound rpc
                            if outbound.calls_in_flight
                                .as_ref()
                                .unwrap_or(&Vec::new())
                                .iter()
                                .any(|calls_in_flight| calls_in_flight.header.is_some())
                            {
                                endpoint_count
                                    .entry(hostname_port.clone().expect("hostname:port should be set"))
                                    .and_modify(|(active, total)| {
                                        *active += 1;
                                        *total += 1
                                    })
                                    .or_insert((1, 1));
                            }
                            // active cql, should always be inbound
                            if outbound.calls_in_flight
                                .as_ref()
                                .unwrap_or(&Vec::new())
                                .iter()
                                .any(|calls_in_flight| calls_in_flight.cql_details.is_some())
                            {
                                error!("Found outbound calls_in_flight.cql_details?")
                            }
                            // inactive outbound rpc
                            else {
                                endpoint_count
                                    .entry(hostname_port.clone().expect("hostname:port should be set"))
                                    .and_modify(|(_, total)| { *total += 1 })
                                    .or_insert((0, 1));
                            }
                        }
                    }
                _ => {}
            }
            debug!("{:#?}", rpcs);
        }
        let mut previous_hostname = String::new();
        for (endpoint, (active, inactive)) in &endpoint_count {
            if hostname_filter.is_match(endpoint) {
                let current_hostname = &endpoint.split(':').next().unwrap();
                let current_port = &endpoint.split(':').nth(1).unwrap();
                if *current_hostname != previous_hostname {
                    println!("\n{}", "-".repeat(120));
                    if !current_port.is_empty() {
                        self.print_details(previous_hostname, details_enable, hostname_filter);
                    }

                    print!("{}", current_hostname);
                    previous_hostname = current_hostname.to_string();
                }
                print!("; port: {}, {}/{} act/tot", current_port, active, inactive);
            };
        }
        println!("\n{}", "-".repeat(120));
        self.print_details(previous_hostname, details_enable, hostname_filter);


        Ok(())
    }
    fn print_details(
        &self,
        hostname: String,
        details_enable: &bool,
        hostname_filter: &Regex,
    )
    {
        let mut activity_counter = 0;
        for rpcs in &self.rpcs
        {
            match rpcs
            {
                Ysql { connections, hostname_port, .. } =>
                    {
                        if hostname_port.is_none() { continue };
                        if hostname_port
                            .clone()
                            .expect("hostname:port should be set")
                            .split(':')
                            .next()
                            .unwrap() == hostname
                            && hostname_filter.is_match(&hostname_port.clone().expect("hostname:port should be set"))
                        {
                            for connection in connections
                            {
                                // active inbound ysql
                                if connection.backend_status == "active"
                                {
                                    println!("{:30}<-{:30} {:6} {:5} {:>6} ms db:{}, q:{}",
                                             hostname_port
                                                 .clone()
                                                 .expect("hostname:port should be set"),
                                             format!("{}:{}",
                                                     connection.host.as_ref().unwrap_or(&"".to_string()),
                                                     &connection.port.as_ref().unwrap_or(&"".to_string())
                                             ),
                                             connection.backend_status,
                                             "",
                                             connection.query_running_for_ms
                                                 .unwrap_or_default(),
                                             connection.db_name
                                                 .as_ref()
                                                 .unwrap_or(&"".to_string()),
                                             connection.query
                                                 .as_ref()
                                                 .unwrap_or(&"".to_string()),
                                    );
                                    activity_counter += 1;
                                }
                                // inactive inbound ysql
                                else if *details_enable
                                {
                                    let remote_host = if connection.host
                                        .clone()
                                        .unwrap_or_default()
                                        .is_empty()
                                        && connection.port
                                        .clone()
                                        .unwrap_or_default()
                                        .is_empty()
                                    {
                                        format!("background:{}", connection.backend_type.clone())
                                    }
                                    else
                                    {
                                        format!("{}:{}",
                                             connection.host
                                                 .clone()
                                                 .unwrap_or_default(),
                                             connection.port
                                                 .clone()
                                                 .unwrap_or_default(),
                                        )
                                    };
                                    println!("{:30}<-{:30} {:6} {:5} {:>6} ms db:{}, q:{}",
                                             hostname_port
                                                 .clone()
                                                 .expect("hostname:port should be set"),
                                             remote_host,
                                             connection.backend_status.clone(),
                                             "",
                                             connection.query_running_for_ms
                                                 .unwrap_or_default(),
                                             connection.db_name
                                                 .as_ref()
                                                 .unwrap_or(&"".to_string()),
                                             connection.query
                                                 .as_ref()
                                                 .unwrap_or(&"".to_string()),
                                    );
                                    activity_counter += 1;
                                }
                            }
                        }
                    }
                Rpc { inbound_connections, outbound_connections, hostname_port, .. } =>
                    {
                        if hostname_port.is_none() { continue };
                        if hostname_port
                            .clone()
                            .expect("hostname:port should be set")
                            .split(':')
                            .next()
                            .unwrap() == hostname
                            && hostname_filter.is_match(&hostname_port.clone().expect("hostname:port should be set"))
                        {
                            for inbound in inbound_connections
                                .as_ref()
                                .unwrap_or(&Vec::new())
                            {
                                // for both YCQL and RPC inbound requests, active requests are identified by having 'calls_in_flight'.
                                // for YCQL, calls_in_flight.cql_details has information.
                                // for RPC, calls_in_flight.header is information.
                                for (calls_in_flight_counter, calls_in_flight) in inbound.calls_in_flight
                                    .as_ref()
                                    .unwrap_or(&Vec::new())
                                    .iter()
                                    .enumerate()
                                {
                                    for call_details in &calls_in_flight.cql_details
                                        .as_ref()
                                        .unwrap_or(&CQLCallDetailsPB::default())
                                        .call_details
                                    {
                                        // this is an active inbound YCQL
                                        // special case of call type: BATCH: count the number of call_details, and do not print all of them.
                                        let call_type = if calls_in_flight.cql_details
                                            .as_ref()
                                            .unwrap_or(&CQLCallDetailsPB::default())
                                            .call_type
                                            .as_ref()
                                            .unwrap_or(&"".to_string()) == "BATCH"
                                        {
                                            format!("BATCH ({})", calls_in_flight.cql_details.as_ref().unwrap_or(&CQLCallDetailsPB::default()).call_details.len())
                                        }
                                        else
                                        {
                                            // otherwise, just print the call type.
                                            calls_in_flight.cql_details
                                                .as_ref()
                                                .unwrap_or(&CQLCallDetailsPB::default())
                                                .call_type
                                                .as_ref()
                                                .unwrap_or(&"".to_string()).to_string()
                                        };
                                        // first call in flight to be printed gets the full details
                                        if calls_in_flight_counter == 0
                                        {
                                            print!("{:30}<-{:30} {:6} #{:>6} ",
                                                   hostname_port
                                                       .clone()
                                                       .expect("hostname:port should be set"),
                                                   inbound.remote_ip,
                                                   inbound.state,
                                                   inbound.processed_call_count
                                                       .unwrap_or_default(),
                                            );
                                        }
                                        else
                                        {
                                            // others get spaces. this allows to see the multiple calls in flight
                                            print!("{:75} ", "");
                                        }
                                        println!("{:>6} ms {:17} ks:{}, q: {}",
                                                 calls_in_flight.elapsed_millis
                                                     .unwrap_or_default(),
                                                 call_type,
                                                 inbound.connection_details
                                                     .as_ref()
                                                     .unwrap_or(&RpcConnectionDetailsPB::default())
                                                     .cql_connection_details
                                                     .as_ref()
                                                     .unwrap_or(&CqlConnectionDetails::default())
                                                     .keyspace
                                                     .as_ref()
                                                     .unwrap_or(&"".to_string()),
                                                 call_details.sql_string
                                                     .as_ref()
                                                     .unwrap_or(&"".to_string()),
                                        );
                                        activity_counter += 1;
                                        if calls_in_flight.cql_details
                                            .as_ref()
                                            .unwrap_or(&CQLCallDetailsPB::default())
                                            .call_type
                                            .as_ref()
                                            .unwrap_or(&"".to_string()) == "BATCH"
                                        {
                                            break;
                                        }
                                    }
                                    // if header has entries, it is an active inbound RPC
                                    if calls_in_flight.header.is_some()
                                    {
                                        // first call in flight to be printed gets the full details
                                        if calls_in_flight_counter == 0
                                        {
                                            print!("{:30}<-{:30} {:6} #{:>6} ",
                                                   hostname_port
                                                       .clone()
                                                       .expect("hostname:port should be set"),
                                                   inbound.remote_ip,
                                                   inbound.state,
                                                   inbound.processed_call_count
                                                       .unwrap_or_default(),
                                            );
                                        }
                                        else
                                        {
                                            // others get spaces. this allows to see the multiple calls in flight
                                            print!("{:75} ", "");
                                        }
                                        println!("{:>6} ms {:17} {}:{}  (timeout: {} ms)",
                                            calls_in_flight.elapsed_millis
                                               .unwrap_or_default(),
                                            calls_in_flight.state
                                               .as_ref()
                                               .unwrap_or(&RpcCallState::default()),
                                            calls_in_flight.header
                                               .as_ref()
                                               .unwrap_or(&RequestHeader::default())
                                               .remote_method
                                               .as_ref()
                                               .unwrap_or(&RemoteMethodPB::default())
                                               .service_name,
                                            calls_in_flight.header
                                               .as_ref()
                                               .unwrap_or(&RequestHeader::default())
                                               .remote_method
                                               .as_ref()
                                               .unwrap_or(&RemoteMethodPB::default())
                                               .method_name,
                                            calls_in_flight.header
                                               .as_ref()
                                               .unwrap_or(&RequestHeader::default())
                                               .timeout_millis
                                               .unwrap_or_default(),
                                        );
                                        activity_counter += 1;
                                    }
                                }
                                // inactive inbound connections.
                                // inactive connections do not have calls in flight.
                                if inbound.calls_in_flight.is_none()
                                    && *details_enable
                                {
                                    println!("{:30}<-{:30} {:6} #{:>6}",
                                             hostname_port
                                                 .clone()
                                                 .expect("hostname:port should be set"),
                                             inbound.remote_ip,
                                             inbound.state,
                                             inbound.processed_call_count
                                                 .unwrap_or_default(),
                                    );
                                    activity_counter += 1;

                                }
                            }
                            for outbound in outbound_connections
                                .as_ref()
                                .unwrap_or(&Vec::new())
                            {
                                for (calls_in_flight_counter, calls_in_flight) in outbound.calls_in_flight
                                    .as_ref()
                                    .unwrap_or(&Vec::new())
                                    .iter()
                                    .enumerate()
                                {
                                    if calls_in_flight.header.is_some()
                                    {
                                        // first call in flight to be printed gets the full details
                                        if calls_in_flight_counter == 0
                                        {
                                            print!("{:30}->{:30} {:6} #{:>6} ",
                                                   hostname_port
                                                       .clone()
                                                       .expect("hostname:port should be set"),
                                                   outbound.remote_ip,
                                                   outbound.state,
                                                   outbound.processed_call_count
                                                       .unwrap_or_default(),
                                            );
                                        }
                                        else
                                        {
                                            // others get spaces. this allows to see the multiple calls in flight
                                            print!("{:75} ", "");
                                        }
                                        println!("{:>6} ms {:17} {}:{}  (timeout: {} ms)",
                                                 calls_in_flight.elapsed_millis
                                                     .unwrap_or_default(),
                                                 calls_in_flight.state
                                                     .as_ref()
                                                     .unwrap_or(&RpcCallState::default()),
                                                 calls_in_flight.header
                                                     .as_ref()
                                                     .unwrap_or(&RequestHeader::default())
                                                     .remote_method
                                                     .as_ref()
                                                     .unwrap_or(&RemoteMethodPB::default())
                                                     .service_name,
                                                 calls_in_flight.header
                                                     .as_ref()
                                                     .unwrap_or(&RequestHeader::default())
                                                     .remote_method
                                                     .as_ref()
                                                     .unwrap_or(&RemoteMethodPB::default())
                                                     .method_name,
                                                 calls_in_flight.header
                                                     .as_ref()
                                                     .unwrap_or(&RequestHeader::default())
                                                     .timeout_millis
                                                     .unwrap_or_default(),
                                        );
                                        activity_counter += 1;
                                    }
                                    if calls_in_flight.cql_details.is_some()
                                    {
                                        error!("Found outbound calls_in_flight.cql_details?");
                                    }
                                }
                                // inactive outbound connections.
                                // inactive connections do not have calls in flight.
                                if outbound.calls_in_flight.is_none()
                                    && *details_enable
                                {
                                    println!("{:30}->{:30} {:6} #{:>6}",
                                             hostname_port
                                                 .clone()
                                                 .expect("hostname:port should be set"),
                                             outbound.remote_ip,
                                             outbound.state,
                                             outbound.processed_call_count
                                                 .unwrap_or_default(),
                                    );
                                    activity_counter += 1;
                                }
                            }
                        }
                    }
                _ => {}
            }
        }
        if activity_counter > 0
        {
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
    match options.print_rpcs
        .as_ref()
        .unwrap()
    {
        Some(snapshot_number) =>
        {
            let mut allrpcs = AllRpcs::new();
            allrpcs.rpcs = snapshot::read_snapshot_json(snapshot_number, "rpcs")?;
            allrpcs.print(&options.details_enable, &hostname_filter)?;
        }
        None =>
        {
            let allrpcs = AllRpcs::read_rpcs(&hosts, &ports, parallel).await;
            allrpcs.print(&options.details_enable, &hostname_filter)?;
        }
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
        let result = AllRpcs::parse_rpcs(json, "", "");
        if let Ysql { connections,.. } = result
        {
                assert_eq!(connections[0].process_start_time, "2022-08-11 10:06:23.639902+00");
                assert_eq!(connections[0].application_name, "");
                assert_eq!(connections[0].backend_type, "checkpointer");
                assert_eq!(connections[0].backend_status, "");
        }
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
        let result = AllRpcs::parse_rpcs(json, "", "");
        if let Ysql { connections, .. } = result
        {
                    assert_eq!(connections[0].db_oid.unwrap_or_default(), 13285);
                    assert_eq!(connections[0].db_name.clone().unwrap_or_default(), "yugabyte");
                    assert_eq!(connections[0].query.clone().unwrap_or_default(), "insert into t select x from generate_series(1,100000000) x;");
                    assert_eq!(connections[0].query_start_time.clone().unwrap_or_default(), "2022-08-12 14:20:38.366499+00");
                    assert_eq!(connections[0].query_running_for_ms.unwrap_or_default(), 1824);
        }
    }

    #[test]
    fn unit_parse_inboundrpc_idle_ycql() {
         // This is how a simple, inactive simple connection via ycqlsh looks like.
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
        let result = AllRpcs::parse_rpcs(json, "", "");
        if let Rpc { inbound_connections, .. } = result
                {
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].remote_ip, "127.0.0.1:33086");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].state, StateType::OPEN);
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].processed_call_count.unwrap_or_default(), 2);
                    assert_eq!(inbound_connections.as_ref().unwrap()[1].remote_ip, "127.0.0.1:33084");
                    assert_eq!(inbound_connections.as_ref().unwrap()[1].state, StateType::OPEN);
                    assert_eq!(inbound_connections.as_ref().unwrap()[1].processed_call_count.unwrap_or_default(), 13);
                }
    }

    #[test]
    fn unit_parse_inboundrpc_active_query_ycql() {
         // This is how an active query via ycqlsh looks like.
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
        let result = AllRpcs::parse_rpcs(json, "", "");

        if let Rpc { inbound_connections, .. } = result
        {
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].remote_ip, "127.0.0.1:35518");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].state, StateType::OPEN);
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].processed_call_count.unwrap_or_default(), 20);
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].connection_details.as_ref().unwrap().cql_connection_details.as_ref().unwrap().keyspace.as_ref().unwrap(), "cr");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].calls_in_flight.as_ref().unwrap()[0].cql_details.as_ref().unwrap().call_type.as_ref().unwrap(), "QUERY");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].calls_in_flight.as_ref().unwrap()[0].cql_details.as_ref().unwrap().call_details[0].sql_string.as_ref().unwrap(), "select avg(permit), avg(permit_recheck), avg( handgun), avg( long_gun), avg( other), avg( multiple), avg( admin), avg( prepawn_handgun), avg( prepawn_long_gun), avg( prepawn_other), avg( redemption_handgun), avg( redemption_long_gun), avg( redemption_other), avg( returned_handgun), avg( returned_long_gun), avg( returned_other), avg( rentals_handgun), avg( rentals_long_gun), avg( private_sale_handgun), avg( private_sale_long_gun), avg( private_sale_other), avg( return_to_seller_handgun), avg( return_to_seller_long_gun), avg( return_to_seller_other), avg( totals) from fa_bg_checks;");
        }
    }

     // Please mind clippy remains on complaining about invisible characters despite  #[allow(clippy::invisible_characters)]
     // I know there are invisible characters in the params, but this is an actual sample of the params.
    #[test]
    #[allow(clippy::invisible_characters)]
    fn unit_parse_inboundrpc_active_batch_query_ycql() {
         // This is how an active batch query via ycqlsh looks like.
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
        let result = AllRpcs::parse_rpcs(json, "", "");

        if let Rpc { inbound_connections, .. } = result
                {
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].remote_ip, "127.0.0.1:35692");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].state, StateType::OPEN);
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].processed_call_count.unwrap_or_default(), 135);
                    assert_eq!(inbound_connections.as_ref().unwrap()[0]
                                   .connection_details.as_ref().unwrap()
                                   .cql_connection_details.as_ref().unwrap()
                                   .keyspace.as_ref().unwrap(), "cr");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .cql_details.as_ref().unwrap()
                                   .call_type.as_ref().unwrap(), "BATCH");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .cql_details.as_ref().unwrap()
                                   .call_details[0]
                                   .sql_id.as_ref().unwrap(), "344cf13216c84b621b82d4c212f04b0a");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .cql_details.as_ref().unwrap()
                                   .call_details[0]
                                   .sql_string.as_ref().unwrap(), "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .cql_details.as_ref().unwrap()
                                   .call_details[0]
                                   .params.as_ref().unwrap(), "[2008-06, Alabama, \0\0\0\0, n/a, \0\0\u{1c},, \0\0\u{1c}\u{1c}, n/a, \0\0\u{1}B, \0\0\0\0, \0\0\0\t, \0\0\0\u{b}, n/a, \0\0\u{5}\u{f}, \0\0\u{5}\u{81}, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \0\0D.]");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .cql_details.as_ref().unwrap()
                                   .call_details[19]
                                   .sql_id.as_ref().unwrap(), "344cf13216c84b621b82d4c212f04b0a");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .cql_details.as_ref().unwrap()
                                   .call_details[19]
                                   .sql_string.as_ref().unwrap(), "INSERT INTO cr.fa_bg_checks (year_month, state, permit, permit_recheck, handgun, long_gun, other, multiple, admin, prepawn_handgun, prepawn_long_gun, prepawn_other, redemption_handgun, redemption_long_gun, redemption_other, returned_handgun, returned_long_gun, returned_other, rentals_handgun, rentals_long_gun, private_sale_handgun, private_sale_long_gun, private_sale_other, return_to_seller_handgun, return_to_seller_long_gun, return_to_seller_other, totals) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .cql_details.as_ref().unwrap()
                                   .call_details[19]
                                   .params.as_ref().unwrap(), "[2008-06, Louisiana, \0\0\0\0, n/a, \0\0\u{19}\u{00cd}, \0\0\u{14}L, n/a, \0\0\0\u{00da}, \0\0\0\0, \0\0\0\u{5}, \0\0\0\u{3}, n/a, \0\0\u{2}\u{0192}, \0\0\u{3}@, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, n/a, \0\04\u{00be}]");
                }
    }

    #[test]
    fn unit_parse_inboundrpc_and_outboundrpc_simple_tabletserver() {
         // This is the simple, most usual form of inbound and outbound connections for tablet server and master.
         // The entries can have a more verbose form when they are active.
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
        let result = AllRpcs::parse_rpcs(json, "", "");
        if let Rpc { inbound_connections, outbound_connections, .. } = result
        {
            assert_eq!(inbound_connections.as_ref().unwrap()[0].remote_ip, "172.158.40.206:38776");
            assert_eq!(inbound_connections.as_ref().unwrap()[0].state, StateType::OPEN);
            assert_eq!(inbound_connections.as_ref().unwrap()[0].processed_call_count.unwrap_or_default(), 314238);
            assert_eq!(outbound_connections.as_ref().unwrap()[0].remote_ip, "172.158.40.206:9100");
            assert_eq!(outbound_connections.as_ref().unwrap()[0].state, StateType::OPEN);
            assert_eq!(outbound_connections.as_ref().unwrap()[0].processed_call_count.unwrap_or_default(), 316390);
            assert_eq!(outbound_connections.as_ref().unwrap()[0].sending_bytes.unwrap_or_default(), 0);
        }
    }

    #[test]
    fn unit_parse_outboundrpc_simple_tabletserver() {
         // This is the simple, most usual form of outbound connections for tablet server and master.
         // The entries can have a more verbose form when they are active.
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
        let result = AllRpcs::parse_rpcs(json, "", "");
        if let Rpc { outbound_connections, .. } = result
        {
                    assert_eq!(outbound_connections.as_ref().unwrap()[0].remote_ip, "192.168.66.80:7100");
                    assert_eq!(outbound_connections.as_ref().unwrap()[0].state, StateType::OPEN);
                    assert_eq!(outbound_connections.as_ref().unwrap()[0].processed_call_count.unwrap_or_default(), 3526);
                    assert_eq!(outbound_connections.as_ref().unwrap()[0].sending_bytes.unwrap_or_default(), 0);
        }
    }

    #[test]
    fn unit_parse_inboundrpc_only_simple_tabletserver() {
         // This is the simple, most usual form of inbound connections for tablet server and master.
         // The entries can have a more verbose form when they are active.
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
        let result = AllRpcs::parse_rpcs(json, "", "");
        if let Rpc { inbound_connections, .. } = result
        {
            assert_eq!(inbound_connections.as_ref().unwrap()[0].remote_ip, "172.158.40.206:38776");
            assert_eq!(inbound_connections.as_ref().unwrap()[0].state, StateType::OPEN);
            assert_eq!(inbound_connections.as_ref().unwrap()[0].processed_call_count.unwrap_or_default(), 314238);
        }
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
        let result = AllRpcs::parse_rpcs(json, "", "");

        if let Rpc { inbound_connections, outbound_connections, .. } = result
                {
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].remote_ip, "192.168.66.82:51316");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].state, StateType::OPEN);
                    assert_eq!(inbound_connections.as_ref().unwrap()[0].processed_call_count.unwrap_or_default(), 74);
                    assert_eq!(inbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .header.as_ref().unwrap()
                                   .call_id.unwrap_or_default(), 3160);
                    assert_eq!(inbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .header.as_ref().unwrap()
                                   .remote_method.as_ref().unwrap()
                                   .service_name.clone(), "yb.consensus.ConsensusService");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .header.as_ref().unwrap()
                                   .remote_method.as_ref().unwrap()
                                   .method_name.clone(), "UpdateConsensus");
                    assert_eq!(inbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .header.as_ref().unwrap()
                                   .timeout_millis.unwrap_or_default(), 3000);
                    assert_eq!(inbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .elapsed_millis.unwrap_or_default(), 6);

                    assert_eq!(outbound_connections.as_ref().unwrap()[0].remote_ip, "172.158.40.206:9100");
                    assert_eq!(outbound_connections.as_ref().unwrap()[0].state, StateType::OPEN);
                    assert_eq!(outbound_connections.as_ref().unwrap()[0].processed_call_count.unwrap_or_default(), 316390);
                    assert_eq!(outbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .header.as_ref().unwrap()
                                   .call_id.unwrap_or_default(), 4595);
                    assert_eq!(outbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .header.as_ref().unwrap()
                                   .remote_method.as_ref().unwrap()
                                   .service_name.clone(), "yb.tserver.TabletServerService");
                    assert_eq!(outbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .header.as_ref().unwrap()
                                   .remote_method.as_ref().unwrap()
                                   .method_name.clone(), "Write");
                    assert_eq!(outbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .header.as_ref().unwrap()
                                   .timeout_millis.unwrap_or_default(), 119994);
                    assert_eq!(outbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .elapsed_millis.unwrap_or_default(), 84);
                    assert_eq!(outbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[0]
                                   .state.as_ref().unwrap(), &RpcCallState::SENT);
                    assert_eq!(outbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[1]
                                   .header.as_ref().unwrap()
                                   .call_id.unwrap_or_default(), 4615);
                    assert_eq!(outbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[1]
                                   .header.as_ref().unwrap()
                                   .remote_method.as_ref().unwrap()
                                   .service_name.clone(), "yb.tserver.TabletServerService");
                    assert_eq!(outbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[1]
                                   .header.as_ref().unwrap()
                                   .remote_method.as_ref().unwrap()
                                   .method_name.clone(), "Write");
                    assert_eq!(outbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[1]
                                   .header.as_ref().unwrap()
                                   .timeout_millis.unwrap_or_default(), 119991);
                    assert_eq!(outbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[1]
                                   .elapsed_millis.unwrap_or_default(), 7);
                    assert_eq!(outbound_connections.as_ref().unwrap()[0]
                                   .calls_in_flight.as_ref().unwrap()[1]
                                   .state.as_ref().unwrap(), &RpcCallState::SENT);
                }
    }

    #[test]
    fn unit_parse_inboundrpc_and_outboundrpc_master_server() {
         // It turns out the master can have a different format with lesser JSON fields used.
         // The below outbound_connections JSON document is an example of that.
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
        let result = AllRpcs::parse_rpcs(json, "", "");
        if let Rpc { inbound_connections, outbound_connections, .. } = result
        {
            assert_eq!(inbound_connections.as_ref().unwrap()[0].remote_ip, "192.168.66.80:53856");
            assert_eq!(inbound_connections.as_ref().unwrap()[0].state, StateType::OPEN);
            assert_eq!(inbound_connections.as_ref().unwrap()[0].processed_call_count.unwrap_or_default(), 186);

            assert_eq!(outbound_connections.as_ref().unwrap()[0].remote_ip, "192.168.66.80:7100");
        }
    }

    #[tokio::test]
    async fn integration_parse_rpcs_tserver() {
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();
        let allrpcs = AllRpcs::read_rpcs(&vec![&hostname], &vec![&port], 1_usize).await;
        for rpcs in allrpcs.rpcs {
            match rpcs {
                Ysql { connections, .. } =>
                    {
                        // a tserver / port 9000 does not have YSQL rpcs, port 13000 has.
                        assert!(connections.is_empty())
                    }
                Rpc { outbound_connections, .. } =>
                    {
                        // a tserver will have inbound RPCs, even RF=1 / 1 tserver. NOPE, no inbound RPCS for tserver with RF1
                        //assert!(!inbound_connections.is_empty());
                        // a tserver will have outbound RPCs, even RF=1 / 1 tserver.
                        assert!(!outbound_connections.unwrap().is_empty());
                    }
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn integration_parse_rpcs_master() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();
        let allrpcs = AllRpcs::read_rpcs(&vec![&hostname], &vec![&port], 1_usize).await;

        for rpcs in allrpcs.rpcs {
            match rpcs {
                Ysql { connections, .. } =>
                    {
                        // a master / port 7000 does not have YSQL rpcs, port 13000 has.
                        assert!(connections.is_empty())
                    }
                Rpc { inbound_connections, .. } =>
                    {
                        // a master will have inbound RPCs, even RF=1 / 1 tserver.
                        assert!(!inbound_connections.unwrap().is_empty());
                        //assert!(!outbound_connections.is_empty());
                    }
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn integration_parse_rpcs_ysql() {
        let hostname = utility::get_hostname_ysql();
        let port = utility::get_port_ysql();
        let allrpcs = AllRpcs::read_rpcs(&vec![&hostname], &vec![&port], 1_usize).await;

        for rpcs in allrpcs.rpcs {
            match rpcs {
                Ysql { connections, .. } =>
                    {
                        // ysql does have a single RPC connection by default after startup, which is the checkpointer process
                        assert!(!connections.is_empty())
                    }
                Rpc { inbound_connections, outbound_connections, .. } =>
                    {
                        // ysql does not have inbound RPCs
                        assert!(inbound_connections.unwrap().is_empty());
                        // ysql does not have outbound RPCs
                        assert!(outbound_connections.unwrap().is_empty());
                    }
                _ => {}
            }
        }
    }
}