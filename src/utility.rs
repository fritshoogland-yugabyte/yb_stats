//! Utilities
use std::env;
use port_scanner::scan_port_addr;
use log::*;

use crate::ACCEPT_INVALID_CERTS;

#[allow(dead_code)]
pub fn get_hostname_master() -> String {
    match env::var("HOSTNAME_MASTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_MASTER should be set") },
    }
}
#[allow(dead_code)]
pub fn get_port_master() -> String {
    match env::var("PORT_MASTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_MASTER should be set") },
    }
}
#[allow(dead_code)]
pub fn get_hostname_tserver() -> String {
    match env::var("HOSTNAME_TSERVER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_TSERVER should be set") },
    }
}
#[allow(dead_code)]
pub fn get_port_tserver() -> String {
    match env::var("PORT_TSERVER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_TSERVER should be set") },
    }
}
#[allow(dead_code)]
pub fn get_hostname_ysql() -> String {
    match env::var("HOSTNAME_YSQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_YSQL should be set") },
    }
}
#[allow(dead_code)]
pub fn get_port_ysql() -> String {
    match env::var("PORT_YSQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_YSQL should be set") },
    }
}
#[allow(dead_code)]
pub fn get_hostname_ycql() -> String {
    match env::var("HOSTNAME_YCQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_YCQL should be set") },
    }
}
#[allow(dead_code)]
pub fn get_port_ycql() -> String {
    match env::var("PORT_YCQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_YCQL should be set") },
    }
}
#[allow(dead_code)]
pub fn get_hostname_yedis() -> String {
    match env::var("HOSTNAME_YEDIS") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_YEDIS should be set") },
    }
}
#[allow(dead_code)]
pub fn get_port_yedis() -> String {
    match env::var("PORT_YEDIS") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_YEDIS should be set") },
    }
}
#[allow(dead_code)]
pub fn get_hostname_node_exporter() -> String {
    match env::var("HOSTNAME_NODE_EXPORTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_NODE_EXPORTER should be set") },
    }
}
#[allow(dead_code)]
pub fn get_port_node_exporter() -> String {
    match env::var("PORT_NODE_EXPORTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_NODE_EXPORTER should be set") },
    }
}

pub fn scan_host_port(
    host: &str,
    port: &str,
) -> bool
{
    if ! scan_port_addr( format!("{}:{}", host, port)) {
        warn!("Port scanner: hostname:port {}:{} cannot be reached, skipping",host ,port);
        false
    } else {
        true
    }
}

pub fn http_get(
    host: &str,
    port: &str,
    url: &str,
) -> String
{
    if let Ok(data_from_web_request) = reqwest::blocking::Client::builder()
        .danger_accept_invalid_certs(ACCEPT_INVALID_CERTS)
        .build()
        .unwrap()
        .get(format!("http://{}:{}/{}", host, port, url))
        .send()
    {
        if ! &data_from_web_request.status().is_success()
        {
            debug!("Non success response: {}:{}/{} = {}", host, port, url, &data_from_web_request.status());
        }
        else
        {
           debug!("Success response: {}:{}/{} = {}", host, port, url, &data_from_web_request.status());
        }
        data_from_web_request.text().unwrap()
    } else {
        debug!("Non-Ok success response: {}:{}/{}", host, port, url);
        String::new()
    }
}

