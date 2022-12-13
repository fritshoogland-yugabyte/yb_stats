//! Utilities
use port_scanner::scan_port_addr;
use log::*;

use crate::ACCEPT_INVALID_CERTS;

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

