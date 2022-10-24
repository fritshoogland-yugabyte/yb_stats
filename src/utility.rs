//! Utilities
use std::env;

#[allow(dead_code)]
pub fn get_hostname_master() -> String {
    match env::var("HOSTNAME_MASTER") {
        Ok(value) => value,
        Err(_e) => {String::from("")},
    }
}
#[allow(dead_code)]
pub fn get_port_master() -> String {
    match env::var("PORT_MASTER") {
        Ok(value) => value,
        Err(_e) => {String::from("")},
    }
}
#[allow(dead_code)]
pub fn get_hostname_tserver() -> String {
    match env::var("HOSTNAME_TSERVER") {
        Ok(value) => value,
        Err(_e) => {String::from("")},
    }
}
#[allow(dead_code)]
pub fn get_port_tserver() -> String {
    match env::var("PORT_TSERVER") {
        Ok(value) => value,
        Err(_e) => {String::from("")},
    }
}
#[allow(dead_code)]
pub fn get_hostname_ysql() -> String {
    match env::var("HOSTNAME_YSQL") {
        Ok(value) => value,
        Err(_e) => {String::from("")},
    }
}
#[allow(dead_code)]
pub fn get_port_ysql() -> String {
    match env::var("PORT_YSQL") {
        Ok(value) => value,
        Err(_e) => {String::from("")},
    }
}
#[allow(dead_code)]
pub fn get_hostname_ycql() -> String {
    match env::var("HOSTNAME_YCQL") {
        Ok(value) => value,
        Err(_e) => {String::from("")},
    }
}
#[allow(dead_code)]
pub fn get_port_ycql() -> String {
    match env::var("PORT_YCQL") {
        Ok(value) => value,
        Err(_e) => {String::from("")},
    }
}
#[allow(dead_code)]
pub fn get_hostname_yedis() -> String {
    match env::var("HOSTNAME_YEDIS") {
        Ok(value) => value,
        Err(_e) => {String::from("")},
    }
}
#[allow(dead_code)]
pub fn get_port_yedis() -> String {
    match env::var("PORT_YEDIS") {
        Ok(value) => value,
        Err(_e) => {String::from("")},
    }
}
#[allow(dead_code)]
pub fn get_hostname_node_exporter() -> String {
    match env::var("HOSTNAME_NODE_EXPORTER") {
        Ok(value) => value,
        Err(_e) => {String::from("")},
    }
}
#[allow(dead_code)]
pub fn get_port_node_exporter() -> String {
    match env::var("PORT_NODE_EXPORTER") {
        Ok(value) => value,
        Err(_e) => {String::from("")},
    }
}