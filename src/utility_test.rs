//! Utilities
use std::env;

pub fn get_hostname_master() -> String {
    match env::var("HOSTNAME_MASTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_MASTER should be set") },
    }
}
pub fn get_port_master() -> String {
    match env::var("PORT_MASTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_MASTER should be set") },
    }
}
pub fn get_hostname_tserver() -> String {
    match env::var("HOSTNAME_TSERVER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_TSERVER should be set") },
    }
}
pub fn get_port_tserver() -> String {
    match env::var("PORT_TSERVER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_TSERVER should be set") },
    }
}
pub fn get_hostname_ysql() -> String {
    match env::var("HOSTNAME_YSQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_YSQL should be set") },
    }
}
pub fn get_port_ysql() -> String {
    match env::var("PORT_YSQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_YSQL should be set") },
    }
}
pub fn get_hostname_ycql() -> String {
    match env::var("HOSTNAME_YCQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_YCQL should be set") },
    }
}
pub fn get_port_ycql() -> String {
    match env::var("PORT_YCQL") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_YCQL should be set") },
    }
}
pub fn get_hostname_yedis() -> String {
    match env::var("HOSTNAME_YEDIS") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_YEDIS should be set") },
    }
}
pub fn get_port_yedis() -> String {
    match env::var("PORT_YEDIS") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_YEDIS should be set") },
    }
}
pub fn get_hostname_node_exporter() -> String {
    match env::var("HOSTNAME_NODE_EXPORTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable HOSTNAME_NODE_EXPORTER should be set") },
    }
}
pub fn get_port_node_exporter() -> String {
    match env::var("PORT_NODE_EXPORTER") {
        Ok(value) => value,
        Err(_e) => { panic!("The environment variable PORT_NODE_EXPORTER should be set") },
    }
}