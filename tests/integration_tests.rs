use chrono::Local;
use std::env;

fn get_hostname_tserver() -> String {
    let hostname_port = match env::var("HOSTNAME_PORT_TSERVER") {
        Ok(value) => value,
        Err(e) => panic!("Error reading environment variable HOSTNAME_PORT_TSERVER: {:?}", e)
    };
    hostname_port
}
fn get_hostname_ysql() -> String {
    let hostname_port = match env::var("HOSTNAME_PORT_YSQL") {
        Ok(value) => value,
        Err(e) => panic!("Error reading environment variable HOSTNAME_PORT_YSQL: {:?}", e)
    };
    hostname_port
}
fn get_hostname_ycql() -> String {
    let hostname_port = match env::var("HOSTNAME_PORT_YCQL") {
        Ok(value) => value,
        Err(e) => panic!("Error reading environment variable HOSTNAME_PORT_YCQL: {:?}", e)
    };
    hostname_port
}
fn get_hostname_yedis() -> String {
    let hostname_port = match env::var("HOSTNAME_PORT_YEDIS") {
        Ok(value) => value,
        Err(e) => panic!("Error reading environment variable HOSTNAME_PORT_YEDIS: {:?}", e)
    };
    hostname_port
}
fn get_hostname_master() -> String {
    let hostname_port = match env::var("HOSTNAME_PORT_MASTER") {
        Ok(value) => value,
        Err(e) => panic!("Error reading environment variable HOSTNAME_PORT_MASTER: {:?}", e)
    };
    hostname_port
}

use yb_stats::gflags::{StoredGFlags, read_gflags, add_to_gflags_vector};
#[test]
fn parse_gflags_master() {
    let mut stored_gflags: Vec<StoredGFlags> = Vec::new();
    let detail_snapshot_time = Local::now();
    let hostname_port = get_hostname_master();

    let gflags = read_gflags(&hostname_port.as_str());
    add_to_gflags_vector(gflags, &hostname_port.as_str(), detail_snapshot_time, &mut stored_gflags);
}
#[test]
fn parse_gflags_tserver() {
    let mut stored_gflags: Vec<StoredGFlags> = Vec::new();
    let detail_snapshot_time = Local::now();
    let hostname_port = get_hostname_tserver();

    let gflags = read_gflags(&hostname_port.as_str());
    add_to_gflags_vector(gflags, &hostname_port.as_str(), detail_snapshot_time, &mut stored_gflags);
}

use yb_stats::memtrackers::{MemTrackers, StoredMemTrackers, read_memtrackers, add_to_memtrackers_vector};
#[test]
fn parse_memtrackers_master() {
    let mut stored_memtrackers: Vec<StoredMemTrackers> = Vec::new();
    let detail_snapshot_time = Local::now();
    let hostname_port = get_hostname_master();

    let memtrackers: Vec<MemTrackers> = read_memtrackers(&hostname_port.as_str());
    add_to_memtrackers_vector(memtrackers, &hostname_port.as_str(), detail_snapshot_time, &mut stored_memtrackers);
}
#[test]
fn parse_memtrackers_tserver() {
    let mut stored_memtrackers: Vec<StoredMemTrackers> = Vec::new();
    let detail_snapshot_time = Local::now();
    let hostname_port = get_hostname_tserver();

    let memtrackers: Vec<MemTrackers> = read_memtrackers(&hostname_port.as_str());
    add_to_memtrackers_vector(memtrackers, &hostname_port.as_str(), detail_snapshot_time, &mut stored_memtrackers);
}

use yb_stats::loglines::{StoredLogLines, read_loglines, add_to_loglines_vector};
#[test]
fn parse_loglines_master() {
    let mut stored_loglines: Vec<StoredLogLines> = Vec::new();
    let hostname_port = get_hostname_master();

    let loglines = read_loglines(&hostname_port.as_str());
    add_to_loglines_vector(loglines, &hostname_port.as_str(), &mut stored_loglines);
}
#[test]
fn parse_loglines_tserver() {
    let mut stored_loglines: Vec<StoredLogLines> = Vec::new();
    let hostname_port = get_hostname_tserver();

    let loglines = read_loglines(&hostname_port.as_str());
    add_to_loglines_vector(loglines, &hostname_port.as_str(), &mut stored_loglines);
}

use yb_stats::versions::{StoredVersionData, read_version, add_to_version_vector};
#[test]
fn parse_versiondata_master() {
    let mut stored_versiondata: Vec<StoredVersionData> = Vec::new();
    let detail_snapshot_time = Local::now();
    let hostname_port = get_hostname_master();

    let data_parsed_from_json = read_version(&hostname_port.as_str());
    add_to_version_vector(data_parsed_from_json, &hostname_port.as_str(), detail_snapshot_time, &mut stored_versiondata);
}
#[test]
fn parse_versiondata_tserver() {
    let mut stored_versiondata: Vec<StoredVersionData> = Vec::new();
    let detail_snapshot_time = Local::now();
    let hostname_port = get_hostname_tserver();

    let data_parsed_from_json = read_version(&hostname_port.as_str());
    add_to_version_vector(data_parsed_from_json, &hostname_port.as_str(), detail_snapshot_time, &mut stored_versiondata);
}

use yb_stats::statements::{StoredStatements, read_statements, add_to_statements_vector};
#[test]
fn parse_statements_ysql() {
    let mut stored_statements: Vec<StoredStatements> = Vec::new();
    let detail_snapshot_time = Local::now();
    let hostname_port = get_hostname_ysql();

    let data_parsed_from_json = read_statements(&hostname_port.as_str());
    add_to_statements_vector(data_parsed_from_json, &hostname_port.as_str(), detail_snapshot_time, &mut stored_statements);
}

use yb_stats::metrics::{StoredValues,StoredCountSum, StoredCountSumRows, read_metrics, add_to_metric_vectors};
#[test]
fn parse_metrics_master() {
    let mut stored_values: Vec<StoredValues> = Vec::new();
    let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
    let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
    let detail_snapshot_time = Local::now();
    let hostname_port = get_hostname_master();

    let data_parsed_from_json = read_metrics(&hostname_port.as_str());
    add_to_metric_vectors(data_parsed_from_json, &hostname_port.as_str(), detail_snapshot_time, &mut stored_values, &mut stored_countsum, &mut stored_countsumrows);
}
#[test]
fn parse_metrics_tserver() {
    let mut stored_values: Vec<StoredValues> = Vec::new();
    let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
    let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
    let detail_snapshot_time = Local::now();
    let hostname_port = get_hostname_tserver();

    let data_parsed_from_json = read_metrics(&hostname_port.as_str());
    add_to_metric_vectors(data_parsed_from_json, &hostname_port.as_str(), detail_snapshot_time, &mut stored_values, &mut stored_countsum, &mut stored_countsumrows);
}
#[test]
fn parse_metrics_ysql() {
    let mut stored_values: Vec<StoredValues> = Vec::new();
    let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
    let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
    let detail_snapshot_time = Local::now();
    let hostname_port = get_hostname_ysql();

    let data_parsed_from_json = read_metrics(&hostname_port.as_str());
    add_to_metric_vectors(data_parsed_from_json, &hostname_port.as_str(), detail_snapshot_time, &mut stored_values, &mut stored_countsum, &mut stored_countsumrows);
}
#[test]
fn parse_metrics_ycql() {
    let mut stored_values: Vec<StoredValues> = Vec::new();
    let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
    let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
    let detail_snapshot_time = Local::now();
    let hostname_port = get_hostname_ycql();

    let data_parsed_from_json = read_metrics(&hostname_port.as_str());
    add_to_metric_vectors(data_parsed_from_json, &hostname_port.as_str(), detail_snapshot_time, &mut stored_values, &mut stored_countsum, &mut stored_countsumrows);
}
#[test]
fn parse_metrics_yedis() {
    let mut stored_values: Vec<StoredValues> = Vec::new();
    let mut stored_countsum: Vec<StoredCountSum> = Vec::new();
    let mut stored_countsumrows: Vec<StoredCountSumRows> = Vec::new();
    let detail_snapshot_time = Local::now();
    let hostname_port = get_hostname_yedis();

    let data_parsed_from_json = read_metrics(&hostname_port.as_str());
    add_to_metric_vectors(data_parsed_from_json, &hostname_port.as_str(), detail_snapshot_time, &mut stored_values, &mut stored_countsum, &mut stored_countsumrows);
}
