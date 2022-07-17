use port_scanner::scan_port_addr;
use chrono::{DateTime, Local};
use serde_derive::{Serialize,Deserialize};
use std::fs;
use std::process;
use std::path::PathBuf;
use std::collections::BTreeMap;
use regex::Regex;
use substring::Substring;
use std::env;
use rayon;
use std::sync::mpsc::channel;
use log::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct Statement {
    pub statements: Vec<Queries>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Queries {
    pub query: String,
    pub calls: i64,
    pub total_time: f64,
    pub min_time: f64,
    pub max_time: f64,
    pub mean_time: f64,
    pub stddev_time: f64,
    pub rows: i64,
}

#[derive(Debug)]
struct UniqueStatementData {
    pub calls: i64,
    pub total_time: f64,
    pub min_time: f64,
    pub max_time: f64,
    pub mean_time: f64,
    pub stddev_time: f64,
    pub rows: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredStatements {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub query: String,
    pub calls: i64,
    pub total_time: f64,
    pub min_time: f64,
    pub max_time: f64,
    pub mean_time: f64,
    pub stddev_time: f64,
    pub rows: i64,
}

#[derive(Debug)]
pub struct SnapshotDiffStatements {
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub first_calls: i64,
    pub second_calls: i64,
    pub first_total_time: f64,
    pub second_total_time: f64,
    pub first_rows: i64,
    pub second_rows: i64,
}

pub fn read_statements(
    host: &str,
    port: &str,
) -> Statement {
    if ! scan_port_addr( format!("{}:{}", host, port) ) {
        println!("Warning: hostname:port {}:{} cannot be reached, skipping (statements)", host, port);
        return parse_statements(String::from(""))
    }
    if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}:{}/statements", host, port)) {
        parse_statements(data_from_http.text().unwrap())
    } else {
        parse_statements(String::from(""))
    }
}

pub fn read_statements_into_vector(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    parallel: usize
) -> Vec<StoredStatements>
{
    let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
    let (tx, rx) = channel();
    pool.scope(move |s| {
        for host in hosts {
            for port in ports {
                let tx = tx.clone();
                s.spawn(move |_| {
                    let detail_snapshot_time = Local::now();
                    let statements = read_statements(&host, &port);
                    tx.send((format!("{}:{}", host, port), detail_snapshot_time, statements)).expect("error sending data via tx (statements)");
                });
            }
        }
    });
    let mut stored_statements: Vec<StoredStatements> = Vec::new();
    for (hostname_port, detail_snapshot_time, statements) in rx {
        add_to_statements_vector(statements, &hostname_port, detail_snapshot_time, &mut stored_statements);
    }
    stored_statements
}

fn parse_statements( statements_data: String ) -> Statement {
    serde_json::from_str( &statements_data )
        .unwrap_or_else(|_e| {
            return Statement { statements: Vec::<Queries>::new() };
        })
}

#[allow(dead_code)]
pub fn perform_statements_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    snapshot_number: i32,
    yb_stats_directory: &PathBuf,
    parallel: usize
) {
    info!("perform_statements_snapshot");
    let stored_statements = read_statements_into_vector(hosts, ports, parallel);

    let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
    let statements_file = &current_snapshot_directory.join("statements");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&statements_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing statements data in snapshot directory {}: {}", &statements_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_statements {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();
}

pub fn add_to_statements_vector(
    statementdata: Statement,
    hostname: &str,
    snapshot_time: DateTime<Local>,
    stored_statements: &mut Vec<StoredStatements>
) {
    let mut unique_statement: BTreeMap<(String, DateTime<Local>, String), UniqueStatementData> = BTreeMap::new();
    for statement in statementdata.statements {
        match unique_statement.get_mut(&(hostname.to_string().clone(), snapshot_time, statement.query.to_string())) {
            Some(row) => {
                *row = UniqueStatementData {
                    calls: row.calls + statement.calls,
                    total_time: row.total_time + statement.total_time,
                    min_time: 0.,
                    max_time: 0.,
                    mean_time: 0.,
                    stddev_time: 0.,
                    rows: row.rows + statement.rows,
                }
            },
            None => {
                unique_statement.insert((hostname.to_string(), snapshot_time, statement.query.to_string()), UniqueStatementData {
                    calls: statement.calls,
                    total_time: statement.total_time,
                    min_time: statement.min_time,
                    max_time: statement.max_time,
                    mean_time: statement.mean_time,
                    stddev_time: statement.stddev_time,
                    rows: statement.calls,
                });
            },
        }
    }
    for ((sd_hostname, sd_snapshot_time, sd_query), sd_data) in unique_statement {
        stored_statements.push( StoredStatements {
            hostname_port: sd_hostname.to_string(),
            timestamp: sd_snapshot_time,
            query: sd_query.to_string(),
            calls: sd_data.calls,
            total_time: sd_data.total_time,
            min_time: sd_data.min_time,
            max_time: sd_data.max_time,
            mean_time: sd_data.mean_time,
            stddev_time: sd_data.stddev_time,
            rows: sd_data.rows,
        });
    }
}

#[allow(dead_code)]
pub fn read_statements_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf
) -> Vec<StoredStatements> {

    let mut stored_statements: Vec<StoredStatements> = Vec::new();
    let statements_file = &yb_stats_directory.join(&snapshot_number.to_string()).join("statements");
    let file = fs::File::open(&statements_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error reading file: {}: {}", &statements_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for  row in reader.deserialize() {
        let data: StoredStatements = row.unwrap();
        let _ = &stored_statements.push(data);
    }
    stored_statements
}

#[allow(dead_code)]
pub fn insert_first_snapshot_statements(
    stored_statements: Vec<StoredStatements>
) -> BTreeMap<(String, String), SnapshotDiffStatements>
{
    let mut statements_diff: BTreeMap<(String, String), SnapshotDiffStatements> = BTreeMap::new();
    for statement in stored_statements {
        statements_diff.insert( (statement.hostname_port.to_string(), statement.query.to_string()), SnapshotDiffStatements {
            first_snapshot_time: statement.timestamp,
            second_snapshot_time: statement.timestamp,
            first_calls: statement.calls,
            first_total_time: statement.total_time,
            first_rows: statement.rows,
            second_calls: 0,
            second_total_time: 0.0,
            second_rows: 0
        });
    }
    statements_diff
}

#[allow(dead_code)]
pub fn insert_second_snapshot_statements(
    stored_statements: Vec<StoredStatements>,
    statements_diff: &mut BTreeMap<(String, String), SnapshotDiffStatements>,
    first_snapshot_time: &DateTime<Local>
) {
    for statement in stored_statements {
        match statements_diff.get_mut( &(statement.hostname_port.to_string(), statement.query.to_string()) ) {
            Some( statements_diff_row ) => {
                *statements_diff_row = SnapshotDiffStatements {
                    first_snapshot_time: statements_diff_row.first_snapshot_time,
                    second_snapshot_time: statement.timestamp,
                    first_calls: statements_diff_row.first_calls,
                    second_calls: statement.calls,
                    first_total_time: statements_diff_row.first_total_time,
                    second_total_time: statement.total_time,
                    first_rows: statements_diff_row.first_rows,
                    second_rows: statement.rows
                }
            },
            None => {
                statements_diff.insert( (statement.hostname_port.to_string(), statement.query.to_string()), SnapshotDiffStatements {
                    first_snapshot_time: *first_snapshot_time,
                    second_snapshot_time: statement.timestamp,
                    first_calls: 0,
                    second_calls: statement.calls,
                    first_total_time: 0.0,
                    second_total_time: statement.total_time,
                    first_rows: 0,
                    second_rows: statement.rows
                });
            }
        }
    }
}

#[allow(dead_code)]
pub fn print_diff_statements(
    statements_diff: &BTreeMap<(String, String), SnapshotDiffStatements>,
    hostname_filter: &Regex,
    sql_length: usize,
) {
    for ((hostname, query), statements_row) in statements_diff {
        if hostname_filter.is_match(&hostname)
            && statements_row.second_calls - statements_row.first_calls != 0 {
            let adaptive_length = if query.len() < sql_length { query.len() } else { sql_length };
            println!("{:20} {:10} avg: {:15.3} tot: {:15.3} ms avg: {:10} tot: {:10} rows: {:0adaptive_length$}",
                     hostname,
                     statements_row.second_calls - statements_row.first_calls,
                     (statements_row.second_total_time - statements_row.first_total_time) / (statements_row.second_calls as f64 - statements_row.first_calls as f64),
                     statements_row.second_total_time - statements_row.first_total_time as f64,
                     (statements_row.second_rows - statements_row.first_rows) / (statements_row.second_calls - statements_row.first_calls),
                     statements_row.second_rows - statements_row.first_rows,
                     query.substring(0, adaptive_length).escape_default()
                     //query.substring(0, adaptive_length).replace("\n", "")
            );
        }
    }
}

#[allow(dead_code)]
pub fn print_statements_diff_for_snapshots(
    begin_snapshot: &String,
    end_snapshot: &String,
    begin_snapshot_timestamp: &DateTime<Local>,
    hostname_filter: &Regex,
    sql_length: usize,
) {
    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    // read begin_snapshot statements and load into statements_diff
    let stored_statements: Vec<StoredStatements> = read_statements_snapshot(&begin_snapshot, &yb_stats_directory);
    let mut statements_diff = insert_first_snapshot_statements(stored_statements);
    // read end_snapshot statements and load into statements_diff
    let stored_statements: Vec<StoredStatements> = read_statements_snapshot(&end_snapshot, &yb_stats_directory);
    insert_second_snapshot_statements(stored_statements, &mut statements_diff, &begin_snapshot_timestamp);

    print_diff_statements(&statements_diff, &hostname_filter, sql_length);
}

#[allow(dead_code)]
pub fn get_statements_into_diff_first_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    parallel: usize
) -> BTreeMap<(String, String), SnapshotDiffStatements> {
    let stored_statements = read_statements_into_vector(&hosts, &ports, parallel);
    let statements_diff = insert_first_snapshot_statements(stored_statements);
    statements_diff
}

#[allow(dead_code)]
pub fn get_statements_into_diff_second_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    statements_diff: &mut BTreeMap<(String, String), SnapshotDiffStatements>,
    first_snapshot_time: &DateTime<Local>,
    parallel: usize
) {
    let stored_statements = read_statements_into_vector(&hosts, &ports, parallel);
    insert_second_snapshot_statements(stored_statements, statements_diff, &first_snapshot_time);
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_statements_simple() {
        // This is a very simple example of the statements json.
        let statements_json = r#"{
    "statements": [
        {
            "query": "select count(*) from ybio1.benchmark_table",
            "calls": 1,
            "total_time": 13.76067,
            "min_time": 13.76067,
            "max_time": 13.76067,
            "mean_time": 13.76067,
            "stddev_time": 0.0,
            "rows": 1
        },
        {
            "query": "select $1+$2",
            "calls": 1,
            "total_time": 0.006206000000000001,
            "min_time": 0.006206000000000001,
            "max_time": 0.006206000000000001,
            "mean_time": 0.006206000000000001,
            "stddev_time": 0.0,
            "rows": 1
        }
    ]
}"#.to_string();
        let result = parse_statements(statements_json.clone());
        assert_eq!(result.statements.len(), 2);
    }
    #[test]
    fn parse_statements_multiple() {
        // This is a very simple example of the statements json.
        let statements_json = r#"{
    "statements": [
        {
            "query": "select count(*) from ybio1.benchmark_table",
            "calls": 1,
            "total_time": 13.76067,
            "min_time": 13.76067,
            "max_time": 13.76067,
            "mean_time": 13.76067,
            "stddev_time": 0.0,
            "rows": 1
        },
        {
            "query": "select $1+$2",
            "calls": 1,
            "total_time": 0.006206000000000001,
            "min_time": 0.006206000000000001,
            "max_time": 0.006206000000000001,
            "mean_time": 0.006206000000000001,
            "stddev_time": 0.0,
            "rows": 1
        },
        {
            "query": "select count(*) from ybio1.benchmark_table",
            "calls": 1,
            "total_time": 13.76067,
            "min_time": 13.76067,
            "max_time": 13.76067,
            "mean_time": 13.76067,
            "stddev_time": 0.0,
            "rows": 1
        }
    ]
}"#.to_string();
        let mut stored_statements: Vec<StoredStatements> = Vec::new();
        let result = parse_statements(statements_json.clone());
        add_to_statements_vector(result, "localhost", Local::now(), &mut stored_statements);
        // with the new way of adding up all relevant statistics, we still should have 2 statements
        assert_eq!(stored_statements.len(), 2);
        // the first statement, being the select count(*) should have a total number of calls of 2
        assert_eq!(stored_statements[1].query, "select count(*) from ybio1.benchmark_table");
        // the call count should be 2
        assert_eq!(stored_statements[1].calls, 2);
        // the min_time should be 0., because these can be two totally different statements
        assert_eq!(stored_statements[1].min_time, 0.);
    }
}