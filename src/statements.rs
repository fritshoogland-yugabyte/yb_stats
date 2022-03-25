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
use scoped_threadpool::Pool;
use std::sync::mpsc::channel;

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

fn read_statements( hostname: &str) -> Statement {
    if ! scan_port_addr( hostname ) {
        println!("Warning: hostname:port {} cannot be reached, skipping", hostname.to_string());
        return parse_statements(String::from(""))
    }
    if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}/statements", hostname.to_string())) {
        parse_statements(data_from_http.text().unwrap())
    } else {
        parse_statements(String::from(""))
    }
}

pub fn read_statements_into_vector(
    hostname_port_vec: &Vec<&str>,
    parallel: &u32
) -> Vec<StoredStatements>
{
    let mut pool = Pool::new(*parallel);
    let (tx, rx) = channel();
    pool.scoped(|scope| {
        for hostname_port in hostname_port_vec {
            let tx = tx.clone();
            scope.execute(move || {
                let detail_snapshot_time = Local::now();
                let statements = read_statements(&hostname_port);
                tx.send((hostname_port, detail_snapshot_time, statements)).expect("channel will be waiting in the pool");
            });
        }
    });
    drop(tx);
    let mut stored_statements: Vec<StoredStatements> = Vec::new();
    for (hostname_port, detail_snapshot_time, statements) in rx {
        add_to_statements_vector(statements, hostname_port, detail_snapshot_time, &mut stored_statements);
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
    hostname_port_vec: &Vec<&str>,
    snapshot_number: i32,
    yb_stats_directory: &PathBuf,
    parallel: &u32
) {
   let stored_statements = read_statements_into_vector(hostname_port_vec, &parallel);

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
    for statement in statementdata.statements {
        stored_statements.push( StoredStatements {
            hostname_port: hostname.to_string(),
            timestamp: snapshot_time,
            query: statement.query.to_string(),
            calls: statement.calls,
            total_time: statement.total_time,
            min_time: statement.min_time,
            max_time: statement.max_time,
            mean_time: statement.mean_time,
            stddev_time: statement.stddev_time,
            rows: statement.rows
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
) {
    for ((hostname, query), statements_row) in statements_diff {
        if hostname_filter.is_match(&hostname)
            && statements_row.second_calls - statements_row.first_calls != 0 {
            let adaptive_length = if query.len() < 50 { query.len() } else { 50 };
            println!("{:20} {:10} avg: {:15.3} tot: {:15.3} ms avg: {:10} tot: {:10} rows: {:50}",
                     hostname,
                     statements_row.second_calls - statements_row.first_calls,
                     (statements_row.second_total_time - statements_row.first_total_time) / (statements_row.second_calls as f64 - statements_row.first_calls as f64),
                     statements_row.second_total_time - statements_row.first_total_time as f64,
                     (statements_row.second_rows - statements_row.first_rows) / (statements_row.second_calls - statements_row.first_calls),
                     statements_row.second_rows - statements_row.first_rows,
                     query.substring(0, adaptive_length).replace("\n", "")
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
) {
    let current_directory = env::current_dir().unwrap();
    let yb_stats_directory = current_directory.join("yb_stats.snapshots");

    // read begin_snapshot statements and load into statements_diff
    let stored_statements: Vec<StoredStatements> = read_statements_snapshot(&begin_snapshot, &yb_stats_directory);
    let mut statements_diff = insert_first_snapshot_statements(stored_statements);
    // read end_snapshot statements and load into statements_diff
    let stored_statements: Vec<StoredStatements> = read_statements_snapshot(&end_snapshot, &yb_stats_directory);
    insert_second_snapshot_statements(stored_statements, &mut statements_diff, &begin_snapshot_timestamp);

    print_diff_statements(&statements_diff, &hostname_filter);
}

#[allow(dead_code)]
pub fn get_statements_into_diff_first_snapshot(
    hostname_port_vec: &Vec<&str>,
    parallel: &u32
) -> BTreeMap<(String, String), SnapshotDiffStatements> {
    let stored_statements = read_statements_into_vector(&hostname_port_vec, &parallel);
    let statements_diff = insert_first_snapshot_statements(stored_statements);
    statements_diff
}

#[allow(dead_code)]
pub fn get_statements_into_diff_second_snapshot(
    hostname_port_vec: &Vec<&str>,
    statements_diff: &mut BTreeMap<(String, String), SnapshotDiffStatements>,
    first_snapshot_time: &DateTime<Local>,
    parallel: &u32
) {
    let stored_statements = read_statements_into_vector(&hostname_port_vec, &parallel);
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
}