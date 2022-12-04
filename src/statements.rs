//! The module for JSON statements from /statements endpoint, unique to YSQL.
//!
//! This endpoint has no prometheus format endpoint.
//!
//! The functionality for statements has 3 public entries:
//! 1. Snapshot creation: [AllStoredStatements::perform_snapshot]
//! 2. Snapshot diff: [SnapshotDiffBTreeMapStatements::snapshot_diff]
//! 3. Ad-hoc mode (diff): [SnapshotDiffBTreeMapStatements::adhoc_read_first_snapshot], [SnapshotDiffBTreeMapStatements::adhoc_read_second_snapshot], [SnapshotDiffBTreeMapStatements::print].
//!
//! # Snapshot creation
//! When a snapshot is created using the `--snapshot` option, this functionality is provided via the method [AllStoredStatements::perform_snapshot]
//!
//! 1. The snapshot is called via the [crate::perform_snapshot] function, which calls all snapshot functions for all data sources.
//! 2. For statements, this is done via the [AllStoredStatements::perform_snapshot] method. This method performs two calls.
//!
//!    * The method [AllStoredStatements::read_statements]
//!      * This method starts a rayon threadpool, and runs the general function [AllStoredStatements::read_http] for all host and port combinations.
//!        * [AllStoredStatements::read_http] calls [AllStoredStatements::parse_statements] to parse the http JSON output into a vector of [Statement].
//!        * The vector is iterated over and processed using the function [AllStoredStatements::add_and_sum_statements], which produces a struct [AllStoredStatements], which contains a vector [StoredStatements].
//!
//!    * The method [AllStoredStatements::save_snapshot]
//!      * This method takes the vector of [StoredStatements] inside the struct of [AllStoredStatements] and saves it to a CSV file in the numbered directory.
//!
//! # Snapshot diff
//! When a snapshot diff is requested via the `--snapshot-diff` option, this function is provided via the method [SnapshotDiffBTreeMapStatements::snapshot_diff].
//!
//! 1. Snapshot-diff is called directory in main, which calls the method [SnapshotDiffBTreeMapStatements::snapshot_diff].
//!
//!    * The method [AllStoredStatements::read_snapshot] is called to read all the data for an [AllStoredStatements] struct for the begin snapshot.
//!      * The method [SnapshotDiffBTreeMapStatements::first_snapshot] is called with [AllStoredStatements] as argument to insert the first snapshot data.
//!    * The method [AllStoredStatements::read_snapshot] is called to read all the data for an [AllStoredStatements] struct for the end snapshot.
//!      * The method [SnapshotDiffBTreeMapStatements::second_snapshot] is called with [AllStoredStatements] as argument to insert the second snapshot data.
//!    * The method [SnapshotDiffBTreeMapStatements::print] is called to print out the diff report from the [SnapshotDiffBTreeMapStatements] data.
//!
//! # Ad-hoc mode diff
//! When an ad-hoc diff is requested by not specifying an action on the `yb_stats` executable commandline, this is performed using three methods of [SnapshotDiffBTreeMapStatements].
//!
//! 1. [SnapshotDiffBTreeMapStatements::adhoc_read_first_snapshot]
//!
//!   * The method calls [AllStoredStatements::read_statements]
//!      * This method starts a rayon threadpool, and runs the general function [AllStoredStatements::read_http] for all host and port combinations.
//!        * [AllStoredStatements::read_http] calls [AllStoredStatements::parse_statements] to parse the http JSON output into a vector of [Statement].
//!        * The vector is iterated over and processed using the function [AllStoredStatements::add_and_sum_statements], which produces a struct [AllStoredStatements], which contains a vector [StoredStatements].
//!   * The method calls [SnapshotDiffBTreeMapStatements::first_snapshot] with [AllStoredStatements] as argument to insert the first snapshot data.
//!
//! 2. The user is asked for enter via stdin().read_line()
//!
//! 3. [SnapshotDiffBTreeMapStatements::adhoc_read_second_snapshot]
//!
//!   * The method calls [AllStoredStatements::read_statements]
//!      * This method starts a rayon threadpool, and runs the general function [AllStoredStatements::read_http] for all host and port combinations.
//!        * [AllStoredStatements::read_http] calls [AllStoredStatements::parse_statements] to parse the http JSON output into a vector of [Statement].
//!        * The vector is iterated over and processed using the function [AllStoredStatements::add_and_sum_statements], which produces a struct [AllStoredStatements], which contains a vector [StoredStatements].
//!   * The method calls [SnapshotDiffBTreeMapStatements::second_snapshot] with [AllStoredStatements] as argument to insert the second snapshot data.
//!
//! 4. [SnapshotDiffBTreeMapStatements::print]
//!
use port_scanner::scan_port_addr;
use chrono::{DateTime, Local};
use serde_derive::{Serialize,Deserialize};
use std::{fs, process, error::Error, collections::BTreeMap, env, sync::mpsc::channel, time::Instant};
use regex::Regex;
use substring::Substring;
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

impl UniqueStatementData {
    fn add_existing(row: &mut UniqueStatementData, statement: Queries) -> Self {
        Self {
            calls: row.calls + statement.calls,
            total_time: row.total_time + statement.total_time,
            min_time: 0.,
            max_time: 0.,
            mean_time: 0.,
            stddev_time: 0.,
            rows: row.rows + statement.rows,
        }
    }
    fn add_new(statement: Queries) -> Self {
        Self {
            calls: statement.calls,
            total_time: statement.total_time,
            min_time: statement.min_time,
            max_time: statement.max_time,
            mean_time: statement.mean_time,
            stddev_time: statement.stddev_time,
            rows: statement.rows,
        }
    }
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

impl StoredStatements {
    fn new(hostname: String, snapshot_time: DateTime<Local>, query: String, unique_statement_data: UniqueStatementData) -> Self {
        Self {
            hostname_port: hostname,
            timestamp: snapshot_time,
            query,
            calls: unique_statement_data.calls,
            total_time: unique_statement_data.total_time,
            min_time: unique_statement_data.min_time,
            max_time: unique_statement_data.max_time,
            mean_time: unique_statement_data.mean_time,
            stddev_time: unique_statement_data.stddev_time,
            rows:unique_statement_data.rows,
        }
    }
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

impl SnapshotDiffStatements {
    fn first_snapshot(statement: StoredStatements) -> Self {
        Self {
            first_snapshot_time: statement.timestamp,
            second_snapshot_time: statement.timestamp,
            first_calls: statement.calls,
            second_calls: 0,
            first_total_time: statement.total_time,
            second_total_time: 0.,
            first_rows: statement.rows,
            second_rows: 0
        }
    }
    fn second_snapshot_existing(statement: StoredStatements, statements_diff_row: &mut SnapshotDiffStatements) -> Self {
        Self {
            first_snapshot_time: statements_diff_row.first_snapshot_time,
            second_snapshot_time: statement.timestamp,
            first_calls: statements_diff_row.first_calls,
            second_calls: statement.calls,
            first_total_time: statements_diff_row.first_total_time,
            second_total_time: statement.total_time,
            first_rows: statements_diff_row.first_rows,
            second_rows: statement.rows,
        }
    }
    fn second_snapshot_new(statement: StoredStatements, first_snapshot_time: DateTime<Local>) -> Self {
        Self {
            first_snapshot_time,
            second_snapshot_time: statement.timestamp,
            first_calls: 0,
            second_calls: statement.calls,
            first_total_time: 0.,
            second_total_time: statement.total_time,
            first_rows: 0,
            second_rows: statement.rows,
        }
    }
}

type BTreeMapSnapshotDiffStatements = BTreeMap<(String, String), SnapshotDiffStatements>;

#[derive(Default)]
pub struct SnapshotDiffBTreeMapStatements {
    pub btreemap_snapshotdiff_statements: BTreeMapSnapshotDiffStatements,
}

impl SnapshotDiffBTreeMapStatements {
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
        begin_snapshot_time: &DateTime<Local>,
    ) -> SnapshotDiffBTreeMapStatements
    {
        let allstoredstatements = AllStoredStatements::read_snapshot(begin_snapshot)
            .unwrap_or_else(|e| {
                error!("Fatal: error reading snapshot: {}", e);
                process::exit(1);
            });
        let mut statements_snapshot_diff = SnapshotDiffBTreeMapStatements::new();
        statements_snapshot_diff.first_snapshot(allstoredstatements);

        let allstoredstatements = AllStoredStatements::read_snapshot(end_snapshot)
            .unwrap_or_else(|e| {
                error!("Fatal: error reading snapshot: {}", e);
                process::exit(1);
            });

        statements_snapshot_diff.second_snapshot(allstoredstatements, begin_snapshot_time);

        statements_snapshot_diff
    }
    pub fn new() -> Self {
        Default::default()
    }
    fn first_snapshot(
        &mut self,
        allstoredstatements: AllStoredStatements,
    )
    {
        for statement in allstoredstatements.stored_statements {
            self.btreemap_snapshotdiff_statements.insert(
                (statement.hostname_port.to_string(), statement.query.to_string()),
               SnapshotDiffStatements::first_snapshot(statement)
            );
        }
    }
    fn second_snapshot(
        &mut self,
        allstoredstatements: AllStoredStatements,
        first_snapshot_time: &DateTime<Local>,
    ) {
        for statement in allstoredstatements.stored_statements {
            match self.btreemap_snapshotdiff_statements.get_mut( &(statement.hostname_port.to_string(), statement.query.to_string()) ) {
                Some( statements_diff_row ) => {
                    *statements_diff_row = SnapshotDiffStatements::second_snapshot_existing(statement, statements_diff_row)
                },
                None => {
                    self.btreemap_snapshotdiff_statements.insert( (statement.hostname_port.to_string(), statement.query.to_string()),
                                            SnapshotDiffStatements::second_snapshot_new(statement, *first_snapshot_time)
                    );
                },
            }
        }
    }
    pub async fn adhoc_read_first_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allstoredstatements = AllStoredStatements::read_statements(hosts, ports, parallel).await;
        self.first_snapshot(allstoredstatements);
    }
    pub async fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
        first_snapshot_time: &DateTime<Local>,
    )
    {
        let allstoredstatements = AllStoredStatements::read_statements(hosts, ports, parallel).await;
        self.second_snapshot(allstoredstatements, first_snapshot_time);
    }
    pub async fn print(
        &self,
        hostname_filter: &Regex,
        sql_length: usize,
    )
    {
        for ((hostname, query), statements_row) in &self.btreemap_snapshotdiff_statements {
            if hostname_filter.is_match(hostname)
                && statements_row.second_calls - statements_row.first_calls != 0 {
                let adaptive_length = if query.len() < sql_length { query.len() } else { sql_length };
                trace!("PRINT {}: second_calls: {}, first_calls: {}, query: {}", hostname, statements_row.second_calls, statements_row.first_calls, query.substring(0,adaptive_length).escape_default());
                println!("{:20} {:10} avg: {:15.3} tot: {:15.3} ms avg: {:10} tot: {:10} rows: {:0adaptive_length$}",
                         hostname,
                         statements_row.second_calls - statements_row.first_calls,
                         (statements_row.second_total_time - statements_row.first_total_time) / (statements_row.second_calls as f64 - statements_row.first_calls as f64),
                         statements_row.second_total_time - statements_row.first_total_time as f64,
                         (statements_row.second_rows - statements_row.first_rows) / (statements_row.second_calls - statements_row.first_calls),
                         statements_row.second_rows - statements_row.first_rows,
                         query.substring(0, adaptive_length).escape_default()
                );
            } else {
                trace!("SKIP {}: second_calls: {}, first_calls: {}, query: {}", hostname, statements_row.second_calls, statements_row.first_calls, query.escape_default());
            }
        }
    }
}

#[derive(Debug)]
pub struct AllStoredStatements {
    pub stored_statements: Vec<StoredStatements>
}
impl AllStoredStatements {
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize
    ) {
        info!("begin snapshot");
        let timer = Instant::now();

        let allstoredstatements = AllStoredStatements::read_statements(hosts, ports, parallel);
        allstoredstatements.await.save_snapshot(snapshot_number)
            .unwrap_or_else(|e| {
                error!("error saving snapshot: {}", e);
                process::exit(1);
            });

        info!("end snapshot: {:?}", timer.elapsed())
    }
    fn save_snapshot ( self, snapshot_number: i32 ) -> Result<(), Box<dyn Error>>
    {
        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number.to_string());

        let statements_file = &current_snapshot_directory.join("statements");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(statements_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_statements {
            writer.serialize(row)?;
        }
        writer.flush()?;

        Ok(())
    }
    fn read_snapshot( snapshot_number: &String, ) -> Result<AllStoredStatements, Box<dyn Error>>
    {
        let mut allstoredstatements = AllStoredStatements { stored_statements: Vec::new() };

        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(snapshot_number);

        let statements_file = &current_snapshot_directory.join("statements");
        let file = fs::File::open(statements_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for  row in reader.deserialize() {
            let data: StoredStatements = row?;
            allstoredstatements.stored_statements.push(data);
        }

        Ok(allstoredstatements)
    }
    pub async fn read_statements (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize
    ) -> AllStoredStatements
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
                        let statements = AllStoredStatements::read_http(host, port);
                        tx.send((format!("{}:{}", host, port), detail_snapshot_time, statements)).expect("error sending data via tx (statements)");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allstoredstatements = AllStoredStatements { stored_statements: Vec::new() };
        for (hostname_port, detail_snapshot_time, statements) in rx {
            AllStoredStatements::add_and_sum_statements(statements, &hostname_port, detail_snapshot_time, &mut allstoredstatements);
        }
        allstoredstatements
    }
    pub fn add_and_sum_statements(
        statementdata: Statement,
        hostname: &str,
        snapshot_time: DateTime<Local>,
        allstoredstatements: &mut AllStoredStatements,
    ) {
        /*
         * This construction creates a BTreeMap in order to summarize the statements per YSQL endpoint.
         * The YSQL endpoint data does not carry the query_id field from pg_stat_statements to be able to distinguish between identical statement versions.
         * Therefore the statistics of identical queries are added.
         * Excluding min/max/mean/stddev: there is nothing mathematically correct that can be done with it.
         */
        let mut unique_statement: BTreeMap<(String, DateTime<Local>, String), UniqueStatementData> = BTreeMap::new();
        for statement in statementdata.statements {
            match unique_statement.get_mut(&(hostname.to_string().clone(), snapshot_time, statement.query.to_string())) {
                Some(row) => {
                    *row = UniqueStatementData::add_existing(row, statement)
                },
                None => {
                    unique_statement.insert((hostname.to_string(), snapshot_time, statement.query.to_string()),
                                            UniqueStatementData::add_new(statement)
                    );
                },
            }
        }
        for ((hostname, snapshot_time, query), unique_statement_data) in unique_statement {
            allstoredstatements.stored_statements.push( StoredStatements::new(hostname, snapshot_time, query, unique_statement_data) );
        }
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> Statement {
        if ! scan_port_addr( format!("{}:{}", host, port) ) {
            warn!("Warning: hostname:port {}:{} cannot be reached, skipping (statements)", host, port);
            return AllStoredStatements::parse_statements(String::from(""))
        }
        if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}:{}/statements", host, port)) {
            AllStoredStatements::parse_statements(data_from_http.text().unwrap())
        } else {
            AllStoredStatements::parse_statements(String::from(""))
        }
    }
    fn parse_statements( statements_data: String ) -> Statement {
        serde_json::from_str( &statements_data )
            .unwrap_or_else(|_e| {
                Statement { statements: Vec::<Queries>::new() }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_statements_simple() {
        // This is a very simple example of the statements json.
        // please mind the query_id which is added!
        let statements_json = r#"
{
    "statements":
    [
        {
            "query_id": -7776630665575107081,
            "query": "SELECT inhparent::pg_catalog.regclass,\n  pg_catalog.pg_get_expr(c.relpartbound, inhrelid)\nFROM pg_catalog.pg_class c JOIN pg_catalog.pg_inherits i ON c.oid = inhrelid\nWHERE c.oid = $1 AND c.relispartition",
            "calls": 1,
            "total_time": 0.60377,
            "min_time": 0.60377,
            "max_time": 0.60377,
            "mean_time": 0.60377,
            "stddev_time": 0.0,
            "rows": 0
        },
        {
            "query_id": 1950307723985771540,
            "query": "SELECT pubname\nFROM pg_catalog.pg_publication p\nJOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid\nWHERE pr.prrelid = $1\nUNION ALL\nSELECT pubname\nFROM pg_catalog.pg_publication p\nWHERE p.puballtables AND pg_catalog.pg_relation_is_publishable($2)\nORDER BY 1",
            "calls": 1,
            "total_time": 1.548288,
            "min_time": 1.548288,
            "max_time": 1.548288,
            "mean_time": 1.548288,
            "stddev_time": 0.0,
            "rows": 0
        },
        {
            "query_id": -874942831733610733,
            "query": "SELECT tg.grpname\nFROM yb_table_properties($1::regclass) props,\n   pg_yb_tablegroup tg\nWHERE tg.oid = props.tablegroup_oid",
            "calls": 2,
            "total_time": 29.426344,
            "min_time": 0.9091859999999999,
            "max_time": 28.517158000000003,
            "mean_time": 14.713172,
            "stddev_time": 13.803986000000002,
            "rows": 0
        }
    ]
}"#.to_string();
        let result = AllStoredStatements::parse_statements(statements_json);
        assert_eq!(result.statements.len(), 3);
    }
    #[test]
    fn unit_parse_statements_multiple_statements() {
        // This is a very simple example of the statements json.
        let statements_json = r#"
{
    "statements":
    [
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
            "query_id": -4118693658226780799,
            "query": "select $1",
            "calls": 1,
            "total_time": 0.019489,
            "min_time": 0.019489,
            "max_time": 0.019489,
            "mean_time": 0.019489,
            "stddev_time": 0.0,
            "rows": 1
        },
        {
            "query_id": -4118693658226780799,
            "query": "select $1",
            "calls": 1,
            "total_time": 0.023414,
            "min_time": 0.023414,
            "max_time": 0.023414,
            "mean_time": 0.023414,
            "stddev_time": 0.0,
            "rows": 1
        }
    ]
}"#.to_string();
        let mut allstoredstatements = AllStoredStatements { stored_statements: Vec::new() };
        let result = AllStoredStatements::parse_statements(statements_json);
        AllStoredStatements::add_and_sum_statements(result, "localhost", Local::now(), &mut allstoredstatements);
        //add_to_statements_vector(result, "localhost", Local::now(), &mut stored_statements);
        // with the new way of adding up all relevant statistics, we still should have 2 statements
        assert_eq!(allstoredstatements.stored_statements.len(), 2);
        // the second statement, being the select 1 should have a total number of calls of 2
        assert_eq!(allstoredstatements.stored_statements[0].query, "select $1");
        // the call count should be 2
        assert_eq!(allstoredstatements.stored_statements[0].calls, 2);
        // the min_time should be 0., because these can be two totally different statements
        assert_eq!(allstoredstatements.stored_statements[0].min_time, 0.);
    }

    use crate::utility;

    #[test]
    fn integration_parse_statements_ysql() {
        let mut allstoredstatements = AllStoredStatements { stored_statements: Vec::new() };
        let hostname = utility::get_hostname_ysql();
        let port = utility::get_port_ysql();

        let result = AllStoredStatements::read_http(hostname.as_str(), port.as_str());
        AllStoredStatements::add_and_sum_statements(result, &hostname, Local::now(), &mut allstoredstatements);
        // likely in a test scenario, there are no SQL commands executed, and thus no rows are returned.
        // to make sure this test works in both the scenario of no statements, and with statements, perform no assertion.
    }
}