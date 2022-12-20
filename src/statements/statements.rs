//! The module for JSON statements from /statements endpoint, unique to YSQL.
//!
use chrono::{DateTime, Local};
//use serde_derive::{Serialize,Deserialize};
use std::{collections::BTreeMap, sync::mpsc::channel, time::Instant};
use regex::Regex;
use substring::Substring;
use log::*;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::statements::{Statement, StoredStatements, AllStoredStatements, UniqueStatementData, Queries, SnapshotDiffStatements, SnapshotDiffBTreeMapStatements};

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


impl SnapshotDiffBTreeMapStatements {
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
        begin_snapshot_time: &DateTime<Local>,
    ) -> Result<SnapshotDiffBTreeMapStatements>
    {
        let mut allstoredstatements = AllStoredStatements::new();
        allstoredstatements.stored_statements = snapshot::read_snapshot(begin_snapshot, "statements")?;
        let mut statements_snapshot_diff = SnapshotDiffBTreeMapStatements::new();
        statements_snapshot_diff.first_snapshot(allstoredstatements);

        let mut allstoredstatements = AllStoredStatements::new();
        allstoredstatements.stored_statements = snapshot::read_snapshot(end_snapshot, "statements")?;

        statements_snapshot_diff.second_snapshot(allstoredstatements, begin_snapshot_time);

        Ok(statements_snapshot_diff)
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
                         statements_row.second_total_time - statements_row.first_total_time,
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

impl AllStoredStatements {
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allstoredstatements = AllStoredStatements::read_statements(hosts, ports, parallel).await;
        snapshot::save_snapshot(snapshot_number, "statements", allstoredstatements.stored_statements)?;

        info!("end snapshot: {:?}", timer.elapsed());
        Ok(())
    }
    pub fn new() -> Self {
        Default::default()
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

        let mut allstoredstatements = AllStoredStatements::new();

        for (hostname_port, detail_snapshot_time, statements) in rx {
            allstoredstatements.add_and_sum_statements(statements, &hostname_port, detail_snapshot_time);
        }
        allstoredstatements
    }
    pub fn add_and_sum_statements(
        &mut self,
        statementdata: Statement,
        hostname: &str,
        snapshot_time: DateTime<Local>,
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
                    //*row = UniqueStatementData::add_existing(row, statement)
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
            self.stored_statements.push( StoredStatements::new(hostname, snapshot_time, query, unique_statement_data) );
        }
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> Statement
    {
        let data_from_http = if utility::scan_host_port( host, port) {
            utility::http_get(host, port, "statements")
        } else {
            String::new()
        };
        AllStoredStatements::parse_statements(data_from_http)
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
    //use crate::utility_test::*;

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
        allstoredstatements.add_and_sum_statements(result, "localhost", Local::now());
        //AllStoredStatements::add_and_sum_statements(result, "localhost", Local::now(), &mut allstoredstatements);
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

    #[tokio::test]
    async fn integration_parse_statements_ysql() {
        let hostname = utility::get_hostname_ysql();
        let port = utility::get_port_ysql();

        let _allstoredstatements = AllStoredStatements::read_statements(&vec![&hostname], &vec![&port], 1).await;
        // likely in a test scenario, there are no SQL commands executed, and thus no rows are returned.
        // to make sure this test works in both the scenario of no statements, and with statements, perform no assertion.


    }
}