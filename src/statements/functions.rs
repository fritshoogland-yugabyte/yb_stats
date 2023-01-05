//! The the impls and functions.
//!
use chrono::{DateTime, Local};
use std::{sync::mpsc::channel, time::Instant};
use regex::Regex;
use substring::Substring;
use log::*;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::statements::{Statements, AllStatements, StatementsDiff, GroupedStatements};

impl AllStatements {
    pub fn new() -> Self { Default::default() }
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allstatements = AllStatements::read_statements(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "statements", allstatements.statements)?;

        info!("end snapshot: {:?}", timer.elapsed());
        Ok(())
    }
    pub async fn read_statements (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize
    ) -> AllStatements
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
                        let mut statements = AllStatements::read_http(host, port);
                        statements.timestamp = Some(detail_snapshot_time);
                        statements.hostname_port = Some(format!("{}:{}", host, port));
                        tx.send(statements).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allstatements = AllStatements::new();

        for statements in rx
        {
            if statements.statements.len() > 0
            {
                allstatements.statements.push(statements);
            }
        }

        allstatements
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> Statements
    {
        let data_from_http = utility::http_get(host, port, "statements");
        AllStatements::parse_statements(data_from_http, host, port)
    }
    fn parse_statements(
        http_data: String,
        host: &str,
        port: &str,
    ) -> Statements {
        serde_json::from_str( &http_data )
            .unwrap_or_else(|e| {
                debug!("({}:{}) could not parse /statements json data, error: {}", host, port, e);
                Statements::default()
            })
    }
    // there is no print function
}

impl StatementsDiff {
    pub fn new() -> Self { Default::default() }
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
        begin_snapshot_time: &DateTime<Local>,
    ) -> Result<StatementsDiff>
    {
        let mut statementsdiff = StatementsDiff::new();

        let mut allstatements = AllStatements::new();
        allstatements.statements = snapshot::read_snapshot_json(begin_snapshot, "statements")?;
        statementsdiff.first_snapshot(allstatements);

        let mut allstatements = AllStatements::new();
        allstatements.statements = snapshot::read_snapshot_json(end_snapshot, "statements")?;
        statementsdiff.second_snapshot(allstatements, begin_snapshot_time);

        Ok(statementsdiff)
    }
    fn first_snapshot(
        &mut self,
        allstatements: AllStatements,
    )
    {
        for statements in allstatements.statements
        {
            for statement in statements.statements
            {
                self.btreestatementsdiff
                    .entry((statements.hostname_port.clone().expect("hostname:port should be set"), statement.query))
                    .and_modify(|statementdiff| {
                        statementdiff.first_calls += statement.calls;
                        statementdiff.first_total_time += statement.total_time;
                        statementdiff.first_rows += statement.rows;
                    })
                    .or_insert( GroupedStatements {
                        first_snapshot_time: statements.timestamp.expect("timestamp should be set"),
                        first_calls: statement.calls,
                        first_total_time: statement.total_time,
                        first_rows: statement.rows,
                        ..Default::default()
                    });

            }
        }
    }
    fn second_snapshot(
        &mut self,
        allstatements: AllStatements,
        begin_snapshot_time: &DateTime<Local>,
    )
    {
        for statements in allstatements.statements
        {
            for statement in statements.statements
            {
                self.btreestatementsdiff
                    .entry((statements.hostname_port.clone().expect("hostname:port should be set"), statement.query))
                    .and_modify(|statementdiff| {
                        statementdiff.second_calls += statement.calls;
                        statementdiff.second_total_time += statement.total_time;
                        statementdiff.second_rows += statement.rows;
                    })
                    .or_insert( GroupedStatements {
                        first_snapshot_time: *begin_snapshot_time,
                        second_snapshot_time: statements.timestamp.expect("timestamp should be set"),
                        second_calls: statement.calls,
                        second_total_time: statement.total_time,
                        second_rows: statement.rows,
                        ..Default::default()
                    });
            }
        }
    }
    pub async fn print(
        &self,
        hostname_filter: &Regex,
        sql_length: usize,
    )
    {
        for ((hostname, query), statements_row) in &self.btreestatementsdiff
        {
            if hostname_filter.is_match(hostname)
                && statements_row.second_calls - statements_row.first_calls != 0
            {
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
    pub async fn adhoc_read_first_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    )
    {
        let allstatements = AllStatements::read_statements(hosts, ports, parallel).await;
        self.first_snapshot(allstatements);
    }
    pub async fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
        first_snapshot_time: &DateTime<Local>,
    )
    {
        let allstatements = AllStatements::read_statements(hosts, ports, parallel).await;
        self.second_snapshot(allstatements, first_snapshot_time);
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
        let result = AllStatements::parse_statements(statements_json, "", "");
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
        let result = AllStatements::parse_statements(statements_json, "", "");
        // with the new way of adding up all relevant statistics, we still should have 2 statements
        assert_eq!(result.statements.len(), 3);
        // the second statement, being the select 1 should have a total number of calls of 2
        assert_eq!(result.statements[0].query, "select count(*) from ybio1.benchmark_table");
        // the call count should be 1
        assert_eq!(result.statements[0].calls, 1);
        // the min_time should be 0., because these can be two totally different statements
        assert_eq!(result.statements[0].min_time, 13.76067);
    }

    #[tokio::test]
    async fn integration_parse_statements_ysql() {
        let hostname = utility::get_hostname_ysql();
        let port = utility::get_port_ysql();

        let _allstoredstatements = AllStatements::read_statements(&vec![&hostname], &vec![&port], 1).await;
        // likely in a test scenario, there are no SQL commands executed, and thus no rows are returned.
        // to make sure this test works in both the scenario of no statements, and with statements, perform no assertion.
    }
}