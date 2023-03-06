//! The impls and functions
//!
use chrono::Local;
use std::{sync::mpsc::channel, time::Instant};
use scraper::{Html, Selector};
use log::*;
use anyhow::Result;
use regex::Regex;
use crate::utility;
use crate::snapshot;
use crate::tablet_server_operations::{AllOperations, Operations, Operation};
use crate::Opts;

impl Operations {
    pub fn new() -> Self{ Default::default() }
}
impl AllOperations {
    pub fn new() -> Self { Default::default() }
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let alloperations = AllOperations::read_tablet_server_operations(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "tablet_server_operations", alloperations.operations)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub async fn read_tablet_server_operations (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllOperations
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
                        let mut operations = AllOperations::read_http(host, port);
                        operations.timestamp = Some(detail_snapshot_time);
                        operations.hostname_port = Some(format!("{}:{}", host, port));
                        tx.send(operations).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut alloperations = AllOperations::new();

        for operations in rx.iter().filter(|row| !row.tasks.is_empty())
        {
            alloperations.operations.push(operations);
        }

        alloperations
    }
    fn read_http(
        host: &str,
        port: &str,
    ) -> Operations
    {
        let data_from_http = utility::http_get(host, port, "operations");
        AllOperations::parse_tablet_server_operations(data_from_http)
    }
    fn parse_tablet_server_operations(
        http_data: String
    ) -> Operations
    {
        let table_selector = Selector::parse("table").unwrap();
        let tr_selector = Selector::parse("tr").unwrap();
        let th_selector = Selector::parse("th").unwrap();
        let div_selector = Selector::parse("div.yb-main").unwrap();

        let mut tasks = Operations::new();

        let html = Html::parse_document(&http_data);

        for div_select in html.select(&div_selector)
        {
            for table in div_select.select(&table_selector)
            {
                match table
                {
                    // Sole table: Transactions (? this was operations?)
                    th
                    if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Tablet id"
                            && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Op Id"
                            && th.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Transaction Type"
                            && th.select(&th_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Total time in-flight"
                            && th.select(&th_selector).nth(4).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Description" =>
                    {
                            for tr in table.select(&tr_selector).skip(1)
                            {
                                // Please mind all rows are th/table header rows here, I am not sure if this has been done intentionally.
                                // Normally this would be th/table data.
                                tasks.tasks.push( Some(Operation {
                                    tablet_id: tr.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    op_id: tr.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    transaction_type: tr.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    total_time_in_flight: tr.select(&th_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    description: tr.select(&th_selector).nth(4).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                }));
                            }
                    },
                    _ => {
                            info!("Found a table that doesn't match specified headings, this shouldn't happen: {:#?}", table.clone());
                    },
                }
            }
        }
        tasks
    }
    pub fn print(
        &self,
        hostname_filter: &Regex
    ) -> Result<()>
    {
        for operations in self.operations.iter().filter(|row| hostname_filter.is_match(row.hostname_port.as_ref().unwrap()))
        {
                for task in &operations.tasks
                {
                    println!("{:20} {:32} {:10} {:10} {:10} {:80}",
                        operations.hostname_port.as_ref().unwrap(),
                        task.as_ref().unwrap().tablet_id,
                        task.as_ref().unwrap().op_id,
                        task.as_ref().unwrap().transaction_type,
                        task.as_ref().unwrap().total_time_in_flight,
                        task.as_ref().unwrap().description.get(..80).unwrap_or(&task.as_ref().unwrap().description),
                    );
                }
        }
        Ok(())
    }
}

pub async fn print_operations(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    let hostname_filter = utility::set_regex(&options.hostname_match);
    match options.print_tablet_server_operations.as_ref().unwrap() {
        Some(snapshot_number) => {
            let mut alloperations = AllOperations::new();
            alloperations.operations = snapshot::read_snapshot_json(snapshot_number, "tablet_server_operations")?;
            alloperations.print(&hostname_filter)?;
        },
        None => {
            let alloperations = AllOperations::read_tablet_server_operations(&hosts, &ports, parallel).await;
            alloperations.print(&hostname_filter)?;
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_operations_empty_table() {
        let operations = r#"
    <div class='yb-main container-fluid'>
        <h1>Transactions</h1>
        <table class='table table-striped'>
            <tr>
                <th>Tablet id</th>
                <th>Op Id</th>
                <th>Transaction Type</th>
                <th>Total time in-flight</th>
                <th>Description</th>
            </tr>
        </table>
        <div class='yb-bottom-spacer'></div>
    </div>
        "#.to_string();
        let result = AllOperations::parse_tablet_server_operations(operations);

        assert_eq!(result.tasks.len(), 0);
    }
    #[test]
    fn unit_parse_operations_table_with_op() {
        let operations = r#"
    <div class='yb-main container-fluid'>
        <h1>Transactions</h1>
        <table class='table table-striped'>
            <tr>
                <th>Tablet id</th>
                <th>Op Id</th>
                <th>Transaction Type</th>
                <th>Total time in-flight</th>
                <th>Description</th>
            </tr>
            <tr>
                <th>baa5fc1db83248a88e95cd5004d93e57</th>
                <th>term: 1 index: 61</th>
                <th>WRITE_OP</th>
                <th>28836 us.</th>
                <th>R-P { type: kWrite consensus_round: 0x0000561fe9629300 -&gt; id { term: 1 index: 61 } hybrid_time: 6867885357922844672 op_type: WRITE_OP write { unused_tablet_id: "" write_batch { write_pairs { key: "478C9F488002C40121214A80" value: "$" } write_pairs { key: "478C9F488002C40121214B81" value: "Sxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" } write_pairs { key: "475EED488002C40221214A80" value: "$" } write_pairs { key: "475EED488002C40221214B81" value: "Sxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" } write_pairs { key: "47DA0D488002C40321214A80" value: "$" } write_pairs { key: "47DA0D488002C40321214B81" value: "Sxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" } write_pairs { key: "47D1B5488002C40421214A80" value: "$" } </th>
                </tr>
        </table>
        <div class='yb-bottom-spacer'></div>
    </div>
        "#.to_string();
        let result = AllOperations::parse_tablet_server_operations(operations);

        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].as_ref().unwrap().tablet_id, "baa5fc1db83248a88e95cd5004d93e57");
        assert_eq!(result.tasks[0].as_ref().unwrap().op_id, "term: 1 index: 61");
        assert_eq!(result.tasks[0].as_ref().unwrap().transaction_type, "WRITE_OP");
        assert_eq!(result.tasks[0].as_ref().unwrap().total_time_in_flight, "28836 us.");
    }

    #[tokio::test]
    async fn integration_parse_tablet_server_operations() {
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();

        let _alloperations = AllOperations::read_tablet_server_operations(&vec![&hostname], &vec![&port], 1).await;
        // the master returns none or more tasks.
    }
}