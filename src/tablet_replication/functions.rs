//! The impls and functions
//!
use chrono::Local;
use std::{sync::mpsc::channel, time::Instant};
use scraper::{Html, Selector};
use log::*;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::tablet_replication::{AllTabletReplication, LeaderlessTablet, TabletReplication, UnderReplicatedTablets};

impl TabletReplication {
    pub fn new() -> Self { Default::default() }
}
impl AllTabletReplication {
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

        let alltabletreplication = AllTabletReplication::read_tablet_replication(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "tablet_replication", alltabletreplication.tablet_replication)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub async fn read_tablet_replication (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllTabletReplication
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
                        let mut tabletreplication = AllTabletReplication::read_http(host, port);
                        tabletreplication.timestamp = Some(detail_snapshot_time);
                        tabletreplication.hostname_port = Some(format!("{}:{}", host, port));
                        tx.send(tabletreplication).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut alltabletreplication = AllTabletReplication::new();

        for tablet_replication in rx.iter().filter(|row| !row.leaderless_tablets.is_empty() && !row.under_replicated_tablets.is_empty())
        {
            alltabletreplication.tablet_replication.push(tablet_replication);
        }

        alltabletreplication
    }
    fn read_http(
        host: &str,
        port: &str,
    ) -> TabletReplication
    {
        let data_from_http = utility::http_get(host, port, "tablet-replication");
        AllTabletReplication::parse_tablet_replication(data_from_http)
    }
    fn parse_tablet_replication(
        http_data: String
    ) -> TabletReplication
    {
        let table_selector = Selector::parse("table").unwrap();
        let tr_selector = Selector::parse("tr").unwrap();
        let th_selector = Selector::parse("th").unwrap();
        let td_selector = Selector::parse("td").unwrap();
        let div_selector = Selector::parse("div.yb-main").unwrap();

        let mut tabletreplication = TabletReplication::new();

        let html = Html::parse_document(&http_data);

        for div_select in html.select(&div_selector)
        {
            for table in div_select.select(&table_selector)
            {
                match table
                {
                    // Second table: Underreplicated Tablets
                    // The second table put first, to match the fourth column, otherwise the under-replicated table would match the leaderless tablets specification.
                    th
                    if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Table Name"
                        && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Table UUID"
                        && th.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Tablet ID"
                        && th.select(&th_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Tablet Replication Count" =>
                        {
                            for tr in table.select(&tr_selector).skip(1)
                            {
                                tabletreplication.under_replicated_tablets.push( Some(UnderReplicatedTablets {
                                    table_name: tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default().replace([' ','\n'], ""),
                                    table_uuid: tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    tablet_id: tr.select(&td_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    tablet_replication_count: tr.select(&td_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                }));
                            }
                        },
                    // First table: Leaderless Tablets
                    th
                    if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Table Name"
                            && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Table UUID"
                            && th.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Tablet ID" =>
                    {
                            for tr in table.select(&tr_selector).skip(1)
                            {
                                tabletreplication.leaderless_tablets.push( Some(LeaderlessTablet {
                                    table_name: tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default().replace([' ','\n'], ""),
                                    table_uuid: tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    tablet_id: tr.select(&td_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                }));
                            }
                    },
                    _ => {
                            info!("Found a table that doesn't match specified headings, this shouldn't happen: {:#?}", table.clone());
                    },
                }
            }
        }
        tabletreplication
    }
    /*
    pub fn print(
        &self,
        hostname_filter: &Regex
    ) -> Result<()>
    {
        /*
        let mut previous_hostname_port = String::from("");
        for row in &self.threads
        {
            if hostname_filter.is_match(&row.hostname_port)
            {
                if row.hostname_port != previous_hostname_port
                {
                    println!("--------------------------------------------------------------------------------------------------------------------------------------");
                    println!("Host: {}, Snapshot time: {}", &row.hostname_port.to_string(), row.timestamp);
                    println!("--------------------------------------------------------------------------------------------------------------------------------------");
                    println!("{:20} {:40} {:>20} {:>20} {:>20} {:50}",
                             "hostname_port",
                             "thread_name",
                             "cum_user_cpu_s",
                             "cum_kernel_cpu_s",
                             "cum_iowait_cpu_s",
                             "stack");
                    println!("--------------------------------------------------------------------------------------------------------------------------------------");
                    previous_hostname_port = row.hostname_port.to_string();
                };
                println!("{:20} {:40} {:>20} {:>20} {:>20} {:50}", row.hostname_port, row.thread_name, row.cumulative_user_cpu_s, row.cumulative_kernel_cpu_s, row.cumulative_iowait_cpu_s, row.stack.replace('\n', ""));
            }
        }

         */
        Ok(())
    }

     */
}

/*
pub async fn print_tables(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    let hostname_filter = utility::set_regex(&options.hostname_match);
    match options.print_threads.as_ref().unwrap() {
        Some(snapshot_number) => {
            let mut alltables = AllTables::new();
            alltables.table = snapshot::read_snapshot_json(snapshot_number, "threads")?;
            alltables.print(&hostname_filter)?;
        },
        None => {
            let alltables = AllTables::read_tables(&hosts, &ports, parallel).await;
            alltables.print(&hostname_filter)?;
        },
    }
    Ok(())
}

 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_tablet_replication_no_tablets() {
        let tabletreplication = r#"
    <div class='yb-main container-fluid'>
        <h3>Leaderless Tablets</h3>
        <table class='table table-striped'>
            <tr>
                <th>Table Name</th>
                <th>Table UUID</th>
                <th>Tablet ID</th>
            </tr>
        </table>
        <h3>Underreplicated Tablets</h3>
        <table class='table table-striped'>
            <tr>
                <th>Table Name</th>
                <th>Table UUID</th>
                <th>Tablet ID</th>
                <th>Tablet Replication Count</th>
            </tr>
        </table>
        <div class='yb-bottom-spacer'></div>
    </div>
        "#.to_string();
        let result = AllTabletReplication::parse_tablet_replication(tabletreplication);

        assert_eq!(result.leaderless_tablets.len(), 0);
        assert_eq!(result.under_replicated_tablets.len(), 0);
    }

    #[test]
    fn unit_parse_tablet_replication_under_replicated_tablets() {
        let tabletreplication = r#"
    <div class='yb-main container-fluid'>
        <h3>Leaderless Tablets</h3>
        <table class='table table-striped'>
            <tr>
                <th>Table Name</th>
                <th>Table UUID</th>
                <th>Tablet ID</th>
            </tr>
        </table>
        <h3>Underreplicated Tablets</h3>
        <table class='table table-striped'>
            <tr>
                <th>Table Name</th>
                <th>Table UUID</th>
                <th>Tablet ID</th>
                <th>Tablet Replication Count</th>
            </tr>
            <tr>
                <td>
                    <a href="/table?id=000033e8000030008000000000004200">t</a>
                </td>
                <td>000033e8000030008000000000004200</td>
                <td>10a28b5baffd4a28b541d633a8259c88</td>
                <td>2</td>
            </tr>
            <tr>
                <td>
                    <a href="/table?id=c7693c5862134cf9b6c6b295a554a5fe">transactions</a>
                </td>
                <td>c7693c5862134cf9b6c6b295a554a5fe</td>
                <td>ac487570432b47618721d5dff0f760bf</td>
                <td>2</td>
            </tr>
            </table>
        <div class='yb-bottom-spacer'></div>
    </div>

        "#.to_string();
        let result = AllTabletReplication::parse_tablet_replication(tabletreplication);

        assert_eq!(result.leaderless_tablets.len(), 0);
        assert_eq!(result.under_replicated_tablets.len(), 2);
        assert_eq!(result.under_replicated_tablets[0].as_ref().unwrap().table_name, "t");
        assert_eq!(result.under_replicated_tablets[0].as_ref().unwrap().table_uuid, "000033e8000030008000000000004200");
        assert_eq!(result.under_replicated_tablets[0].as_ref().unwrap().tablet_id, "10a28b5baffd4a28b541d633a8259c88");
        assert_eq!(result.under_replicated_tablets[0].as_ref().unwrap().tablet_replication_count, "2");
        assert_eq!(result.under_replicated_tablets[1].as_ref().unwrap().table_name, "transactions");
        assert_eq!(result.under_replicated_tablets[1].as_ref().unwrap().table_uuid, "c7693c5862134cf9b6c6b295a554a5fe");
        assert_eq!(result.under_replicated_tablets[1].as_ref().unwrap().tablet_id, "ac487570432b47618721d5dff0f760bf");
        assert_eq!(result.under_replicated_tablets[1].as_ref().unwrap().tablet_replication_count, "2");

    }
    #[tokio::test]
    async fn integration_parse_master_tasks() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let _result = AllTabletReplication::read_tablet_replication(&vec![&hostname], &vec![&port], 1).await;
        // the master returns none or more tasks.
    }
}