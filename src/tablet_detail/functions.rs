//! The impls and functions
//!
use chrono::Local;
use std::{sync::mpsc::channel, time::Instant};
use scraper::{Html, Selector};
use log::*;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::tablet_detail::{AllTablets, Tablet, TabletBasic, TabletDetail, Column, ConsensusStatus, Watermark, Message};

impl Tablet {
    pub fn new() -> Self { Default::default() }
}
impl TabletDetail {
    pub fn new() -> Self { Default::default() }
}
impl Column {
    pub fn new() -> Self { Default::default() }
}
impl ConsensusStatus {
    pub fn new() -> Self { Default::default() }
}
impl Watermark {
    pub fn new() -> Self { Default::default() }
}
impl Message {
    pub fn new() -> Self { Default::default() }
}

impl AllTablets {
    pub fn new() -> Self { Default::default() }
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
        extra_data: &bool,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let alltablets = AllTablets::read_tablets(hosts, ports, parallel, extra_data).await;
        snapshot::save_snapshot_json(snapshot_number, "tablets", alltablets.tablet)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub async fn read_tablets (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
        extra_data: &bool,
    ) -> AllTablets
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
                        let mut tablets = AllTablets::read_http(host, port, extra_data);
                        tablets.timestamp = Some(detail_snapshot_time);
                        tablets.hostname_port = Some(format!("{}:{}", host, port));
                        tx.send(tablets).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut alltablets = AllTablets::new();

        for tablet in rx.iter().filter(|row| !row.tabletbasic.is_empty())
        {
            alltablets.tablet.push(tablet);
        }

        alltablets
    }
    fn read_http(
        host: &str,
        port: &str,
        extra_data: &bool,
    ) -> Tablet
    {
        let data_from_http = utility::http_get(host, port, "tablets");
        let mut tablet = AllTablets::parse_tablets(data_from_http);
        if *extra_data
        {
            AllTablets::parse_tablets_add_detail(host, port, &mut tablet);
        }
        tablet
    }
    fn parse_tablets(
        http_data: String
    ) -> Tablet
    {
        let table_selector = Selector::parse("table").unwrap();
        let tr_selector = Selector::parse("tr").unwrap();
        let th_selector = Selector::parse("th").unwrap();
        let td_selector = Selector::parse("td").unwrap();
        let div_selector = Selector::parse("div.yb-main").unwrap();

        let mut tablet = Tablet::new();

        let html = Html::parse_document(&http_data);

        //
        // ---
        //     <div class='yb-main container-fluid'>
        //         <h1>Tablets</h1>
        //         <table class='table table-striped'>
        //             <tr>
        //                 <th>Namespace</th>
        //                 <th>Table name</th>
        //                 <th>Table UUID</th>
        //                 <th>Tablet ID</th>
        //                 <th>Partition</th>
        //                 <th>State</th>
        //                 <th>Hidden</th>
        //                 <th>Num SST Files</th>
        //                 <th>On-disk size</th>
        //                 <th>RaftConfig</th>
        //                 <th>Last status</th>
        //             </tr>
        //             <tr>
        //                 <td>system</td>
        //                 <td>transactions</td>
        //                 <td>c14b0e3be8fc4ffa96ac5873c93c114f</td>
        //                 <td>
        //                     <a href="/tablet?id=069090d18ca24e7b912118666725e92e">069090d18ca24e7b912118666725e92e</a>
        //                 </td>
        //                 <td>hash_split: [0x954C, 0x9FF5]</td>
        //                 <td>RUNNING</td>
        //                 <td>false</td>
        //                 <td>0</td>
        //                 <td>
        //                     <ul>
        //                         <li>Total: 2.00M
        //                         <li>Consensus Metadata: 1.5K
        //                         <li>WAL Files: 2.00M
        //                         <li>SST Files: 0B
        //                         <li>SST Files Uncompressed: 0B
        //                     </ul>
        //                 </td>
        //                 <td>
        //                     <ul>
        //                         <li>LEADER: yb-3.local</li>
        //                         <li>
        //                             <b>FOLLOWER: yb-1.local</b>
        //                         </li>
        //                         <li>FOLLOWER: yb-2.local</li>
        //                     </ul>
        //                 </td>
        //                 <td>transactions0</td>
        //             </tr>
        //          </table>
        //          <div class='yb-bottom-spacer'></div>
        //      </div>
        // ---
        // The things to watch for here:
        // - The tables have an html tags path that is selected by `div_selector` as the root of a user, index or system table.

        for div_panel in html.select(&div_selector)
        {
            match div_panel.select(&table_selector).next()
            {
                Some(html_table) => {
                    match html_table
                    {
                        th
                        if th.select(&th_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Namespace"
                            && th.select(&th_selector).nth(1).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Table name"
                            && th.select(&th_selector).nth(2).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Table UUID"
                            && th.select(&th_selector).nth(3).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Tablet ID"
                            && th.select(&th_selector).nth(4).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Partition"
                            && th.select(&th_selector).nth(5).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"State"
                            && th.select(&th_selector).nth(6).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Hidden"
                            && th.select(&th_selector).nth(7).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Num SST Files"
                            && th.select(&th_selector).nth(8).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"On-disk size"
                            && th.select(&th_selector).nth(9).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"RaftConfig"
                            && th.select(&th_selector).nth(10).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Last status" => {
                            for tr in html_table.select(&tr_selector).skip(1)
                            {
                                tablet.tabletbasic.push( TabletBasic {
                                    namespace: tr.select(&td_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                    table_name: tr.select(&td_selector).nth(1).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                    table_uuid: tr.select(&td_selector).nth(2).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                    tablet_id: tr.select(&td_selector).nth(3).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default().trim().to_string(),
                                    partition: tr.select(&td_selector).nth(4).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                    state: tr.select(&td_selector).nth(5).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                    hidden: tr.select(&td_selector).nth(6).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                    num_sst_files: tr.select(&td_selector).nth(7).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                    on_disk_size: tr.select(&td_selector).nth(8).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default().trim().to_string(),
                                    raftconfig: tr.select(&td_selector).nth(9).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default().trim().to_string(),
                                    last_status: tr.select(&td_selector).nth(10).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                });
                            }
                        },
                        _ => {
                            info!("Tablet html table doesn't match specified heading for tablets, this shouldn't happen.");
                        },
                    }
                },
                None => debug!("Unable to find html table."),
            }
        }
        tablet
    }
    fn parse_tablets_add_detail(
        host: &str,
        port: &str,
        tablets: &mut Tablet
    )
    {
        for row in tablets.tabletbasic.iter_mut()
        {
            let data_from_http = utility::http_get(host, port, format!("tablet?id={}", row.tablet_id).as_str());
            let mut detail = AllTablets::parse_tablet_detail(data_from_http, &row.tablet_id);
            let data_from_http = utility::http_get(host, port, format!("tablet-consensus-status?id={}", row.tablet_id).as_str());
            let consensus_status = AllTablets::parse_tablet_detail_consensus_status(data_from_http);
            detail.consensus_status = consensus_status;
            tablets.tabletdetail.push(Some(detail));
        }
    }
    fn parse_tablet_detail(
        data_from_http: String,
        tablet_id: &str,
    ) -> TabletDetail
    {
        let html = Html::parse_document(&data_from_http);
        let table_selector = Selector::parse("table").unwrap();
        let td_selector = Selector::parse("td").unwrap();
        let th_selector = Selector::parse("th").unwrap();
        let tr_selector = Selector::parse("tr").unwrap();
        let mut tablet_detail = TabletDetail::new();
        tablet_detail.tablet_id = tablet_id.to_string();

        // This table contains the columns
        let schema_table = html.select(&table_selector).next().expect("Schema html table in /tablet?id=tablet_id page should exist");
        match schema_table
        {
            th
            if th.select(&th_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Column"
                && th.select(&th_selector).nth(1).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"ID"
                && th.select(&th_selector).nth(2).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Type" => {
                // skip heading
                for tr in schema_table.select(&tr_selector).skip(1)
                {
                    let mut column = Column::new();
                    // It looks to me like the table column definitions are a bit off logically:
                    // The first table data column is defined as table header again, probably to make the column name bold typefaced.
                    column.column = tr.select(&th_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default();
                    column.id = tr.select(&td_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default();
                    column.column_type = tr.select(&td_selector).nth(1).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default();
                    tablet_detail.columns.push(Some(column));
                }
            },
            non_matching_table => {
                info!("Found another table, this shouldn't happen: {:?}.", non_matching_table);
            },
        }
        tablet_detail
    }
    fn parse_tablet_detail_consensus_status(
        data_from_http: String,
    ) -> ConsensusStatus
    {
        let html = Html::parse_document(&data_from_http);
        let h2_selector = Selector::parse("h2").unwrap();
        let pre_selector = Selector::parse("pre").unwrap();
        let div_selector = Selector::parse("div.yb-main").unwrap();
        let td_selector = Selector::parse("td").unwrap();
        let th_selector = Selector::parse("th").unwrap();
        let tr_selector = Selector::parse("tr").unwrap();
        let table_selector = Selector::parse("table").unwrap();
        let mut consensus_state = ConsensusStatus::new();

        for div_select in html.select(&div_selector)
        {
            let mut element_counter = 0;
            for h2 in div_select.select(&h2_selector)
            {
                match h2
                {
                    h2_text if h2_text.text().collect::<String>() == *"State" => {
                        debug!("state: {}", div_select.select(&pre_selector).nth(element_counter).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default());
                        consensus_state.state = div_select.select(&pre_selector).nth(element_counter).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default();
                    },
                    h2_text if h2_text.text().collect::<String>() == *"Queue overview" => {
                        debug!("queue overview: {}", div_select.select(&pre_selector).nth(element_counter).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default());
                        consensus_state.queue_overview = Some(div_select.select(&pre_selector).nth(element_counter).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default());
                    },
                    _ => {},
                }
                element_counter += 1;
            }
            for table in div_select.select(&table_selector)
            {
                match table
                {
                    th
                    if th.select(&th_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Peer"
                        && th.select(&th_selector).nth(1).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Watermark" => {
                        for tr in table.select(&tr_selector).skip(1)
                        {
                            let mut watermark = Watermark::new();
                            debug!("{}", tr.select(&td_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default());
                            watermark.peer = tr.select(&td_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default();
                            debug!("{}", tr.select(&td_selector).nth(1).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default());
                            watermark.watermark = tr.select(&td_selector).nth(1).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default();
                            consensus_state.watermark.push(Some(watermark));
                        }
                    },
                    th
                    if th.select(&th_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Entry"
                        && th.select(&th_selector).nth(1).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"OpId"
                        && th.select(&th_selector).nth(2).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Type"
                        && th.select(&th_selector).nth(3).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Size"
                        && th.select(&th_selector).nth(4).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Status" => {
                        for tr in table.select(&tr_selector).skip(1)
                        {
                            let mut message = Message::new();
                            // the first two table columns are actually table header columns
                            message.entry = tr.select(&th_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default();
                            message.opid = tr.select(&th_selector).nth(1).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default();
                            // from the third column on the table data columns
                            message.message_type = tr.select(&td_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default();
                            message.size = tr.select(&td_selector).nth(1).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default();
                            message.status = tr.select(&td_selector).nth(2).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default();
                            consensus_state.messages.push(Some(message))
                        }
                    }
                    _ => {},
                }
            }
        }
        consensus_state
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
    fn unit_parse_basic_new_no_user_tablets() {
        let tablets = r#"
        <div class='yb-main container-fluid'>
        <h1>Tablets</h1>
        <table class='table table-striped'>
            <tr>
                <th>Namespace</th>
                <th>Table name</th>
                <th>Table UUID</th>
                <th>Tablet ID</th>
                <th>Partition</th>
                <th>State</th>
                <th>Hidden</th>
                <th>Num SST Files</th>
                <th>On-disk size</th>
                <th>RaftConfig</th>
                <th>Last status</th>
            </tr>
            <tr>
                <td>system</td>
                <td>transactions</td>
                <td>3e4326ce234d4a248c0b75b5c0f948c7</td>
                <td>
                    <a href="/tablet?id=11f0cbff280f4ac0a25fcbcec107e932">11f0cbff280f4ac0a25fcbcec107e932</a>
                </td>
                <td>hash_split: [0x7FF8, 0x8AA1]</td>
                <td>RUNNING</td>
                <td>false</td>
                <td>0</td>
                <td>
                    <ul>
                        <li>Total: 1.00M
                        <li>Consensus Metadata: 1.5K
                        <li>WAL Files: 1.00M
                        <li>SST Files: 0B
                        <li>SST Files Uncompressed: 0B
                    </ul>
                </td>
                <td>
                    <ul>
                        <li>
                            <b>LEADER: yb-2.local</b>
                        </li>
                        <li>FOLLOWER: yb-1.local</li>
                        <li>FOLLOWER: yb-3.local</li>
                    </ul>
                </td>
                <td>transactions0</td>
            </tr>
                </table>
            </div>
        </div>
        <!-- panel -->
        <div class='yb-bottom-spacer'></div>
        </div>
        "#.to_string();
        let result = AllTablets::parse_tablets(tablets);

        assert_eq!(result.tabletbasic.len(), 1);
        //
        assert_eq!(result.tabletbasic[0].namespace, "system");
        assert_eq!(result.tabletbasic[0].table_name, "transactions");
        assert_eq!(result.tabletbasic[0].table_uuid, "3e4326ce234d4a248c0b75b5c0f948c7");
        assert_eq!(result.tabletbasic[0].tablet_id, "11f0cbff280f4ac0a25fcbcec107e932");
        assert_eq!(result.tabletbasic[0].partition, "hash_split: [0x7FF8, 0x8AA1]");
        assert_eq!(result.tabletbasic[0].state, "RUNNING");
        assert_eq!(result.tabletbasic[0].hidden, "false");
        assert_eq!(result.tabletbasic[0].on_disk_size, "Total: 1.00M\n                        Consensus Metadata: 1.5K\n                        WAL Files: 1.00M\n                        SST Files: 0B\n                        SST Files Uncompressed: 0B");
        assert_eq!(result.tabletbasic[0].raftconfig, "LEADER: yb-2.local\n                        \n                        FOLLOWER: yb-1.local\n                        FOLLOWER: yb-3.local");
        assert_eq!(result.tabletbasic[0].last_status, "transactions0");
    }

    /*
    #[test]
    fn unit_parse_basic_new_single_user_table_no_index() {
        let tables = r#"
        <div class='yb-main container-fluid'>
        <div class='panel panel-default'>
            <div class='panel-heading'>
                <h2 class='panel-title'>User tables</h2>
            </div>
            <div class='panel-body table-responsive'>
                <table class='table table-responsive'>
                    <tr>
                        <th>Keyspace</th>
                        <th>Table Name</th>
                        <th>State</th>
                        <th>Message</th>
                        <th>UUID</th>
                        <th>YSQL OID</th>
                        <th>Hidden</th>
                        <th>On-disk size</th>
                    </tr>
                    <tr>
                        <td>yugabyte</td>
                        <td>
                            <a href="/table?id=000033e8000030008000000000004000">t</a>
                        </td>
                        <td>Running</td>
                        <td></td>
                        <td>000033e8000030008000000000004000</td>
                        <td>16384</td>
                        <td>false</td>
                        <td>
                            <ul>
                                <li>Total: 3.00M
                                <li>WAL Files: 3.00M
                                <li>SST Files: 0B
                                <li>SST Files Uncompressed: 0B
                            </ul>
                        </td>
                    </tr>
                </table>
            </div>
            <!-- panel-body -->
        </div>
        <!-- panel -->
        <div class='panel panel-default'>
            <div class='panel-heading'>
                <h2 class='panel-title'>Index tables</h2>
            </div>
            <div class='panel-body table-responsive'>There are no index tables.
            </div>
            <!-- panel-body -->
        </div>
        <!-- panel -->
        <div class='panel panel-default'>
            <div class='panel-heading'>
                <h2 class='panel-title'>System tables</h2>
            </div>
            <div class='panel-body table-responsive'>
                <table class='table table-responsive'>
                    <tr>
                        <th>Keyspace</th>
                        <th>Table Name</th>
                        <th>State</th>
                        <th>Message</th>
                        <th>UUID</th>
                        <th>YSQL OID</th>
                        <th>Hidden</th>
                    <tr>
                        <td>template1</td>
                        <td>
                            <a href="/table?id=000000010000300080000000000000af">pg_user_mapping_user_server_index</a>
                        </td>
                        <td>Running</td>
                        <td></td>
                        <td>000000010000300080000000000000af</td>
                        <td>175</td>
                        <td>false</td>
                    </tr>
                </table>
            </div>
        </div>
        <!-- panel -->
        <div class='yb-bottom-spacer'></div>
        </div>
        "#.to_string();
        let result = AllTables::parse_tables(tables);
        //
        assert_eq!(result.tablebasic.len(), 2);

        assert_eq!(result.tablebasic[0].keyspace, "yugabyte");
        assert_eq!(result.tablebasic[0].table_name, "t");
        assert_eq!(result.tablebasic[0].state, "Running");
        assert_eq!(result.tablebasic[0].message, "");
        assert_eq!(result.tablebasic[0].uuid, "000033e8000030008000000000004000");
        assert_eq!(result.tablebasic[0].ysql_oid, "16384");
        assert_eq!(result.tablebasic[0].hidden, "false");
        assert_eq!(result.tablebasic[0].on_disk_size, "Total: 3.00M\n                                WAL Files: 3.00M\n                                SST Files: 0B\n                                SST Files Uncompressed: 0B");
        assert_eq!(result.tablebasic[0].object_type, "User tables");
        assert_eq!(result.tablebasic[1].keyspace, "template1");
        assert_eq!(result.tablebasic[1].table_name, "pg_user_mapping_user_server_index");
        assert_eq!(result.tablebasic[1].state, "Running");
        assert_eq!(result.tablebasic[1].message, "");
        assert_eq!(result.tablebasic[1].uuid, "000000010000300080000000000000af");
        assert_eq!(result.tablebasic[1].ysql_oid, "175");
        assert_eq!(result.tablebasic[1].hidden, "false");
        assert_eq!(result.tablebasic[1].on_disk_size, "");
        assert_eq!(result.tablebasic[1].object_type, "System tables");
    }

    #[test]
    fn unit_parse_basic_new_single_user_table_index_table() {
        let tables = r#"
        <div class='yb-main container-fluid'>
        <div class='panel panel-default'>
            <div class='panel-heading'>
                <h2 class='panel-title'>User tables</h2>
            </div>
            <div class='panel-body table-responsive'>
                <table class='table table-responsive'>
                    <tr>
                        <th>Keyspace</th>
                        <th>Table Name</th>
                        <th>State</th>
                        <th>Message</th>
                        <th>UUID</th>
                        <th>YSQL OID</th>
                        <th>Hidden</th>
                        <th>On-disk size</th>
                    </tr>
                    <tr>
                        <td>yugabyte</td>
                        <td>
                            <a href="/table?id=000033e8000030008000000000004000">t</a>
                        </td>
                        <td>Running</td>
                        <td></td>
                        <td>000033e8000030008000000000004000</td>
                        <td>16384</td>
                        <td>false</td>
                        <td>
                            <ul>
                                <li>Total: 3.00M
                                <li>WAL Files: 3.00M
                                <li>SST Files: 0B
                                <li>SST Files Uncompressed: 0B
                            </ul>
                        </td>
                    </tr>
                </table>
            </div>
            <!-- panel-body -->
        </div>
        <!-- panel -->
        <div class='panel panel-default'>
            <div class='panel-heading'>
                <h2 class='panel-title'>Index tables</h2>
            </div>
            <div class='panel-body table-responsive'>
                <table class='table table-responsive'>
                    <tr>
                        <th>Keyspace</th>
                        <th>Table Name</th>
                        <th>State</th>
                        <th>Message</th>
                        <th>UUID</th>
                        <th>YSQL OID</th>
                        <th>Hidden</th>
                        <th>On-disk size</th>
                    </tr>
                    <tr>
                        <td>yugabyte</td>
                        <td>
                            <a href="/table?id=000033e8000030008000000000004003">t_i</a>
                        </td>
                        <td>Running</td>
                        <td></td>
                        <td>000033e8000030008000000000004003</td>
                        <td>16387</td>
                        <td>false</td>
                        <td>
                            <ul>
                                <li>Total: 3.00M
                                <li>WAL Files: 3.00M
                                <li>SST Files: 0B
                                <li>SST Files Uncompressed: 0B
                            </ul>
                        </td>
                    </tr>
                </table>
            </div>
            <!-- panel-body -->
        </div>
        <!-- panel -->
        <div class='panel panel-default'>
            <div class='panel-heading'>
                <h2 class='panel-title'>System tables</h2>
            </div>
            <div class='panel-body table-responsive'>
                <table class='table table-responsive'>
                    <tr>
                        <th>Keyspace</th>
                        <th>Table Name</th>
                        <th>State</th>
                        <th>Message</th>
                        <th>UUID</th>
                        <th>YSQL OID</th>
                        <th>Hidden</th>
                    <tr>
                        <td>template1</td>
                        <td>
                            <a href="/table?id=000000010000300080000000000000af">pg_user_mapping_user_server_index</a>
                        </td>
                        <td>Running</td>
                        <td></td>
                        <td>000000010000300080000000000000af</td>
                        <td>175</td>
                        <td>false</td>
                    </tr>
                </table>
            </div>
        </div>
        <!-- panel -->
        <div class='yb-bottom-spacer'></div>
        </div>
        "#.to_string();
        let result = AllTables::parse_tables(tables);
        //
        assert_eq!(result.tablebasic.len(), 3);

        assert_eq!(result.tablebasic[0].keyspace, "yugabyte");
        assert_eq!(result.tablebasic[0].table_name, "t");
        assert_eq!(result.tablebasic[0].state, "Running");
        assert_eq!(result.tablebasic[0].message, "");
        assert_eq!(result.tablebasic[0].uuid, "000033e8000030008000000000004000");
        assert_eq!(result.tablebasic[0].ysql_oid, "16384");
        assert_eq!(result.tablebasic[0].hidden, "false");
        assert_eq!(result.tablebasic[0].on_disk_size, "Total: 3.00M\n                                WAL Files: 3.00M\n                                SST Files: 0B\n                                SST Files Uncompressed: 0B");
        assert_eq!(result.tablebasic[0].keyspace, "yugabyte");
        assert_eq!(result.tablebasic[1].table_name, "t_i");
        assert_eq!(result.tablebasic[1].state, "Running");
        assert_eq!(result.tablebasic[1].message, "");
        assert_eq!(result.tablebasic[1].uuid, "000033e8000030008000000000004003");
        assert_eq!(result.tablebasic[1].ysql_oid, "16387");
        assert_eq!(result.tablebasic[1].hidden, "false");
        assert_eq!(result.tablebasic[1].on_disk_size, "Total: 3.00M\n                                WAL Files: 3.00M\n                                SST Files: 0B\n                                SST Files Uncompressed: 0B");
        assert_eq!(result.tablebasic[1].object_type, "User tables");
        assert_eq!(result.tablebasic[1].object_type, "User tables");
        assert_eq!(result.tablebasic[2].keyspace, "template1");
        assert_eq!(result.tablebasic[2].table_name, "pg_user_mapping_user_server_index");
        assert_eq!(result.tablebasic[2].state, "Running");
        assert_eq!(result.tablebasic[2].message, "");
        assert_eq!(result.tablebasic[2].uuid, "000000010000300080000000000000af");
        assert_eq!(result.tablebasic[2].ysql_oid, "175");
        assert_eq!(result.tablebasic[2].hidden, "false");
        assert_eq!(result.tablebasic[2].on_disk_size, "");
        assert_eq!(result.tablebasic[2].object_type, "System tables");
    }
    #[test]
    fn unit_parse_basic_new_no_user_tables_colocated_parent() {
        let tables = r#"
        <div class='yb-main container-fluid'>
        <div class='panel panel-default'>
            <div class='panel-heading'>
                <h2 class='panel-title'>User tables</h2>
            </div>
            <div class='panel-body table-responsive'>There are no user tables.
            </div>
            <!-- panel-body -->
        </div>
        <!-- panel -->
        <div class='panel panel-default'>
            <div class='panel-heading'>
                <h2 class='panel-title'>Index tables</h2>
            </div>
            <div class='panel-body table-responsive'>There are no index tables.
            </div>
            <!-- panel-body -->
        </div>
        <!-- panel -->
        <div class='panel panel-default'>
            <div class='panel-heading'>
                <h2 class='panel-title'>Parent tables</h2>
            </div>
            <div class='panel-body table-responsive'>
                <table class='table table-responsive'>
                    <tr>
                        <th>Keyspace</th>
                        <th>Table Name</th>
                        <th>State</th>
                        <th>Message</th>
                        <th>UUID</th>
                        <th>YSQL OID</th>
                        <th>Hidden</th>
                        <th>On-disk size</th>
                    </tr>
                    <tr>
                        <td>colocated</td>
                        <td>
                            <a href="/table?id=00004000000030008000000000000000.colocated.parent.uuid">00004000000030008000000000000000
                            .colocated.parent.tablename</a>
                        </td>
                        <td>Running</td>
                        <td></td>
                        <td>00004000000030008000000000000000
                        .colocated.parent.uuid</td>
                        <td></td>
                        <td>false</td>
                        <td>
                            <ul>
                                <li>Total: 1.00M
                                <li>WAL Files: 1.00M
                                <li>SST Files: 0B
                                <li>SST Files Uncompressed: 0B
                            </ul>
                        </td>
                    </tr>
                </table>
            </div>
            <!-- panel-body -->
        </div>
        <div class='panel panel-default'>
            <div class='panel-heading'>
                <h2 class='panel-title'>System tables</h2>
            </div>
            <div class='panel-body table-responsive'>
                <table class='table table-responsive'>
                    <tr>
                        <th>Keyspace</th>
                        <th>Table Name</th>
                        <th>State</th>
                        <th>Message</th>
                        <th>UUID</th>
                        <th>YSQL OID</th>
                        <th>Hidden</th>
                    <tr>
                        <td>template1</td>
                        <td>
                            <a href="/table?id=000000010000300080000000000000af">pg_user_mapping_user_server_index</a>
                        </td>
                        <td>Running</td>
                        <td></td>
                        <td>000000010000300080000000000000af</td>
                        <td>175</td>
                        <td>false</td>
                    </tr>
                </table>
            </div>
        </div>
        <!-- panel -->
        <div class='yb-bottom-spacer'></div>
        </div>
        "#.to_string();
        let result = AllTables::parse_tables(tables);

        assert_eq!(result.tablebasic.len(), 2);
        //
        assert_eq!(result.tablebasic[0].keyspace, "colocated");
        assert_eq!(result.tablebasic[0].table_name, "00004000000030008000000000000000.colocated.parent.tablename");
        assert_eq!(result.tablebasic[0].state, "Running");
        assert_eq!(result.tablebasic[0].message, "");
        assert_eq!(result.tablebasic[0].uuid, "00004000000030008000000000000000.colocated.parent.uuid");
        assert_eq!(result.tablebasic[0].ysql_oid, "");
        assert_eq!(result.tablebasic[0].hidden, "false");
        assert_eq!(result.tablebasic[0].on_disk_size, "Total: 1.00M\n                                WAL Files: 1.00M\n                                SST Files: 0B\n                                SST Files Uncompressed: 0B");
        assert_eq!(result.tablebasic[0].object_type, "Parent tables");
        assert_eq!(result.tablebasic[1].keyspace, "template1");
        assert_eq!(result.tablebasic[1].table_name, "pg_user_mapping_user_server_index");
        assert_eq!(result.tablebasic[1].state, "Running");
        assert_eq!(result.tablebasic[1].message, "");
        assert_eq!(result.tablebasic[1].uuid, "000000010000300080000000000000af");
        assert_eq!(result.tablebasic[1].ysql_oid, "175");
        assert_eq!(result.tablebasic[1].hidden, "false");
        assert_eq!(result.tablebasic[1].on_disk_size, "");
        assert_eq!(result.tablebasic[1].object_type, "System tables");
    }
    #[test]
    fn unit_parse_table_detail_by_id() {
        let tables = r#"
    <div class='yb-main container-fluid'>
        <h1>Table: yugabyte.t (000033e8000030008000000000004100) </h1>
        <table class='table table-striped'>
            <tr>
                <td>Version:</td>
                <td>0</td>
            </tr>
            <tr>
                <td>Type:</td>
                <td>PGSQL_TABLE_TYPE</td>
            </tr>
            <tr>
                <td>State:</td>
                <td>Running</td>
            </tr>
            <tr>
                <td>Replication Info:</td>
                <td>
                    <pre class="prettyprint"></pre>
                </td>
            </tr>
        </table>
        <table class='table table-striped'>
            <tr>
                <th>Column</th>
                <th>ID</th>
                <th>Type</th>
            </tr>
            <tr>
                <th>id</th>
                <td>0</td>
                <td>int32 NOT NULL PARTITION KEY</td>
            </tr>
            <tr>
                <th>f1</th>
                <td>1</td>
                <td>string NULLABLE NOT A PARTITION KEY</td>
            </tr>
        </table>
        <table class='table table-striped'>
            <tr>
                <th>Tablet ID</th>
                <th>Partition</th>
                <th>SplitDepth</th>
                <th>State</th>
                <th>Hidden</th>
                <th>Message</th>
                <th>RaftConfig</th>
            </tr>
            <tr>
                <th>5a4f24f5159f4c518bf3bcc5e2c4193c</th>
                <td>hash_split: [0x0000, 0xFFFF]</td>
                <td>0</td>
                <td>Running</td>
                <td>false</td>
                <td>Tablet reported with an active leader</td>
                <td>
                    <ul>
                        <li>
                            <b>
                                LEADER:
                                <a href="http://yb-3.local:9000/tablet?id=5a4f24f5159f4c518bf3bcc5e2c4193c">yb-3.local</a>
                            </b>
                        </li>
                        <li>
                            FOLLOWER:
                            <a href="http://yb-1.local:9000/tablet?id=5a4f24f5159f4c518bf3bcc5e2c4193c">yb-1.local</a>
                        </li>
                        <li>
                            FOLLOWER:
                            <a href="http://yb-2.local:9000/tablet?id=5a4f24f5159f4c518bf3bcc5e2c4193c">yb-2.local</a>
                        </li>
                    </ul>
                </td>
            </tr>
        </table>
        <table class='table table-striped'>
            <tr>
                <th>Task Name</th>
                <th>State</th>
                <th>Start Time</th>
                <th>Duration</th>
                <th>Description</th>
            </tr>
        </table>
        <div class='yb-bottom-spacer'></div>
    </div>
        "#.to_string();
        let result = AllTables::parse_table_detail( tables,"deadbeef");
        println!("{:#?}", result);
        //
    }


    #[tokio::test]
    async fn integration_parse_master_tables() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let alltables = AllTables::read_tables(&vec![&hostname], &vec![&port], 1).await;
        // the master returns more than one thread.
        assert!(alltables.table.len() > 1);
    }

     */
}