//! The impls and functions
//!
use chrono::Local;
use std::{sync::mpsc::channel, time::Instant};
use scraper::{Html, Selector};
use log::*;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::tablet_detail::{AllTablets, Tablet, TabletBasic, TabletDetail, Column, ConsensusStatus, Watermark, Message, TabletLogAnchor, Transactions, RocksDb, RocksDbFile};
use crate::Opts;

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
impl TabletLogAnchor {
    pub fn new() -> Self { Default::default() }
}
impl Transactions {
    pub fn new() -> Self { Default::default() }
}
impl RocksDb {
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
                        if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Namespace"
                            && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Table name"
                            && th.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Table UUID"
                            && th.select(&th_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Tablet ID"
                            && th.select(&th_selector).nth(4).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Partition"
                            && th.select(&th_selector).nth(5).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"State"
                            && th.select(&th_selector).nth(6).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Hidden"
                            && th.select(&th_selector).nth(7).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Num SST Files"
                            && th.select(&th_selector).nth(8).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"On-disk size"
                            && th.select(&th_selector).nth(9).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"RaftConfig"
                            && th.select(&th_selector).nth(10).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Last status" => {
                            for tr in html_table.select(&tr_selector).skip(1)
                            {
                                tablet.tabletbasic.push( TabletBasic {
                                    namespace: tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    table_name: tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    table_uuid: tr.select(&td_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    tablet_id: tr.select(&td_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default().trim().to_string(),
                                    partition: tr.select(&td_selector).nth(4).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    state: tr.select(&td_selector).nth(5).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    hidden: tr.select(&td_selector).nth(6).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    num_sst_files: tr.select(&td_selector).nth(7).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    on_disk_size: tr.select(&td_selector).nth(8).map(|row| row.text().collect::<Vec<_>>()).unwrap_or_default().iter().map(|r| r.trim()).filter(|r| !r.is_empty()).collect::<Vec<_>>().join(" "),
                                    raftconfig: tr.select(&td_selector).nth(9).map(|row| row.text().collect::<String>()).unwrap_or_default().split('\n').map(|r| r.trim()).filter(|r| !r.is_empty()).collect::<Vec<_>>().join(" "),
                                    last_status: tr.select(&td_selector).nth(10).map(|row| row.text().collect::<String>()).unwrap_or_default(),
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
            // This construction is because a tablet that is still shown but tombstoned will not have any details.
            // The page will say 'Tablet <UUID> has not yet started'
            if let Ok(mut detail) = AllTablets::parse_tablet_detail(data_from_http, &row.tablet_id)
            {
                let data_from_http = utility::http_get(host, port, format!("tablet-consensus-status?id={}", row.tablet_id).as_str());
                let consensus_status = AllTablets::parse_tablet_detail_consensus_status(data_from_http);
                detail.consensus_status = consensus_status;
                let data_from_http = utility::http_get(host, port, format!("log-anchors?id={}", row.tablet_id).as_str());
                let loganchor = AllTablets::parse_tablet_detail_log_anchors(data_from_http);
                detail.tabletloganchor = loganchor;
                let data_from_http = utility::http_get(host, port, format!("transactions?id={}", row.tablet_id).as_str());
                let transaction = AllTablets::parse_tablet_detail_transactions(data_from_http);
                detail.transactions = transaction;
                let data_from_http = utility::http_get(host, port, format!("rocksdb?id={}", row.tablet_id).as_str());
                let rocksdb = AllTablets::parse_tablet_detail_rocksdb(data_from_http);
                detail.rocksdb = rocksdb;
                //detail
                tablets.tabletdetail.push(Some(detail));
            };
        }
    }
    fn parse_tablet_detail(
        data_from_http: String,
        tablet_id: &str,
    ) -> Result<TabletDetail, &'static str>
    {
        let html = Html::parse_document(&data_from_http);
        let table_selector = Selector::parse("table").unwrap();
        let td_selector = Selector::parse("td").unwrap();
        let th_selector = Selector::parse("th").unwrap();
        let tr_selector = Selector::parse("tr").unwrap();
        let mut tablet_detail = TabletDetail::new();
        tablet_detail.tablet_id = tablet_id.to_string();

        //let schema_table = match html.select(&table_selector).next().expect(format!("Schema html table in /tablet?id={} page should exist.\n{}", tablet_id, &data_from_http).as_str())
        // The table that we are looking for might not exist if the tablet data has been tombstoned.
        // The next() will return None in that case.
        match html.select(&table_selector).next()
        {
            Some(schema_table) => {
                // This table contains the columns
                match schema_table
                {
                    th
                    if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Column"
                        && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"ID"
                        && th.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Type" => {
                        // skip heading
                        for tr in schema_table.select(&tr_selector).skip(1)
                        {
                            let mut column = Column::new();
                            // It looks to me like the table column definitions are a bit off logically:
                            // The first table data column is defined as table header again, probably to make the column name bold typefaced.
                            column.column = tr.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default();
                            column.id = tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default();
                            column.column_type = tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default();
                            tablet_detail.columns.push(Some(column));
                        }
                    },
                    non_matching_table => {
                        info!("Found another table, this shouldn't happen: {:?}.", non_matching_table);
                    },
                }
            },
            None => return Err("No html tables found"),
        };
        Ok(tablet_detail)
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
            for (element_counter, h2) in div_select.select(&h2_selector).enumerate()
            {
                match h2
                {
                    h2_text if h2_text.text().collect::<String>() == *"State" => {
                        debug!("state: {}", div_select.select(&pre_selector).nth(element_counter).map(|row| row.text().collect::<String>()).unwrap_or_default());
                        consensus_state.state = div_select.select(&pre_selector).nth(element_counter).map(|row| row.text().collect::<String>()).unwrap_or_default();
                    },
                    h2_text if h2_text.text().collect::<String>() == *"Queue overview" => {
                        debug!("queue overview: {}", div_select.select(&pre_selector).nth(element_counter).map(|row| row.text().collect::<String>()).unwrap_or_default());
                        consensus_state.queue_overview = Some(div_select.select(&pre_selector).nth(element_counter).map(|row| row.text().collect::<String>()).unwrap_or_default());
                    },
                    _ => {},
                }
            }
            for table in div_select.select(&table_selector)
            {
                match table
                {
                    th
                    if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Peer"
                        && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Watermark" => {
                        for tr in table.select(&tr_selector).skip(1)
                        {
                            let mut watermark = Watermark::new();
                            debug!("{}", tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default());
                            watermark.peer = tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default();
                            debug!("{}", tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default());
                            watermark.watermark = tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default();
                            consensus_state.watermark.push(Some(watermark));
                        }
                    },
                    th
                    if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Entry"
                        && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"OpId"
                        && th.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Type"
                        && th.select(&th_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Size"
                        && th.select(&th_selector).nth(4).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Status" => {
                        for tr in table.select(&tr_selector).skip(1)
                        {
                            let mut message = Message::new();
                            // the first two table columns are actually table header columns
                            message.entry = tr.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default();
                            message.opid = tr.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default();
                            // from the third column on the table data columns
                            message.message_type = tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default();
                            message.size = tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default();
                            message.status = tr.select(&td_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default();
                            consensus_state.messages.push(Some(message))
                        }
                    }
                    _ => {},
                }
            }
        }
        consensus_state
    }
    fn parse_tablet_detail_log_anchors(
        data_from_http: String,
    ) -> TabletLogAnchor
    {
        let html = Html::parse_document(&data_from_http);
        let pre_selector = Selector::parse("pre").unwrap();
        let div_selector = Selector::parse("div.yb-main").unwrap();
        let mut log_anchor = TabletLogAnchor::new();

        for div_select in html.select(&div_selector)
        {
            for pre in div_select.select(&pre_selector)
            {
                for line in pre.text().collect::<String>().lines()
                {
                    log_anchor.loganchor.push(line.to_string());
                }
            }
        }
        log_anchor
    }
    fn parse_tablet_detail_transactions(
        data_from_http: String,
    ) -> Transactions
    {
        let html = Html::parse_document(&data_from_http);
        let pre_selector = Selector::parse("pre").unwrap();
        let div_selector = Selector::parse("div.yb-main").unwrap();
        let mut transactions = Transactions::new();

        for div_select in html.select(&div_selector)
        {
            for pre in div_select.select(&pre_selector)
            {
                for line in pre.text().collect::<String>().lines()
                {
                    transactions.transactions.push(line.to_string());
                }
            }
        }
        transactions
    }
    fn parse_tablet_detail_rocksdb(
        data_from_http: String,
    ) -> RocksDb
    {
        let html = Html::parse_document(&data_from_http);
        let h3_selector = Selector::parse("h3").unwrap();
        let pre_selector = Selector::parse("pre").unwrap();
        let div_selector = Selector::parse("div.yb-main").unwrap();
        let mut rocksdb = RocksDb::new();

        let mut pre_counter = 0;
        let mut use_regular = false;
        for div_select in html.select(&div_selector)
        {
            for h3 in div_select.select(&h3_selector)
            {
                match h3
                {
                    h3_options if h3_options.text().collect::<String>() == *"Options" => {
                        // toggle boolean using bitwise xor
                        use_regular ^= true;
                        if use_regular
                        {
                            rocksdb.regular_options.push(div_select.select(&pre_selector).nth(pre_counter).map(|row| row.text().collect::<String>()).unwrap_or_default());
                        }
                        else
                        {
                            rocksdb.intents_options.push(div_select.select(&pre_selector).nth(pre_counter).map(|row| row.text().collect::<String>()).unwrap_or_default());
                        }
                        pre_counter += 1;
                    },
                    h3_files if h3_files.text().collect::<String>() == *"Files" => {
                        for file in div_select.select(&pre_selector).nth(pre_counter).map(|row| row.text().collect::<String>()).unwrap_or_default().lines().map(|row| row.trim()).filter(|row| !row.is_empty())
                        {
                            if use_regular
                            {
                                rocksdb.regular_files.push(file.to_string());
                            }
                            else
                            {
                                rocksdb.intents_files.push(file.to_string());
                            }
                        }
                        pre_counter += 1;
                    },
                    h3_file_detail => {
                        let details = vec![ div_select.select(&pre_selector).nth(pre_counter).map( | row| row.text().collect::<String>()).unwrap_or_default() ];
                        if use_regular
                        {
                            rocksdb.regular_files_detail.push( RocksDbFile {
                                filename: h3_file_detail.text().collect::<String>().split_whitespace().next().unwrap_or_default().to_string(),
                                details,
                            });
                        }
                        else
                        {
                            rocksdb.intents_files_detail.push( RocksDbFile {
                                filename: h3_file_detail.text().collect::<String>().split_whitespace().next().unwrap_or_default().to_string(),
                                details,
                            });
                        }
                        pre_counter += 1;
                    },
                }
            }
        }
        rocksdb
    }
    pub fn print(
        &self,
        uuid: &str,
    ) -> Result<()>
    {
        for alltablets in &self.tablet
        {
            for (keyspace, table_name, on_disk_size, state) in alltablets.tabletbasic.iter()
                .filter(|row| row.tablet_id == *uuid)
                .map(|row| (row.namespace.clone(), row.table_name.clone(), row.on_disk_size.clone(), row.state.clone()))
            {
                println!("{}\n General info:", alltablets.hostname_port.as_ref().unwrap());
                println!("  Keyspace:       {}", keyspace);
                println!("  Object name:    {}", table_name);
                println!("  On disk sizes:  {}", on_disk_size);
                println!("  State:          {}", state);
                if state == *"RUNNING" && alltablets.tabletdetail.iter().any(|row| row.as_ref().unwrap().tablet_id == *uuid)
                {
                    //
                    // consensus
                    //
                    println!(" Consensus:");
                    if let Some(consensus) = alltablets.tabletdetail.iter()
                        .find(|row| row.as_ref().unwrap().tablet_id == *uuid)
                        .map(|row| &row.as_ref().unwrap().consensus_status)
                    {
                        println!("  State:          {}", consensus.state);
                        println!("  Queue overview: {}", consensus.queue_overview.as_ref().unwrap_or(&"".to_string()));
                        println!("  Watermark:");
                        for watermark in &consensus.watermark
                        {
                            println!("  - {}", watermark.as_ref().unwrap().watermark);
                        }
                        println!("  Messages:");
                        for message in &consensus.messages
                        {
                            println!("  - Entry: {}, Opid: {}, mesg. type: {}, size: {}, status: {}",
                                message.as_ref().unwrap().entry,
                                message.as_ref().unwrap().opid,
                                message.as_ref().unwrap().message_type,
                                message.as_ref().unwrap().size,
                                message.as_ref().unwrap().status,
                            );

                        }
                    }
                    //
                    // Tablet LogAnchor
                    //
                    println!(" LogAnchor:");
                    for rows in alltablets.tabletdetail.iter()
                        .filter(|row| row.as_ref().unwrap().tablet_id == *uuid)
                        .map(|row| &row.as_ref().unwrap().tabletloganchor.loganchor)
                    {
                       for row in rows
                       {
                           println!("  {}", row);
                       }

                    }
                    //
                    // Transactions
                    //
                    println!(" Transactions:");
                    for rows in alltablets.tabletdetail.iter()
                        .filter(|row| row.as_ref().unwrap().tablet_id == *uuid)
                        .map(|row| row.as_ref().unwrap().transactions.transactions.clone())
                    {
                        for row in rows
                        {
                            println!("  - {}", row);
                        }
                    }
                    //
                    // Rocksdb
                    //
                    println!(" Rocksdb:");
                    println!("  IntentDB:");
                    for rows in alltablets.tabletdetail.iter()
                        .filter(|row| row.as_ref().unwrap().tablet_id == *uuid)
                        .map(|row| &row.as_ref().unwrap().rocksdb)
                    {
                        for file in &rows.intents_files
                        {
                            println!("   {} {}, {} {}, {} {}, {}",
                                     file.split_whitespace().nth(1).unwrap(),
                                     file.split_whitespace().nth(2).unwrap(),
                                     file.split_whitespace().nth(5).unwrap(),
                                     file.split_whitespace().nth(6).unwrap(),
                                     file.split_whitespace().nth(7).unwrap(),
                                     file.split_whitespace().nth(8).unwrap(),
                                     file.split_whitespace().nth(10).unwrap(),
                            );
                        }
                    }
                    println!("  RegularDB:");
                    for rows in alltablets.tabletdetail.iter()
                        .filter(|row| row.as_ref().unwrap().tablet_id == *uuid)
                        .map(|row| &row.as_ref().unwrap().rocksdb)
                    {
                        for file in &rows.regular_files
                        {
                            println!("   {} {}, {} {}, {} {}, {}",
                                     file.split_whitespace().nth(1).unwrap(),
                                     file.split_whitespace().nth(2).unwrap(),
                                     file.split_whitespace().nth(5).unwrap(),
                                     file.split_whitespace().nth(6).unwrap(),
                                     file.split_whitespace().nth(7).unwrap(),
                                     file.split_whitespace().nth(8).unwrap(),
                                     file.split_whitespace().nth(10).unwrap(),
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

pub async fn print_tablet_detail(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    match options.print_tablet_detail.as_ref().unwrap() {
        Some(snapshot_number) => {
            let mut alltablets = AllTablets::new();
            alltablets.tablet = snapshot::read_snapshot_json(snapshot_number, "tablets")?;
            alltablets.print(&options.uuid)?;
        },
        None => {
            let alltablets = AllTablets::read_tablets(&hosts, &ports, parallel, &options.extra_data).await;
            alltablets.print(&options.uuid)?;
        },
    }
    Ok(())
}

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
        assert_eq!(result.tabletbasic[0].on_disk_size, "Total: 1.00M Consensus Metadata: 1.5K WAL Files: 1.00M SST Files: 0B SST Files Uncompressed: 0B");
        assert_eq!(result.tabletbasic[0].raftconfig, "LEADER: yb-2.local FOLLOWER: yb-1.local FOLLOWER: yb-3.local");
        assert_eq!(result.tabletbasic[0].last_status, "transactions0");
    }

    #[test]
    fn unit_parse_tablet_detail_consensus_status_only_state() {
        let tablets = r#"
    <div class='yb-main container-fluid'>
        <h1>Tablet 9a15172a8e72450693ae64edd546849b</h1>
        <h1>Raft Consensus State</h1>
        <h2>State</h2>
        <pre>Consensus queue metrics:Only Majority Done Ops: 0, In Progress Ops: 0, Cache: LogCacheStats(num_ops=0, bytes=0, disk_reads=0)</pre>
        <div class='yb-bottom-spacer'></div>
    </div>
        "#.to_string();
        let result = AllTablets::parse_tablet_detail_consensus_status(tablets);

        assert_eq!(result.state, "Consensus queue metrics:Only Majority Done Ops: 0, In Progress Ops: 0, Cache: LogCacheStats(num_ops=0, bytes=0, disk_reads=0)");
    }

    #[test]
    fn unit_parse_tablet_detail_consensus_status_full() {
        let tablets = r#"
    <div class='yb-main container-fluid'>
        <h1>Tablet 845b75004ef14c918a4b263c52f7583c</h1>
        <h1>Raft Consensus State</h1>
        <h2>State</h2>
        <pre>Consensus queue metrics:Only Majority Done Ops: 0, In Progress Ops: 0, Cache: LogCacheStats(num_ops=0, bytes=0, disk_reads=0)</pre>
        <h2>Queue overview</h2>
        <pre>Consensus queue metrics:Only Majority Done Ops: 0, In Progress Ops: 0, Cache: LogCacheStats(num_ops=0, bytes=0, disk_reads=0)</pre>
        <hr/>
        <h2>Queue details</h2>
        <h3>Watermarks</h3>
        <table>
            <tr>
                <th>Peer</th>
                <th>Watermark</th>
            </tr>
            <tr>
                <td>c4ba4bb2cea04a2eade78bed94406fb9</td>
                <td>{ peer: c4ba4bb2cea04a2eade78bed94406fb9 is_new: 0 last_received: 5.108 next_index: 109 last_known_committed_idx: 108 is_last_exchange_successful: 1 needs_remote_bootstrap: 0 member_type: VOTER num_sst_files: 0 last_applied: 5.108 }</td>
            </tr>
            <tr>
                <td>1f8a43d3668d4235a990e6045d4f59db</td>
                <td>{ peer: 1f8a43d3668d4235a990e6045d4f59db is_new: 0 last_received: 5.108 next_index: 109 last_known_committed_idx: 108 is_last_exchange_successful: 1 needs_remote_bootstrap: 0 member_type: VOTER num_sst_files: 0 last_applied: 5.108 }</td>
            </tr>
            <tr>
                <td>cea6e8a0388749d18ea9496401608c4e</td>
                <td>{ peer: cea6e8a0388749d18ea9496401608c4e is_new: 0 last_received: 5.108 next_index: 109 last_known_committed_idx: 108 is_last_exchange_successful: 1 needs_remote_bootstrap: 0 member_type: VOTER num_sst_files: 0 last_applied: 5.108 }</td>
            </tr>
        </table>
        <h3>Messages:</h3>
        <table>
            <tr>
                <th>Entry</th>
                <th>OpId</th>
                <th>Type</th>
                <th>Size</th>
                <th>Status</th>
            </tr>
            <tr>
                <th>0</th>
                <th>0.0</th>
                <td>REPLICATE UNKNOWN_OP</td>
                <td>6</td>
                <td>term: 0 index: 0</td>
            </tr>
        </table>
        <div class='yb-bottom-spacer'></div>
    </div>
        "#.to_string();
        let result = AllTablets::parse_tablet_detail_consensus_status(tablets);

        assert_eq!(result.state, "Consensus queue metrics:Only Majority Done Ops: 0, In Progress Ops: 0, Cache: LogCacheStats(num_ops=0, bytes=0, disk_reads=0)");
        assert_eq!(result.queue_overview, Some("Consensus queue metrics:Only Majority Done Ops: 0, In Progress Ops: 0, Cache: LogCacheStats(num_ops=0, bytes=0, disk_reads=0)".to_string()));
        assert_eq!(result.watermark[0].as_ref().unwrap().peer, "c4ba4bb2cea04a2eade78bed94406fb9".to_string());
        assert_eq!(result.watermark[0].as_ref().unwrap().watermark, "{ peer: c4ba4bb2cea04a2eade78bed94406fb9 is_new: 0 last_received: 5.108 next_index: 109 last_known_committed_idx: 108 is_last_exchange_successful: 1 needs_remote_bootstrap: 0 member_type: VOTER num_sst_files: 0 last_applied: 5.108 }".to_string());
        assert_eq!(result.watermark[1].as_ref().unwrap().peer, "1f8a43d3668d4235a990e6045d4f59db".to_string());
        assert_eq!(result.watermark[1].as_ref().unwrap().watermark, "{ peer: 1f8a43d3668d4235a990e6045d4f59db is_new: 0 last_received: 5.108 next_index: 109 last_known_committed_idx: 108 is_last_exchange_successful: 1 needs_remote_bootstrap: 0 member_type: VOTER num_sst_files: 0 last_applied: 5.108 }".to_string());
        assert_eq!(result.watermark[2].as_ref().unwrap().peer, "cea6e8a0388749d18ea9496401608c4e".to_string());
        assert_eq!(result.watermark[2].as_ref().unwrap().watermark, "{ peer: cea6e8a0388749d18ea9496401608c4e is_new: 0 last_received: 5.108 next_index: 109 last_known_committed_idx: 108 is_last_exchange_successful: 1 needs_remote_bootstrap: 0 member_type: VOTER num_sst_files: 0 last_applied: 5.108 }".to_string());
        assert_eq!(result.messages[0].as_ref().unwrap().entry, "0");
        assert_eq!(result.messages[0].as_ref().unwrap().opid, "0.0");
        assert_eq!(result.messages[0].as_ref().unwrap().message_type, "REPLICATE UNKNOWN_OP");
        assert_eq!(result.messages[0].as_ref().unwrap().size, "6");
        assert_eq!(result.messages[0].as_ref().unwrap().status, "term: 0 index: 0");
    }


    #[test]
    fn unit_parse_tablet_detail_rocksdb_no_rocksdb() {
        let tablets = r#"
    <div class='yb-main container-fluid'>
        <h1>RocksDB for Tablet 0f4244458b3e48b98d7802d3be456e64</h1>
        <div class='yb-bottom-spacer'></div>
    </div>
        "#.to_string();
        let result = AllTablets::parse_tablet_detail_rocksdb(tablets);

        assert_eq!(result.regular_options.len(), 0);
        assert_eq!(result.regular_files.len(), 0);
        assert_eq!(result.regular_files_detail.len(),  0);
        assert_eq!(result.intents_options.len(), 0);
        assert_eq!(result.intents_files.len(), 0);
        assert_eq!(result.intents_files_detail.len(), 0);
    }

    #[test]
    fn unit_parse_tablet_detail_rocksdb_empty_rocksdb() {
        let tablets = r#"
    <div class='yb-main container-fluid'><h1>RocksDB for Tablet 08372aa6c40c4a0c89dd3692f0b09563</h1>
        <h2>Regular</h2>
        <input type="checkbox" id="572a8320b8047e8ed945f2aa06d7f9f2" class="yb-collapsible-cb"/><label for="572a8320b8047e8ed945f2aa06d7f9f2"><h3>Options</h3></label>
        <pre>
        [Version]
          yugabyte_version=version 2.17.1.0 build 439 revision 8a09a531b55a0564fc186f69e93d5c56fb0f8b67 build_type RELEASE built at 03 Feb 2023 22:14:11 UTC

        [DBOptions]
          max_file_size_for_compaction=94545423876928
          info_log_level=INFO_LEVEL
          access_hint_on_compaction_start=NORMAL
          write_thread_max_yield_usec=100
          write_thread_slow_yield_usec=3
          enable_write_thread_adaptive_yield=false
          max_log_file_size=0
          stats_dump_period_sec=600
          max_manifest_file_size=18446744073709551615
          bytes_per_sync=1048576
          delayed_write_rate=2097152
          WAL_ttl_seconds=0
          allow_concurrent_memtable_write=false
          paranoid_checks=true
          writable_file_max_buffer_size=1048576
          WAL_size_limit_MB=0
          max_subcompactions=1
          wal_dir=/mnt/d0/yb-data/tserver/data/rocksdb/table-000033e8000030008000000000004000/tablet-08372aa6c40c4a0c89dd3692f0b09563
          wal_bytes_per_sync=0
          max_total_wal_size=0
          db_write_buffer_size=0
          keep_log_file_num=1000
          table_cache_numshardbits=4
          compaction_size_threshold_bytes=2147483648
          max_file_opening_threads=1
          initial_seqno=1125899906842624
          recycle_log_file_num=0
          num_reserved_small_compaction_threads=0
          random_access_max_buffer_size=1048576
          use_fsync=false
          max_open_files=5000
          skip_stats_update_on_db_open=false
          max_background_compactions=1
          error_if_exists=false
          manifest_preallocation_size=65536
          max_background_flushes=1
          is_fd_close_on_exec=true
          advise_random_on_open=true
          create_missing_column_families=false
          delete_obsolete_files_period_micros=21600000000
          create_if_missing=true
          disable_data_sync=false
          log_file_time_to_roll=0
          compaction_readahead_size=0
          use_adaptive_mutex=false
          enable_thread_tracking=false
          disableDataSync=false
          allow_fallocate=true
          skip_log_error_on_recovery=false
          allow_mmap_reads=false
          fail_if_options_file_error=false
          allow_os_buffer=true
          wal_recovery_mode=kTolerateCorruptedTailRecords
          db_log_dir=
          new_table_reader_for_compaction_inputs=false
          base_background_compactions=1
          allow_mmap_writes=false

        [CFOptions "default"]
          compaction_style=kCompactionStyleUniversal
          merge_operator=nullptr
          compaction_filter=nullptr
          num_levels=1
          table_factory=BlockBasedTable
          comparator=leveldb.BytewiseComparator
          max_sequential_skip_in_iterations=8
          soft_rate_limit=0
          max_bytes_for_level_base=10485760
          soft_pending_compaction_bytes_limit=0
          memtable_prefix_bloom_probes=6
          memtable_prefix_bloom_bits=0
          memtable_prefix_bloom_huge_page_tlb_size=0
          max_successive_merges=0
          arena_block_size=65536
          min_write_buffer_number_to_merge=1
          target_file_size_multiplier=1
          source_compaction_factor=1
          compression_per_level=
          max_bytes_for_level_multiplier=10
          compaction_filter_factory=nullptr
          max_write_buffer_number=2
          level0_stop_writes_trigger=2147483647
          compression=kSnappyCompression
          level0_file_num_compaction_trigger=5
          purge_redundant_kvs_while_flush=true
          max_write_buffer_number_to_maintain=0
          memtable_factory=SkipListFactory
          max_grandparent_overlap_factor=10
          expanded_compaction_factor=25
          hard_pending_compaction_bytes_limit=0
          inplace_update_num_locks=10000
          level0_slowdown_writes_trigger=2147483647
          level_compaction_dynamic_level_bytes=false
          filter_deletes=false
          verify_checksums_in_compaction=true
          min_partial_merge_operands=2
          paranoid_file_checks=false
          target_file_size_base=2097152
          optimize_filters_for_hits=false
          compaction_measure_io_stats=false
          prefix_extractor=nullptr
          bloom_locality=0
          write_buffer_size=134217728
          disable_auto_compactions=false
          inplace_update_support=false
          [TableOptions/BlockBasedTable "default"]
          format_version=2
          whole_key_filtering=true
          index_block_size=32768
          index_block_restart_interval=1
          block_size_deviation=10
          hash_index_allow_collision=true
          filter_block_size=65536
          min_keys_per_index_block=100
          block_size=32768
          block_restart_interval=16
          skip_table_builder_flush=false
          filter_policy=DocKeyV3Filter
          no_block_cache=false
          checksum=kCRC32c
          cache_index_and_filter_blocks=true
          index_type=kMultiLevelBinarySearch
          flush_block_policy_factory=FlushBlockBySizePolicyFactory
          </pre>
        <h3>Files</h3>
        <pre>
        </pre>
        <h2>Intents</h2>
        <input type="checkbox" id="bc2acb6dde99ea88a5454061d2b769ae" class="yb-collapsible-cb"/><label for="bc2acb6dde99ea88a5454061d2b769ae"><h3>Options</h3></label>
        <pre>
        [Version]
          yugabyte_version=version 2.17.1.0 build 439 revision 8a09a531b55a0564fc186f69e93d5c56fb0f8b67 build_type RELEASE built at 03 Feb 2023 22:14:11 UTC

        [DBOptions]
          max_file_size_for_compaction=94545423876928
          info_log_level=INFO_LEVEL
          access_hint_on_compaction_start=NORMAL
          write_thread_max_yield_usec=100
          write_thread_slow_yield_usec=3
          enable_write_thread_adaptive_yield=false
          max_log_file_size=0
          stats_dump_period_sec=600
          max_manifest_file_size=18446744073709551615
          bytes_per_sync=1048576
          delayed_write_rate=2097152
          WAL_ttl_seconds=0
          allow_concurrent_memtable_write=false
          paranoid_checks=true
          writable_file_max_buffer_size=1048576
          WAL_size_limit_MB=0
          max_subcompactions=1
          wal_dir=/mnt/d0/yb-data/tserver/data/rocksdb/table-000033e8000030008000000000004000/tablet-08372aa6c40c4a0c89dd3692f0b09563.intents
          wal_bytes_per_sync=0
          max_total_wal_size=0
          db_write_buffer_size=0
          keep_log_file_num=1000
          table_cache_numshardbits=4
          compaction_size_threshold_bytes=2147483648
          max_file_opening_threads=1
          initial_seqno=1125899906842624
          recycle_log_file_num=0
          num_reserved_small_compaction_threads=0
          random_access_max_buffer_size=1048576
          use_fsync=false
          max_open_files=5000
          skip_stats_update_on_db_open=false
          max_background_compactions=1
          error_if_exists=false
          manifest_preallocation_size=65536
          max_background_flushes=1
          is_fd_close_on_exec=true
          advise_random_on_open=true
          create_missing_column_families=false
          delete_obsolete_files_period_micros=21600000000
          create_if_missing=true
          disable_data_sync=false
          log_file_time_to_roll=0
          compaction_readahead_size=0
          use_adaptive_mutex=false
          enable_thread_tracking=false
          disableDataSync=false
          allow_fallocate=true
          skip_log_error_on_recovery=false
          allow_mmap_reads=false
          fail_if_options_file_error=false
          allow_os_buffer=true
          wal_recovery_mode=kTolerateCorruptedTailRecords
          db_log_dir=
          new_table_reader_for_compaction_inputs=false
          base_background_compactions=1
          allow_mmap_writes=false

        [CFOptions "default"]
          compaction_style=kCompactionStyleUniversal
          merge_operator=nullptr
          compaction_filter=nullptr
          num_levels=1
          table_factory=BlockBasedTable
          comparator=leveldb.BytewiseComparator
          max_sequential_skip_in_iterations=8
          soft_rate_limit=0
          max_bytes_for_level_base=10485760
          soft_pending_compaction_bytes_limit=0
          memtable_prefix_bloom_probes=6
          memtable_prefix_bloom_bits=0
          memtable_prefix_bloom_huge_page_tlb_size=0
          max_successive_merges=0
          arena_block_size=65536
          min_write_buffer_number_to_merge=1
          target_file_size_multiplier=1
          source_compaction_factor=1
          compression_per_level=
          max_bytes_for_level_multiplier=10
          compaction_filter_factory=DocDBIntentsCompactionFilterFactory
          max_write_buffer_number=2
          level0_stop_writes_trigger=2147483647
          compression=kSnappyCompression
          level0_file_num_compaction_trigger=5
          purge_redundant_kvs_while_flush=true
          max_write_buffer_number_to_maintain=0
          memtable_factory=SkipListFactory
          max_grandparent_overlap_factor=10
          expanded_compaction_factor=25
          hard_pending_compaction_bytes_limit=0
          inplace_update_num_locks=10000
          level0_slowdown_writes_trigger=2147483647
          level_compaction_dynamic_level_bytes=false
          filter_deletes=false
          verify_checksums_in_compaction=true
          min_partial_merge_operands=2
          paranoid_file_checks=false
          target_file_size_base=2097152
          optimize_filters_for_hits=false
          compaction_measure_io_stats=false
          prefix_extractor=nullptr
          bloom_locality=0
          write_buffer_size=134217728
          disable_auto_compactions=false
          inplace_update_support=false
          [TableOptions/BlockBasedTable "default"]
          format_version=2
          whole_key_filtering=true
          index_block_size=32768
          index_block_restart_interval=1
          block_size_deviation=10
          hash_index_allow_collision=true
          filter_block_size=65536
          min_keys_per_index_block=100
          block_size=32768
          block_restart_interval=16
          skip_table_builder_flush=false
          filter_policy=DocKeyV3Filter
          no_block_cache=false
          checksum=kCRC32c
          cache_index_and_filter_blocks=true
          index_type=kMultiLevelBinarySearch
          flush_block_policy_factory=FlushBlockBySizePolicyFactory
          </pre>
        <h3>Files</h3>
        <pre>
        </pre>
    <div class='yb-bottom-spacer'></div></div>
        "#.to_string();
        let result = AllTablets::parse_tablet_detail_rocksdb(tablets);

        assert_eq!(result.regular_options.len(), 1);
        assert_eq!(result.regular_files.len(), 0);
        assert_eq!(result.regular_files_detail.len(), 0);
        assert_eq!(result.intents_options.len(), 1);
        assert_eq!(result.intents_files.len(), 0);
        assert_eq!(result.intents_files_detail.len(), 0);
    }

    #[test]
    fn unit_parse_tablet_detail_rocksdb_files_regular() {
        let tablets = r#"
    <div class='yb-main container-fluid'><h1>RocksDB for Tablet 08372aa6c40c4a0c89dd3692f0b09563</h1>
        <h2>Regular</h2>
        <input type="checkbox" id="322d9a493f9fc182334907c0a5d5f1ad" class="yb-collapsible-cb"/><label for="322d9a493f9fc182334907c0a5d5f1ad"><h3>Options</h3></label>
        <pre>
        [Version]
          yugabyte_version=version 2.17.1.0 build 439 revision 8a09a531b55a0564fc186f69e93d5c56fb0f8b67 build_type RELEASE built at 03 Feb 2023 22:14:11 UTC

        [DBOptions]
          max_file_size_for_compaction=94034441347424
          info_log_level=INFO_LEVEL
          access_hint_on_compaction_start=NORMAL
          write_thread_max_yield_usec=100
          write_thread_slow_yield_usec=3
          enable_write_thread_adaptive_yield=false
          max_log_file_size=0
          stats_dump_period_sec=600
          max_manifest_file_size=18446744073709551615
          bytes_per_sync=1048576
          delayed_write_rate=2097152
          WAL_ttl_seconds=0
          allow_concurrent_memtable_write=false
          paranoid_checks=true
          writable_file_max_buffer_size=1048576
          WAL_size_limit_MB=0
          max_subcompactions=1
          wal_dir=/mnt/d0/yb-data/tserver/data/rocksdb/table-000033e8000030008000000000004000/tablet-08372aa6c40c4a0c89dd3692f0b09563
          wal_bytes_per_sync=0
          max_total_wal_size=0
          db_write_buffer_size=0
          keep_log_file_num=1000
          table_cache_numshardbits=4
          compaction_size_threshold_bytes=2147483648
          max_file_opening_threads=1
          initial_seqno=1125899906842624
          recycle_log_file_num=0
          num_reserved_small_compaction_threads=0
          random_access_max_buffer_size=1048576
          use_fsync=false
          max_open_files=5000
          skip_stats_update_on_db_open=false
          max_background_compactions=1
          error_if_exists=false
          manifest_preallocation_size=65536
          max_background_flushes=1
          is_fd_close_on_exec=true
          advise_random_on_open=true
          create_missing_column_families=false
          delete_obsolete_files_period_micros=21600000000
          create_if_missing=true
          disable_data_sync=false
          log_file_time_to_roll=0
          compaction_readahead_size=0
          use_adaptive_mutex=false
          enable_thread_tracking=false
          disableDataSync=false
          allow_fallocate=true
          skip_log_error_on_recovery=false
          allow_mmap_reads=false
          fail_if_options_file_error=false
          allow_os_buffer=true
          wal_recovery_mode=kTolerateCorruptedTailRecords
          db_log_dir=
          new_table_reader_for_compaction_inputs=false
          base_background_compactions=1
          allow_mmap_writes=false

        [CFOptions "default"]
          compaction_style=kCompactionStyleUniversal
          merge_operator=nullptr
          compaction_filter=nullptr
          num_levels=1
          table_factory=BlockBasedTable
          comparator=leveldb.BytewiseComparator
          max_sequential_skip_in_iterations=8
          soft_rate_limit=0
          max_bytes_for_level_base=10485760
          soft_pending_compaction_bytes_limit=0
          memtable_prefix_bloom_probes=6
          memtable_prefix_bloom_bits=0
          memtable_prefix_bloom_huge_page_tlb_size=0
          max_successive_merges=0
          arena_block_size=65536
          min_write_buffer_number_to_merge=1
          target_file_size_multiplier=1
          source_compaction_factor=1
          compression_per_level=
          max_bytes_for_level_multiplier=10
          compaction_filter_factory=nullptr
          max_write_buffer_number=2
          level0_stop_writes_trigger=2147483647
          compression=kSnappyCompression
          level0_file_num_compaction_trigger=5
          purge_redundant_kvs_while_flush=true
          max_write_buffer_number_to_maintain=0
          memtable_factory=SkipListFactory
          max_grandparent_overlap_factor=10
          expanded_compaction_factor=25
          hard_pending_compaction_bytes_limit=0
          inplace_update_num_locks=10000
          level0_slowdown_writes_trigger=2147483647
          level_compaction_dynamic_level_bytes=false
          filter_deletes=false
          verify_checksums_in_compaction=true
          min_partial_merge_operands=2
          paranoid_file_checks=false
          target_file_size_base=2097152
          optimize_filters_for_hits=false
          compaction_measure_io_stats=false
          prefix_extractor=nullptr
          bloom_locality=0
          write_buffer_size=134217728
          disable_auto_compactions=false
          inplace_update_support=false
          [TableOptions/BlockBasedTable "default"]
          format_version=2
          whole_key_filtering=true
          index_block_size=32768
          index_block_restart_interval=1
          block_size_deviation=10
          hash_index_allow_collision=true
          filter_block_size=65536
          min_keys_per_index_block=100
          block_size=32768
          block_restart_interval=16
          skip_table_builder_flush=false
          filter_policy=DocKeyV3Filter
          no_block_cache=false
          checksum=kCRC32c
          cache_index_and_filter_blocks=true
          index_type=kMultiLevelBinarySearch
          flush_block_policy_factory=FlushBlockBySizePolicyFactory
          </pre>
        <h3>Files</h3>
        <pre>
        { total_size: 1616571 base_size: 71889 uncompressed_size: 10043418 name_id: 84 db_path: /mnt/d0/yb-data/tserver/data/rocksdb/table-000033e8000030008000000000004000/tablet-08372aa6c40c4a0c89dd3692f0b09563 imported: 0 being_compacted: 0 column_family_name: default level: 0 smallest: { seqno: 1125899907442631 user_frontier: 0x000055861aecfc00 -> { op_id: 5.1643 hybrid_time: { physical: 1676022333759183 } history_cutoff: <invalid> hybrid_time_filter: <invalid> max_value_level_ttl_expiration_time: <invalid> primary_schema_version: <NULL> cotable_schema_versions: [] } } largest: { seqno: 1125899907510063 user_frontier: 0x000055861aecea10 -> { op_id: 5.1643 hybrid_time: { physical: 1676022333818649 } history_cutoff: <invalid> hybrid_time_filter: <invalid> max_value_level_ttl_expiration_time: <invalid> primary_schema_version: <NULL> cotable_schema_versions: [] } } }
        { total_size: 4813130 base_size: 148559 uncompressed_size: 29723903 name_id: 83 db_path: /mnt/d0/yb-data/tserver/data/rocksdb/table-000033e8000030008000000000004000/tablet-08372aa6c40c4a0c89dd3692f0b09563 imported: 0 being_compacted: 0 column_family_name: default level: 0 smallest: { seqno: 1125899907242629 user_frontier: 0x000055861987a310 -> { op_id: 5.1643 hybrid_time: { physical: 1676022333759183 } history_cutoff: <invalid> hybrid_time_filter: <invalid> max_value_level_ttl_expiration_time: <invalid> primary_schema_version: <NULL> cotable_schema_versions: [] } } largest: { seqno: 1125899907442630 user_frontier: 0x000055861987bb20 -> { op_id: 5.1643 hybrid_time: { physical: 1676022333818649 } history_cutoff: <invalid> hybrid_time_filter: <invalid> max_value_level_ttl_expiration_time: <invalid> primary_schema_version: <NULL> cotable_schema_versions: [] } } }
        { total_size: 4746155 base_size: 148554 uncompressed_size: 29723642 name_id: 82 db_path: /mnt/d0/yb-data/tserver/data/rocksdb/table-000033e8000030008000000000004000/tablet-08372aa6c40c4a0c89dd3692f0b09563 imported: 0 being_compacted: 0 column_family_name: default level: 0 smallest: { seqno: 1125899907042627 user_frontier: 0x000055861987b3b0 -> { op_id: 5.1643 hybrid_time: { physical: 1676022333759183 } history_cutoff: <invalid> hybrid_time_filter: <invalid> max_value_level_ttl_expiration_time: <invalid> primary_schema_version: <NULL> cotable_schema_versions: [] } } largest: { seqno: 1125899907242628 user_frontier: 0x000055861987acb0 -> { op_id: 5.1643 hybrid_time: { physical: 1676022333818649 } history_cutoff: <invalid> hybrid_time_filter: <invalid> max_value_level_ttl_expiration_time: <invalid> primary_schema_version: <NULL> cotable_schema_versions: [] } } }
        { total_size: 4811702 base_size: 148618 uncompressed_size: 29690683 name_id: 80 db_path: /mnt/d0/yb-data/tserver/data/rocksdb/table-000033e8000030008000000000004000/tablet-08372aa6c40c4a0c89dd3692f0b09563 imported: 0 being_compacted: 0 column_family_name: default level: 0 smallest: { seqno: 1125899906842625 user_frontier: 0x000055861aecf500 -> { op_id: 5.1643 hybrid_time: { physical: 1676022333759183 } history_cutoff: <invalid> hybrid_time_filter: <invalid> max_value_level_ttl_expiration_time: <invalid> primary_schema_version: <NULL> cotable_schema_versions: [] } } largest: { seqno: 1125899907042626 user_frontier: 0x000055861987bc70 -> { op_id: 5.1643 hybrid_time: { physical: 1676022333818649 } history_cutoff: <invalid> hybrid_time_filter: <invalid> max_value_level_ttl_expiration_time: <invalid> primary_schema_version: <NULL> cotable_schema_versions: [] } } }
        </pre>
        <h3>/mnt/d0/yb-data/tserver/data/rocksdb/table-000033e8000030008000000000004000/tablet-08372aa6c40c4a0c89dd3692f0b09563/000080.sst properties</h3>
        <pre># data blocks=807
        # data index blocks=1
        # filter blocks=2
        # entries=200002
        raw key size=6767057
        raw average key size=33.8349466505335
        raw value size=22775008
        raw average value size=113.873901260987
        data blocks total size=4663084
        data index size=25451
        filter blocks total size=130964
        filter index block size=40
        (estimated) table size=4819499
        filter policy name=DocKeyV3Filter
        </pre>
        <h3>/mnt/d0/yb-data/tserver/data/rocksdb/table-000033e8000030008000000000004000/tablet-08372aa6c40c4a0c89dd3692f0b09563/000082.sst properties</h3>
        <pre># data blocks=807
        # data index blocks=1
        # filter blocks=2
        # entries=200002
        raw key size=6800080
        raw average key size=34.0000599994
        raw value size=22775008
        raw average value size=113.873901260987
        data blocks total size=4597601
        data index size=25447
        filter blocks total size=130964
        filter index block size=41
        (estimated) table size=4754012
        filter policy name=DocKeyV3Filter
        </pre>
        <h3>/mnt/d0/yb-data/tserver/data/rocksdb/table-000033e8000030008000000000004000/tablet-08372aa6c40c4a0c89dd3692f0b09563/000083.sst properties</h3>
        <pre># data blocks=807
        # data index blocks=1
        # filter blocks=2
        # entries=200002
        raw key size=6800080
        raw average key size=34.0000599994
        raw value size=22775264
        raw average value size=113.875181248188
        data blocks total size=4664571
        data index size=25451
        filter blocks total size=130964
        filter index block size=41
        (estimated) table size=4820986
        filter policy name=DocKeyV3Filter
        </pre>
        <h3>/mnt/d0/yb-data/tserver/data/rocksdb/table-000033e8000030008000000000004000/tablet-08372aa6c40c4a0c89dd3692f0b09563/000084.sst properties</h3>
        <pre># data blocks=272
        # data index blocks=1
        # filter blocks=1
        # entries=67433
        raw key size=2292728
        raw average key size=34.000088977207
        raw value size=7678801
        raw average value size=113.873044355138
        data blocks total size=1544682
        data index size=8427
        filter blocks total size=65482
        filter index block size=22
        (estimated) table size=1618591
        filter policy name=DocKeyV3Filter
        </pre>
        <h2>Intents</h2>
        <input type="checkbox" id="6a82c850eaa2709293452bc20741f09e" class="yb-collapsible-cb"/><label for="6a82c850eaa2709293452bc20741f09e"><h3>Options</h3></label>
        <pre>
        [Version]
          yugabyte_version=version 2.17.1.0 build 439 revision 8a09a531b55a0564fc186f69e93d5c56fb0f8b67 build_type RELEASE built at 03 Feb 2023 22:14:11 UTC

        [DBOptions]
          max_file_size_for_compaction=94034441347424
          info_log_level=INFO_LEVEL
          access_hint_on_compaction_start=NORMAL
          write_thread_max_yield_usec=100
          write_thread_slow_yield_usec=3
          enable_write_thread_adaptive_yield=false
          max_log_file_size=0
          stats_dump_period_sec=600
          max_manifest_file_size=18446744073709551615
          bytes_per_sync=1048576
          delayed_write_rate=2097152
          WAL_ttl_seconds=0
          allow_concurrent_memtable_write=false
          paranoid_checks=true
          writable_file_max_buffer_size=1048576
          WAL_size_limit_MB=0
          max_subcompactions=1
          wal_dir=/mnt/d0/yb-data/tserver/data/rocksdb/table-000033e8000030008000000000004000/tablet-08372aa6c40c4a0c89dd3692f0b09563.intents
          wal_bytes_per_sync=0
          max_total_wal_size=0
          db_write_buffer_size=0
          keep_log_file_num=1000
          table_cache_numshardbits=4
          compaction_size_threshold_bytes=2147483648
          max_file_opening_threads=1
          initial_seqno=1125899906842624
          recycle_log_file_num=0
          num_reserved_small_compaction_threads=0
          random_access_max_buffer_size=1048576
          use_fsync=false
          max_open_files=5000
          skip_stats_update_on_db_open=false
          max_background_compactions=1
          error_if_exists=false
          manifest_preallocation_size=65536
          max_background_flushes=1
          is_fd_close_on_exec=true
          advise_random_on_open=true
          create_missing_column_families=false
          delete_obsolete_files_period_micros=21600000000
          create_if_missing=true
          disable_data_sync=false
          log_file_time_to_roll=0
          compaction_readahead_size=0
          use_adaptive_mutex=false
          enable_thread_tracking=false
          disableDataSync=false
          allow_fallocate=true
          skip_log_error_on_recovery=false
          allow_mmap_reads=false
          fail_if_options_file_error=false
          allow_os_buffer=true
          wal_recovery_mode=kTolerateCorruptedTailRecords
          db_log_dir=
          new_table_reader_for_compaction_inputs=false
          base_background_compactions=1
          allow_mmap_writes=false

        [CFOptions "default"]
          compaction_style=kCompactionStyleUniversal
          merge_operator=nullptr
          compaction_filter=nullptr
          num_levels=1
          table_factory=BlockBasedTable
          comparator=leveldb.BytewiseComparator
          max_sequential_skip_in_iterations=8
          soft_rate_limit=0
          max_bytes_for_level_base=10485760
          soft_pending_compaction_bytes_limit=0
          memtable_prefix_bloom_probes=6
          memtable_prefix_bloom_bits=0
          memtable_prefix_bloom_huge_page_tlb_size=0
          max_successive_merges=0
          arena_block_size=65536
          min_write_buffer_number_to_merge=1
          target_file_size_multiplier=1
          source_compaction_factor=1
          compression_per_level=
          max_bytes_for_level_multiplier=10
          compaction_filter_factory=DocDBIntentsCompactionFilterFactory
          max_write_buffer_number=2
          level0_stop_writes_trigger=2147483647
          compression=kSnappyCompression
          level0_file_num_compaction_trigger=5
          purge_redundant_kvs_while_flush=true
          max_write_buffer_number_to_maintain=0
          memtable_factory=SkipListFactory
          max_grandparent_overlap_factor=10
          expanded_compaction_factor=25
          hard_pending_compaction_bytes_limit=0
          inplace_update_num_locks=10000
          level0_slowdown_writes_trigger=2147483647
          level_compaction_dynamic_level_bytes=false
          filter_deletes=false
          verify_checksums_in_compaction=true
          min_partial_merge_operands=2
          paranoid_file_checks=false
          target_file_size_base=2097152
          optimize_filters_for_hits=false
          compaction_measure_io_stats=false
          prefix_extractor=nullptr
          bloom_locality=0
          write_buffer_size=134217728
          disable_auto_compactions=false
          inplace_update_support=false
          [TableOptions/BlockBasedTable "default"]
          format_version=2
          whole_key_filtering=true
          index_block_size=32768
          index_block_restart_interval=1
          block_size_deviation=10
          hash_index_allow_collision=true
          filter_block_size=65536
          min_keys_per_index_block=100
          block_size=32768
          block_restart_interval=16
          skip_table_builder_flush=false
          filter_policy=DocKeyV3Filter
          no_block_cache=false
          checksum=kCRC32c
          cache_index_and_filter_blocks=true
          index_type=kMultiLevelBinarySearch
          flush_block_policy_factory=FlushBlockBySizePolicyFactory
          </pre>
        <h3>Files</h3>
        <pre>
        </pre>
    <div class='yb-bottom-spacer'></div></div>
        "#.to_string();
        let result = AllTablets::parse_tablet_detail_rocksdb(tablets);

        assert_eq!(result.regular_options.len(), 1);
        assert_eq!(result.regular_files.len(), 4);
        assert_eq!(result.regular_files_detail.len(), 4);
        assert_eq!(result.intents_options.len(), 1);
        assert_eq!(result.intents_files.len(), 0);
        assert_eq!(result.intents_files_detail.len(), 0);
    }

    #[tokio::test]
    async fn integration_parse_tablets() {
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();

        let alltablets = AllTablets::read_tablets(&vec![&hostname], &vec![&port], 1, &true).await;
        // the tablet server returns more than one tablet.
        assert!(!alltablets.tablet.is_empty());
    }
}