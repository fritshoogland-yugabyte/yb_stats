//! The impls and functions
//!
use chrono::Local;
use std::{sync::mpsc::channel, time::Instant};
use scraper::{Html, Selector};
use log::*;
use anyhow::Result;

use crate::isleader::AllIsLeader;
use crate::utility;
use crate::snapshot;
use crate::table_detail::{AllTables, Column, Tablet, Table, TableBasic, TableDetail, Task};
use crate::Opts;

impl Table {
    pub fn new() -> Self { Default::default() }
}
impl TableDetail {
    pub fn new() -> Self { Default::default() }
}
impl Column {
    pub fn new() -> Self { Default::default() }
}
impl Tablet {
    pub fn new() -> Self { Default::default() }
}
impl Task {
    pub fn new() -> Self { Default::default() }
}

impl AllTables {
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

        let alltables = AllTables::read_tables(hosts, ports, parallel, extra_data).await;
        snapshot::save_snapshot_json(snapshot_number, "tables", alltables.table)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub async fn read_tables (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
        extra_data: &bool,
    ) -> AllTables
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
                        let mut tables = AllTables::read_http(host, port, extra_data);
                        tables.timestamp = Some(detail_snapshot_time);
                        tables.hostname_port = Some(format!("{}:{}", host, port));
                        tx.send(tables).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut alltables = AllTables::new();

        for table in rx.iter().filter(|row| !row.tablebasic.is_empty())
        {
            alltables.table.push(table);
        }

        alltables
    }
    fn read_http(
        host: &str,
        port: &str,
        extra_data: &bool,
    ) -> Table
    {
        let data_from_http = utility::http_get(host, port, "tables");
        let mut table = AllTables::parse_tables(data_from_http);
        if *extra_data
        {
            AllTables::parse_tables_add_detail(host, port, &mut table);
        }
        table
    }
    fn parse_tables(
        http_data: String
    ) -> Table
    {
        let table_selector = Selector::parse("table").unwrap();
        let tr_selector = Selector::parse("tr").unwrap();
        let th_selector = Selector::parse("th").unwrap();
        let td_selector = Selector::parse("td").unwrap();
        let div_selector = Selector::parse("div.yb-main > div.panel").unwrap();
        let header_selector = Selector::parse("div.panel-heading > h2.panel-title").unwrap();

        let mut table = Table::new();

        let html = Html::parse_document(&http_data);

        // Things are a bit all over the place here.
        // If no user table and/or index exist, there is an heading 'User tables' and 'Index tables',
        // but it contains no html table.
        // System tables always contain an html table, because there have to be PostgreSQL catalog tables.
        // But system tables lack the html table column 'On-disk size' (for the obvious reason they do not have an on-disk size).
        // And if colocated databases are created, another new type 'pops up' here: parent tables.
        // ---
        //     <div class='yb-main container-fluid'>
        //         <div class='panel panel-default'>
        //             <div class='panel-heading'>
        //                 <h2 class='panel-title'>User tables</h2>
        //             </div>
        //             <div class='panel-body table-responsive'>
        //                 <table class='table table-responsive'>
        //                     <tr>
        //                         <th>Keyspace</th>
        //                         <th>Table Name</th>
        //                         <th>State</th>
        //                         <th>Message</th>
        //                         <th>UUID</th>
        //                         <th>YSQL OID</th>
        //                         <th>Hidden</th>
        //                         <th>On-disk size</th>
        //                     </tr>
        //                     <tr>
        //                         <td>yugabyte</td>
        //                         <td>
        //                             <a href="/table?id=000033e8000030008000000000004000">t</a>
        //                         </td>
        //                         <td>Running</td>
        //                         <td></td>
        //                         <td>000033e8000030008000000000004000</td>
        //                         <td>16384</td>
        //                         <td>false</td>
        //                         <td>
        //                             <ul>
        //                                 <li>Total: 3.00M
        //                                 <li>WAL Files: 3.00M
        //                                 <li>SST Files: 0B
        //                                 <li>SST Files Uncompressed: 0B
        //                             </ul>
        //                         </td>
        //                     </tr>
        //                 </table>
        //             </div>
        //             <!-- panel-body -->
        //         </div>
        //         <!-- panel -->
        //         <div class='panel panel-default'>
        //             <div class='panel-heading'>
        //                 <h2 class='panel-title'>Index tables</h2>
        //             </div>
        //             <div class='panel-body table-responsive'>There are no index tables.
        //             </div>
        //             <!-- panel-body -->
        //         </div>
        //         <!-- panel -->
        //         <div class='panel panel-default'>
        //             <div class='panel-heading'>
        //                 <h2 class='panel-title'>System tables</h2>
        //             </div>
        //             <div class='panel-body table-responsive'>
        //                 <table class='table table-responsive'>
        //                     <tr>
        //                         <th>Keyspace</th>
        //                         <th>Table Name</th>
        //                         <th>State</th>
        //                         <th>Message</th>
        //                         <th>UUID</th>
        //                         <th>YSQL OID</th>
        //                         <th>Hidden</th>
        //                     <tr>
        //                         <td>template1</td>
        //                         <td>
        //                             <a href="/table?id=000000010000300080000000000000af">pg_user_mapping_user_server_index</a>
        //                         </td>
        //                         <td>Running</td>
        //                         <td></td>
        //                         <td>000000010000300080000000000000af</td>
        //                         <td>175</td>
        //                         <td>false</td>
        //                     </tr>
        // ---
        // The things to watch for here:
        // - The tables have an html tags path that is selected by `div_selector` as the root of a user, index or system table.
        // - The user tables fragment shows a single table (t).
        // - The index tables fragment shows no indexes, and has no html table.
        // - The system tables fragment shows the beginning of the system (postgres catalog) tables.
        // - A new category 'Parent table' appears when colocated databases are used.

        for div_panel in html.select(&div_selector)
        {
            let table_type = div_panel.select(&header_selector).next().unwrap().text().collect::<String>();
            match div_panel.select(&table_selector).next()
            {
                Some(html_table) => {
                    match html_table
                    {
                        // Please mind the absence of the seventh column "On-disk size" is deliberate: this doesn't exist for "System tables"
                        th
                        if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Keyspace"
                            && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Table Name"
                            && th.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"State"
                            && th.select(&th_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Message"
                            && th.select(&th_selector).nth(4).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"UUID"
                            && th.select(&th_selector).nth(5).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"YSQL OID"
                            && th.select(&th_selector).nth(6).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Hidden" => {
                            for tr in html_table.select(&tr_selector).skip(1)
                            {
                                table.tablebasic.push( TableBasic {
                                    keyspace: tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    table_name: tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default().replace(['\n',' '], ""),
                                    state: tr.select(&td_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    message: tr.select(&td_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    uuid: tr.select(&td_selector).nth(4).map(|row| row.text().collect::<String>()).unwrap_or_default().replace(['\n', ' '], ""),
                                    ysql_oid: tr.select(&td_selector).nth(5).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    hidden: tr.select(&td_selector).nth(6).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    on_disk_size: tr.select(&td_selector).nth(7).map(|row| row.text().collect::<Vec<_>>()).unwrap_or_default().iter().map(|r| r.trim()).filter(|r| !r.is_empty()).collect::<Vec<_>>().join(" "),
                                    object_type: table_type.clone(),
                                });
                            }
                        },
                        _ => {
                            info!("Found a table that doesn't match specified heading for {}, this shouldn't happen.", table_type.clone());
                        },
                    }
                },
                None => debug!("Found table type: {}, but html table found", table_type),
            }
        }
        table
    }
    fn parse_tables_add_detail(
        host: &str,
        port: &str,
        tables: &mut Table
    )
    {
        for row in tables.tablebasic.iter_mut()
        {
            let data_from_http = utility::http_get(host, port, format!("table?id={}", row.uuid).as_str());
            let detail = AllTables::parse_table_detail(data_from_http, &row.uuid);
            tables.tabledetail.push(Some(detail));
        }
    }
    fn parse_table_detail(
        data_from_http: String,
        uuid: &str,
    ) -> TableDetail
    {
        let html = Html::parse_document(&data_from_http);
        let table_selector = Selector::parse("table").unwrap();
        let td_selector = Selector::parse("td").unwrap();
        let th_selector = Selector::parse("th").unwrap();
        let tr_selector = Selector::parse("tr").unwrap();
        let mut table_detail = TableDetail::new();
        table_detail.uuid = uuid.to_string();

        // The first table is a table without an heading, acting as key-value
        // This table shows some table properties
        let first_table = html.select(&table_selector).next().expect("First html table in /table?id=UUID page (general info) should exist");
        for row in first_table.select(&tr_selector)
        {
            match row
            {
                tr if tr.select(&td_selector).next().map(|r| r.text().collect::<String>()).unwrap_or_default() == "Version:" => {
                    table_detail.version = tr.select(&td_selector).nth(1).map(|r| r.text().collect::<String>()).unwrap_or_default();
                },
                tr if tr.select(&td_selector).next().map(|r| r.text().collect::<String>()).unwrap_or_default() == "Type:" => {
                    table_detail.detail_type = tr.select(&td_selector).nth(1).map(|r| r.text().collect::<String>()).unwrap_or_default();
                },
                tr if tr.select(&td_selector).next().map(|r| r.text().collect::<String>()).unwrap_or_default() == "State:" => {
                    table_detail.state = tr.select(&td_selector).nth(1).map(|r| r.text().collect::<String>()).unwrap_or_default().trim().to_string();
                },
                tr if tr.select(&td_selector).next().map(|r| r.text().collect::<String>()).unwrap_or_default() == "Replication Info:" => {
                    table_detail.replication_info = tr.select(&td_selector).nth(1).map(|r| r.text().collect::<String>()).unwrap_or_default().trim().to_string();
                },
                unknown => info!("Unknown table row found: {:?}", unknown),
            }
        }

        // The second table is a regular table with heading
        // This table contains the columns
        let second_table = html.select(&table_selector).nth(1).expect("Second html table in /table?id=UUID page (columns) should exist");
        match second_table
        {
            th
            if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Column"
                && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"ID"
                && th.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Type" => {
                // skip heading
                for tr in second_table.select(&tr_selector).skip(1)
                {
                    let mut column = Column::new();
                    // It looks to me like the table column definitions are a bit off logically:
                    // The first table data column is defined as table header again, probably to make the column name bold typefaced.
                    column.column = tr.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default();
                    column.id = tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default();
                    column.column_type = tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default();
                    table_detail.columns.push(Some(column));
                }
            },
            non_matching_table => {
                    info!("Found another table, this shouldn't happen: {:?}.", non_matching_table);
            },
        }

        // The third table is a regular table with heading
        // This table contains the tablets
        let third_table = html.select(&table_selector).nth(2).expect("Third html table in /table?id=UUID page (tablets) should exist");
        match third_table
        {
            th
            if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Tablet ID"
                && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Partition"
                && th.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"SplitDepth"
                && th.select(&th_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"State"
                && th.select(&th_selector).nth(4).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Hidden"
                && th.select(&th_selector).nth(5).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Message"
                && th.select(&th_selector).nth(6).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"RaftConfig" => {
                // skip heading
                for tr in third_table.select(&tr_selector).skip(1)
                {
                    let mut tablet = Tablet::new();
                    // It looks to me like the table column definitions are a bit off logically:
                    // The first table data column is defined as table header again, probably to make the column name bold typefaced.
                    tablet.id = tr.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default();
                    tablet.partition = tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default();
                    tablet.split_depth = tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default();
                    tablet.state = tr.select(&td_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default();
                    tablet.hidden = tr.select(&td_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default();
                    tablet.message = tr.select(&td_selector).nth(4).map(|row| row.text().collect::<String>()).unwrap_or_default();
                    tablet.raftconfig = tr.select(&td_selector).nth(5).map(|row| row.text().map(|r| r.trim()).filter(|r| !r.is_empty()).collect::<Vec<_>>()).unwrap_or_default().join(" ");
                    table_detail.tablets.push(Some(tablet));
                }
            },
            non_matching_table => {
                info!("Found another table, this shouldn't happen: {:?}.", non_matching_table);
            },
        }

        // The third table is a regular table with heading
        // This table contains the tablets
        let fourth_table = html.select(&table_selector).nth(3).expect("Fourth html table in /table?id=UUID page (tasks) should exist");
        match fourth_table
        {
            th
            if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Task Name"
                && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"State"
                && th.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Start Time"
                && th.select(&th_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Duration"
                && th.select(&th_selector).nth(4).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Description" => {
                // skip heading
                for tr in fourth_table.select(&tr_selector).skip(1)
                {
                    let mut task = Task::new();
                    // It looks to me like the table column definitions are a bit off logically:
                    // The first table data column is defined as table header again, probably to make the column name bold typefaced.
                    task.task_name = tr.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default();
                    task.state = tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default();
                    task.start_time = tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default();
                    task.duration = tr.select(&td_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default();
                    task.description = tr.select(&td_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default();
                    table_detail.tasks.push(Some(task));
                }
            },
            non_matching_table => {
                info!("Found another table, this shouldn't happen: {:?}.", non_matching_table);
            },
        }

        table_detail
    }
    pub fn print(
        &self,
        uuid: &str,
        leader_hostname: String,
    ) -> Result<()>
    {
        for alltables in &self.table
        {
            if alltables.hostname_port != Some(leader_hostname.clone())
            {
                continue;
            }
            if let Some((keyspace, table_name, on_disk_size, object_type)) = alltables.tablebasic.iter()
                .find(|row| row.uuid == *uuid)
                .map(|row| (row.keyspace.clone(), row.table_name.clone(), row.on_disk_size.clone(), row.object_type.clone()))
            {
                // We want to warn about not using the --extra-data switch
                #[allow(clippy::collapsible_else_if)]
                if alltables.tabledetail.is_empty()
                {
                    println!("print table detail requires the --extra-data switch");
                    return Ok(());
                }
                else
                {
                    if let Some(table_detail) = alltables.tabledetail.iter()
                        .find(|row| row.as_ref().unwrap().uuid == *uuid)
                    {
                        println!("Table UUID: {}, version: {}, type: {}, state: {}, keyspace: {}, object_type: {}, name: {}",
                            uuid.to_owned(),
                            table_detail.as_ref().unwrap().version,
                            table_detail.as_ref().unwrap().detail_type,
                            table_detail.as_ref().unwrap().state,
                            keyspace,
                            object_type,
                            table_name,
                        );
                        println!("On disk size: {}", on_disk_size);
                        println!("Replication info: {}", table_detail.as_ref().unwrap().replication_info);
                        println!("Columns:");
                        for column in &table_detail.as_ref().unwrap().columns
                        {
                            println!("{:4} {:32} {}",
                                column.as_ref().unwrap().id,
                                column.as_ref().unwrap().column,
                                column.as_ref().unwrap().column_type,
                            );
                        }
                        println!("Tablets:");
                        for tablets in &table_detail.as_ref().unwrap().tablets
                        {
                            println!("{} {}, Split depth: {}, State: {}, Hidden: {}, Message: {}, Raft: {}",
                                tablets.as_ref().unwrap().id,
                                tablets.as_ref().unwrap().partition,
                                tablets.as_ref().unwrap().split_depth,
                                tablets.as_ref().unwrap().state,
                                tablets.as_ref().unwrap().hidden,
                                tablets.as_ref().unwrap().message,
                                tablets.as_ref().unwrap().raftconfig,
                            );
                        }
                        println!("Tasks:");
                        for tasks in &table_detail.as_ref().unwrap().tasks
                        {
                            println!("{} {} {} {} {} {}",
                                tasks.as_ref().unwrap().task_name,
                                tasks.as_ref().unwrap().task_name,
                                tasks.as_ref().unwrap().state,
                                tasks.as_ref().unwrap().start_time,
                                tasks.as_ref().unwrap().duration,
                                tasks.as_ref().unwrap().description,
                            );
                        }
                    }
                    else
                    {
                        println!("Error: no table detail found");
                    }
                }
            }
            else
            {
                println!("UUID: {} not found", uuid.to_owned());
            }

        }

        Ok(())
    }
}

pub async fn print_table_detail(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    match options.print_table_detail.as_ref().unwrap() {
        Some(snapshot_number) => {
            let mut alltables = AllTables::new();
            alltables.table = snapshot::read_snapshot_json(snapshot_number, "tables")?;
            let leader_hostname = AllIsLeader::return_leader_snapshot(snapshot_number)?;
            alltables.print(&options.uuid, leader_hostname)?;
        },
        None => {
            let alltables = AllTables::read_tables(&hosts, &ports, parallel, &options.extra_data).await;
            let leader_hostname = AllIsLeader::return_leader_http(&hosts, &ports, parallel).await;
            alltables.print(&options.uuid, leader_hostname)?;
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_basic_new_no_user_tables() {
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

        assert_eq!(result.tablebasic.len(), 1);
        //
        assert_eq!(result.tablebasic[0].keyspace, "template1");
        assert_eq!(result.tablebasic[0].table_name, "pg_user_mapping_user_server_index");
        assert_eq!(result.tablebasic[0].state, "Running");
        assert_eq!(result.tablebasic[0].message, "");
        assert_eq!(result.tablebasic[0].uuid, "000000010000300080000000000000af");
        assert_eq!(result.tablebasic[0].ysql_oid, "175");
        assert_eq!(result.tablebasic[0].hidden, "false");
        assert_eq!(result.tablebasic[0].on_disk_size, "");
        assert_eq!(result.tablebasic[0].object_type, "System tables");
    }

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
        assert_eq!(result.tablebasic[0].on_disk_size, "Total: 3.00M WAL Files: 3.00M SST Files: 0B SST Files Uncompressed: 0B");
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
        assert_eq!(result.tablebasic[0].on_disk_size, "Total: 3.00M WAL Files: 3.00M SST Files: 0B SST Files Uncompressed: 0B");
        assert_eq!(result.tablebasic[0].keyspace, "yugabyte");
        assert_eq!(result.tablebasic[0].object_type, "User tables");
        assert_eq!(result.tablebasic[1].table_name, "t_i");
        assert_eq!(result.tablebasic[1].state, "Running");
        assert_eq!(result.tablebasic[1].message, "");
        assert_eq!(result.tablebasic[1].uuid, "000033e8000030008000000000004003");
        assert_eq!(result.tablebasic[1].ysql_oid, "16387");
        assert_eq!(result.tablebasic[1].hidden, "false");
        assert_eq!(result.tablebasic[1].on_disk_size, "Total: 3.00M WAL Files: 3.00M SST Files: 0B SST Files Uncompressed: 0B");
        assert_eq!(result.tablebasic[1].object_type, "Index tables");
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
        assert_eq!(result.tablebasic[0].on_disk_size, "Total: 1.00M WAL Files: 1.00M SST Files: 0B SST Files Uncompressed: 0B");
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

        assert_eq!(result.version, "0");
        assert_eq!(result.detail_type, "PGSQL_TABLE_TYPE");
        assert_eq!(result.state, "Running");
        assert_eq!(result.replication_info, "");
        assert_eq!(result.columns[0].as_ref().unwrap().column, "id");
        assert_eq!(result.columns[0].as_ref().unwrap().id, "0");
        assert_eq!(result.columns[0].as_ref().unwrap().column_type, "int32 NOT NULL PARTITION KEY");
        assert_eq!(result.columns[1].as_ref().unwrap().column, "f1");
        assert_eq!(result.columns[1].as_ref().unwrap().id, "1");
        assert_eq!(result.columns[1].as_ref().unwrap().column_type, "string NULLABLE NOT A PARTITION KEY");
        assert_eq!(result.tablets[0].as_ref().unwrap().id, "5a4f24f5159f4c518bf3bcc5e2c4193c");
        assert_eq!(result.tablets[0].as_ref().unwrap().partition, "hash_split: [0x0000, 0xFFFF]");
        assert_eq!(result.tablets[0].as_ref().unwrap().split_depth, "0");
        assert_eq!(result.tablets[0].as_ref().unwrap().state, "Running");
        assert_eq!(result.tablets[0].as_ref().unwrap().hidden, "false");
        assert_eq!(result.tablets[0].as_ref().unwrap().message, "Tablet reported with an active leader");
        assert_eq!(result.tablets[0].as_ref().unwrap().raftconfig, "LEADER: yb-3.local FOLLOWER: yb-1.local FOLLOWER: yb-2.local");
        assert_eq!(result.tasks.len(), 0);
    }
    #[test]
    fn unit_parse_table_detail_tasks() {
        let tables = r#"
    <div class='yb-main container-fluid'>
        <h1>Table: yugabyte.t (000033e8000030008000000000004205) </h1>
        <table class='table table-striped'>
            <tr>
                <td>Version:</td>
                <td>1</td>
            </tr>
            <tr>
                <td>Type:</td>
                <td>PGSQL_TABLE_TYPE</td>
            </tr>
            <tr>
                <td>State:</td>
                <td>AlteringCurrent schema version=1</td>
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
                <th>f1d6be1540054591b12c575df06345e5</th>
                <td>hash_split: [0xAAAA, 0xFFFF]</td>
                <td>0</td>
                <td>Running</td>
                <td>false</td>
                <td>Tablet reported with an active leader</td>
                <td>
                    <ul>
                        <li>
                            FOLLOWER:
                            <a href="http://yb-3.local:9000/tablet?id=f1d6be1540054591b12c575df06345e5">yb-3.local</a>
                        </li>
                        <li>
                            FOLLOWER:
                            <a href="http://yb-1.local:9000/tablet?id=f1d6be1540054591b12c575df06345e5">yb-1.local</a>
                        </li>
                        <li>
                            <b>
                                LEADER:
                                <a href="http://yb-2.local:9000/tablet?id=f1d6be1540054591b12c575df06345e5">yb-2.local</a>
                            </b>
                        </li>
                    </ul>
                </td>
            </tr>
            <tr>
                <th>3366c516ed2a46d48811a15f11741c52</th>
                <td>hash_split: [0x5555, 0xAAA9]</td>
                <td>0</td>
                <td>Running</td>
                <td>false</td>
                <td>Tablet reported with an active leader</td>
                <td>
                    <ul>
                        <li>
                            <b>
                                LEADER:
                                <a href="http://yb-3.local:9000/tablet?id=3366c516ed2a46d48811a15f11741c52">yb-3.local</a>
                            </b>
                        </li>
                        <li>
                            FOLLOWER:
                            <a href="http://yb-1.local:9000/tablet?id=3366c516ed2a46d48811a15f11741c52">yb-1.local</a>
                        </li>
                        <li>
                            FOLLOWER:
                            <a href="http://yb-2.local:9000/tablet?id=3366c516ed2a46d48811a15f11741c52">yb-2.local</a>
                        </li>
                    </ul>
                </td>
            </tr>
            <tr>
                <th>d8bf132251e441adb78d0f32165050a2</th>
                <td>hash_split: [0x0000, 0x5554]</td>
                <td>0</td>
                <td>Running</td>
                <td>false</td>
                <td>Tablet reported with an active leader</td>
                <td>
                    <ul>
                        <li>
                            FOLLOWER:
                            <a href="http://yb-3.local:9000/tablet?id=d8bf132251e441adb78d0f32165050a2">yb-3.local</a>
                        </li>
                        <li>
                            <b>
                                LEADER:
                                <a href="http://yb-1.local:9000/tablet?id=d8bf132251e441adb78d0f32165050a2">yb-1.local</a>
                            </b>
                        </li>
                        <li>
                            FOLLOWER:
                            <a href="http://yb-2.local:9000/tablet?id=d8bf132251e441adb78d0f32165050a2">yb-2.local</a>
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
            <tr>
                <th>Backfill Index Table</th>
                <td>kRunning</td>
                <td>21.2 s ago</td>
                <td>21.2 s</td>
                <td>Backfilling indexes { t_f1_idx } for tablet 3366c516ed2a46d48811a15f11741c52 from key '4774d948800d451d21212380015dffe7dccaa6804a'</td>
            </tr>
            <tr>
                <th>Backfill Index Table</th>
                <td>kRunning</td>
                <td>21.1 s ago</td>
                <td>21.1 s</td>
                <td>Backfilling indexes { t_f1_idx } for tablet f1d6be1540054591b12c575df06345e5 from key '47d6ea488005c47421212380015dffe7dccaa6804a'</td>
            </tr>
            <tr>
                <th>Backfill Index Table</th>
                <td>kRunning</td>
                <td>21.1 s ago</td>
                <td>21.1 s</td>
                <td>Backfilling indexes { t_f1_idx } for tablet d8bf132251e441adb78d0f32165050a2 from key '47281d488004bd6d21212380015dffe7dccaa6804a'</td>
            </tr>
        </table>
        <div class='yb-bottom-spacer'></div>
    </div>
        "#.to_string();
        let result = AllTables::parse_table_detail( tables,"deadbeef");

        assert_eq!(result.version, "1");
        assert_eq!(result.detail_type, "PGSQL_TABLE_TYPE");
        assert_eq!(result.state, "AlteringCurrent schema version=1");
        assert_eq!(result.replication_info, "");
        assert_eq!(result.columns[0].as_ref().unwrap().column, "id");
        assert_eq!(result.columns[0].as_ref().unwrap().id, "0");
        assert_eq!(result.columns[0].as_ref().unwrap().column_type, "int32 NOT NULL PARTITION KEY");
        assert_eq!(result.columns[1].as_ref().unwrap().column, "f1");
        assert_eq!(result.columns[1].as_ref().unwrap().id, "1");
        assert_eq!(result.columns[1].as_ref().unwrap().column_type, "string NULLABLE NOT A PARTITION KEY");
        assert_eq!(result.tablets[0].as_ref().unwrap().id, "f1d6be1540054591b12c575df06345e5");
        assert_eq!(result.tablets[0].as_ref().unwrap().partition, "hash_split: [0xAAAA, 0xFFFF]");
        assert_eq!(result.tablets[0].as_ref().unwrap().split_depth, "0");
        assert_eq!(result.tablets[0].as_ref().unwrap().state, "Running");
        assert_eq!(result.tablets[0].as_ref().unwrap().hidden, "false");
        assert_eq!(result.tablets[0].as_ref().unwrap().message, "Tablet reported with an active leader");
        assert_eq!(result.tablets[0].as_ref().unwrap().raftconfig, "FOLLOWER: yb-3.local FOLLOWER: yb-1.local LEADER: yb-2.local");
        assert_eq!(result.tasks[0].as_ref().unwrap().task_name, "Backfill Index Table");
        assert_eq!(result.tasks[0].as_ref().unwrap().state, "kRunning");
        assert_eq!(result.tasks[0].as_ref().unwrap().start_time, "21.2 s ago");
        assert_eq!(result.tasks[0].as_ref().unwrap().duration, "21.2 s");
        assert_eq!(result.tasks[0].as_ref().unwrap().description, "Backfilling indexes { t_f1_idx } for tablet 3366c516ed2a46d48811a15f11741c52 from key '4774d948800d451d21212380015dffe7dccaa6804a'");
    }


    #[tokio::test]
    async fn integration_parse_master_tables() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let alltables = AllTables::read_tables(&vec![&hostname], &vec![&port], 1, &true).await;
        // the master returns more than one thread.
        assert!(alltables.table.is_empty());
    }
}