//! The impls and functions
//!
use chrono::Local;
use std::{sync::mpsc::channel, time::Instant};
use scraper::{Html, Selector};
use log::*;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::tasks::{AllTasks, TaskDetail, Tasks};

impl Tasks {
    pub fn new() -> Self{ Default::default() }
}
impl AllTasks {
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

        let alltasks = AllTasks::read_tasks(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "tasks", alltasks.tasks)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub async fn read_tasks (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllTasks
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
                        let mut tasks = AllTasks::read_http(host, port);
                        tasks.timestamp = Some(detail_snapshot_time);
                        tasks.hostname_port = Some(format!("{}:{}", host, port));
                        tx.send(tasks).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut alltasks = AllTasks::new();

        for tasks in rx.iter().filter(|row| !row.tasks.is_empty())
        {
            alltasks.tasks.push(tasks);
        }

        alltasks
    }
    fn read_http(
        host: &str,
        port: &str,
    ) -> Tasks
    {
        let data_from_http = utility::http_get(host, port, "tasks");
        AllTasks::parse_tasks(data_from_http)
    }
    fn parse_tasks(
        http_data: String
    ) -> Tasks
    {
        let table_selector = Selector::parse("table").unwrap();
        let tr_selector = Selector::parse("tr").unwrap();
        let th_selector = Selector::parse("th").unwrap();
        let td_selector = Selector::parse("td").unwrap();
        let div_selector = Selector::parse("div.yb-main").unwrap();

        let mut tasks = Tasks::new();

        let html = Html::parse_document(&http_data);

        for div_select in html.select(&div_selector)
        {
            for table in div_select.select(&table_selector)
            {
                match table
                {
                    // First table: Active Tasks
                    th
                    if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Task Name"
                            && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"State"
                            && th.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Start Time"
                            && th.select(&th_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Time"
                            && th.select(&th_selector).nth(4).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Description" =>
                    {
                            for tr in table.select(&tr_selector).skip(1)
                            {
                                tasks.tasks.push( Some(TaskDetail {
                                    task_type: "task".to_string(),
                                    status: "active".to_string(),
                                    // mind the th selector here!
                                    name: tr.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    state: tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    start_time: tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    duration: tr.select(&td_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    description: tr.select(&td_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                }));
                            }
                    },
                        // Second table: Last 20 user-initiated jobs started n the past 24 hours
                        th
                        if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Job Name"
                            && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"State"
                            && th.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Start Time"
                            && th.select(&th_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Duration"
                            && th.select(&th_selector).nth(4).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Description" =>
                    {
                                for tr in table.select(&tr_selector).skip(1)
                                {
                                    tasks.tasks.push( Some(TaskDetail {
                                        task_type: "user job".to_string(),
                                        status: "done".to_string(),
                                        // mind the th selector here!
                                        name: tr.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                        state: tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                        start_time: tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                        duration: tr.select(&td_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                        description: tr.select(&td_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    }));
                                }
                    },
                        // Third table: last 100 tasks started in the past 300 seconds
                        th
                        if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Task Name"
                            && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"State"
                            && th.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Start Time"
                            && th.select(&th_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Duration"
                            && th.select(&th_selector).nth(4).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Description" =>
                    {
                                for tr in table.select(&tr_selector).skip(1)
                                {
                                    tasks.tasks.push( Some(TaskDetail {
                                        task_type: "task".to_string(),
                                        status: "done".to_string(),
                                        // mind the th selector here!
                                        name: tr.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                        state: tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                        start_time: tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                        duration: tr.select(&td_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                        description: tr.select(&td_selector).nth(3).map(|row| row.text().collect::<String>()).unwrap_or_default(),
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
    fn unit_parse_tasks_no_tasks() {
        let tasks = r#"
    <div class='yb-main container-fluid'>
        <h3>Active Tasks</h3>
        <table class='table table-striped'>
            <tr>
                <th>Task Name</th>
                <th>State</th>
                <th>Start Time</th>
                <th>Time</th>
                <th>Description</th>
            </tr>
        </table>
        <h3>Last 20 user-initiated jobs started in the past 24 hours</h3>
        <table class='table table-striped'>
            <tr>
                <th>Job Name</th>
                <th>State</th>
                <th>Start Time</th>
                <th>Duration</th>
                <th>Description</th>
            </tr>
        </table>
        <h3>Last 100 tasks started in the past 300 seconds</h3>
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
        let result = AllTasks::parse_tasks(tasks);

        assert_eq!(result.tasks.len(), 0);
        //
        /*
        assert_eq!(result.tablebasic[0].keyspace, "template1");
        assert_eq!(result.tablebasic[0].table_name, "pg_user_mapping_user_server_index");
        assert_eq!(result.tablebasic[0].state, "Running");
        assert_eq!(result.tablebasic[0].message, "");
        assert_eq!(result.tablebasic[0].uuid, "000000010000300080000000000000af");
        assert_eq!(result.tablebasic[0].ysql_oid, "175");
        assert_eq!(result.tablebasic[0].hidden, "false");
        assert_eq!(result.tablebasic[0].on_disk_size, "");
        assert_eq!(result.tablebasic[0].object_type, "System tables");

         */
    }
    #[test]
    fn unit_parse_tasks_done_tasks_delete_tablet() {
        let tasks = r#"
    <div class='yb-main container-fluid'>
        <h3>Active Tasks</h3>
        <table class='table table-striped'>
            <tr>
                <th>Task Name</th>
                <th>State</th>
                <th>Start Time</th>
                <th>Time</th>
                <th>Description</th>
            </tr>
        </table>
        <h3>Last 20 user-initiated jobs started in the past 24 hours</h3>
        <table class='table table-striped'>
            <tr>
                <th>Job Name</th>
                <th>State</th>
                <th>Start Time</th>
                <th>Duration</th>
                <th>Description</th>
            </tr>
        </table>
        <h3>Last 100 tasks started in the past 300 seconds</h3>
        <table class='table table-striped'>
            <tr>
                <th>Task Name</th>
                <th>State</th>
                <th>Start Time</th>
                <th>Duration</th>
                <th>Description</th>
            </tr>
            <tr>
                <th>Delete Tablet</th>
                <td>kComplete</td>
                <td>7.77 s ago</td>
                <td>41.3 ms</td>
                <td>DeleteTablet RPC for tablet d7dac11732064102b56bbe90edb3230a (t [id=000033e8000030008000000000005403]) on TS=214398b20f3b432ebbe4d1e82a395bd5</td>
            </tr>
        </table>
        <div class='yb-bottom-spacer'></div>
    </div>
        "#.to_string();
        let result = AllTasks::parse_tasks(tasks);

        assert_eq!(result.tasks.len(), 1);
        //
        assert_eq!(result.tasks[0].as_ref().unwrap().task_type, "task");
        assert_eq!(result.tasks[0].as_ref().unwrap().status, "done");
        assert_eq!(result.tasks[0].as_ref().unwrap().name, "Delete Tablet");
        assert_eq!(result.tasks[0].as_ref().unwrap().state, "kComplete");
        assert_eq!(result.tasks[0].as_ref().unwrap().start_time, "7.77 s ago");
        assert_eq!(result.tasks[0].as_ref().unwrap().duration, "41.3 ms");
        assert_eq!(result.tasks[0].as_ref().unwrap().description, "DeleteTablet RPC for tablet d7dac11732064102b56bbe90edb3230a (t [id=000033e8000030008000000000005403]) on TS=214398b20f3b432ebbe4d1e82a395bd5");
    }
    #[test]
    fn unit_parse_tasks_active_tasks() {
        let tasks = r#"
    <div class='yb-main container-fluid'>
        <h3>Active Tasks</h3>
        <table class='table table-striped'>
            <tr>
                <th>Task Name</th>
                <th>State</th>
                <th>Start Time</th>
                <th>Time</th>
                <th>Description</th>
            </tr>
             <tr>
                <th>Alter Table</th>
                <td>kRunning</td>
                <td>32.6 s ago</td>
                <td>32.6 s</td>
                <td>Alter Table RPC for tablet 0x00005592a0c982c0 -&gt; 143ce41b11104f7c8d5490a4c98587d4 (table t [id=000033e8000030008000000000004000]) (t [id=000033e8000030008000000000004000])</td>
            </tr>
        </table>
        <h3>Last 20 user-initiated jobs started in the past 24 hours</h3>
        <table class='table table-striped'>
            <tr>
                <th>Job Name</th>
                <th>State</th>
                <th>Start Time</th>
                <th>Duration</th>
                <th>Description</th>
            </tr>
        </table>
        <h3>Last 100 tasks started in the past 300 seconds</h3>
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
        let result = AllTasks::parse_tasks(tasks);

        assert_eq!(result.tasks.len(), 1);
        //
        assert_eq!(result.tasks[0].as_ref().unwrap().task_type, "task");
        assert_eq!(result.tasks[0].as_ref().unwrap().status, "active");
        assert_eq!(result.tasks[0].as_ref().unwrap().name, "Alter Table");
        assert_eq!(result.tasks[0].as_ref().unwrap().state, "kRunning");
        assert_eq!(result.tasks[0].as_ref().unwrap().start_time, "32.6 s ago");
        assert_eq!(result.tasks[0].as_ref().unwrap().duration, "32.6 s");
        assert_eq!(result.tasks[0].as_ref().unwrap().description, "Alter Table RPC for tablet 0x00005592a0c982c0 -> 143ce41b11104f7c8d5490a4c98587d4 (table t [id=000033e8000030008000000000004000]) (t [id=000033e8000030008000000000004000])");
    }

    #[tokio::test]
    async fn integration_parse_master_tasks() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let _alltasks = AllTasks::read_tasks(&vec![&hostname], &vec![&port], 1).await;
        // the master returns none or more tasks.
    }
}