//! The impls and functions
//!
use chrono::Local;
use std::{sync::mpsc::channel, time::Instant};
use scraper::{Html, Selector};
use log::*;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::drives::{AllDrives, Drives, Drive};

impl Drives {
    pub fn new() -> Self { Default::default() }
}
impl AllDrives {
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

        let alldrives = AllDrives::read_drives(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "drives", alldrives.drives)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub async fn read_drives (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllDrives
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
                        let mut drives = AllDrives::read_http(host, port);
                        drives.timestamp = Some(detail_snapshot_time);
                        drives.hostname_port = Some(format!("{}:{}", host, port));
                        tx.send(drives).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut alldrives = AllDrives::new();

        for drives in rx.iter().filter(|row| !row.drive.is_empty())
        {
            alldrives.drives.push(drives);
        }

        alldrives
    }
    fn read_http(
        host: &str,
        port: &str,
    ) -> Drives
    {
        let data_from_http = utility::http_get(host, port, "drives");
        AllDrives::parse_drives(data_from_http)
    }
    fn parse_drives(
        http_data: String
    ) -> Drives
    {
        let table_selector = Selector::parse("table").unwrap();
        let tr_selector = Selector::parse("tr").unwrap();
        let th_selector = Selector::parse("th").unwrap();
        let td_selector = Selector::parse("td").unwrap();
        let div_selector = Selector::parse("div.yb-main").unwrap();

        let mut drives = Drives::new();

        let html = Html::parse_document(&http_data);

        for div_select in html.select(&div_selector)
        {
            for table in div_select.select(&table_selector)
            {
                match table
                {
                    // Single table: Drives usage by subsystem
                    th
                    if th.select(&th_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Path"
                        && th.select(&th_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Used Space"
                        && th.select(&th_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default() == *"Total Space" =>
                        {
                            for tr in table.select(&tr_selector).skip(1)
                            {
                                drives.drive.push( Some( Drive {
                                    path: tr.select(&td_selector).next().map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    used_space: tr.select(&td_selector).nth(1).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                    total_space: tr.select(&td_selector).nth(2).map(|row| row.text().collect::<String>()).unwrap_or_default(),
                                }));
                            }
                        },
                    _ => {
                            warn!("Found a table that doesn't match specified headings, this shouldn't happen: {:#?}", table.clone());
                    },
                }
            }
        }
        drives
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
    fn unit_parse_drives_simple_one_drive() {
        let drives = r#"
    <div class='yb-main container-fluid'>
        <h1>Drives usage by subsystem</h1>
        <table class='table table-striped'>
            <tr>
                <th>Path</th>
                <th>Used Space</th>
                <th>Total Space</th>
            </tr>
            <tr>
                <td>/mnt/d0</td>
                <td>174.52M</td>
                <td>9.99G</td>
            </tr>
        </table>
        <div class='yb-bottom-spacer'></div>
    </div>
        "#.to_string();
        let result = AllDrives::parse_drives(drives);

        assert_eq!(result.drive.len(), 1);
        assert_eq!(result.drive[0].as_ref().unwrap().path, "/mnt/d0");
        assert_eq!(result.drive[0].as_ref().unwrap().used_space, "174.52M");
        assert_eq!(result.drive[0].as_ref().unwrap().total_space, "9.99G");
    }

    #[tokio::test]
    async fn integration_parse_master_drives() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let result = AllDrives::read_drives(&vec![&hostname], &vec![&port], 1).await;

        assert!(result.drives.len() > 0);
    }

    #[tokio::test]
    async fn integration_parse_tablet_server_drives() {
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();

        let result = AllDrives::read_drives(&vec![&hostname], &vec![&port], 1).await;

        assert!(result.drives.len() > 0);
    }
}