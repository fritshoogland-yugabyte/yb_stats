//! The impls and functions
//!
use chrono::Local;
use std::{sync::mpsc::channel, time::Instant};
use regex::Regex;
use scraper::{Html, Selector};
use log::*;
use anyhow::Result;
use crate::utility;
use crate::snapshot;
use crate::threads::{Threads, AllThreads};
use crate::Opts;

impl AllThreads {
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

        let allthreads = AllThreads::read_threads(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "threads", allthreads.threads)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub async fn read_threads (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllThreads
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
                        let mut threads = AllThreads::read_http(host, port);
                        threads.iter_mut().for_each(|r| r.timestamp = detail_snapshot_time);
                        threads.iter_mut().for_each(|r| r.hostname_port = format!("{}:{}", host, port));
                        tx.send(threads).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allthreads = AllThreads::new();

        for threads in rx
        {
            for thread in threads
            {
                allthreads.threads.push(thread);
            }
        }

        allthreads
    }
    fn read_http(
        host: &str,
        port: &str,
    ) -> Vec<Threads>
    {
        let data_from_http = utility::http_get(host, port, "threadz?group=all");
        AllThreads::parse_threads(data_from_http)
    }
    fn parse_threads(
        http_data: String
    ) -> Vec<Threads>
    {
        let table_selector = Selector::parse("table").unwrap();
        let tr_selector = Selector::parse("tr").unwrap();
        let th_selector = Selector::parse("th").unwrap();
        let td_selector = Selector::parse("td").unwrap();

        let mut threads: Vec<Threads> = Vec::new();

        let html = Html::parse_document(&http_data);
        let function_regex = Regex::new(r"@\s+0x[[:xdigit:]]+\s+(\S+)\n").unwrap();

        // This is how the 'All Threads' table looks like:
        // ---
        // <table class='table table-hover table-border'>
        //             <tr>
        //                 <th>Thread name</th>
        //                 <th>Cumulative User CPU(s)</th>
        //                 <th>Cumulative Kernel CPU(s)</th>
        //                 <th>Cumulative IO-wait(s)</th>
        //             </tr>
        //             <tr>
        //                 <td>Master_reactorx-6721</td>
        //                 <td>2.050s</td>
        //                 <td>0.000s</td>
        //                 <td>0.000s</td>
        //                 <td rowspan="4">
        //                     <pre>    @     0x7f7d738c59f2  __GI_epoll_wait
        //                         @          0x3637986  epoll_poll
        //                         @          0x363a2be  ev_run
        //                         @          0x3672c69  yb::rpc::Reactor::RunThread()
        //                         @          0x3b8d771  yb::Thread::SuperviseThread()
        //                         @     0x7f7d733c3693  start_thread
        //                         @     0x7f7d738c541c  __clone
        //
        //                     Total number of threads: 4</pre>
        //                 </td>
        //             </tr>
        //             <tr>
        //                 <td>Master_reactorx-6720</td>
        //                 <td>1.990s</td>
        //                 <td>0.000s</td>
        //                 <td>0.000s</td>
        //             </tr>
        // ---
        // One very weird thing here is that the stack above is printed for the first table row only,
        // any following table row that has NO stack column is supposed to have the same stack.
        // For this the 'stack' variable is used that contains the stack:
        // - If tr.select(&td_selector).nth(4).is_some() then we fill it with the new stack,
        // - If tr.select(&td_selector).nth(4).is_none() then we do nothing, so the stack variable remains the same.
        for table in html.select(&table_selector)
        {
            let mut stack = String::new();
            match table
            {
                th
                if th.select(&th_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Thread name"
                    && th.select(&th_selector).nth(1).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Cumulative User CPU(s)"
                    && th.select(&th_selector).nth(2).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Cumulative Kernel CPU(s)"
                    && th.select(&th_selector).nth(3).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default() == *"Cumulative IO-wait(s)" =>
                    {
                        // The first row contains the table headings, so we skip the first row.
                        for tr in table.select(&tr_selector).skip(1)
                        {
                            // check if we got a fourth column or not for changing the stack variable
                            match tr.select(&td_selector).nth(4)
                            {
                                Some(found_stack) => {
                                    // if so, we collect the stack, and replace some HTMLisms to the correct characters.
                                    let original_stack = found_stack.text().collect::<String>().replace("&lt;", "<").replace("&gt;", ">");
                                    // This code collects the functions printed in the backtrace based on a regular expression into a vector.
                                    // Reverses the vector so that the first called function is first in order.
                                    // And then puts the functions in that order into the variable stack separated by ";".
                                    // I think this is most readable.
                                    let mut stack_vec = Vec::new();
                                    for capture in function_regex.captures_iter(&original_stack)
                                    {
                                       stack_vec.push(capture[1].to_string().clone())
                                    }
                                    stack_vec.reverse();
                                    stack = stack_vec.join(";");
                                }
                                None => {
                                    // This entry has no stack table row, so we reuse the previous stack.
                                }
                            }
                            threads.push( Threads{
                                thread_name: tr.select(&td_selector).next().and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                cumulative_user_cpu_s: tr.select(&td_selector).nth(1).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                cumulative_kernel_cpu_s: tr.select(&td_selector).nth(2).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                cumulative_iowait_cpu_s: tr.select(&td_selector).nth(3).and_then(|row| Some(row.text().collect::<String>())).unwrap_or_default(),
                                stack: stack.clone(),
                                ..Default::default()
                            })


                        }
                    },
                non_matching_table => {
                    info!("Found another table, this shouldn't happen: {:?}.", non_matching_table);
                },
            }
        }

        threads
    }
    pub fn print(
        &self,
        hostname_filter: &Regex
    ) -> Result<()>
    {
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
        Ok(())
    }
}

pub async fn print_threads(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    let hostname_filter = utility::set_regex(&options.hostname_match);
    match options.print_threads.as_ref().unwrap() {
        Some(snapshot_number) => {
            let mut allthreads = AllThreads::new();
            allthreads.threads = snapshot::read_snapshot_json(snapshot_number, "threads")?;
            allthreads.print(&hostname_filter)?;
        },
        None => {
            let allthreads = AllThreads::read_threads(&hosts, &ports, parallel).await;
            allthreads.print(&hostname_filter)?;
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_threads_data() {
        // This is what /threadz?group=all returns.
        let threads = r#"<!DOCTYPE html><html>  <head>    <title>YugabyteDB</title>    <link rel='shortcut icon' href='/favicon.ico'>    <link href='/bootstrap/css/bootstrap.min.css' rel='stylesheet' media='screen' />    <link href='/bootstrap/css/bootstrap-theme.min.css' rel='stylesheet' media='screen' />    <link href='/font-awesome/css/font-awesome.min.css' rel='stylesheet' media='screen' />    <link href='/yb.css' rel='stylesheet' media='screen' />  </head>
<body>
  <nav class="navbar navbar-fixed-top navbar-inverse sidebar-wrapper" role="navigation">    <ul class="nav sidebar-nav">      <li><a href='/'><img src='/logo.png' alt='YugabyteDB' class='nav-logo' /></a></li>
<li class='nav-item'><a href='/'><div><i class='fa fa-home'aria-hidden='true'></i></div>Home</a></li>
<li class='nav-item'><a href='/tables'><div><i class='fa fa-table'aria-hidden='true'></i></div>Tables</a></li>
<li class='nav-item'><a href='/tablet-servers'><div><i class='fa fa-server'aria-hidden='true'></i></div>Tablet Servers</a></li>
<li class='nav-item'><a href='/utilz'><div><i class='fa fa-wrench'aria-hidden='true'></i></div>Utilities</a></li>
    </ul>  </nav>

    <div class='yb-main container-fluid'><h2>Thread Group: all</h2>
<h3>All Threads : </h3><table class='table table-hover table-border'><tr><th>Thread name</th><th>Cumulative User CPU(s)</th><th>Cumulative Kernel CPU(s)</th><th>Cumulative IO-wait(s)</th></tr><tr><td>Master_reactorx-6127</td><td>2.960s</td><td>0.000s</td><td>0.000s</td><td rowspan="1"><pre>    @     0x7f035af7a9f2  __GI_epoll_wait
    @     0x7f035db7fbf7  epoll_poll
    @     0x7f035db7ac5d  ev_run
    @     0x7f035e02f07b  yb::rpc::Reactor::RunThread()
    @     0x7f035de5f1d4  yb::Thread::SuperviseThread()
    @     0x7f035b83e693  start_thread
    @     0x7f035af7a41c  __clone

Total number of threads: 1</pre></td></tr>
<tr><td>acceptorxxxxxxx-6136</td><td>0.000s</td><td>0.000s</td><td>0.000s</td><td rowspan="1"><pre>    @     0x7f035af7a9f2  __GI_epoll_wait
    @     0x7f035db7fbf7  epoll_poll
    @     0x7f035db7ac5d  ev_run
    @     0x7f035dff41d6  yb::rpc::Acceptor::RunThread()
    @     0x7f035de5f1d4  yb::Thread::SuperviseThread()
    @     0x7f035b83e693  start_thread
    @     0x7f035af7a41c  __clone

Total number of threads: 1</pre></td></tr>
<tr><td>bgtasksxxxxxxxx-6435</td><td>0.630s</td><td>0.000s</td><td>0.000s</td><td rowspan="1"><pre>    @     0x7f035b8433b7  __pthread_cond_timedwait
    @     0x7f035dd52946  yb::ConditionVariable::TimedWait()
    @     0x7f03610c21f0  yb::master::CatalogManagerBgTasks::Wait()
    @     0x7f03610c255b  yb::master::CatalogManagerBgTasks::Run()
    @     0x7f035de5f1d4  yb::Thread::SuperviseThread()
    @     0x7f035b83e693  start_thread
    @     0x7f035af7a41c  __clone

Total number of threads: 1</pre></td></tr>
<tr><td>iotp_Master_3xx-6126</td><td>0.160s</td><td>0.000s</td><td>0.000s</td><td rowspan="3"><pre>    @     0x7f035b84300c  __pthread_cond_wait
    @     0x7f035e00c927  yb::rpc::IoThreadPool::Impl::Execute()
    @     0x7f035de5f1d4  yb::Thread::SuperviseThread()
    @     0x7f035b83e693  start_thread
    @     0x7f035af7a41c  __clone

Total number of threads: 3</pre></td></tr>
<tr><td>iotp_Master_2xx-6125</td><td>0.230s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>iotp_Master_0xx-6123</td><td>0.200s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>iotp_Master_1xx-6124</td><td>0.220s</td><td>0.000s</td><td>0.000s</td><td rowspan="1"><pre>    @     0x7f035af7a9f2  __GI_epoll_wait
    @     0x7f035e00c9c6  yb::rpc::IoThreadPool::Impl::Execute()
    @     0x7f035de5f1d4  yb::Thread::SuperviseThread()
    @     0x7f035b83e693  start_thread
    @     0x7f035af7a41c  __clone

Total number of threads: 1</pre></td></tr>
<tr><td>iotp_call_home_0-6437</td><td>0.000s</td><td>0.000s</td><td>0.000s</td><td rowspan="1"><pre>    @     0x7f035af7a9f2  __GI_epoll_wait
    @     0x7f035e00c9c6  yb::rpc::IoThreadPool::Impl::Execute()
    @     0x7f035de5f1d4  yb::Thread::SuperviseThread()
    @     0x7f035b83e693  start_thread
    @     0x7f035af7a41c  __clone

Total number of threads: 1</pre></td></tr>
<tr><td>maintenance_scheduler-6130</td><td>1.370s</td><td>0.000s</td><td>0.000s</td><td rowspan="1"><pre>    @     0x7f035b8433b7  __pthread_cond_timedwait
    @     0x7f035c0217ea  std::__1::condition_variable::__do_timed_wait()
    @     0x7f0360ce4cd9  yb::MaintenanceManager::RunSchedulerThread()
    @     0x7f035de5f1d4  yb::Thread::SuperviseThread()
    @     0x7f035b83e693  start_thread
    @     0x7f035af7a41c  __clone

Total number of threads: 1</pre></td></tr>
<tr><td>rb-session-expx-6134</td><td>0.010s</td><td>0.000s</td><td>0.000s</td><td rowspan="1"><pre>    @     0x7f035b8433b7  __pthread_cond_timedwait
    @     0x7f035dd5287c  yb::ConditionVariable::WaitUntil()
    @     0x7f035dd52c46  yb::CountDownLatch::WaitFor()
    @     0x7f03617dbae3  yb::tserver::RemoteBootstrapServiceImpl::EndExpiredSessions()
    @     0x7f035de5f1d4  yb::Thread::SuperviseThread()
    @     0x7f035b83e693  start_thread
    @     0x7f035af7a41c  __clone

Total number of threads: 1</pre></td></tr>
<tr><td>rpc_tp_Master_16-6449</td><td>0.000s</td><td>0.000s</td><td>0.000s</td><td rowspan="18"><pre>    @     0x7f035b84300c  __pthread_cond_wait
    @     0x7f035c021751  std::__1::condition_variable::wait()
    @     0x7f035e06d1ce  yb::rpc::(anonymous namespace)::Worker::Execute()
    @     0x7f035de5f1d4  yb::Thread::SuperviseThread()
    @     0x7f035b83e693  start_thread
    @     0x7f035af7a41c  __clone

Total number of threads: 18</pre></td></tr>
<tr><td>rpc_tp_Master_15-6448</td><td>0.000s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_14-6447</td><td>0.000s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_13-6446</td><td>0.000s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_12-6445</td><td>0.000s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_11-6444</td><td>0.000s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_10-6443</td><td>0.000s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_9-6442</td><td>0.000s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_5-6438</td><td>0.000s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_7-6440</td><td>0.000s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_4-6390</td><td>0.020s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_8-6441</td><td>0.000s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_3-6381</td><td>0.020s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master-high-pri_0-6140</td><td>2.990s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_1-6139</td><td>0.020s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_6-6439</td><td>0.000s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_0-6137</td><td>0.020s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>rpc_tp_Master_2-6375</td><td>0.060s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>flush scheduler bgtask-6434</td><td>0.000s</td><td>0.000s</td><td>0.000s</td><td rowspan="1"><pre>    @     0x7f035b84300c  __pthread_cond_wait
    @     0x7f035c021751  std::__1::condition_variable::wait()
    @     0x7f035dd475ba  yb::BackgroundTask::WaitForJob()
    @     0x7f035dd47393  yb::BackgroundTask::Run()
    @     0x7f035de5f1d4  yb::Thread::SuperviseThread()
    @     0x7f035b83e693  start_thread
    @     0x7f035af7a41c  __clone

Total number of threads: 1</pre></td></tr>
<tr><td>MaintenanceMgr [worker]-6122</td><td>0.000s</td><td>0.000s</td><td>0.000s</td><td rowspan="4"><pre>    @     0x7f035b84300c  __pthread_cond_wait
    @     0x7f035de63bf9  yb::ThreadPool::DispatchThread()
    @     0x7f035de5f1d4  yb::Thread::SuperviseThread()
    @     0x7f035b83e693  start_thread
    @     0x7f035af7a41c  __clone

Total number of threads: 4</pre></td></tr>
<tr><td>log-alloc [worker]-6121</td><td>0.000s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>append [worker]-6120</td><td>0.090s</td><td>0.000s</td><td>0.000s</td></tr>
<tr><td>prepare [worker]-6119</td><td>0.100s</td><td>0.000s</td><td>0.000s</td></tr>
</table><div class='yb-bottom-spacer'></div></div>
<footer class='footer'><div class='yb-footer container text-muted'><pre class='message'><i class="fa-lg fa fa-gift" aria-hidden="true"></i> Congratulations on installing YugabyteDB. We'd like to welcome you to the community with a free t-shirt and pack of stickers! Please claim your reward here: <a href='https://www.yugabyte.com/community-rewards/'>https://www.yugabyte.com/community-rewards/</a></pre><pre>version 2.13.0.0 build 42 revision cd3c1a4bb1cca183be824851f8158ebbffd1d3d8 build_type RELEASE built at 06 Mar 2022 03:13:49 UTC
server uuid 4ce571a18f8c4a9a8b35246222d12025 local time 2022-03-16 12:33:37.634419</pre></div></footer></body></html>"#.to_string();
        let result = AllThreads::parse_threads(threads);
        // this results in 33 Threads
        assert_eq!(result.len(), 33);
        // and the thread name is Master_reactorx-6127
        // these are all the fields, for completeness sake
        assert_eq!(result[0].thread_name, "Master_reactorx-6127");
        assert_eq!(result[0].cumulative_user_cpu_s, "2.960s");
        assert_eq!(result[0].cumulative_kernel_cpu_s, "0.000s");
        assert_eq!(result[0].cumulative_iowait_cpu_s, "0.000s");
        assert_eq!(result[0].stack, "__clone;start_thread;yb::Thread::SuperviseThread();yb::rpc::Reactor::RunThread();ev_run;epoll_poll;__GI_epoll_wait");
    }

    #[tokio::test]
    async fn integration_parse_threadsdata_master() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let allthreads = AllThreads::read_threads(&vec![&hostname], &vec![&port], 1).await;
        // the master returns more than one thread.
        assert!(allthreads.threads.len() > 1);
    }
    #[tokio::test]
    async fn integration_parse_threadsdata_tserver() {
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();

        let allthreads = AllThreads::read_threads(&vec![&hostname], &vec![&port], 1).await;

        // the tablet server returns more than one thread.
        assert!(allthreads.threads.len() > 1);
    }

}