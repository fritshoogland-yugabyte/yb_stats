use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use std::path::PathBuf;
use std::fs;
use std::process;
use serde_derive::{Serialize,Deserialize};
use regex::Regex;
//use rayon;
use std::sync::mpsc::channel;
use scraper::{ElementRef, Html, Selector};
use log::*;

#[derive(Debug)]
pub struct Threads {
    pub thread_name: String,
    pub cumulative_user_cpu_s: String,
    pub cumulative_kernel_cpu_s: String,
    pub cumulative_iowait_cpu_s: String,
    pub stack: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredThreads {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub thread_name: String,
    pub cumulative_user_cpu_s: String,
    pub cumulative_kernel_cpu_s: String,
    pub cumulative_iowait_cpu_s: String,
    pub stack: String,
}

#[allow(dead_code)]
#[allow(clippy::ptr_arg)]
pub fn perform_threads_snapshot(
    hosts: &Vec<&str>,
    ports: &Vec<&str>,
    snapshot_number: i32,
    yb_stats_directory: &PathBuf,
    parallel: usize
) {
    info!("perform_threads_snapshot");
    let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
    let (tx, rx) = channel();

    pool.scope(move |s| {
        for host in hosts {
            for port in ports {
                let tx = tx.clone();
                s.spawn(move |_| {
                    let detail_snapshot_time = Local::now();
                    let threads = read_threads(host, port);
                    tx.send((format!("{}:{}", host, port), detail_snapshot_time, threads)).expect("error sending data via tx (threads)");
                });
            }
        }
    });
    let mut stored_threads: Vec<StoredThreads> = Vec::new();
    for (hostname_port, detail_snapshot_time, threads) in rx {
        add_to_threads_vector(threads, &hostname_port, detail_snapshot_time, &mut stored_threads);
    }

    let current_snapshot_directory = &yb_stats_directory.join( &snapshot_number.to_string());
    let threads_file = &current_snapshot_directory.join("threads");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&threads_file)
        .unwrap_or_else(|e| {
            error!("Fatal: error writing threads data in snapshots directory {}: {}", &threads_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut writer = csv::Writer::from_writer(file);
    for row in stored_threads {
        writer.serialize(row).unwrap();
    }
    writer.flush().unwrap();
}

#[allow(dead_code)]
pub fn read_threads(
    host: &str,
    port: &str,
) -> Vec<Threads> {
    if ! scan_port_addr( format!("{}:{}", host, port) ) {
        warn!("Warning: hostname:port {}:{} cannot be reached, skipping (threads)", host, port);
        return Vec::new();
    }
    //if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}/threadz?group=all", hostname.to_string())) {
    if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}:{}/threadz?group=all", host, port)) {
        parse_threads(data_from_http.text().unwrap())
    } else {
        parse_threads(String::from(""))
    }
}

#[allow(dead_code)]
fn parse_threads(
    http_data: String
) -> Vec<Threads> {
    let mut threads: Vec<Threads> = Vec::new();
    let function_regex = Regex::new(r"@\s+0x[[:xdigit:]]+\s+(\S+)\n").unwrap();
    if let Some(table) = find_table(&http_data) {
        let (headers, rows) = table;

        let try_find_header = |target| headers.iter().position(|h| h == target);
        // mind "Thread name": name doesn't start with a capital, unlike all other headings
        let thread_name_pos = try_find_header("Thread name");
        let cumul_user_cpus_pos = try_find_header("Cumulative User CPU(s)");
        let cumul_kernel_cpus_pos = try_find_header("Cumulative Kernel CPU(s)");
        let cumul_iowaits_pos = try_find_header("Cumulative IO-wait(s)");

        let take_or_missing =
            |row: &mut [String], pos: Option<usize>| match pos.and_then(|pos| row.get_mut(pos)) {
                Some(value) => std::mem::take(value),
                None => "<Missing>".to_string(),
            };

        let mut stack_from_table = String::from("Initial value: this should not be visible");
        for mut row in rows {
            stack_from_table = if row.len() == 5 {
                std::mem::take(&mut row[4])
            } else {
                stack_from_table.to_string()
            };
            let stack_from_table = stack_from_table.replace("&lt;", "<");
            let stack_from_table = stack_from_table.replace("&gt;", ">");
            let mut st = Vec::new();
            for c in function_regex.captures_iter(&stack_from_table) {
                st.push(c[1].to_string().clone());
            };
            st.reverse();
            let mut final_stack = String::from("");
            for function in &st {
                final_stack.push_str(function );
                #[allow(clippy::single_char_add_str)]
                final_stack.push_str(";");
            }
            final_stack.pop();
            threads.push(Threads {
                thread_name: take_or_missing(&mut row, thread_name_pos),
                cumulative_user_cpu_s: take_or_missing(&mut row, cumul_user_cpus_pos),
                cumulative_kernel_cpu_s: take_or_missing(&mut row, cumul_kernel_cpus_pos),
                cumulative_iowait_cpu_s: take_or_missing(&mut row, cumul_iowaits_pos),
                //stack: stack_from_table.clone(),
                stack: final_stack,
            });
        }
    }
    threads
}

fn find_table(http_data: &str) -> Option<(Vec<String>, Vec<Vec<String>>)> {
    let css = |selector| Selector::parse(selector).unwrap();
    let get_cells = |row: ElementRef, selector| {
        row.select(&css(selector))
            .map(|cell| cell.inner_html().trim().to_string())
            .collect()
    };
    let html = Html::parse_fragment(http_data);
    let table = html.select(&css("table")).next()?;
    let tr = css("tr");
    let mut rows = table.select(&tr);
    let headers = get_cells(rows.next()?, "th");
    let rows: Vec<_> = rows.map(|row| get_cells(row, "td")).collect();
    Some((headers, rows))
}

#[allow(dead_code)]
pub fn print_threads_data(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
    hostname_filter: &Regex
) {
    info!("print_threads");
    let stored_threads: Vec<StoredThreads> = read_threads_snapshot(snapshot_number, yb_stats_directory);
    let mut previous_hostname_port = String::from("");
    for row in stored_threads {
        if hostname_filter.is_match(&row.hostname_port) {
            if row.hostname_port != previous_hostname_port {
                println!("--------------------------------------------------------------------------------------------------------------------------------------");
                println!("Host: {}, Snapshot number: {}, Snapshot time: {}", &row.hostname_port.to_string(), &snapshot_number, row.timestamp);
                println!("--------------------------------------------------------------------------------------------------------------------------------------");
                println!("{:20} {:30} {:>20} {:>20} {:>20} {:50}",
                         "hostname_port",
                         "thread_name",
                         "cum_user_cpu_s",
                         "cum_kernel_cpu_s",
                         "cum_iowait_cpu_s",
                         "stack");
                println!("--------------------------------------------------------------------------------------------------------------------------------------");
                previous_hostname_port = row.hostname_port.to_string();
            };
            println!("{:20} {:30} {:>20} {:>20} {:>20} {:50}", row.hostname_port, row.thread_name, row.cumulative_user_cpu_s, row.cumulative_kernel_cpu_s, row.cumulative_iowait_cpu_s, row.stack.replace('\n', ""));
        }
    }
}

#[allow(dead_code)]
#[allow(clippy::ptr_arg)]
fn read_threads_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf
) -> Vec<StoredThreads> {
    let mut stored_threads: Vec<StoredThreads> = Vec::new();
    let threads_file = &yb_stats_directory.join(snapshot_number).join("threads");
    let file = fs::File::open(&threads_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error reading file: {}: {}", &threads_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredThreads = row.unwrap();
        let _ = &stored_threads.push(data);
    }
    stored_threads
}

#[allow(dead_code)]
pub fn add_to_threads_vector(threadsdata: Vec<Threads>,
                                 hostname: &str,
                                 snapshot_time: DateTime<Local>,
                                 stored_threads: &mut Vec<StoredThreads>
) {
    for line in threadsdata {
        stored_threads.push( StoredThreads {
            hostname_port: hostname.to_string(),
            timestamp: snapshot_time,
            thread_name: line.thread_name.to_string(),
            cumulative_user_cpu_s: line.cumulative_user_cpu_s.to_string(),
            cumulative_kernel_cpu_s: line.cumulative_kernel_cpu_s.to_string(),
            cumulative_iowait_cpu_s: line.cumulative_iowait_cpu_s.to_string(),
            stack: line.stack.to_string()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_threads_data() {
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
        let result = parse_threads(threads);
        // this results in 33 Threads
        assert_eq!(result.len(), 33);
        // and the thread name is Master_reactorx-6127
        // these are all the fields, for completeness sake
        assert_eq!(result[0].thread_name, "Master_reactorx-6127");
        assert_eq!(result[0].cumulative_user_cpu_s, "2.960s");
        assert_eq!(result[0].cumulative_kernel_cpu_s, "0.000s");
        assert_eq!(result[0].cumulative_iowait_cpu_s, "0.000s");
        //assert_eq!(result[0].stack, "<pre>    @     0x7f035af7a9f2  __GI_epoll_wait\n    @     0x7f035db7fbf7  epoll_poll\n    @     0x7f035db7ac5d  ev_run\n    @     0x7f035e02f07b  yb::rpc::Reactor::RunThread()\n    @     0x7f035de5f1d4  yb::Thread::SuperviseThread()\n    @     0x7f035b83e693  start_thread\n    @     0x7f035af7a41c  __clone\n\nTotal number of threads: 1</pre>");
        assert_eq!(result[0].stack, "__clone;start_thread;yb::Thread::SuperviseThread();yb::rpc::Reactor::RunThread();ev_run;epoll_poll;__GI_epoll_wait");
    }
}