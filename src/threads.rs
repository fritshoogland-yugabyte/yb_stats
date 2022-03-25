use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use table_extract;
use std::path::PathBuf;
use std::fs;
use std::process;
use serde_derive::{Serialize,Deserialize};
use regex::Regex;
use rayon;
use std::sync::mpsc::channel;

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
pub fn perform_threads_snapshot(
    hostname_port_vec: &Vec<&str>,
    snapshot_number: i32,
    yb_stats_directory: &PathBuf,
    parallel: usize
) {
    let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
    let (tx, rx) = channel();

    pool.scope(move |s| {
        for hostname_port in hostname_port_vec {
            let tx = tx.clone();
            s.spawn(move |_| {
                let detail_snapshot_time = Local::now();
                let threads = read_threads(&hostname_port);
                tx.send( (hostname_port, detail_snapshot_time, threads )).expect("channel will be waiting in the pool");
            });
        }
    });
    //drop(tx);
    let mut stored_threads: Vec<StoredThreads> = Vec::new();
    for (hostname_port, detail_snapshot_time, threads) in rx {
        add_to_threads_vector(threads, hostname_port, detail_snapshot_time, &mut stored_threads);
    }

    let current_snapshot_directory = &yb_stats_directory.join( &snapshot_number.to_string());
    let threads_file = &current_snapshot_directory.join("threads");
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&threads_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error writing threads data in snapshots directory {}: {}", &threads_file.clone().into_os_string().into_string().unwrap(), e);
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
    hostname: &str
) -> Vec<Threads> {
    if ! scan_port_addr( hostname ) {
        println!("Warning: hostname:port {} cannot be reached, skipping", hostname.to_string());
        return Vec::new();
    }
    if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}/threadz?group=all", hostname.to_string())) {
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
    if let Some ( table ) = table_extract::Table::find_first(&http_data) {
        let empty_stack_from_table = String::from("^-^");
        for row in &table {
            let stack_from_table = if row.as_slice().len() == 5 {
                &row.as_slice()[4]
            } else {
                &empty_stack_from_table
            };
            threads.push(Threads {
                thread_name: row.get("Thread name").unwrap_or("<Missing>").to_string(),
                cumulative_user_cpu_s: row.get("Cumulative User CPU(s)").unwrap_or("<Missing>").to_string(),
                cumulative_kernel_cpu_s: row.get("Cumulative Kernel CPU(s)").unwrap_or("<Missing>").to_string(),
                cumulative_iowait_cpu_s: row.get("Cumulative IO-wait(s)").unwrap_or("<Missing>").to_string(),
                stack: stack_from_table.to_string()
            });
        }
    }
    threads
}
#[allow(dead_code)]
pub fn print_threads_data(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
    hostname_filter: &Regex
) {
    let stored_threads: Vec<StoredThreads> = read_threads_snapshot(&snapshot_number, yb_stats_directory);
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
            println!("{:20} {:30} {:>20} {:>20} {:>20} {:50}", row.hostname_port, row.thread_name, row.cumulative_user_cpu_s, row.cumulative_kernel_cpu_s, row.cumulative_iowait_cpu_s, row.stack.replace("\n", ""));
        }
    }
}

#[allow(dead_code)]
fn read_threads_snapshot(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf
) -> Vec<StoredThreads> {
    let mut stored_threads: Vec<StoredThreads> = Vec::new();
    let threads_file = &yb_stats_directory.join(&snapshot_number.to_string()).join("threads");
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
        let result = parse_threads(threads.clone());
        // this results in 33 Threads
        assert_eq!(result.len(), 33);
        // and the thread name is Master_reactorx-6127
        assert_eq!(result[0].thread_name, "Master_reactorx-6127");
    }
}