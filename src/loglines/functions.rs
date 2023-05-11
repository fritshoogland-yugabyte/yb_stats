//! The impls and functions.
//!
use std::{sync::mpsc::channel, time::{Instant, Duration}, collections::BTreeMap};
use chrono::{DateTime, Utc, TimeZone};
use regex::{Regex,Captures};
use log::*;
use colored::*;
use tokio::time;
use anyhow::Result;
use scraper::{Html, Selector};
use crate::snapshot;
use crate::Opts;
use crate::utility;
use crate::loglines::{AllLogLines, LogLine};

impl AllLogLines {
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

        let allloglines = AllLogLines::read_loglines(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "loglines", allloglines.loglines)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub async fn read_loglines(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllLogLines
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
                        // no detail_snapshot_time: the time of the logline is part of LogLine!
                        let mut loglines = AllLogLines::read_http(host, port);
                        loglines.iter_mut().for_each(|r| r.hostname_port = Some(format!("{}:{}", host, port)));
                        tx.send(loglines).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allloglines = AllLogLines::new();

        for loglines in rx
        {
            for logline in loglines
            {
                allloglines.loglines.push(logline);
            }
        }
       allloglines
    }
    fn read_http(
        host: &str,
        port: &str,
    ) -> Vec<LogLine>
    {
        let data_from_http = utility::http_get(host, port, "logs");
        AllLogLines::parse_loglines(data_from_http)
    }
    fn parse_loglines(
        http_data: String
    ) -> Vec<LogLine>
    {
        let mut loglines: Vec<LogLine> = Vec::new();

        let html = Html::parse_document(&http_data);

        //  <div class='yb-main container-fluid'>
        //         <h2>INFO logs</h2>
        //
        //         Log path is: /mnt/d0/yb-data/tserver/logs/yb-tserver.INFO
        //         <br/>
        //         Showing last 1048576 bytes of log
        //         <br/>
        //         <pre>
        //             Log file created at: 2023/02/03 11:13:22
        //             Current UTC time: 2023/02/03 11:13:22
        //             Running on machine: yb-1.local
        //             Application fingerprint: version 2.17.0.0 build 24 revision d4f01a5e26b168585e59f9c1a95766ffdd9655b1 build_type RELEASE built at 16 Nov 2022 00:21:52 UTC
        //             Running duration (h:mm:ss): 0:00:00
        //             Log line format: [IWEF]mmdd hh:mm:ss.uuuuuu threadid file:line] msg
        //             I0203 11:13:22.698063  7164 server_main_util.cc:72] NumCPUs determined to be: 4
        //
        // The loglines in this page are inside <div class='yb-main.., inside the <pre> tag.
        let selector = Selector::parse("div.yb-main > pre").unwrap();

        // If the selector returns a result, start parsing glog lines
        if let Some(raw_loglines) = html.select(&selector).next() {
            // fs_manager:
            //I0217 10:12:35.491056 26960 fs_manager.cc:278] Opened local filesystem: /mnt/d0
            //uuid: "05b8d17620eb4cd79eddaddb2fbcbb42"
            //format_stamp: "Formatted at 2022-02-13 16:26:17 on yb-1.local"
            //let regular_log_line = Regex::new( r"([IWFE])(\d{2}\d{2} \d{2}:\d{2}:\d{2}\.\d{6})\s+(\d{1,6}) ([a-z_A-Z.:0-9]*)] (.*)(?=\n|[WIEF]\d{2}\d{2})" ).unwrap();
            //FIXME: this assumes that a log line is followed by a newline. This does not always happen. fancy_regex crate provides lookahead
            let regular_log_line = Regex::new( r"([IWFE])(\d{2}\d{2} \d{2}:\d{2}:\d{2}\.\d{6})\s+(\d{1,6}) ([a-z_A-Z.:0-9]*)] (.*)\n" ).unwrap();

            // Just take the year, it's not in the loglines, however, when the year switches this will lead to error results
            let year= Utc::now().format("%Y").to_string();
            let to_logline = |captures: Captures<'_>|
                {
                    let timestamp_string = format!("{}{}", year, &captures[2]);
                    let timestamp = Utc
                        .datetime_from_str(&timestamp_string, "%Y%m%d %H:%M:%S.%6f")
                        .unwrap();

                    LogLine {
                        severity: captures[1].to_string(),
                        timestamp,
                        tid: captures[3].to_string(),
                        sourcefile_nr: captures[4].to_string(),
                        message: captures[5].to_string(),
                        ..Default::default()
                    }
                };
            // Find first log line.  Any non-regular-log-line data at the beginning of
            // the logs is discarded.  `remaining` covers all the logs following the
            // first regular log line.
            let mut logline;
            let mut remaining;
            let stored_raw_loglines = &raw_loglines.text().collect::<String>();
            match regular_log_line.captures(stored_raw_loglines)
            {
                None => return loglines,
                Some(captures) => {
                    let offset = captures.get(0).map(|m| m.end()).unwrap_or(0);
                    remaining = &stored_raw_loglines[offset..];
                    logline = to_logline(captures);
                }
            };
            // For each subsequent match, append any lines before the match to the
            // current `LogLine`, store it, and start a new `LogLine`.  Update where
            // we are in the logs by updating `remaining`.
            while let Some(captures) = regular_log_line.captures(remaining)
            {
                let all = captures.get(0).unwrap();
                let from = all.start();
                let offset = all.end();
                logline.message += &remaining[..from];
                loglines.push(logline);
                logline = to_logline(captures);
                remaining = &remaining[offset..]
            }

            // Append final logline and return
            logline.message += remaining;
            loglines.push(logline);
        }

        loglines
    }
    pub fn print(
        &self,
        hostname_filter: &Regex,
        stat_name_filter: &Regex,
        log_severity: &str,
    ) -> Result<()>
    {
        info!("print log");

        // create a copy of the stored_loglines vector and sort it based on the timestamp.
        let mut sorted_loglines = self.loglines.clone();
        sorted_loglines.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        // use the sorted vector to loop over.
        for row in &sorted_loglines
        {
            if hostname_filter.is_match(&row.hostname_port.clone().expect("no hostname:port set"))
                && log_severity.contains(&row.severity)
                && ( stat_name_filter.is_match(&row.message) || stat_name_filter.is_match(&row.sourcefile_nr) )
            {
                print!("{:20} {:33} ", row.hostname_port.clone().expect("no hostname:port set"), row.timestamp);
                match row.severity.as_str()
                {
                    "I" => print!("{} ", "I".green()),
                    "W" => print!("{} ", "W".yellow()),
                    "E" => print!("{} ", "E".red()),
                    "F" => print!("{} ", "F".purple()),
                    _   => print!("{} ", row.severity.underline()),
                }
                println!("{:20} {:50}",row.sourcefile_nr, row.message.trim());
            }
        }
        Ok(())
    }
}

pub async fn print_loglines(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    let hostname_filter = utility::set_regex(&options.hostname_match);
    let stat_name_filter = utility::set_regex(&options.stat_name_match);
    match options.print_log.as_ref().unwrap()
    {
        Some(snapshot_number) => {
            let mut allloglines = AllLogLines::new();
            allloglines.loglines = snapshot::read_snapshot_json(snapshot_number, "loglines")?;
            allloglines.print(&hostname_filter, &stat_name_filter, &options.log_severity)?;
        },
        None => {
            let allloglines = AllLogLines::read_loglines(&hosts, &ports, parallel).await;
            allloglines.print(&hostname_filter, &stat_name_filter, &options.log_severity)?;
        },
    }
    Ok(())
}

pub async fn tail_loglines(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    let hostname_filter = utility::set_regex(&options.hostname_match);
    let stat_name_filter = utility::set_regex(&options.stat_name_match);
    let mut interval = time::interval(Duration::from_secs(3));

    #[derive(Debug, Clone)]
    struct SpecialLogLine { severity: String, _tid: String, message: String }
    let into_btreemap = |allstored: AllLogLines| -> BTreeMap<(DateTime<Utc>, String, String), SpecialLogLine>
    {
        let mut btreemap: BTreeMap<(DateTime<Utc>, String, String), SpecialLogLine> = BTreeMap::new();
        for logline in allstored.loglines
        {
            btreemap.insert((logline.timestamp, logline.hostname_port.expect("no hostname:port set").to_string(), logline.sourcefile_nr.to_string()),
                            SpecialLogLine { severity: logline.severity.to_string(), _tid: logline.tid.to_string(), message: logline.message.to_string(),
            });
        }
        btreemap
    };
    let loglines = AllLogLines::read_loglines(&hosts, &ports, parallel).await;
    let mut first_loglines_btreemap = into_btreemap(loglines);

    println!("Tail log ready, showing severities: {}", &options.log_severity);

    loop
    {
        let mut display_loglines_btreemap: BTreeMap<(DateTime<Utc>, String, String), SpecialLogLine> = BTreeMap::new();
        let loglines = AllLogLines::read_loglines(&hosts, &ports, parallel).await;
        let second_loglines_btreemap = into_btreemap(loglines);
        // add all loglines that are not found in the second loglines snapshot to display loglines
        for (key, value) in &second_loglines_btreemap
        {
            if first_loglines_btreemap.get(key).is_none()
            {
                display_loglines_btreemap.insert( key.clone(), value.clone());
            }
        }
        for ((timestamp, hostname_port, sourcefile_nr), logline) in &display_loglines_btreemap
        {
            if hostname_filter.is_match(hostname_port)
                && options.log_severity.contains(&logline.severity)
                && ( stat_name_filter.is_match(&logline.message) || stat_name_filter.is_match(sourcefile_nr) )
            {
                print!("{:20} {:33} ", hostname_port, timestamp);
                match logline.severity.as_str()
                {
                    "I" => print!("{} ", "I".green()),
                    "W" => print!("{} ", "W".yellow()),
                    "E" => print!("{} ", "E".red()),
                    "F" => print!("{} ", "F".purple()),
                    _   => print!("{} ", logline.severity.underline()),
                }
                println!("{:20} {:50}", sourcefile_nr, logline.message.trim());
            }
        }
        interval.tick().await;
        drop(display_loglines_btreemap);
        first_loglines_btreemap = second_loglines_btreemap;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // This is a test function to test log line output.
    // Run with `cargo test unit_parse_proble_logline -- --nocapture` to see the output of println!
    #[test]
    #[ignore = "for testing logline parsing"]
    fn unit_parse_problem_logline() {
        // This is a regular log line.
        let logline = r#"
        <div class='yb-main container-fluid'><pre>
E1218 12:12:33.463250  6986 async_initializer.cc:95] Failed to initialize client: Timed out (yb/rpc/rpc.cc:221): Could not locate the leader master: GetLeaderMasterRpc(addrs: [yb-1.local:7100, yb-2.local:7100, yb-3.local:7100, yb-1.local:7100, yb-2.local:7100, yb-3.local:7100], num_attempts: 46) passed its deadline 34.910s (passed: 1.521s): Not found (yb/master/master_rpc.cc:287): no leader found: GetLeaderMasterRpc(addrs: [yb-1.local:7100, yb-2.local:7100, yb-3.local:7100, yb-1.local:7100, yb-2.local:7100, yb-3.local:7100], num_attempts: 1)
I1218 12:12:34.464701  7650 client-internal.cc:2273] Reinitialize master addresses from file: /opt/yugabyte/conf/master.conf
I1218 12:12:34.464808  7650 client-internal.cc:2302] New master addresses: [yb-1.local:7100,yb-2.local:7100,yb-3.local:7100, yb-1.local:7100, yb-2.local:7100, yb-3.local:7100]
        </pre></div>
        "#.to_string();
        let result = AllLogLines::parse_loglines(logline);
        //assert_eq!(result[0].message,"FLAGS_rocksdb_base_background_compactions was not set, automatically configuring 1 base background compactions.");
        println!("{:#?}", result);
    }

    #[test]
    fn unit_parse_regular_logline() {
        // This is a regular log line.
        let logline = r#"
        <div class='yb-main container-fluid'><pre>
        I0217 10:19:56.834905  31987 docdb_rocksdb_util.cc:416] FLAGS_rocksdb_base_background_compactions was not set, automatically configuring 1 base background compactions.\n
        </pre></div>
        "#.to_string();
        let result = AllLogLines::parse_loglines(logline);
        assert_eq!(result[0].message.trim(),"FLAGS_rocksdb_base_background_compactions was not set, automatically configuring 1 base background compactions.\\n");
    }

    #[test]
    fn unit_parse_long_operation_backtrace_multiline_logline() {
        // This is a log line that contains a backtrace.
        // WARNING! Currently, the backtrace is not added to the line, and just ignored.
        // This means success here means not correctly parsing!
        let logline = r#"
        <div class='yb-main container-fluid'><pre>
        W0214 11:22:36.980569  7803 long_operation_tracker.cc:114] UpdateReplica running for 1.000s in thread 7814:
    @     0x7fa344eb611f  (unknown)
    @     0x7fa345833b39  __lll_lock_wait
    @     0x7fa34582e6e2  __GI___pthread_mutex_lock
    @     0x7fa34e609858  rocksdb::port::Mutex::Lock()
    @     0x7fa34e6897fb  rocksdb::InstrumentedMutex::Lock()
    @     0x7fa34e564a4f  rocksdb::DBImpl::WriteImpl()
    @     0x7fa34e5666a3  rocksdb::DBImpl::Write()
    @     0x7fa353ff6e40  yb::tablet::Tablet::WriteToRocksDB()
    @     0x7fa354007114  yb::tablet::Tablet::ApplyKeyValueRowOperations()
    @     0x7fa354007660  yb::tablet::Tablet::ApplyOperation()
    @     0x7fa35400791a  yb::tablet::Tablet::ApplyRowOperations()
    @     0x7fa3540929d4  yb::tablet::WriteOperation::DoReplicated()
    @     0x7fa354083efb  yb::tablet::Operation::Replicated()
    @     0x7fa354089720  yb::tablet::OperationDriver::ApplyTask()
    @     0x7fa354089e8e  yb::tablet::OperationDriver::ReplicationFinished()
    @     0x7fa353cb3ae7  yb::consensus::ReplicaState::NotifyReplicationFinishedUnlocked()
            </pre></div>
    "#.to_string();
        let result = AllLogLines::parse_loglines(logline);
        // this is the old assertion when the following lines of a multiline logoine were not joined:
        //assert_eq!(result[0].message,"UpdateReplica running for 1.000s in thread 7814:");
        // this is the new line with the new code that adds the lines of a multiline message:
        assert_eq!(result[0].message.trim(),"UpdateReplica running for 1.000s in thread 7814:    @     0x7fa344eb611f  (unknown)\n    @     0x7fa345833b39  __lll_lock_wait\n    @     0x7fa34582e6e2  __GI___pthread_mutex_lock\n    @     0x7fa34e609858  rocksdb::port::Mutex::Lock()\n    @     0x7fa34e6897fb  rocksdb::InstrumentedMutex::Lock()\n    @     0x7fa34e564a4f  rocksdb::DBImpl::WriteImpl()\n    @     0x7fa34e5666a3  rocksdb::DBImpl::Write()\n    @     0x7fa353ff6e40  yb::tablet::Tablet::WriteToRocksDB()\n    @     0x7fa354007114  yb::tablet::Tablet::ApplyKeyValueRowOperations()\n    @     0x7fa354007660  yb::tablet::Tablet::ApplyOperation()\n    @     0x7fa35400791a  yb::tablet::Tablet::ApplyRowOperations()\n    @     0x7fa3540929d4  yb::tablet::WriteOperation::DoReplicated()\n    @     0x7fa354083efb  yb::tablet::Operation::Replicated()\n    @     0x7fa354089720  yb::tablet::OperationDriver::ApplyTask()\n    @     0x7fa354089e8e  yb::tablet::OperationDriver::ReplicationFinished()\n    @     0x7fa353cb3ae7  yb::consensus::ReplicaState::NotifyReplicationFinishedUnlocked()");
    }

    #[test]
    fn unit_parse_tabletserver_ulimit_multiline_logline() {
        // This is a log line that contains a ulimit "dump".
        // WARNING! Currently, the ulimit info on the other lines are not added to the line, and just ignored.
        // This means success here means not correctly parsing!
        let logline = r#"
    <div class='yb-main container-fluid'><pre>
    I0217 10:12:35.521059  26960 tablet_server_main.cc:225] ulimit cur(max)...
    ulimit: core file size 0(unlimited) blks
    ulimit: data seg size unlimited(unlimited) kb
    ulimit: open files 1048576(1048576)
    ulimit: file size unlimited(unlimited) blks
    ulimit: pending signals 119934(119934)
    ulimit: file locks unlimited(unlimited)
    ulimit: max locked memory 64(64) kb
    ulimit: max memory size unlimited(unlimited) kb
    ulimit: stack size 8192(unlimited) kb
    ulimit: cpu time unlimited(unlimited) secs
    ulimit: max user processes 12000(12000)
    </pre></div>
    "#.to_string();
        let result = AllLogLines::parse_loglines(logline);
        // this is the old assertion when the following lines of a multiline logline were not joined:
        //assert_eq!(result[0].message,"ulimit cur(max)...");
        // this is the new line with the new code that adds the lines of a multiline message:
        assert_eq!(result[0].message.trim(),"ulimit cur(max)...    ulimit: core file size 0(unlimited) blks\n    ulimit: data seg size unlimited(unlimited) kb\n    ulimit: open files 1048576(1048576)\n    ulimit: file size unlimited(unlimited) blks\n    ulimit: pending signals 119934(119934)\n    ulimit: file locks unlimited(unlimited)\n    ulimit: max locked memory 64(64) kb\n    ulimit: max memory size unlimited(unlimited) kb\n    ulimit: stack size 8192(unlimited) kb\n    ulimit: cpu time unlimited(unlimited) secs\n    ulimit: max user processes 12000(12000)");
    }

    #[test]
    fn unit_parse_process_context_sql_error_multiline_logline() {
        // This is a log line that contains a SQL error statement.
        // WARNING!! Currently, any following line is ignored.
        // This means success here means not correctly parsing!
        let logline = r#"
        <div class='yb-main container-fluid'><pre>
        W0221 17:18:06.190536  7924 process_context.cc:185] SQL Error: Type Not Found. Could not find user defined type
create table test (id int primary key, f1 tdxt);
                                          ^^^^
        </pre></div>
        "#.to_string();
        let result = AllLogLines::parse_loglines(logline);
        //assert_eq!(result[0].message,"SQL Error: Type Not Found. Could not find user defined type");
        assert_eq!(result[0].message.trim(),"SQL Error: Type Not Found. Could not find user defined typecreate table test (id int primary key, f1 tdxt);\n                                          ^^^^");
    }

    #[test]
    fn unit_parse_process_context_cql_error_multiline_logline() {
        // This is a log line that contains a CQL error statement.
        // WARNING!! Currently, any following line is ignored.
        // This means success here means not correctly parsing!
        let logline = r#"
        <div class='yb-main container-fluid'><pre>
        W0221 17:18:28.396759  7925 process_context.cc:185] SQL Error: Invalid CQL Statement. Missing list of target columns
insert into test values (1,'a');
                 ^^^^^^^^^^^^^^
        </pre></div>
        "#.to_string();
        let result = AllLogLines::parse_loglines(logline);
        //assert_eq!(result[0].message,"SQL Error: Invalid CQL Statement. Missing list of target columns");
        assert_eq!(result[0].message.trim(),"SQL Error: Invalid CQL Statement. Missing list of target columnsinsert into test values (1,'a');\n                 ^^^^^^^^^^^^^^");
    }

    #[test]
    fn unit_parse_fs_manager_multiline_logline() {
        // This is a log line that contains a fs manager message.
        // WARNING!! Currently, any following line is ignored.
        // This means success here means not correctly parsing!
        let logline = r#"
        <div class='yb-main container-fluid'><pre>
        I0227 17:18:28.714171  2510 fs_manager.cc:278] Opened local filesystem: /mnt/d0
uuid: "05b8d17620eb4cd79eddaddb2fbcbb42"
format_stamp: "Formatted at 2022-02-13 16:26:17 on yb-1.local"
        </pre></div>
        "#.to_string();
        let result = AllLogLines::parse_loglines(logline);
        //assert_eq!(result[0].message,"Opened local filesystem: /mnt/d0");
        assert_eq!(result[0].message.trim(),"Opened local filesystem: /mnt/d0uuid: \"05b8d17620eb4cd79eddaddb2fbcbb42\"\nformat_stamp: \"Formatted at 2022-02-13 16:26:17 on yb-1.local\"");
    }

    #[test]
    fn unit_parse_writing_version_edit_logline() {
        // This is a log line that contains a version edit. It is an informal (I) line, I don't know how important this is.
        // WARNING!! Currently, any following line is ignored.
        // This means success here means not correctly parsing!
        let logline = r#"
        <div class='yb-main container-fluid'><pre>
        I0227 17:19:07.745766  3319 version_set.cc:3349] T b770079b94ad430493ba5f729fb1f0e7 P 05b8d17620eb4cd79eddaddb2fbcbb42 [R]: Writing version edit: log_number: 33
new_files {
  level: 0
  number: 10
  total_file_size: 66565
  base_file_size: 66384
  smallest {
    key: "Gg\303I\200\000\000\000\000\0003\341I\200\000\000\000\000\000B\225!!J\200#\200\001|E\302\264\205v\200J\001\004\000\000\000\000\000\004"
    seqno: 1125899906842625
    user_values {
      tag: 1
      data: "\200\001|E\302\274j0\200J"
    }
    user_frontier {
      [type.googleapis.com/yb.docdb.ConsensusFrontierPB] {
        op_id {
          term: 1
          index: 2
        }
        hybrid_time: 6737247907820138496
        history_cutoff: 18446744073709551614
        max_value_level_ttl_expiration_time: 18446744073709551614
      }
    }
  }
  largest {
    key: "G\222oI\200\000\000\000\000\0003\341I\200\000\000\000\000\000B{!!K\203#\200\001|E\302\274j0\200?\213\001\003\000\000\000\000\000\004"
    seqno: 1125899906842632
    user_values {
      tag: 1
      data: "\200\001|EXN\364\273\200?\253"
    }
    user_frontier {
      [type.googleapis.com/yb.docdb.ConsensusFrontierPB] {
        op_id {
          term: 1
          index: 4
        }
        hybrid_time: 6737255221467299840
        history_cutoff: 18446744073709551614
        max_value_level_ttl_expiration_time: 1
      }
    }
  }
}
flushed_frontier {
  [type.googleapis.com/yb.docdb.ConsensusFrontierPB] {
    op_id {
      term: 1
      index: 4
    }
    hybrid_time: 6737255221467299840
    history_cutoff: 18446744073709551614
    max_value_level_ttl_expiration_time: 1
  }
}
        </pre></div>
        "#.to_string();
        let result = AllLogLines::parse_loglines(logline);
        assert_eq!(result[0].message.trim(),"T b770079b94ad430493ba5f729fb1f0e7 P 05b8d17620eb4cd79eddaddb2fbcbb42 [R]: Writing version edit: log_number: 33new_files {\n  level: 0\n  number: 10\n  total_file_size: 66565\n  base_file_size: 66384\n  smallest {\n    key: \"Gg\\303I\\200\\000\\000\\000\\000\\0003\\341I\\200\\000\\000\\000\\000\\000B\\225!!J\\200#\\200\\001|E\\302\\264\\205v\\200J\\001\\004\\000\\000\\000\\000\\000\\004\"\n    seqno: 1125899906842625\n    user_values {\n      tag: 1\n      data: \"\\200\\001|E\\302\\274j0\\200J\"\n    }\n    user_frontier {\n      [type.googleapis.com/yb.docdb.ConsensusFrontierPB] {\n        op_id {\n          term: 1\n          index: 2\n        }\n        hybrid_time: 6737247907820138496\n        history_cutoff: 18446744073709551614\n        max_value_level_ttl_expiration_time: 18446744073709551614\n      }\n    }\n  }\n  largest {\n    key: \"G\\222oI\\200\\000\\000\\000\\000\\0003\\341I\\200\\000\\000\\000\\000\\000B{!!K\\203#\\200\\001|E\\302\\274j0\\200?\\213\\001\\003\\000\\000\\000\\000\\000\\004\"\n    seqno: 1125899906842632\n    user_values {\n      tag: 1\n      data: \"\\200\\001|EXN\\364\\273\\200?\\253\"\n    }\n    user_frontier {\n      [type.googleapis.com/yb.docdb.ConsensusFrontierPB] {\n        op_id {\n          term: 1\n          index: 4\n        }\n        hybrid_time: 6737255221467299840\n        history_cutoff: 18446744073709551614\n        max_value_level_ttl_expiration_time: 1\n      }\n    }\n  }\n}\nflushed_frontier {\n  [type.googleapis.com/yb.docdb.ConsensusFrontierPB] {\n    op_id {\n      term: 1\n      index: 4\n    }\n    hybrid_time: 6737255221467299840\n    history_cutoff: 18446744073709551614\n    max_value_level_ttl_expiration_time: 1\n  }\n}");
    }

    #[test]
    fn unit_parse_tablet_alter_schema_from_schema_multiline_logline() {
        // This is a log line that contains a tablet message.
        // WARNING!! Currently, any following line is ignored.
        // This means success here means not correctly parsing!
        let logline = r#"
        <div class='yb-main container-fluid'><pre>
        I0214 10:51:38.273955 10148 tablet.cc:1821] T c6099b05976f49d9b782ccbe126f9b2d P 05b8d17620eb4cd79eddaddb2fbcbb42: Alter schema from Schema [
        0:ybrowid[binary NOT NULL PARTITION KEY],
        1:dir[string NULLABLE NOT A PARTITION KEY],
        2:dirname[string NULLABLE NOT A PARTITION KEY]
]
properties: contain_counters: false is_transactional: true consistency_level: STRONG use_mangled_column_name: false is_ysql_catalog_table: false retain_delete_markers: false version 0 to Schema [
        0:ybrowid[binary NOT NULL PARTITION KEY],
        1:dir[string NULLABLE NOT A PARTITION KEY],
        2:dirname[string NULLABLE NOT A PARTITION KEY]
]
properties: contain_counters: false is_transactional: true consistency_level: STRONG use_mangled_column_name: false is_ysql_catalog_table: false retain_delete_markers: false version 1
        </pre></div>
        "#.to_string();
        let result = AllLogLines::parse_loglines(logline);
        //assert_eq!(result[0].message,"T c6099b05976f49d9b782ccbe126f9b2d P 05b8d17620eb4cd79eddaddb2fbcbb42: Alter schema from Schema [");
        assert_eq!(result[0].message.trim(),"T c6099b05976f49d9b782ccbe126f9b2d P 05b8d17620eb4cd79eddaddb2fbcbb42: Alter schema from Schema [        0:ybrowid[binary NOT NULL PARTITION KEY],\n        1:dir[string NULLABLE NOT A PARTITION KEY],\n        2:dirname[string NULLABLE NOT A PARTITION KEY]\n]\nproperties: contain_counters: false is_transactional: true consistency_level: STRONG use_mangled_column_name: false is_ysql_catalog_table: false retain_delete_markers: false version 0 to Schema [\n        0:ybrowid[binary NOT NULL PARTITION KEY],\n        1:dir[string NULLABLE NOT A PARTITION KEY],\n        2:dirname[string NULLABLE NOT A PARTITION KEY]\n]\nproperties: contain_counters: false is_transactional: true consistency_level: STRONG use_mangled_column_name: false is_ysql_catalog_table: false retain_delete_markers: false version 1");
    }

    #[ignore]
    #[test]
    fn unit_parse_no_newline() {
        //I0305 11:11:11.123456 12345 tablet.cc:12345] Master_R010: DEBUG: Closing idle connection: Connection (0x0000555e57a90378) server [2a02:6b8:c34:14:0:1354:eb1f:29b2]:44809 => [2a02:6b8:c34:14:0:1354:eb1f:29b2]:7100 - it has been idle for 65.0001sW0503 18:40:36.919598 936000 master-path-handlers.cc:178] Error retrieving leader master URL: http://ydb-vla-dev04-001.search.yandex.net:7000/dump-entities?raw, error :Network error (yb/util/curl_util.cc:57): curl error: Couldn't resolve host name\nW0503 18:40:36.919770 935999 master-path-handlers.cc:178] Error retrieving leader master URL: http://ydb-vla-dev04-001.search.yandex.net:7000/api/v1/tablet-servers?raw, error :Network error (yb/util/curl_util.cc:57): curl error: Couldn't resolve host name\nW0503 18:40:36.929548 935996 master-path-handlers.cc:178] Error retrieving leader master URL: http://ydb-vla-dev04-001.search.yandex.net:7000/api/v1/health-check?raw, error :Network error (yb/util/curl_util.cc:57): curl error: Couldn't resolve host name\n
        let logline = r#"
        <div class='yb-main container-fluid'><pre>
        I0305 11:11:11.123456 12345 tablet.cc:12345] Master_R010: DEBUG: Closing idle connection: Connection (0x0000555e57a90378) server [2a02:6b8:c34:14:0:1354:eb1f:29b2]:44809 => [2a02:6b8:c34:14:0:1354:eb1f:29b2]:7100 - it has been idle for 65.0001sW0503 18:40:36.919598 936000 master-path-handlers.cc:178] Error retrieving leader master URL: http://ydb-vla-dev04-001.search.yandex.net:7000/dump-entities?raw, error :Network error (yb/util/curl_util.cc:57): curl error: Couldn't resolve host name\nW0503 18:40:36.919770 935999 master-path-handlers.cc:178] Error retrieving leader master URL: http://ydb-vla-dev04-001.search.yandex.net:7000/api/v1/tablet-servers?raw, error :Network error (yb/util/curl_util.cc:57): curl error: Couldn't resolve host name\nW0503 18:40:36.929548 935996 master-path-handlers.cc:178] Error retrieving leader master URL: http://ydb-vla-dev04-001.search.yandex.net:7000/api/v1/health-check?raw, error :Network error (yb/util/curl_util.cc:57): curl error: Couldn't resolve host name\n
        "#.to_string();
        let result = AllLogLines::parse_loglines(logline);
        assert_eq!(result[0].message.trim(),"Master_R010: DEBUG: Closing idle connection: Connection (0x0000555e57a90378) server [2a02:6b8:c34:14:0:1354:eb1f:29b2]:44809 => [2a02:6b8:c34:14:0:1354:eb1f:29b2]:7100 - it has been idle for 65.0001s");
    }

    #[tokio::test]
    async fn integration_parse_loglines_master() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();
        let allloglines = AllLogLines::read_loglines(&vec![&hostname], &vec![&port], 1).await;

        // it's likely there will be logging
        assert!(!allloglines.loglines.is_empty());
    }

    #[tokio::test]
    async fn integration_parse_loglines_tserver() {
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();
        let allloglines = AllLogLines::read_loglines(&vec![&hostname], &vec![&port], 1).await;

        // it's likely there will be logging
        assert!(!allloglines.loglines.is_empty());
    }

}

