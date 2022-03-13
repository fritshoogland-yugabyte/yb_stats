use std::process;
use chrono::{DateTime, Local, TimeZone};
use port_scanner::scan_port_addr;
use std::path::PathBuf;
use regex::Regex;
use std::fs;
use serde_derive::{Serialize,Deserialize};

#[derive(Debug)]
pub struct LogLine {
    pub severity: String,
    pub timestamp: DateTime<Local>,
    pub tid: String,
    pub sourcefile_nr: String,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredLogLines {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub severity: String,
    pub tid: String,
    pub sourcefile_nr: String,
    pub message: String,
}

#[allow(dead_code)]
pub fn read_loglines( hostname: &str) -> Vec<LogLine> {
    if ! scan_port_addr( hostname ) {
        println!("Warning: hostname:port {} cannot be reached, skipping", hostname.to_string());
        return Vec::new();
    }
    if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}/logs?raw", hostname.to_string())) {
        parse_loglines(data_from_http.text().unwrap())
    } else {
        parse_loglines(String::from(""))
    }
}

#[allow(dead_code)]
fn read_loglines_snapshot(snapshot_number: &String, yb_stats_directory: &PathBuf ) -> Vec<StoredLogLines> {

    let mut stored_loglines: Vec<StoredLogLines> = Vec::new();
    let loglines_file = &yb_stats_directory.join(&snapshot_number.to_string()).join("loglines");
    let file = fs::File::open(&loglines_file)
        .unwrap_or_else(|e| {
            eprintln!("Fatal: error reading file: {}: {}", &loglines_file.clone().into_os_string().into_string().unwrap(), e);
            process::exit(1);
        });
    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: StoredLogLines = row.unwrap();
        let _ = &stored_loglines.push(data);
    }
    stored_loglines
}

#[allow(dead_code)]
pub fn print_loglines(
    snapshot_number: &String,
    yb_stats_directory: &PathBuf,
    hostname_filter: &Regex,
    log_severity: &String
) {

    let stored_loglines: Vec<StoredLogLines> = read_loglines_snapshot(&snapshot_number, yb_stats_directory);
    let mut previous_hostname_port = String::from("");
    for row in stored_loglines {
        if hostname_filter.is_match(&row.hostname_port)
            && log_severity.contains(&row.severity) {
            if row.hostname_port != previous_hostname_port {
                println!("--------------------------------------------------------------------------------------------------------------------------------------");
                println!("Host: {}, Snapshot number: {}, Snapshot time: {}", &row.hostname_port.to_string(), &snapshot_number, row.timestamp);
                println!("--------------------------------------------------------------------------------------------------------------------------------------");
                previous_hostname_port = row.hostname_port.to_string();
            }
            println!("{:20} {:33} {:1} {:20} {:50}", row.hostname_port, row.timestamp, row.severity, row.sourcefile_nr, row.message)
        }
    }
}

#[allow(dead_code)]
pub fn add_to_loglines_vector(loglinedata: Vec<LogLine>,
                              hostname: &str,
                              stored_loglines: &mut Vec<StoredLogLines>
) {
    for logline in loglinedata {
        stored_loglines.push( StoredLogLines {
            hostname_port: hostname.to_string(),
            timestamp: logline.timestamp,
            severity: logline.severity.to_string(),
            tid: logline.tid.to_string(),
            sourcefile_nr: logline.sourcefile_nr.to_string(),
            message: logline.message.to_string()
        });
    }
}

#[allow(dead_code)]
fn parse_loglines( logs_data: String ) -> Vec<LogLine> {
    let mut loglines: Vec<LogLine> = Vec::new();
    // fs_manager:
    //I0217 10:12:35.491056 26960 fs_manager.cc:278] Opened local filesystem: /mnt/d0
    //uuid: "05b8d17620eb4cd79eddaddb2fbcbb42"
    //format_stamp: "Formatted at 2022-02-13 16:26:17 on yb-1.local"
    let regular_log_line = Regex::new( r"([IWFE])(\d{2}\d{2} \d{2}:\d{2}:\d{2}\.\d{6})\s+(\d{1,6}) ([a-z_A-Z.:0-9]*)] (.*)\n" ).unwrap();

    // Just take the year, it's not in the loglines, however, when the year switches this will lead to error results
    let year= Local::now().format("%Y").to_string();
    for captures in regular_log_line.captures_iter(&logs_data) {
        let datetime = captures.get(2).unwrap().as_str();
        let timestamp_string = format!("{} {}", year, datetime);
        let timestamp = Local.datetime_from_str(&timestamp_string, "%Y%m%d %H:%M:%S.%6f").unwrap();

        loglines.push(LogLine {
            severity: captures.get(1).unwrap().as_str().to_string(),
            timestamp,
            tid: captures.get(3).unwrap().as_str().to_string(),
            sourcefile_nr: captures.get(4).unwrap().as_str().to_string(),
            message: captures.get(5).unwrap().as_str().to_string()
        });
    }
    loglines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_regular_logline() {
        // This is a regular log line.
        let logline = "I0217 10:19:56.834905  31987 docdb_rocksdb_util.cc:416] FLAGS_rocksdb_base_background_compactions was not set, automatically configuring 1 base background compactions.\n".to_string();
        let result = parse_loglines(logline.clone());
        assert_eq!(result[0].message,"FLAGS_rocksdb_base_background_compactions was not set, automatically configuring 1 base background compactions.");
    }

    #[test]
    fn parse_long_operation_backtrace_multiline_logline() {
        // This is a log line that contains a backtrace.
        // WARNING! Currently, the backtrace is not added to the line, and just ignored.
        // This means success here means not correctly parsing!
        let logline = r#"W0214 11:22:36.980569  7803 long_operation_tracker.cc:114] UpdateReplica running for 1.000s in thread 7814:
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
    @     0x7fa353cb3ae7  yb::consensus::ReplicaState::NotifyReplicationFinishedUnlocked()"#.to_string();
        let result = parse_loglines(logline.clone());
        assert_eq!(result[0].message,"UpdateReplica running for 1.000s in thread 7814:");
    }

    #[test]
    fn parse_tabletserver_ulimit_multiline_logline() {
        // This is a log line that contains a ulimit "dump".
        // WARNING! Currently, the ulimit info on the other lines are not added to the line, and just ignored.
        // This means success here means not correctly parsing!
        let logline = r#"I0217 10:12:35.521059  26960 tablet_server_main.cc:225] ulimit cur(max)...
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
    ulimit: max user processes 12000(12000)"#.to_string();
        let result = parse_loglines(logline.clone());
        assert_eq!(result[0].message,"ulimit cur(max)...");
    }

    #[test]
    fn parse_process_context_sql_error_multiline_logline() {
        // This is a log line that contains a SQL error statement.
        // WARNING!! Currently, any following line is ignored.
        // This means success here means not correctly parsing!
        let logline = r#"W0221 17:18:06.190536  7924 process_context.cc:185] SQL Error: Type Not Found. Could not find user defined type
create table test (id int primary key, f1 tdxt);
                                          ^^^^"#.to_string();
        let result = parse_loglines(logline.clone());
        assert_eq!(result[0].message,"SQL Error: Type Not Found. Could not find user defined type");
    }

    #[test]
    fn parse_process_context_cql_error_multiline_logline() {
        // This is a log line that contains a CQL error statement.
        // WARNING!! Currently, any following line is ignored.
        // This means success here means not correctly parsing!
        let logline = r#"W0221 17:18:28.396759  7925 process_context.cc:185] SQL Error: Invalid CQL Statement. Missing list of target columns
insert into test values (1,'a');
                 ^^^^^^^^^^^^^^"#.to_string();
        let result = parse_loglines(logline.clone());
        assert_eq!(result[0].message,"SQL Error: Invalid CQL Statement. Missing list of target columns");
    }

    #[test]
    fn parse_fs_manager_multiline_logline() {
        // This is a log line that contains a fs manager message.
        // WARNING!! Currently, any following line is ignored.
        // This means success here means not correctly parsing!
        let logline = r#"I0227 17:18:28.714171  2510 fs_manager.cc:278] Opened local filesystem: /mnt/d0
uuid: "05b8d17620eb4cd79eddaddb2fbcbb42"
format_stamp: "Formatted at 2022-02-13 16:26:17 on yb-1.local""#.to_string();
        let result = parse_loglines(logline.clone());
        assert_eq!(result[0].message,"Opened local filesystem: /mnt/d0");
    }

    #[test]
    fn parse_writing_version_edit_logline() {
        // This is a log line that contains a version edit. It is an informal (I) line, I don't know how important this is.
        // WARNING!! Currently, any following line is ignored.
        // This means success here means not correctly parsing!
        let logline = r#"I0227 17:19:07.745766  3319 version_set.cc:3349] T b770079b94ad430493ba5f729fb1f0e7 P 05b8d17620eb4cd79eddaddb2fbcbb42 [R]: Writing version edit: log_number: 33
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
}"#.to_string();
        let result = parse_loglines(logline.clone());
        assert_eq!(result[0].message,"T b770079b94ad430493ba5f729fb1f0e7 P 05b8d17620eb4cd79eddaddb2fbcbb42 [R]: Writing version edit: log_number: 33");
    }

    #[test]
    fn parse_tablet_alter_schema_from_schema_multiline_logline() {
        // This is a log line that contains a tablet message.
        // WARNING!! Currently, any following line is ignored.
        // This means success here means not correctly parsing!
        let logline = r#"I0214 10:51:38.273955 10148 tablet.cc:1821] T c6099b05976f49d9b782ccbe126f9b2d P 05b8d17620eb4cd79eddaddb2fbcbb42: Alter schema from Schema [
        0:ybrowid[binary NOT NULL PARTITION KEY],
        1:dir[string NULLABLE NOT A PARTITION KEY],
        2:dirname[string NULLABLE NOT A PARTITION KEY]
]
properties: contain_counters: false is_transactional: true consistency_level: STRONG use_mangled_column_name: false is_ysql_catalog_table: false retain_delete_markers: false version 0 to Schema [
        0:ybrowid[binary NOT NULL PARTITION KEY],
        1:dir[string NULLABLE NOT A PARTITION KEY],
        2:dirname[string NULLABLE NOT A PARTITION KEY]
]
properties: contain_counters: false is_transactional: true consistency_level: STRONG use_mangled_column_name: false is_ysql_catalog_table: false retain_delete_markers: false version 1"#.to_string();
        let result = parse_loglines(logline.clone());
        assert_eq!(result[0].message,"T c6099b05976f49d9b782ccbe126f9b2d P 05b8d17620eb4cd79eddaddb2fbcbb42: Alter schema from Schema [");
    }

}

