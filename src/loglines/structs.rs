//! The structs
//!
use chrono::{DateTime, Local};

/// The root struct for deserializing the glog lines.
///
/// Please mind the glog format has some severe flaws for parsing.
/// - The timestamp has no year field, so we have to guess the year,
///   which is problematic when the timestamps include december and january.
/// - The timestamp has no timezone indicator, so we have to guess that too.
///   This is problematic if cluster nodes are in different timezones, and the servers set with different ones.
///   The parser has to guess the timezone, so it picks the parser machine timezone (Local).
///
/// There is no `pub timestamp: Option<DateTime<Local>>` field, because the fetch timestamp would have no function.
/// The only timestamp that matters is the logging timestamp, which is in the logging itself.
///
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct LogLine {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    pub severity: String,
    pub timestamp: DateTime<Local>,
    pub tid: String,
    pub sourcefile_nr: String,
    pub message: String,
}

#[derive(Debug, Default)]
pub struct AllLogLines {
    pub loglines: Vec<LogLine>,
}