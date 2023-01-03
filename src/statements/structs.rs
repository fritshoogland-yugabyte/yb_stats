//! The structs
//!
use chrono::{DateTime, Local};
use std::collections::BTreeMap;
/// The root struct for deserializing `/statements`.
///
/// This struct contains a single json object that holds a list:
/// ```text
/// {
///   "statements": []
/// }
/// ```
/// As soon as statements are executed, it lists the properties:
/// ```text
/// {
///   "statements": [
///     {
///       "query_id": -5860932178841992000,
///       "query": "select current_timestamp",
///       "calls": 1,
///       "total_time": 0.053192,
///       "min_time": 0.053192,
///       "max_time": 0.053192,
///       "mean_time": 0.053192,
///       "stddev_time": 0,
///       "rows": 1
///     }
///   ]
/// }
/// ```
/// Please mind query_id is new, and might not be exposed on older YB versions
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Statements {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the timestamp.
    pub timestamp: Option<DateTime<Local>>,
    pub statements: Vec<Statement>,
}
/// The statement entry holding the actual statement data.
/// Please mind postgresql pg_stat_statements groups queries by:
/// - queryid
/// - userid
/// - dbid
/// As you can see from the below fields, we do not expose userid and dbid at this time.
#[derive(Serialize, Deserialize, Debug)]
pub struct Statement {
    pub query_id: Option<i64>,
    pub query: String,
    pub calls: i64,
    pub total_time: f64,
    pub min_time: f64,
    pub max_time: f64,
    pub mean_time: f64,
    pub stddev_time: f64,
    pub rows: i64,
}
/// Wrapper struct for holding the different statements structs
#[derive(Debug, Default)]
pub struct AllStatements {
    pub statements: Vec<Statements>,
}
// diff
/// BTreeMap for storing a statements diff struct per `hostname_port` and `query`.
///
/// This is not elegant not fully correct, but we are limited by the exposed fields.
/// We need query_id, dbid, userid.
/// And even if it would be available, it would take for all clients to get to that version.
type BTreeStatementsDiff = BTreeMap<(String, String), GroupedStatements>;
/// The wrapper struct for holding the btreemap holding the diff structs.
#[derive(Debug, Default)]
pub struct StatementsDiff {
    pub btreestatementsdiff: BTreeStatementsDiff,
}
#[derive(Debug, Default)]
pub struct GroupedStatements {
    pub first_snapshot_time: DateTime<Local>,
    pub first_calls: i64,
    pub first_total_time: f64,
    pub first_rows: i64,
    pub second_snapshot_time: DateTime<Local>,
    pub second_calls: i64,
    pub second_total_time: f64,
    pub second_rows: i64,
}


#[derive(Debug)]
pub struct UniqueStatementData {
    pub calls: i64,
    pub total_time: f64,
    pub min_time: f64,
    pub max_time: f64,
    pub mean_time: f64,
    pub stddev_time: f64,
    pub rows: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredStatements {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub query: String,
    pub calls: i64,
    pub total_time: f64,
    pub min_time: f64,
    pub max_time: f64,
    pub mean_time: f64,
    pub stddev_time: f64,
    pub rows: i64,
}


#[derive(Debug)]
pub struct SnapshotDiffStatements {
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub first_calls: i64,
    pub second_calls: i64,
    pub first_total_time: f64,
    pub second_total_time: f64,
    pub first_rows: i64,
    pub second_rows: i64,
}

type BTreeMapSnapshotDiffStatements = BTreeMap<(String, String), SnapshotDiffStatements>;

#[derive(Default)]
pub struct SnapshotDiffBTreeMapStatements {
    pub btreemap_snapshotdiff_statements: BTreeMapSnapshotDiffStatements,
}


#[derive(Debug, Default)]
pub struct AllStoredStatements {
    pub stored_statements: Vec<StoredStatements>
}

