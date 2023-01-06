//! The structs
use chrono::{DateTime, Local};
use std::collections::{BTreeMap};
/// The root struct for deserializing `/metrics`.
///
/// Struct to represent the metric entities found in the YugabyteDB
/// - master
/// - tablet server
/// - YSQL
/// metrics endpoint.
///
/// Mind the '[' at the top: this is a list of MetricEntity's.
///
/// This is how a metric entity looks like:
/// ```json
/// [
///     {
///         "type": "cluster",
///         "id": "yb.cluster",
///         "attributes": {},
///         "metrics": [
///             {
///                 "name": "is_load_balancing_enabled",
///                 "value": true
///             },
///             {
///                 "name": "num_tablet_servers_dead",
///                 "value": 0
///             },
///             {
///                 "name": "num_tablet_servers_live",
///                 "value": 3
///             }
///         ]
///     }
/// ```
/// There can be more than one entity, typically in the master and tablet servers.
/// Or one entity, typically in the YSQL server.
#[derive(Serialize, Deserialize, Debug)]
pub struct MetricEntity {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the timestamp.
    pub timestamp: Option<DateTime<Local>>,
    /// The json output contains a field 'type', which is not allowed as struct field name.
    /// For that reason, the name of the field 'type' is changed to 'metrics_type' here.
    #[serde(rename = "type")]
    pub metrics_type: String,
    pub id: String,
    pub attributes: Option<Attributes>,
    pub metrics: Vec<Metrics>,
}
/// Struct to represent the attributes nested json found in a [MetricEntity] entity.
/// Each of the fields in this json structure can be empty, which is why these are wrapped in options.
/// This is how an Attributes json looks like:
/// ```json
///       "attributes": {
///             "namespace_name": "yugabyte",
///             "table_name": "tt",
///             "table_id": "000033e8000030008000000000004100"
///       }
/// ```
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Attributes {
    pub namespace_name: Option<String>,
    pub table_name: Option<String>,
    /// The table_id for a table is the table id, and thus does match the MetricEntity.id.
    /// The table_id for a tablet is the table id, while the MetricEntity.id is the tablet id, and thus is different.
    pub table_id: Option<String>,
}
/// Enum to represent the possible metric types found the nested metrics json structure.
/// These can be a number of valid metric types:
///
/// - MetricValue: a name/value pair.
/// - MetricCountSum: a structure with a name, total_count (count of uses), total_sum (count of time),
///   min, mean, max statistical values, where the oddity is mean is a float, whilst others are u64 numbers.
///   And percentiles (75, 95, 99, 99,9, 99,99).
/// - MetricCountSumRows: a structure with a name, count (count of uses), sum (count of time) and rows (count of rows).
///
/// Please mind the metrics fetched as MetricCountSum are currently reset when fetched (!)
///
/// And a number of invalid metric types:
///
/// - RejectedU64MetricValue: a name/value pair where value ONLY fits in an u64 value.
/// - RejectedBooleanMetricValue: a name/value pair where the value is a boolean.
///
/// The way this works is that if a value is matched with an invalid metric type, it will be catched
/// in the function add_to_metric_vectors() and printed as a warning, if RUST_LOG is set.
/// If RUST_LOG is not set to a lower level than error, metrics that are ignored are printed to the screen.
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum Metrics {
    /// MetricValue is what serde will use for a value, such as:
    /// ```json
    ///             {
    ///                 "name": "mem_tracker_RegularDB_MemTable",
    ///                 "value": 198656
    ///             }
    /// ```
    MetricValue {
        name: String,
        value: i64,
    },
    /// MetricCountSum is what serde will use for a countsum, such as:
    /// ```json
    ///             {
    ///                 "name": "ql_read_latency",
    ///                 "total_count": 0,
    ///                 "min": 0,
    ///                 "mean": 0.0,
    ///                 "percentile_75": 0,
    ///                 "percentile_95": 0,
    ///                 "percentile_99": 0,
    ///                 "percentile_99_9": 0,
    ///                 "percentile_99_99": 0,
    ///                 "max": 0,
    ///                 "total_sum": 0
    ///             }
    /// ```
    MetricCountSum {
        name: String,
        total_count: u64,
        min: u64,
        /// This is a float. All other numbers are u64 numbers.
        mean: f64,
        percentile_75: u64,
        percentile_95: u64,
        percentile_99: u64,
        percentile_99_9: u64,
        percentile_99_99: u64,
        max: u64,
        total_sum: u64,
    },
    /// MetricCountSumRows is what serde will use for a countsumrows, such as:
    /// ```json
    ///             {
    ///                 "name": "handler_latency_yb_ysqlserver_SQLProcessor_SelectStmt",
    ///                 "count": 25,
    ///                 "sum": 631456,
    ///                 "rows": 26
    ///             }
    /// ```
    MetricCountSumRows {
        name: String,
        count: u64,
        sum: u64,
        rows: u64,
    },
    /// RejectedU64MetricValue is a value that cannot be parsed into the above MetricValue because the value doesn't fit into a signed 64 bit integer, but does fit into an unsigned 64 bit.
    /// An example of this:
    /// ```json
    ///     {
    ///         name: "threads_running_thread_pool",
    ///         value: 18446744073709551610,
    ///     }
    /// ```
    RejectedU64MetricValue {
        name: String,
        value: u64,
    },
    /// RejectedBooleanMetricValue is a value that cannot be parsed into the above MetricValue because the value doesn't fit because it is a boolean.
    /// An example of this:
    /// ```json
    ///             {
    ///                 "name": "is_load_balancing_enabled",
    ///                 "value": true
    ///             },
    /// ```
    RejectedBooleanMetricValue {
        name: String,
        value: bool,
    },
}
/// This struct is used by yb_stats for saving and loading the MetricEntity data.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AllMetricEntity {
    pub metricentity: Vec<MetricEntity>,
}
// diff
/// BTreeMap for storing a metricentity value.
///
/// The key fields are: `hostname_port`, `metric_type`, `metric_id`, `metric_name`
type BTreeMetricDiffValues = BTreeMap<(String, String, String, String), MetricDiffValues>;
/// The struct that holds the first and second snapshot statistics.
#[derive(Debug, Default)]
pub struct MetricDiffValues {
    pub table_name: String,
    pub namespace: String,
    pub first_snapshot_time: DateTime<Local>,
    pub first_value: i64,
    pub second_snapshot_time: DateTime<Local>,
    pub second_value: i64,
}
/// BTreeMap for storing a metricentity countsum.
///
/// The key fields are: `hostname_port`, `metric_type`, `metric_id`, `metric_name`
type BTreeMetricDiffCountSum = BTreeMap<(String, String, String, String), MetricDiffCountSum>;
/// The struct that holds the first and second snapshot statistics.
#[derive(Debug, Default)]
pub struct MetricDiffCountSum {
    pub table_name: String,
    pub namespace: String,
    pub first_snapshot_time: DateTime<Local>,
    pub first_total_sum: u64,
    pub first_total_count: u64,
    pub second_snapshot_time: DateTime<Local>,
    pub second_total_sum: u64,
    pub second_total_count: u64,
}
/// BTreeMap for storing a metricentity countsum.
///
/// The key fields are: `hostname_port`, `metric_type`, `metric_id`, `metric_name`
type BTreeMetricDiffCountSumRows = BTreeMap<(String, String, String, String), MetricDiffCountSumRows>;
/// The struct that holds the first and second snapshot statistics.
#[derive(Debug, Default)]
pub struct MetricDiffCountSumRows {
    pub table_name: String,
    pub namespace: String,
    pub first_snapshot_time: DateTime<Local>,
    pub first_count: u64,
    pub first_sum: u64,
    pub first_rows: u64,
    pub second_snapshot_time: DateTime<Local>,
    pub second_count: u64,
    pub second_sum: u64,
    pub second_rows: u64,
}
/// Wrapper for holding the diffs
#[derive(Debug, Default)]
pub struct MetricEntityDiff {
    pub btreemetricdiffvalue: BTreeMetricDiffValues,
    pub btreemetricdiffcountsum: BTreeMetricDiffCountSum,
    pub btreemetricdiffcountsumrows: BTreeMetricDiffCountSumRows,
}