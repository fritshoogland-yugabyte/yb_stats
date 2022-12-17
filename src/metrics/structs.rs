use chrono::{DateTime, Local};
use std::collections::{BTreeMap};
///
/// Struct to represent the metric entities found in the YugabyteDB master and tserver metrics endpoint.
///
/// The struct [MetricEntity] uses two child structs: [Attributes] and a vector of [Metrics].
///
/// This is how a metric entity looks like:
/// ```json
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
/// The entity is the the type cluster, contains no attributes, and contains 3 metrics.
/// This struct is used by serde to parse the json from the metrics endpoint into a struct.
/// The attributes nested json can be empty, which is why this wrapped in an option.
#[derive(Serialize, Deserialize, Debug)]
pub struct MetricEntity {
    /// The json output contains a field 'type', which is not allowed as struct field name.
    /// For that reason, the name of the field 'type' is changed to 'metrics_type' here.
    #[serde(rename = "type")]
    pub metrics_type: String,
    pub id: String,
    pub attributes: Option<Attributes>,
    pub metrics: Vec<Metrics>,
}
/// Struct to represent the attributes nested json found in a metric entity.
/// This struct is used by serde to parse the json from the metrics endpoint into a struct.
/// Each of the fields in this json structure can be empty, which is why these are wrapped in options.
/// This struct is used in the [MetricEntity] struct.
/// This is how an Attributes json looks like:
/// ```json
///       "attributes": {
///             "namespace_name": "yugabyte",
///             "table_name": "tt",
///             "table_id": "000033e8000030008000000000004100"
///       }
/// ```
#[derive(Serialize, Deserialize, Debug)]
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
#[allow(rustdoc::private_intra_doc_links)]
/// [StoredValues] is the format in which a [Metrics::MetricValue] is transformed into before it's used in the current 3 different ways of usage:
/// - Perform a snapshot: the parsed http endpoint data is first stored in [MetricEntity] and [Metrics::MetricValue], after which it's transformed into [StoredValues] format which then is saved as CSV using serde.
/// - Read a snapshot: the saved CSV data is read back into [StoredValues] using serde. Once in [StoredValues] format, it is added to the [AllStoredMetrics] super struct together with [StoredCountSum] and [StoredCountSumRows]. This is then diffed with another [AllStoredMetrics] struct using the [SnapshotDiffBTreeMapsMetrics::first_snapshot] and [SnapshotDiffBTreeMapsMetrics::second_snapshot] functions.
/// - Ad-hoc mode: Read and parse the http endpoint data into [MetricEntity] and [Metrics::MetricValue], after which it's transformed into [StoredValues]. Once in [StoredValues] format, it is added to the [AllStoredMetrics] super struct together with [StoredCountSum] and [StoredCountSumRows]. This is then diffed with another [AllStoredMetrics] struct using the [SnapshotDiffBTreeMapsMetrics::first_snapshot] and [SnapshotDiffBTreeMapsMetrics::second_snapshot] functions.
///
/// This struct is used in a superstruct called [AllStoredMetrics].
/// Th superstruct adds hostname and port and timestamp to the [MetricEntity] and [Metrics::MetricValue] data.
#[derive(Debug, Serialize, Deserialize)]
pub struct StoredValues {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub metric_type: String,
    pub metric_id: String,
    pub attribute_namespace: String,
    pub attribute_table_name: String,
    pub metric_name: String,
    pub metric_value: i64,
}
#[allow(rustdoc::private_intra_doc_links)]
/// [AllStoredMetrics] is a struct that functions as a superstruct for holding [StoredValues], [StoredCountSum] and [StoredCountSumRows].
/// It is the main struct that is used when dealing with the statistics obtained from YugabyteDB metric endpoints.
#[derive(Debug, Default)]
pub struct AllStoredMetrics {
    pub stored_values: Vec<StoredValues>,
    pub stored_countsum: Vec<StoredCountSum>,
    pub stored_countsumrows: Vec<StoredCountSumRows>,
}
#[allow(rustdoc::private_intra_doc_links)]
/// [StoredCountSum] is the format in which a [Metrics::MetricCountSum] is transformed into before it's used in the current 3 different ways of usage:
/// - Perform a snapshot: the parsed http endpoint data is first stored in [MetricEntity] and [Metrics::MetricCountSum], after which it's transformed into [StoredCountSum] format which then is saved as CSV using serde.
/// - Read a snapshot: the saved CSV data is read back into [StoredCountSum] using serde. Once in [StoredCountSum] format, it is added to the [AllStoredMetrics] super struct together with [StoredCountSum] and [StoredCountSumRows]. This is then diffed with another [AllStoredMetrics] struct using the [SnapshotDiffBTreeMapsMetrics::first_snapshot] and [SnapshotDiffBTreeMapsMetrics::second_snapshot] functions.
/// - Ad-hoc mode: Read and parse the http endpoint data into [MetricEntity] and [Metrics::MetricCountSum], after which it's transformed into [StoredCountSum]. Once in [StoredCountSum] format, it is added to the [AllStoredMetrics] super struct together with [StoredValues] and [StoredCountSumRows]. This is then diffed with another [AllStoredMetrics] struct using the [SnapshotDiffBTreeMapsMetrics::first_snapshot] and [SnapshotDiffBTreeMapsMetrics::second_snapshot] functions.
///
/// This struct is used in a superstruct called [AllStoredMetrics].
/// This struct adds hostname and port and timestamp to the [MetricEntity] and [Metrics::MetricCountSum] data.
#[derive(Debug, Serialize, Deserialize)]
pub struct StoredCountSum {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub metric_type: String,
    pub metric_id: String,
    pub attribute_namespace: String,
    pub attribute_table_name: String,
    pub metric_name: String,
    pub metric_total_count: u64,
    pub metric_min: u64,
    /// Please mind the f64, the other metrics are u64
    pub metric_mean: f64,
    pub metric_percentile_75: u64,
    pub metric_percentile_95: u64,
    pub metric_percentile_99: u64,
    pub metric_percentile_99_9: u64,
    pub metric_percentile_99_99: u64,
    pub metric_max: u64,
    pub metric_total_sum: u64,
}
#[allow(rustdoc::private_intra_doc_links)]
/// [StoredCountSumRows] is the format in which a [Metrics::MetricCountSumRows] is transformed into before it's used in the current 3 different ways of usage:
/// - Perform a snapshot: the parsed http endpoint data is first stored in [MetricEntity] and [Metrics::MetricCountSumRows], after which it's transformed into [StoredCountSumRows] format which then is saved as CSV using serde.
/// - Read a snapshot: the saved CSV data is read back into [StoredCountSumRows] using serde. Once in [StoredCountSumRows] format, it is added to the [AllStoredMetrics] super struct together with [StoredValues] and [StoredCountSum]. This is then diffed with another [AllStoredMetrics] struct using the [SnapshotDiffBTreeMapsMetrics::first_snapshot] and [SnapshotDiffBTreeMapsMetrics::second_snapshot] functions.
/// - Ad-hoc mode: Read and parse the http endpoint data into [MetricEntity] and [Metrics::MetricCountSumRows], after which it's transformed into [StoredCountSumRows]. Once in [StoredCountSumRows] format, it is added to the [AllStoredMetrics] super struct together with [StoredValues] and [StoredCountSum]. This is then diffed with another [AllStoredMetrics] struct using the [SnapshotDiffBTreeMapsMetrics::first_snapshot] and [SnapshotDiffBTreeMapsMetrics::second_snapshot] functions.
///
/// This struct is used in a superstruct called [AllStoredMetrics].
/// This struct adds hostname and port and timestamp to the [MetricEntity] and [Metrics::MetricCountSumRows] data.
#[derive(Debug, Serialize, Deserialize)]
pub struct StoredCountSumRows {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub metric_type: String,
    pub metric_id: String,
    pub attribute_namespace: String,
    pub attribute_table_name: String,
    pub metric_name: String,
    pub metric_count: u64,
    pub metric_sum: u64,
    pub metric_rows: u64,
}
#[allow(rustdoc::private_intra_doc_links)]
/// [SnapshotDiffValues] is the struct that holds the value data for being able to diff it.
/// It is used as the value in the [BTreeMapSnapshotDiffValues] BTreeMap, where the key is a record consisting of: hostname_port, metric_type, metric_id and metric_name.
/// Therefore, this struct doesn't need to contain the metric_name.
///
/// The base data for the diff are first_snapshot_value and a second_snapshot_value.
/// The extra statistic info is provided with the first_snapshot_time and second_snapshot_time times,
/// which allows us to understand the timestamps of the two snapshots,
/// which obviously allows us to calculate the amount of time between the two snapshots,
/// and thus allows the calculation of the difference of the values per second.
///
/// The table_name and namespace are added so that we can show the table_name and namespace, in case the `--details-enable` switch is used.
/// The table_name and namespace are not needed for uniqueness: that is guaranteed by the key of the BTreeMap, it's purely to have the ability to show table_name and namespace to the user.
/// In the case of the metric not being a table, tablet or cdc metricentity, the table_name and namespace are filled with a "-".
#[derive(Debug)]
pub struct SnapshotDiffValues {
    pub table_name: String,
    pub namespace: String,
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub first_snapshot_value: i64,
    pub second_snapshot_value: i64,
}
#[allow(rustdoc::private_intra_doc_links)]
/// [SnapshotDiffCountSum] is the struct that holds the countsum data for being able to diff it.
/// It is used as the value in the [BTreeMapSnapshotDiffCountSum] BTreeMap, where the key is a record consisting of: hostname_port, metric_type, metric_id and metric_name.
/// Therefore, this struct doesn't need to contain the metric_name.
///
/// The base data for the diff are first_snapshot_total_count and second_snapshot_total_count and first_snapshot_total_sum and second_snapshot_total_sum.
/// The extra statistic info is provided with the first_snapshot_time and second_snapshot_time times,
/// which allows us to understand the timestamps of the two snapshots,
/// which obviously allows us to calculate the amount of time between the two snapshots,
/// and thus allows the calculation of the difference per second.
///
/// The 'count' is the number of times the statistic was hit, the 'sum' is the amount the statistic is about.
/// This mostly is time, but sometimes be different, such as bytes or tasks, etc.
///
/// The table_name and namespace are added so that we can show the table_name and namespace, in case the `--details-enable` switch is used.
/// The table_name and namespace are not needed for uniqueness: that is guaranteed by the key of the BTreeMap, it's purely to have the ability to show table_name and namespace to the user.
/// In the case of the metric not being a table, tablet or cdc metricentity, the table_name and namespace are filled with a "-".
#[derive(Debug)]
pub struct SnapshotDiffCountSum {
    pub table_name: String,
    pub namespace: String,
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub second_snapshot_total_count: u64,
    pub second_snapshot_min: u64,
    /// Please mind the f64, the other metrics are u64
    pub second_snapshot_mean: f64,
    pub second_snapshot_percentile_75: u64,
    pub second_snapshot_percentile_95: u64,
    pub second_snapshot_percentile_99: u64,
    pub second_snapshot_percentile_99_9: u64,
    pub second_snapshot_percentile_99_99: u64,
    pub second_snapshot_max: u64,
    pub second_snapshot_total_sum: u64,
    pub first_snapshot_total_count: u64,
    pub first_snapshot_total_sum: u64,
}
#[allow(rustdoc::private_intra_doc_links)]
/// [SnapshotDiffCountSumRows] is the struct that holds the countsumrows data for being able to diff it.
/// It is used as the value in the [BTreeMapSnapshotDiffCountSumRows] BTreeMap, where the key is a record consisting of: hostname_port, metric_type, metric_id and metric_name.
/// Therefore, this struct doesn't need to contain the metric_name.
/// The data for [SnapshotDiffCountSumRows] comes from the 13000/YSQL endpoint uniquely. This metricentity is more limited, and doesn't use metric_id.
///
/// The base data for the diff are first_snapshot_count, first_snapshot_sum and first_snapshot_rows, and second_snapshot_count, second_snapshot_sum and second_snapshot_rows.
/// The extra statistic info is provided with the first_snapshot_time and second_snapshot_time times,
/// which allows us to understand the timestamps of the two snapshots,
/// which obviously allows us to calculate the amount of time between the two snapshots,
/// and thus allows the calculation of the difference per second.
///
/// The 'count' is the number of times the statistic was hit, the 'sum' is the amount the statistic is about.
/// The sum unit for CountSumRows/YSQL data currently is always milliseconds.
///
/// The table_name and namespace fields are taken from the parsed result, which always are "-", because these statistics don't specify these properties.
#[derive(Debug)]
pub struct SnapshotDiffCountSumRows {
    pub table_name: String,
    pub namespace: String,
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub first_snapshot_count: u64,
    pub first_snapshot_sum: u64,
    pub first_snapshot_rows: u64,
    pub second_snapshot_count: u64,
    pub second_snapshot_sum: u64,
    pub second_snapshot_rows: u64,
}

type BTreeMapSnapshotDiffValues = BTreeMap<(String, String, String, String), SnapshotDiffValues>;
type BTreeMapSnapshotDiffCountSum = BTreeMap<(String, String, String, String), SnapshotDiffCountSum>;
type BTreeMapSnapshotDiffCountSumRows = BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows>;

#[allow(rustdoc::private_intra_doc_links)]
/// The [SnapshotDiffBTreeMapsMetrics] struct holds 3 btreemaps for Values, CountSum and CountSumRows.
/// Which are [BTreeMapSnapshotDiffValues], [BTreeMapSnapshotDiffCountSum] and [BTreeMapSnapshotDiffCountSumRows].
/// The key for each of the btreemaps is a record of hostname_port, metric_type, metric_id and metric_name.
/// Because the key holds the metric_name, the struct that is the value belonging to the key doesn't need to hold that.
/// The values are the structs [SnapshotDiffValues], [SnapshotDiffCountSum] and [SnapshotDiffCountSumRows].
/// The reason for the BTreeMap is to order the output in a consistent and logical way, and to be able to find entries back based on the key.
#[derive(Default)]
pub struct SnapshotDiffBTreeMapsMetrics {
    pub btreemap_snapshotdiff_values: BTreeMapSnapshotDiffValues,
    pub btreemap_snapshotdiff_countsum: BTreeMapSnapshotDiffCountSum,
    pub btreemap_snapshotdiff_countsumrows: BTreeMapSnapshotDiffCountSumRows,
}
