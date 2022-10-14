use std::{process, fs, env, error::Error, sync::mpsc::channel, collections::BTreeMap};
use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use serde_derive::{Serialize,Deserialize};
use regex::Regex;
use substring::Substring;
use rayon;
use log::*;
// the value_statistic_details crate allows to create a lookup table for statistics
use crate::value_statistic_details::ValueStatistics;
// the countsum_statistic_details crate allows to create a lookup table for statistics
use crate::countsum_statistic_details::CountSumStatistics;
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
/// StoredValues is the format in which a MetricValue is transformed into before it's used in the current 3 different ways of usage:
/// - Perform a snapshot, which reads and parses the metrics http endpoints into this struct for MetricValue data, saving the output into a CSV file.
/// - Read a snapshot from a file into StoredValues, which is used in the super struct [AllStoredMetrics], which is used for the 'diff' procedures to 'diff' it with another snapshot.
/// - Read and parse the http endpoint data into StoredValues, which is used in the super struct [AllStoredMetrics], which is used for the 'diff' procedures to 'diff' it.
///
/// This struct is used in a superstruct called [AllStoredMetrics].
/// This struct adds hostname and port and timestamp to the [MetricEntity] data.
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

impl StoredValues {
    /// This is a private function to create a StoredValues struct.
    fn new(hostname_port: &str,
           timestamp: DateTime<Local>,
           metric: &MetricEntity,
           attribute_namespace: &str,
           attribute_table_name: &str,
           metric_name: &str,
           metric_value: i64,
    ) -> Self {
        Self {
            hostname_port: hostname_port.to_string(),
            timestamp,
            metric_type: metric.metrics_type.to_string(),
            metric_id: metric.id.to_string(),
            attribute_namespace: attribute_namespace.to_string(),
            attribute_table_name: attribute_table_name.to_string(),
            metric_name: metric_name.to_string(),
            metric_value,
        }
    }
}
/// [StoredCountSum] is the format in which a MetricCountSum is transformed into before it's used in the current 3 different ways of usage:
/// - Perform a snapshot, which reads and parses the metrics http endpoints into this struct for MetricCountSum data, saving the output into a CSV file.
/// - Read a snapshot from a file into [StoredCountSum], which is used in the super struct [AllStoredMetrics], which is used for the 'diff' procedures to 'diff' it with another snapshot.
/// - Read and parse the http endpoint data into [StoredCountSum], which is used in the super struct [AllStoredMetrics], which is used for the 'diff' procedures to 'diff' it.
///
/// This struct is used in a superstruct called [AllStoredMetrics].
/// This struct adds hostname and port and timestamp to the [MetricEntity] data.
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
    pub metric_mean: f64,
    pub metric_percentile_75: u64,
    pub metric_percentile_95: u64,
    pub metric_percentile_99: u64,
    pub metric_percentile_99_9: u64,
    pub metric_percentile_99_99: u64,
    pub metric_max: u64,
    pub metric_total_sum: u64,
}

impl StoredCountSum {
    /// This is a private function to create a StoredCountSum struct.
    #[allow(clippy::too_many_arguments)]
    fn new(hostname_port: &str,
           timestamp: DateTime<Local>,
           metric: &MetricEntity,
           attribute_namespace: &str,
           attribute_table_name: &str,
           metric_name: &str,
           metric_total_count: u64,
           metric_min: u64,
           metric_mean: f64,
           metric_percentile_75: u64,
           metric_percentile_95: u64,
           metric_percentile_99: u64,
           metric_percentile_99_9: u64,
           metric_percentile_99_99: u64,
           metric_max: u64,
           metric_total_sum: u64,
          ) -> Self {
        Self {
            hostname_port: hostname_port.to_string(),
            timestamp,
            metric_type: metric.metrics_type.to_string(),
            metric_id: metric.id.to_string(),
            attribute_namespace: attribute_namespace.to_string(),
            attribute_table_name: attribute_table_name.to_string(),
            metric_name: metric_name.to_string(),
            metric_total_count,
            metric_min,
            metric_mean,
            metric_percentile_75,
            metric_percentile_95,
            metric_percentile_99,
            metric_percentile_99_9,
            metric_percentile_99_99,
            metric_max,
            metric_total_sum,
        }
    }
}
/// StoredCountSumRows is the format in which a MetricCountSumRows is transformed into before it's used in the current 3 different ways of usage:
/// - Perform a snapshot, which reads and parses the metrics http endpoints into this struct for MetricCountSumRows data, saving the output into a CSV file.
/// - Read a snapshot from a file into StoredCountSumRows, which is used in the super struct [AllStoredMetrics], which is used for the 'diff' procedures to 'diff' it with another snapshot.
/// - Read and parse the http endpoint data into StoredCountSumRows, which is used in the super struct [AllStoredMetrics], which is used for the 'diff' procedures to 'diff' it.
///
/// This struct is used in a superstruct called [AllStoredMetrics].
/// This struct adds hostname and port and timestamp to the [MetricEntity] data.
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

impl StoredCountSumRows {
    /// This is a private function to create a StoredCountSumRows struct.
    #[allow(clippy::too_many_arguments)]
    fn new(hostname_port: &str,
           timestamp: DateTime<Local>,
           metric: &MetricEntity,
           attribute_namespace: &str,
           attribute_table_name: &str,
           metric_name: &str,
           metric_count: u64,
           metric_sum: u64,
           metric_rows: u64
    ) -> Self {
        Self {
            hostname_port: hostname_port.to_string(),
            timestamp,
            metric_type: metric.metrics_type.to_string(),
            metric_id: metric.id.to_string(),
            attribute_namespace: attribute_namespace.to_string(),
            attribute_table_name: attribute_table_name.to_string(),
            metric_name: metric_name.to_string(),
            metric_count,
            metric_sum,
            metric_rows,
        }
    }
}
/// SnapshotDiffValues is the struct that holds the value data for being able to diff value data.
/// The base data for the diff are first_snapshot_value and a second_snapshot_value.
/// The extra info is provided with the first_snapshot_time and second_snapshot_time,
/// which allows us to understand the timestamps of the two snapshots,
/// which obviously allows us to calculate the amount of time between the two snapshots,
/// and thus allows the calculation of the difference of the values per second.
///
/// The table_name and namespace are added so that we can use this data per table.
#[derive(Debug)]
pub struct SnapshotDiffValues {
    pub table_name: String,
    pub namespace: String,
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub first_snapshot_value: i64,
    pub second_snapshot_value: i64,
}

impl SnapshotDiffValues {
    /// This is a private function that values from a [StoredValues] struct and creates a SnapshotDiffValues struct for the first snapshot.
    /// The insertion of a first snapshot obviously fills out the first snapshot time and value.
    /// The second snapshot time is set to the first snapshot time, and the value to 0.
    /// In that way, when this value doesn't occur in the second snapshot, it can be detected by the value 0, and thus omitted from output.
    fn first_snapshot(storedvalues: StoredValues) -> Self {
        Self {
            table_name: storedvalues.attribute_table_name.to_string(),
            namespace: storedvalues.attribute_namespace.to_string(),
            first_snapshot_time: storedvalues.timestamp,
            second_snapshot_time: storedvalues.timestamp,
            first_snapshot_value: storedvalues.metric_value,
            second_snapshot_value: 0,
        }
    }
    /// This is a private function that takes values from a [StoredValues] struct and inserts relevant details into a SnapshotDiffValues struct for the second snapshot .
    /// This happens using an existing SnapshotDiffValues struct, where only the second snapshot time and value are set using the [StoredValues] struct values.
    fn second_snapshot_existing(values_diff_row: &mut SnapshotDiffValues, storedvalues: StoredValues) -> Self {
        Self {
            table_name: values_diff_row.table_name.to_string(),
            namespace: values_diff_row.namespace.to_string(),
            first_snapshot_time: values_diff_row.first_snapshot_time,
            second_snapshot_time: storedvalues.timestamp,
            first_snapshot_value: values_diff_row.first_snapshot_value,
            second_snapshot_value: storedvalues.metric_value,
        }
    }
    /// This is a private function that takes values from a [StoredValues] struct and creates a SnapshotDiffValues struct for the second snapshot.
    /// The second snapshot being a new value happens for example if the second snapshot measures statistics from a new tablet, where new means created after the first snapshot was created.
    /// This means we need at least the first snapshot time, which has to be provided, because we cannot derive that from the data in [StoredValues]
    /// The value for the first snapshot will be 0.
    fn second_snapshot_new(storedvalues: StoredValues, first_snapshot_time: DateTime<Local>) -> Self {
        Self {
            table_name: storedvalues.attribute_table_name.to_string(),
            namespace: storedvalues.attribute_namespace.to_string(),
            first_snapshot_time,
            second_snapshot_time: storedvalues.timestamp,
            first_snapshot_value: 0,
            second_snapshot_value: storedvalues.metric_value,
        }
    }
    /// This is a private function for a special use of SnapshotDiffValues, which happens in the [SnapshotDiffBtreeMaps::print] function.
    /// The special use is if the `--details-enable` flag is not set, statistics that are kept per table, tablet or cdc as metric_type as added together per server.
    /// This function takes an existing SnapshotDiffValues struct, and adds the values of first_snapshot_value and second_snapshot_value from another SnapshotDiffValues struct.
    fn diff_sum_existing(sum_value_diff_row: &mut SnapshotDiffValues, value_diff_row: &SnapshotDiffValues) -> Self {
        Self {
            table_name: sum_value_diff_row.table_name.to_string(),
            namespace: sum_value_diff_row.namespace.to_string(),
            first_snapshot_time: sum_value_diff_row.first_snapshot_time,
            second_snapshot_time: sum_value_diff_row.second_snapshot_time,
            first_snapshot_value: sum_value_diff_row.first_snapshot_value + value_diff_row.first_snapshot_value,
            second_snapshot_value: sum_value_diff_row.second_snapshot_value + value_diff_row.second_snapshot_value,
        }
    }
    /// This is a private function for a special use of SnapshotDiffValues, which happens in the [SnapshotDiffBtreeMaps::print] function.
    /// The special use is if the `--details-enable` flag is not set, statistics that are kept per table, tablet or cdc as metric_type as added together per server.
    /// This function creates the first occurence of a SnapshotDiffValues struct with the intention to have others be added for the first_snapshot_value and second_snapshot_value fields.
    fn diff_sum_new(value_diff_row: &SnapshotDiffValues) -> Self {
        Self {
            table_name: "-".to_string(),
            namespace: "-".to_string(),
            first_snapshot_time: value_diff_row.first_snapshot_time,
            second_snapshot_time: value_diff_row.second_snapshot_time,
            first_snapshot_value: value_diff_row.first_snapshot_value,
            second_snapshot_value: value_diff_row.second_snapshot_value,
        }
    }
}

#[derive(Debug)]
pub struct SnapshotDiffCountSum {
    pub table_name: String,
    pub namespace: String,
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub second_snapshot_total_count: u64,
    pub second_snapshot_min: u64,
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

impl SnapshotDiffCountSum {
    fn first_snapshot(storedcountsum: StoredCountSum) -> Self {
        Self {
            table_name: storedcountsum.attribute_table_name.to_string(),
            namespace: storedcountsum.attribute_namespace.to_string(),
            first_snapshot_time: storedcountsum.timestamp,
            second_snapshot_time: storedcountsum.timestamp,
            second_snapshot_total_count: 0,
            second_snapshot_min: 0,
            second_snapshot_mean: 0.,
            second_snapshot_percentile_75: 0,
            second_snapshot_percentile_95: 0,
            second_snapshot_percentile_99: 0,
            second_snapshot_percentile_99_9: 0,
            second_snapshot_percentile_99_99: 0,
            second_snapshot_max: 0,
            second_snapshot_total_sum: 0,
            first_snapshot_total_count: storedcountsum.metric_total_count,
            first_snapshot_total_sum: storedcountsum.metric_total_sum,
        }
    }
    fn second_snapshot_existing(countsum_diff_row: &mut SnapshotDiffCountSum, storedcountsum: StoredCountSum) -> Self
    {
        Self {
            table_name: countsum_diff_row.table_name.to_string(),
            namespace: countsum_diff_row.namespace.to_string(),
            first_snapshot_time: countsum_diff_row.first_snapshot_time,
            second_snapshot_time: storedcountsum.timestamp,
            second_snapshot_total_count: storedcountsum.metric_total_count,
            second_snapshot_min: storedcountsum.metric_min,
            second_snapshot_mean: storedcountsum.metric_mean,
            second_snapshot_percentile_75: storedcountsum.metric_percentile_75,
            second_snapshot_percentile_95: storedcountsum.metric_percentile_95,
            second_snapshot_percentile_99: storedcountsum.metric_percentile_99,
            second_snapshot_percentile_99_9: storedcountsum.metric_percentile_99_9,
            second_snapshot_percentile_99_99: storedcountsum.metric_percentile_99_99,
            second_snapshot_max: storedcountsum.metric_max,
            second_snapshot_total_sum: storedcountsum.metric_total_sum,
            first_snapshot_total_count: countsum_diff_row.first_snapshot_total_count,
            first_snapshot_total_sum: countsum_diff_row.first_snapshot_total_sum,
        }
    }
    fn second_snapshot_new(storedcountsum: StoredCountSum, first_snapshot_time: DateTime<Local>) -> Self
    {
        Self {
            table_name: storedcountsum.attribute_table_name.to_string(),
            namespace: storedcountsum.attribute_namespace.to_string(),
            first_snapshot_time,
            second_snapshot_time: storedcountsum.timestamp,
            second_snapshot_total_count: storedcountsum.metric_total_count,
            second_snapshot_min: storedcountsum.metric_min,
            second_snapshot_mean: storedcountsum.metric_mean,
            second_snapshot_percentile_75: storedcountsum.metric_percentile_75,
            second_snapshot_percentile_95: storedcountsum.metric_percentile_95,
            second_snapshot_percentile_99: storedcountsum.metric_percentile_99,
            second_snapshot_percentile_99_9: storedcountsum.metric_percentile_99_9,
            second_snapshot_percentile_99_99: storedcountsum.metric_percentile_99_99,
            second_snapshot_max: storedcountsum.metric_max,
            second_snapshot_total_sum: storedcountsum.metric_total_sum,
            first_snapshot_total_count: 0,
            first_snapshot_total_sum: 0,
        }
    }
    fn diff_sum_existing(sum_countsum_diff_row: &mut SnapshotDiffCountSum, countsum_diff_row: &SnapshotDiffCountSum) -> Self
    {
        Self {
            table_name: sum_countsum_diff_row.table_name.to_string(),
            namespace: sum_countsum_diff_row.namespace.to_string(),
            first_snapshot_time: sum_countsum_diff_row.first_snapshot_time,
            second_snapshot_time: sum_countsum_diff_row.second_snapshot_time,
            second_snapshot_total_count: sum_countsum_diff_row.second_snapshot_total_count + countsum_diff_row.second_snapshot_total_count,
            second_snapshot_min: 0,
            second_snapshot_mean: 0.,
            second_snapshot_percentile_75: 0,
            second_snapshot_percentile_95: 0,
            second_snapshot_percentile_99: 0,
            second_snapshot_percentile_99_9: 0,
            second_snapshot_percentile_99_99: 0,
            second_snapshot_max: 0,
            second_snapshot_total_sum: sum_countsum_diff_row.second_snapshot_total_sum + countsum_diff_row.second_snapshot_total_sum,
            first_snapshot_total_count: sum_countsum_diff_row.first_snapshot_total_count + countsum_diff_row.first_snapshot_total_count,
            first_snapshot_total_sum: sum_countsum_diff_row.first_snapshot_total_sum + countsum_diff_row.first_snapshot_total_sum,
        }
    }
    fn diff_sum_new(countsum_diff_row: &SnapshotDiffCountSum) -> Self
    {
        Self {
            table_name: "-".to_string(),
            namespace: "-".to_string(),
            first_snapshot_time: countsum_diff_row.first_snapshot_time,
            second_snapshot_time: countsum_diff_row.second_snapshot_time,
            second_snapshot_total_count: countsum_diff_row.second_snapshot_total_count,
            second_snapshot_min: 0,
            second_snapshot_mean: 0.,
            second_snapshot_percentile_75: 0,
            second_snapshot_percentile_95: 0,
            second_snapshot_percentile_99: 0,
            second_snapshot_percentile_99_9: 0,
            second_snapshot_percentile_99_99: 0,
            second_snapshot_max: 0,
            second_snapshot_total_sum: countsum_diff_row.second_snapshot_total_sum,
            first_snapshot_total_count: countsum_diff_row.first_snapshot_total_count,
            first_snapshot_total_sum: countsum_diff_row.first_snapshot_total_sum,
        }
    }
}

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

impl SnapshotDiffCountSumRows {
    fn first_snapshot(storedcountsumrows: StoredCountSumRows) -> Self {
        Self {
            table_name: storedcountsumrows.attribute_table_name.to_string(),
            namespace: storedcountsumrows.attribute_namespace.to_string(),
            first_snapshot_time: storedcountsumrows.timestamp,
            second_snapshot_time: storedcountsumrows.timestamp,
            first_snapshot_count: storedcountsumrows.metric_count,
            first_snapshot_sum: storedcountsumrows.metric_sum,
            first_snapshot_rows: storedcountsumrows.metric_rows,
            second_snapshot_count: 0,
            second_snapshot_sum: 0,
            second_snapshot_rows: 0,
        }
    }
    fn second_snapshot_existing(countsumrows_diff_row: &mut SnapshotDiffCountSumRows, storedcountsumrows: StoredCountSumRows) -> Self {
        Self {
            table_name: countsumrows_diff_row.table_name.to_string(),
            namespace: countsumrows_diff_row.namespace.to_string(),
            first_snapshot_time: countsumrows_diff_row.first_snapshot_time,
            second_snapshot_time: storedcountsumrows.timestamp,
            first_snapshot_count: countsumrows_diff_row.first_snapshot_count,
            first_snapshot_sum: countsumrows_diff_row.first_snapshot_sum,
            first_snapshot_rows: countsumrows_diff_row.first_snapshot_rows,
            second_snapshot_count: storedcountsumrows.metric_count,
            second_snapshot_sum: storedcountsumrows.metric_sum,
            second_snapshot_rows: storedcountsumrows.metric_rows,
        }
    }
    fn second_snapshot_new(storedcountsumrows: StoredCountSumRows, first_snapshot_time: DateTime<Local>) -> Self {
        Self {
            table_name: storedcountsumrows.attribute_table_name.to_string(),
            namespace: storedcountsumrows.attribute_namespace.to_string(),
            first_snapshot_time,
            second_snapshot_time: storedcountsumrows.timestamp,
            first_snapshot_count: 0,
            first_snapshot_sum: 0,
            first_snapshot_rows: 0,
            second_snapshot_count: storedcountsumrows.metric_count,
            second_snapshot_sum: storedcountsumrows.metric_sum,
            second_snapshot_rows: storedcountsumrows.metric_rows,
        }
    }
}

#[derive(Debug)]
pub struct AllStoredMetrics {
    pub stored_values: Vec<StoredValues>,
    pub stored_countsum: Vec<StoredCountSum>,
    pub stored_countsumrows: Vec<StoredCountSumRows>,
}

impl AllStoredMetrics {
    pub fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) {
        info!("perform_snapshot (metrics)");

        let allstoredmetrics = AllStoredMetrics::read_metrics(hosts, ports, parallel);
        allstoredmetrics.save_snapshot(snapshot_number)
            .unwrap_or_else(|e| {
                error!("error saving snapshot: {}",e);
                process::exit(1);
            });
    }
    fn read_metrics (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllStoredMetrics {
        let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
        let (tx, rx) = channel();
        pool.scope(move |s| {
            for host in hosts {
                for port in ports {
                    let tx = tx.clone();
                    s.spawn(move |_| {
                        let detail_snapshot_time = Local::now();
                        let metrics = read_metrics(host, port);
                        tx.send((format!("{}:{}", host, port), detail_snapshot_time, metrics)).expect("error sending data via tx (metrics)");
                    });
                }
            }
        });

        let mut allstoredmetrics = AllStoredMetrics { stored_values: Vec::new(), stored_countsum: Vec::new(), stored_countsumrows: Vec::new() };
        for (hostname_port, detail_snapshot_time, metrics) in rx {
            add_to_metric_vectors(metrics, &hostname_port, detail_snapshot_time, &mut allstoredmetrics);
        }

        allstoredmetrics
    }
    fn save_snapshot ( self, snapshot_number: i32, ) -> Result<(), Box<dyn Error>>
    {
        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number.to_string());

        let values_file = &current_snapshot_directory.join("values");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&values_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_values {
            writer.serialize(row)?;
        }
        writer.flush()?;

        let countsum_file = &current_snapshot_directory.join("countsum");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&countsum_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_countsum {
            writer.serialize(row)?;
        }
        writer.flush()?;

        let countsumrows_file = &current_snapshot_directory.join("countsumrows");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&countsumrows_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_countsumrows {
            writer.serialize(row)?;
        }
        writer.flush()?;

        Ok(())
    }
    fn read_snapshot( snapshot_number: &String, ) -> Result<AllStoredMetrics, Box<dyn Error>>
    {
        let mut allstoredmetrics = AllStoredMetrics { stored_values: Vec::new(), stored_countsum: Vec::new(), stored_countsumrows: Vec::new() };

        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number);

        let values_file = &current_snapshot_directory.join("values");
        let file = fs::File::open(&values_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredValues = row?;
            allstoredmetrics.stored_values.push(data);
        };

        let countsum_file = &current_snapshot_directory.join("countsum");
        let file = fs::File::open(&countsum_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredCountSum = row?;
            allstoredmetrics.stored_countsum.push(data);
        };

        let countsumrows_file = &current_snapshot_directory.join("countsumrows");
        let file = fs::File::open(&countsumrows_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredCountSumRows = row?;
            allstoredmetrics.stored_countsumrows.push(data);
        };

        Ok(allstoredmetrics)
    }
}

type BTreeMapSnapshotDiffValues = BTreeMap<(String, String, String, String), SnapshotDiffValues>;
type BTreeMapSnapshotDiffCountSum = BTreeMap<(String, String, String, String), SnapshotDiffCountSum>;
type BTreeMapSnapshotDiffCountSumRows = BTreeMap<(String, String, String, String), SnapshotDiffCountSumRows>;

pub struct SnapshotDiffBtreeMaps {
    pub btreemap_snapshotdiff_values: BTreeMapSnapshotDiffValues,
    pub btreemap_snapshotdiff_countsum: BTreeMapSnapshotDiffCountSum,
    pub btreemap_snapshotdiff_countsumrows: BTreeMapSnapshotDiffCountSumRows,
}

impl SnapshotDiffBtreeMaps {
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
        begin_snapshot_timestamp: &DateTime<Local>,
    ) -> SnapshotDiffBtreeMaps {
        let allstoredmetrics = AllStoredMetrics::read_snapshot(begin_snapshot)
            .unwrap_or_else(|e| {
                error!("Fatal: error reading snapshot: {}", e);
                process::exit(1);
            });

        let mut metrics_snapshot_diff = SnapshotDiffBtreeMaps::first_snapshot(allstoredmetrics);

        let allstoredmetrics = AllStoredMetrics::read_snapshot(end_snapshot)
            .unwrap_or_else(|e| {
                error!("Fatal: error reading snapshot: {}", e);
                process::exit(1);
            });

        metrics_snapshot_diff.second_snapshot(allstoredmetrics, begin_snapshot_timestamp);

        metrics_snapshot_diff
    }
    pub fn adhoc_read_first_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> SnapshotDiffBtreeMaps {
        let allstoredmetrics = AllStoredMetrics::read_metrics(hosts, ports, parallel);
        SnapshotDiffBtreeMaps::first_snapshot(allstoredmetrics)
    }
    pub fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
        first_snapshot_time: &DateTime<Local>,
    ) {
        let allstoredmetrics = AllStoredMetrics::read_metrics(hosts, ports, parallel);
        self.second_snapshot(allstoredmetrics, first_snapshot_time);
    }
    fn first_snapshot(allstoredmetrics: AllStoredMetrics) -> SnapshotDiffBtreeMaps {
        let mut snapshotdiff_btreemaps = SnapshotDiffBtreeMaps { btreemap_snapshotdiff_values: BTreeMap::new(), btreemap_snapshotdiff_countsum: BTreeMap::new(), btreemap_snapshotdiff_countsumrows: BTreeMap::new() };
        // values
        for row in allstoredmetrics.stored_values {
            if row.metric_type == "table" || row.metric_type == "tablet" || row.metric_type == "cdc" {
                match snapshotdiff_btreemaps.btreemap_snapshotdiff_values.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
                    Some(_value_row) => {
                        panic!("Error: (SnapshotDiffBtreeMaps values) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port, &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                    },
                    None => {
                        snapshotdiff_btreemaps.btreemap_snapshotdiff_values.insert(
                            (row.hostname_port.to_string(), row.metric_type.to_string(), row.metric_id.to_string(), row.metric_name.to_string()),
                            SnapshotDiffValues::first_snapshot(row)
                        );
                    },
                }
            } else {
                match snapshotdiff_btreemaps.btreemap_snapshotdiff_values.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone())) {
                    Some(_value_row) => {
                        panic!("Error: (SnapshotDiffBtreeMaps values) found second entry for hostname: {}, type: {}, id: {} name: {}", &row.hostname_port, &row.metric_type.clone(), String::from("-"), &row.metric_name.clone());
                    },
                    None => {
                        snapshotdiff_btreemaps.btreemap_snapshotdiff_values.insert(
                            (row.hostname_port.to_string(), row.metric_type.to_string(), String::from("-"), row.metric_name.to_string()),
                            SnapshotDiffValues::first_snapshot(row)
                        );
                    },
                }
            }
        }
        // countsum
        for row in allstoredmetrics.stored_countsum {
            if row.metric_type == "table" || row.metric_type == "tablet" || row.metric_type == "cdc" {
                match snapshotdiff_btreemaps.btreemap_snapshotdiff_countsum.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
                    Some(_countsum_row) => {
                        panic!("Error: (SnapshotDiffBtreeMaps countsum) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port, &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                    },
                    None => {
                        snapshotdiff_btreemaps.btreemap_snapshotdiff_countsum.insert(
                            (row.hostname_port.to_string(), row.metric_type.to_string(), row.metric_id.to_string(), row.metric_name.to_string()),
                            SnapshotDiffCountSum::first_snapshot(row)
                        );
                    },
                }
            } else {
                match snapshotdiff_btreemaps.btreemap_snapshotdiff_countsum.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone())) {
                    Some(_countsum_row) => {
                        panic!("Error: (SnapshotDiffBtreeMaps countsum) found second entry for hostname: {}, type: {}, id: {} name: {}", &row.hostname_port, &row.metric_type.clone(), String::from("-"), &row.metric_name.clone());
                    },
                    None => {
                        snapshotdiff_btreemaps.btreemap_snapshotdiff_countsum.insert(
                            (row.hostname_port.to_string(), row.metric_type.to_string(), String::from("-"), row.metric_name.to_string()),
                            SnapshotDiffCountSum::first_snapshot(row)
                        );
                    },
                }
            }
        }
        // countsumrows
        for row in allstoredmetrics.stored_countsumrows {
            match snapshotdiff_btreemaps.btreemap_snapshotdiff_countsumrows.get_mut(&(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
                Some(_countsumrows_row) => {
                    panic!("Error: (SnapshotDiffBtreeMaps countsumrows) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port, &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                },
                None => {
                    snapshotdiff_btreemaps.btreemap_snapshotdiff_countsumrows.insert(
                        (row.hostname_port.to_string(), row.metric_type.to_string(), row.metric_id.to_string(), row.metric_name.to_string()),
                        SnapshotDiffCountSumRows::first_snapshot(row),
                    );
                },
            }
        }
        snapshotdiff_btreemaps
    }
    fn second_snapshot(
        &mut self,
        allstoredmetrics: AllStoredMetrics,
        first_snapshot_time: &DateTime<Local>,
    )
    {
        // values
        for row in allstoredmetrics.stored_values {
            if row.metric_type == "table" || row.metric_type == "tablet" || row.metric_type == "cdc" {
                match self.btreemap_snapshotdiff_values.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
                    Some(value_row) => {
                        *value_row = SnapshotDiffValues::second_snapshot_existing(value_row, row);
                    },
                    None => {
                        self.btreemap_snapshotdiff_values.insert(
                            (row.hostname_port.to_string(), row.metric_type.to_string(), row.metric_id.to_string(), row.metric_name.to_string()),
                            SnapshotDiffValues::second_snapshot_new(row, *first_snapshot_time)
                        );
                    },
                }
            } else {
                match self.btreemap_snapshotdiff_values.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone())) {
                    Some(value_row) => {
                        *value_row = SnapshotDiffValues::second_snapshot_existing(value_row, row);
                    },
                    None => {
                        self.btreemap_snapshotdiff_values.insert(
                            (row.hostname_port.to_string(), row.metric_type.to_string(), String::from("-"), row.metric_name.to_string()),
                            SnapshotDiffValues::second_snapshot_new(row, *first_snapshot_time)
                        );
                    },
                }
            }
        }
        // countsum
        for row in allstoredmetrics.stored_countsum {
            if row.metric_type == "table" || row.metric_type == "tablet" || row.metric_type == "cdc" {
                match self.btreemap_snapshotdiff_countsum.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
                    Some(countsum_row) => {
                        *countsum_row = SnapshotDiffCountSum::second_snapshot_existing(countsum_row, row);
                    },
                    None => {
                        self.btreemap_snapshotdiff_countsum.insert(
                            (row.hostname_port.to_string(), row.metric_type.to_string(), row.metric_id.to_string(), row.metric_name.to_string()),
                            SnapshotDiffCountSum::second_snapshot_new(row, *first_snapshot_time)
                        );
                    },
                }
            } else {
                match self.btreemap_snapshotdiff_countsum.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone())) {
                    Some(countsum_row) => {
                        *countsum_row = SnapshotDiffCountSum::second_snapshot_existing(countsum_row, row);
                    },
                    None => {
                        self.btreemap_snapshotdiff_countsum.insert(
                            (row.hostname_port.to_string(), row.metric_type.to_string(), String::from("-"), row.metric_name.to_string()),
                            SnapshotDiffCountSum::second_snapshot_new(row, *first_snapshot_time)
                        );
                    },
                }
            }
        }
        // countsumrows
        for row in allstoredmetrics.stored_countsumrows {
            match self.btreemap_snapshotdiff_countsumrows.get_mut(&(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
                Some(countsumrows_row) => {
                    *countsumrows_row = SnapshotDiffCountSumRows::second_snapshot_existing(countsumrows_row, row);
                },
                None => {
                    self.btreemap_snapshotdiff_countsumrows.insert(
                        (row.hostname_port.to_string(), row.metric_type.to_string(), row.metric_id.to_string(), row.metric_name.to_string()),
                        SnapshotDiffCountSumRows::second_snapshot_new(row, *first_snapshot_time)
                    );
                },
            }
        }
    }
    pub fn print(
        &self,
        hostname_filter: &Regex,
        stat_name_filter: &Regex,
        table_name_filter: &Regex,
        details_enable: &bool,
        gauges_enable: &bool
    ) {
        /*
         * These are the value and countsum statistics as they have been captured.
         */
        if *details_enable {
            // value_diff
            let value_statistics = ValueStatistics::create();
            for ((hostname, metric_type, metric_id, metric_name), value_diff_row) in &self.btreemap_snapshotdiff_values {
                if value_diff_row.second_snapshot_value > 0
                    && hostname_filter.is_match(hostname)
                    && stat_name_filter.is_match(metric_name)
                    && table_name_filter.is_match(&value_diff_row.table_name) {
                    let details = value_statistics.lookup(metric_name);
                    let adaptive_length = if metric_id.len() < 15 { 0 } else { metric_id.len() - 15 };
                    if details.stat_type != "gauge"
                        && value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value != 0 {
                        if *details_enable {
                            println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:6} {:>15.3} /s",
                                     hostname,
                                     metric_type,
                                     metric_id.substring(adaptive_length, metric_id.len()),
                                     value_diff_row.namespace,
                                     value_diff_row.table_name,
                                     metric_name,
                                     value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value,
                                     details.unit_suffix,
                                     ((value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value) as f64 / (value_diff_row.second_snapshot_time - value_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000_f64)
                            );
                        } else {
                            println!("{:20} {:8} {:70} {:15} {:6} {:>15.3} /s",
                                     hostname,
                                     metric_type,
                                     metric_name,
                                     value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value,
                                     details.unit_suffix,
                                     ((value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value) as f64 / (value_diff_row.second_snapshot_time - value_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000_f64)
                            );
                        }
                    }
                    if details.stat_type == "gauge"
                        && *gauges_enable {
                        if *details_enable {
                            println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:6} {:+15}",
                                     hostname,
                                     metric_type,
                                     metric_id.substring(adaptive_length, metric_id.len()),
                                     value_diff_row.namespace,
                                     value_diff_row.table_name,
                                     metric_name,
                                     value_diff_row.second_snapshot_value,
                                     details.unit_suffix,
                                     value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value
                            );
                        } else {
                            println!("{:20} {:8} {:70} {:15} {:6} {:+15}",
                                     hostname,
                                     metric_type,
                                     metric_name,
                                     value_diff_row.second_snapshot_value,
                                     details.unit_suffix,
                                     value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value
                            );
                        }
                    }
                }
            }
            // countsum_diff
            let countsum_statistics = CountSumStatistics::create();
            for ((hostname, metric_type, metric_id, metric_name), countsum_diff_row) in &self.btreemap_snapshotdiff_countsum {
                if countsum_diff_row.second_snapshot_total_count > 0
                    && hostname_filter.is_match(hostname)
                    && stat_name_filter.is_match(metric_name)
                    && table_name_filter.is_match(&countsum_diff_row.table_name) {
                    let details = countsum_statistics.lookup(metric_name);
                    let adaptive_length = if metric_id.len() < 15 { 0 } else { metric_id.len() - 15 };
                    if countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count != 0 {
                        if *details_enable {
                            println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15}        {:>15.3} /s avg: {:9.0} tot: {:>15.3} {:10}",
                                     hostname,
                                     metric_type,
                                     metric_id.substring(adaptive_length, metric_id.len()),
                                     countsum_diff_row.namespace,
                                     countsum_diff_row.table_name,
                                     metric_name,
                                     countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count,
                                     (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count) as f64 / (countsum_diff_row.second_snapshot_time - countsum_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000_f64,
                                     ((countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum) / (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count)) as f64,
                                     countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum,
                                     details.unit_suffix
                            );
                        } else {
                            println!("{:20} {:8} {:70} {:15}        {:>15.3} /s avg: {:9.0} tot: {:>15.3} {:10}",
                                     hostname,
                                     metric_type,
                                     metric_name,
                                     countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count,
                                     (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count) as f64 / (countsum_diff_row.second_snapshot_time - countsum_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000_f64,
                                     ((countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum) / (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count)) as f64,
                                     countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum,
                                     details.unit_suffix
                            );
                        }
                    }
                }
            }
        } else {
            /*
             * These are the value and countsum statistics summed.
             * The statistics for table, tablet and cdc types are per object.
             */
            // value_diff
            let value_statistics = ValueStatistics::create();
            let mut sum_value_diff: BTreeMap<(String, String, String, String), SnapshotDiffValues> = BTreeMap::new();
            for ((hostname_port, metric_type, _metric_id, metric_name), value_diff_row) in &self.btreemap_snapshotdiff_values {
                if metric_type == "table" || metric_type == "tablet" || metric_type == "cdc"{
                    /*
                     * If a table and thus its tablets have been deleted between the first and second snapshot, the second_snapshot_value is 0.
                     * However, the first_snapshot_value is > 0, it means it can make the subtraction between the second and the first snapshot get negative, and a summary overview be incorrect.
                     * Therefore we remove individual statistics where the second snapshot value is set to 0.
                     */
                    if value_diff_row.second_snapshot_value > 0 {
                        match sum_value_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string())) {
                            Some(sum_value_diff_row) => *sum_value_diff_row = SnapshotDiffValues::diff_sum_existing(sum_value_diff_row, value_diff_row),
                            None => {
                                sum_value_diff.insert(( hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string() ),
                                                      SnapshotDiffValues::diff_sum_new(value_diff_row)
                                );
                            },
                        }
                    }
                } else {
                    match sum_value_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string())) {
                        Some(_sum_value_diff) => {
                            panic!("Error: (sum_value_diff) found second entry for hostname: {}, type: {}, id: {}, name: {}", &hostname_port.clone(), &metric_type.clone(), String::from("-"), &metric_name.clone());
                        },
                        None => {
                            sum_value_diff.insert(( hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string() ),
                                                  SnapshotDiffValues::diff_sum_new(value_diff_row)
                            );
                        }
                    }
                }
            }
            //for ((hostname, metric_type, metric_id, metric_name), value_diff_row) in &self.btreemap_snapshotdiff_values {
            for ((hostname, metric_type, metric_id, metric_name), value_diff_row) in &sum_value_diff {
                if hostname_filter.is_match(hostname)
                    && stat_name_filter.is_match(metric_name)
                    && table_name_filter.is_match(&value_diff_row.table_name) {
                    let details = value_statistics.lookup(metric_name);
                    let adaptive_length = if metric_id.len() < 15 { 0 } else { metric_id.len() - 15 };
                    if details.stat_type != "gauge"
                        && value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value != 0 {
                        if *details_enable {
                            println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:6} {:>15.3} /s",
                                     hostname,
                                     metric_type,
                                     metric_id.substring(adaptive_length, metric_id.len()),
                                     value_diff_row.namespace,
                                     value_diff_row.table_name,
                                     metric_name,
                                     value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value,
                                     details.unit_suffix,
                                     ((value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value) as f64 / (value_diff_row.second_snapshot_time - value_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000_f64)
                            );
                        } else {
                            println!("{:20} {:8} {:70} {:15} {:6} {:>15.3} /s",
                                     hostname,
                                     metric_type,
                                     metric_name,
                                     value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value,
                                     details.unit_suffix,
                                     ((value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value) as f64 / (value_diff_row.second_snapshot_time - value_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000_f64)
                            );
                        }
                    }
                    if details.stat_type == "gauge"
                        && *gauges_enable {
                        if *details_enable {
                            println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:6} {:+15}",
                                     hostname,
                                     metric_type,
                                     metric_id.substring(adaptive_length, metric_id.len()),
                                     value_diff_row.namespace,
                                     value_diff_row.table_name,
                                     metric_name,
                                     value_diff_row.second_snapshot_value,
                                     details.unit_suffix,
                                     value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value
                            );
                        } else {
                            println!("{:20} {:8} {:70} {:15} {:6} {:+15}",
                                     hostname,
                                     metric_type,
                                     metric_name,
                                     value_diff_row.second_snapshot_value,
                                     details.unit_suffix,
                                     value_diff_row.second_snapshot_value - value_diff_row.first_snapshot_value
                            );
                        }
                    }
                }
            }
            // countsum_diff
            let countsum_statistics = CountSumStatistics::create();
            let mut sum_countsum_diff: BTreeMap<(String, String, String, String), SnapshotDiffCountSum> = BTreeMap::new();
            for ((hostname_port, metric_type, _metric_id, metric_name), countsum_diff_row) in &self.btreemap_snapshotdiff_countsum {
                if metric_type == "table" || metric_type == "tablet" || metric_type == "cdc" {
                    /*
                     * If a table and thus its tablets have been deleted between the first and second snapshot, the second_snapshot_value is 0.
                     * However, the first_snapshot_value is > 0, it means it can make the subtraction between the second and the first snapshot get negative, and a summary overview be incorrect.
                     * Therefore we remove individual statistics where the second snapshot value is set to 0.
                     */
                    if countsum_diff_row.second_snapshot_total_count > 0 {
                        match sum_countsum_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string())) {
                            Some(sum_countsum_diff_row) => *sum_countsum_diff_row = SnapshotDiffCountSum::diff_sum_existing(sum_countsum_diff_row, countsum_diff_row),
                            None => {
                                sum_countsum_diff.insert(( hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string() ),
                                                         SnapshotDiffCountSum::diff_sum_new(countsum_diff_row)
                                );
                            }
                        }
                    }
                } else {
                    match sum_countsum_diff.get_mut(&(hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string())) {
                        Some(_sum_countsum_diff_row) => {
                            panic!("Error: (sum_countsum_diff) found second entry for hostname: {}, type: {}, id: {}, name: {}", &hostname_port.clone(), &metric_type.clone(), String::from("-"), &metric_name.clone());
                        },
                        None => {
                            sum_countsum_diff.insert(( hostname_port.to_string(), metric_type.to_string(), String::from("-"), metric_name.to_string() ),
                                                     SnapshotDiffCountSum::diff_sum_new(countsum_diff_row)
                            );
                        }
                    }
                }
            }
            for ((hostname, metric_type, metric_id, metric_name), countsum_diff_row) in sum_countsum_diff {
                if hostname_filter.is_match(&hostname)
                    && stat_name_filter.is_match(&metric_name)
                    && table_name_filter.is_match(&countsum_diff_row.table_name) {
                    let details = countsum_statistics.lookup(&metric_name);
                    let adaptive_length = if metric_id.len() < 15 { 0 } else { metric_id.len() - 15 };
                    if countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count != 0 {
                        if *details_enable {
                            println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15}        {:>15.3} /s avg: {:9.0} tot: {:>15.3} {:10}",
                                     hostname,
                                     metric_type,
                                     metric_id.substring(adaptive_length, metric_id.len()),
                                     countsum_diff_row.namespace,
                                     countsum_diff_row.table_name,
                                     metric_name,
                                     countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count,
                                     (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count) as f64 / (countsum_diff_row.second_snapshot_time - countsum_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000_f64,
                                     ((countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum) / (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count)) as f64,
                                     countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum,
                                     details.unit_suffix
                            );
                        } else {
                            println!("{:20} {:8} {:70} {:15}        {:>15.3} /s avg: {:9.0} tot: {:>15.3} {:10}",
                                     hostname,
                                     metric_type,
                                     metric_name,
                                     countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count,
                                     (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count) as f64 / (countsum_diff_row.second_snapshot_time - countsum_diff_row.first_snapshot_time).num_milliseconds() as f64 * 1000_f64,
                                     ((countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum) / (countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count)) as f64,
                                     countsum_diff_row.second_snapshot_total_sum - countsum_diff_row.first_snapshot_total_sum,
                                     details.unit_suffix
                            );
                        }
                    }
                }
            }
        }
        // countsumrows_diff
        for ((hostname, _metric_type, _metric_id, metric_name), countsumrows_diff_row) in &self.btreemap_snapshotdiff_countsumrows {
            if hostname_filter.is_match(hostname)
                && stat_name_filter.is_match(metric_name)
                && countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count != 0 {
                println!("{:20} {:70} {:>15} avg: {:>15.3} tot: {:>15.3} ms, avg: {:>15} tot: {:>15} rows",
                         hostname,
                         metric_name,
                         countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count,
                         ((countsumrows_diff_row.second_snapshot_sum as f64 - countsumrows_diff_row.first_snapshot_sum as f64) / 1000.0) / (countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count) as f64,
                         (countsumrows_diff_row.second_snapshot_sum as f64 - countsumrows_diff_row.first_snapshot_sum as f64) / 1000.0,
                         (countsumrows_diff_row.second_snapshot_rows - countsumrows_diff_row.first_snapshot_rows) / (countsumrows_diff_row.second_snapshot_count - countsumrows_diff_row.first_snapshot_count),
                         countsumrows_diff_row.second_snapshot_rows - countsumrows_diff_row.first_snapshot_rows
                );
            }
        }
    }
}

pub fn read_metrics(
    host: &str,
    port: &str,
) -> Vec<MetricEntity> {
    if ! scan_port_addr(format!("{}:{}", host, port)) {
        warn!("hostname: port {}:{} cannot be reached, skipping", host, port);
        return parse_metrics(String::from(""), "", "")
    };
    let data_from_http = reqwest::blocking::get(format!("http://{}:{}/metrics", host, port))
        .unwrap_or_else(|e| {
            error!("Fatal: error reading from URL: {}", e);
            process::exit(1);
        })
        .text().unwrap();
    parse_metrics(data_from_http, host, port)
}

pub fn add_to_metric_vectors(
    data_parsed_from_json: Vec<MetricEntity>,
    hostname: &str,
    detail_snapshot_time: DateTime<Local>,
    allstoredmetrics: &mut AllStoredMetrics,
) {
    for metric in data_parsed_from_json {
        // This takes the option from attributes via as_ref(), and then the option of namespace_name/table_name via as_deref().
        let metric_attribute_namespace_name = metric.attributes.as_ref().map(|x| x.namespace_name.as_deref().unwrap_or("-")).unwrap_or("-").to_string();
        let metric_attribute_table_name = metric.attributes.as_ref().map(|x| x.table_name.as_deref().unwrap_or("-")).unwrap_or("-").to_string();
        trace!("metric_type: {}, metric_id: {}, metric_attribute_namespace_name: {}, metric_attribute_table_name: {}", &metric.metrics_type, &metric.id, metric_attribute_namespace_name, metric_attribute_table_name);
        for statistic in &metric.metrics {
            match statistic {
                Metrics::MetricValue { name, value } => {
                    /*
                     * Important! Any value that is 0 is never used.
                     * These values are skipped!
                     */
                    if *value > 0 {
                        //stored_values.push( StoredValues::new(hostname, detail_snapshot_time, &metric, &metric_attribute_namespace_name, &metric_attribute_table_name, name, *value));
                        allstoredmetrics.stored_values.push( StoredValues::new(hostname, detail_snapshot_time, &metric, &metric_attribute_namespace_name, &metric_attribute_table_name, name, *value));
                    }
                },
                Metrics::MetricCountSum { name, total_count, min, mean, percentile_75, percentile_95, percentile_99, percentile_99_9, percentile_99_99, max, total_sum } => {
                    /*
                     * Important! Any total_count that is 0 is never used.
                     * These values are skipped!
                     */
                    if *total_count > 0 {
                        //stored_countsum.push(StoredCountSum::new(hostname, detail_snapshot_time, &metric, &metric_attribute_namespace_name, &metric_attribute_table_name, name, *total_count, *min, *mean, *percentile_75, *percentile_95, *percentile_99, *percentile_99_9, *percentile_99_99, *max, *total_sum));
                        allstoredmetrics.stored_countsum.push(StoredCountSum::new(hostname, detail_snapshot_time, &metric, &metric_attribute_namespace_name, &metric_attribute_table_name, name, *total_count, *min, *mean, *percentile_75, *percentile_95, *percentile_99, *percentile_99_9, *percentile_99_99, *max, *total_sum));
                    }
                },
                Metrics::MetricCountSumRows { name, count, sum, rows} => {
                    /*
                     * Important! Any count that is 0 is never used.
                     * These values are skipped!
                     */
                    if *count > 0 {
                        //stored_countsumrows.push(StoredCountSumRows::new(hostname, detail_snapshot_time, &metric, &metric_attribute_namespace_name, &metric_attribute_table_name, name, *count, *sum, *rows));
                        allstoredmetrics.stored_countsumrows.push(StoredCountSumRows::new(hostname, detail_snapshot_time, &metric, &metric_attribute_namespace_name, &metric_attribute_table_name, name, *count, *sum, *rows));
                    }
                },
                // This is to to soak up invalid/rejected values.
                // See the explanation with the unit tests below.
                _ => {
                    warn!("statistic that is unknown or inconsistent: hostname_port: {}, type: {}, namespace: {}, table_name: {}: {:#?}", hostname, metric.metrics_type, metric_attribute_namespace_name, metric_attribute_table_name, statistic);
                },
            }
        }
    }
}

fn parse_metrics( metrics_data: String, host: &str, port: &str ) -> Vec<MetricEntity> {
    serde_json::from_str(&metrics_data )
        .unwrap_or_else(|e| {
            info!("({}:{}) error parsing /metrics json data for metrics, error: {}", host, port, e);
            Vec::<MetricEntity>::new()
        })
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cdc_metrics_value() {
        let json = r#"
[
    {
        "type": "cdc",
        "id": ":face4edb05934e77b564857878cf5015:4457a26b28a64393ac626504aba5f571",
        "attributes": {
            "stream_id": "face4edb05934e77b564857878cf5015",
            "table_name": "table0",
            "namespace_name": "test",
            "table_id": "c70ffbbe28f14e84b0559c405ae20197"
        },
        "metrics": [
            {
                "name": "async_replication_sent_lag_micros",
                "value": 0
            }
        ]
    }
]"#.to_string();
        let result = parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"cdc");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricValue { name, value} => format!("{}, {}",name, value),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "async_replication_sent_lag_micros, 0");
    }

    #[test]
    fn parse_cdc_metrics_countsum() {
        let json = r#"
[
    {
        "type": "cdc",
        "id": ":face4edb05934e77b564857878cf5015:4457a26b28a64393ac626504aba5f571",
        "attributes": {
            "stream_id": "face4edb05934e77b564857878cf5015",
            "table_name": "table0",
            "namespace_name": "test",
            "table_id": "c70ffbbe28f14e84b0559c405ae20197"
        },
        "metrics": [
            {
                "name": "rpc_payload_bytes_responded",
                "total_count": 3333,
                "min": 0,
                "mean": 0.0,
                "percentile_75": 0,
                "percentile_95": 0,
                "percentile_99": 0,
                "percentile_99_9": 0,
                "percentile_99_99": 0,
                "max": 0,
                "total_sum": 4444
            }
        ]
    }
]"#.to_string();
        let result = parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"cdc");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricCountSum { name, total_count, min: _, mean: _, percentile_75: _, percentile_95: _, percentile_99: _, percentile_99_9: _, percentile_99_99: _, max: _, total_sum} => format!("{}, {}, {}",name, total_count, total_sum),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "rpc_payload_bytes_responded, 3333, 4444");
    }

    #[test]
    fn parse_tablet_metrics_value() {
        let json = r#"[
    {
        "type": "tablet",
        "id": "16add7b1248a45d2880e5527b2059b54",
        "attributes": {
            "namespace_name": "yugabyte",
            "table_name": "config",
            "table_id": "000033e10000300080000000000042d9"
        },
        "metrics": [
            {
                "name": "rocksdb_sequence_number",
                "value": 1125899906842624
            }
        ]
    }
    ]"#.to_string();
        let result = parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"tablet");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricValue { name, value} => format!("{}, {}",name, value),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "rocksdb_sequence_number, 1125899906842624");
    }

    #[test]
    fn parse_tablet_metrics_countsum() {
        let json = r#"[
    {
        "type": "table",
        "id": "000033e10000300080000000000042ac",
        "attributes": {
            "namespace_name": "yugabyte",
            "table_name": "benchmark_table",
            "table_id": "000033e10000300080000000000042ac"
        },
        "metrics": [
            {
                "name": "log_sync_latency",
                "total_count": 21,
                "min": 0,
                "mean": 0.0,
                "percentile_75": 0,
                "percentile_95": 0,
                "percentile_99": 0,
                "percentile_99_9": 0,
                "percentile_99_99": 0,
                "max": 0,
                "total_sum": 22349
            }
        ]
    }
    ]"#.to_string();
        let result = parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"table");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricCountSum { name, total_count, min: _, mean: _, percentile_75: _, percentile_95: _, percentile_99: _, percentile_99_9: _, percentile_99_99: _, max: _, total_sum} => format!("{}, {}, {}",name, total_count, total_sum),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "log_sync_latency, 21, 22349");
    }

    #[test]
    fn parse_tablet_metrics_countsumrows() {
        let json = r#"[
    {
        "type": "server",
        "id": "yb.ysqlserver",
        "metrics": [
            {
                "name": "handler_latency_yb_ysqlserver_SQLProcessor_CatalogCacheMisses",
                "count": 439,
                "sum": 0,
                "rows": 439
            }
        ]
    }
    ]"#.to_string();
        let result = parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"server");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricCountSumRows { name, count, sum, rows} => format!("{}, {}, {}, {}",name, count, sum, rows),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "handler_latency_yb_ysqlserver_SQLProcessor_CatalogCacheMisses, 439, 0, 439");
    }

    #[test]
    fn parse_tablet_metrics_rejectedu64metricvalue() {
        // Funny, when I checked with version 2.11.2.0-b89 I could not find the value that only fitted in an unsigned 64 bit integer.
        // Still let's check for it.
        // The id is yb.cqlserver, because that is where I found this value.
        // The value 18446744073709551615 is too big for a signed 64 bit integer (limit = 2^63-1), this value is 2^64-1.
        let json = r#"
[
    {
        "type": "server",
        "id": "yb.cqlserver",
        "attributes": {},
        "metrics":
        [
            {
                "name": "madeup_value",
                "value": 18446744073709551615
            }
        ]
    }
]"#.to_string();
        let result = parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"server");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::RejectedU64MetricValue { name, value} => format!("{}, {}", name, value),
            _ => String::from("Not RejectedMetricValue")
        };
        assert_eq!(statistic_value, "madeup_value, 18446744073709551615");
    }

    #[test]
    fn parse_tablet_metrics_rejectedbooleanmetricvalue() {
        // Version 2.15.2.0-b83 a value appeared that is boolean instead of a number.
        // For now, I will just ditch the value, and request for it to be handled as all the other booleans, which are by the values of 0 and 1.
        let json = r#"
[
   {
        "type": "cluster",
        "id": "yb.cluster",
        "attributes": {},
        "metrics":
        [
            {
                "name": "is_load_balancing_enabled",
                "value": false
            }
        ]
   }
]"#.to_string();
        let result = parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"cluster");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::RejectedBooleanMetricValue { name, value } => format!("{}, {}", name, value),
            _ => String::from("Not RejectedMetricValue")
        };
        assert_eq!(statistic_value, "is_load_balancing_enabled, false");
    }

}
