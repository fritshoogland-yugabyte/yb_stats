//! The module for JSON metrics from /metrics endpoints of master, tablet server, YCQL and YSQL.
//! (and YEDIS)
//!
//! These endpoints provide a separate metrics endpoint in the prometheus format (/prometheus-metrics) too.
//!
//! The functionality for metrics has 3 public entries:
//! 1. Snapshot creation: [AllStoredMetrics::perform_snapshot]
//! 2. Snapshot diff: [SnapshotDiffBTreeMapsMetrics::snapshot_diff]
//! 3. Ad-hoc mode (diff): [SnapshotDiffBTreeMapsMetrics::adhoc_read_first_snapshot], [SnapshotDiffBTreeMapsMetrics::adhoc_read_second_snapshot], [SnapshotDiffBTreeMapsMetrics::print].
//!
//! In general, the metric data is stored in the struct [AllStoredMetrics], which contains vectors of the three types of data.
//! When data is fetched to be shown to the user, it is fetched in [AllStoredMetrics] for both snapshots, and put in [SnapshotDiffBTreeMapsMetrics] which has specific vectors for the three types.
//! The essence of the vectors in [SnapshotDiffBTreeMapsMetrics] is that the statistics can be subtracted.
//!
//! # Snapshot creation
//! When a snapshot is created using the `--snapshot` option, this function is provided via the method [AllStoredMetrics::perform_snapshot].
//!
//! 1. The snapshot is called via the [crate::perform_snapshot] function, which calls all snapshot functions for all data sources.
//! 2. For metrics, this is via the [AllStoredMetrics::perform_snapshot] method. This method performs two calls.
//!
//!   * The method [AllStoredMetrics::read_metrics]
//!     * This method starts a rayon threadpool, and runs the general function [AllStoredMetrics::read_http] for all host and port combinations.
//!       * [AllStoredMetrics::read_http] calls [AllStoredMetrics::parse_metrics] to parse the http JSON output into a Vector of [MetricEntity].
//!     * The vector is iterated over and processed in using the function [AllStoredMetrics::split_into_vectors] into the struct [AllStoredMetrics] into the vectors [StoredValues], [StoredCountSum] and [StoredCountSumRows].
//!
//!   * The method [AllStoredMetrics::save_snapshot]
//!     * This method takes each of the vectors in the struct [AllStoredMetrics] and saves these to CSV files in the numbered snapshot directory.
//!
//! # Snapshot diff
//! When a snapshot diff is requested via the `--snapshot-diff` option, this function is provided via the method [SnapshotDiffBTreeMapsMetrics::snapshot_diff]
//!
//! 1. Snapshot-diff is called directly in main, which calls the method [SnapshotDiffBTreeMapsMetrics::snapshot_diff].
//!
//!   * The method [AllStoredMetrics::read_snapshot] is called to read all data for an [AllStoredMetrics] struct for the begin snapshot.
//!     * The method [SnapshotDiffBTreeMapsMetrics::first_snapshot] is called with [AllStoredMetrics] as argument to insert the first snapshot data.
//!   * The method [AllStoredMetrics::read_snapshot] is called to read all data for an [AllStoredMetrics] struct for the end snapshot.
//!     * The method [SnapshotDiffBTreeMapsMetrics::second_snapshot] is called with [AllStoredMetrics] as argument to insert the second snapshot data.
//!   * The method [SnapshotDiffBTreeMapsMetrics::print] is called to print out the the diff report from the [SnapshotDiffBTreeMapsMetrics] data.
//!
//! # Ad-hoc mode diff
//! When an ad-hoc diff is requested by not specifying any option, this is performed by three methods of [SnapshotDiffBTreeMapsMetrics].
//!
//! 1. [SnapshotDiffBTreeMapsMetrics::adhoc_read_first_snapshot]
//!
//!   * The method calls [AllStoredMetrics::read_metrics]
//!     * This method starts a rayon threadpool, and runs the general function [AllStoredMetrics::read_http] for all host and port combinations.
//!       * [AllStoredMetrics::read_http] calls [AllStoredMetrics::parse_metrics] to parse the http JSON output into a Vector of [MetricEntity].
//!     * The vector is iterated over and processed in using the function [AllStoredMetrics::split_into_vectors] into the struct [AllStoredMetrics] into the vectors [StoredValues], [StoredCountSum] and [StoredCountSumRows].
//!   * The method [SnapshotDiffBTreeMapsMetrics::first_snapshot] is called with [AllStoredMetrics] as argument to insert the first snapshot data.
//!
//! 2. The user is asked for enter via stdin().read_line()
//!
//! 3. [SnapshotDiffBTreeMapsMetrics::adhoc_read_second_snapshot]
//!
//!   * The method calls [AllStoredMetrics::read_metrics]
//!     * This method starts a rayon threadpool, and runs the general function [AllStoredMetrics::read_http] for all host and port combinations.
//!       * [AllStoredMetrics::read_http] calls [AllStoredMetrics::parse_metrics] to parse the http JSON output into a Vector of [MetricEntity].
//!     * The vector is iterated over and processed in using the function [AllStoredMetrics::split_into_vectors] into the struct [AllStoredMetrics] into the vectors [StoredValues], [StoredCountSum] and [StoredCountSumRows].
//!   * The method [SnapshotDiffBTreeMapsMetrics::second_snapshot] is called with [AllStoredMetrics] as argument to insert the second snapshot data.
//!
//! 4. [SnapshotDiffBTreeMapsMetrics::print]
//!
/// This imports extrnal crates
use std::{sync::mpsc::channel, collections::BTreeMap, time::Instant};
use chrono::{DateTime, Local};
//use serde_derive::{Serialize,Deserialize};
use regex::Regex;
use substring::Substring;
use log::*;
use anyhow::Result;
/// This imports two utility crates
//use crate::value_statistic_details;
//use crate::countsum_statistic_details;
use crate::{metrics, utility};
use crate::snapshot;
use crate::metrics::{StoredValues, Metrics, MetricEntity, AllStoredMetrics, StoredCountSum, StoredCountSumRows, SnapshotDiffValues, SnapshotDiffCountSum, SnapshotDiffCountSumRows, SnapshotDiffBTreeMapsMetrics};

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
           // Please mind the f64, the other metrics are u64
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

impl SnapshotDiffValues {
    /// This is a private function that uses the data from a [StoredValues] struct to create a [SnapshotDiffValues] struct for the first snapshot.
    /// The insertion of a first snapshot obviously fills out the first snapshot time and value.
    /// The second snapshot time is set to the first snapshot time, and the value to 0.
    /// In that way, when this statistic doesn't occur in the second snapshot, it can be detected by the value 0, and thus be omitted from output.
    /// The case of a value not occuring in a second snapshot could happen when a table is deleted between two snapshots.
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
    /// This is a private function that uses the data from a [StoredValues] struct to insert the data for the second snapshot into an existing [SnapshotDiffValues] struct, which thus already contains the first snapshot.
    /// The only fields that are modified are second_snapshot_time and second_snapshot_value.
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
    /// This is a private function that takes the values from a [StoredValues] struct and creates a [SnapshotDiffValues] struct for the second snapshot. There is no first snapshot value.
    /// In order to produce a [SnapshotDiffValues] struct with only a second snapshot, we set the first_snapshot_value field to 0, and use the provided first_snapshot_time as first_snapshot_time.
    ///
    /// This could happen if a tablet is created between snapshots.
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
    /// This is a private function for a special use of [SnapshotDiffValues], which happens in the [SnapshotDiffBTreeMapsMetrics::print] function.
    /// The special use is if the `--details-enable` flag is not set, statistics that are kept per table, tablet or cdc as metric_type are added together per server.
    /// This is the default mode, in order to try to reduce the amount of output.
    /// The way this works is that the existing, detailed, [BTreeMapSnapshotDiffValues] BTreeMap is iterated over, and for each key consisting of hostname_port, metric_type, metric_id and metric_name, the metric_id is set to "-".
    /// This new key is inserted into another BTreeMap together with the existing [SnapshotDiffValues] struct as value.
    /// If that key already exists in the other BTreeMap, this function is used.
    /// The existing [SnapshotDiffValues] struct values are largely kept identical, the only things which are changed is first_snapshot_value and second_snapshot_value, for which the values in the result of the iterator over the detailed one are added to the existing values.
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
    /// This is a private function for a special use of [SnapshotDiffValues], which happens in the [SnapshotDiffBTreeMapsMetrics::print] function.
    /// The special use is if the `--details-enable` flag is not set, statistics that are kept per table, tablet or cdc as metric_type are added together per server.
    /// This is the default mode, in order to try to reduce the amount of output.
    /// The way this works is that the existing, detailed, [BTreeMapSnapshotDiffValues] BTreeMap is iterated over, and for each key consisting of hostname_port, metric_type, metric_id and metric_name, the metric_id is set to "-".
    /// This new key is inserted into another BTreeMap together with the existing [SnapshotDiffValues] struct as value.
    /// If that key does not exist in the other BTreeMap, this function is used.
    /// The found [SnapshotDiffValues] struct is used to create a new one as value for the newly inserted key.
    /// Because the values are added for all objects, it doesn't make sense to keep the table_name or namespace; therefore these are set to "-".
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

impl SnapshotDiffCountSum {
    /// This is a private function that uses the data from a [StoredCountSum] struct to create a [SnapshotDiffCountSum] struct for the first snapshot.
    /// The insertion of a first snapshot obviously fills out the first snapshot time and first snapshot total_count and total_sum.
    /// The second snapshot time is set to the first snapshot time, and the total_count and total_sum to 0.
    /// In that way, when this statistic doesn't occur in the second snapshot, it can be detected by the value 0, and thus be omitted from output.
    /// The case of a statistic not occuring in a second snapshot could happen when a table is deleted between two snapshots.
    /// The values of min, mean, max and percentile 75, 95, 99_9 and 99_99 are set to 0.
    fn first_snapshot(storedcountsum: StoredCountSum) -> Self {
        Self {
            table_name: storedcountsum.attribute_table_name.to_string(),
            namespace: storedcountsum.attribute_namespace.to_string(),
            first_snapshot_time: storedcountsum.timestamp,
            second_snapshot_time: storedcountsum.timestamp,
            second_snapshot_total_count: 0,
            second_snapshot_min: 0,
            /// Please mind the f64, the other metrics are u64
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
    /// This is a private function that uses the data from a [StoredCountSum] struct to insert the data for the second snapshot into an existing [SnapshotDiffCountSum] struct, which thus already contains the first snapshot.
    /// The fields second_snapshot_time, second_snapshot_total_count and second_snapshot_total_sum are changed with the values from [StoredCountSum].
    /// Additionally, the min, mean, max, percentile_75, percentile_95, percentile_99, percentile_99_9 and percentile_99_99 are inserted.
    /// These additionally added fields are not used, these cannot be diffed in a meaningful way.
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
    /// This is a private function that uses the data from a [StoredCountSum] struct and creates a [SnapshotDiffCountSum] struct for the second snapshot. There are no first snapshot values.
    /// In order to produce a [SnapshotDiffCountSum] struct with only a second snapshot, we set the first snapshot total_count and total_sum fields to 0, and use the provided first_snapshot_time as first_snapshot_time.
    ///
    /// This could happen if a tablet is created between snapshots.
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
    /// This is a private function for a special use of [SnapshotDiffCountSum], which happens in the [SnapshotDiffBTreeMapsMetrics::print] function.
    /// The special use is if the `--details-enable` flag is not set, statistics that are kept per table, tablet or cdc as metric_type are added together per server.
    /// This is the default mode, in order to try to reduce the amount of output.
    /// The way this works is that the existing, detailed, [BTreeMapSnapshotDiffCountSum] BTreeMap is iterated over, and for each key consisting of hostname_port, metric_type, metric_id and metric_name, the metric_id is set to "-".
    /// This new key is inserted into another BTreeMap together with the existing [SnapshotDiffCountSum] struct as value.
    /// If that key already exists in the other BTreeMap, this function is used.
    /// The existing [SnapshotDiffCountSum] struct is kept identical, except for first_snapshot_total_count, first_snapshot_sum, second_snapshot_total_count and second_snapshot_total_sum for which the values in the result of the iterator are added to the existing values.
    fn diff_sum_existing(sum_countsum_diff_row: &mut SnapshotDiffCountSum, countsum_diff_row: &SnapshotDiffCountSum) -> Self
    {
        Self {
            table_name: sum_countsum_diff_row.table_name.to_string(),
            namespace: sum_countsum_diff_row.namespace.to_string(),
            first_snapshot_time: sum_countsum_diff_row.first_snapshot_time,
            second_snapshot_time: sum_countsum_diff_row.second_snapshot_time,
            second_snapshot_total_count: sum_countsum_diff_row.second_snapshot_total_count + countsum_diff_row.second_snapshot_total_count,
            second_snapshot_min: 0,
            /// Please mind the f64, the other metrics are u64
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
    /// This is a private function for a special use of [SnapshotDiffCountSum], which happens in the [SnapshotDiffBTreeMapsMetrics::print] function.
    /// The special use is if the `--details-enable` flag is not set, statistics that are kept per table, tablet or cdc as metric_type are added together per server.
    /// This is the default mode, in order to try to reduce the amount of output.
    /// The way this works is that the existing, detailed, [BTreeMapSnapshotDiffCountSum] BTreeMap is iterated over, and for each key consisting of hostname_port, metric_type, metric_id and metric_name, the metric_id is set to "-".
    /// This new key is inserted into another BTreeMap together with the existing [SnapshotDiffCountSum] struct as value.
    /// If that key does not exist in the other BTreeMap, this function is used.
    /// The found [SnapshotDiffCountSum] struct is used to create a new one as value for the newly inserted key.
    /// Because the values are added for all objects, it doesn't make sense to keep the table_name or namespace; therefore these are set to "-".
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

impl SnapshotDiffCountSumRows {
    /// This is a private function that uses the data from a [StoredCountSumRows] struct to create a [SnapshotDiffCountSumRows] struct for the first snapshot.
    /// The insertion of a first snapshot obviously fills out the first snapshot time and first snapshot count, sum and rows.
    /// The second snapshot time is set to the first snapshot time, and the count, sum and rows fields are set to 0.
    /// In that way, when this statistic doesn't occur in the second snapshot, it can be detected by the value 0, and thus be omitted from output.
    /// The YSQL CountSumRows statistics seem to be always present in the metrics endpoint, they are not dependent on something to happen or to be created.
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
    /// This is a private function that uses the data from a [StoredCountSumRows] struct to insert the data for the second snapshot into an existing [SnapshotDiffCountSumRows] struct, which thus already contains the first snapshot.
    /// The fields second_snapshot_time, second_snapshot_count, seconds_snapshot_sum and second_snapshot_rows are changed with the values from [StoredCountSumRows].
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
    /// This is a private function that uses the data from a [StoredCountSumRows] struct and creates a [SnapshotDiffCountSumRows] struct for the second snapshot. There are no first snapshot values.
    /// In order to produce a [SnapshotDiffCountSumRows] struct with only a second snapshot, we set the first snapshot count, sum and rows fields to 0, and use the provided first_snapshot_time as first_snapshot_time.
    /// For YSQL CountSumRows this currently cannot happen, each statistic is always present.
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


impl AllStoredMetrics {
    #[allow(rustdoc::private_intra_doc_links)]
    /// This function reads all the host/port combinations for metrics and saves these in a snapshot indicated by the snapshot_number.
    /// Reading the metrics from http endpoints is performed by [AllStoredMetrics::read_metrics], saving the data as CSV is done using [AllStoredMetrics::save_snapshot].
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allstoredmetrics = AllStoredMetrics::read_metrics(hosts, ports, parallel).await;
        snapshot::save_snapshot(snapshot_number, "values", allstoredmetrics.stored_values)?;
        snapshot::save_snapshot(snapshot_number, "countsum", allstoredmetrics.stored_countsum)?;
        snapshot::save_snapshot(snapshot_number, "countsumrows", allstoredmetrics.stored_countsumrows)?;

        info!("end snapshot: {:?}", timer.elapsed());
        Ok(())
    }
    pub fn new() -> Self{
        Default::default()
    }
    /// This function reads all the host/port combinations for metric endpoints and returns an [AllStoredMetrics] struct containing vectors of [StoredValues], [StoredCountSum] and [StoredCountSumRows].
    async fn read_metrics (
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllStoredMetrics
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
                        let metrics = AllStoredMetrics::read_http(host, port);
                        tx.send((format!("{}:{}", host, port), detail_snapshot_time, metrics)).expect("error sending data via tx (metrics)");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allstoredmetrics = AllStoredMetrics::new();
        for (hostname_port, detail_snapshot_time, metrics) in rx {
            allstoredmetrics.split_into_vectors(metrics, &hostname_port, detail_snapshot_time);
        }

        allstoredmetrics
    }
    /// This function takes the host and port &str values, and tries to read it, and parse the result.
    /// This function is public because the integration tests need access to it.
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> Vec<MetricEntity>
    {
        let data_from_http = if utility::scan_host_port( host, port) {
            utility::http_get(host, port, "metrics")
        } else {
            String::new()
        };
        AllStoredMetrics::parse_metrics(data_from_http, host, port)
    }
    /// This function takes the metrics data as String, and tries to parse the JSON in it to a vector [MetricEntity].
    fn parse_metrics(metrics_data: String, host: &str, port: &str) -> Vec<MetricEntity> {
        serde_json::from_str(&metrics_data)
            .unwrap_or_else(|e| {
                debug!("({}:{}) unable to parse, error: {}", host, port, e);
                Vec::<MetricEntity>::new()
            })
    }
    /// This function takes the [MetricEntity] struct, and splits the different types of values into [StoredValues], [StoredCountSum] and [StoredCountSumRows] vectors of structs.
    /// It is public because it is used in the integration tests too.
    pub fn split_into_vectors(
        &mut self,
        data_parsed_from_json: Vec<MetricEntity>,
        hostname: &str,
        detail_snapshot_time: DateTime<Local>,
    ) {
        // Thesee are the lookup tables for value statistics and countsum statistics
        let value_statistics = metrics::ValueStatistics::create();
        let countsum_statistics = metrics::CountSumStatistics::create();

        for metric in data_parsed_from_json {
            // This takes the option from attributes via as_ref(), and then the option of namespace_name/table_name via as_deref().
            let metric_attribute_namespace_name = metric.attributes.as_ref().map(|x| x.namespace_name.as_deref().unwrap_or("-")).unwrap_or("-").to_string();
            let metric_attribute_table_name = metric.attributes.as_ref().map(|x| x.table_name.as_deref().unwrap_or("-")).unwrap_or("-").to_string();
            trace!("metric_type: {}, metric_id: {}, metric_attribute_namespace_name: {}, metric_attribute_table_name: {}", &metric.metrics_type, &metric.id, metric_attribute_namespace_name, metric_attribute_table_name);
            for statistic in &metric.metrics {
                match statistic {
                    Metrics::MetricValue { name, value } => {
                        value_statistics.lookup(name);
                        /*
                         * Important! Any value that is 0 is never used.
                         * These values are skipped!
                         */
                        if *value > 0 {
                            self.stored_values.push( StoredValues::new(hostname, detail_snapshot_time, &metric, &metric_attribute_namespace_name, &metric_attribute_table_name, name, *value));
                        }
                    },
                    Metrics::MetricCountSum { name, total_count, min, mean, percentile_75, percentile_95, percentile_99, percentile_99_9, percentile_99_99, max, total_sum } => {
                        countsum_statistics.lookup(name);
                        /*
                         * Important! Any total_count that is 0 is never used.
                         * These values are skipped!
                         */
                        if *total_count > 0 {
                            self.stored_countsum.push(StoredCountSum::new(hostname, detail_snapshot_time, &metric, &metric_attribute_namespace_name, &metric_attribute_table_name, name, *total_count, *min, *mean, *percentile_75, *percentile_95, *percentile_99, *percentile_99_9, *percentile_99_99, *max, *total_sum));
                        }
                    },
                    Metrics::MetricCountSumRows { name, count, sum, rows} => {
                        /*
                         * Important! Any count that is 0 is never used.
                         * These values are skipped!
                         */
                        if *count > 0 {
                            self.stored_countsumrows.push(StoredCountSumRows::new(hostname, detail_snapshot_time, &metric, &metric_attribute_namespace_name, &metric_attribute_table_name, name, *count, *sum, *rows));
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
    /*
    /// This function takes the [AllStoredMetrics] struct vectors and saves these as CSV files.
    /// The vectors this struct holds are of structs of [StoredValues], [StoredCountSum] and [StoredCountSumRows].
    /// The directory with the snapshot number must exist already.
    /// This function returns a Result.
    ///
    fn save_snapshot ( self, snapshot_number: i32, ) -> Result<(), Box<dyn Error>>
    {
        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number.to_string());

        let values_file = &current_snapshot_directory.join("values");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(values_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_values {
            writer.serialize(row)?;
        }
        writer.flush()?;

        let countsum_file = &current_snapshot_directory.join("countsum");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(countsum_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_countsum {
            writer.serialize(row)?;
        }
        writer.flush()?;

        let countsumrows_file = &current_snapshot_directory.join("countsumrows");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(countsumrows_file)?;
        let mut writer = csv::Writer::from_writer(file);
        for row in self.stored_countsumrows {
            writer.serialize(row)?;
        }
        writer.flush()?;

        Ok(())
    }

     */
    /*
    /// This function takes a snapshot number, and reads the CSV data from a snapshut number directory into the vectors in [AllStoredMetrics].
    /// The vectors this struct holds are of structs of [StoredValues], [StoredCountSum] and [StoredCountSumRows].
    /// This function returns a Result that contains the struct [AllStoredMetrics] or an Error.
    fn read_snapshot( snapshot_number: &String, ) -> Result<AllStoredMetrics, Box<dyn Error>>
    {
        let mut allstoredmetrics = AllStoredMetrics::new();

        let current_directory = env::current_dir()?;
        let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(snapshot_number);

        let values_file = &current_snapshot_directory.join("values");
        let file = fs::File::open(values_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredValues = row?;
            allstoredmetrics.stored_values.push(data);
        };

        let countsum_file = &current_snapshot_directory.join("countsum");
        let file = fs::File::open(countsum_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredCountSum = row?;
            allstoredmetrics.stored_countsum.push(data);
        };

        let countsumrows_file = &current_snapshot_directory.join("countsumrows");
        let file = fs::File::open(countsumrows_file)?;

        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: StoredCountSumRows = row?;
            allstoredmetrics.stored_countsumrows.push(data);
        };

        Ok(allstoredmetrics)
    }

     */
}

impl SnapshotDiffBTreeMapsMetrics {
    /// This function takes a begin and end snapshot number and begin snapshot time, and returns the [SnapshotDiffBTreeMapsMetrics] struct.
    /// The function assumes the snapshot directories and the needed files in the snapshot directories exist, otherwise it exits yb_stats.
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
        begin_snapshot_timestamp: &DateTime<Local>,
    ) -> Result<SnapshotDiffBTreeMapsMetrics>
    {
        let mut allstoredmetrics = AllStoredMetrics::new();
        allstoredmetrics.stored_values = snapshot::read_snapshot(begin_snapshot, "values")?;
        allstoredmetrics.stored_countsum= snapshot::read_snapshot(begin_snapshot, "countsum")?;
        allstoredmetrics.stored_countsumrows = snapshot::read_snapshot(begin_snapshot, "countsumrows")?;

        let mut metrics_snapshot_diff = SnapshotDiffBTreeMapsMetrics::new();
        metrics_snapshot_diff.first_snapshot(allstoredmetrics);

        let mut allstoredmetrics = AllStoredMetrics::new();
        allstoredmetrics.stored_values = snapshot::read_snapshot(end_snapshot, "values")?;
        allstoredmetrics.stored_countsum= snapshot::read_snapshot(end_snapshot, "countsum")?;
        allstoredmetrics.stored_countsumrows = snapshot::read_snapshot(end_snapshot, "countsumrows")?;

        metrics_snapshot_diff.second_snapshot(allstoredmetrics, begin_snapshot_timestamp);

        Ok(metrics_snapshot_diff)
    }
    pub fn new() -> Self {
        Default::default()
    }
    /// This function reads the first snapshot data from the http endpoints itself (=adhoc mode), and stores it as first snapshot data in [SnapshotDiffBTreeMapsMetrics], and returns the struct.
    pub async fn adhoc_read_first_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) {
        let allstoredmetrics = AllStoredMetrics::read_metrics(hosts, ports, parallel).await;
        self.first_snapshot(allstoredmetrics);
    }
    /// This function reads the second snapshot data from the http endpoints itself (=adhoc mode), and stores it as second snapshot data in [SnapshotDiffBTreeMapsMetrics].
    /// Because not all metrics might have had a first snapshot entry, we need the first_snapshot_time to know when the first snapshot was taken.
    pub async fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
        first_snapshot_time: &DateTime<Local>,
    ) {
        let allstoredmetrics = AllStoredMetrics::read_metrics(hosts, ports, parallel).await;
        self.second_snapshot(allstoredmetrics, first_snapshot_time);
    }
    /// This function takes the data from the struct [AllStoredMetrics], creates a struct [SnapshotDiffBTreeMapsMetrics] and adds the data as first snapshot.
    /// The struct [AllStoredMetrics] contains vectors of the structs of [StoredValues], [StoredCountSum] and [StoredCountSumRows].
    /// This function is used in [SnapshotDiffBTreeMapsMetrics::snapshot_diff], [SnapshotDiffBTreeMapsMetrics::adhoc_read_first_snapshot], but never directly, which is why it is private.
    fn first_snapshot(
        &mut self,
        allstoredmetrics: AllStoredMetrics
    )
    {
        // values
        for row in allstoredmetrics.stored_values {
            if row.metric_type == "table" || row.metric_type == "tablet" || row.metric_type == "cdc" || row.metric_type == "cdcsdk" {
                match self.btreemap_snapshotdiff_values.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
                    // If we encounter a match, it means we found another row of the combination of hostname_port, metric_type (table, tablet or cdc), metric_id and metric_name, which is supposed to be unique.
                    // Therefore we tell the user the exact details and quit.
                    Some(_value_row) => {
                        panic!("Error: (SnapshotDiffBTreeMapsMetrics values) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port, &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                    },
                    // This inserts the key-value combination.
                    None => {
                        self.btreemap_snapshotdiff_values.insert(
                            (row.hostname_port.to_string(), row.metric_type.to_string(), row.metric_id.to_string(), row.metric_name.to_string()),
                            SnapshotDiffValues::first_snapshot(row)
                        );
                    },
                }
            } else {
                match self.btreemap_snapshotdiff_values.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone())) {
                    // If we encounter a match, it means we found another row of the combination of hostname_port, metric_type (server or cluster AFAIK), metric_id set to "-", because server and cluster metrics have a metric_id set to a static value of "yb.master", "yb.tabletserver", "yb.cqlserver", "yb.redisserver" and "yb.cluster", and metric_name.
                    // The combination of hostname_port, metric_type, metric_id == "-" and metric_name is supposed to be unique.
                    // Therefore if we have a match, we tell the user the exact details and quit.
                    Some(_value_row) => {
                        panic!("Error: (SnapshotDiffBTreeMapsMetrics values) found second entry for hostname: {}, type: {}, id: {} name: {}", &row.hostname_port, &row.metric_type.clone(), String::from("-"), &row.metric_name.clone());
                    },
                    // This inserts the key-value combination.
                    None => {
                        self.btreemap_snapshotdiff_values.insert(
                            (row.hostname_port.to_string(), row.metric_type.to_string(), String::from("-"), row.metric_name.to_string()),
                            SnapshotDiffValues::first_snapshot(row)
                        );
                    },
                }
            }
        }
        // countsum
        for row in allstoredmetrics.stored_countsum {
            if row.metric_type == "table" || row.metric_type == "tablet" || row.metric_type == "cdc" || row.metric_type == "cdcsdk" {
                match self.btreemap_snapshotdiff_countsum.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
                    // If we encounter a match, it means we found another row of the combination of hostname_port, metric_type (table, tablet or cdc), metric_id and metric_name, which is supposed to be unique.
                    // Therefore we tell the user the exact details and quit.
                    Some(_countsum_row) => {
                        panic!("Error: (SnapshotDiffBTreeMapsMetrics countsum) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port, &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                    },
                    // This inserts the key-value combination.
                    None => {
                        self.btreemap_snapshotdiff_countsum.insert(
                            (row.hostname_port.to_string(), row.metric_type.to_string(), row.metric_id.to_string(), row.metric_name.to_string()),
                            SnapshotDiffCountSum::first_snapshot(row)
                        );
                    },
                }
            } else {
                match self.btreemap_snapshotdiff_countsum.get_mut( &(row.hostname_port.clone(), row.metric_type.clone(), String::from("-"), row.metric_name.clone())) {
                    // If we encounter a match, it means we found another row of the combination of hostname_port, metric_type (server or cluster AFAIK), metric_id set to "-", because server and cluster metrics have a metric_id set to a static value of "yb.master", "yb.tabletserver", "yb.cqlserver", "yb.redisserver" and "yb.cluster", and metric_name.
                    // The combination of hostname_port, metric_type, metric_id == "-" and metric_name is supposed to be unique.
                    // Therefore if we have a match, we tell the user the exact details and quit.
                    Some(_countsum_row) => {
                        panic!("Error: (SnapshotDiffBTreeMapsMetrics countsum) found second entry for hostname: {}, type: {}, id: {} name: {}", &row.hostname_port, &row.metric_type.clone(), String::from("-"), &row.metric_name.clone());
                    },
                    // This inserts the key-value combination.
                    None => {
                        self.btreemap_snapshotdiff_countsum.insert(
                            (row.hostname_port.to_string(), row.metric_type.to_string(), String::from("-"), row.metric_name.to_string()),
                            SnapshotDiffCountSum::first_snapshot(row)
                        );
                    },
                }
            }
        }
        // countsumrows
        for row in allstoredmetrics.stored_countsumrows {
            // For countsumrows we currently have only one metric_type (server), and one metric_id (yb.ysqlserver), so there is no need to make a distinction between different types.
            match self.btreemap_snapshotdiff_countsumrows.get_mut(&(row.hostname_port.clone(), row.metric_type.clone(), row.metric_id.clone(), row.metric_name.clone())) {
                // If we found another combination of hostname_port, metric_type, metric_id and metric_name, something is wrong.
                // Therefore if we have a match, we tell the user the exact details and quit.
                Some(_countsumrows_row) => {
                    panic!("Error: (SnapshotDiffBTreeMapsMetrics countsumrows) found second entry for hostname: {}, type: {}, id: {}, name: {}", &row.hostname_port, &row.metric_type.clone(), &row.metric_id.clone(), &row.metric_name.clone());
                },
                // This inserts the key-value combination.
                None => {
                    self.btreemap_snapshotdiff_countsumrows.insert(
                        (row.hostname_port.to_string(), row.metric_type.to_string(), row.metric_id.to_string(), row.metric_name.to_string()),
                        SnapshotDiffCountSumRows::first_snapshot(row),
                    );
                },
            }
        }
    }
    /// This function takes the data from the struct [AllStoredMetrics], creates a struct [SnapshotDiffBTreeMapsMetrics] and adds the data as second snapshot.
    /// The struct [AllStoredMetrics] contains vectors of the structs of [StoredValues], [StoredCountSum] and [StoredCountSumRows].
    /// This function is used in [SnapshotDiffBTreeMapsMetrics::snapshot_diff] and [SnapshotDiffBTreeMapsMetrics::adhoc_read_second_snapshot], but never directly, which is why it is private.
    fn second_snapshot(
        &mut self,
        allstoredmetrics: AllStoredMetrics,
        first_snapshot_time: &DateTime<Local>,
    )
    {
        // values
        for row in allstoredmetrics.stored_values {
            if row.metric_type == "table" || row.metric_type == "tablet" || row.metric_type == "cdc" || row.metric_type == "cdcsdk" {
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
            if row.metric_type == "table" || row.metric_type == "tablet" || row.metric_type == "cdc" || row.metric_type == "cdcsdk" {
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
    /// This function prints the BTreeMaps in the [SnapshotDiffBTreeMapsMetrics] struct.
    /// It first is taking the details_enable boolean, which splits the printing between printing per table and tablet or summing it all up per server portnumber combination.
    /// Inside it, it first reads
    /// - [BTreeMapSnapshotDiffValues] for hostname_port, metric_type, metrid_id and metric_name as key and the struct [SnapshotDiffValues] as value, and then
    /// - [BTreeMapSnapshotDiffCountSum] for hostname_port, metric_type, metric_id and metric_name as key and the struct [SnapshotDiffCountSum] as value.
    /// If details are not enabled, it loops over them and for the types of "cdc", "table" and "tablet" adds up the values. // FIXME: cdcsdk
    /// It then prints out the values in the new summed BTreeMap if details are not enabled, or the original BTreeMap.
    /// The last thing is to print out the values in [BTreeMapSnapshotDiffCountSumRows].
    pub async fn print(
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
            let value_statistics = metrics::ValueStatistics::create();
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
            let countsum_statistics = metrics::CountSumStatistics::create();
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
            let value_statistics = metrics::ValueStatistics::create();
            let mut sum_value_diff: BTreeMap<(String, String, String, String), SnapshotDiffValues> = BTreeMap::new();
            for ((hostname_port, metric_type, _metric_id, metric_name), value_diff_row) in &self.btreemap_snapshotdiff_values {
                if metric_type == "table" || metric_type == "tablet" || metric_type == "cdc" || metric_type == "cdcsdk" {
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
            let countsum_statistics = metrics::CountSumStatistics::create();
            let mut sum_countsum_diff: BTreeMap<(String, String, String, String), SnapshotDiffCountSum> = BTreeMap::new();
            for ((hostname_port, metric_type, _metric_id, metric_name), countsum_diff_row) in &self.btreemap_snapshotdiff_countsum {
                if metric_type == "table" || metric_type == "tablet" || metric_type == "cdc" || metric_type == "cdcsdk" {
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
                    // this gives attemptp to subtract with overflow in debug mode.
                    //if countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count != 0 {
                    if countsum_diff_row.first_snapshot_total_count < countsum_diff_row.second_snapshot_total_count
                    && countsum_diff_row.second_snapshot_total_count - countsum_diff_row.first_snapshot_total_count != 0
                    {
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

#[cfg(test)]
mod tests {
    use super::*;
    //use crate::utility_test::*;

    #[test]
    /// cdcsdk (change data capture software development kit) metrics value
    /// Please mind type cdc has an extra, unique, attribute: stream_id. This is currently not parsed.
    fn unit_parse_metrics_cdcsdk_value() {
        let json = r#"
[
    {
        "type": "cdcsdk",
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
        let result = AllStoredMetrics::parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"cdcsdk");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricValue { name, value} => format!("{}, {}",name, value),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "async_replication_sent_lag_micros, 0");
    }

    #[test]
    /// cdc (change data capture) metrics value
    /// Please mind type cdc has an extra, unique, attribute: stream_id. This is currently not parsed.
    fn unit_parse_metrics_cdc_value() {
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
        let result = AllStoredMetrics::parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"cdc");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricValue { name, value} => format!("{}, {}",name, value),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "async_replication_sent_lag_micros, 0");
    }

    #[test]
    /// cdc (change data capture) metrics value
    /// Please mind type cdc has an extra, unique, attribute: stream_id. This is currently not parsed.
    fn unit_parse_metrics_cdc_countsum() {
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
        let result = AllStoredMetrics::parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"cdc");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricCountSum { name, total_count, min: _, mean: _, percentile_75: _, percentile_95: _, percentile_99: _, percentile_99_9: _, percentile_99_99: _, max: _, total_sum} => format!("{}, {}, {}",name, total_count, total_sum),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "rpc_payload_bytes_responded, 3333, 4444");
    }

    #[test]
    /// Parse tablet metrics value.
    /// It seems there are no other types of metrics in tablet metrics.
    fn unit_parse_metrics_tablet_value() {
        let json = r#"
[
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
        let result = AllStoredMetrics::parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"tablet");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricValue { name, value} => format!("{}, {}",name, value),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "rocksdb_sequence_number, 1125899906842624");
    }

    #[test]
    /// Parse table metrics countsum
    /// It seems tables can have both countsum as well as value metrics
    fn unit_parse_metrics_table_countsum() {
        let json = r#"
[
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
        let result = AllStoredMetrics::parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"table");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricCountSum { name, total_count, min: _, mean: _, percentile_75: _, percentile_95: _, percentile_99: _, percentile_99_9: _, percentile_99_99: _, max: _, total_sum} => format!("{}, {}, {}",name, total_count, total_sum),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "log_sync_latency, 21, 22349");
    }

    #[test]
    /// Parse table metrics countsum
    /// It seems tables can have both countsum as well as value metrics
    fn unit_parse_metrics_table_value() {
        let json = r#"
[
    {
        "type": "table",
        "id": "sys.catalog.uuid",
        "attributes": {
            "table_name": "sys.catalog",
            "namespace_name": "",
            "table_id": "sys.catalog.uuid"
        },
        "metrics": [
            {
                "name": "log_gc_running",
                "value": 0
            }
        ]
    }
]"#.to_string();
        let result = AllStoredMetrics::parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"table");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricValue { name, value} => format!("{}, {}",name, value),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "log_gc_running, 0");
    }

    #[test]
    /// Parse cluster metrics value
    fn unit_parse_metrics_cluster_value() {
        let json = r#"
[
        {
        "type": "cluster",
        "id": "yb.cluster",
        "attributes": {},
        "metrics": [
           {
                "name": "num_tablet_servers_live",
                "value": 0
            }
        ]
    }
]"#.to_string();
        let result = AllStoredMetrics::parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type, "cluster");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricValue { name, value } => format!("{}, {}", name, value),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "num_tablet_servers_live, 0");
    }

    #[test]
    /// Parse server metrics value
    fn unit_parse_metrics_server_value() {
        let json = r#"
[
    {
        "type": "server",
        "id": "yb.master",
        "attributes": {},
        "metrics": [
            {
                "name": "mem_tracker",
                "value": 529904
            }
        ]
    }
]"#.to_string();
        let result = AllStoredMetrics::parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type, "server");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricValue { name, value } => format!("{}, {}", name, value),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "mem_tracker, 529904");
    }

    #[test]
    /// Parse server metrics countsum
    fn unit_parse_metrics_server_countsum() {
        let json = r#"
[
    {
        "type": "server",
        "id": "yb.tabletserver",
        "attributes": {},
        "metrics": [
            {
                "name": "handler_latency_outbound_call_time_to_response",
                "total_count": 1384630,
                "min": 0,
                "mean": 676.4016688575184,
                "percentile_75": 2,
                "percentile_95": 2,
                "percentile_99": 2,
                "percentile_99_9": 2,
                "percentile_99_99": 2,
                "max": 25000,
                "total_sum": 1057260382
            }
        ]
    }
]"#.to_string();
        let result = AllStoredMetrics::parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"server");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricCountSum { name, total_count, min: _, mean: _, percentile_75: _, percentile_95: _, percentile_99: _, percentile_99_9: _, percentile_99_99: _, max: _, total_sum} => format!("{}, {}, {}",name, total_count, total_sum),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "handler_latency_outbound_call_time_to_response, 1384630, 1057260382");
    }

    #[test]
    /// Parse YSQL server countsumrows
    /// The YSQL metrics are unique metrics
    fn unit_parse_metrics_server_countsumrows() {
        let json = r#"
[
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
        let result = AllStoredMetrics::parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"server");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::MetricCountSumRows { name, count, sum, rows} => format!("{}, {}, {}, {}",name, count, sum, rows),
            _ => String::from("")
        };
        assert_eq!(statistic_value, "handler_latency_yb_ysqlserver_SQLProcessor_CatalogCacheMisses, 439, 0, 439");
    }

    #[test]
    fn unit_parse_metrics_server_rejectedu64metricvalue() {
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
        let result = AllStoredMetrics::parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"server");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::RejectedU64MetricValue { name, value} => format!("{}, {}", name, value),
            _ => String::from("Not RejectedMetricValue")
        };
        assert_eq!(statistic_value, "madeup_value, 18446744073709551615");
    }

    #[test]
    fn unit_parse_metrics_cluster_rejectedbooleanmetricvalue() {
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
        let result = AllStoredMetrics::parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"cluster");
        let statistic_value = match &result[0].metrics[0] {
            Metrics::RejectedBooleanMetricValue { name, value } => format!("{}, {}", name, value),
            _ => String::from("Not RejectedMetricValue")
        };
        assert_eq!(statistic_value, "is_load_balancing_enabled, false");
    }

    /*
    fn test_function_read_metrics(
        hostname: String,
        port: String
    ) -> AllStoredMetrics
    {
        let mut allstoredmetrics = AllStoredMetrics::new();

        let data_parsed_from_json = AllStoredMetrics::read_http(hostname.as_str(), port.as_str());
        allstoredmetrics.split_into_vectors(data_parsed_from_json, format!("{}:{}", hostname, port).as_str(), Local::now());
        allstoredmetrics
    }

     */
    #[tokio::test]
    async fn integration_parse_metrics_master()
    {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();
        let allstoredmetrics = AllStoredMetrics::read_metrics(&vec![&hostname], &vec![&port], 1).await;
        // a master will produce values and countsum rows, but no countsumrows rows, because that belongs to YSQL.
        assert!(!allstoredmetrics.stored_values.is_empty());
        assert!(!allstoredmetrics.stored_countsum.is_empty());
        assert!(allstoredmetrics.stored_countsumrows.is_empty());
    }
    #[tokio::test]
    async fn integration_parse_metrics_tserver() {
        let hostname = utility::get_hostname_tserver();
        let port = utility::get_port_tserver();
        let allstoredmetrics = AllStoredMetrics::read_metrics(&vec![&hostname], &vec![&port], 1).await;
        // a tablet server will produce values and countsum rows, but no countsumrows rows, because that belongs to YSQL.
        assert!(!allstoredmetrics.stored_values.is_empty());
        assert!(!allstoredmetrics.stored_countsum.is_empty());
        assert!(allstoredmetrics.stored_countsumrows.is_empty());
    }
    #[tokio::test]
    async fn integration_parse_metrics_ysql() {
        let hostname = utility::get_hostname_ysql();
        let port = utility::get_port_ysql();
        let allstoredmetrics = AllStoredMetrics::read_metrics(&vec![&hostname], &vec![&port], 1).await;
        // YSQL will produce countsumrows rows, but no value or countsum rows
        assert!(allstoredmetrics.stored_values.is_empty());
        assert!(allstoredmetrics.stored_countsum.is_empty());
        //assert!(!allstoredmetrics.stored_countsumrows.is_empty());
    }
    #[tokio::test]
    async fn integration_parse_metrics_ycql() {
        let hostname = utility::get_hostname_ycql();
        let port = utility::get_port_ycql();
        let allstoredmetrics = AllStoredMetrics::read_metrics(&vec![&hostname], &vec![&port], 1).await;
        // YCQL will produce values and countsum rows, but no countsumrows rows, because that belongs to YSQL.
        // countsum rows are filtered on count == 0, which is true if it wasn't used. therefore, we do not check on countsum statistics. likely, YCQL wasn't used prior to the test.
        assert!(!allstoredmetrics.stored_values.is_empty());
        //assert!(allstoredmetrics.stored_countsum.len() > 0);
        assert!(allstoredmetrics.stored_countsumrows.is_empty());
    }
    #[tokio::test]
    async fn integration_parse_metrics_yedis() {
        let hostname = utility::get_hostname_yedis();
        let port = utility::get_port_yedis();
        let allstoredmetrics = AllStoredMetrics::read_metrics(&vec![&hostname], &vec![&port], 1).await;
        // YEDIS will produce values and countsum rows, but no countsumrows rows, because that belongs to YSQL.
        // countsum rows are filtered on count == 0, which is true when it wasn't used. therefore, we do not check on countsum statistics. likely, YEDIS wasn't used prior to the test.
        assert!(!allstoredmetrics.stored_values.is_empty());
        assert!(allstoredmetrics.stored_countsumrows.is_empty());
    }
}
