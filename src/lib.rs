extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

use std::time::SystemTime;

#[derive(Serialize, Deserialize, Debug)]
pub struct Metrics {
    #[serde(rename = "type")]
    pub metrics_type: String,
    pub id: String,
    pub attributes: Attributes,
    pub metrics: Vec<NamedMetrics>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Attributes {
    pub namespace_name: Option<String>,
    pub table_name: Option<String>,
    pub table_id: Option<String>,
}
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum NamedMetrics {
    MetricValue {
        name: String,
        value: i64,
    },
    MetricLatency {
        name: String,
        total_count: i64,
        min: i64,
        mean: f64,
        percentile_75: i64,
        percentile_95: i64,
        percentile_99: i64,
        percentile_99_9: i64,
        percentile_99_99: i64,
        max: i64,
        total_sum: i64,
    }
}

#[derive(Debug)]
pub struct XValues {
    pub table_name: String,
    pub namespace: String,
    pub current_time: SystemTime,
    pub previous_time: SystemTime,
    pub current_value: i64,
    pub previous_value: i64,
}
#[derive(Debug)]
pub struct XLatencies {
    pub table_name: String,
    pub namespace: String,
    pub current_time: SystemTime,
    pub previous_time: SystemTime,
    pub current_total_count: i64,
    pub current_min: i64,
    pub current_mean: f64,
    pub current_percentile_75: i64,
    pub current_percentile_95: i64,
    pub current_percentile_99: i64,
    pub current_percentile_99_9: i64,
    pub current_percentile_99_99: i64,
    pub current_max: i64,
    pub current_total_sum: i64,
    pub previous_total_count: i64,
    pub previous_total_sum: i64,
}

#[derive(Debug)]
pub struct LatencyStatisticDetails {
    pub unit: String,
    pub unit_suffix: String,
    pub divisor: i64,
    pub stat_type: String,
}

#[derive(Debug)]
pub struct ValueStatisticDetails {
    pub unit: String,
    pub unit_suffix: String,
    pub stat_type: String,
}
