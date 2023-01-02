//! The structs
//!
use chrono::{DateTime, Local, Utc};
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct NodeExporter {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: String,
    /// yb_stats added to allow understanding the snapshot timestamp.
    pub timestamp: DateTime<Local>,
    pub name: String,
    pub exporter_type: String,
    pub labels: String,
    pub category: String,
    pub value: f64,
    pub exporter_timestamp: DateTime<Utc>,
}
#[derive(Debug, Default)]
pub struct AllNodeExporter {
    pub nodeexporter: Vec<NodeExporter>,
}

#[derive(Debug, Default)]
pub struct NameCategoryDiff {
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub exporter_type: String,
    pub category: String,
    pub first_value: f64,
    pub second_value: f64,
}

// (String, String, String) = (hostname_port, name, labels)
type BTreeNodeExporterDiff = BTreeMap<(String, String, String), NameCategoryDiff>;

#[derive(Default)]
pub struct NodeExporterDiff {
    pub btreemapnodeexporterdiff: BTreeNodeExporterDiff,
}