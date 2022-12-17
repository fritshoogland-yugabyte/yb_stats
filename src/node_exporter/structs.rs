use chrono::{DateTime, Local, Utc};
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct NodeExporterValues {
    pub node_exporter_name: String,
    pub node_exporter_type: String,
    pub node_exporter_labels: String,
    pub node_exporter_category: String,
    pub node_exporter_value: f64,
    pub node_exporter_timestamp: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredNodeExporterValues {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub node_exporter_name: String,
    pub node_exporter_type: String,
    pub node_exporter_labels: String,
    pub node_exporter_category: String,
    pub node_exporter_value: f64,
}

#[derive(Debug)]
pub struct SnapshotDiffNodeExporter {
    pub first_snapshot_time: DateTime<Local>,
    pub second_snapshot_time: DateTime<Local>,
    pub node_exporter_type: String,
    pub category: String,
    pub first_value: f64,
    pub second_value: f64,
}


type BTreeMapSnapshotDiffNodeExporter = BTreeMap<(String, String), SnapshotDiffNodeExporter>;

#[derive(Default)]
pub struct SnapshotDiffBTreeMapNodeExporter {
    pub btreemap_snapshotdiff_nodeexporter: BTreeMapSnapshotDiffNodeExporter,
}