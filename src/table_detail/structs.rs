//! The structs
//!
use chrono::{DateTime, Local};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Table {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the snapshot timestamp.
    pub timestamp: Option<DateTime<Local>>,
    pub tablebasic: Vec<TableBasic>,
    pub tabledetail: Vec<Option<TableDetail>>,
}

#[derive(Debug, Default)]
pub struct AllTables {
    pub table: Vec<Table>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TableBasic {
    pub keyspace: String,
    pub table_name: String,
    pub state: String,
    pub message: String,
    pub uuid: String,
    pub ysql_oid: String,
    pub hidden: String,
    pub on_disk_size: String,
    pub object_type: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TableDetail {
    pub uuid: String,
    pub version: String,
    pub detail_type: String,
    pub state: String,
    pub replication_info: String,
    pub columns: Vec<Option<Column>>,
    pub tablets: Vec<Option<Tablet>>,
    pub tasks: Vec<Option<Task>>,

}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Column {
    pub column: String,
    pub id: String,
    pub column_type: String,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Tablet {
    pub id: String,
    pub partition: String,
    pub split_depth: String,
    pub state: String,
    pub hidden: String,
    pub message: String,
    pub raftconfig: String,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Task {
    pub task_name: String,
    pub state: String,
    pub start_time: String,
    pub duration: String,
    pub description: String,
}
