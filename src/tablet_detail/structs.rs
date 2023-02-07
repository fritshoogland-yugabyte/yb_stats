//! The structs
//!
use chrono::{DateTime, Local};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Tablet {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the snapshot timestamp.
    pub timestamp: Option<DateTime<Local>>,
    pub tabletbasic: Vec<TabletBasic>,
    pub tabletdetail: Vec<Option<TabletDetail>>,
}

#[derive(Debug, Default)]
pub struct AllTablets {
    pub tablet: Vec<Tablet>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TabletBasic {
    pub namespace: String,
    pub table_name: String,
    pub table_uuid: String,
    pub tablet_id: String,
    pub partition: String,
    pub state: String,
    pub hidden: String,
    pub num_sst_files: String,
    pub on_disk_size: String,
    pub raftconfig: String,
    pub last_status: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TabletDetail {
    pub tablet_id: String,
    pub columns: Vec<Option<Column>>,
    pub consensus_status: ConsensusStatus,
    //pub tabletloganchor: Vec<Option<TabletLogAnchor>>,
    //pub transactions: Vec<Option<Transactions>>,
    //pub rocksdb: Vec<Option<RocksDb>>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Column {
    pub column: String,
    pub id: String,
    pub column_type: String,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct ConsensusStatus {
    pub state: String,
    pub queue_overview: Option<String>,
    pub watermark: Vec<Option<Watermark>>,
    pub messages: Vec<Option<Message>>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Watermark {
    pub peer: String,
    pub watermark: String,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Message {
    pub entry: String,
    pub opid: String,
    pub message_type: String,
    pub size: String,
    pub status: String,
}
