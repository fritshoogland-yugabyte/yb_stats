use chrono::{DateTime, Local};
/// Struct to represent the snapshots metadata in yb_stats in a vector as well as on disk as CSV using serde.
/// The comment can be empty, unless a snapshot is made with the `--snapshot-comment` flag and a comment.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Snapshot {
    pub number: i32,
    pub timestamp: DateTime<Local>,
    pub comment: String,
}
