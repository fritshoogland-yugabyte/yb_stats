//! The structs
//!
use chrono::{DateTime, Local};
/// The struct that is used to parse the JSON returned from /api/v1/is-leader using serde.
///
/// Please mind that only the leader shows:
/// ```
/// {"STATUS":"OK"}
/// ```
/// The master followers do not return anything after being parsed.
///
/// // 
// hostname_port + timestamp: added
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct IsLeader {
    pub hostname_port: Option<String>,
    pub timestamp: Option<DateTime<Local>>,
    /// The key for the status is in capitals, in this way it's renamed to 'status'.
    #[serde(rename = "STATUS")]
    pub status: String,
}
/// This struct is a wrapper for the [IsLeader] struct.
///
/// In this way, the struct can be used with function in impl.
#[derive(Debug, Default)]
pub struct AllIsLeader {
    pub isleader: Vec<IsLeader>
}
