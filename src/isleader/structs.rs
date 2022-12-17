use chrono::{DateTime, Local};
/// The struct that is used to parse the JSON returned from /api/v1/is-leader using serde.
///
/// Please mind that only the leader shows:
/// ```
/// {"STATUS":"OK"}
/// ```
/// The master followers do not return anything after being parsed.
#[derive(Serialize, Deserialize, Debug)]
pub struct IsLeader {
    /// The key for the status is in capitals, in this way it's renamed to 'status'.
    #[serde(rename = "STATUS")]
    pub status: String,
}
/// The struct that is used to store and retrieve the fetched and parsed data in CSV using serde.
///
/// The hostname_port and timestamp fields are filled out.
/// One of all the servers will have status field reading 'OK', indicating being the master leader.
#[derive(Serialize, Deserialize, Debug)]
pub struct StoredIsLeader {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub status: String,
}
/// This struct is used to handle the [StoredIsLeader] struct.
///
/// In this way, the struct can be using the impl functions.
#[derive(Debug, Default)]
pub struct AllStoredIsLeader {
    pub stored_isleader: Vec<StoredIsLeader>
}
