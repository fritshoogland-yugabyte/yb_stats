//! The structs
//!
use std::collections::BTreeMap;
use chrono::{DateTime, Local};
/// The root struct for deserializing `/api/v1/varz`
///
/// ```text
/// {
///   "flags": [
///     {
///       "name": "log_filename",
///       "value": "yb-master",
///       "type": "NodeInfo"
///     },
///     {
///       "name": "placement_cloud",
///       "value": "local",
///       "type": "NodeInfo"
///     },
///     {
///       "name": "placement_region",
///       "value": "local",
///       "type": "NodeInfo"
///     },
///    ...
/// ```
/// The endpoint provides a list named 'flags', which does contain a list.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Vars {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the timestamp.
    pub timestamp: Option<DateTime<Local>>,
    pub flags: Vec<Var>,
}
/// The list contains the actual flags/vars details.
#[derive(Serialize, Deserialize, Debug)]
pub struct Var {
    pub name: String,
    pub value: String,
    #[serde(rename = "type")]
    pub vars_type: String,
}
/// Wrapper struct for holding the different vars structs.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AllVars {
    pub vars: Vec<Vars>,
}
// diff
/// BTreeMap for storing a vars diff struct per hostname_port, var.name
type BTreeVarsDiff = BTreeMap<(String, String), VarsDiffFields>;
/// Wrapper struct for holding the btreemap
#[derive(Debug, Default)]
pub struct VarsDiff {
    pub btreevarsdiff: BTreeVarsDiff,
}
/// The vars diff struct.
///
/// The hostname:port and variable is the key of the btreemap,
/// This struct holds the first and second value and type.
#[derive(Debug, Default)]
pub struct VarsDiffFields {
    pub first_value: String,
    pub first_vars_type: String,
    pub second_value: String,
    pub second_vars_type: String,
}
