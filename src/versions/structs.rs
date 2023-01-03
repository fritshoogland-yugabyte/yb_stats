//! The structs
//!
use chrono::{DateTime, Local};
use std::collections::BTreeMap;
/// The root struct for deserializing `/api/v1/version`.
///
/// This struct is a very simple json object,
/// with a number of properties.
///
/// ```text
/// {
///     "git_hash": "d4f01a5e26b168585e59f9c1a95766ffdd9655b1",
///     "build_hostname": "alma8-gcp-cloud-jenkins-worker-3ako4v",
///     "build_timestamp": "16 Nov 2022 00:21:52 UTC",
///     "build_username": "jenkins",
///     "build_clean_repo": true,
///     "build_id": "221",
///     "build_type": "RELEASE",
///     "version_number": "2.17.0.0",
///     "build_number": "24"
/// }
/// ```
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Version {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the timestamp.
    pub timestamp: Option<DateTime<Local>>,
    pub git_hash: String,
    pub build_hostname: String,
    pub build_timestamp: String,
    pub build_username: String,
    pub build_clean_repo: bool,
    pub build_id: String,
    pub build_type: String,
    pub version_number: String,
    pub build_number: String
}
/// Wrapper struct for holding the different version structs.
#[derive(Debug, Default)]
pub struct AllVersions {
    pub versions: Vec<Version>,
}
// diff
/// BTreeMap for storing the version diff struct per `hostname_port`
type BTreeVersionsDiff = BTreeMap<String, VersionsDiffFields>;
/// The wrapper struct for holding the btreemap holding the diff structs.
#[derive(Debug, Default)]
pub struct VersionsDiff {
    pub btreeversionsdiff: BTreeVersionsDiff,
}
/// The versions diff struct.
///
/// Every property above is listed as 'first' and 'second', to find the differences.
#[derive(Debug, Default)]
pub struct VersionsDiffFields {
    pub first_git_hash: String,
    pub first_build_hostname: String,
    pub first_build_timestamp: String,
    pub first_build_username: String,
    pub first_build_clean_repo: String,
    pub first_build_id: String,
    pub first_build_type: String,
    pub first_version_number: String,
    pub first_build_number: String,
    pub second_git_hash: String,
    pub second_build_hostname: String,
    pub second_build_timestamp: String,
    pub second_build_username: String,
    pub second_build_clean_repo: String,
    pub second_build_id: String,
    pub second_build_type: String,
    pub second_version_number: String,
    pub second_build_number: String,
}
