use chrono::{DateTime, Local};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct Version {
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

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredVersion {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub git_hash: String,
    pub build_hostname: String,
    pub build_timestamp: String,
    pub build_username: String,
    pub build_clean_repo: String,
    pub build_id: String,
    pub build_type: String,
    pub version_number: String,
    pub build_number: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AllStoredVersions {
    pub stored_versions: Vec<StoredVersion>,
}

#[derive(Debug)]
pub struct SnapshotDiffStoredVersions {
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

type BTreeMapSnapshotDiffVersions = BTreeMap<String, SnapshotDiffStoredVersions>;

#[derive(Default)]
pub struct SnapshotDiffBTreeMapsVersions {
    pub btreemap_snapshotdiff_versions: BTreeMapSnapshotDiffVersions,
}


