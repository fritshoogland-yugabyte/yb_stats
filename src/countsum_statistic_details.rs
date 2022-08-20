use std::collections::HashMap;
use log::*;

#[derive(Debug)]
pub struct CountSumStatisticDetails {
    pub unit: String,
    pub unit_suffix: String,
    pub divisor: i64,
    pub stat_type: String,
}

impl CountSumStatisticDetails {
    fn new(unit: &str, stat_type: &str) -> Self {
        let (suffix, divisor) = suffix_lookup_countsum(unit);
        Self {
            unit: unit.to_string(),
            unit_suffix: suffix,
            divisor,
            stat_type: stat_type.to_string(),
        }
    }
}

struct Suffixes {
    suffix: String,
    divisor: i64,
}

impl Suffixes {
    fn new(suffix: &str, divisor: i64) -> Self {
        Self {
            suffix: suffix.to_owned(),
            divisor,
        }
    }
}

fn suffix_lookup_countsum(unit: &str) -> (String, i64) {
    let suffix = HashMap::from([
        ("microseconds", Suffixes::new("us", 1000000)),
        ("operations", Suffixes::new("ops", 1)),
        ("bytes", Suffixes::new("bytes", 1)),
        ("files", Suffixes::new("files", 1)),
        ("tasks", Suffixes::new("tasks", 1)),
        ("requests", Suffixes::new("reqs", 1)),
    ]);
    match suffix.get(unit) {
        Some(x) => {
            (x.suffix.to_string(), x.divisor)
        },
        None => {
            info!("The suffix for {} does not exist, please add to suffix_lookup_countsum!", unit);
            ("?".to_string(), 1)
        },
    }
}

pub fn countsum_create_hashmap() -> HashMap<&'static str, CountSumStatisticDetails> {
    let mut countsum_statistic_details: HashMap<&str, CountSumStatisticDetails> = HashMap::new();

    countsum_statistic_details.insert("Create_Tablet_Attempt", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("Create_Tablet_Task", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("Delete_Tablet_Attempt", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("Delete_Tablet_Task", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("Flush_Tablets_Attempt", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("Flush_Tablets_Task", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("Hinted_Leader_Start_Election_Attempt", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("Hinted_Leader_Start_Election_Task", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("Stepdown_Leader_Attempt", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("Stepdown_Leader_Task", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("Truncate_Tablet_Attempt", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("Truncate_Tablet_Task", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("admin_triggered_compaction_pool_queue_time_us", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("admin_triggered_compaction_pool_run_time_us", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("dns_resolve_latency_during_init_proxy", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("dns_resolve_latency_during_sys_catalog_setup", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("dns_resolve_latency_during_update_raft_config", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_outbound_call_queue_time", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_outbound_call_send_time", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_outbound_call_time_to_response", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_outbound_transfer", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cdc_CDCService_BootstrapProducer", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cdc_CDCService_CreateCDCStream", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cdc_CDCService_DeleteCDCStream", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cdc_CDCService_GetChanges", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cdc_CDCService_GetCheckPoint", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cdc_CDCService_GetLatestEntryOpId", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cdc_CDCService_ListTablets", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cdc_CDCService_UpdateCdcReplicatedIndex", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_client_read_local", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_client_read_remote", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_client_time_to_send", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_client_write_local", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_client_write_remote", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_CQLServerService_Any", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_CQLServerService_ExecuteRequest", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_CQLServerService_GetProcessor", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_CQLServerService_ParseRequest", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_CQLServerService_ProcessRequest", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_CQLServerService_QueueResponse", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_consensus_ConsensusService_ChangeConfig", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_consensus_ConsensusService_GetConsensusState", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_consensus_ConsensusService_GetLastOpId", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_consensus_ConsensusService_GetNodeInstance", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_consensus_ConsensusService_LeaderElectionLost", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_consensus_ConsensusService_LeaderStepDown", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_consensus_ConsensusService_MultiRaftUpdateConsensus", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_consensus_ConsensusService_RequestConsensusVote", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_consensus_ConsensusService_RunLeaderElection", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_consensus_ConsensusService_StartRemoteBootstrap", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_consensus_ConsensusService_UpdateConsensus", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_SQLProcessor_AnalyzeRequest", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_SQLProcessor_ExecuteRequest", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_SQLProcessor_InsertStmt", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_SQLProcessor_NumFlushesToExecute", CountSumStatisticDetails::new("operations","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_SQLProcessor_NumRetriesToExecute", CountSumStatisticDetails::new("operations","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_SQLProcessor_NumRoundsToAnalyze", CountSumStatisticDetails::new("operations","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_SQLProcessor_OtherStmts", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_SQLProcessor_ParseRequest", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_SQLProcessor_ResponseSize", CountSumStatisticDetails::new("bytes","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_SQLProcessor_SelectStmt", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_cqlserver_SQLProcessor_UseStmt", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterClient_GetTableLocations", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterClient_GetTabletLocations", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterClient_GetTransactionStatusTablets", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterClient_ReservePgsqlOids", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterCluster_ListTabletServers", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterCluster_GetMasterRegistration", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterDdl_CreateTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterDdl_CreateNamespace", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterDdl_DeleteTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterDdl_GetNamespaceInfo", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterDdl_GetTableSchema", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterDdl_IsCreateTableDone", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterDdl_IsDeleteTableDone", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterDdl_IsTruncateTableDone", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterDdl_ListNamespaces", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterDdl_TruncateTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterEncryption_GetUniverseKeyRegistry", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterBackupService_CreateSnapshot", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterBackupService_CreateSnapshotSchedule", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterBackupService_DeleteSnapshot", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterBackupService_DeleteSnapshotSchedule", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterBackupService_ImportSnapshotMeta", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterBackupService_ListSnapshotRestorations", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterBackupService_ListSnapshotSchedules", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterBackupService_ListSnapshots", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterBackupService_RestoreSnapshot", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterHeartbeat_TSHeartbeat", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_AddUniverseKeys", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_AlterNamespace", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_AlterRole", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_AlterTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_AlterUniverseReplication", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_AreLeaderOnPreferredOnly", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_BackfillIndex", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_ChangeEncryptionInfo", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_ChangeLoadBalancerState", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_ChangeMasterClusterConfig", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_CreateCDCStream", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_CreateNamespace", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_CreateRole", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_CreateTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_CreateTablegroup", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_CreateTransactionStatusTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_CreateUDType", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_DdlLog", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_DeleteCDCStream", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_DeleteNamespace", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_DeleteNotServingTablet", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_DeleteRole", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_DeleteTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_DeleteTablegroup", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_DeleteUniverseReplication", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_DumpState", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_FlushTables", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetBackfillJobs", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetCDCStream", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetColocatedTabletSchema", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetLeaderBlacklistCompletion", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetLoadBalancerState", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetLoadMoveCompletion", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetMasterClusterConfig", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetMasterRegistration", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetNamespaceInfo", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetPermissions", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetTableLocations", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetTableSchema", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetTabletLocations", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetUDType", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetUniveerserReplication", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetUniverseKeyRegistration", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetYsqlCatalogConfig", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GetYsqlCatalogConfig", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GrantRevokePermission", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_GrantRevokeRole", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_HasUniverseKeyInMemory", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_IsAlterTableDone", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_IsCreateNamespaceDone", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_IsCreateTableDone", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_IsDeleteNamespaceDone", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_IsDeleteTableDone", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_IsEncryptionEnabled", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_IsFlushTablesDone", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_IsInitDbDone", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_IsLoadBalanced", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_IsLoadBalancerIdle", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_IsMasterLeaderServiceReady", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_IsSetupUniverseReplicationDone", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_IsTruncateTableDone", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_LaunchBackfillIndexForTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_ListCDCStreams", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_ListLiveTabletServers", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_ListMasterRaftPeers", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_ListMasters", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_ListNamespaces", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_ListTablegroups", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_ListTables", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_ListTabletServers", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_ListUDType", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_RedisConfigGet", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_RedisConfigSet", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_RemoveMasterUpdate", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_ReservePgsqlOids", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_SetPreferredZones", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_SetUniverseReplicationEnabled", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_SetupUniverseReplication", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_SplitTablet", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_TSHeartbeat", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_TruncateTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_master_MasterService_UpdateCDCStream", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_server_GenericService_FlushCoverage", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_server_GenericService_GetFlag", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_server_GenericService_GetStatus", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_server_GenericService_Ping", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_server_GenericService_RefreshFlags", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_server_GenericService_ServerClock", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_server_GenericService_SetFlag", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_AlterDatabase", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_AlterTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_BackfillIndex", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_CreateDatabase", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_CreateSequencesDataTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_CreateTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_CreateTablegroup", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_DropDatabase", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_DropTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_DropTablegroup", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_FinishTransaction", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_GetCatalogMasterVersion", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_GetDatabaseInfo", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_Heartbeat", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_IsInitDbDone", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_ListLiveTabletServers", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_OpenTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_Perform", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_ReserveOids", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_TabletServerCount", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_PgClientService_TruncateTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_RemoteBootstrapService_BeginRemoteBootstrapSession", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_RemoteBootstrapService_CheckSessionActive", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_RemoteBootstrapService_EndRemoteBootstrapSession", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_RemoteBootstrapService_FetchData", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_RemoteBootstrapService_RemoveSession", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerAdminService_AddTableToTablet", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerAdminService_AlterSchema", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerAdminService_BackfillDone", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerAdminService_BackfillIndex", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerAdminService_CopartitionTable", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerAdminService_CountIntents", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerAdminService_CreateTablet", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerAdminService_DeleteTablet", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerAdminService_FlushTablets", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerAdminService_GetSafeTime", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerAdminService_RemoveTableFromTablet", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerAdminService_SplitTablet", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerAdminService_TabletSnapshotOp", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerAdminService_UpgradeYsql", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerForwardService_Read", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerForwardService_Write", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_AbortTransaction", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_Checksum", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_GetLogLocation", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_GetMasterAddresses", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_GetSharedData", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_GetSplitKey", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_GetTabletStatus", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_GetTransactionStatus", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_GetTransactionStatusAtParticipant", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_ImportData", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_IsTabletServerReady", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_ListTablets", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_ListTabletsForTabletServer", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_NoOp", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_Publish", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_Read", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_TakeTransaction", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_Truncate", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_UpdateTransaction", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_VerifyTableRowRange", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("handler_latency_yb_tserver_TabletServerService_Write", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("log_append_latency", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("log_bytes_logged", CountSumStatisticDetails::new("bytes","counter"));
    countsum_statistic_details.insert("log_entry_batches_per_group", CountSumStatisticDetails::new("requests","counter"));
    countsum_statistic_details.insert("log_gc_duration", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("log_group_commit_latency", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("log_reader_read_batch_latency", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("log_roll_latency", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("log_sync_latency", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("log_wal_size", CountSumStatisticDetails::new("bytes","counter"));
    countsum_statistic_details.insert("op_apply_queue_length", CountSumStatisticDetails::new("tasks","counter"));
    countsum_statistic_details.insert("op_apply_queue_time", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("op_apply_run_time", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("op_read_queue_length", CountSumStatisticDetails::new("tasks","counter"));
    countsum_statistic_details.insert("op_read_queue_run_time", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("op_read_queue_time", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("post_split_trigger_compaction_pool_queue_time_us", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("post_split_trigger_compaction_pool_run_time_us", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ql_read_latency", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("redis_read_latency", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("rocksdb_bytes_per_multiget", CountSumStatisticDetails::new("bytes","counter"));
    countsum_statistic_details.insert("rocksdb_bytes_per_read", CountSumStatisticDetails::new("bytes","counter"));
    countsum_statistic_details.insert("rocksdb_bytes_per_write", CountSumStatisticDetails::new("bytes","counter"));
    countsum_statistic_details.insert("rocksdb_compaction_times_micros", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("rocksdb_db_get_micros", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("rocksdb_db_multiget_micros", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("rocksdb_db_seek_micros", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("rocksdb_db_write_micros", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("rocksdb_numfiles_in_singlecompaction", CountSumStatisticDetails::new("files","counter"));
    countsum_statistic_details.insert("rocksdb_read_block_compaction_micros", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("rocksdb_read_block_get_micros", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("rocksdb_sst_read_micros", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("rocksdb_wal_file_sync_micros", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("rocksdb_write_raw_block_micros", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("rpc_incoming_queue_time", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("snapshot_read_inflight_wait_duration", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("transaction_pool_cache", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ts_bootstrap_time", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("write_lock_latency", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("write_op_duration_client_propagated_consistency", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_auth_resource_role_permission_index", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_auth_role_permissions", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_auth_roles", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_local", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_partitions", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_peers", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_schema_aggregates", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_schema_columns", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_schema_functions", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_schema_indexes", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_schema_keyspaces", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_schema_tables", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_schema_triggers", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_schema_types", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_schema_views", CountSumStatisticDetails::new("microseconds","counter"));
    countsum_statistic_details.insert("ycsql_queries_system_size_estimates", CountSumStatisticDetails::new("microseconds","counter"));

    countsum_statistic_details
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_countsum_statistic_details_simple_value() {
        let countsum_statistic_details_lookup = countsum_create_hashmap();
        let lookup = countsum_statistic_details_lookup.get("Create_Tablet_Attempt").unwrap();
        assert_eq!(lookup.unit_suffix, "us");
    }
    #[test]
    fn lookup_nonexistent_countsum_statistic_details_simple_value() {
        let countsum_statistic_details_lookup = countsum_create_hashmap();
        let non_existent = CountSumStatisticDetails { unit: String::from("?"), unit_suffix: String::from("?"), divisor: 1, stat_type: String::from("?") };
        let lookup = countsum_statistic_details_lookup.get("doesnotexist").unwrap_or(&non_existent);
        assert_eq!(lookup.unit_suffix, "?");
    }

}
