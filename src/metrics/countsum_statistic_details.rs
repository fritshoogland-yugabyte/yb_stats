//! Utility module for metrics of the type CountSum, with helper functions.
use std::collections::HashMap;
use log::*;
/// The struct that contains all the details for a named statistic.
/// This struct is used in [CountSumStatistics.countsumstatisticdetails], which holds a HashMap with the statistic name as key and this struct as value.
#[derive(Debug)]
pub struct CountSumStatisticDetails {
    pub unit: String,
    pub unit_suffix: String,
    pub divisor: i64,
    pub stat_type: String,
}
/// This struct is the main struct that provides the functionality for CountSum statistics.
pub struct CountSumStatistics {
    pub countsumstatisticsdetails: HashMap<String, CountSumStatisticDetails>
}

impl CountSumStatistics {
    /// Take a statistic name, and return the details about it.
    /// If it doesn't exist, it returns '?', and generates logging at the info level.
    pub fn lookup(&self, argument: &str) -> &CountSumStatisticDetails {
        match self.countsumstatisticsdetails.get(&argument.to_string()) {
            Some(lookup) => lookup,
            None => {
                info!("statistic not found! -> table.insert(\"{}\",\"?\",\"?\");", argument);
                Self::lookup(self, "?")
            },
        }
    }
    /// Create a struct holding a HashMap with all the known statistics and the specifics.
    pub fn create() -> CountSumStatistics {
        let mut table = CountSumStatistics { countsumstatisticsdetails: HashMap::new() };
        // special row for unknown values. Do NOT remove!
        table.insert("?", "?", "?");
        table.insert("AddServer_ChangeConfig_Attempt", "microseconds","counter"); // 2.15.2
        table.insert("AddServer_ChangeConfig_Task", "microseconds","counter"); // 2.15.2
        table.insert("Create_Tablet_Attempt", "microseconds","counter");
        table.insert("Create_Tablet_Task", "microseconds","counter");
        table.insert("Delete_Tablet_Attempt", "microseconds","counter");
        table.insert("Delete_Tablet_Task", "microseconds","counter");
        table.insert("Flush_Tablets_Attempt", "microseconds","counter");
        table.insert("Flush_Tablets_Task", "microseconds","counter");
        table.insert("Hinted_Leader_Start_Election_Attempt", "microseconds","counter");
        table.insert("Hinted_Leader_Start_Election_Task", "microseconds","counter");
        table.insert("Stepdown_Leader_Attempt", "microseconds","counter");
        table.insert("Stepdown_Leader_Task", "microseconds","counter");
        table.insert("Truncate_Tablet_Attempt", "microseconds","counter");
        table.insert("Truncate_Tablet_Task", "microseconds","counter");
        table.insert("admin_triggered_compaction_pool_queue_time_us", "microseconds","counter");
        table.insert("admin_triggered_compaction_pool_run_time_us", "microseconds","counter");
        table.insert("deadlock_probe_latency","milliseconds","counter"); // 2.17.2
        table.insert("deadlock_size","transactions","counter"); // 2.17.2
        table.insert("dns_resolve_latency_during_init_proxy", "microseconds","counter");
        table.insert("dns_resolve_latency_during_sys_catalog_setup", "microseconds","counter");
        table.insert("dns_resolve_latency_during_update_raft_config", "microseconds","counter");
        table.insert("full_compaction_pool_queue_time_us","microseconds","counter"); // 2.17.2
        table.insert("full_compaction_pool_run_time_us","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_outbound_call_queue_time", "microseconds","counter");
        table.insert("handler_latency_outbound_call_send_time", "microseconds","counter");
        table.insert("handler_latency_outbound_call_time_to_response", "microseconds","counter");
        table.insert("handler_latency_outbound_transfer", "microseconds","counter");
        table.insert("handler_latency_yb_cdc_CDCService_BootstrapProducer", "microseconds","counter");
        table.insert("handler_latency_yb_cdc_CDCService_CheckReplicationDrain", "microseconds","counter");
        table.insert("handler_latency_yb_cdc_CDCService_CreateCDCStream", "microseconds","counter");
        table.insert("handler_latency_yb_cdc_CDCService_DeleteCDCStream", "microseconds","counter");
        table.insert("handler_latency_yb_cdc_CDCService_GetCDCDBStreamInfo", "microseconds","counter");
        table.insert("handler_latency_yb_cdc_CDCService_GetChanges", "microseconds","counter");
        table.insert("handler_latency_yb_cdc_CDCService_GetCheckpoint", "microseconds","counter");
        table.insert("handler_latency_yb_cdc_CDCService_GetLastOpId", "microseconds","counter");
        table.insert("handler_latency_yb_cdc_CDCService_GetLatestEntryOpId", "microseconds","counter");
        table.insert("handler_latency_yb_cdc_CDCService_GetTabletListToPollForCDC","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_cdc_CDCService_IsBootstrapRequired", "microseconds","counter");
        table.insert("handler_latency_yb_cdc_CDCService_ListTablets", "microseconds","counter");
        table.insert("handler_latency_yb_cdc_CDCService_SetCDCCheckpoint", "microseconds","counter");
        table.insert("handler_latency_yb_cdc_CDCService_UpdateCdcReplicatedIndex", "microseconds","counter");
        table.insert("handler_latency_yb_client_read_local", "microseconds","counter");
        table.insert("handler_latency_yb_client_read_remote", "microseconds","counter");
        table.insert("handler_latency_yb_client_time_to_send", "microseconds","counter");
        table.insert("handler_latency_yb_client_write_local", "microseconds","counter");
        table.insert("handler_latency_yb_client_write_remote", "microseconds","counter");
        table.insert("handler_latency_yb_consensus_ConsensusService_ChangeConfig", "microseconds","counter");
        table.insert("handler_latency_yb_consensus_ConsensusService_GetConsensusState", "microseconds","counter");
        table.insert("handler_latency_yb_consensus_ConsensusService_GetLastOpId", "microseconds","counter");
        table.insert("handler_latency_yb_consensus_ConsensusService_GetNodeInstance", "microseconds","counter");
        table.insert("handler_latency_yb_consensus_ConsensusService_LeaderElectionLost", "microseconds","counter");
        table.insert("handler_latency_yb_consensus_ConsensusService_LeaderStepDown", "microseconds","counter");
        table.insert("handler_latency_yb_consensus_ConsensusService_MultiRaftUpdateConsensus", "microseconds","counter");
        table.insert("handler_latency_yb_consensus_ConsensusService_RequestConsensusVote", "microseconds","counter");
        table.insert("handler_latency_yb_consensus_ConsensusService_RunLeaderElection", "microseconds","counter");
        table.insert("handler_latency_yb_consensus_ConsensusService_StartRemoteBootstrap", "microseconds","counter");
        table.insert("handler_latency_yb_consensus_ConsensusService_UnregisterLogAnchor", "microseconds","counter");
        table.insert("handler_latency_yb_consensus_ConsensusService_UnsafeChangeConfig", "microseconds","counter");
        table.insert("handler_latency_yb_consensus_ConsensusService_UpdateConsensus", "microseconds","counter");
        table.insert("handler_latency_yb_cqlserver_CQLServerService_Any", "microseconds","counter");
        table.insert("handler_latency_yb_cqlserver_CQLServerService_ExecuteRequest", "microseconds","counter");
        table.insert("handler_latency_yb_cqlserver_CQLServerService_GetProcessor", "microseconds","counter");
        table.insert("handler_latency_yb_cqlserver_CQLServerService_ParseRequest", "microseconds","counter");
        table.insert("handler_latency_yb_cqlserver_CQLServerService_ProcessRequest", "microseconds","counter");
        table.insert("handler_latency_yb_cqlserver_CQLServerService_QueueResponse", "microseconds","counter");
        table.insert("handler_latency_yb_cqlserver_SQLProcessor_AnalyzeRequest", "microseconds","counter");
        table.insert("handler_latency_yb_cqlserver_SQLProcessor_DeleteStmt","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_cqlserver_SQLProcessor_ExecuteRequest", "microseconds","counter");
        table.insert("handler_latency_yb_cqlserver_SQLProcessor_InsertStmt", "microseconds","counter");
        table.insert("handler_latency_yb_cqlserver_SQLProcessor_NumFlushesToExecute", "operations","counter");
        table.insert("handler_latency_yb_cqlserver_SQLProcessor_NumRetriesToExecute", "operations","counter");
        table.insert("handler_latency_yb_cqlserver_SQLProcessor_NumRoundsToAnalyze", "operations","counter");
        table.insert("handler_latency_yb_cqlserver_SQLProcessor_OtherStmts", "microseconds","counter");
        table.insert("handler_latency_yb_cqlserver_SQLProcessor_ParseRequest", "microseconds","counter");
        table.insert("handler_latency_yb_cqlserver_SQLProcessor_ResponseSize", "bytes","counter");
        table.insert("handler_latency_yb_cqlserver_SQLProcessor_SelectStmt", "microseconds","counter");
        table.insert("handler_latency_yb_cqlserver_SQLProcessor_Transaction","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_cqlserver_SQLProcessor_UpdateStmt","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_cqlserver_SQLProcessor_UseStmt", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterAdmin_AddTransactionStatusTablet","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterAdmin_CheckIfPitrActive","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterAdmin_CompactSysCatalog","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterAdmin_CreateTransactionStatusTable","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterAdmin_DdlLog","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterAdmin_DeleteNotServingTablet","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterAdmin_DisableTabletSplitting","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterAdmin_FlushSysCatalog","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterAdmin_FlushTables","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterAdmin_IsFlushTablesDone","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterAdmin_IsInitDbDone","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterAdmin_IsTabletSplittingComplete","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterAdmin_SplitTablet","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterBackupService_CreateSnapshot", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterBackupService_CreateSnapshotSchedule", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterBackupService_DeleteSnapshot", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterBackupService_DeleteSnapshotSchedule", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterBackupService_ImportSnapshotMeta", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterBackupService_ListSnapshotRestorations", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterBackupService_ListSnapshotSchedules", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterBackupService_ListSnapshots", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterBackupService_RestoreSnapshot", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterBackup_CreateSnapshot","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterBackup_CreateSnapshotSchedule","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterBackup_DeleteSnapshot","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterBackup_DeleteSnapshotSchedule","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterBackup_EditSnapshotSchedule","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterBackup_ImportSnapshotMeta","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterBackup_ListSnapshotRestorations","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterBackup_ListSnapshotSchedules","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterBackup_ListSnapshots","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterBackup_RestoreSnapshot","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterBackup_RestoreSnapshotSchedule","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterClient_GetTableLocations", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterClient_GetTabletLocations", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterClient_GetTransactionStatusTablets", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterClient_GetYsqlCatalogConfig","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterClient_RedisConfigGet","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterClient_RedisConfigSet","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterClient_ReservePgsqlOids", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterCluster_AreLeadersOnPreferredOnly","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_ChangeLoadBalancerState","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_ChangeMasterClusterConfig","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_DumpState","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_GetAutoFlagsConfig", "microseconds","counter"); // 2.15.2.1
        table.insert("handler_latency_yb_master_MasterCluster_GetLeaderBlacklistCompletion","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_GetLoadBalancerState","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_GetLoadMoveCompletion","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_GetMasterClusterConfig","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_GetMasterRegistration", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterCluster_IsLoadBalanced","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_IsLoadBalancerIdle","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_IsMasterLeaderServiceReady","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_ListLiveTabletServers","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_ListMasterRaftPeers","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_ListMasters","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_ListTabletServers", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterCluster_PromoteAutoFlags","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_RemovedMasterUpdate","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterCluster_SetPreferredZones","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDcl_AlterRole","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDcl_CreateRole","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDcl_DeleteRole","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDcl_GetPermissions","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDcl_GrantRevokePermission","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDcl_GrantRevokeRole","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_AlterNamespace","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_AlterTable","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_BackfillIndex","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_CreateNamespace", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterDdl_CreateTable", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterDdl_CreateTablegroup","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_CreateUDType","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_DeleteNamespace","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_DeleteTable", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterDdl_DeleteTablegroup","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_DeleteUDType","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_GetBackfillJobs","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_GetColocatedTabletSchema","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_GetNamespaceInfo", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterDdl_GetTableDiskSize","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_GetTableSchema", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterDdl_GetTablegroupSchema","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_GetUDTypeInfo","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_IsAlterTableDone","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_IsCreateNamespaceDone","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_IsCreateTableDone", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterDdl_IsDeleteNamespaceDone","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_IsDeleteTableDone", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterDdl_IsTruncateTableDone", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterDdl_LaunchBackfillIndexForTable","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_ListNamespaces", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterDdl_ListTablegroups","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_ListTables", "microseconds","counter"); // 2.15.3.0
        table.insert("handler_latency_yb_master_MasterDdl_ListUDTypes","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterDdl_TruncateTable", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterEncryption_AddUniverseKeys","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterEncryption_ChangeEncryptionInfo","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterEncryption_GetUniverseKeyRegistry", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterEncryption_HasUniverseKeyInMemory","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterEncryption_IsEncryptionEnabled","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterHeartbeat_TSHeartbeat", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterReplication_AlterUniverseReplication","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_ChangeXClusterRole","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_CreateCDCStream","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_DeleteCDCStream","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_DeleteUniverseReplication","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_GetCDCDBStreamInfo","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_GetCDCStream","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_GetReplicationStatus","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_GetTableSchemaFromSysCatalog","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_GetUDTypeMetadata","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_GetUniverseReplication","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_GetXClusterEstimatedDataLoss","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_GetXClusterSafeTime","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_IsBootstrapRequired","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_IsSetupUniverseReplicationDone","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_ListCDCStreams","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_SetUniverseReplicationEnabled","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_SetupNSUniverseReplication","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_SetupUniverseReplication","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_UpdateCDCStream","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_UpdateConsumerOnProducerMetadata","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_UpdateConsumerOnProducerSplit","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_ValidateReplicationInfo","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterReplication_WaitForReplicationDrain","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_master_MasterService_AddUniverseKeys", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_AlterNamespace", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_AlterRole", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_AlterTable", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_AlterUniverseReplication", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_AreLeaderOnPreferredOnly", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_BackfillIndex", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_ChangeEncryptionInfo", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_ChangeLoadBalancerState", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_ChangeMasterClusterConfig", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_CreateCDCStream", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_CreateNamespace", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_CreateRole", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_CreateTable", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_CreateTablegroup", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_CreateTransactionStatusTable", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_CreateUDType", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_DdlLog", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_DeleteCDCStream", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_DeleteNamespace", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_DeleteNotServingTablet", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_DeleteRole", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_DeleteTable", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_DeleteTablegroup", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_DeleteUniverseReplication", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_DumpState", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_FlushCoverage", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_FlushTables", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetBackfillJobs", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetCDCStream", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetColocatedTabletSchema", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetLeaderBlacklistCompletion", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetLoadBalancerState", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetLoadMoveCompletion", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetMasterClusterConfig", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetMasterRegistration", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetNamespaceInfo", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetPermissions", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetTableLocations", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetTableSchema", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetTabletLocations", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetUDType", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetUniveerserReplication", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetUniverseKeyRegistration", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetYsqlCatalogConfig", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GetYsqlCatalogConfig", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GrantRevokePermission", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_GrantRevokeRole", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_HasUniverseKeyInMemory", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_IsAlterTableDone", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_IsCreateNamespaceDone", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_IsCreateTableDone", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_IsDeleteNamespaceDone", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_IsDeleteTableDone", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_IsEncryptionEnabled", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_IsFlushTablesDone", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_IsInitDbDone", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_IsLoadBalanced", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_IsLoadBalancerIdle", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_IsMasterLeaderServiceReady", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_IsSetupUniverseReplicationDone", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_IsTruncateTableDone", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_LaunchBackfillIndexForTable", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_ListCDCStreams", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_ListLiveTabletServers", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_ListMasterRaftPeers", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_ListMasters", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_ListNamespaces", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_ListTablegroups", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_ListTables", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_ListTabletServers", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_ListUDType", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_RedisConfigGet", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_RedisConfigSet", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_RemoveMasterUpdate", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_ReservePgsqlOids", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_SetPreferredZones", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_SetUniverseReplicationEnabled", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_SetupUniverseReplication", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_SplitTablet", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_TSHeartbeat", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_TruncateTable", "microseconds","counter");
        table.insert("handler_latency_yb_master_MasterService_UpdateCDCStream", "microseconds","counter");
        table.insert("handler_latency_yb_server_GenericService_FlushCoverage", "microseconds","counter");
        table.insert("handler_latency_yb_server_GenericService_GetAutoFlagsConfigVersion", "microseconds","counter");
        table.insert("handler_latency_yb_server_GenericService_GetFlag", "microseconds","counter");
        table.insert("handler_latency_yb_server_GenericService_GetStatus", "microseconds","counter");
        table.insert("handler_latency_yb_server_GenericService_Ping", "microseconds","counter");
        table.insert("handler_latency_yb_server_GenericService_RefreshFlags", "microseconds","counter");
        table.insert("handler_latency_yb_server_GenericService_ReloadCertificates", "microseconds","counter");
        table.insert("handler_latency_yb_server_GenericService_ServerClock", "microseconds","counter");
        table.insert("handler_latency_yb_server_GenericService_SetFlag", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_AlterDatabase", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_AlterTable", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_BackfillIndex", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_CheckIfPitrActive", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_CreateDatabase", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_CreateSequencesDataTable", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_CreateTable", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_CreateTablegroup", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_DeleteDBSequences", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_DeleteSequenceTuple", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_DropDatabase", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_DropTable", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_DropTablegroup", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_FinishTransaction", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_GetCatalogMasterVersion", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_GetDatabaseInfo", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_GetTableDiskSize", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_GetTserverCatalogVersionInfo","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_tserver_PgClientService_GetTserverCatalogVersionInfo","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_tserver_PgClientService_Heartbeat", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_InsertSequenceTuple", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_IsInitDbDone", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_ListLiveTabletServers", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_OpenTable", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_Perform", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_ReadSequenceTuple", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_ReserveOids", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_RollbackToSubTransaction", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_SetActiveSubTransaction", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_TabletServerCount", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_TruncateTable", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_UpdateSequenceTuple", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_PgClientService_ValidatePlacement", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_RemoteBootstrapService_BeginRemoteBootstrapSession", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_RemoteBootstrapService_ChangePeerRole", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_RemoteBootstrapService_CheckRemoteBootstrapSessionActive", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_RemoteBootstrapService_CheckSessionActive", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_RemoteBootstrapService_EndRemoteBootstrapSession", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_RemoteBootstrapService_FetchData", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_RemoteBootstrapService_KeepLogAnchorAlive", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_RemoteBootstrapService_RegisterLogAnchor", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_RemoteBootstrapService_RemoveRemoteBootstrapSession", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_RemoteBootstrapService_RemoveSession", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_RemoteBootstrapService_UnregisterLogAnchor", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_RemoteBootstrapService_UpdateLogAnchor", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_AddTableToTablet", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_AlterSchema", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_BackfillDone", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_BackfillIndex", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_CopartitionTable", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_CountIntents", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_CreateTablet", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_DeleteTablet", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_FlushTablets", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_GetSafeTime", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_GetTransactionStatusAtParticipant", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_PrepareDeleteTransactionTablet","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_RemoveTableFromTablet", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_SplitTablet", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_TabletSnapshotOp", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_TestRetry", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_UpdateTransaction", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_UpdateTransactionTablesVersion","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_tserver_TabletServerAdminService_UpgradeYsql", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerBackupService_TabletSnapshotOp", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerForwardService_Read", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerForwardService_Write", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_AbortTransaction", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_Checksum", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_GetLogLocation", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_GetMasterAddresses", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_GetSharedData", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_GetSplitKey", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_GetTabletStatus", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_GetTransactionStatus", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_GetTransactionStatusAtParticipant", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_GetTserverCatalogVersionInfo", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_ImportData", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_IsTabletServerReady", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_ListMasterServers","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_tserver_TabletServerService_ListMasterServers","microseconds","counter"); // 2.17.2
        table.insert("handler_latency_yb_tserver_TabletServerService_ListTablets", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_ListTabletsForTabletServer", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_NoOp", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_ProbeTransactionDeadlock", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_Publish", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_Read", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_TakeTransaction", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_Truncate", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_UpdateTransaction", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_UpdateTransactionStatusLocation", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_UpdateTransactionWaitingForStatus", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_VerifyTableRowRange", "microseconds","counter");
        table.insert("handler_latency_yb_tserver_TabletServerService_Write", "microseconds","counter");
        table.insert("log_append_latency", "microseconds","counter");
        table.insert("log_bytes_logged", "bytes","counter");
        table.insert("log_entry_batches_per_group", "requests","counter");
        table.insert("log_gc_duration", "microseconds","counter");
        table.insert("log_group_commit_latency", "microseconds","counter");
        table.insert("log_reader_read_batch_latency", "microseconds","counter");
        table.insert("log_roll_latency", "microseconds","counter");
        table.insert("log_sync_latency", "microseconds","counter");
        table.insert("log_wal_size", "bytes","counter");
        table.insert("op_apply_queue_length", "tasks","counter");
        table.insert("op_apply_queue_time", "microseconds","counter");
        table.insert("op_apply_run_time", "microseconds","counter");
        table.insert("op_read_queue_length", "tasks","counter");
        table.insert("op_read_queue_run_time", "microseconds","counter");
        table.insert("op_read_queue_time", "microseconds","counter");
        table.insert("op_read_run_time", "microseconds","counter");
        table.insert("post_split_trigger_compaction_pool_queue_time_us", "microseconds","counter");
        table.insert("post_split_trigger_compaction_pool_run_time_us", "microseconds","counter");
        table.insert("ql_read_latency", "microseconds","counter");
        table.insert("ql_write_latency","microseconds","counter"); // 2.17.2
        table.insert("read_time_wait", "microseconds","counter"); // 2.15.3.2
        table.insert("redis_read_latency", "microseconds","counter");
        table.insert("rocksdb_bytes_per_multiget", "bytes","counter");
        table.insert("rocksdb_bytes_per_read", "bytes","counter");
        table.insert("rocksdb_bytes_per_write", "bytes","counter");
        table.insert("rocksdb_compaction_times_micros", "microseconds","counter");
        table.insert("rocksdb_db_get_micros", "microseconds","counter");
        table.insert("rocksdb_db_multiget_micros", "microseconds","counter");
        table.insert("rocksdb_db_seek_micros", "microseconds","counter");
        table.insert("rocksdb_db_write_micros", "microseconds","counter");
        table.insert("rocksdb_numfiles_in_singlecompaction", "files","counter");
        table.insert("rocksdb_read_block_compaction_micros", "microseconds","counter");
        table.insert("rocksdb_read_block_get_micros", "microseconds","counter");
        table.insert("rocksdb_sst_read_micros", "microseconds","counter");
        table.insert("rocksdb_wal_file_sync_micros", "microseconds","counter");
        table.insert("rocksdb_write_raw_block_micros", "microseconds","counter");
        table.insert("rpc_incoming_queue_time", "microseconds","counter");
        table.insert("snapshot_read_inflight_wait_duration", "microseconds","counter");
        table.insert("transaction_pool_cache", "microseconds","counter");
        table.insert("ts_bootstrap_time", "microseconds","counter");
        table.insert("wait_queue_resume_waiter_pool_queue_time_us","microseconds","counter"); // 2.17.2
        table.insert("wait_queue_resume_waiter_pool_run_time_us","microseconds","counter"); // 2.17.2
        table.insert("write_lock_latency", "microseconds","counter");
        table.insert("write_op_duration_client_propagated_consistency", "microseconds","counter");
        table.insert("ycql_queries_system_auth_resource_role_permission_index", "microseconds","counter");
        table.insert("ycql_queries_system_auth_resource_role_permissions_index","?","counter"); // 2.17.2
        table.insert("ycql_queries_system_auth_role_permissions", "microseconds","counter");
        table.insert("ycql_queries_system_auth_roles", "microseconds","counter");
        table.insert("ycql_queries_system_local", "microseconds","counter");
        table.insert("ycql_queries_system_partitions", "microseconds","counter");
        table.insert("ycql_queries_system_peers", "microseconds","counter");
        table.insert("ycql_queries_system_schema_aggregates", "microseconds","counter");
        table.insert("ycql_queries_system_schema_columns", "microseconds","counter");
        table.insert("ycql_queries_system_schema_functions", "microseconds","counter");
        table.insert("ycql_queries_system_schema_indexes", "microseconds","counter");
        table.insert("ycql_queries_system_schema_keyspaces", "microseconds","counter");
        table.insert("ycql_queries_system_schema_tables", "microseconds","counter");
        table.insert("ycql_queries_system_schema_triggers", "microseconds","counter");
        table.insert("ycql_queries_system_schema_types", "microseconds","counter");
        table.insert("ycql_queries_system_schema_views", "microseconds","counter");
        table.insert("ycql_queries_system_size_estimates", "microseconds","counter");
        table
    }
    /// Insert a row into the HashMap.
    fn insert(&mut self, name: &str, unit: &str, statistic_type: &str) {
        self.countsumstatisticsdetails.insert( name.to_string(), 
                                               CountSumStatisticDetails { unit: unit.to_string(), unit_suffix: Self::suffix_lookup_countsum(unit), divisor: Self::divisor_lookup_countsum(unit), stat_type: statistic_type.to_string() }
        );
    }
    /// This creates a small lookup table to translate the full statistic type to the display version, which is abbreviated.
    /// This also helps to document the known statistic types.
    fn suffix_lookup_countsum(unit: &str) -> String {
        let suffix = HashMap::from( [
            ("microseconds",    "us"),
            ("operations",      "ops"),
            ("bytes",           "bytes"),
            ("files",           "files"),
            ("tasks",           "tasks"),
            ("requests",        "reqs"),
            ("?",               "?"),
        ]);
        match suffix.get(unit) {
            Some(x) => x.to_string(),
            None => {
                info!("The suffix for {} does not exist.", unit);
                "?".to_string()
            },
        }
    }
    /// Translate the type of divisor to the unit to make calculations.
    /// Please mind this is currently not used.
    fn divisor_lookup_countsum(unit: &str) -> i64 {
        let divisor = HashMap::from( [
            ("microseconds", 1000000_i64),
            ("operations",         1_i64),
            ("bytes",              1_i64),
            ("files",              1_i64),
            ("tasks",              1_i64),
            ("requests",           1_i64),
            ("?",                  0_i64),
        ]);
        match divisor.get(unit) {
            Some(x) => *x,
            None => {
                info!("The divisor for {} does not exist.", unit);
                0_i64
            },
        }
    }
}
/// These are the unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_countsum_statistic_existing_name() {
        let countsum_statistics = CountSumStatistics::create();
        let lookup = countsum_statistics.lookup("Create_Tablet_Attempt");
        assert_eq!(lookup.unit, "microseconds");
        assert_eq!(lookup.unit_suffix, "us");
        assert_eq!(lookup.divisor, 1000000);
        assert_eq!(lookup.stat_type, "counter");
    }
    #[test]
    fn lookup_countsum_statistic_non_existing_name() {
        let countsum_statistics = CountSumStatistics::create();
        let lookup = countsum_statistics.lookup("does not exist");
        assert_eq!(lookup.unit, "?");
        assert_eq!(lookup.unit_suffix, "?");
        assert_eq!(lookup.divisor, 0);
        assert_eq!(lookup.stat_type, "?");
    }

}
