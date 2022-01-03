use structopt::StructOpt;
use port_scanner::scan_port_addr;
use std::process;
use std::collections::{BTreeMap, HashMap};
use std::io::stdin;
use std::time::SystemTime;
use regex::Regex;
use substring::Substring;


use yb_stats::{Metrics, NamedMetrics, XValues, XLatencies, StatisticDetails};

#[derive(Debug, StructOpt)]
struct Opts {
    #[structopt(short, long, default_value = "192.168.66.80:7000,192.168.66.81:7000,192.168.66.82:7000")]
    metric_sources: String,
    #[structopt(short, long, default_value = ".*")]
    stat_name_match: String,
    #[structopt(short, long, default_value = ".*")]
    table_name_match: String,
    #[structopt(short, long, default_value = "2")]
    wait_time: i32,
    #[structopt(short, long)]
    begin_end_mode: bool,
}

fn main()
{
    let mut statistic_details: HashMap<String, StatisticDetails> = HashMap::new();
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_Heartbeat"),  StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_AlterDatabase"),  StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_AlterTable"),  StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_BackfillIndex"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_CreateDatabase"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_CreateSequencesDataTable"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_CreateTable"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_CreateTablegroup"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_DropDatabase"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_DropTable"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_DropTablegroup"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_GetCatalogMasterVersion"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_GetDatabaseInfo"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_IsInitDbDone"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_ListLiveTabletServers"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_OpenTable"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_ReserveOids"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_TabletServerCount"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_PgClientService_TruncateTable"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_server_GenericService_SetFlag"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_server_GenericService_GetFlag"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_server_GenericService_RefreshFlags"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_server_GenericService_FlushCoverage"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_server_GenericService_ServerClock"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_server_GenericService_GetStatus"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_server_GenericService_Ping"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerAdminService_CreateTablet"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerAdminService_DeleteTablet"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerAdminService_AlterSchema"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerAdminService_GetSafeTime"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerAdminService_BackfillIndex"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerAdminService_BackfillDone"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerAdminService_CopartitionTable"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerAdminService_FlushTablets"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerAdminService_CountIntents"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerAdminService_AddTableToTablet"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerAdminService_RemoveTableFromTablet"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerAdminService_SplitTablet"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerAdminService_UpgradeYsql"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerAdminService_TabletSnapshotOp"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_consensus_ConsensusService_UpdateConsensus"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_consensus_ConsensusService_RequestConsensusVote"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_consensus_ConsensusService_ChangeConfig"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_consensus_ConsensusService_GetNodeInstance"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_consensus_ConsensusService_RunleaderElection"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_consensus_ConsensusService_LeaderElectionLost"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_consensus_ConsensusService_LeaderStepDown"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_consensus_ConsensusService_GetLastOpId"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_consensus_ConsensusService_GetConsensusState"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_consensus_ConsensusService_StartRemoteBootstrap"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_RemoteBootstrapService_BeginRemoteBootstrapSession"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_RemoteBootstrapService_CheckSessionActive"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_RemoteBootstrapService_FetchData"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_RemoteBootstrapService_EndRemoteBootstrapSession"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_RemoteBootstrapService_RemoveSession"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerForwardService_Write"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerForwardService_Read"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_Write"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_Read"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_VerifyTableRowRange"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_NoOp"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_ListTablets"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_GetLogLocation"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_Checksum"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_ListTabletsForTabletServer"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_ImportData"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_UpdateTransaction"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_GetTransactionStatus"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_GetTransactionStatusAtParticipant"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_AbortTransaction"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_Truncate"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_GetTabletStatus"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_GetMasterAddresses"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_Publish"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_IsTabletServerReady"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_TakeTransaction"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_GetSplitKey"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_tserver_TabletServerService_GetSharedData"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("dns_resolve_latency_during_init_proxy"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("transaction_pool_cache"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_cdc_CDCService_CreateCDCStream"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_cdc_CDCService_DeleteCDCStream"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_cdc_CDCService_ListTablets"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_cdc_CDCService_GetChanges"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_cdc_CDCService_GetCheckPoint"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_cdc_CDCService_UpdateCdcReplicatedIndex"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_cdc_CDCService_BootstrapProducer"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_yb_cdc_CDCService_GetLatestEntryOpId"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("op_apply_queue_length"), StatisticDetails { unit: String::from("tasks"), unit_suffix: String::from("tasks"), divisor: 1, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("op_apply_queue_time"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("op_apply_run_time"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("op_read_queue_length"), StatisticDetails { unit: String::from("tasks"), unit_suffix: String::from("tasks"), divisor: 1, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("op_read_queue_time"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("op_read_queue_run_time"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("ts_bootstrap_time"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("post_split_trigger_compaction_pool_queue_time_us"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("post_split_trigger_compaction_pool_run_time_us"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("admin_triggered_compaction_pool_queue_time_us"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("admin_triggered_compaction_pool_run_time_us"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_outbound_call_queue_time"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_outbound_call_send_time"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_outbound_call_time_to_response"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("handler_latency_outbound_transfer"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rpc_incoming_queue_time"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("log_gc_duration"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("snapshot_read_inflight_wait_duration"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("write_op_duration_client_propagated_consistency"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("redis_read_latency"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("ql_read_latency"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("write_lock_latency"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("dns_resolve_latency_during_update_raft_config"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("log_reader_read_batch_latency"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("log_bytes_logged"), StatisticDetails { unit: String::from("byte"), unit_suffix: String::from("b"), divisor: 1, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("log_wal_size"), StatisticDetails { unit: String::from("byte"), unit_suffix: String::from("b"), divisor: 1, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("log_sync_latency"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("log_append_latency"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("log_group_commit_latency"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("log_roll_latency"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("log_entry_batches_per_group"), StatisticDetails { unit: String::from("requests"), unit_suffix: String::from("req"), divisor: 1, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rocksdb_db_get_micros"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rocksdb_db_write_micros"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rocksdb_compaction_times_micros"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rocksdb_wal_file_sync_micros"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rocksdb_db_multiget_micros"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rocksdb_read_block_compaction_micros"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rocksdb_read_block_get_micros"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rocksdb_write_raw_block_micros"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rocksdb_numfiles_in_singlecompaction"), StatisticDetails { unit: String::from("files"), unit_suffix: String::from("files"), divisor: 1, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rocksdb_db_seek_micros"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rocksdb_sst_read_micros"), StatisticDetails { unit: String::from("microseconds"), unit_suffix: String::from("us"), divisor: 1000000, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rocksdb_bytes_per_read"), StatisticDetails { unit: String::from("byte"), unit_suffix: String::from("byte"), divisor: 1, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rocksdb_bytes_per_write"), StatisticDetails { unit: String::from("byte"), unit_suffix: String::from("byte"), divisor: 1, stat_type: String::from("counter") } );
    statistic_details.insert( String::from("rocksdb_bytes_per_multiget"), StatisticDetails { unit: String::from("byte"), unit_suffix: String::from("byte"), divisor: 1, stat_type: String::from("counter") } );
    
    let non_counter_statistic_names = vec![
        "block_cache_usage",
        "block_cache_multi_touch_usage",
        "block_cache_single_touch_usage",
        "compact_rs_running",
        "delta_minor_compact_rs_running",
        "delta_major_compact_rs_running",
        "follower_lag_ms",
        "generic_current_allocated_bytes",
        "generic_heap_size",
        "in_progress_ops",
        "is_raft_leader",
        "hybrid_clock_error",
        "hybrid_clock_hybrid_time",
        "hybrid_clock_skew",
        "log_gc_running",
        "mem_tracker",
        "mem_tracker_BlockBasedTable",
        "mem_tracker_Call",
        "mem_tracker_Call_CQL",
        "mem_tracker_Call_Inbound_RPC",
        "mem_tracker_Call_Outbound_RPC",
        "mem_tracker_Call_Redis",
        "mem_tracker_IntentsDB",
        "mem_tracker_IntentsDB_MemTable",
        "mem_tracker_Read_Buffer",
        "mem_tracker_Read_Buffer_CQL",
        "mem_tracker_Read_Buffer_Inbound_RPC",
        "mem_tracker_Read_Buffer_Outbound_RPC",
        "mem_tracker_Read_Buffer_Outbound_RPC_Queueing",
        "mem_tracker_Read_Buffer_Outbound_RPC_Reading",
        "mem_tracker_Read_Buffer_Outbound_RPC_Receive",
        "mem_tracker_Read_Buffer_Outbound_RPC_Sending",
        "mem_tracker_Read_Buffer_Redis",
        "mem_tracker_Read_Buffer_Redis_Allocated",
        "mem_tracker_Read_Buffer_Redis_Mandatory",
        "mem_tracker_Read_Buffer_Redis_Used",
        "mem_tracker_RegularDB",
        "mem_tracker_RegularDB_MemTable",
        "mem_tracker_Tablets",
        "mem_tracker_log_cache",
        "tcmalloc_current_total_thread_cache_bytes",
        "tcmalloc_max_total_thread_cache_bytes",
        "tcmalloc_pageheap_unmapped_bytes",
        "tcmalloc_pageheap_free_bytes",
        "threads_running",
        "threads_running_Master_reactor",
        "threads_running_TabletServer_reactor",
        "threads_running_acceptor",
        "threads_running_catalog_manager",
        "threads_running_heartbeater",
        "threads_running_iotp_Master",
        "threads_running_iotp_TabletServer",
        "threads_running_iotp_call_home",
        "threads_running_maintenance",
        "threads_running_remote_bootstrap",
        "threads_running_tablet_manager",
        "threads_running_thread_pool",
        "threads_running_rpc_thread_pool",
        "raft_term",
        "rocksdb_sequence_number",
        "rpcs_in_queue_yb_GenericService",
        "rpcs_in_queue_yb_consensus_ConsensusService",
        "rpcs_in_queue_yb_master_masterService",
        "rpcs_in_queue_yb_master_masterBackupService",
        "rpcs_in_queue_yb_server_GenericService",
        "rpcs_in_queue_yb_tserver_TabletServerService",
        "rpcs_in_queue_yb_tserver_TabletServerAdminService",
        "rpcs_in_queue_yb_tserver_TabletServerBackupService",
        "rpcs_in_queue_yb_tserver_RemoteBootstrapService",
        "rpc_connections_alive",
        "rpc_inbound_calls_alive",
        "rpc_outbound_calls_alive"
    ];

    let options = Opts::from_args();
    let metric_sources_vec: Vec<&str> = options.metric_sources.split(",").collect();
    let stat_name_match = &options.stat_name_match.as_str();
    let stat_name_filter = Regex::new(stat_name_match).unwrap();
    let table_name_match = &options.table_name_match.as_str();
    let table_name_filter = Regex::new(table_name_match).unwrap();
    let wait_time = options.wait_time as u64;
    let begin_end_mode = options.begin_end_mode as bool;
    let mut bail_out = false;
    let mut first_pass = true;
    let mut fetch_time = SystemTime::now();
    let mut previous_fetch_time = SystemTime::now();

    let mut value_statistics: BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, XValues>>>> = BTreeMap::new();
    let mut latency_statistics: BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, XLatencies>>>> = BTreeMap::new();

    loop {
        for hostname in &metric_sources_vec {
            if !scan_port_addr(hostname) {
                println!("Warning, unresponsive: {}", hostname.to_string());
                continue;
            };
            fetch_time = SystemTime::now();
            previous_fetch_time = if first_pass { fetch_time } else { previous_fetch_time };
            let metrics_data = reqwest::blocking::get(format!("http://{}/metrics", hostname.to_string()))
                .unwrap_or_else(|e| {
                    eprintln!("Error reading from URL: {}", e);
                    process::exit(1);
                })
                .text().unwrap();
            let metrics_parse: Vec<Metrics> = serde_json::from_str(&metrics_data)
                .unwrap_or_else(|e| {
                    eprintln!("Error parsing response: {}", e);
                    process::exit(1);
                });

            for metric in &metrics_parse {
                let metrics_type = &metric.metrics_type;
                let metrics_id = &metric.id;
                let metrics_attribute_namespace_name = match &metric.attributes.namespace_name {
                    Some(namespace_name) => namespace_name.to_string(),
                    None => "-".to_string(),
                };
                let metrics_attribute_table_name = match &metric.attributes.table_name {
                    Some(table_name) => table_name.to_string(),
                    None => "-".to_string(),
                };

                for statistic in &metric.metrics {
                    match statistic {
                        NamedMetrics::MetricValue { name, value } => {
                            if *value > 0 {
                                value_statistics.entry(hostname.to_string().into()).or_insert(BTreeMap::new());
                                match value_statistics.get_mut(&hostname.to_string()) {
                                    None => { panic!("value_statistics - hostname not found, should have been inserted") }
                                    Some(vs_hostname) => {
                                        vs_hostname.entry(metrics_type.to_string().into()).or_insert(BTreeMap::new());
                                        match vs_hostname.get_mut(&metrics_type.to_string()) {
                                            None => { panic!("value_statistics - hostname.type not found, should have been inserted")}
                                            Some (vs_type) => {
                                                vs_type.entry( metrics_id.to_string().into()).or_insert(BTreeMap::new());
                                                match vs_type.get_mut( &metrics_id.to_string()) {
                                                    None => { panic!("value_statistics - hostname.type.id not found, should have been inserted")}
                                                    Some (vs_id) => {
                                                        match vs_id.get_mut(&name.to_string()) {
                                                            None => {
                                                                vs_id.insert(name.to_string(), XValues { table_name: metrics_attribute_table_name.to_string(), namespace: metrics_attribute_namespace_name.to_string(), current_time: fetch_time, previous_time: previous_fetch_time, current_value: *value, previous_value: 0 });
                                                            }
                                                            Some(vs_name) => {
                                                                *vs_name = XValues { table_name: metrics_attribute_table_name.to_string(), namespace: metrics_attribute_namespace_name.to_string(), current_time: fetch_time, previous_time: vs_name.current_time, current_value: *value, previous_value: vs_name.current_value };
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            };
                        },
                        NamedMetrics::MetricLatency { name, total_count, min, mean, percentile_75, percentile_95, percentile_99, percentile_99_9, percentile_99_99, max, total_sum } => {
                            if *total_count > 0 {
                                latency_statistics.entry(hostname.to_string().into()).or_insert(BTreeMap::new());
                                match latency_statistics.get_mut(&hostname.to_string()) {
                                    None => { panic!("latency_statistics - hostname not found, should have been inserted") }
                                    Some(ls_hostname) => {
                                        ls_hostname.entry(metrics_type.to_string().into()).or_insert(BTreeMap::new());
                                        match ls_hostname.get_mut(&metrics_type.to_string()) {
                                            None => { panic!("latency_statistics - hostname.type not found, should have been inserted")}
                                            Some (ls_type) => {
                                                ls_type.entry( metrics_id.to_string().into()).or_insert(BTreeMap::new());
                                                match ls_type.get_mut( &metrics_id.to_string()) {
                                                    None => { panic!("latency_statistics - hostname.type.id not found, should have been inserted")}
                                                    Some (ls_id) => {
                                                        match ls_id.get_mut(&name.to_string()) {
                                                            None => {
                                                                ls_id.insert(name.to_string(), XLatencies { table_name: metrics_attribute_table_name.to_string(), namespace: metrics_attribute_namespace_name.to_string(), current_time: fetch_time, previous_time: previous_fetch_time, current_total_count: *total_count, previous_total_count: 0, current_min: *min, current_mean: *mean, current_percentile_75: *percentile_75, current_percentile_95: *percentile_95, current_percentile_99: *percentile_99, current_percentile_99_9: *percentile_99_9, current_percentile_99_99: *percentile_99_99, current_max: *max, current_total_sum: *total_sum, previous_total_sum: 0 });
                                                            }
                                                            Some(ls_name) => {
                                                                *ls_name = XLatencies { table_name: metrics_attribute_table_name.to_string(), namespace: metrics_attribute_namespace_name.to_string(), current_time: fetch_time, previous_time: ls_name.current_time, current_total_count: *total_count, previous_total_count: ls_name.current_total_count, current_min: *min, current_mean: *mean, current_percentile_75: *percentile_75, current_percentile_95: *percentile_95, current_percentile_99: *percentile_99, current_percentile_99_9: *percentile_99_9, current_percentile_99_99: *percentile_99_99, current_max: *max, current_total_sum: *total_sum, previous_total_sum: ls_name.current_total_sum };
                                                            }
                                                        };
                                                    }
                                                };
                                            }
                                        };
                                    }
                                };
                            };
                        }
                    };
                };
            };
        };

        std::process::Command::new("clear").status().unwrap();
        for (hostname_key, hostname_value) in value_statistics.iter() {
            for (type_key, type_value) in hostname_value.iter() {
                for (id_key,  id_value) in type_value.iter() {
                    for (name_key, name_value) in id_value.iter().filter(|(k,_v)| stat_name_filter.is_match(k)) {
                        if name_value.current_value - name_value.previous_value != 0
                           && name_value.current_time.duration_since(name_value.previous_time).unwrap().as_millis() != 0
                           && table_name_filter.is_match(&name_value.table_name) {
                            let adaptive_length = if id_key.len() < 15 { 0 }  else { id_key.len()-15 };
                            if  non_counter_statistic_names.contains(&&name_key[..]) {
                                 println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:+15}",
                                          hostname_key,
                                          type_key,
                                          id_key.substring(adaptive_length,id_key.len()),
                                          name_value.namespace,
                                          name_value.table_name,
                                          name_key,
                                          name_value.current_value,
                                          name_value.current_value-name_value.previous_value
                                 );
                            }  else {
                                 println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:>15.3}/s",
                                          hostname_key,
                                          type_key,
                                          id_key.substring(adaptive_length,id_key.len()),
                                          name_value.namespace,
                                          name_value.table_name,
                                          name_key,
                                          name_value.current_value - name_value.previous_value,
                                          ((name_value.current_value - name_value.previous_value) as f64 / (name_value.current_time.duration_since(name_value.previous_time).unwrap().as_millis() as f64) * 1000 as f64),
                                );
                            };
                        };
                    };
                };
            };
        };
        for (hostname_key, hostname_value) in latency_statistics.iter() {
            for (type_key, type_value) in hostname_value.iter() {
                for (id_key, id_value) in type_value.iter() {
                    for (name_key, name_value) in id_value.iter().filter(|(k,_v)| stat_name_filter.is_match(k)) {
                        if name_value.current_total_count - name_value.previous_total_count != 0
                            && name_value.current_time.duration_since(name_value.previous_time).unwrap().as_millis() != 0
                            && table_name_filter.is_match(&name_value.table_name) {
                            let details = match statistic_details.get(&name_key.to_string()) {
                                 None => { StatisticDetails { unit: String::from('?'), unit_suffix: String::from('?'), divisor: 1, stat_type: String::from('?') }},
                                 Some(x) => { StatisticDetails { unit: x.unit.to_string(), unit_suffix: x.unit_suffix.to_string(), divisor: x.divisor, stat_type: x.stat_type.to_string() }  }
                            } ;
                            let adaptive_length = if id_key.len() < 15 { 0 } else { id_key.len()-15 };
                            println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:>15.3}/s avg: {:>9.0} {:10}",
                                      hostname_key,
                                      type_key,
                                      id_key.substring(adaptive_length,id_key.len()),
                                      name_value.namespace,
                                      name_value.table_name,
                                      name_key,
                                      name_value.current_total_count-name_value.previous_total_count,
                                      ((name_value.current_total_count-name_value.previous_total_count) as f64 / (name_value.current_time.duration_since(name_value.previous_time).unwrap().as_millis() as f64) *100 as f64),
                                      (
                                             (name_value.current_total_sum-name_value.previous_total_sum) /
                                             (name_value.current_total_count-name_value.previous_total_count) ) as f64,
                                     details.unit_suffix
                            );
                        };
                    };
                };
            };
        };

        first_pass = false;
        previous_fetch_time = fetch_time;

        if begin_end_mode {
            if bail_out {
                std::process::exit(0);
            } else {
                bail_out = true;
                println!("Begin metrics snapshot created, press enter to create end snapshot for difference calculation.");
                let mut input = String::new();
                stdin().read_line(&mut input).ok().expect("failed");
            };
        } else {
            std::thread::sleep(std::time::Duration::from_secs(wait_time));
        };
   };
}