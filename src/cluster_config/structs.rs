#![allow(non_camel_case_types)]

use chrono::{DateTime, Local};

/// This struct is a wrapper for the SysClusterConfigEntryPB struct.
///
/// In this way, the struct can be used with function in impl.
#[derive(Debug, Default)]
pub struct AllSysClusterConfigEntryPB {
    pub sysclusterconfigentrypb: Vec<SysClusterConfigEntryPB>,
}

// src/yb/master/catalog_entity_info.proto
// hostname_port + timestamp: added
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SysClusterConfigEntryPB {
    pub hostname_port: Option<String>,
    pub timestamp: Option<DateTime<Local>>,
    pub version: i32,
    pub replication_info: Option<ReplicationInfoPB>,
    pub server_blacklist: Option<BlacklistPB>,
    pub cluster_uuid: String,
    pub encryption_info: Option<EncryptionInfoPB>,
    pub consumer_registry: Option<ConsumerRegistryPB>,
    pub leader_blacklist: Option<BlacklistPB>,
}

// src/yb/master/catalog_entity_info.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct ReplicationInfoPB {
    pub live_replicas: Option<PlacementInfoPB>,
    pub read_replicas: Option<Vec<PlacementInfoPB>>,
    pub affinitized_leaders: Option<Vec<CloudInfoPB>>,
    pub multi_affinitized_leaders: Option<Vec<CloudInfoListPB>>,
}


// src/yb/master/catalog_entity_info.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct PlacementInfoPB {
    pub num_replicas: Option<i32>,
    pub placement_blocks: Option<Vec<PlacementBlockPB>>,
    pub placement_uuid: Option<String>,
}

// src/yb/master/catalog_entity_info.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct PlacementBlockPB {
    pub cloud_info: Option<CloudInfoPB>,
    pub min_num_replicas: Option<i32>,
}

// src/yb/common/common_net.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct CloudInfoPB {
    pub placement_cloud: Option<String>,
    pub placement_region: Option<String>,
    pub placement_zone: Option<String>,
}

// src/yb/common/common_net.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct CloudInfoListPB {
    pub zones: Option<Vec<CloudInfoPB>>,
}

// src/yb/common/catalog_entity_info.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct BlacklistPB {
    pub hosts: Option<Vec<HostPortPB>>,
    pub initial_replica_load: Option<i32>,
    pub initial_leader_load: Option<i32>,
}

// src/yb/common/common_net.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct HostPortPB {
    pub host: String,
    pub port: u32,
}

// src/yb/common/catalog_entity_info.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct EncryptionInfoPB {
    pub encryption_enabled: Option<bool>,
    pub universe_key_registry_encoded: Option<String>, // bytes
    pub key_path: Option<String>,
    pub latest_version_id: Option<String>,
    pub key_in_memory: Option<bool>,
}

// src/yb/cdc/cdc_consumer.proto
// producer_map requires an extra key-value struct
// enable_replicate_transaction_status_table + role have to be optional, not in PB definition.
#[derive(Serialize, Deserialize, Debug)]
pub struct ConsumerRegistryPB {
    pub producer_map: Vec<ConsumerRegistryKeyValue>,
    pub enable_replicate_transaction_status_table: Option<bool>,
    pub role: Option<XClusterRole>,

}
// custom: see above.
#[derive(Serialize, Deserialize, Debug)]
pub struct ConsumerRegistryKeyValue {
    pub key: String,
    pub value: ProducerEntryPB,
}

// src/yb/cdc/cdc_consumer.proto
#[derive(Serialize, Deserialize, Debug)]
#[allow(clippy::upper_case_acronyms)]
pub enum XClusterRole {
    ACTIVE = 0,
    STANDBY = 1,
}

// src/yb/cdc/cdc_consumer.proto
// stream_map requires an extra key-value struct
// disable_stream has to be optional, not in PB definition.
#[derive(Serialize, Deserialize, Debug)]
#[allow(non_snake_case)]
pub struct ProducerEntryPB {
    pub stream_map: Vec<ProducerEntryKeyValue>,
    pub master_addrs: Option<Vec<HostPortPB>>,
    pub DEPRECATED_tserver_addrs: Option<Vec<HostPortPB>>,
    pub disable_stream: Option<bool>,
}
// custom: see above
#[derive(Serialize, Deserialize, Debug)]
pub struct ProducerEntryKeyValue {
    pub key: String,
    pub value: StreamEntryPB,
}

// src/yb/cdc/cdc_consumer.proto
// consumer_producer_tablet_map requires an extra key-value struct
// producer_schema has to be optional, not in PB definition.
#[derive(Serialize, Deserialize, Debug)]
pub struct StreamEntryPB {
    pub consumer_producer_tablet_map: Vec<StreamEntryKeyValue>,
    pub consumer_table_id: String,
    pub producer_table_id: String,
    pub local_tserver_optimized: bool,
    pub producer_schema: Option<ProducerSchemaPB>,
}
// custom: see above
#[derive(Serialize, Deserialize, Debug)]
pub struct StreamEntryKeyValue {
    key: String,
    value: ProducerTabletListPB,
}

// src/yb/cdc/cdc_consumer.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct ProducerTabletListPB {
    pub tablets: Option<Vec<String>>,
    pub start_key: Option<Vec<String>>,
    pub end_key: Option<Vec<String>>,
}

// src/yb/cdc/cdc_consumer.proto
// pending_schema_version, pending_schema and last_compatible_consumer_schema_version have to be optional, not in PB definition.
#[derive(Serialize, Deserialize, Debug)]
pub struct ProducerSchemaPB {
    pub validated_schema_version: u32,
    pub pending_schema_version: Option<u32>,
    pub pending_schema: Option<SchemaPB>,
    pub last_compatible_consumer_schema_version: Option<u32>,
}

// src/yb/common/common.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaPB {
    pub columns: Option<Vec<ColumnSchemaPB>>,
    pub table_properties: Option<TablePropertiesPB>,
    pub colocated_table_id: Option<ColocatedTableIdentifierPB>,
    pub pgschema_name: Option<String>,
}

// src/yb/common/common.proto
#[derive(Serialize, Deserialize, Debug)]
#[allow(non_snake_case)]
pub struct ColumnSchemaPB {
    pub id: Option<u32>,
    pub name: String,
    #[serde(rename = "type")]
    pub columnschemapb_type: QLTypePB,
    pub is_key: Option<bool>,
    pub is_hash_key: Option<bool>,
    pub is_nullable: Option<bool>,
    pub is_static: Option<bool>,
    pub is_counter: Option<bool>,
    pub sorting_type: Option<u32>,
    pub order: Option<i32>,
    pub OBSOLETE_json_operations: Option<Vec<QLJsonOperationPB>>,
    pub pg_type_id: Option<u32>,
}

// src/yb/common/common.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct QLTypePB {
    pub main: Option<DataType>,
    pub params: Option<Vec<QLTypePB>>,
    pub udtype_info: Option<UDTypeInfo>,
}

// src/yb/common/common.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct UDTypeInfo {
    pub keyspace_name: Option<String>,
    pub name: Option<String>,
    pub id: Option<String>,
    pub field_names: Option<Vec<String>>,
}

// src/yb/common/value.proto
#[derive(Serialize, Deserialize, Debug)]
#[allow(clippy::upper_case_acronyms)]
pub enum DataType {
    UNKNOWN_DATA = 999,
    NULL_VALUE_TYPE = 0,
    INT8 = 1,
    INT16 = 2,
    INT32 = 3,
    INT64 = 4,
    STRING = 5,
    BOOL = 6,
    FLOAT = 7,
    DOUBLE = 8,
    BINARY = 9,
    TIMESTAMP = 10,
    DECIMAL = 11,
    VARINT = 12,
    INET = 13,
    LIST = 14,
    MAP = 15,
    SET = 16,
    UUID = 17,
    TIMEUUID = 18,
    TUPLE = 19,  // TUPLE is not yet fully implemented, but it is a CQL type.
    TYPEARGS = 20,
    USER_DEFINED_TYPE = 21,
    FROZEN = 22,
    DATE = 23,
    TIME = 24,
    JSONB = 25,
    // All unsigned datatypes will be removed from QL because databases do not have these types.
    UINT8 = 100,
    UINT16 = 101,
    UINT32 = 102,
    UINT64 = 103,
    GIN_NULL = 104,
}

// src/yb/common/common.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct QLJsonOperationPB {
    pub json_operator: JsonOperatorPB,
    pub operand: QLExpressionPB,
}

// src/yb/common/common.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct QLExpressionPB {
    pub expr: QLExpression,
}

// src/yb/common/common.proto // sort of
#[derive(Serialize, Deserialize, Debug)]
pub enum QLExpression {
    value(QLValuePB),
    column_id(i32),
    subscripted_col(QLSubscriptedColPB),
    bind_id(i32),
    condition(QLConditionPB),
    bfcall(QLBCallPB),
    tscall(QLBCallPB),
    bocall(QLBCallPB),
    json_column(QLJsonColumnOperationsPB),
    tuple(QLTupleExpressionPB),
}

// src/yb/common/common.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct QLJsonColumnOperationsPB {
    column_id: Option<i32>,
    json_operations: Option<Vec<QLJsonOperationPB>>,
}


// src/yb/common/common_types.proto
#[derive(Serialize, Deserialize, Debug)]
pub enum JsonOperatorPB {
    JSON_OBJECT = 0,
    JSON_TEXT = 1,
}

// src/yb/common/value.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct QLValuePB {
    pub value: QLValuePBValue,
}

// src/yb/common/value.proto // sort of
#[derive(Serialize, Deserialize, Debug)]
pub enum QLValuePBValue {
    int8_value(i32),
    int16_value(i32),
    int32_value(i32),
    int64_value(i64),
    float_value(f32),
    double_value(f64),
    string_value(String),
    bool_value(bool),
    timestamp_value(i64),
    binary_value(String),
    inetaddress_value(String),
    map_value(QLMapValuePB),
    set_value(QLSeqValuePB),
    list_value(QLSeqValuePB),
    decimal_value(String),
    varint_value(String),
    frozen_value(QLSeqValuePB),
    uuid_value(String),
    timeuuid_value(String),
    jsonb_value(String),
    date_value(u32),
    time_Value(i64),
    uint32_value(u32),
    uint64_value(u64),
    virtual_value(QLVirtualValuePB),
    gin_null_value(u32),
    tuple_value(QLSeqValuePB),
}

// src/yb/common/value.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct QLMapValuePB {
    pub keys: Option<Vec<QLValuePB>>,
    pub values: Option<Vec<QLValuePB>>,
}

// src/yb/common/value.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct QLSeqValuePB {
    pub elems: Option<Vec<QLValuePB>>,
}

// src/yb/common/value.proto
#[derive(Serialize, Deserialize, Debug)]
#[allow(clippy::upper_case_acronyms)]
pub enum QLVirtualValuePB {
    LIMIT_MAX = 1,
    LIMIT_MIN = 2,
    COUNTER = 3,
    SS_FORWARD = 4,
    SS_REVERSE = 5,
    TOMBSTONE = 6,
    NULL_LOW = 7,
    ARRAY = 8,
}

// src/yb/common/common.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct QLSubscriptedColPB {
    pub column_id: Option<i32>,
    pub subscript_args: Option<Vec<QLExpressionPB>>,
}

// src/yb/common/common.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct QLConditionPB {
    pub op: Option<QLOperator>,
    pub operands: Option<Vec<QLExpressionPB>>,
}

// src/yb/common/value.proto
#[derive(Serialize, Deserialize, Debug)]
pub enum QLOperator {
    QL_OP_NOOP = 0,
    QL_OP_NOT = 1,
    QL_OP_IS_TRUE = 2,
    QL_OP_IS_FALSE = 3,
    // Logic operators that take two or more operands.
    QL_OP_AND = 4,
    QL_OP_OR = 5,
    // Relation operators that take one operand.
    QL_OP_IS_NULL = 6,
    QL_OP_IS_NOT_NULL = 7,
    // Relation operators that take two operands.
    QL_OP_EQUAL = 8,
    QL_OP_LESS_THAN = 9,
    QL_OP_LESS_THAN_EQUAL = 10,
    QL_OP_GREATER_THAN = 11,
    QL_OP_GREATER_THAN_EQUAL = 12,
    QL_OP_NOT_EQUAL = 13,
    QL_OP_LIKE = 14,
    QL_OP_NOT_LIKE = 15,
    QL_OP_IN = 16,
    QL_OP_NOT_IN = 17,
    // Relation operators that take three operands.
    QL_OP_BETWEEN = 18,
    QL_OP_NOT_BETWEEN = 19,
    // Operators that take no operand. For use in "if" clause only currently.
    QL_OP_EXISTS = 20,     // IF EXISTS
    QL_OP_NOT_EXISTS = 21, // IF NOT EXISTS
}

// src/yb/common/common.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct QLBCallPB {
    pub opcode: Option<i32>,
    pub operands: Option<Vec<QLExpressionPB>>,
}

// src/yb/common/common.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct TablePropertiesPB {
    pub default_time_to_live: Option<u64>,
    pub contain_counters: Option<bool>,
    pub is_transactional: Option<bool>,
    pub copartition_table_id: Option<String>,
    pub consistency_level: Option<YBConsistencyLevel>,
    pub use_mangled_column_name: Option<bool>,
    pub num_tablets: Option<i32>,
    pub is_ysql_catalog_table: Option<bool>,
    pub retain_delete_markers: Option<bool>,
    pub backfilling_timestamp: Option<u64>,
    pub partitioning_version: Option<u32>,
}

// src/yb/common/common_types.proto
#[derive(Serialize, Deserialize, Debug)]
#[allow(clippy::upper_case_acronyms)]
pub enum YBConsistencyLevel {
    STRONG = 1,
    CONSISTENT_PREFIX = 2,
    USER_ENFORCED = 3,
}

// src/yb/common/common.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct ColocatedTableIdentifierPB {
    pub value: ColocatedTableIdentifierPBValue,
}

// src/yb/common/common.proto // sort of
#[derive(Serialize, Deserialize, Debug)]
pub enum ColocatedTableIdentifierPBValue {
    colocation_id(u32),
    cotable_id(String),
}

// src/yb/common/common.proto
#[derive(Serialize, Deserialize, Debug)]
pub struct QLTupleExpressionPB {
    pub elems: Option<Vec<QLExpressionPB>>,
}
