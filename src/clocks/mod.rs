//! Module for reading `/tablet-server-clocks` on the masters.
//!
//! The `/tablet-server-clocks` endpoint contains an HTML table with clock/time information of the tablet servers in the cluster.
//! The times in it are the ones registered by the *master leader*, and replicated to the master followers.
//!
//! The `/tablet-server-clocks` information is read into [Clocks], and contains:
//! - server (tablet server)
//! - time_since_heartbeat
//! - status_uptime (status field and uptime field)
//! - physical_time_utc
//! - hybrid_time_utc
//! - heartbeat_rtt (the time it took for a heartbeat packet to return to the master leader)
//! - cloud
//! - region
//! - zone
//!
//! The `/tablet-server-clocks` endpoint is only available on the masters, default port 7000.
//!
//! The clocks functionality is called from:
//! - [crate::snapshot::perform_snapshot] -> [crate::clocks::AllClocks::perform_snapshot] (general snapshot, saves masters data)
//! - [crate] -> [print_clocks] (print adhoc (live) or snapshot clocks info)
//! - [crate] -> [print_latencies] (print adhoc (live) or snapshot clocks info specialized for printing latencies between master leader and tablet servers)
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;
