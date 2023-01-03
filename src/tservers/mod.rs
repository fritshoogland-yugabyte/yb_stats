//! Module for reading `/api/v1/tablet-servers` on the masters.
//!
//! the `/api/v1/tablet-servers` endpoint contains the current status of all tablet servers, including:
//! ([TabletServer])
//! - time since heartbeat
//! - status
//! - uptime
//! - ram used
//! - num sst files
//! - total sst files size
//! - uncompressed sst files size
//! - path metrics:
//!   ([PathMetrics])
//!   - path
//!   - space used
//!   - total space size
//! - read ops per second
//! - write ops per second
//! - user tablets total
//! - user tablets leaders
//! - system tablets total
//! - system tablets leaders
//! - active tablets
//! - cloud
//! - region
//! - zone
//!
//!  The `/api/v1/tablet-servers` endpoint is only available on the masters, default port 7000.
//! It is available on all the masters, the leader as well as the followers.
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;