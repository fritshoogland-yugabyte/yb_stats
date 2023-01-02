//! Module for reading `/api/v1/masters` on the masters.
//!
//! The `/api/v1/masters` endpoint contains the current status of all masters, including:
//! ([GetMasterRegistrationRequestPB])
//! - Instance ID ([NodeInstancePB]):
//!   - permanent_uuid
//!   - instance_seqno
//!   - start_time_us (Optional)
//! - Role (Optional, [PeerRole])
//! - Registration (Optional, [ServerRegistrationPB]):
//!   - Cloud info (Optional, placement info: cloud, region, zone), [CloudInfoPB]).
//!   - Placement UUID (Optional).
//!   - Private RPC addresses (Optional, [HostPortPB]).
//!   - HTTP addresses (Optional, [HostPortPB]).
//!   - Broadcast addresses (Optional, [HostPortPB]).
//!   - pg_port (Optional).
//! - Error (Optional, this is only filled when an error is countered, [AppStatusPB])
//!
//! The `/api/v1/masters` endpoint is only available on the masters, default port 7000.
//! It is available on all the masters, the leader as well as the followers.
//!
//! The masters functionality is called from:
//! - [crate::snapshot::perform_snapshot] -> [crate::masters::Masters::perform_snapshot] (general snapshot, saves masters data)
//! - [crate::snapshot::snapshot_diff] -> [crate::masters::MastersDiff::snapshot_diff] (general diff, show masters diff based on snapshot data)
//! - [crate] -> [print_masters] (prints adhoc (live) or snapshot masters info)
//! - [crate] -> [masters_diff] (prints masters diff only, based on snapshot data)
//! - [crate::snapshot::adhoc_diff] -> [crate::masters::MastersDiff::adhoc_read_first_snapshot],
//! [crate::masters::MastersDiff::adhoc_read_second_snapshot],
//! [crate::masters::MastersDiff::print] (prints masters diff, based on live data)
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;
