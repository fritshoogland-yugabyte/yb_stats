//! Module for reading `/rpcz` on the masters, tablet servers and YSQL webservers.
//!
//! The `/rpcz` endpoint contains current active and inactive connections for the following endpoints:
//!
//! | endpoint     | default port | type (struct)                             |
//! |--------------|--------------|-------------------------------------------|
//! | master       | port 7000    | [InboundConnection], [OutboundConnection] |
//! | tablet server| port 9000    | [InboundConnection], [OutboundConnection] |
//! | YCQL         | port 12000   | [InboundConnection]                       |
//! | YSQL         | port 13000   | [YsqlConnection]                          |
//!
//! The rpc functionality is called from:
//! - [crate::snapshot::perform_snapshot] -> [crate::rpcs::AllRpcs::perform_snapshot] (general snapshot, saves rpc data)
//! - [crate] -> [print_rpcs] (print adhoc (live) or snapshot rpc info)
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;
