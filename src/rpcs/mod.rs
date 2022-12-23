//! Module for reading `/rpcz` on the masters, tablet servers and YSQL webservers.
//!
//! The `/rpcz` endpoint contains current active and inactive connections for the following endpoints:
//!
//! | endpoint     | default port | type                                      |
//! |--------------|--------------|-------------------------------------------|
//! | master       | port 7000    | [InboundConnection], [OutboundConnection] |
//! | tablet server| port 9000    | [InboundConnection], [OutboundConnection] |
//! | YCQL         | port 12000   | [InboundConnection]                       |
//! | YSQL         | port 13000   | [YsqlConnection]                          |
//!
//! The rpc functionality is called from:
//! - [snapshot::perform_snapshot] -> [AllStoredConnections::perform_snapshot] (general snapshot, saves rpc data)
//! - main -> [print_rpcs] (print adhoc or snapshot rpc info)
//!
mod structs;
mod rpcs;

pub use structs::*;
pub use rpcs::*;
