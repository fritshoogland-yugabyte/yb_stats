//! Module for reading `/rpcz` on the masters, tablet servers and YSQL webservers.
//!
mod structs;
mod rpcs;

pub use structs::*;
pub use rpcs::*;
