//! Module for reading `/mem-trackers` on the masters and tablet servers.
//!
//! The `/memtrackers` endpoint contains an overview of the memtrackers framework categories in use.
//! This is done in an HTML table. The memtrackers module parses the HTML table for the values of the categories.
//!
//! The `/memtrackers` endpoint is only available on:
//! - the masters, default port 7000
//! - the tablet servers, default port 9000
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;