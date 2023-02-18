//! Module for reading `/operations` on the tablet servers.
//!
//! The purpose of the `/operations` endpoint on the tablet server is to show:
//! - the active operations.
//!
//! This is all in an HTML table.
//!
//! The `/operations` endpoint is available on:
//! - the tablet server, default port 9000
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;
