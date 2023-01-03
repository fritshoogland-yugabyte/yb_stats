//! Module for reading `/threadz` on the masters and tablet servers.
//!
//! The `/threadz` endpoints contains an overview of the backtrace
//! and some statistics of each thread.
//! This is an HTML table.
//! The threads module parses the HTML table for the values.
//!
//! The `/threadz` endpoint is only available on:
//! - the masters, default port 7000
//! - the tablet servers, default port 9000
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;