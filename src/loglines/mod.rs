//! Module for reading `/logs` on the masters and tablet servers.
//!
//! The `/logs` endpoint contains *the last 1MB* of lines from the YugabyteDB master or tablet server.
//!
//! The `/logs` endpoint is only available on:
//! - the masters, default port 7000
//! - the tablet servers, default port 9000.
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;
