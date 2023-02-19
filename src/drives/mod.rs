//! Module for reading `/drives` on the masters and tablet servers.
//!
//! The purpose of the `/drives` endpoint on the masters and tablet servers is to show:
//! - the drives assigned to the server via the `fs_data_dirs` flag.
//! - the total amount of space, and the amount of space in use on each drive.
//!
//! This is all in HTML tables.
//!
//! The `/drives` endpoint is available on:
//! - the masters, default port 7000
//! - the tablet servers, default port 9000
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;
