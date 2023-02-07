//! Module for reading `/tables` on the masters.
//!
//! Please mind there is a `/tables` endpoint on the tablet servers as well!
//! The purpose of the `/tables` endpoint on the tablet server is to show the tables that have a tablet on the local tablet server.
//!
//! The purpose of the `/tables` endpoint on the masters is to show the logical defined tables.
//! And allows to get more data from each logical table.
//!
//! This is all in HTML tables.
//!
//! The `/tables` endpoint is available on:
//! - the masters, default port 7000
//! - the tablet servers, default port 9000
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;
