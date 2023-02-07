//! Module for reading `/tablets` on the tablet servers.
//!
//! There is a `/tablets` endpoint on the tablet servers only!
//! The purpose of the `/tablets` endpoint on the tablet server is to show the tablets on the local tablet server.
//!
//! This is all in HTML tables.
//!
//! The `/tablets` endpoint is available on:
//! - the tablet servers, default port 9000
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;
