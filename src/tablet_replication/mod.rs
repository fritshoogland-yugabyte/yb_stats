//! Module for reading `/tablet-replication` on the masters.
//!
//! The purpose of the `/tablet-replication` endpoint on the masters is to show:
//! - leaderless tablets.
//! - under-replicated tablets.
//!
//! This is all in HTML tables.
//!
//! The `/tablet-replication` endpoint is available on:
//! - the masters, default port 7000
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;
