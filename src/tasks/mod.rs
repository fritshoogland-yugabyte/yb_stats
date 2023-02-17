//! Module for reading `/tasks` on the masters.
//!
//! The purpose of the `/tasks` endpoint on the masters is to show:
//! - the active tasks.
//! - the last 100 tasks of the past 300 seconds (5 minutes) and the last 20 user.
//! - the last 20 user-initiated jobs started in the past 24 hours.
//!
//! This is all in HTML tables.
//!
//! The `/tasks` endpoint is available on:
//! - the masters, default port 7000
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;
