//! Module for reading `/memz` on the masters and tablet servers.
//!
//! The `/memz` endpoint contains two memory overviews.
//! It is available on all master and tablet server endpoints, default port numbers 7000 (master), 9000, 12000 (tablet server).
//!
//! yb_stats doesn't do anything other than:
//! - perform a HTTP Get and collect the result from the endpoint.
//! - check if the returned data start with "------------------------------------------------".
//! - If so, save the result as "mems_*hostname:port*"
//!
mod functions;

pub use functions::*;
