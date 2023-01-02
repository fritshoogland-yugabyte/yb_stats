//! Module for reading `/pprof/growth` on the masters and tablet servers.
//!
//! The `/pprof/growth` endpoint contains gperftools heap-profiling delta (growth) information.
//! It is available on all master and tablet server endpoints, default port numbers 7000 (master), 9000, 12000 (tablet server).
//!
//! yb_stats doesn't do anything other than:
//! - perform a HTTP GET and collect the result from the endpoint.
//! - check if the returned data starts with "heap profile".
//! - if so, save the result as "pprof_growth_*hostname:port*".
//!
mod functions;

pub use functions::*;
