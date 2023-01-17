//! Module for reading `/api/v1/is-leader` on the masters.
//!
//! The `/api/v1/is-leader` endpoint reports if the master is the leader.
//! This returns: 
//! - `{"STATUS":"OK"}` on the leader.
//! - *Nothing* on the followers.
//!
//! The `/api/v1/is-leader` endpoint is only available on the masters, default port 7000.
//! 
//! The isleader functionality is a helper module, it is not used directly.
//! 
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;
