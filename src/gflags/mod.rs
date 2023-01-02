//! Module for reading `/varz` on the masters and tablet servers (deprecated: use the vars module).
//!
//! This module is deprecated. Since Yugabyte version 2.?.?:
//! - the gflags have additional property with them: type, which can be:
//!     - Default (not changed)
//!     - NodeInfo (set for local node identification)
//!     - Custom (explicitly set to a value)
//!   this property is not visible when the `/varz` endpoint is requested with `?raw` added.
//!
//! - the `/api/v1/varz` endpoint exists, which provides the gflags/vars data in JSON format.
//!
//! The `/varz` endpoint is available on the masters, default port 7000 and tablet servers, default port 9000.
//!
mod structs;
mod functions;

pub use structs::*;
pub use functions::*;
