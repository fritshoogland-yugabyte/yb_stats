//! Module for reading `/tablet-server-clocks` on the masters.
//!
//! All masters are read, the display function determines the master leader, and displays the data from that master.
//! The `--details-enable` switch shows the clock information from all masters.
//!
mod structs;
mod clocks;

pub use structs::*;
pub use clocks::*;
