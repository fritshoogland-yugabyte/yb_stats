//! The structs
//!
use chrono::{DateTime, Local};
/// This struct is a wrapper for the Clocks struct.
///
/// In this way, the struct can be used with functions in the impl.
#[derive(Debug, Default)]
pub struct AllClocks {
    pub clocks: Vec<Clocks>
}
/// The main struct containing the clocks information.
///
/// The fields are parsed from the HTML page /tablet-server-clocks on the master.
/// From the tablet server table:
/// ```html
///<h2>Tablet Servers</h2>
/// <table class='table table-striped'>
///     <tr>
///       <th>Server</th>
///       <th>Time since </br>heartbeat</th>
///       <th>Status & Uptime</th>
///       <th>Physical Time (UTC)</th>
///       <th>Hybrid Time (UTC)</th>
///       <th>Heartbeat RTT</th>
///       <th>Cloud</th>
///       <th>Region</th>
///       <th>Zone</th>
///     </tr>
///   <tr>
///   <td><a href="http://yb-1.local:9000/">yb-1.local:9000</a></br>  bca5ecc0f10a44c1a9d2d85a9c97ea49</td><td>0.1s</td>    <td style="color:Green">ALIVE: 2:09:21</td>    <td>2023-01-17 14:22:18.908376</td>    <td>2023-01-17 14:22:18.908375</td>    <td>2.24ms</td>    <td>local</td>    <td>local</td>    <td>local1</td>  </tr>
///   <tr>
///   <td><a href="http://yb-2.local:9000/">yb-2.local:9000</a></br>  7db98e6ba63d41cc8f731faa52470cb5</td><td>0.1s</td>    <td style="color:Green">ALIVE: 2:09:19</td>    <td>2023-01-17 14:22:18.899160</td>    <td>2023-01-17 14:22:18.899160</td>    <td>1.06ms</td>    <td>local</td>    <td>local</td>    <td>local2</td>  </tr>
///   <tr>
///   <td><a href="http://yb-3.local:9000/">yb-3.local:9000</a></br>  b00600497d314ab1a7f590b91d6c1699</td><td>1.0s</td>    <td style="color:Green">ALIVE: 2:09:21</td>    <td>2023-01-17 14:22:18.025692</td>    <td>2023-01-17 14:22:18.025691</td>    <td>0.41ms</td>    <td>local</td>    <td>local</td>    <td>local3</td>  </tr>
/// </table>
/// ```
///
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Clocks {
    /// yb_stats added to allow understanding the source host.
    pub hostname_port: Option<String>,
    /// yb_stats added to allow understanding the timestamp.
    pub timestamp: Option<DateTime<Local>>,
    pub server: String,
    pub time_since_heartbeat: String,
    pub status_uptime: String,
    pub physical_time_utc: String,
    pub hybrid_time_utc: String,
    pub heartbeat_rtt: String,
    pub cloud: String,
    pub region: String,
    pub zone: String,
}
