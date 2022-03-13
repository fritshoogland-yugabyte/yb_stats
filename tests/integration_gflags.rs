use yb_stats::gflags::{StoredGFlags, read_gflags, add_to_gflags_vector};
use chrono::Local;
use std::env;

fn get_hostname() -> String {
    let hostname_port = match env::var("HOSTNAME_PORT") {
        Ok(value) => value,
        Err(e) => panic!("Error reading environment variable HOSTNAME_PORT: {:?}", e)
    };
    hostname_port
}

#[test]
fn parse_gflags() {
    let mut stored_gflags: Vec<StoredGFlags> = Vec::new();
    let detail_snapshot_time = Local::now();
    let hostname_port = get_hostname();

    let gflags = read_gflags(&hostname_port.as_str());
    add_to_gflags_vector(gflags, &hostname_port.as_str(), detail_snapshot_time, &mut stored_gflags);
}