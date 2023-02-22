cargo build --release
strip -S target/release/yb_stats
CARGO_APP_VERSION=$(grep ^version Cargo.toml | sed 's/.*"\(.*\)"/\1/')
tar czvf yb_stats-osx-intel-v${CARGO_APP_VERSION}-1.tar.gz -C target/release yb_stats
