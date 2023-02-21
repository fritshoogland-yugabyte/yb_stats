# 1. Does it build?
cargo build --release
BUILD_RETURN=$?
if [ BUILD_RETURN -gt 0 ]; then
  echo "cargo build --release not successful."
  exit 1
fi

# 2. fetch app version from Cargo.toml
CARGO_APP_VERSION=$(grep ^version Cargo.toml | sed 's/.*"\(.*\)"/\1/')

# 3. build using docker file
docker build --rm --tag yb_stats:${CARGO_APP_VERSION} --tag yb_stats:latest --file Dockerfile.container .
BUILD_RETURN=$?
if [ BUILD_RETURN -gt 0 ]; then
  echo "docker build not successful."
  exit 1
fi

# 4. tag images for push
docker tag yb_stats:latest fritshoogland/yb_stats:latest
docker tag yb_stats:${CARGO_APP_VERSION} fritshoogland/yb_stats:${CARGO_APP_VERSION}

# 5. push new container to docker hub
#docker push -a fritshoogland/yb_stats
