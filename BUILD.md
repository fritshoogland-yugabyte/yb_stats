This file describes how to build yb_stats.

# build from source
In order to build yb_stats from source you need cargo, which contains the rust compiler.

## operating system dependencies
When on linux, the following dependencies must be installed. 
These are the dependencies for RPM/yum based distributions (validated with Centos 7 and Alma 8):
- git
- openssl-devel  
 
Yum command for install:
```
sudo yum install -y git openssl-devel gcc
```
When on Mac OSX, there doesn't seem to any dependencies, provided xcode is installed.

## install cargo
The most common way to install cargo on OSX or Linux is:
```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## clone yb_stats repository
The next thing to do is clone this repository:
```
git clone https://github.com/fritshoogland-yugabyte/yb_stats.git
```

## build yb_stats
Now yb_stats can be build using cargo. Cargo is the rust package manager, which takes care of obtaining all the rust libraries and compile and link it into an executable.
```
cd yb_stats
cargo build --release
```

# generate RPM
Cargo can be extended by an helper to generate a binary RPM package from a cargo project.
As far as I know, this needs to be done on the target OS system, so linux, and is build for that version.

This needs the cargo-generate-rpm helper to be installed into cargo:
```
cargo install cargo-generate-rpm
```

1. Build a release executable:
```
cargo build --release
```
2. Strip debug symbols:
```
strip -s target/release/yb_stats
```
3. Generate RPM
The steps are slightly different between Centos 7 and Alma 8:  
First review the `Cargo.toml` file. For Centos 7, release should be set to '1.el7', for Alma 8, release should be set to '1.el8'.  
 
    1. Centos 7:
    ```
    cargo generate-rpm --payload-compress gzip
    ```
    The reason is Centos 7 doesn't support a newer form of compression that was adopted with EL8, and thus will complain about a checksum error.  

    2. Alma 8:
    ```
    cargo generate-rpm
    ```

The RPM file will be created in the `target/generate-rpm` directory.

# homebrew / OSX
(warning: experimental!)
1. Build a release executable
```
cargo build --release
```
2. Create a gzipped tarball
```
tar czf ~/Downloads/yb_stats.tar.gz -C target/release/ yb_stats
```
3. Obtain sha256 hash of the tarball
```
shasum -a 256 ~/Downloads/yb_stats.tar.gz
```
4. Upload to a github release
Save the full path.

5. create github repository for homebrew
name it homebrew-yb_stats
 
6. git clone the homebrew repository
```
cd ~/code
git clone https://github.com/fritshoogland-yugabyte/homebrew-yb_stats.git
cd homebriew-yb_stats
```
7. Setup the formula
```shell
mkdir Formula
vi Formula/yb_stats.rb
```

```shell
# Documentation: https://docs.brew.sh/Formula-Cookbook
#                https://rubydoc.brew.sh/Formula
# PLEASE REMOVE ALL GENERATED COMMENTS BEFORE SUBMITTING YOUR PULL REQUEST!
class yb_stats < Formula
  desc "A utility to read all available meta-data that should be present in a standard YugabyteDB cluster"
  homepage "https://github.com/fritshoogland-yugabyte/yb_stats"
  url "https://github.com/fritshoogland-yugabyte/yb_stats/releases/latest/download/yb_stats.tar.gz"
  sha256 "480c978c8ba5c27a096523a22e74b2a389121538ab89fee8468a294a80dbbbd2"
  version "0.8.7"

  def install
    bin.install "yb_stats"
  end
end
```

8. install yb_stats via homebrew
```shell
brew tap fritshoogland-yugabyte/yb_stats
brew install yb_stats

```