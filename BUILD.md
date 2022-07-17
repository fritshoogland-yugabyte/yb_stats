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
yum install -y git openssl-devel
```
When on Mac OSX, there doesn't seem to any dependencies.

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
2. Strip debugs symbols:
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