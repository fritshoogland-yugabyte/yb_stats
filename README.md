# yb_stats

This is a utility to extract the metrics endpoints of the master and tablet servers in a YugebyteDB cluster. 
The tool extracts the statistics groups of:
- cluster
- server
- table
- tablet

And understands the two different metrics types:
- 'value type' (name, value)
- 'latency type' (name, total_count, min, mean, percentiles, max, total_sum)

The 'value type' contains counter and gauge values. The tool classifies these as such (but is work in progress).

The goal of the tool is to provide a simple way to measure statistics in an ad-hoc way with no dependencies but existence of the tool and access to the metrics endpoint.

# usage
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % ./target/debug/yb_stats -h
yb_stats 0.1.0

USAGE:
    yb_stats [FLAGS] [OPTIONS]

FLAGS:
    -b, --begin-end-mode
    -d, --details-enable
    -g, --gauges-enable
    -h, --help              Prints help information
    -V, --version           Prints version information

OPTIONS:
    -m, --metric-sources <metric-sources>         [default: 192.168.66.80:7000,192.168.66.81:7000,192.168.66.82:7000]
    -s, --stat-name-match <stat-name-match>       [default: .*]
    -t, --table-name-match <table-name-match>     [default: .*]
    -w, --wait-time <wait-time>                   [default: 2]
```
The most important switch is `-m`, which allows you to specify a comma-separated list of hostname:port combinations. 

The second consideration is to use the tool in the default mode, which is showing the difference of non-gauge statistics each `-w` interval, or use begin-end mode by specifying `-b`.

By default, the tool summarizes the table and tablet statistics per hostname:port. If you want the statistics to be separated per table and tablet, use the `-d` switch.  

Lots of the statistics are counters, which are the statistics that are shown. If you want to include the gauge statistics, use the `-g` switch.  

The number of statistics is overwhelming. For that reason, two switches can be used to filter the output:
- `-t` table name match  
The table name match switch allows you to specify a regex to filter on a table name or table names. You cannot match table names only in `-d`/`--details-enable` mode.
- `-s` statistic name match  
The statistic name match switch allows you to specify a regex to filter on a statistic name or statistics names.


# output
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:9000,192.168.66.81:9000,192.168.66.82:9000
192.168.66.80:9000   server   yb.tabletserver -               -                              cpu_stime                                                                            9 ms               4.302/s
192.168.66.80:9000   server   yb.tabletserver -               -                              cpu_utime                                                                           23 ms              10.994/s
192.168.66.80:9000   server   yb.tabletserver -               -                              proxy_request_bytes_yb_consensus_ConsensusService_UpdateConsensus                 5040 bytes         2409.178/s
192.168.66.80:9000   server   yb.tabletserver -               -                              proxy_request_bytes_yb_master_MasterService_TSHeartbeat                            312 bytes          149.140/s
192.168.66.80:9000   server   yb.tabletserver -               -                              proxy_response_bytes_yb_consensus_ConsensusService_UpdateConsensus                1656 bytes          791.587/s
192.168.66.80:9000   server   yb.tabletserver -               -                              proxy_response_bytes_yb_master_MasterService_TSHeartbeat                           934 bytes          446.463/s
192.168.66.80:9000   server   yb.tabletserver -               -                              rpc_inbound_calls_created                                                           23 req             10.994/s
192.168.66.80:9000   server   yb.tabletserver -               -                              rpc_outbound_calls_created                                                          26 req             12.428/s
192.168.66.80:9000   server   yb.tabletserver -               -                              server_uptime_ms                                                                  2092 ms            1000.000/s
192.168.66.80:9000   server   yb.tabletserver -               -                              service_request_bytes_yb_consensus_ConsensusService_UpdateConsensus               4532 bytes         2166.348/s
192.168.66.80:9000   server   yb.tabletserver -               -                              service_request_bytes_yb_tserver_PgClientService_Heartbeat                          51 bytes           24.379/s
192.168.66.80:9000   server   yb.tabletserver -               -                              service_response_bytes_yb_consensus_ConsensusService_UpdateConsensus              1782 bytes          851.816/s
192.168.66.80:9000   server   yb.tabletserver -               -                              service_response_bytes_yb_tserver_PgClientService_Heartbeat                         11 bytes            5.258/s
192.168.66.80:9000   server   yb.tabletserver -               -                              tcp_bytes_received                                                                7591 bytes         3628.585/s
192.168.66.80:9000   server   yb.tabletserver -               -                              tcp_bytes_sent                                                                    7145 bytes         3415.392/s
192.168.66.80:9000   server   yb.tabletserver -               -                              voluntary_context_switches                                                         632 csws           302.103/s
```
(this is a partial output)
- The first column shows the hostname:port number endpoint specification.
- The second column shows the metric type (cluster, server, table, tablet).
- The third column shows the 'id' of the type. This results in different types: for server, this is yb.tabletserver or yb.master. In detail mode (not default), for a table, this is the unique identifier for a table, and for a tablet this is the UUID of the tablet. The last 15 characters are displayed.
- The fourth column is the namespace (in detail mode). For the server type, this is '-'.
- The fifth column is the table_name (in detail mode). For the server type, this is '-'.
- The sixth column is the statistic name.
- The seventh column is the difference between the previous and the current fetch for counter type statistics, and for gauge types this is the value of the current fetch. For non-latency statistics, it includes the type of the statistic, such as 'ms' for milliseconds, bytes, etc.
- The eighth column is the difference between the previous and the current fetch divided by the time of the two fetches, to get an idea of rate of the statistic happening, and for gauge types it's the difference (positive or negative) between the previous and current fetch.
- The ninth column is unique to latency type, and shows the difference between the previous and current fetch divided by the difference of the total_sum statistic. This way the average latency over the period of the snapshot for that event is calculated. It also includes the type of the statistic.

# examples
## investigate CPU usage
Are the tablet servers busy?
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:9000,192.168.66.81:9000,192.168.66.82:9000,192.168.66.80:7000,192.168.66.81:7000,192.168.66.82:7000 -s cpu
192.168.66.80:7000   server   yb.master       -               -                              cpu_stime                                                                            3 ms               0.587/s
192.168.66.80:7000   server   yb.master       -               -                              cpu_utime                                                                           11 ms               2.152/s
192.168.66.80:9000   server   yb.tabletserver -               -                              cpu_stime                                                                           34 ms               6.652/s
192.168.66.80:9000   server   yb.tabletserver -               -                              cpu_utime                                                                           29 ms               5.674/s
192.168.66.81:7000   server   yb.master       -               -                              cpu_stime                                                                            5 ms               0.978/s
192.168.66.81:7000   server   yb.master       -               -                              cpu_utime                                                                            8 ms               1.564/s
192.168.66.81:9000   server   yb.tabletserver -               -                              cpu_stime                                                                           32 ms               6.261/s
192.168.66.81:9000   server   yb.tabletserver -               -                              cpu_utime                                                                           28 ms               5.478/s
192.168.66.82:7000   server   yb.master       -               -                              cpu_stime                                                                            9 ms               1.760/s
192.168.66.82:7000   server   yb.master       -               -                              cpu_utime                                                                           28 ms               5.476/s
192.168.66.82:9000   server   yb.tabletserver -               -                              cpu_stime                                                                           25 ms               4.891/s
192.168.66.82:9000   server   yb.tabletserver -               -                              cpu_utime                                                                           35 ms               6.848/s
```
(this clears the screen, begin-end doesn't do that)  
The column indicating 'ms' shows the absolute difference between the two measurements. The column after that shows the amount divided by the time in seconds, and thus shows the amount per second. Because the amount of CPU as a statistic is in milliseconds, a value of 1000/s means a full CPU (as seen by the operating system) is used.

## investigate memory usage
How is memory used for a tablet server?
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:9000 -g -b -s '(mem_tracker|generic_heap)'
Begin metrics snapshot created, press enter to create end snapshot for difference calculation.

192.168.66.80:9000   server   yb.tabletserver -               -                              generic_heap_size                                                             54525952 bytes               +0
192.168.66.80:9000   server   yb.tabletserver -               -                              mem_tracker                                                                      12288 bytes               +0
192.168.66.80:9000   server   yb.tabletserver -               -                              mem_tracker_Compressed_Read_Buffer_Receive                                     9067088 bytes               +0
192.168.66.80:9000   server   yb.tabletserver -               -                              mem_tracker_Read_Buffer_Inbound_RPC_Receive                                    6969968 bytes               +0
192.168.66.80:9000   server   yb.tabletserver -               -                              mem_tracker_Read_Buffer_Outbound_RPC_Receive                                   2097120 bytes               +0
192.168.66.80:9000   server   yb.tabletserver -               -                              mem_tracker_Tablets                                                              12288 bytes               +0
192.168.66.80:9000   tablet   da72a340ef21288 yugabyte        test                           mem_tracker                                                                       4096 bytes               +0
192.168.66.80:9000   tablet   da72a340ef21288 yugabyte        test                           mem_tracker_IntentsDB                                                             2048 bytes               +0
192.168.66.80:9000   tablet   da72a340ef21288 yugabyte        test                           mem_tracker_IntentsDB_MemTable                                                    2048 bytes               +0
192.168.66.80:9000   tablet   da72a340ef21288 yugabyte        test                           mem_tracker_RegularDB                                                             2048 bytes               +0
192.168.66.80:9000   tablet   da72a340ef21288 yugabyte        test                           mem_tracker_RegularDB_MemTable                                                    2048 bytes               +0
192.168.66.80:9000   tablet   d6157816e9e2356 yugabyte        test                           mem_tracker                                                                       4096 bytes               +0
192.168.66.80:9000   tablet   d6157816e9e2356 yugabyte        test                           mem_tracker_IntentsDB                                                             2048 bytes               +0
192.168.66.80:9000   tablet   d6157816e9e2356 yugabyte        test                           mem_tracker_IntentsDB_MemTable                                                    2048 bytes               +0
192.168.66.80:9000   tablet   d6157816e9e2356 yugabyte        test                           mem_tracker_RegularDB                                                             2048 bytes               +0
192.168.66.80:9000   tablet   d6157816e9e2356 yugabyte        test                           mem_tracker_RegularDB_MemTable                                                    2048 bytes               +0
192.168.66.80:9000   tablet   52cc722bb53ca15 yugabyte        test                           mem_tracker                                                                       4096 bytes               +0
192.168.66.80:9000   tablet   52cc722bb53ca15 yugabyte        test                           mem_tracker_IntentsDB                                                             2048 bytes               +0
192.168.66.80:9000   tablet   52cc722bb53ca15 yugabyte        test                           mem_tracker_IntentsDB_MemTable                                                    2048 bytes               +0
192.168.66.80:9000   tablet   52cc722bb53ca15 yugabyte        test                           mem_tracker_RegularDB                                                             2048 bytes               +0
192.168.66.80:9000   tablet   52cc722bb53ca15 yugabyte        test                           mem_tracker_RegularDB_MemTable                                                    2048 bytes               +0
```
- generic_heap_size is the size as reported as 'root' in the mem-trackers page.  
This is generally the RSS size of the heap of the process.
- the mem_tracker assignments show the division of these.   
The mem_tracker root (mem_tracker for yb.tabletserver) is low (12288) here, because the read buffers do not account for these (!), and the actual allocations are because of the memtable's of the three tablets (please take some time to see and understand the mem_tracker->IntentDB/RegularDB->MemTable) for this test table. 

## investigate network usage
How much network traffic is executed by the master and tablet servers?
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:9000,192.168.66.81:9000,192.168.66.82:9000,192.168.66.80:7000,192.168.66.81:7000,192.168.66.82:7000 -b -s tcp
Begin metrics snapshot created, press enter to create end snapshot for difference calculation.

192.168.66.80:7000   server   yb.master       -               -                              tcp_bytes_received                                                                 636 bytes          562.832/s
192.168.66.80:7000   server   yb.master       -               -                              tcp_bytes_sent                                                                     255 bytes          225.664/s
192.168.66.80:9000   server   yb.tabletserver -               -                              tcp_bytes_received                                                                3394 bytes         3006.200/s
192.168.66.80:9000   server   yb.tabletserver -               -                              tcp_bytes_sent                                                                    3070 bytes         2719.221/s
192.168.66.81:7000   server   yb.master       -               -                              tcp_bytes_received                                                                 424 bytes          375.554/s
192.168.66.81:7000   server   yb.master       -               -                              tcp_bytes_sent                                                                     170 bytes          150.576/s
192.168.66.81:9000   server   yb.tabletserver -               -                              tcp_bytes_received                                                                3976 bytes         3518.584/s
192.168.66.81:9000   server   yb.tabletserver -               -                              tcp_bytes_sent                                                                    3648 bytes         3228.319/s
192.168.66.82:7000   server   yb.master       -               -                              tcp_bytes_received                                                                 893 bytes          792.369/s
192.168.66.82:7000   server   yb.master       -               -                              tcp_bytes_sent                                                                    2512 bytes         2228.926/s
192.168.66.82:9000   server   yb.tabletserver -               -                              tcp_bytes_received                                                                3766 bytes         3335.695/s
192.168.66.82:9000   server   yb.tabletserver -               -                              tcp_bytes_sent                                                                    3567 bytes         3159.433/s
```
This way you can see the amount of bytes that is sent and received per server on average over the measuring period. Please mind this is bytes, network traffic regularly is expressed in bits. In order to gets bits, multiply the bytes figure by 8.

## investigate (WAL) logging
What is happening for WAL logging?
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:9000,192.168.66.81:9000,192.168.66.82:9000,192.168.66.80:7000,192.168.66.81:7000,192.168.66.82:7000 -b -s log
Begin metrics snapshot created, press enter to create end snapshot for difference calculation.

192.168.66.80:9000   tablet   52cc722bb53ca15 yugabyte        test                           log_bytes_logged                                                                   180 bytes           66.079/s
192.168.66.81:9000   tablet   52cc722bb53ca15 yugabyte        test                           log_bytes_logged                                                                   180 bytes           66.176/s
192.168.66.82:9000   tablet   52cc722bb53ca15 yugabyte        test                           log_bytes_logged                                                                   180 bytes           66.176/s
192.168.66.80:9000   table    000000000004000 yugabyte        test                           log_append_latency                                                                   2           0.073/s avg:        28 us
192.168.66.80:9000   table    000000000004000 yugabyte        test                           log_entry_batches_per_group                                                          2           0.073/s avg:         1 req
192.168.66.80:9000   table    000000000004000 yugabyte        test                           log_group_commit_latency                                                             2           0.073/s avg:      6823 us
192.168.66.80:9000   table    000000000004000 yugabyte        test                           log_sync_latency                                                                     2           0.073/s avg:      6373 us
192.168.66.81:9000   table    000000000004000 yugabyte        test                           log_append_latency                                                                   2           0.074/s avg:        29 us
192.168.66.81:9000   table    000000000004000 yugabyte        test                           log_entry_batches_per_group                                                          2           0.074/s avg:         1 req
192.168.66.81:9000   table    000000000004000 yugabyte        test                           log_group_commit_latency                                                             2           0.074/s avg:      7925 us
192.168.66.81:9000   table    000000000004000 yugabyte        test                           log_sync_latency                                                                     2           0.074/s avg:      7852 us
192.168.66.82:9000   table    000000000004000 yugabyte        test                           log_append_latency                                                                   2           0.074/s avg:        29 us
192.168.66.82:9000   table    000000000004000 yugabyte        test                           log_entry_batches_per_group                                                          2           0.074/s avg:         1 req
192.168.66.82:9000   table    000000000004000 yugabyte        test                           log_group_commit_latency                                                             2           0.074/s avg:      8035 us
192.168.66.82:9000   table    000000000004000 yugabyte        test                           log_sync_latency                                                                     2           0.074/s avg:      7958 us
```
This shows the write ahead logging statistics. Please notice most of the statistics are per table, except for the bytes (log_bytes_logged), which are accounted on the server level.

## investigate number of sst files for each tablet
How many sst files does each tablet have? Please mind a tablet can have zero sst files if all the data is in the memtable only, and not flushed yet.
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:7000,192.168.66.80:9000 -b -g -s rocksdb_current_version_num_sst_files
Begin metrics snapshot created, press enter to create end snapshot for difference calculation.

192.168.66.80:7000   tablet   000000000000000                 sys.catalog                    rocksdb_current_version_num_sst_files                                                2 files               +0
192.168.66.80:9000   tablet   e77b84366a68636 yugabyte        test                           rocksdb_current_version_num_sst_files                                                1 files               +0
```

# How to install
Currently, this is only available as source code, not as executable.  
However, it's easy to compile the tool:

1. Get rust: visit `https://www.rust-lang.org/tools/install`.
2. Clone this repository.
3. Build the executable:
```
cd yb_stats
cargo build
```
The executable should be available in the target/debug directory.

Warning: alpha version, tested and built on OSX 12.1. 
Testing and feedback welcome!