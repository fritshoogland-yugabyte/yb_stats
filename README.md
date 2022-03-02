# yb_stats

This is a utility to extract the metrics for the master and tablet servers in a YugebyteDB cluster. 
It uses the metric endpoints (not the prometheus-metrics endpoints, these do not contain all data, although for what is provided these do contain exactly the same data, but in prometheus format).
The metric endpoints are available at the following places with default settings:
- HOSTNAME:7000/metrics for the master server
- HOSTNAME:9000/metrics for the tablet server
- HOSTNAME:12000/metrics for the YCQL (cassandra API) server
- HOSTNAME:13000/metrics for the YSQL (postgres API) server
- HOSTNAME:13000/statements for the YSQL (postgres API) server

The main types of data are 'values', 'countsum' and 'countsumrows', 'statements' type statistics.
- The 'values' type consists of a statistic name and a value. The value can be time, bytes, a number of occurences, etc. Also the value can be a counter or a gauge (absolute number).
- The 'countsum' type consists of a statistic name and total_count (number of times the statistic is triggered), total_sum (total time in microseconds), which are both counters. The statistics also includes min, mean, max and several percentiles. These are absolute values (obviously). 
- The 'countsumrows' type consists of a statistic name and count (the number of times the statistic is triggered), sum (total time in millisecons) and rows (amount of rows returned from the statistic topic). The statistics are counters.
- The 'statements' type consists of the query, together with common pg_stat_statements timing information.

The goal of yb_stats is to capture this information and show it or store it, to learn and help with performance investigations.
Another goal is to make this a support package (hence the CSV format), which allows a client to capture all current available information and transport it to Yugabyte for review.

There are a lot of statistics, and a cluster of machines means the statistics multiply by the number of nodes in the cluster.
Therefore, the tools allows filtering data via regexes, to be ultimately flexible.

# Usage
```
yb_stats 0.5.0

USAGE:
    yb_stats [FLAGS] [OPTIONS]

FLAGS:
    -d, --details-enable    boolean (set to enable) to report for each table or tablet individually
    -g, --gauges-enable     boolean (set to enable) to add statistics that are not counters
    -h, --help              Prints help information
        --snapshot          boolean (set to enable) to perform a snapshot of the statistics, stored as CSV files in
                            yb_stats.snapshots
        --snapshot-diff     this lists the snapshots, and allows you to select a begin and end snapshot for a diff
                            report
    -V, --version           Prints version information

OPTIONS:
        --hostname-match <hostname-match>
            regex to select hostnames or ports (so you can select master or tserver by port number) [default: .*]

        --log-severity <log-severity>
            log data severity to include: default: WEF, optional: I

    -m, --metric-sources <metric-sources>
            all metric endpoints to be used, a metric endpoint is a hostname or ip address with colon and port number,
            comma separated [default: 192.168.66.80:7000,192.168.66.81:7000,192.168.66.82:7000]
        --print-log <print-log>                    print log data for the given snapshot [default: -1]
        --print-memtrackers <print-memtrackers>    print memtrackers data for the given snapshot [default: -1]
        --snapshot-comment <snapshot-comment>
            comment to be added with the snapshot, to make review or use more easy [default: ]

    -s, --stat-name-match <stat-name-match>        regex to select specific statistic names [default: .*]
    -t, --table-name-match <table-name-match>
            regex to select specific table names (only sensible with --details-enable, default mode adds the statistics
            for all tables) [default: .*]
```
The most important switch is `-m` or `--metric-sources`, which allows you to specify a comma-separated list of hostname:port combinations. 
Yes, this is quite a lot of work for even a modest cluster; to capture the master and tservers in my lab my setting is: `yb_stats -m 192.168.66.80:7000,192.168.66.80:9000,192.168.66.80:13000,192.168.66.81:7000,192.168.66.81:9000,192.168.66.81:13000,192.168.66.82:7000,192.168.66.82:9000,192.168.66.82:13000`. 
The reason for doing it in this way is that there is no view/URL/endpoint in the cluster that can consistenly be used to query the ip addresses to be used as an administrator.
One thing which might be a useful addition is to create a settings file that allows storing the hostnames, so you reuse that.

The yb_stats has five modes:
- STDOUT mode (default): when invoked, it gathers data to take as the begin situation, and then displays `Begin metrics snapshot created, press enter to create end snapshot for difference calculation.`. If you press enter, yb_stats gathers data again, and shows any statistic that has changed since the begin situation.
- Snapshot mode: when the `--snapshot` flag is specified, yb_stats gathers the data from the YugabyteDB cluster, reads the `yb_stats.snapshots/snapshot.index` file in the current directory, determines the highest snapshot number, increases it by one, and adds that to the file. If the file or directory doesn't exist, it tries to create it, and begins with snapshot number 0. It then creates a directory with the snapshot number in the `yb_stats.snapshots` directory, and writes the values and latencies statistics to files with that name.
- Snapshot-diff mode: when the `--snapshot-diff` flag is specified, yb_stats reads the `yb_stats.snapshots/snapshot.index` file for available snapshots, and displays the snapshot information, and asks for a begin and an end snapshot. When these have been specified, a diff is run between these snapshots.
- Memtrackers mode: when the `--print-memtrackers` flag is specified, yb_stats reads the `yb_stats.snapshots/snapshot.index` file for available snapshots, and displays the memtrackers snapshot data for the snapshot number given as the argument. Please mind the hostname (`--hostnane-match`) and stat-name (`--stat-name-match`) filters (see below) can be used for the hosts and the memtracker id (name). The default value is `-1`, which means not to print memtrackers data.
- Loglines mode: when the `--print-log` flag is specified, yb_stats reads the `yb_stats.snapshots/snapshot.index` file for the available snapshots, and displays the loglines for the snapshot number given as argument. Please mind the hostname (`--hostname-match`) and log severity letter (`--log-severity`) filters can be used to include or exclude hosts and lines with a certain severity. Standard severity filter is `WEF`: (W)arning, (E)rror and (F)ailure. (I)nformal messages are not printed by default, but can be enabled. Please mind at current this is pretty simple, and just prints all loglines that it found, it does NOT filter lines to show only the log lines between the chosen snapshot and the snapshot before that.

Even modest changes do generate lots of statistic differences. Therefore, by default yb_stats does not specify statistics per table or tablet, but adds these together per server (per hostname:port combination). If you want to see the statistics per table and tablet, specify the `--details-enable` switch.

By default, the gauge type statistics are not shown. However, there can be reasons you want to see them. This can be done using the `--gauges-enable` switch.

For investigations, you might be interested in a subset of the data. yb_stats always gathers all (non zero) statistics, but you might not want to see all the gathered data. Therefore, the output can be filtered using regexes on multiple levels:
- hostname: `--hostname-match`: one practical example is to be able to select the masters or the tservers only by specifying '7000' or '9000' as a filter.
- statisticname: `--stat-name-match`: if you know the names of one or more statistics, you can filter for their names.
- tablename: `--table-name-match`: this only makes sense if you use `--details-enable` to have the table names be printed. If you did, this allows you to filter only for the tables you are interested in.

# Output

## value statistics
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:7000,192.168.66.80:9000,192.168.66.81:7000,192.168.66.81:9000,192.168.66.82:7000,192.168.66.82:9000
Begin metrics snapshot created, press enter to create end snapshot for difference calculation.

192.168.66.80:7000   server   cpu_utime                                                                            6 ms               5.085/s
192.168.66.80:7000   server   rpc_inbound_calls_created                                                            2 req              1.695/s
192.168.66.80:7000   server   server_uptime_ms                                                                  1179 ms             999.153/s
192.168.66.80:7000   server   service_request_bytes_yb_consensus_ConsensusService_UpdateConsensus                416 bytes          352.542/s
192.168.66.80:7000   server   service_response_bytes_yb_consensus_ConsensusService_UpdateConsensus               170 bytes          144.068/s
192.168.66.80:7000   server   tcp_bytes_received                                                                 424 bytes          359.322/s
192.168.66.80:7000   server   tcp_bytes_sent                                                                     170 bytes          144.068/s
192.168.66.80:7000   server   voluntary_context_switches                                                          70 csws            59.322/s
192.168.66.80:9000   server   cpu_stime                                                                            2 ms               1.696/s
192.168.66.80:9000   server   cpu_utime                                                                           17 ms              14.419/s
192.168.66.80:9000   server   proxy_request_bytes_yb_consensus_ConsensusService_UpdateConsensus                 2520 bytes         2137.405/s
192.168.66.80:9000   server   proxy_request_bytes_yb_master_MasterService_TSHeartbeat                            156 bytes          132.316/s
...snipped 
```
These are 'value' statistics. 'value' statistics contain a statistic name and a value with the statistic.
- The first column shows the hostname:port number endpoint specification.
- The second column shows the metric type (cluster, server, table, tablet).
- The third column shows the name of the statistic.
- The fourth column shows the difference of the value in the statistic between the first and second snapshot.
- The fifth column shows the difference of the value in the statistic between the first and second snapshot, divided by the time between the two snapshots.
The statistics are ordered by hostname-portnumber, metric type, id.

## countsum statistics
The next section are countsum statistics. 'countsum' statistics contain a value for the count of occurences and a value for the sum of data that the statistic is collecting. This is mostly time (mostly in us, microseconds) but can also be something else (like bytes):
```
...snipped
192.168.66.82:7000   server   handler_latency_yb_master_MasterService_TSHeartbeat                                  4           3.387/s avg.time: 187       tot:             749 us
192.168.66.82:7000   server   rpc_incoming_queue_time                                                              4           3.387/s avg.time: 134       tot:             537 us
192.168.66.82:9000   server   handler_latency_outbound_call_queue_time                                            16          13.571/s avg.time: 0         tot:               0 us
192.168.66.82:9000   server   handler_latency_outbound_call_send_time                                             16          13.571/s avg.time: 0         tot:               0 us
192.168.66.82:9000   server   handler_latency_outbound_call_time_to_response                                      16          13.571/s avg.time: 500       tot:            8000 us
192.168.66.82:9000   server   handler_latency_yb_consensus_ConsensusService_UpdateConsensus                       12          10.178/s avg.time: 89        tot:            1072 us
192.168.66.82:9000   server   rpc_incoming_queue_time                                                             12          10.178/s avg.time: 121       tot:            1459 us
```
- The first column is the hostname:port number endpoint specification.
- The second column shows the metric type (cluster, server, table, tablet).
- The third column shows the name of the statistic.
- The fourth column shows the difference between the first and second snapshot for the total_count statistic, which counts the number of occurences of the statistic.
- The fifth column shows the difference between the first and second snapshot for the total_count statistic, divided by the time between the two snapshots.
- The sixth column show the difference between the first and second snapshot for the total_sum statistic, divided by the difference between the total_count statistics, to get the average amount of time per occasion of the statistic.
- The seventh column shows the difference between the first and the second snapshot for the total_sum statistic to get the total amount of time measured by this statistic.

## countsumrows statistics
The optional next section are countsumrows statistics. 'countsumrows' statistics are unique to YSQL and contain: a value for the count of occurences, a sum about the data that the statistic is collecting, which is time (in ms, milliseconds), and rows, which are the number of rows that are processed by the topic about which the statistic is collecting information:
```
...snipped
192.168.66.80:13000  handler_latency_yb_ysqlserver_SQLProcessor_CatalogCacheMisses                     3189 avg.time:               0 ms, avg.rows:            3280
192.168.66.80:13000  handler_latency_yb_ysqlserver_SQLProcessor_InsertStmt                                2 avg.time:              54 ms, avg.rows:               2
192.168.66.80:13000  handler_latency_yb_ysqlserver_SQLProcessor_OtherStmts                                8 avg.time:         2787939 ms, avg.rows:               0
192.168.66.80:13000  handler_latency_yb_ysqlserver_SQLProcessor_SelectStmt                                4 avg.time:            7051 ms, avg.rows:               4
192.168.66.80:13000  handler_latency_yb_ysqlserver_SQLProcessor_Single_Shard_Transactions                 8 avg.time:            3739 ms, avg.rows:               8
192.168.66.80:13000  handler_latency_yb_ysqlserver_SQLProcessor_Transactions                             15 avg.time:          494247 ms, avg.rows:               8
192.168.66.80:13000  handler_latency_yb_ysqlserver_SQLProcessor_UpdateStmt                                2 avg.time:             800 ms, avg.rows:               2
```
- The first column is the hostname:port number endpoint specification.
- The second columns shows the statistic.
- The third column shows the number of times the statistic was triggered.
- The amount between avg.time: and ms is the sum value divided by the number of times the statistic was triggered.
- The number after avg.rows: is the rows value divided by the number of times the statistic was triggered.  
 
CatalogCacheMisses
- Please mind the CatalogCacheMisses statistic currently does not measure time (sum): this is an item on the todo list of development.
- A backend that is initialized as part of logon does create a catalogcache in its heap. CatalogCacheMisses does currently not count these in the statistic.

## statement statistics
The optional next section are statement statistics. 'statement' statistics are unique to YSQL and are the externalisation of the pg_stat_statement statistics.
```
...snipped
192.168.66.80:13000           1 avg.time:          21.452 ms avg.rows:          0 : create or replace procedure ybio.insert( p_rows bi
192.168.66.80:13000           1 avg.time:          15.222 ms avg.rows:          0 : create or replace procedure ybio.remove ( p_config
192.168.66.80:13000           1 avg.time:          18.475 ms avg.rows:          0 : create or replace procedure ybio.run( p_config_id
192.168.66.80:13000           1 avg.time:          20.190 ms avg.rows:          0 : create or replace procedure ybio.setup ( p_config_
192.168.66.80:13000           1 avg.time:          10.476 ms avg.rows:          0 : create schema ybio
192.168.66.80:13000           1 avg.time:         453.551 ms avg.rows:          0 : create table ybio.config (  id 				    serial   p
192.168.66.80:13000           1 avg.time:         180.096 ms avg.rows:          0 : create table ybio.results (  run_id
192.168.66.80:13000           1 avg.time:           2.749 ms avg.rows:          0 : do $$declare  orafce_available int;  orafce_ins
```
- The first column is the hostname:port endpoint specification.
- The second column shows the number of calls for the query, which is calculated as the difference between the calls figure of the second snapshot and the first snapshot.
- The number between the avg.time: and ms is the average query latency, calculated as the difference between the total_time figure of the second snapshot and the first snapshot, divided by the difference in calls from the second column. This value is in milliseconds (ms).
- The number after avg.rows: is the average number of rows returned by the query, calculated as the difference between the rows figure of the second snapshot and the first snapshot, divided by the difference in calls from the second column.

# Examples
## Investigate CPU usage
Are the servers busy?
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:7000,192.168.66.80:9000,192.168.66.81:7000,192.168.66.81:9000,192.168.66.82:7000,192.168.66.82:9000 --stat-name-match '(cpu|context_switches)'
Begin metrics snapshot created, press enter to create end snapshot for difference calculation.

192.168.66.80:7000   server   cpu_utime                                                                            8 ms               4.269/s
192.168.66.80:7000   server   voluntary_context_switches                                                         111 csws            59.232/s
192.168.66.80:9000   server   cpu_stime                                                                           10 ms               5.342/s
192.168.66.80:9000   server   cpu_utime                                                                           17 ms               9.081/s
192.168.66.80:9000   server   voluntary_context_switches                                                         520 csws           277.778/s
192.168.66.81:7000   server   cpu_stime                                                                            6 ms               3.203/s
192.168.66.81:7000   server   cpu_utime                                                                            2 ms               1.068/s
192.168.66.81:7000   server   involuntary_context_switches                                                         1 csws             0.534/s
192.168.66.81:7000   server   voluntary_context_switches                                                         117 csws            62.467/s
192.168.66.81:9000   server   cpu_stime                                                                            9 ms               4.803/s
192.168.66.81:9000   server   cpu_utime                                                                           19 ms              10.139/s
192.168.66.81:9000   server   voluntary_context_switches                                                         516 csws           275.347/s
192.168.66.82:7000   server   cpu_stime                                                                            6 ms               3.195/s
192.168.66.82:7000   server   cpu_utime                                                                            8 ms               4.260/s
192.168.66.82:7000   server   voluntary_context_switches                                                         201 csws           107.029/s
192.168.66.82:9000   server   cpu_stime                                                                            2 ms               1.063/s
192.168.66.82:9000   server   cpu_utime                                                                           25 ms              13.291/s
192.168.66.82:9000   server   voluntary_context_switches                                                         554 csws           294.524/s
```
The `--stat-name-match` switch filters on the statistic names. `cpu` matches the statistics cpu_utime and cpu_stime (amount of CPU time spent in user mode (cpu_utime) and system (kernel) mode (cpu_stime)), `context_switches` matches the statistics voluntary_context_switches (number of times a process could finish in it's assigned CPU slice) and involuntary_context_switches (number of times a process wasn't able to finish in it's assigned CPU slice). 
By looking at these statistic, the average amount of CPU time can be measured. 
The CPU time is in ms, milliseconds. 
This means that a value of 1000 per second means 1 CPU busy. 
The above usage of around 14 (the average amount added of cpu_stime and cpu_utime for 192.168.66.80:9000) means 0.014 of a single CPU. For the context switches, high amounts of involuntary context switches per second is an indication multiple processes are contending for CPU runtime.

## Investigate memory usage
How is memory used for a server?
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:7000,192.168.66.80:9000,192.168.66.81:7000,192.168.66.81:9000,192.168.66.82:7000,192.168.66.82:9000 --stat-name-match '(tcmalloc|generic|mem_tracker)' --gauges-enable
Begin metrics snapshot created, press enter to create end snapshot for difference calculation.

192.168.66.80:7000   server   generic_current_allocated_bytes                                               17198984 bytes            +1856
192.168.66.80:7000   server   generic_heap_size                                                            165421056 bytes               +0
192.168.66.80:7000   server   mem_tracker                                                                     200704 bytes               +0
192.168.66.80:7000   server   mem_tracker_Compressed_Read_Buffer_Receive                                     2954240 bytes               +0
192.168.66.80:7000   server   mem_tracker_Read_Buffer_Inbound_RPC_Receive                                    2954240 bytes               +0
192.168.66.80:7000   server   tcmalloc_current_total_thread_cache_bytes                                      4311168 bytes          +139728
192.168.66.80:7000   server   tcmalloc_max_total_thread_cache_bytes                                        268435456 bytes               +0
192.168.66.80:7000   server   tcmalloc_pageheap_free_bytes                                                 140247040 bytes          -114688
192.168.66.80:7000   tablet   mem_tracker                                                                     200704 bytes               +0
192.168.66.80:7000   tablet   mem_tracker_IntentsDB                                                           133120 bytes               +0
192.168.66.80:7000   tablet   mem_tracker_IntentsDB_MemTable                                                  133120 bytes               +0
192.168.66.80:7000   tablet   mem_tracker_RegularDB                                                            67584 bytes               +0
192.168.66.80:7000   tablet   mem_tracker_RegularDB_MemTable                                                   67584 bytes               +0
192.168.66.80:9000   server   generic_current_allocated_bytes                                               41923144 bytes            -8064
...snipped
```
The first thing to notice here is I added the `--gauges-enable` flag, because the memory statistics are not counters.
The statistics contain 3 different types of memory statistics, for which I created a regular expression to show them.
- generic refers to the total allocated heap size.
- tcmalloc refers to the tcmalloc managed area's and statistics nside the generic heap.
- mem_tracker refers to allocations inside tcmalloc memory which have been tracked by the mem_tracker framework.  
 
Please notice server based mem_tracker allocations, and the tablet based ones.
The last line shows the first statistic for the next process.

## Investigate network usage
How much network traffic is executed by the master and tablet servers?
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:7000,192.168.66.80:9000,192.168.66.81:7000,192.168.66.81:9000,192.168.66.82:7000,192.168.66.82:9000 --stat-name-match tcp
Begin metrics snapshot created, press enter to create end snapshot for difference calculation.

192.168.66.80:7000   server   tcp_bytes_received                                                                 424 bytes          336.241/s
192.168.66.80:7000   server   tcp_bytes_sent                                                                     170 bytes          134.814/s
192.168.66.80:9000   server   tcp_bytes_received                                                                3976 bytes         3155.556/s
192.168.66.80:9000   server   tcp_bytes_sent                                                                    3648 bytes         2895.238/s
192.168.66.81:7000   server   tcp_bytes_received                                                                 636 bytes          504.762/s
192.168.66.81:7000   server   tcp_bytes_sent                                                                     255 bytes          202.381/s
192.168.66.81:9000   server   tcp_bytes_received                                                                3895 bytes         3088.818/s
192.168.66.81:9000   server   tcp_bytes_sent                                                                    4185 bytes         3318.795/s
192.168.66.82:7000   server   tcp_bytes_received                                                                1640 bytes         1298.496/s
192.168.66.82:7000   server   tcp_bytes_sent                                                                    2512 bytes         1988.915/s
192.168.66.82:9000   server   tcp_bytes_received                                                                4186 bytes         3314.331/s
192.168.66.82:9000   server   tcp_bytes_sent                                                                    3729 bytes         2952.494/s
```
This shows the amount of bytes sent and received per process over the time in the snapshot. 

## Investigate (WAL) logging
What is happening for WAL logging?
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:7000,192.168.66.80:9000,192.168.66.81:7000,192.168.66.81:9000,192.168.66.82:7000,192.168.66.82:9000 --stat-name-match '^log'
Begin metrics snapshot created, press enter to create end snapshot for difference calculation.

192.168.66.80:9000   tablet   log_bytes_logged                                                                   176 bytes           21.646/s
192.168.66.81:9000   tablet   log_bytes_logged                                                                   176 bytes           21.643/s
192.168.66.82:9000   tablet   log_bytes_logged                                                                   180 bytes           22.140/s
192.168.66.80:9000   table    log_append_latency                                                                   2           0.246/s avg.time: 1406      tot:            2812 us
192.168.66.80:9000   table    log_entry_batches_per_group                                                          2           0.246/s avg.time: 1         tot:               2 req
192.168.66.80:9000   table    log_group_commit_latency                                                             2           0.246/s avg.time: 2616      tot:            5233 us
192.168.66.80:9000   table    log_sync_latency                                                                     2           0.246/s avg.time: 1096      tot:            2193 us
192.168.66.81:9000   table    log_append_latency                                                                   2           0.246/s avg.time: 30        tot:              61 us
192.168.66.81:9000   table    log_entry_batches_per_group                                                          2           0.246/s avg.time: 1         tot:               2 req
192.168.66.81:9000   table    log_group_commit_latency                                                             2           0.246/s avg.time: 1571      tot:            3143 us
192.168.66.81:9000   table    log_sync_latency                                                                     2           0.246/s avg.time: 1498      tot:            2996 us
192.168.66.82:9000   table    log_append_latency                                                                   2           0.246/s avg.time: 27        tot:              54 us
192.168.66.82:9000   table    log_entry_batches_per_group                                                          2           0.246/s avg.time: 1         tot:               2 req
192.168.66.82:9000   table    log_group_commit_latency                                                             2           0.246/s avg.time: 2014      tot:            4029 us
192.168.66.82:9000   table    log_sync_latency                                                                     2           0.246/s avg.time: 1938      tot:            3876 us
```
This shows statistics related to the YugabyteDB write ahead logging mechanism. Please notice most of the statistics are per table, except for the bytes (log_bytes_logged), which are accounted on the server level.

## Investigate number and size of sst files
How many sst files does the server have? And what is the size (which means compressed), and the uncompressed size of these?
Please mind a table can have zero sst files if all the data is in the memtable only and not flushed yet.
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:7000,192.168.66.80:9000,192.168.66.81:7000,192.168.66.81:9000,192.168.66.82:7000,192.168.66.82:9000 --stat-name-match sst_files --gauges-enable --hostname-match 9000
Begin metrics snapshot created, press enter to create end snapshot for difference calculation.

192.168.66.80:9000   tablet   rocksdb_current_version_num_sst_files                                                8 files               +0
192.168.66.80:9000   tablet   rocksdb_current_version_sst_files_size                                       992041573 bytes               +0
192.168.66.80:9000   tablet   rocksdb_current_version_sst_files_uncompressed_size                         1456980905 bytes               +0
192.168.66.81:9000   tablet   rocksdb_current_version_num_sst_files                                               12 files               +0
192.168.66.81:9000   tablet   rocksdb_current_version_sst_files_size                                       995005984 bytes               +0
192.168.66.81:9000   tablet   rocksdb_current_version_sst_files_uncompressed_size                         1460605799 bytes               +0
192.168.66.82:9000   tablet   rocksdb_current_version_num_sst_files                                               12 files               +0
192.168.66.82:9000   tablet   rocksdb_current_version_sst_files_size                                       994599640 bytes               +0
192.168.66.82:9000   tablet   rocksdb_current_version_sst_files_uncompressed_size                         1461586770 bytes               +0
```
Please mind I had to enable the `--gauges-enable` flag again. (officially these are counters, but these make more sense as gauge).
This is very fast and easy way to understand the amount and the sizes of the SST files.  

If you want to know more about a specific table(/tablet), you can use the `--details-enable` flag to show the statistics per table/tablet:
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:7000,192.168.66.80:9000,192.168.66.81:7000,192.168.66.81:9000,192.168.66.82:7000,192.168.66.82:9000 --stat-name-match sst_files --gauges-enable --details-enable  --hostname-match 9000
Begin metrics snapshot created, press enter to create end snapshot for difference calculation.

192.168.66.80:9000   tablet   3ba5f729fb1f0e7 system_postgres sequences_data                 rocksdb_current_version_num_sst_files                                                1 files               +0
192.168.66.80:9000   tablet   3ba5f729fb1f0e7 system_postgres sequences_data                 rocksdb_current_version_sst_files_size                                           66565 bytes               +0
192.168.66.80:9000   tablet   3ba5f729fb1f0e7 system_postgres sequences_data                 rocksdb_current_version_sst_files_uncompressed_size                              66773 bytes               +0
192.168.66.80:9000   tablet   a75aa293b4e061b yugabyte        benchmark_table                rocksdb_current_version_num_sst_files                                                2 files               +0
192.168.66.80:9000   tablet   a75aa293b4e061b yugabyte        benchmark_table                rocksdb_current_version_sst_files_size                                       328174660 bytes               +0
192.168.66.80:9000   tablet   a75aa293b4e061b yugabyte        benchmark_table                rocksdb_current_version_sst_files_uncompressed_size                          481972237 bytes               +0
192.168.66.80:9000   tablet   efd7118f32e0d37 yugabyte        config                         rocksdb_current_version_num_sst_files                                                1 files               +0
192.168.66.80:9000   tablet   efd7118f32e0d37 yugabyte        config                         rocksdb_current_version_sst_files_size                                           66796 bytes               +0
192.168.66.80:9000   tablet   efd7118f32e0d37 yugabyte        config                         rocksdb_current_version_sst_files_uncompressed_size                              67332 bytes               +0
192.168.66.80:9000   tablet   422e112629d3dfa yugabyte        benchmark_table                rocksdb_current_version_num_sst_files                                                2 files               +0
192.168.66.80:9000   tablet   422e112629d3dfa yugabyte        benchmark_table                rocksdb_current_version_sst_files_size                                       331407656 bytes               +0
192.168.66.80:9000   tablet   422e112629d3dfa yugabyte        benchmark_table                rocksdb_current_version_sst_files_uncompressed_size                          486494998 bytes               +0
192.168.66.80:9000   tablet   f51033d9b8ed29c yugabyte        benchmark_table                rocksdb_current_version_num_sst_files                                                2 files               +0
192.168.66.80:9000   tablet   f51033d9b8ed29c yugabyte        benchmark_table                rocksdb_current_version_sst_files_size                                       332325896 bytes               +0
192.168.66.80:9000   tablet   f51033d9b8ed29c yugabyte        benchmark_table                rocksdb_current_version_sst_files_uncompressed_size                          488379565 bytes               +0
192.168.66.81:9000   tablet   3ba5f729fb1f0e7 system_postgres sequences_data                 rocksdb_current_version_num_sst_files                                                1 files               +0
192.168.66.81:9000   tablet   3ba5f729fb1f0e7 system_postgres sequences_data                 rocksdb_current_version_sst_files_size                                           66565 bytes               +0
192.168.66.81:9000   tablet   3ba5f729fb1f0e7 system_postgres sequences_data                 rocksdb_current_version_sst_files_uncompressed_size                              66773 bytes               +0
192.168.66.81:9000   tablet   a75aa293b4e061b yugabyte        benchmark_table                rocksdb_current_version_num_sst_files                                                4 files               +0
192.168.66.81:9000   tablet   a75aa293b4e061b yugabyte        benchmark_table                rocksdb_current_version_sst_files_size                                       330375390 bytes               +0
192.168.66.81:9000   tablet   a75aa293b4e061b yugabyte        benchmark_table                rocksdb_current_version_sst_files_uncompressed_size                          485473077 bytes               +0
192.168.66.81:9000   tablet   efd7118f32e0d37 yugabyte        config                         rocksdb_current_version_num_sst_files                                                1 files               +0
192.168.66.81:9000   tablet   efd7118f32e0d37 yugabyte        config                         rocksdb_current_version_sst_files_size                                           66796 bytes               +0
192.168.66.81:9000   tablet   efd7118f32e0d37 yugabyte        config                         rocksdb_current_version_sst_files_uncompressed_size                              67332 bytes               +0
192.168.66.81:9000   tablet   422e112629d3dfa yugabyte        benchmark_table                rocksdb_current_version_num_sst_files                                                4 files               +0
192.168.66.81:9000   tablet   422e112629d3dfa yugabyte        benchmark_table                rocksdb_current_version_sst_files_size                                       335030701 bytes               +0
192.168.66.81:9000   tablet   422e112629d3dfa yugabyte        benchmark_table                rocksdb_current_version_sst_files_uncompressed_size                          491055899 bytes               +0
192.168.66.81:9000   tablet   f51033d9b8ed29c yugabyte        benchmark_table                rocksdb_current_version_num_sst_files                                                2 files               +0
192.168.66.81:9000   tablet   f51033d9b8ed29c yugabyte        benchmark_table                rocksdb_current_version_sst_files_size                                       329466532 bytes               +0
192.168.66.81:9000   tablet   f51033d9b8ed29c yugabyte        benchmark_table                rocksdb_current_version_sst_files_uncompressed_size                          483942718 bytes               +0
192.168.66.82:9000   tablet   3ba5f729fb1f0e7 system_postgres sequences_data                 rocksdb_current_version_num_sst_files                                                1 files               +0
192.168.66.82:9000   tablet   3ba5f729fb1f0e7 system_postgres sequences_data                 rocksdb_current_version_sst_files_size                                           66565 bytes               +0
192.168.66.82:9000   tablet   3ba5f729fb1f0e7 system_postgres sequences_data                 rocksdb_current_version_sst_files_uncompressed_size                              66773 bytes               +0
192.168.66.82:9000   tablet   a75aa293b4e061b yugabyte        benchmark_table                rocksdb_current_version_num_sst_files                                                4 files               +0
192.168.66.82:9000   tablet   a75aa293b4e061b yugabyte        benchmark_table                rocksdb_current_version_sst_files_size                                       332053412 bytes               +0
192.168.66.82:9000   tablet   a75aa293b4e061b yugabyte        benchmark_table                rocksdb_current_version_sst_files_uncompressed_size                          488401350 bytes               +0
192.168.66.82:9000   tablet   efd7118f32e0d37 yugabyte        config                         rocksdb_current_version_num_sst_files                                                1 files               +0
192.168.66.82:9000   tablet   efd7118f32e0d37 yugabyte        config                         rocksdb_current_version_sst_files_size                                           66796 bytes               +0
192.168.66.82:9000   tablet   efd7118f32e0d37 yugabyte        config                         rocksdb_current_version_sst_files_uncompressed_size                              67332 bytes               +0
192.168.66.82:9000   tablet   422e112629d3dfa yugabyte        benchmark_table                rocksdb_current_version_num_sst_files                                                3 files               +0
192.168.66.82:9000   tablet   422e112629d3dfa yugabyte        benchmark_table                rocksdb_current_version_sst_files_size                                       330458839 bytes               +0
192.168.66.82:9000   tablet   422e112629d3dfa yugabyte        benchmark_table                rocksdb_current_version_sst_files_uncompressed_size                          486059811 bytes               +0
192.168.66.82:9000   tablet   f51033d9b8ed29c yugabyte        benchmark_table                rocksdb_current_version_num_sst_files                                                3 files               +0
192.168.66.82:9000   tablet   f51033d9b8ed29c yugabyte        benchmark_table                rocksdb_current_version_sst_files_size                                       331954028 bytes               +0
192.168.66.82:9000   tablet   f51033d9b8ed29c yugabyte        benchmark_table                rocksdb_current_version_sst_files_uncompressed_size                          486991504 bytes               +0
```
With `--details-enable` all table and tablet data is shown. This is my lab cluster after a run of ybio. The data is ordered by the tablet id, so not all tablets might be grouped with the table. 
## Investigate physical IO and latencies
How much physical IO was performed, and how much did that take on average and overall?
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:7000,192.168.66.80:9000,192.168.66.81:7000,192.168.66.81:9000,192.168.66.82:7000,192.168.66.82:9000 --stat-name-match '(log_append_latency|log_sync_latency|rocksdb_sst_read_micros|rocksdb_write_raw_blocks)'
Begin metrics snapshot created, press enter to create end snapshot for difference calculation.

192.168.66.80:9000   table    log_append_latency                                                                 762          57.276/s avg.time: 36        tot:           28041 us
192.168.66.80:9000   table    log_sync_latency                                                                   761          57.201/s avg.time: 835       tot:          635686 us
192.168.66.80:9000   table    rocksdb_sst_read_micros                                                           5899         443.400/s avg.time: 96        tot:          571313 us
192.168.66.81:9000   table    log_append_latency                                                                 842          57.683/s avg.time: 63        tot:           53503 us
192.168.66.81:9000   table    log_sync_latency                                                                   842          57.683/s avg.time: 1467      tot:         1235801 us
192.168.66.81:9000   table    rocksdb_sst_read_micros                                                          21424        1467.699/s avg.time: 117       tot:         2507733 us
192.168.66.82:9000   table    log_append_latency                                                                 846          57.535/s avg.time: 36        tot:           31079 us
192.168.66.82:9000   table    log_sync_latency                                                                   846          57.535/s avg.time: 819       tot:          693134 us
192.168.66.82:9000   table    rocksdb_sst_read_micros                                                           7502         510.201/s avg.time: 62        tot:          468070 us
```
This shows the physical IO statistics:
- log_append_latency: the number of and time spent adding the changes to the WAL (via a buffer writev() call). (older versions perform two write calls per WAL entry: https://github.com/yugabyte/yugabyte-db/issues/11035)
- rocksdb_sst_read_micros: the number of and time spent reading (via a buffered pread64() call).
- rocksdb_write_raw_blocks: the number of and time spent writing to SST files (via a buffered write() call).
- log_sync_latency: this the number of and time spent in the function that performs fsync() of the WAL file. Mind the specific wording: not all calls to the function result in fsync() being called, which means that this statistic also measures running the function not performing the fsync() call, and thus will be much too positive, and not a valid average of time spent in the fsync() call. The total time is still a valid amount for tuning. See: https://github.com/yugabyte/yugabyte-db/issues/11039


# How to install
Currently, this is only available as source code, not as executable.  
However, it's easy to compile the tool:

1. Get rust: visit `https://www.rust-lang.org/tools/install`, and run the installation tool. (MacOS: just install; Linux/EL: yum install -y gcc openssl-devel)
2. Clone this repository. (git clone https://github.com/fritshoogland-yugabyte/yb_stats.git)
3. Build the executable:
```
cd yb_stats
cargo build --release
```
The executable should be available in the target/release directory.

Warning: alpha version, built on OSX 12.1, tested on OSX and linux.
Testing and feedback welcome!