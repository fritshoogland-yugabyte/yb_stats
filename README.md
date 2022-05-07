# yb_stats

yb_stats is a utility to extract detailed runtime data data from a YugabyteDB cluster for performance and troubleshooting purposes.  
It functions in two modes: 
- ad-hoc mode: begin and end capturing of performance (metric) data for performance analysis, which does not store any data (called without the `--snapshot` switch).
- snapshot mode: full capturing of all YugabyteDB cluster metadata and metrics for performance and troubleshooting analysis. The data is stored in CSV files in a directory in the current working directory.

The function of the CSV snapshot is to create an overview of the current situation as completely as possible to be able to work based on factual information and rule out guessing.

In ad-hoc mode the following data is gathered to show totals and the difference after pressing enter:
- metrics (master and tserver /metrics data, YCQL /metrics data and YSQL /metrics data).
- statements (YSQL /statements data).
- node_exporter /metrics data (non-YugabyteDB data).

In snapshot mode (`--snapshot`) the following data is gathered and stored in a CSV snapshot:
- metrics (master and tserver /metrics data, YCQL /metrics data and YSQL /metrics data).
- statements (YSQL /statements data).
- node_exporter /metrics data (non-YugabyteDB data).  
 
Plus:
- version via the versions endpoints (/api/v1/version)
- gflags (/varz)
- logging (/logs). Caveat: only the last 1M of logs is available via this endpoint.
- memtrackers (/mem-trackers)
- threads (/threadz)

In order to conveniently view existing snapshots performance data, use the `--snapshot-diff` switch. In order to make using different snapshots more easy, use the `--snapshot-comment` switch when creating a snapshot.

For both ad-hoc and snapshot modes for displaying data (`--snapshot-diff`), a number of options exist to filter, to add non-counter (gauge) statistics and to increase the detail of the statistics (by default YugabyteDB table and tablet statistics are summed by statistic name for the whole server in order to give a better overview, enabling detail level shows the statistics by actual source):
- `--gauges-enable`: add gauges (absolute number statistics) to the overview.
- `--details-enable`: split out statistics to their original metric source, instead of summarizing them for a server, or show data that is considered to be too detailed or not directly related (node_exporter).
- `--hostname-match`: regex filter to include or exclude based on hostname.
- `--stat-name-match`: regex filter to include or exclude based on the statistic name.
- `--table-name-match`: regex filter to include or exclude based on the table name (for table and tablets data only).

For snapshots, the additional gathered non-metric data can be viewed for a single snapshot using the following flags:
- `--print-version`: requires a single snapshot number as argument, and prints the versions that are gathered.
- `--print-gflags`: requires a single snapshot number as argument, and prints the gflags that are gathered.
- `--print-threads`: requires a single snapshot number as argument, and prints the thread information that is captured.
- `--print-memtrackers`: requires a single snapshot number as argument, and prints the mem-trackers information that is captured.
- `--print-log`: requires a single snapshot number as argument, and prints the loglines that are gathered.  
For `--print-log` specific, another flag can be used to filter the log rows:
- `--log-severity`: by default this filter is set to 'WEF' (Warning, Error, Fail), and thus will not show the I (Informal) lines.

# Usage
For data gathering, yb_stats requires to be provded the hostnames or ip addresses, and the port numbers if these are non-default. Hostnames and ports are provided using separate switches: `--hosts` and `--ports`.
Once hostnames or ports have been specified, these will be stored in the current working directory in a file called `.env`, which allows yb_stats to use the hostnames and ports without requiring to specify them again. 

This is how hostnames or ip addresses are specified:
```
./target/release/yb_stats --hosts 192.168.66.80,192.168.66.81,192.168.66.82
```
yb_stats will collect statistics from the ports 7000, 9000, 12000, 13000 and 9300 by default. If this list needs to be changed, you can specify the required ports list using the `-p` or `--ports` switch, for example:
```
./target/release/yb_stats --ports 9000,13001
```

## Online performance data display alias ad-hoc mode
For online performance data display (metric and statements data only), simply do not provide any further switch:
```
./target/release/yb_stats
```
This will capture the metric and statements endpoint data in memory, and then display:
```
Begin metrics snapshot created, press enter to create end snapshot for difference calculation.
```
Now perform any action that needs to be measured in YugabyteDB, and then press enter.

This will display the difference of the counters only, and provide all table and tablet level statistics summed per host.

## Gathering a snapshot
For gathering a snapshot (which collects all data), add the --snapshot switch. Optionally add a comment (useful for automated testing):
```
./target/release/yb_stats --snapshot --snapshot-comment "this is the first snapshot"
snapshot number 0
```

## Using snapshot data
Once snapshots are captured, they are stored in the current working directory in a directory called 'yb_stats.snapshots'. Inside this directory, there is a file 'snapshot.index', which is a CSV file which contains snapshot number, timestamp, comment.
The snapshot data is stored in a directory with a number, which corresponds with the snapshot number. Inside the snapshot number directory, there are CSV files with all the data.
- Because yb_stats works from the current working directory, it can be used for several projects simply by using it in another directory.
- Because all the data is common UTF8 data, it can be zipped/tarred/etc. and sent to someone else for investigation.
- Using UTF8 CSV data should allow the data to be used on any platform and OS, and do not suffer from any cross platform or OS issues.

## Display switches and filters
### Gauges
By default, statistics which are defined as gauges are not shown. An example of such a statistic is absolute memory usage. To see gauge statistics, add the `--gauges-enable` switch.
Gauges are shown different from counters:
```
192.168.66.80:7000   server   tcmalloc_current_total_thread_cache_bytes                                      4489808 bytes           +90080
192.168.66.80:7000   server   tcmalloc_max_total_thread_cache_bytes                                         33554432 bytes               +0
192.168.66.80:7000   server   tcmalloc_pageheap_free_bytes                                                   3268608 bytes          +647168
192.168.66.80:7000   server   tcmalloc_pageheap_unmapped_bytes                                              33488896 bytes          -696320
192.168.66.80:7000   server   tcp_bytes_received                                                                 424 bytes          388.991 /s
192.168.66.80:7000   server   tcp_bytes_sent                                                                     170 bytes          155.963 /s
```
tcp_bytes_received and tcp_bytes_sent are counters, and therefore the amount in the fourth column is the difference between the second and the first snapshot value. The sixth column shows the amount of the difference divided by the total time between the two snapshots.  
The tcmalloc statistics are gauges (absolute values, not counters), and therefore it does not make sense to show the difference between the end and begin snapshot value, the only sensible thing to show is the absolute value, which is the value of the second snapshot. 
It also doesn't make sense to divide the amount over time, therefore the sixth column shows the difference from the second snapshot value with the first one.

### Details
By default, table and tablet statistics are summed per hostname-port combination to try to reduce output clutter as much as possible. However sometimes you want to see the data per table and tablet. This is done using the `--details-enable` switch.

### Filters
#### --hostname-match
In a lot of cases, you might want to filter out data that is not needed for your analysis. A common filter is only filter the tserver and YSQL endpoints, and thus leaving out the master data:
`--hostname-match '(9000|13000)'`.  
Please mind this works for online performance data display, as well as looking at snapshot data, including showing version, memtrackers, log and threads data.
#### --stat-name-match
A very common case is to filter out some of the data that is displayed by its name. For example to filter out the statistics for the amount of bytes sent and received: `--stat-name-match tcp_bytes`.  
The --stat-name-match switch can also be used to filter memtrackers (id). 
#### --table-name-match
When `--details-enable` is used, a lot of extra lines are shown. In order to reduce it, the `--table-name-match` switch can be used to filter on a table regex.

# Output

## value statistics
```
./target/release/yb_stats
Begin metrics snapshot created, press enter to create end snapshot for difference calculation.

192.168.66.80:7000   server   cpu_utime                                                                           10 ms               3.210 /s
192.168.66.80:7000   server   involuntary_context_switches                                                         1 csws             0.321 /s
192.168.66.80:7000   server   rpc_inbound_calls_created                                                            6 req              1.926 /s
192.168.66.80:7000   server   server_uptime_ms                                                                  3114 ms             999.679 /s
192.168.66.80:7000   server   service_request_bytes_yb_consensus_ConsensusService_UpdateConsensus               1248 bytes          400.642 /s
192.168.66.80:7000   server   service_response_bytes_yb_consensus_ConsensusService_UpdateConsensus               510 bytes          163.724 /s
192.168.66.80:7000   server   tcp_bytes_received                                                                1272 bytes          408.347 /s
192.168.66.80:7000   server   tcp_bytes_sent                                                                     510 bytes          163.724 /s
192.168.66.80:7000   server   threads_started                                                                      1 threads           0.321 /s
192.168.66.80:7000   server   threads_started_thread_pool                                                          1 threads           0.321 /s
...snipped 
```
These are 'value' statistics. 'value' statistics contain a statistic name and a value with the statistic.
- The first column shows the hostname:port number endpoint specification.
- The second column shows the metric type (cluster, server, table, tablet).
- The third column shows the name of the statistic.
- The fourth column shows the difference of the value in the statistic between the first and second snapshot.
- The fifth column shows the unit of the measured number. Common units are bytes and ms (milliseconds), but others exist, such as req (requests) or csws (context switches). A '?' is shown if the statistic unit is currently unknown.
- The sixth column shows the difference of the value in the statistic between the first and second snapshot, divided by the time between the two snapshots.
The statistics are ordered by hostname-portnumber, metric type, id.

## countsum statistics
The next section are countsum statistics. 'countsum' statistics contain a value for the count of occurences and a value for the sum of data that the statistic is collecting. This is mostly time (mostly in us, microseconds) but can also be something else (like bytes):
```
...snipped
192.168.66.82:9000   server   dns_resolve_latency_during_init_proxy                                                1                  0.322 /s avg:         1 tot:               1 us
192.168.66.82:9000   server   handler_latency_outbound_call_queue_time                                            69                 22.186 /s avg:         0 tot:               0 us
192.168.66.82:9000   server   handler_latency_outbound_call_send_time                                             69                 22.186 /s avg:         0 tot:               0 us
192.168.66.82:9000   server   handler_latency_outbound_call_time_to_response                                      69                 22.186 /s avg:       826 tot:           57000 us
192.168.66.82:9000   server   handler_latency_yb_consensus_ConsensusService_UpdateConsensus                       71                 22.830 /s avg:       147 tot:           10497 us
192.168.66.82:9000   server   rpc_incoming_queue_time                                                             71                 22.830 /s avg:       111 tot:            7916 us
192.168.66.82:9000   table    log_append_latency                                                                  12                  3.859 /s avg:        20 tot:             248 us
192.168.66.82:9000   table    log_entry_batches_per_group                                                         12                  3.859 /s avg:         1 tot:              12 req
192.168.66.82:9000   table    log_group_commit_latency                                                            12                  3.859 /s avg:       505 tot:            6064 us
192.168.66.82:9000   table    log_sync_latency                                                                    12                  3.859 /s avg:       470 tot:            5644 us
```
- The first column is the hostname:port number endpoint specification.
- The second column shows the metric type (cluster, server, table, tablet).
- The third column shows the name of the statistic.
- The fourth column shows the difference between the first and second snapshot for the total_count statistic, which counts the number of occurences of the statistic.
- The fifth column shows the difference between the first and second snapshot for the total_count statistic, divided by the time between the two snapshots.
- The eighth column shows the difference between the first and second snapshot for the total_sum statistic, divided by the difference between the total_count statistics, to get the average amount of time per occasion of the statistic.
- The tenth column shows the difference between the first and the second snapshot for the total_sum statistic to get the total amount of time measured by this statistic.
- The eleventh column shows the unit of the total_sum statistic.

## countsumrows statistics
The optional next section are countsumrows statistics. 'countsumrows' statistics are unique to YSQL and contain: a value for the count of occurences, a sum about the data that the statistic is collecting, which is time (in ms, milliseconds), and rows, which are the number of rows that are processed by the topic about which the statistic is collecting information:
```
...snipped
192.168.66.80:13000  handler_latency_yb_ysqlserver_SQLProcessor_CatalogCacheMisses                       29 avg:           0.000 tot:           0.000 ms, avg:               1 tot:              29 rows
192.168.66.80:13000  handler_latency_yb_ysqlserver_SQLProcessor_InsertStmt                                1 avg:           0.079 tot:           0.079 ms, avg:               1 tot:               1 rows
192.168.66.80:13000  handler_latency_yb_ysqlserver_SQLProcessor_RollbackStmt                              1 avg:           0.008 tot:           0.008 ms, avg:               0 tot:               0 rows
192.168.66.80:13000  handler_latency_yb_ysqlserver_SQLProcessor_SelectStmt                                1 avg:          13.539 tot:          13.539 ms, avg:               1 tot:               1 rows
192.168.66.80:13000  handler_latency_yb_ysqlserver_SQLProcessor_SingleShardTransactions                   2 avg:           6.809 tot:          13.618 ms, avg:               1 tot:               2 rows
192.168.66.80:13000  handler_latency_yb_ysqlserver_SQLProcessor_Single_Shard_Transactions                 2 avg:           6.809 tot:          13.618 ms, avg:               1 tot:               2 rows
```
- The first column is the hostname:port number endpoint specification.
- The second columns shows the statistic.
- The third column shows the number of times the statistic was triggered.
- The fifth column shows the average time per occasion.
- The seventh column shows the total time for all occasions.
- The eighth column shows the unit of the time, ms (milliseconds).
- The tenth column shows the average number of rows per occasion.
- The twelfth column shows the total amount of rows.
 
CatalogCacheMisses
- Please mind the CatalogCacheMisses statistic currently does not measure time (sum): this is an item on the todo list of development.
- A backend that is initialized as part of logon does create a catalogcache in its heap. CatalogCacheMisses does currently not count these in the statistic.

## statement statistics
The optional next section are statement statistics. 'statement' statistics are unique to YSQL and are the externalisation of the pg_stat_statement statistics.
```
...snipped
192.168.66.80:13000           1 avg:          42.422 tot:          42.422 ms avg:          0 tot:          0 rows: create or replace procedure ybio.insert(   p_conf
192.168.66.80:13000           1 avg:          20.807 tot:          20.807 ms avg:          0 tot:          0 rows: create or replace procedure ybio.remove ( p_config
192.168.66.80:13000           1 avg:          24.174 tot:          24.174 ms avg:          0 tot:          0 rows: create or replace procedure ybio.run( p_config_id
192.168.66.80:13000           1 avg:          35.756 tot:          35.756 ms avg:          0 tot:          0 rows: create or replace procedure ybio.setup ( p_config_
192.168.66.80:13000           1 avg:          17.684 tot:          17.684 ms avg:          0 tot:          0 rows: create schema ybio
192.168.66.80:13000           1 avg:         632.546 tot:         632.546 ms avg:          0 tot:          0 rows: create table ybio.config (  id 				    serial   p
192.168.66.80:13000           1 avg:        6339.403 tot:        6339.403 ms avg:          0 tot:          0 rows: create table ybio.results (  config_id
192.168.66.80:13000           1 avg:         117.132 tot:         117.132 ms avg:          0 tot:          0 rows: create view ybio.results_overview asselect run_t
192.168.66.80:13000           1 avg:          59.842 tot:          59.842 ms avg:          0 tot:          0 rows: do $$declare  orafce_available int;  orafce_ins
192.168.66.80:13000           1 avg:        1267.262 tot:        1267.262 ms avg:          0 tot:          0 rows: drop schema if exists ybio cascade
```
- The first column is the hostname:port endpoint specification.
- The second column shows the number of calls for the query, which is calculated as the difference between the calls figure of the second snapshot and the first snapshot.
- The fourth column shows the average amount of time for the given statement, calculated as the second snapshot time minus the first snapshot time for this statement, divided by the number of calls.
- The sixth column shows the total amount of time for the given statement.
- The seventh column shows the unit of the time: ms (milliseconds).
- The ninth column shows the average amount of rows for the given statement.
- The eleventh column shows the total amount of rows for the given statement.
- From the thirteenth column on the statement is shown.

## node_exporter statistics
When a node_exporter endpoint is found, it is parsed, and displayed or saved. This is how that looks like:
```
...snipped
192.168.66.80:9300   counter  node_context_switches_total                                                       3821.000000         636.833 /s
192.168.66.80:9300   counter  node_cpu_seconds_total_idle                                                         11.970000           1.995 /s
192.168.66.80:9300   counter  node_cpu_seconds_total_irq                                                           0.040000           0.007 /s
192.168.66.80:9300   counter  node_cpu_seconds_total_softirq                                                       0.010000           0.002 /s
192.168.66.80:9300   counter  node_cpu_seconds_total_system                                                        0.040000           0.007 /s
192.168.66.80:9300   counter  node_cpu_seconds_total_user                                                          0.040000           0.007 /s
192.168.66.80:9300   counter  node_disk_io_time_seconds_total_sda                                                  0.004000           0.001 /s
192.168.66.80:9300   counter  node_disk_io_time_weighted_seconds_total_sda                                         0.004000           0.001 /s
192.168.66.80:9300   counter  node_disk_write_time_seconds_total_sda                                               0.003000           0.001 /s
192.168.66.80:9300   counter  node_disk_writes_completed_total_sda                                                 3.000000           0.500 /s
192.168.66.80:9300   counter  node_disk_written_bytes_total_sda                                                 1536.000000         256.000 /s
```
- The first column is the hostname:port endpoint specification.
- The second column is the specifier for counter or gauge. By default, gauges are not displayed.
- The third column is the node_exporter name, including labels.
- The fourth column is the value difference as second measurement minus first measurement. The unit of the measurement is in the third column/node_exporter name.
- The fifth column is the value difference divided by the time in the snapshot, to express the value difference as a per second value.

In order to make the statistics as useful as possible, some additional actions have been performed on the node_exporter data:
- All node_exporter exported data is preserved, so no data is gone.
- By default, no 'detail' data is shown. 
- Some data is marked as 'detail' because the data source are measurements about node_exporter itself, which means it's not directly useful for YugabyteDB or OS investigations.
- Some data is grouped in order to make it easier to use (node_cpu_seconds, node_schedstat, node_softnet). The non-grouped data is available as detail data.

# Examples
## Investigate CPU usage
Are the servers busy?
```
./target/release/yb_stats --stat-name-match '(cpu|context_switches)'
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
./target/debug/yb_stats --stat-name-match '(tcmalloc|generic|mem_tracker)' --gauges-enable
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
./target/release/yb_stats --stat-name-match tcp
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
./target/release/yb_stats --stat-name-match '^log'
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
./target/release/yb_stats --stat-name-match sst_files --gauges-enable --hostname-match 9000
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
./target/release/yb_stats --stat-name-match sst_files --gauges-enable --details-enable  --hostname-match 9000
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
./target/release/yb_stats --stat-name-match '(log_append_latency|log_sync_latency|rocksdb_sst_read_micros|rocksdb_write_raw_blocks)'
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
- log_append_latency: the number of and time spent adding the changes to the WAL (via a buffered writev() call). (older versions perform two write calls per WAL entry: https://github.com/yugabyte/yugabyte-db/issues/11035)
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