#yb_stats

This is a utility to extract the metrics endpoints of the master and tablet servers in a YugebyteDB cluster. The tool remains the classification of:
- cluster
- server
- table
- tablet

And understands the two different metrics types:
- 'value type' (name, value)
- 'latency type' (name, total_count, min, mean, percentiles, max, total_sum)

For the 'value type', a classification of counter and gauges has been made (but is work in progress).

The goal of the tool is to provide a simple way to measure statistics in an ad-hoc way with no dependencies but existence of the tool and access to the metrics endpoint.

#usage
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % ./target/debug/yb_stats -h
yb_stats 0.1.0

USAGE:
    yb_stats [FLAGS] [OPTIONS]

FLAGS:
    -b, --begin-end-mode
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

The number of statistics is overwhelming. For that reason, two switches can be used to filter the output:

- `-t` table name match  
The table name match switch allows you to specify a regex to filter on a table name or table names.
- `-s` statistic name match  
The statistic name match switch allows you to specify a regex to filter on a statistic name or statistics names.

#output
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:9000,192.168.66.81:9000,192.168.66.82:9000
192.168.66.80:9000   server   yb.tabletserver -               -                              cpu_stime                                                                           20           9.281/s
192.168.66.80:9000   server   yb.tabletserver -               -                              cpu_utime                                                                           34          15.777/s
192.168.66.80:9000   server   yb.tabletserver -               -                              generic_current_allocated_bytes                                               45490864          -20912
192.168.66.80:9000   server   yb.tabletserver -               -                              hybrid_clock_hybrid_time                                               6722450989613010944     +8822345728
192.168.66.80:9000   server   yb.tabletserver -               -                              proxy_request_bytes_yb_consensus_ConsensusService_UpdateConsensus                 9109        4226.914/s
192.168.66.80:9000   server   yb.tabletserver -               -                              proxy_request_bytes_yb_master_MasterService_TSHeartbeat                            316         146.636/s
192.168.66.80:9000   server   yb.tabletserver -               -                              proxy_response_bytes_yb_consensus_ConsensusService_UpdateConsensus                3039        1410.209/s
192.168.66.80:9000   server   yb.tabletserver -               -                              proxy_response_bytes_yb_master_MasterService_TSHeartbeat                           934         433.411/s
192.168.66.80:9000   server   yb.tabletserver -               -                              rpc_inbound_calls_created                                                           44          20.418/s
192.168.66.80:9000   server   yb.tabletserver -               -                              rpc_outbound_calls_created                                                          45          20.882/s
192.168.66.80:9000   server   yb.tabletserver -               -                              server_uptime_ms                                                                  2154         999.536/s
192.168.66.80:9000   server   yb.tabletserver -               -                              service_request_bytes_yb_consensus_ConsensusService_UpdateConsensus               9086        4216.241/s
192.168.66.80:9000   server   yb.tabletserver -               -                              service_response_bytes_yb_consensus_ConsensusService_UpdateConsensus              3586        1664.037/s
192.168.66.80:9000   server   yb.tabletserver -               -                              tcmalloc_current_total_thread_cache_bytes                                     12825016         +100496
192.168.66.80:9000   server   yb.tabletserver -               -                              tcmalloc_pageheap_free_bytes                                                   5382144         -114688
192.168.66.80:9000   server   yb.tabletserver -               -                              tcp_bytes_received                                                               13830        6417.633/s
192.168.66.80:9000   server   yb.tabletserver -               -                              tcp_bytes_sent                                                                   13011        6037.587/s
192.168.66.80:9000   server   yb.tabletserver -               -                              voluntary_context_switches                                                        1003         465.429/s
192.168.66.80:9000   tablet   d08be22a4159b2e yugabyte        utl_file_dir_dirname_key       follower_lag_ms                                                                     16            -656
192.168.66.80:9000   tablet   b149af46c13bb92 system          transactions                   follower_lag_ms                                                                     79            -975
192.168.66.80:9000   tablet   570786352d51a35 system_postgres sequences_data                 follower_lag_ms                                                                     60             -21
192.168.66.80:9000   tablet   eb5fd34d6c75136 yugabyte        utl_file_dir                   follower_lag_ms                                                                    123             +41
```
- The first column shows the hostname:port number endpoint specification.
- The second column shows the metric type (cluster, server, table, tablet).
- The third column shows the 'id' of the type. This results in different types: for server, this is yb.tabletserver or yb.master. For a table, this is the unique identifier for a table, and for a tablet this is the UUID of the tablet. The last 15 characters are displayed.
- The fourth column is the namespace. For the server type, this is '-'.
- The fifth column is the table_name. For the server type, this is '-'.
- The sixth column is the statistic name.
- The seventh column is the difference between the previous and the current fetch for counter type statistics, and for gauge types this is the value of the current fetch.
- The eighth column is the difference between the previous and the current fetch divided by the time of the two fetches, to get an idea of rate of the statistic happening, and for gauge types it's the difference (positive or negative) between the previous and current fetch.
- The ninth column is unique to latency type, and shows the difference between the previous and current fetch divided by the difference of the total_sum statistic. This way the average latency over the period of the snapshot for that event is calculated.

#examples
## investigate CPU usage
Are the tablet servers busy?
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:9000,192.168.66.81:9000,192.168.66.82:9000 -s cpu
192.168.66.80:9000   server   yb.tabletserver -               -                              cpu_stime                                                                           20           9.320/s
192.168.66.80:9000   server   yb.tabletserver -               -                              cpu_utime                                                                           34          15.843/s
192.168.66.81:9000   server   yb.tabletserver -               -                              cpu_stime                                                                            6           2.796/s
192.168.66.81:9000   server   yb.tabletserver -               -                              cpu_utime                                                                           44          20.503/s
192.168.66.82:9000   server   yb.tabletserver -               -                              cpu_stime                                                                           14           6.527/s
192.168.66.82:9000   server   yb.tabletserver -               -                              cpu_utime                                                                           34          15.851/s
```
( this clears the screen, begin-end doesn't do that)
In my test cluster, the amount of CPU is cpu_stime (for kernel/system mode CPU) and cpu_utime (for user mode CPU) added together. In this case, it's roughly:
- 9+16=15
- 3+20=23
- 7+16=23
To match the above amount with CPU capacity, it has to be divided by 10 to match procentual to a CPU. This means the above figures mean 1.5% and 2.3% of CPU.

## investigate read IO
What happens during a read when blocks need to be read from disk?
```
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_add                                                            116          50.457/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_bytes_read                                                71361220    31040113.093/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_bytes_write                                                3790902     1648935.189/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_data_hit                                                       265         115.268/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_data_miss                                                      116          50.457/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_filter_hit                                                     788         342.758/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_hit                                                           1807         785.994/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_index_hit                                                      754         327.969/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_miss                                                           116          50.457/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_multi_touch_bytes_read                                    59506317    25883565.463/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_multi_touch_hit                                               1406         611.570/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_single_touch_add                                               116          50.457/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_single_touch_bytes_read                                   11854903     5156547.629/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_single_touch_bytes_write                                   3790902     1648935.189/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_block_cache_single_touch_hit                                               401         174.424/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_bloom_filter_checked                                                       788         342.758/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_bloom_filter_useful                                                        411         178.773/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_db_iter_bytes_read                                                      109105       47457.590/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_no_table_cache_iterators                                                   377         163.984/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_number_db_next                                                            1182         514.137/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_number_db_next_found                                                      1182         514.137/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_number_db_seek                                                             394         171.379/s
192.168.66.80:9000   tablet   42d2de5d33edf23 yugabyte        benchmark_table                rocksdb_number_db_seek_found                                                       394         171.379/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_add                                                            131          57.006/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_bytes_read                                                48484156    21098414.273/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_bytes_write                                                4282184     1863439.513/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_data_hit                                                       279         121.410/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_data_miss                                                      131          57.006/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_filter_hit                                                     415         180.592/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_hit                                                           1508         656.223/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_index_hit                                                      814         354.221/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_miss                                                           131          57.006/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_multi_touch_bytes_read                                    35446797    15425063.969/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_multi_touch_hit                                               1074         467.363/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_single_touch_add                                               131          57.006/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_single_touch_bytes_read                                   13037359     5673350.305/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_single_touch_bytes_write                                   4282184     1863439.513/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_block_cache_single_touch_hit                                               434         188.860/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_bloom_filter_checked                                                       415         180.592/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_bloom_filter_useful                                                          8           3.481/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_db_iter_bytes_read                                                      114920       50008.703/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_no_table_cache_iterators                                                   407         177.111/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_number_db_next                                                            1245         541.775/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_number_db_next_found                                                      1245         541.775/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_number_db_seek                                                             415         180.592/s
192.168.66.81:9000   tablet   d915a66f0b9cfae yugabyte        benchmark_table                rocksdb_number_db_seek_found                                                       415         180.592/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_add                                                            139          60.593/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_bytes_read                                                93187129    40622113.775/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_bytes_write                                                4542357     1980103.313/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_data_hit                                                       243         105.929/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_data_miss                                                      139          60.593/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_filter_hit                                                    1140         496.949/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_hit                                                           2115         921.970/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_index_hit                                                      732         319.093/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_miss                                                           139          60.593/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_multi_touch_bytes_read                                    81772361    35646190.497/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_multi_touch_hit                                               1731         754.577/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_single_touch_add                                               139          60.593/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_single_touch_bytes_read                                   11414768     4975923.278/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_single_touch_bytes_write                                   4542357     1980103.313/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_block_cache_single_touch_hit                                               384         167.393/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_bloom_filter_checked                                                      1140         496.949/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_bloom_filter_useful                                                        761         331.735/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_db_iter_bytes_read                                                      105288       45897.123/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_no_table_cache_iterators                                                   379         165.214/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_number_db_next                                                            1140         496.949/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_number_db_next_found                                                      1140         496.949/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_number_db_seek                                                             380         165.650/s
192.168.66.82:9000   tablet   ebffa6f308ab78b yugabyte        benchmark_table                rocksdb_number_db_seek_found                                                       380         165.650/s
192.168.66.80:9000   table    00000000000440a yugabyte        benchmark_table                rocksdb_read_block_get_micros                                                      116           5.046/s avg:       122 us
192.168.66.80:9000   table    00000000000440a yugabyte        benchmark_table                rocksdb_sst_read_micros                                                            116           5.046/s avg:       104 us
192.168.66.81:9000   table    00000000000440a yugabyte        benchmark_table                rocksdb_read_block_get_micros                                                      131           5.701/s avg:        33 us
192.168.66.81:9000   table    00000000000440a yugabyte        benchmark_table                rocksdb_sst_read_micros                                                            131           5.701/s avg:        14 us
192.168.66.82:9000   table    00000000000440a yugabyte        benchmark_table                rocksdb_read_block_get_micros                                                      139           6.059/s avg:        32 us
192.168.66.82:9000   table    00000000000440a yugabyte        benchmark_table                rocksdb_sst_read_micros                                                            139           6.059/s avg:        13 us
```
This is a lot of statistics, multiplied by the tablets over the tservers.

What happens when all blocks are satisfied from the cache for a table?
```
fritshoogland@MacBook-Pro-van-Frits yb_stats % target/debug/yb_stats -m 192.168.66.80:9000,192.168.66.81:9000,192.168.66.82:9000 -t benchmark_table -s rocksdb
192.168.66.80:9000   tablet   42364e002f53b82 yugabyte        benchmark_table                rocksdb_db_iter_bytes_read                                                      369898      170932.532/s
192.168.66.80:9000   tablet   42364e002f53b82 yugabyte        benchmark_table                rocksdb_number_db_next                                                            4007        1851.664/s
192.168.66.80:9000   tablet   42364e002f53b82 yugabyte        benchmark_table                rocksdb_number_db_next_found                                                      4007        1851.664/s
192.168.66.80:9000   tablet   42364e002f53b82 yugabyte        benchmark_table                rocksdb_number_db_seek                                                            1337         617.837/s
192.168.66.80:9000   tablet   42364e002f53b82 yugabyte        benchmark_table                rocksdb_number_db_seek_found                                                      1337         617.837/s
192.168.66.81:9000   tablet   ea0a6589830d51b yugabyte        benchmark_table                rocksdb_db_iter_bytes_read                                                      413384      191116.043/s
192.168.66.81:9000   tablet   ea0a6589830d51b yugabyte        benchmark_table                rocksdb_number_db_next                                                            4475        2068.886/s
192.168.66.81:9000   tablet   ea0a6589830d51b yugabyte        benchmark_table                rocksdb_number_db_next_found                                                      4475        2068.886/s
192.168.66.81:9000   tablet   ea0a6589830d51b yugabyte        benchmark_table                rocksdb_number_db_seek                                                            1492         689.783/s
192.168.66.81:9000   tablet   ea0a6589830d51b yugabyte        benchmark_table                rocksdb_number_db_seek_found                                                      1492         689.783/s
192.168.66.82:9000   tablet   ec776df2eeb5312 yugabyte        benchmark_table                rocksdb_db_iter_bytes_read                                                      379783      175825.463/s
192.168.66.82:9000   tablet   ec776df2eeb5312 yugabyte        benchmark_table                rocksdb_number_db_next                                                            4111        1903.241/s
192.168.66.82:9000   tablet   ec776df2eeb5312 yugabyte        benchmark_table                rocksdb_number_db_next_found                                                      4111        1903.241/s
192.168.66.82:9000   tablet   ec776df2eeb5312 yugabyte        benchmark_table                rocksdb_number_db_seek                                                            1372         635.185/s
192.168.66.82:9000   tablet   ec776df2eeb5312 yugabyte        benchmark_table                rocksdb_number_db_seek_found                                                      1372         635.185/s

```