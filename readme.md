### Running Environment
All the experiments are conducted using a machine with 100G memory. A-Tune-Online implements Bayesian optimization based on OpenBox, a toolkit for black-box optimization. 

The system and application versions are OpenEuler 22.03, MySQL 8.0.29, Redis 6.0.5, Kafka 3.5.0, UnixBench 5.1.3, Hadoop 3.2.3, and Spark 3.2.4.


### Main Directory Structure:

config.py: Macroscopic configuration file
space.py: Configuration parameter space
utils_mysql.py: Database monitoring class and Sysbench running class
tune_ssh.py: Server connection class
Advisor: Bayesian optimization implementation

### Execution Method:
1. Set the global configuration

    config.py: Macroscopic configuration file, configure the server (running mysql) address, username, password, client (running sysbench) address, username, password, and database port, username, password. 
    
    space.py: Configure the parameter space.

2. Execute

    Test_mysql_tunning.py: Main testing program, task arrangement can be changed at the end of the program
    
    To use contextBO with task switch detection, run `python Test_mysql_tunning.py --opt contextBO_tsd`
    
    To use contextBO without task switch detection, run `python Test_mysql_tunning.py --opt contextBO`
    
    To turn on the warm-up, add `--warm_start_flag`; 
    To turn on the safe-guarantee, add `--safe_flag`.


### Config Space:
#### Mysql

| name                        | scope                                   | default    |
|:--------------------------------:|:--------------------------------------------:|:--------------:|
| innodb_buffer_pool_size        | [10307921510, 15618062894]                  | 13958643712  |
| innodb_thread_sleep_delay      | [0, 1000]                                   | 0            |
| innodb_thread_concurrency      | [0, 100]                                    | 0            |
| innodb_max_dirty_pages_pct_lwm | [0, 99]                                     | 0            |
| innodb_read_ahead_threshold    | [0, 64]                                     | 56           |
| innodb_adaptive_max_sleep_delay| [0, 1000000]                                | 150000       |
| thread_cache_size              | [0, 16384]                                  | 0            |
| innodb_adaptive_flushing_lwm   | [0, 70]                                     | 10           |
| innodb_adaptive_hash_index     | {'ON', 'OFF'}                                | 'ON'         |
| innodb_change_buffering        | {'none', 'inserts', 'deletes', 'changes', 'purges', 'all'} | 'all' |
| innodb_max_dirty_pages_pct     | [0, 99]                                     | 75           |
| max_heap_table_size            | [16384, 1073741824]                         | 16777216     |
| tmp_table_size                 | [1024, 1073741824]                          | 16777216     |


#### Redis
| name                        | scope                                   | default    |
|:-------------------------:|:------------------------------:|:--------------:|
| maxmemory-policy        | {'volatile-lru', 'allkeys-lru', 'volatile-lfu', 'allkeys-lfu', 'volatile-random', 'allkeys-random', 'volatile-ttl', 'noeviction'} | 'noeviction' |
| maxmemory-samples       | [1, 10]                       | 5            |
| maxmemory               | [0, 16384]                   | 0            |
| hash-max-ziplist-entries| [32, 4096]                    | 512          |
| hash-max-ziplist-value  | [8, 1024]                     | 64           |
| list-max-ziplist-size   | [-5, -1]                      | -2           |
| list-compress-depth     | [0, 5]                        | 0            |
| zset-max-ziplist-entries| [32, 4096]                    | 128          |
| zset-max-ziplist-value  | [16, 2048]                    | 64           |
| set-max-intset-entries  | [32, 4096]                    | 512          |
| activerehashing         | {'yes', 'no'}                  | 'yes'        |
| appendfsync             | {'always', 'everysec', 'no'}   | 'everysec'   |
| lua-time-limit          | [0, 10000]                    | 5000         |


#### Unix

| name                        | scope                                   | default    |
|:-------------------------------------:|:------------------------------------------------:|:--------------------------------:|
| fs.aio-max-nr                       | [102400, 10240000]                              | 102400                         |
| fs.file-max                         | [102400, 10240000]                              | 102400                         |
| fs.inotify.max_user_instances       | [64, 65472]                                     | 64                             |
| fs.inotify.max_user_watches         | [4096, 819200]                                  | 4096                           |
| fs.suid_dumpable                    | [0, 2]                                          | 1                              |
| kernel.core_uses_pid                | {'0', '1'}                                       | '0'                            |
| kernel.dmesg_restrict               | {'0', '1'}                                       | '0'                            |
| kernel.hung_task_timeout_secs       | [30, 1200]                                      | 30                             |
| kernel.msgmax                       | [4096, 1048576]                                 | 4096                           |
| kernel.msgmnb                       | [4096, 1048576]                                 | 4096                           |
| kernel.msgmni                       | [8000, 128000]                                  | 8000                           |
| kernel.nmi_watchdog                 | {'0', '1'}                                       | '0'                            |
| kernel.numa_balancing               | {'0', '1'}                                       | '0'                            |
| kernel.pid_max                      | [1048576, 4194304]                              | 1048576                        |
| kernel.randomize_va_space           | [0, 2]                                          | 1                              |
| kernel.sched_autogroup_enabled      | {'0', '1'}                                       | '0'                            |
| kernel.sched_migration_cost_ns      | [100000, 5000000]                                | 100000                         |
| kernel.sched_rt_runtime_us          | [950000, 1000000]                                | 950000                         |
| kernel.sem                          | {'16000 512000000 256 16000', '32000 1024000000 500 32000', '64000 2048000000 1000 64000'} | '16000 512000000 256 16000' |
| kernel.shmall                       | [1073741824, 8589934592]                        | 1073741824                    |
| kernel.shmmax                       | [17179869184, 68719476736]                       | 17179869184                   |
| kernel.shmmni                       | [1024, 16384]                                   | 1024                           |
| kernel.sysrq                        | {'0', '1'}                                       | '0'                            |
| kernel.threads-max                  | [655360, 65536000]                              | 655360                         |
| kernel.timer_migration              | {'0', '1'}                                       | '0'                            |
| vm.dirty_background_ratio           | [0, 100]                                        | 0                              |
| vm.dirty_expire_centisecs           | [100, 1000]                                     | 100                            |
| vm.dirty_ratio                      | [0, 100]                                        | 0                              |
| vm.dirty_writeback_centisecs       | [100, 1000]                                     | 100                            |
| vm.max_map_count                    | [100000, 10000000]                              | 100000                         |
| vm.min_free_kbytes                  | [10240, 1024000]                                 | 10240                          |
| vm.overcommit_memory                | {'0', '1'}                                       | '0'                            |
| vm.overcommit_ratio                 | [0, 100]                                        | 0                              |
| vm.page-cluster                     | [0, 8]                                          | 0                              |
| vm.stat_interval                    | [1, 100]                                        | 1                              |
| vm.swappiness                       | [0, 100]                                        | 0                              |
| vm.vfs_cache_pressure               | [0, 500]                                        | 0                              |
| vm.watermark_scale_factor           | [10, 1000]                                      | 10                             |
| vm.zone_reclaim_mode                | {'0', '1', '2', '3'}                            | '0'                            |

#### Kafka

| name                        | scope                                   | default    |
|:-------------------:|:--------------------------:|:--------------:|
| compression_type  | {'none', 'gzip', 'snappy', 'lz4', 'zstd'} | 'none'       |
| acks              | {'0', '1', 'all'}         | '1'          |
| request_timeout_ms| [10000, 1000000]          | 30000        |
| max_block_ms      | [10000, 1000000]          | 60000        |
| linger_ms         | [0, 10000]                | 0            |
| max_request_size  | [128, 1073741824]         | 1048576      |
| batch_size        | [128, 1073741824]         | 16384        |
| buffer_memory     | [128, 1073741824]         | 33554432     |


#### Spark

| name                        | scope                                   | default    |
|:----------------------------------------------:|:----------------------------:|:-------------:|
| spark.sql.files.maxPartitionBytes            | [16777216, 8589934592]      | 134217728   |
| spark.dynamicAllocation.maxExecutors         | [5, 10000]                  | 200         |
| spark.sql.adaptive.maxNumPostShufflePartitions | [20, 20000]               | 1000        |
| spark.executor.cores                         | [1, 6]                      | 2           |
| spark.executor.memory                        | [1, 6]                      | 4           |
| spark.executor.memoryOverhead                | [512, 1024]                 | 512         |
| spark.driver.cores                           | [1, 8]                      | 2           |
| spark.driver.memory                          | [1, 35]                     | 4           |
| spark.driver.memoryOverhead                  | [512, 10240]                | 512         |
