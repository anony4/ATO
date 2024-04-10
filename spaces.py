from openbox import space as sp


def get_mysql_space():
    # Define Search Space
    space = sp.Space()

    # "innodb_log_file_size": "dynamic": "No",
    # sp.Int("innodb_log_file_size", lower=4194304, upper=1073741824, default_value=50331648),
    space.add_variables([
        sp.Int("innodb_buffer_pool_size", lower=10307921510, upper=15618062894, default_value=13958643712),
        sp.Int("innodb_thread_sleep_delay", lower=0, upper=1000, default_value=0),
        sp.Int("innodb_thread_concurrency", lower=0, upper=100, default_value=0),
        sp.Int("innodb_max_dirty_pages_pct_lwm", lower=0, upper=99, default_value=0),
        sp.Int("innodb_read_ahead_threshold", lower=0, upper=64, default_value=56),
        sp.Int("innodb_adaptive_max_sleep_delay", lower=0, upper=1000000, default_value=150000),
        sp.Int("thread_cache_size", lower=0, upper=16384, default_value=0),
        sp.Int("innodb_adaptive_flushing_lwm", 0, 70, default_value=10),
        sp.Categorical("innodb_adaptive_hash_index", choices=['ON', 'OFF'], default_value='ON'),
        sp.Categorical("innodb_change_buffering", choices=["none", "inserts", "deletes", "changes", "purges", "all"], default_value="all"),
        sp.Int("innodb_io_capacity", lower=100, upper=2000000, default_value=200),
        sp.Int("innodb_max_dirty_pages_pct", lower=0, upper=99, default_value=75),
        sp.Int("max_heap_table_size", lower=16384, upper=1073741824, default_value=16777216),
        sp.Int("tmp_table_size", lower=1024, upper=1073741824, default_value=16777216)
    ])

    return space



def get_redis_space():
    # Define Search Space
    space = sp.Space()

    space.add_variables([
        sp.Categorical("maxmemory-policy", choices=["volatile-lru", "allkeys-lru", "volatile-lfu", "allkeys-lfu", 
                                                    "volatile-random", "allkeys-random", "volatile-ttl", "noeviction"], default_value="noeviction"),
        sp.Int("maxmemory-samples", lower=1, upper=10, default_value=5),
        sp.Int("maxmemory", lower=0, upper=16*1024, default_value=0),
        # sp.Categorical("rdbchecksum", choices=["yes", "no"], default_value="yes"),
        sp.Int("hash-max-ziplist-entries", lower=32, upper=4096, default_value=512),
        sp.Int("hash-max-ziplist-value", lower=8, upper=1024, default_value=64),
        sp.Int("list-max-ziplist-size", lower=-5, upper=-1, default_value=-2),
        sp.Int("list-compress-depth", lower=0, upper=5, default_value=0),
        sp.Int("zset-max-ziplist-entries", lower=32, upper=4096, default_value=128),
        sp.Int("zset-max-ziplist-value", lower=16, upper=2048, default_value=64),
        sp.Int("set-max-intset-entries", lower=32, upper=4096, default_value=512),
        sp.Categorical("activerehashing", choices=["yes", "no"], default_value="yes"),
        sp.Categorical("appendfsync", choices=["always", "everysec", "no"], default_value="everysec"),
        sp.Int("lua-time-limit", lower=0, upper=10000, default_value=5000)
    ])

    return space


def get_unix_space():
    # Define Search Space
    space = sp.Space()

    space.add_variables([
        sp.Int("fs.aio-max-nr", lower=102400, upper=10240000, q=102400, default_value=102400),
        sp.Int("fs.file-max", lower=102400, upper=10240000, q=10240, default_value=102400),
        sp.Int("fs.inotify.max_user_instances", lower=64, upper=65472, q=64, default_value=64),
        sp.Int("fs.inotify.max_user_watches", lower=4096, upper=819200, q=4096, default_value=4096),
        sp.Int("fs.suid_dumpable", lower=0, upper=2, default_value=1),

        sp.Categorical("kernel.core_uses_pid", choices=["0", "1"], default_value="0"),
        sp.Categorical("kernel.dmesg_restrict", choices=["0", "1"], default_value="0"),
        sp.Int("kernel.hung_task_timeout_secs", lower=30, upper=1200, q=30, default_value=30),
        sp.Int("kernel.msgmax", lower=4096, upper=1048576, q=4096, default_value=4096),
        sp.Int("kernel.msgmnb", lower=4096, upper=1048576, q=4096, default_value=4096),
        sp.Int("kernel.msgmni", lower=8000, upper=128000, q=8000, default_value=8000),
        sp.Categorical("kernel.nmi_watchdog", choices=["0", "1"], default_value="0"),
        sp.Categorical("kernel.numa_balancing", choices=["0", "1"], default_value="0"),
        sp.Int("kernel.pid_max", lower=1048576, upper=4194304, q=1048576, default_value=1048576),
        sp.Int("kernel.randomize_va_space", lower=0, upper=2, default_value=1),
        sp.Categorical("kernel.sched_autogroup_enabled", choices=["0", "1"], default_value="0"),
        sp.Int("kernel.sched_migration_cost_ns", lower=100000, upper=5000000, q=100000, default_value=100000),
        sp.Int("kernel.sched_rt_runtime_us", lower=950000, upper=1000000, q=10000, default_value=950000),
        sp.Categorical("kernel.sem", choices=["16000 512000000 256 16000", "32000 1024000000 500 32000",
                                              "64000 2048000000 1000 64000"], default_value="16000 512000000 256 16000"),
        sp.Int("kernel.shmall", lower=1073741824, upper=8589934592, q=1073741824, default_value=1073741824),
        sp.Int("kernel.shmmax", lower=17179869184, upper=68719476736, q=17179869184, default_value=17179869184),
        sp.Int("kernel.shmmni", lower=1024, upper=16384, q=1024, default_value=1024),
        sp.Categorical("kernel.sysrq", choices=["0", "1"], default_value="0"),
        sp.Int("kernel.threads-max", lower=655360, upper=65536000, q=655360, default_value=655360),
        sp.Categorical("kernel.timer_migration", choices=["0", "1"], default_value="0"),

        sp.Int("vm.dirty_background_ratio", lower=0, upper=100, default_value=0),
        sp.Int("vm.dirty_expire_centisecs", lower=100, upper=1000, q=100, default_value=100),
        sp.Int("vm.dirty_ratio", lower=0, upper=100, default_value=0),
        sp.Int("vm.dirty_writeback_centisecs", lower=100, upper=1000, q=100, default_value=100),
        sp.Int("vm.max_map_count", lower=100000, upper=10000000, q=100000, default_value=100000),
        sp.Int("vm.min_free_kbytes", lower=10240, upper=1024000, q=10240, default_value=10240),
        sp.Categorical("vm.overcommit_memory", choices=["0", "1"], default_value="0"),
        sp.Int("vm.overcommit_ratio", lower=0, upper=100, q=10, default_value=0),
        sp.Int("vm.page-cluster", lower=0, upper=8, default_value=0),
        sp.Int("vm.stat_interval", lower=1, upper=100, default_value=1),
        sp.Int("vm.swappiness", lower=0, upper=100, default_value=0),
        sp.Int("vm.vfs_cache_pressure", lower=0, upper=500, q=50, default_value=0),
        sp.Int("vm.watermark_scale_factor", lower=10, upper=1000, q=10, default_value=10),
        sp.Categorical("vm.zone_reclaim_mode", choices=["0", "1", "2", "3"], default_value="0"),

    ])

    return space


def get_kafka_space():
    # Define Search Space
    space = sp.Space()

    space.add_variables([
        sp.Categorical("compression_type", choices=["none", "gzip", "snappy", "lz4", "zstd"], default_value="none"),
        sp.Categorical("acks", choices=["0", "1", "all"], default_value="1"),
        sp.Int("request_timeout_ms", lower=10000, upper=1000000, default_value=30000),
        sp.Int("max_block_ms", lower=10000, upper=1000000, default_value=60000),
        sp.Int("linger_ms", lower=0, upper=10000, default_value=0),
        sp.Int("max_request_size", lower=128, upper=1073741824, q=128, default_value=1048576),
        sp.Int("batch_size", lower=128, upper=1073741824, q=128, default_value=16384),
        sp.Int("buffer_memory", lower=128, upper=1073741824, q=128, default_value=33554432),
    ])

    return space


def get_spark_space():
    # Define Search Space
    space = sp.Space()

    space.add_variables([
        sp.Int("spark.sql.files.maxPartitionBytes", lower=16777216, upper=8589934592, default_value=134217728),
        sp.Int("spark.dynamicAllocation.maxExecutors", lower=5, upper=10000, default_value=200),
        sp.Int("spark.sql.adaptive.maxNumPostShufflePartitions", lower=20, upper=20000, default_value=1000),
        sp.Int("spark.executor.cores", lower=1, upper=6, default_value=2),
        sp.Int("spark.executor.memory", lower=1, upper=6, default_value=4),
        sp.Int("spark.executor.memoryOverhead", lower=512, upper=1024, default_value=512),
        sp.Int("spark.driver.cores", lower=1, upper=8, default_value=2),
        sp.Int("spark.driver.memory", lower=1, upper=35, default_value=4),
        sp.Int("spark.driver.memoryOverhead", lower=512, upper=10240, default_value=512),
    ])

    return space

