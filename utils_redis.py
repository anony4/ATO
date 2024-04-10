import threading
from openbox import logger
import multiprocessing as mp
import numpy as np
import redis
import re
import os
from ConfigSpace import Configuration
from tune_ssh import TuneSSH
from config import *
from utils_collector import *


class RedisConnector:
    def __init__(self):
        super().__init__()
        self.re_host = SERVER_IP
        self.re_port = REDIS_PORT

        self.redis = redis.Redis(host=self.re_host, port=self.re_port)

        # ok or not
        self.bool_metrics = [
            'rdb_last_bgsave_status',
            'aof_last_bgrewrite_status',
            'aof_last_write_status',
        ]

        self.ignored_metrics = [
            'redis_version',
            'redis_git_shal',
            'redis_git_dirty',
            'redis_build_id',
            'redis_mode',
            'os',
            'arch_bits',
            'multiplexing_api',
            'atomicvar_api',
            'gcc_version',
            'process_id',
            'run_id',
            'tcp_port',
            'uptime_in_seconds',
            'uptime_in_days',
            'hz',
            'configured_hz',
            'lru_clock',
            'executable',
            'config_file',
            'mem_allocator',
            'rdb_last_save_time',
            'role',
            'master_replid',
            'master_replid2',
            'maxmemory_policy',
            'db0'  # {'keys': 1, 'expires': 0, 'avg_ttl': 0}
        ]

        self.im_alive_init()  # im collection signal
        self.cur_timer = None

    def im_alive_init(self):
        global im_alive
        im_alive = mp.Value('b', True)

    def set_im_alive(self, value):
        im_alive.value = value

    def fetch_metrics(self):
        res_dict = self.redis.info()
        final_dict = dict()

        for key, value in res_dict.items():
            if key in self.ignored_metrics or key.endswith("human") or key.endswith("perc") or key.endswith("ratio"):
                continue
            elif key in self.bool_metrics:
                value = 1 if value == 'ok' else 0

            final_dict[key] = value

        return final_dict

    def get_internal_metrics(self, internal_metrics, BENCHMARK_RUNNING_TIME, BENCHMARK_WARMING_TIME):
        """Get the all internal metrics of MySQL, like io_read, physical_read.
        This func uses a SQL statement to lookup system table: information_schema.INNODB_METRICS
        and returns the lookup result.
        """
        self.set_im_alive(True)

        _counter = 0
        _period = 3  # observe internal metrics every 10 sec.
        count = (BENCHMARK_RUNNING_TIME + BENCHMARK_WARMING_TIME) / _period - 1
        warmup = BENCHMARK_WARMING_TIME / _period

        def collect_metric(counter):
            counter += 1
            self.cur_timer = threading.Timer(float(_period), collect_metric, (counter,))
            self.cur_timer.start()
            if counter >= count or not im_alive.value:
                self.cur_timer.cancel()
                if not im_alive.value:
                    print("Task stops, im_alive cancel, metrics getting cancel!")
                else:
                    print("Time up, metrics getting cancel!")

            if counter > warmup:
                try:
                    # print('collect internal metrics {}'.format(counter))

                    res_dict = self.fetch_metrics()

                    internal_metrics.append(res_dict)

                except Exception as err:
                    self.connect_sucess = False
                    logger.info("connection failed during internal metrics collection")
                    logger.info(err)

        collect_metric(_counter)
        return internal_metrics

    def metrics_handle(self, metrics):
        def do(metric_values):
            return float(sum(metric_values)) / len(metric_values)

        print("handle", len(metrics))
        result = np.zeros(len(metrics[0]))
        keys = list(metrics[0].keys())
        keys.sort()

        for idx in range(len(keys)):
            key = keys[idx]
            data = [x[key] for x in metrics]
            result[idx] = do(data)

        return result

    def clean_up(self):
        pass


class RedisExecutor:
    def __init__(self, redis_type: str = 'set', n: int = 5000000, c: int = 50,
                 d: int = 2, context_type='special'):
        redis_type = redis_type.split(' ')
        for t in redis_type:
            assert t in ['set', 'get', 'sadd', 'mset']
        self.redis_type = ",".join(redis_type)
        self.n = n

        self.c = c  # 指定并发连接数
        self.d = d  # 以字节的形式指定 SET/GET 值的数据大小

        self.kc = None
        if context_type == 'special':
            self.rc = RedisConnector()
        elif context_type == 'default':
            self.rc = AtuneConnector()
        else:
            raise Exception('context_flag 错误!')

        self.tune_ssh = TuneSSH()

    def get_task_str(self):

        return "{}*c{}*d{}".format(self.redis_type, self.c, self.d)

    def redis_run(self):

        command = "/root/redis-6.0.5/src/redis-benchmark -t %s -n %d -h %s -p %d -c %d -d %d" \
                  % (self.redis_type, self.n, SERVER_IP, REDIS_PORT, self.c, self.d)
        logger.info(command)
        results, return_code = self.tune_ssh.exec_command(CLIENT_PORT, command)

        self.rc.cur_timer.cancel()
        print("Task stops, im_alive cancel, metrics getting cancel!")

        if return_code != 0:
            raise Exception("sysbench run errors!")

        return results

    def apply_knob(self, knob):

        self.modify_conf(knob)
        ori_metrics = self.rc.get_internal_metrics(list(), 1000, 5)
        results = self.redis_run()
        context = self.rc.metrics_handle(ori_metrics)

        rps = decode_results_redis(results)

        return context, rps

    def set(self, **kwargs):

        for k, v in kwargs.items():
            assert k in ['redis_type', 'n', 'c', 'd']
            if k == 'redis_type':
                v = v.split(' ')
                for t in v:
                    assert t in ['set', 'get', 'sadd', 'mset']
                self.redis_type = ",".join(v)
            elif k == 'n':
                self.n = v
            elif k == 'c':
                self.c = v
            elif k == 'd':
                self.d = v

    def modify_conf(self, config: Configuration):
        my_redis = redis.Redis(host=SERVER_IP, port=REDIS_PORT)
        if not isinstance(config, dict):
            config = config.get_dictionary()
        dic = config
        for key, value in dic.items():
            my_redis.config_set(key, value)


def decode_results_redis(results: str):
    re_list = re.findall(r'\d+\.\d+\s+requests per second', results)

    try:
        rps = float(re_list[0].split(' ')[0])
    except:
        logger.warn(results)
        print(results)

    return rps
