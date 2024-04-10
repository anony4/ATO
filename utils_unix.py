import threading
from openbox import logger
import multiprocessing as mp
import numpy as np
import re
import os
from ConfigSpace import Configuration
from tune_ssh import TuneSSH
from config import *
from utils_collector import *

im_alive = None


class UnixConnector:
    def __init__(self):
        super().__init__()
        self.tune_ssh = TuneSSH()

        self.im_alive_init()  # im collection signal
        self.cur_timer = None

    def im_alive_init(self):
        global im_alive
        im_alive = mp.Value('b', True)

    def set_im_alive(self, value):
        im_alive.value = value

    def modify_variable(self, variable_name, variable_value):
        command = "sysctl -w %s=%s" % (variable_name, str(variable_value))
        self.tune_ssh.exec_command(SERVER_PORT, command)

    def fetch_metrics(self):
        command = 'vmstat'
        results, _ = self.tune_ssh.exec_command(SERVER_PORT, command)
        key_result = results.split('\n')[1]
        key_list = [x for x in re.split(r'\s+', key_result) if x != '']
        value_result = results.split('\n')[2]
        value_list = [int(x) for x in re.split(r'\s+', value_result) if x != '']

        final_dict = dict(zip(key_list, value_list))

        return final_dict

    def get_internal_metrics(self, internal_metrics, BENCHMARK_RUNNING_TIME, BENCHMARK_WARMING_TIME):
        """Get the all internal metrics of MySQL, like io_read, physical_read.
        This func uses a SQL statement to lookup system table: information_schema.INNODB_METRICS
        and returns the lookup result.
        """
        self.set_im_alive(True)
        _counter = 0
        _period = 10  # observe internal metrics every 2 sec.
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


class UnixExecutor:
    def __init__(self, unix_type: str = 'set', context_type='special'):
        assert unix_type in ['dhry2reg', 'whets', 'syscall', 'pipe', 'context1', 'spawn', 'execl',
                             'fstime-w', 'fstime-r', 'fsdisk-w', 'fsdisk-r', 'hanoi']
        self.unix_type = unix_type

        self.uc = None
        if context_type == 'special':
            self.uc = UnixConnector()
        elif context_type == 'default':
            self.uc = AtuneConnector()
        else:
            raise Exception('context_flag 错误!')

        self.tune_ssh = TuneSSH()

    def get_task_str(self):

        return self.unix_type

    def unix_run(self):

        command = "cd /opt/unixbench \n" + "./Run %s" % self.unix_type

        logger.info(command)
        results, return_code = self.tune_ssh.exec_command(SERVER_PORT, command)
        if return_code != 0:
            raise Exception("sysbench run errors!")

        self.uc.cur_timer.cancel()

        return results

    def apply_knob(self, knob):
        self.modify_conf(knob)

        ori_metrics = self.uc.get_internal_metrics(list(), 500, 3)
        results = self.unix_run()
        metrics = self.uc.metrics_handle(ori_metrics)
        res = decode_results_unix(results)

        return metrics, res

    def set(self, **kwargs):
        for k, v in kwargs.items():
            assert k in ['unix_type']
            if k == 'unix_type':
                assert v in ['dhry2reg', 'whets', 'syscall', 'pipe', 'context1', 'spawn', 'execl',
                             'fstime-w', 'fstime-r', 'fsdisk-w', 'fsdisk-r', 'hanoi']
                self.unix_type = v

    def modify_conf(self, config: Configuration):
        if not isinstance(config, dict):
            config = config.get_dictionary()
        dic = config
        for key, value in dic.items():
            command = "sysctl -w %s=%s" % (key, str(value))
            self.tune_ssh.exec_command(SERVER_PORT, command)


def decode_results_unix(results: str):
    # re_list = re.findall(r'\d+\.\d+\s+\d+\.\d+\nSystem Benchmarks Index Score\s+\(Partial Only\)', results)
    re_list = re.findall(r'\d+\.\d+\s+\d+\.\d+\n\s+========', results)

    try:
        res = float(re.split(r'\s+', re_list[0].split('\n')[0])[0])
    except:
        re_list = re.findall(r'\d+\.\d+\s+KBps', results)
        try:
            res = float(re.split(r'\s+', re_list[0])[0])
        except:
            re_list = re.findall(r'\d+\.\d+\s+lps', results)
            res = float(re.split(r'\s+', re_list[0])[0])
            print(results)

    return res
