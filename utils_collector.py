import threading
from openbox import logger
import multiprocessing as mp
import numpy as np
import re
import os
from ConfigSpace import Configuration
from tune_ssh import TuneSSH
from config import *


class AtuneMonitorThread(threading.Thread):
    def __init__(self, stdout):
        threading.Thread.__init__(self)
        self.run_flag = True
        self.res = None
        self.stdout = stdout
    def run(self):
        while self.run_flag and not self.stdout.channel.exit_status_ready():
            # 检查是否应该中断线程
            if not self.run_flag:
                return
            line = self.stdout.readline()
            if line:
                try:
                    value = [float(x) for x in re.split('\s+', line[:-2])]
                    if self.res is None:
                        self.res = np.array([value])
                    else:
                        self.res = np.vstack([self.res, value])
                except:
                    print("Not internal metrics")
                    continue
    def stop(self):
        self.run_flag = False


class AtuneConnector:
    def __init__(self):
        super().__init__()
        self.tune_ssh = TuneSSH()

        command = "cd /A-Tune-Collector/atune_collector \n python collect_data.py -c collect_data.json"
        stdout = self.tune_ssh.exec_command_async(SERVER_PORT, command)
        self.moni_thread = AtuneMonitorThread(stdout=stdout)
        self.moni_thread.start()

        self.cur_timer = None


    def fetch_metrics(self):

        res = None
        if self.moni_thread.res is not None:
            res = np.mean(self.moni_thread.res, axis=0)
            self.moni_thread.res = None

        return res

    def get_internal_metrics(self, internal_metrics, BENCHMARK_RUNNING_TIME, BENCHMARK_WARMING_TIME):
        """Get the all internal metrics of MySQL, like io_read, physical_read.
        This func uses a SQL statement to lookup system table: information_schema.INNODB_METRICS
        and returns the lookup result.
        """
        
        self.moni_thread.res = None
        _counter = 0
        _period = 5  # observe internal metrics every 2 sec.
        count = (BENCHMARK_RUNNING_TIME + BENCHMARK_WARMING_TIME) / _period - 1
        warmup = BENCHMARK_WARMING_TIME / _period

        def collect_metric(counter):
            counter += 1
            self.cur_timer = threading.Timer(float(_period), collect_metric, (counter,))
            self.cur_timer.start()
            if counter >= count:
                self.cur_timer.cancel()

            if counter > warmup:
                try:
                    # print('collect internal metrics {}'.format(counter))

                    res_array = self.fetch_metrics()
                    if res_array is not None:
                        internal_metrics.append(res_array)

                except Exception as err:
                    self.connect_sucess = False
                    logger.info("connection failed during internal metrics collection")
                    logger.info(err)

        collect_metric(_counter)
        return internal_metrics

    def metrics_handle(self, metrics):
        result = np.mean(metrics, axis=0)

        return result

    
    def clean_up(self):
        self.moni_thread.stop()

        # _, stdout, _ = self.tune_ssh.cli_4250.exec_command(SERVER_PORT, 'ps -ef | grep collect_data')
        # results = stdout.read().decode('utf-8')
        # pid = int(re.split('\s+', results)[1])
        # _, _, stderr = self.tune_ssh.cli_4250.exec_command(SERVER_PORT, 'kill %d' % pid)
        # errors = stderr.read().decode('utf-8')
        # if len(errors) > 0:
        #     raise Exception('Kill data collector Error!')
        # else:
        #     print('Kill data collector Successfully!')

        self.tune_ssh.close_ssh()

