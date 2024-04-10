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


class MonitorThread(threading.Thread):
    def __init__(self, res, stdout, index):
        threading.Thread.__init__(self)
        self.run_flag = True
        self.res = res
        self.stdout = stdout
        self.index = index

    def run(self):
        while self.run_flag and not self.stdout.channel.exit_status_ready():
            line = self.stdout.readline()
            if line:
                try:
                    value = float(line.split(',')[self.index])
                    self.res.append(value)
                except:
                    continue

            # 检查是否应该中断线程
            if not self.run_flag:
                return

    def stop(self):
        self.run_flag = False


class KafkaConnector:
    def __init__(self):
        super().__init__()
        self.tune_ssh = TuneSSH()

        self.value_type_metrics = [
            'kafka.server:type=ReplicaManager,name=PartitionCount',
            'kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent'
        ]

        self.all_metrics_index = {
            'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec': 1,
            'kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec': 1,
            'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec': 1,
            'kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec': 1,
            'kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec': 1,
            'kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec': 1,
            'kafka.server:type=ReplicaManager,name=PartitionCount': 1,
            'kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs': 7,
            'kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent': 1,
            'kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent': 1,
        }

        self.metrics = {
            'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec': list(),
            'kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec': list(),
            'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec': list(),
            'kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec': list(),
            'kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec': list(),
            'kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec': list(),
            'kafka.server:type=ReplicaManager,name=PartitionCount': list(),
            'kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs': list(),
            'kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent': list(),
            'kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent': list(),
        }

        # def task(res, stdout, index):
        #     while not stdout.channel.exit_status_ready():
        #         line = stdout.readline()
        #         if line:
        #             try:
        #                 value = float(line.split(',')[index])
        #                 res.append(value)
        #             except:
        #                 continue

        self.all_threads = {}

        for metric_name, res in self.metrics.items():
            command = 'cd kafka_2.13-3.5.0 \n ./bin/kafka-run-class.sh kafka.tools.JmxTool --object-name %s --reporting-interval 1000' % metric_name
            stdout = self.tune_ssh.exec_command_async(SERVER_PORT, command)
            index = self.all_metrics_index[metric_name]
            # t = threading.Thread(target=task, args=(res, stdout, index))
            t = MonitorThread(res=res, stdout=stdout, index=index)
            t.start()

            self.all_threads[metric_name] = t

        self.im_alive_init()  # im collection signal
        self.cur_timer = None

    def im_alive_init(self):
        global im_alive
        im_alive = mp.Value('b', True)

    def set_im_alive(self, value):
        im_alive.value = value

    def fetch_metrics(self):

        final_dict = {}
        for metric_name, metric_list in self.metrics.items():
            final_dict[metric_name] = metric_list[-1]

        return final_dict

    def get_internal_metrics(self, internal_metrics, BENCHMARK_RUNNING_TIME, BENCHMARK_WARMING_TIME):
        """Get the all internal metrics of MySQL, like io_read, physical_read.
        This func uses a SQL statement to lookup system table: information_schema.INNODB_METRICS
        and returns the lookup result.
        """
        self.set_im_alive(True)
        _counter = 0
        _period = 2  # observe internal metrics every 2 sec.
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
        def do(metric_name, metric_values):
            metric_type = 'counter'
            if metric_name in self.value_type_metrics:
                metric_type = 'value'
            if metric_type == 'counter':
                return float(metric_values[-1] - metric_values[0]) / len(metric_values)
            else:
                return float(sum(metric_values)) / len(metric_values)

        result = np.zeros(len(metrics[0]))
        keys = list(metrics[0].keys())

        for idx in range(len(keys)):
            key = keys[idx]
            data = [x[key] for x in metrics]
            result[idx] = do(key, data)

        return result

    def clean_up(self):
        self.tune_ssh.close_ssh()
        for t in self.all_threads.values():
            t.stop()


class KafkaExecutor:
    def __init__(self, partition: int = 1, replication_factor: int = 1, num_records: int = 2000000,
                 record_size: int = 1024, throughput: int = 100000, context_type='special'):
        self.partition = partition
        self.replication_factor = replication_factor

        self.num_records = num_records
        self.record_size = record_size

        self.throughput = throughput

        self.configs = dict()

        self.kc = None
        if context_type == 'special':
            self.kc = KafkaConnector()
        elif context_type == 'default':
            self.kc = AtuneConnector()
        else:
            raise Exception('context_flag 错误!')

        self.tune_ssh = TuneSSH()

        self.kafka_clean()
        self.kafka_prepare(self.partition, self.replication_factor)

    def get_task_str(self):

        return "p{}*s{}".format(self.partition, self.record_size)

    # 准备好topic
    def kafka_prepare(self, partition=1, replication_factor=1):
        command = "cd kafka_2.13-3.5.0 \n ./bin/kafka-topics.sh --create --topic kafkatest --bootstrap-server %s:9092 --partitions %d --replication-factor %d" % (
        SERVER_IP, partition, replication_factor)

        results, return_code = self.tune_ssh.exec_command(SERVER_PORT, command)
        if return_code != 0:
            raise Exception("sysbench prepare errors!")
        else:
            print(results)

    # 准备好topic
    def kafka_clean(self):
        command = "cd kafka_2.13-3.5.0 \n ./bin/kafka-topics.sh --delete --topic kafkatest --bootstrap-server %s:9092" % SERVER_IP

        results, return_code = self.tune_ssh.exec_command(SERVER_PORT, command)
        if return_code != 0:
            raise Exception("sysbench prepare errors!")
        else:
            print(results)

    def kafka_run(self):

        config_str = " ".join([str(key) + "=" + str(value) for (key, value) in self.configs.items()])
        command = "cd kafka_2.13-3.5.0 \n ./bin/kafka-producer-perf-test.sh --topic kafkatest --num-records %d --record-size %d --throughput %d --producer-props bootstrap.servers=%s:9092 %s" % (
        self.num_records, self.record_size, self.throughput, SERVER_IP, config_str)

        logger.info(command)
        results, return_code = self.tune_ssh.exec_command(SERVER_PORT, command)
        if return_code != 0:
            raise Exception("sysbench run errors!")

        if self.kc.cur_timer is not None:
            self.kc.cur_timer.cancel()

        return results

    def apply_knob(self, knob):

        self.modify_conf(knob)

        ori_metrics = self.kc.get_internal_metrics(list(), 1000, 1)
        results = self.kafka_run()

        metrics = self.kc.metrics_handle(ori_metrics)
        rps = decode_results_kafka(results)

        return metrics, rps

    def set(self, **kwargs):
        for k, v in kwargs.items():
            assert k in ['partition', 'replication_factor', 'num_records', 'record_size', 'throughput']
            if k == 'partition':
                self.partition = v
                self.kafka_clean()
                self.kafka_prepare(self.partition, self.replication_factor)
            elif k == 'replication_factor':
                self.replication_factor = v
            elif k == 'num_recorders':
                self.num_recorders = v
            elif k == 'record_size':
                self.record_size = v
            elif k == 'throughput':
                self.throughput = v

    def modify_conf(self, config: Configuration):
        if not isinstance(config, dict):
            config = config.get_dictionary()
        self.configs = config


def decode_results_kafka(results: str):
    # re_list = re.findall(r'\d+\.\d+\s+\d+\.\d+\nSystem Benchmarks Index Score\s+\(Partial Only\)', results)
    re_list = re.findall(r'\d+\.\d+\s+records/sec', results)

    try:
        res = float(re.split(r'\s+', re_list[0])[0])
    except:
        print(results)

    return res
