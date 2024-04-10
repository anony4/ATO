import threading
from openbox import logger
import multiprocessing as mp
import numpy as np
import re
import os
from ConfigSpace import Configuration
from tune_ssh import TuneSSH
from config import *
import json
from utils_collector import *

im_alive = None


class SparkConnector:
    def __init__(self):
        self.cur_timer = None

    def get_internal_metrics(self, internal_metrics, BENCHMARK_RUNNING_TIME, BENCHMARK_WARMING_TIME):
        return None

    def metrics_handle(self, metrics):
        return None

    def clean_up(self):
        pass


class SparkExecutor:
    def __init__(self, spark_type: str = 'set', exe_infos=None, context_type='special'):
        assert spark_type in [
            'sleep', 'sort', 'terasort', 'wordcount', 'repartition',
            'aggregation', 'join', 'scan', 'pagerank', 'bayes',
            'kmeans', 'lr', 'als', 'pca', 'gbt',
            'rf', 'svd', 'linear', 'lda', 'svm',
            'gmm', 'correlation', 'summarizer', 'nweight'
        ]
        self.spark_type = spark_type

        self.task_path_dict = dict(
            sleep='micro/sleep', sort='micro/sort', terasort='micro/terasort', wordcount='micro/wordcount',
            repartition='micro/repartition', aggregation='sql/aggregation', join='sql/join', scan='sql/scan',
            pagerank='websearch/pagerank', bayes='ml/bayes', kmeans='ml/kmeans', lr='ml/lr',
            als='ml/als', pca='ml/pca', gbt='ml/gbt', rf='ml/rf', svd='ml/svd', linear='ml/linear',
            lda='ml/lda', svm='ml/svm', gmm='ml/gmm', correlation='ml/correlation', summarizer='ml/summarizer',
            nweight='graph/nweight'
        )

        self.remote_app_name = None
        self.infos = exe_infos
        """
            # 运行路径
            workloads_dir = '/root/HiBench/bin/workloads/',
            task_run_path = 'spark/run.sh',
            task_prepare_path = 'prepare/prepare.sh',
            # log 路径
            remote_log_dir = '/root/test/spark-log',
            local_log_dir = './results/%s-log' % target,
            # 配置路径
            remote_conf_dir = '/root/HiBench/conf/',
            local_conf_dir = "./confs/%s/" % target,
            # 应用名称
            local_app_name = "%s_%s_%s" % (args.opt, task_id, timestamp),
        """

        if not os.path.exists(exe_infos['local_log_dir']):
            os.makedirs(exe_infos['local_log_dir'])
        if not os.path.exists(os.path.join(exe_infos['local_conf_dir'], exe_infos['local_app_name'])):
            os.makedirs(os.path.join(exe_infos['local_conf_dir'], exe_infos['local_app_name']))

        self.sc = None
        if context_type == 'special':
            self.sc = SparkConnector()
        elif context_type == 'default':
            self.sc = AtuneConnector()
        else:
            raise Exception('context_flag 错误!')

        self.tune_ssh = TuneSSH()
        self.spark_conf_prepare()

    def get_task_str(self):

        return self.spark_type

    def spark_conf_prepare(self):
        # 下载文件
        flag1 = self.tune_ssh.get_file(
            os.path.join(self.infos['remote_conf_dir'], 'spark.conf'),
            os.path.join(self.infos['local_conf_dir'], self.infos['local_app_name'], 'origin.conf')
        )
        # 下载文件
        flag2 = self.tune_ssh.get_file(
            os.path.join(self.infos['remote_conf_dir'], 'spark.conf'),
            os.path.join(self.infos['local_conf_dir'], self.infos['local_app_name'], 'change.conf')
        )
        if not flag1 or not flag2:
            raise Exception("get conf errors!")

        # prepare
        prepare_path = os.path.join(self.infos['workloads_dir'], self.task_path_dict[self.spark_type],
                                    self.infos['task_prepare_path'])
        command = prepare_path

        logger.info(command)
        results, return_code = self.tune_ssh.exec_command(SERVER_PORT, command)
        if return_code != 0:
            raise Exception("spark prepare errors!")
        else:
            print("Prepare data Successfully!")
            logger.info("Prepare data Successfully!")

    def spark_run(self):

        # run
        run_path = os.path.join(self.infos['workloads_dir'], self.task_path_dict[self.spark_type],
                                self.infos['task_run_path'])
        command = run_path

        logger.info(command)
        results, return_code = self.tune_ssh.exec_command(SERVER_PORT, command)
        if return_code != 0:
            raise Exception("spark run errors!")

        # 获取所有log
        command_ls = 'ls %s' % self.infos['remote_log_dir']
        results_ls, return_code_ls = self.tune_ssh.exec_command(SERVER_PORT, command_ls)
        if return_code_ls != 0:
            raise Exception("ls %s errors!" % self.infos['remote_log_dir'])

        self.remote_app_name = results_ls.split('\n')[-2]
        self.tune_ssh.get_file(
            os.path.join(self.infos['remote_log_dir'], self.remote_app_name),
            os.path.join(self.infos['local_log_dir'], self.infos['local_app_name'])
        )

        # 获取运行结果
        command_cat = 'cat %s' % os.path.join(self.infos['remote_log_dir'], self.remote_app_name)
        results_cat, return_code_cat = self.tune_ssh.exec_command(SERVER_PORT, command_cat)
        if return_code_cat != 0:
            raise Exception('cat %s errors!' % os.path.join(self.infos['remote_log_dir'], self.remote_app_name))

        # 删除log
        command_rm = 'rm %s' % os.path.join(self.infos['remote_log_dir'], self.remote_app_name)
        results_rm, return_code_rm = self.tune_ssh.exec_command(SERVER_PORT, command_rm)
        if return_code_rm != 0:
            raise Exception('rm %s errors!' % os.path.join(self.infos['remote_log_dir'], self.remote_app_name))

        if self.sc.cur_timer is not None:
            self.sc.cur_timer.cancel()

        return results_cat

    def apply_knob(self, knob):

        self.modify_conf(knob)

        ori_metrics = self.sc.get_internal_metrics(list(), 1000, 3)
        results = self.spark_run()
        metrics = self.sc.metrics_handle(ori_metrics)

        run_time, context = decode_results_spark(results)
        if metrics is not None:
            context = metrics

        return context, -run_time

    def set(self, **kwargs):
        for k, v in kwargs.items():
            assert k in ['spark_type']
            if k == 'spark_type':
                assert v in [
                    'sleep', 'sort', 'terasort', 'wordcount', 'repartition',
                    'aggregation', 'join', 'scan', 'pagerank', 'bayes',
                    'kmeans', 'lr', 'als', 'pca', 'gbt',
                    'rf', 'svd', 'linear', 'lda', 'svm',
                    'gmm', 'correlation', 'summarizer', 'nweight'
                ]
                self.spark_type = v

                # prepare
                prepare_path = os.path.join(self.infos['workloads_dir'], self.task_path_dict[self.spark_type],
                                            self.infos['task_prepare_path'])
                command = prepare_path

                logger.info(command)
                results, return_code = self.tune_ssh.exec_command(SERVER_PORT, command)
                if return_code != 0:
                    raise Exception("spark prepare errors!")
                else:
                    print("Prepare data Successfully!")
                    logger.info("Prepare data Successfully!")

    def modify_conf(self, config: Configuration):
        change_conf_path = os.path.join(self.infos['local_conf_dir'], self.infos['local_app_name'], 'change.conf')
        modify_conf_file(change_conf_path, config)
        flag = self.tune_ssh.put_file(
            os.path.join(self.infos['local_conf_dir'], self.infos['local_app_name'], 'change.conf'),
            os.path.join(self.infos['remote_conf_dir'], 'spark.conf')
        )
        if not flag:
            raise Exception("put conf errors!")

    def clean_up(self):
        self.tune_ssh.close_ssh()
        self.sc.clean_up()


def modify_conf_file(conf_file: str, config: Configuration):
    if not isinstance(config, dict):
        config = config.get_dictionary()
    dic = config
    tmp_file = conf_file + '.tmp'
    with open(conf_file, 'r') as f, open(tmp_file, 'w') as ft:
        for line in f:
            for key in dic.keys():
                if line.startswith(key + ' '):
                    line = '%s    %s\n' % (key, dic[key])
                    if key.endswith('memory'):
                        line = '%s    %sg\n' % (key, dic[key])
                    logger.debug('Modify conf file [%s] with [%r]' % (conf_file, line))
                    break
            ft.write(line)
    os.remove(conf_file)
    os.rename(tmp_file, conf_file)


def decode_results_spark(results: str):
    result_list = results.split('\n')
    logs = []
    for line in result_list:
        if line == '':
            continue
        try:
            logs.append(json.loads(line))
        except:
            raise ValueError('Cannot decode json data: %s' % line)

    start_time, end_time = None, None
    task_metrics = dict()
    for event in logs:
        if event['Event'] == "SparkListenerApplicationStart":
            start_time = event['Timestamp']
        elif event['Event'] == "SparkListenerApplicationEnd":
            end_time = event['Timestamp']
        elif event['Event'] == "SparkListenerTaskEnd":
            try:
                metrics_dict = event['Task Metrics']
                for key, value in metrics_dict.items():
                    if isinstance(value, dict):
                        for sub_key, sub_value in value.items():
                            final_key = "%s_%s" % (key, sub_key)
                            task_metrics[final_key] = task_metrics.get(final_key, 0) + sub_value
                    elif isinstance(value, list):
                        continue
                    else:
                        task_metrics[key] = task_metrics.get(key, 0) + value
            except:
                print(event)
                logger.debug(str(event))

    run_time = (end_time - start_time) / 1000

    keys = list(task_metrics.keys())
    keys.sort()
    metrics = np.array([task_metrics[key] for key in keys])

    if start_time is None or end_time is None:
        raise ValueError('Cannot find start or end time in log: %s' % log_path)

    return run_time, metrics


