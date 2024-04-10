from ConfigSpace import ConfigurationSpace, Configuration
import numpy as np
import copy
import pickle as pkl
from openbox import logger
from datetime import datetime
import os
from typing import List, Tuple, Union, Optional

MAXINT = 2 ** 31 - 1


class Baseline:
    def __init__(self, config_space: ConfigurationSpace, eval_func, iter_num=200,
                 save_dir='./results', ini_context=None,
                 method_id='Advisor', task_id='test', target='redis', task_str='run',
                 context_flag=False, tsd_flag='no_detect', backup_flag=False):

        self.method_id = method_id
        self.task_id = task_id
        self.target = target
        self.task_str = task_str
        self.last_task_str = task_str
        print('target: ', target)
        logger.debug('target: ' + target)

        self.config_space = config_space
        self.eval_func = eval_func

        # 任务内部轮数以及任务下标
        self.iter_num = iter_num
        self.iter_id = 0
        self.task_index = 0
        self.task_start_iter = 1

        # 随即种子
        self.seed = 42
        self.rng = np.random.RandomState(self.seed)
        config_space_seed = self.rng.randint(MAXINT)
        self.config_space.seed(config_space_seed)

        self.context_flag = context_flag
        self.tsd_flag = tsd_flag
        self.backup_flag = backup_flag

        # 历史数据 及 最优结果
        self.observations = list()
        self.all_observations = list()

        self.last_context = ini_context
        self.default_context = ini_context

        self.incumbent_value_before20 = np.inf
        self.incumbent_config_before20 = None
        self.incumbent_context_before20 = None

        self.incumbent_value = np.inf
        self.incumbent_config = None
        self.incumbent_context = None

        # 初始化
        self.ini_configs = list()
        default_config = self.config_space.get_default_configuration()
        default_config.origin = "default config"
        self.ini_configs.append(default_config)

        # 结果保存
        # results
        self.result_dir = os.path.join(save_dir, target, method_id)
        if not os.path.exists(self.result_dir):
            os.makedirs(self.result_dir)
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
        self.result_path = os.path.join(self.result_dir, "%s_%s.pkl" % (task_id, timestamp))

        # 过去的任务的备份
        self.ts_backup_file = "./backup/ts_backup_%s.pkl" % self.target

        _logger_kwargs = {'name': "%s" % task_id, 'logdir': './log/%s/%s' % (self.target, method_id)}
        # _logger_kwargs.update(logger_kwargs or {})
        logger.init(**_logger_kwargs)

        # log
        try:
            self.ts_recorder = pkl.load(open(self.ts_backup_file, 'rb'))
            logger.warn("Successfully initialize from %s !" % self.ts_backup_file)
        except FileNotFoundError:
            self.ts_recorder = []
            logger.warn("File \"%s\" not found, initialize to empty list" % self.ts_backup_file)

    @property
    def configurations(self) -> List[Configuration]:
        return [obs['config'] for obs in self.observations]

    @property
    def objectives(self) -> List[List[float]]:
        return [[obs['perf']] for obs in self.observations]

    @property
    def contexts(self):
        contexts = self.observations[0]['last_context']
        for idx in range(1, len(self.observations)):
            contexts = np.vstack([contexts, self.observations[idx]['last_context']])

        return contexts

    def sample(self):
        raise NotImplementedError

    def update(self, config, perf, qps, context):
        self.observations.append(
            {
                'last_context': self.last_context,
                'config': config,
                'perf': perf,
                'context': context,
                'task_index': self.task_index
            }
        )
        self.all_observations.append(
            {
                'last_context': self.last_context,
                'config': config,
                'perf': perf,
                'context': context,
                'task_index': self.task_index
            }
        )
        if config.origin == "default config":
            logger.warn("[{}] update default context!".format(self.task_str))
            self.default_context = context
        self.last_context = context
        if perf < self.incumbent_value:
            self.incumbent_value = perf
            self.incumbent_config = config
            self.incumbent_context = np.hstack((context, [-perf, -qps]))

    def detect_task_swift(self):
        print("No task switching detection component!")

    def run(self):
        while self.iter_id < self.iter_num:
            self.run_one_iter()
    
    """
    将任务的关键信息保存
    """
    def record_task(self):
        if self.iter_id - self.task_start_iter >= 25:
            ob = copy.deepcopy(self.observations)
            self.ts_recorder.append({
                'task_str': self.last_task_str,
                'ts_context': copy.deepcopy(self.default_context),
                'observations': copy.deepcopy(ob),
            })
            self.last_task_str = self.task_str
            logger.warn("[{}] Successfully record task!".format(self.task_str))
        else:
            logger.warn("[{}] Failed to record the task because the number of iterations was less than 25!".format(self.task_str))
    
    def run_one_iter(self):

        self.iter_id += 1

        if self.tsd_flag == 'detect':
            self.detect_task_swift()

        config = self.sample()
        logger.info("[{}] iter ------------------------------------------------{:5d}".format(self.task_str, self.iter_id))
        if config.origin:
            logger.warn("[{}] !!!!!!!!!! {} !!!!!!!!!!".format(self.task_str, config.origin))
        perf, qps, context = self.eval_func(config)
        self.update(config, perf, qps, context)

        logger.info('[{}] Config: '.format(self.task_str) + str(config.get_dictionary()))
        logger.info('[{}] Obj: {}, best obj: {}'.format(self.task_str, perf, self.incumbent_value))

        logger.info("[{}] ===================================================================================================================================================".format(self.task_str))
        if self.iter_id == self.iter_num:
            with open(self.result_path, 'wb') as f:
                pkl.dump(self.all_observations, f)
            if self.backup_flag:
                self.record_task()
                with open(self.ts_backup_file, 'wb') as ts:
                    pkl.dump(self.ts_recorder, ts)
        elif self.iter_id % 10 == 0:
            with open(self.result_path, 'wb') as f:
                pkl.dump(self.all_observations, f)

