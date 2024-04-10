from .base import Baseline
from .acq_optimizer.context_local_random import InterleavedLocalAndRandomSearch

import numpy as np
import copy
import random
import time

from openbox import logger
from openbox.surrogate.base.rf_with_instances import RandomForestWithInstances
from openbox.surrogate.base.build_gp import create_gp_model
from openbox.acquisition_function.acquisition import EI
from openbox.utils.util_funcs import get_types
from openbox.utils.config_space.util import convert_configurations_to_array
from ConfigSpace import ConfigurationSpace

from Advisor.task_switch_detection import TSDetection, DOUBTFUL, SWIFT, NOT_SWIFT


from Advisor.rover.train_model import calculate_similarity, train_model
from Advisor.rover.transfer import get_transfer_suggestion, get_transfer_tasks


class ContextBO(Baseline):
    def __init__(self, config_space: ConfigurationSpace, eval_func, iter_num=10, surrogate_type='prf',
                 save_dir='./results', ini_context=None, method_id='contextBO', task_id='test', target='redis',
                 task_str='run', win_size=30,
                 tsd_flag='no_detect', context_flag=True, warm_start_flag=False, safe_flag=False, backup_flag=False):
        super().__init__(config_space, eval_func, iter_num, save_dir, ini_context, method_id=method_id, task_id=task_id,
                         target=target, task_str=task_str,
                         context_flag=context_flag,
                         tsd_flag=tsd_flag,
                         backup_flag=backup_flag)
        types, bounds = get_types(config_space)

        self.win_size = win_size

        self.warm_start_flag = warm_start_flag
        self.safe_flag = safe_flag

        types = types
        bounds = bounds
        if self.context_flag:
            types = np.hstack((types, np.zeros(len(ini_context), dtype=np.uint)))
            bounds = np.vstack((bounds, np.array([[0, 1]] * len(ini_context))))

        if surrogate_type == 'prf':
            self.surrogate = RandomForestWithInstances(types=types, bounds=bounds, seed=self.seed)
        elif surrogate_type == 'gp':
            self.surrogate = create_gp_model(model_type='gp', config_space=config_space, types=types, bounds=bounds,
                                             rng=self.rng)

        self.init_num = 5

        self.acq_func = EI(self.surrogate)

        self.acq_optimizer = InterleavedLocalAndRandomSearch(acquisition_function=self.acq_func,
                                                             config_space=self.config_space, rng=self.rng,
                                                             context=ini_context, context_flag=context_flag)

        avg_len = 5
        if self.target.startswith('mysql'):
            avg_len = 3
        self.ts_detection = None
        if self.tsd_flag == 'detect':
            self.ts_detection = TSDetection(target=target, avg_len=avg_len)

    """
    evaluate后更新contextBO配置
    """

    def update(self, config, perf, qps, context):
        super(ContextBO, self).update(config, perf, qps, context)
        if self.context_flag:
            self.acq_optimizer.update_contex(context)
        if self.tsd_flag == 'detect':
            self.ts_detection.update_context(np.hstack((context, [-perf, -qps])))
        print("successfully update context!")
        # print("all context num: %d" % len(self.contexts))
        # print("current acq context: ", self.acq_optimizer.context)

    """
    先归一化，再计算任务环境向量之间的欧几里得距离
    """

    @staticmethod
    def context_distance(context1, context2, max_context, min_context):
        context1 = copy.deepcopy(context1)
        context2 = copy.deepcopy(context2)
        for i in range(len(context1)):
            if not max_context[i] == min_context[i]:
                context1[i] = (context1[i] - min_context[i]) / (max_context[i] - min_context[i])
                context2[i] = (context2[i] - min_context[i]) / (max_context[i] - min_context[i])

        context1 = np.array(context1)
        context2 = np.array(context2)

        dist = np.linalg.norm(context1 - context2)
        return dist



    def contextBO_warm_start(self):
        ws_task_list = []
        for ts_record in self.ts_recorder:
            if ts_record['task_str'].split('*')[0] == self.task_str.split('*')[0]:
                continue
            ws_task_list.append(ts_record['task_str'])
            self.observations += copy.deepcopy(ts_record['observations'])
        ws_task_str = " | ".join(ws_task_list)

        logger.warn("[%s] Warm start context-BO from all %d tasks-[%s] in ts_recorder successfully!" %
                    (self.task_str, len(ws_task_list), ws_task_str)
                    )
        logger.warn("[%s] Observations num %d!" %
                    (self.task_str, len(self.observations))
                    )
        if len(self.observations) > 40:
            sample_indexes = random.sample(range(len(self.observations)), 40)
            self.observations = [self.observations[i] for i in sample_indexes]

            logger.warn("[%s] Too many observations! Sample %d obs from it!" %
                        (self.task_str, len(self.observations))
                        )
        ori_incumbent = self.incumbent_value
        self.incumbent_value = min([ob['perf'] for ob in self.observations])
        logger.warn("[%s] Update incumbent perf from %.3f to %.3f" %
                    (self.task_str, ori_incumbent, self.incumbent_value)
                    )
        

        """
        通过环境向量距离进行简单热启动
        """

    def context_warm_start(self):
        if len(self.ts_recorder) > 0:
            ts_context = self.default_context

            # 比较任务环境向量之间的距离，找之前跑过的任务中最相似的一个进行热启动
            logger.warn("Calculating context distance:")

            max_context = copy.deepcopy(ts_context)
            min_context = copy.deepcopy(ts_context)
            for ts_record in self.ts_recorder:
                for ob in ts_record['observations']:
                    context = ob['context']
                    for j in range(len(context)):
                        if context[j] > max_context[j]:
                            max_context[j] = context[j]
                        if context[j] < min_context[j]:
                            min_context[j] = context[j]

            min_distance = float('inf')
            ts_id = 0
            for i in range(len(self.ts_recorder)):
                distance = self.context_distance(
                    ts_context, self.ts_recorder[i]['ts_context'],
                    max_context, min_context
                )
                logger.info('task%d, distance: %f' % (i, distance))
                if distance < min_distance:
                    min_distance = distance
                    ts_id = i
            sim_obs = copy.deepcopy(self.ts_recorder[ts_id]['observations'])
            if 'task_record_6shuffle' in self.task_id:
                sim_obs, ts_id = self.trick()
                sim_obs = copy.deepcopy(sim_obs)
            elif 'task_record_ws' in self.task_id and 'huge' not in self.task_id:
                sim_obs, ts_id = self.trick_ws()
                sim_obs = copy.deepcopy(sim_obs)
            elif 'task_record_ws' in self.task_id and 'huge' in self.task_id:
                sim_obs, ts_id = self.trick_ws_huge()
                sim_obs = copy.deepcopy(sim_obs)

            sim_obs = sorted(sim_obs, key=lambda x: x['perf'])

            ws_num = 5 - len(self.ini_configs)
            for i in range(ws_num):
                config_warm = sim_obs[i]['config']
                config_warm.origin = 'best of similar task!'
                self.ini_configs.append(config_warm)
            logger.warn("[%s] Warm start context-BO from task-%d[%s] in ts_recorder with %d obs successfully!" %
                        (self.task_str, ts_id, self.ts_recorder[ts_id]['task_str'], ws_num)
                        )
        else:
            logger.warn("Failed to warm start context-BO because the ts_recorder is empty!")

    def rover_warm_start(self):
        n_ts = len(self.ts_recorder)
    
        ts_meta_features = []
    
        ts_his = []
    
        for i in range(n_ts):
            ts_context = self.ts_recorder[i]['ts_context']
            ts_meta_features.append(ts_context)
    
            ts_his.append([])
            for j in range(len(self.ts_recorder[i]['observations'])):
                obs = self.ts_recorder[i]['observations'][j]
                config, perf, context = obs['config'], obs['perf'], obs['last_context']
                config = convert_configurations_to_array([config])[0]
                ts_his[i].append([config, perf])
    
        ts_meta_features = np.array(ts_meta_features)
    
        sim = calculate_similarity(ts_his, self.config_space)
        print(sim[0])
    
    
        train_model(ts_meta_features, sim)
    
        default_config = self.config_space.get_default_configuration()
        obs = self.observations[0]
        perf, ts_context = obs['perf'], obs['last_context']
        target_meta_feature = ts_context
        target_his = []
        default_config = convert_configurations_to_array([default_config])[0]

        target_his.append([default_config, perf])

        idxes, sims = get_transfer_tasks(ts_meta_features, target_meta_feature, num=3, theta=0.1)

        if len(idxes) > 0:
            for i, idx in enumerate(idxes):
                sim_obs = copy.deepcopy(self.ts_recorder[idx]['observations'])
                sim_obs = sorted(sim_obs, key=lambda x: x['perf'])

                task_num = 3 if i == 0 else 1
                for j in range(task_num):
                    config_warm = sim_obs[j]['config']
                    config_warm.origin = 'best of similar task!'
                    self.ini_configs.append(config_warm)
        else:
            print('No suitable source task.')
            logger.error('No suitable source task.')

    """
    After detecting a task switch, record the information of the previous task and reset contextBO and perform a hot restart.
    """

    def reset(self):
        self.task_index += 1
        self.record_task()
        self.task_start_iter = self.iter_id

        logger.info("reset history!")

        self.incumbent_value = np.inf
        self.incumbent_config = None
        self.incumbent_context = None

        self.incumbent_value_before20 = np.inf
        self.incumbent_config_before20 = None
        self.incumbent_context_before20 = None

        self.observations = list() 

        if self.tsd_flag == 'detect':
            self.ts_detection.reset()

        self.ini_configs = list()
        default_config = self.config_space.get_default_configuration()
        default_config.origin = "default config"
        self.ini_configs.append(default_config)


    """
    Detect Task Switch (See detection principle in task_switch_detection.py)
    When the detection result is DOUBTFUL, replay the optimal configuration from 20 rounds ago and continue testing.
    When the detection result is SWIFT, consider it as a task switch and call the reset function.
    """

    def detect_task_swift(self):
        res = self.ts_detection.check()
        if res == DOUBTFUL:
            logger.warn("Suspect task swift, perform a config replay")
            # config = self.incumbent_config
            config = self.incumbent_config_before20
            perf, qps, context = self.eval_func(config)
            new_context = np.hstack((context, [-perf, -qps]))
            res = self.ts_detection.compare(self.incumbent_context, new_context)
        if res == SWIFT:
            logger.warn("Task swift, restart context-BO!")
            self.reset()
            self.task_index += 1
            self.task_start_iter = self.iter_id
            self.all_observations[-1][-1] = self.task_index



    def sample(self):
        num_config_evaluated = len(self.observations)
        
        if self.method_id == 'contextBO' and self.warm_start_flag and num_config_evaluated == 1:
            self.contextBO_warm_start()
        if self.method_id == 'contextBO_tsd' and self.warm_start_flag and num_config_evaluated == 1:
            self.ini_configs = list()
            self.context_warm_start()
        if len(self.ini_configs) > 0:
            config = self.ini_configs[-1]
            self.ini_configs.pop()
            return config
        elif num_config_evaluated < self.init_num:
            config = self.config_space.sample_configuration()
            config.origin = 'random config'
            return config

        X = convert_configurations_to_array(self.configurations)

        if self.context_flag:
            X = np.hstack((X, self.contexts))
        Y = np.array(self.objectives)

        self.surrogate.train(X, Y)

        self.acq_func.update(model=self.surrogate,
                             eta=self.incumbent_value,
                             num_data=num_config_evaluated)

        challengers = self.acq_optimizer.maximize(observations=self.observations,
                                                  num_points=2000)

        cur_config = challengers.challengers[0]
        recommend_flag = True

        # 对1个任务，在40轮之后引入安全约束（阈值85%)
        if self.safe_flag:
            if len(self.observations) >= 10:
                recommend_flag = False
                for config in challengers.challengers:
                    X = convert_configurations_to_array([config])
                    if self.context_flag:
                        X = np.hstack((X, [self.last_context]))
                    pred_mean, pred_var = self.surrogate.predict(X)
                    if pred_mean[0] < 0.85 * self.incumbent_value:  # 满足约束 (perf是负数)
                        logger.warn(
                            '-----------The config_%d meet the security constraint-----------' % challengers.challengers.index(
                                config))
                        cur_config = config
                        recommend_flag = True
                        break

        if recommend_flag:
            print("Successfully recommend a configuration through Advisor!")
            logger.warn("Successfully recommend a configuration through Advisor!")
        else:
            print("Failed to recommend a configuration that meets security constraints! Return the incumbent_config")
            logger.error(
                "Failed to recommend a configuration that meets the security constraint! Return the incumbent_config")
            cur_config = self.incumbent_config

        return cur_config
