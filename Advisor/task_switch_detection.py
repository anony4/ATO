import numpy as np
from openbox import logger
import pickle as pkl

DOUBTFUL = 0
SWIFT = 1
NOT_SWIFT = 2


def _my_log10(value):
    value = max(value, 1e-10)
    return np.log10(value)


my_log10 = np.vectorize(_my_log10)



PARAS = {
    'mysql': [0.40, 0.3796502690238287, 0.24168364008577306],
    'redis': [0.40, 0.49446524850504847, 0.3246405435599478],
    'kafka': [0.40, 0.44022347700078046, 0.37165171868296265],
    'spark': [0.40, 0.38501064584811917, 0.36074904959674153],
    'unix': [0.40, 0.4127821355852991, 0.30448098213667846],
    'default': [0.40, 0.40, 0.40]
}

class TSDetection:
    def __init__(self, target='mysql', e=0.02, avg_len=5, std_len=5):
        self.target = target
        
        self.avg_len = avg_len
        self.std_len = std_len

        task_str = 'default'
        for ts in PARAS.keys():
            if ts in target:
                task_str = ts
                break
        self.ratio_thr, self.avg_thr, self.std_thr = PARAS[task_str]

        # patience for avg_change and std_change
        self.avg_changed = 0
        self.std_changed = 0

        self.max_context = None
        self.min_context = None

        self.all_contexts = None
    
    def reset(self):
        self.all_contexts = None

    def update_context(self, context):
        if self.max_context is None:
            self.max_context = context
        else:
            self.max_context = np.max([self.max_context, context], axis=0)
        if self.min_context is None:
            self.min_context = context
        else:
            self.min_context = np.min([self.min_context, context], axis=0)

        if self.all_contexts is None:
            self.all_contexts = context.reshape(1, -1)
        else:
            self.all_contexts = np.vstack([self.all_contexts, context])


    def compare(self, context_1, context_2):
        avg_1 = (context_1 - self.min_context) / (self.max_context - self.min_context)
        avg_2 = (context_2 - self.min_context) / (self.max_context - self.min_context)

        avg_flag = np.abs(avg_1 - avg_2) > self.avg_thr
        avg_num = np.sum(avg_flag)
        avg_ratio = avg_num / self.all_contexts.shape[1]

        if avg_ratio >= self.avg_thr:
            return SWIFT
        elif self.avg_changed > 0:
            return NOT_SWIFT



    def check(self):
        if self.all_contexts is None or self.all_contexts.shape[0] < 4 * max(self.avg_len, self.std_len):
            return NOT_SWIFT

        avg_1 = np.mean(self.all_contexts[-self.avg_len:], axis=0)
        avg_1 = (avg_1 - self.min_context) / (self.max_context - self.min_context)
        avg_1[np.where(np.isnan(avg_1))] = 0
        avg_2 = np.mean(self.all_contexts[-2*self.avg_len: -self.avg_len], axis=0)
        avg_2 = (avg_2 - self.min_context) / (self.max_context - self.min_context)
        avg_2[np.where(np.isnan(avg_2))] = 0

        avg_flag = np.abs(avg_1 - avg_2) > self.avg_thr
        avg_num = np.sum(avg_flag)
        avg_ratio = avg_num / self.all_contexts.shape[1]

        if avg_ratio >= self.avg_thr:
            self.avg_changed += 1
        elif self.avg_changed > 0:
            self.avg_changed += 1

    
        tmp_contexts = (self.all_contexts[-2*self.std_len:] - self.min_context) / (self.max_context - self.min_context)
        tmp_contexts[np.where(np.isnan(tmp_contexts))] = 0

        std_1 = np.std(tmp_contexts[-self.avg_len:], axis=0)
        std_2 = np.std(tmp_contexts[-2*self.avg_len: -self.avg_len], axis=0)

        std_flag = np.abs(std_1 - std_2) > self.std_thr
        std_num = np.sum(std_flag)
        std_ratio = std_num / self.all_contexts.shape[1]

        if std_ratio >= self.std_thr:
            self.std_changed += 1
        elif self.std_changed > 0:
            self.std_changed += 1

        if self.avg_changed > 0 and self.std_changed > 0:
            self.avg_changed = 0
            self.std_changed = 0
            return SWIFT
        elif self.avg_changed >= self.avg_len or self.std_changed >= self.std_len:
            self.avg_changed = 0
            self.std_changed = 0
            return DOUBTFUL
        else:
            return NOT_SWIFT




class TSDetectionRule:
    def __init__(self, avg_len=3, std_len=5, avg_thr=1.5, std_thr=2, target='mysql'):
        self.avg_threshold = avg_thr
        self.std_threshold = std_thr
        if target.startswith('mysql'):
            with open('./thr_test/thr_results/thrs/mysql_mean.pkl', 'rb') as f:
                self.avg_threshold = pkl.load(f) * 1.1
            with open('./thr_test/thr_results/thrs/mysql_std.pkl', 'rb') as f:
                self.std_threshold = pkl.load(f) * 1.1
        elif target.startswith('redis'):
            with open('./thr_test/thr_results/thrs/redis_mean.pkl', 'rb') as f:
                self.avg_threshold = pkl.load(f) * 1.1
            with open('./thr_test/thr_results/thrs/redis_std.pkl', 'rb') as f:
                self.std_threshold = pkl.load(f) * 1.1
        elif target.startswith('unix'):
            with open('./thr_test/thr_results/thrs/unix_mean.pkl', 'rb') as f:
                self.avg_threshold = pkl.load(f) * 1.1
            with open('./thr_test/thr_results/thrs/unix_std.pkl', 'rb') as f:
                self.std_threshold = pkl.load(f) * 1.1
        elif target.startswith('kafka'):
            with open('./thr_test/thr_results/thrs/kafka_mean.pkl', 'rb') as f:
                self.avg_threshold = pkl.load(f) * 1.1
                self.avg_threshold[6] = 0.6
            with open('./thr_test/thr_results/thrs/kafka_std.pkl', 'rb') as f:
                self.std_threshold = pkl.load(f) * 1.2
                self.std_threshold[6] = 0.6
        elif target.startswith('spark'):
            with open('./thr_test/thr_results/thrs/spark_mean.pkl', 'rb') as f:
                self.avg_threshold = pkl.load(f) * 1.1
                self.avg_threshold[6] = 0.6
            with open('./thr_test/thr_results/thrs/spark_std.pkl', 'rb') as f:
                self.std_threshold = pkl.load(f) * 1.2
                self.std_threshold[6] = 0.6
                
        if not target.startswith('mysql'):
            for i in range(len(self.avg_threshold)):
                if self.avg_threshold[i] == 0:
                    self.avg_threshold[i] = 1e-5
                if self.std_threshold[i] == 0:
                    self.std_threshold[i] = 1e-5
            logger.info("avg_threshold: ")
            logger.info(" ".join([str(i) for i in self.avg_threshold]))
            logger.info("std_threshold: ")
            logger.info(" ".join([str(i) for i in self.std_threshold]))
        else:
            logger.info("avg_threshold: ")
            logger.info(str(self.avg_threshold))
            logger.info("std_threshold: ")
            logger.info(str(self.std_threshold))

        self.target = target
        self.avg_changed = 0
        self.std_changed = 0
        self.m = avg_len
        self.n = std_len
        self.all_contexts = None


    def reset(self):
        self.all_contexts = np.array([self.all_contexts[-1]])

    def cleanup(self):
        self.all_contexts = None

    def update_context(self, context):
        # context = context[self.selected]
        if self.target != 'mysql':
            context = context[:-1]

        if self.all_contexts is None:
            self.all_contexts = context.reshape(1, -1)
        else:
            self.all_contexts = np.vstack((self.all_contexts, context))


    def compare(self, context_1, context_2):

        avg_log_gap = my_log10(context_1) - my_log10(context_2)
        if self.target != 'mysql':
            avg_log_gap = context_1 - context_2
            avg_log_gap = avg_log_gap[:-1]
        avg_flag = np.abs(avg_log_gap) > self.avg_threshold
        avg_num = np.sum(avg_flag)

        logger.warn("Suspect test num: %d" % avg_num)

        if self.target.startswith('redis'):
            if avg_num >= 2:
                return SWIFT
            else:
                return NOT_SWIFT
        elif self.target.startswith('unix'):
            if avg_num >= 2:
                return SWIFT
            else:
                return NOT_SWIFT
        elif self.target.startswith('kafka'):
            if avg_num >= 4 or avg_flag[6]:
                return SWIFT
            else:
                return NOT_SWIFT
        else:
            if avg_num >= 1:
                return SWIFT
            else:
                return NOT_SWIFT

    def check(self):

        if self.all_contexts is None or self.all_contexts.shape[0] < 2 * max(self.n, self.m):
            return NOT_SWIFT

        avg_1 = np.mean(self.all_contexts[-self.m:], axis=0)
        avg_2 = np.mean(self.all_contexts[-2*self.m: -self.m], axis=0)
        avg_log_gap = my_log10(avg_2) - my_log10(avg_1)
        if self.target != 'mysql':
            avg_log_gap = avg_2 - avg_1
            
        avg_flag = np.abs(avg_log_gap) > self.avg_threshold
        
        avg_num = np.sum(avg_flag)

        if self.target.startswith('redis'):
            if avg_num >= 2:
                self.avg_changed += 1
            elif self.avg_changed > 0:
                self.avg_changed += 1
        elif self.target.startswith('unix'):
            if avg_num >= 2:
                self.avg_changed += 1
            elif self.avg_changed > 0:
                self.avg_changed += 1
        elif self.target.startswith('kafka'):
            if avg_num >= 4 or avg_flag[6]:
                self.avg_changed += 1
            elif self.avg_changed > 0:
                self.avg_changed += 1
        else:
            if avg_num >= 10:
                self.avg_changed += 1
            elif self.avg_changed > 0:
                self.avg_changed += 1

        std_1 = np.std(self.all_contexts[-self.n:], axis=0)
        std_2 = np.std(self.all_contexts[-2*self.n: -self.n], axis=0)

        std_log_gap = my_log10(std_2) - my_log10(std_1)
        if self.target != 'mysql':
            std_log_gap = std_2 - std_1
        std_flag = np.abs(std_log_gap) > self.std_threshold
        std_num = np.sum(std_flag)

        if self.target.startswith('redis'):
            if std_num > 2:
                self.std_changed += 1
            elif self.std_changed > 0:
                self.std_changed += 1
        elif self.target.startswith('unix'):
            if std_num > 2:
                self.std_changed += 1
            elif self.std_changed > 0:
                self.std_changed += 1
        elif self.target.startswith('kafka'):
            if std_num > 4 or std_flag[6]:
                self.std_changed += 1
            elif self.std_changed > 0:
                self.std_changed += 1
        else:
            if std_num > 11:
                self.std_changed += 1
            elif self.std_changed > 0:
                self.std_changed += 1

        logger.info("%4d, %4d, %3d, %3d" % (avg_num, std_num, self.avg_changed, self.std_changed))
        # print("%4d, %4d, %3d, %3d" % (avg_num, std_num, self.avg_changed, self.std_changed))
        if self.avg_changed > 0 and self.std_changed > 0:
            self.avg_changed = 0
            self.std_changed = 0
            return SWIFT
        elif self.avg_changed >= 2 * self.m or self.std_changed >= 2 * self.n:
            self.avg_changed = 0
            self.std_changed = 0
            return DOUBTFUL
        else:
            return NOT_SWIFT



