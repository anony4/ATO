# License: MIT

import time
import argparse
from functools import partial
from spaces import get_redis_space
from utils_redis import *
from tune_ssh import TuneSSH
from config import *
from build_utils import build_optimizer


parser = argparse.ArgumentParser()
parser.add_argument('--opt', type=str, default='contextBO',
                    choices=['contextBO_tsd', 'contextBO', 'GP', 'TPE'])
parser.add_argument('--context_type', type=str, default='special',
                    choices=['default', 'special'])
parser.add_argument('--warm_start_flag', action='store_true', default=False)
parser.add_argument('--safe_flag', action='store_true', default=False)
parser.add_argument('--backup_flag', action='store_true', default=False)

parser.add_argument('--n', type=int, default=1000000)

parser.add_argument('--task', type=str, default='task_swift_40_120312')
parser.add_argument('--iter_num', type=int, default=40)
parser.add_argument('--task_swift', type=int, nargs='+', default=[1, 1, 1, 0, 1, 1])

args = parser.parse_args()
context_type = args.context_type
target = 'redis'
if context_type == 'default':
    target = 'redis_def'


tasks = []
task_swifts = args.task_swift
for i in range(len(task_swifts) // 3):
    tasks.append(task_swifts[3*i: 3*(i+1)])
logger.info("task swift" + str(tasks))

assert args.iter_num // 40 == len(tasks)


# Define Objective Function
def run_redis(config: Configuration, redis_e: RedisExecutor):
    redis_e.modify_conf(config)

    ori_metrics = redis_e.rc.get_internal_metrics(list(), 1000, 3)
    results = redis_e.redis_run()
    metrics = redis_e.rc.metrics_handle(ori_metrics)
    rps = decode_results_redis(results)

    return -rps, -rps, metrics


space = get_redis_space()
redis_e = RedisExecutor(
    redis_type=REDIS_ALL_TYPEs[tasks[0][0]],
    c=REDIS_ALL_Cs[tasks[0][1]],
    d=REDIS_ALL_Ds[tasks[0][2]],
    n=1000000,
    context_type=context_type
)
eval_func = partial(run_redis, redis_e=redis_e)
time.sleep(10)


# restart mysql services in 4250
tune_ssh = TuneSSH()
_, return_code = tune_ssh.exec_command(port=SERVER_PORT,
                                       command='/root/redis-6.0.5/src/redis-server /root/redis-6.0.5/redis.conf')

# run default config
default_config = space.get_default_configuration()
redis_e.modify_conf(default_config)
ori_metrics = redis_e.rc.get_internal_metrics(list(), 100, 3)
_ = redis_e.redis_run()
ini_context = redis_e.rc.metrics_handle(ori_metrics)
print("ini internal metrics: ", len(ini_context), ini_context)

redis_e.set(n=args.n)

opt_kwargs = {
    'config_space': space,
    'eval_func': eval_func,
    'ini_context': ini_context,
    'target': target,
    'task_str': redis_e.get_task_str()
}
optimizer = build_optimizer(args, **opt_kwargs)

# for i in range(optimizer.iter_num):
#
#     optimizer.run_one_iter()


for i in range(optimizer.iter_num):
    if i % 40 == 0:
        task = tasks[i // 40]
        redis_type, c, d = REDIS_ALL_TYPEs[task[0]], REDIS_ALL_Cs[task[1]], REDIS_ALL_Ds[task[2]]
        redis_e.set(redis_type=redis_type, c=c, d=d)
        logger.warn("task: ====== {} ======".format(redis_e.get_task_str()))
        optimizer.task_str = redis_e.get_task_str()
    optimizer.run_one_iter()

tune_ssh.close_ssh()
redis_e.rc.clean_up()


# for i in range(optimizer.iter_num):
#     if i == 0 or i == 40:
#         index = tasks[i // 40]
#         t, c, d = all_type[index[0]], all_c[index[1]], all_d[index[2]]
#         redis_e.set(redis_type=t, c=c, d=d)

#     if i == 81:
#         optimizer.reset()
#         optimizer.task_index += 1
#         optimizer.task_start_iter = optimizer.iter_id
#         optimizer.all_observations[-1][-1] = optimizer.task_index

#     optimizer.run_one_iter()


