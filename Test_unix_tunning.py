# License: MIT
import time
from datetime import datetime
import argparse
from functools import partial
from spaces import get_unix_space
from utils_unix import *
from build_utils import build_optimizer
from config import UNIX_ALL_TYPEs

parser = argparse.ArgumentParser()
parser.add_argument('--opt', type=str, default='contextBO',
                    choices=['contextBO_tsd', 'contextBO', 'GP', 'TPE'])
parser.add_argument('--context_type', type=str, default='special',
                    choices=['default', 'special'])
parser.add_argument('--warm_start_flag', action='store_true', default=False)
parser.add_argument('--safe_flag', action='store_true', default=False)
parser.add_argument('--backup_flag', action='store_true', default=False)

parser.add_argument('--task', type=str, default='contextBO_test')
parser.add_argument('--iter_num', type=int, default=40)
parser.add_argument('--task_swift', type=int, nargs='+', default=[0, 0])

args = parser.parse_args()
context_type = args.context_type
target = 'unix'
if context_type == 'default':
    target = 'unix_def'

tasks = []
task_swifts = args.task_swift
for i in range(len(task_swifts)):
    tasks.append(task_swifts[i: i+1])
logger.info("task swift" + str(tasks))

assert args.iter_num // 40 == len(tasks)


# Define Objective Function
def run_unix(config: Configuration, unix_e: UnixExecutor):
    unix_e.modify_conf(config)

    ori_metrics = unix_e.uc.get_internal_metrics(list(), 500, 3)
    results = unix_e.unix_run()
    metrics = unix_e.uc.metrics_handle(ori_metrics)
    res = decode_results_unix(results)

    return -res, -res, metrics


space = get_unix_space()
unix_e = UnixExecutor(
    unix_type=UNIX_ALL_TYPEs[tasks[0][0]],
    context_type=context_type
)
eval_func = partial(run_unix, unix_e=unix_e)
time.sleep(10)

# run default config
default_config = space.get_default_configuration()
unix_e.modify_conf(default_config)
ori_metrics = unix_e.uc.get_internal_metrics(list(), 1000, 3)
_ = unix_e.unix_run()
ini_context = unix_e.uc.metrics_handle(ori_metrics)
print("ini internal metrics: ", len(ini_context), ini_context)

opt_kwargs = {
    'config_space': space,
    'eval_func': eval_func,
    'ini_context': ini_context,
    'target': target,
    'task_str': unix_e.get_task_str()
}
optimizer = build_optimizer(args, **opt_kwargs)

# for i in range(optimizer.iter_num):
#     optimizer.run_one_iter()

for i in range(optimizer.iter_num):
    if i % 40 == 0:
        task = tasks[i // 40]
        unix_type = UNIX_ALL_TYPEs[task[0]]
        unix_e.set(unix_type=unix_type)
        logger.warn("task: ====== {} ======".format(unix_e.get_task_str()))
        optimizer.task_str = unix_e.get_task_str()
    optimizer.run_one_iter()


unix_e.uc.clean_up()
