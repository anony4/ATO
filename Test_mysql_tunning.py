# License: MIT
import argparse
from functools import partial
from spaces import get_mysql_space, get_huge_mysql_space
from utils_mysql import *
import time
from tune_ssh import TuneSSH
from config import *
from build_utils import build_optimizer
from openbox import logger

parser = argparse.ArgumentParser()
parser.add_argument('--opt', type=str, default='contextBO',
                    choices=['contextBO_tsd', 'contextBO', 'GP', 'TPE'])
parser.add_argument('--context_type', type=str, default='special',
                    choices=['default', 'special'])
parser.add_argument('--warm_start_flag', action='store_true', default=False)
parser.add_argument('--safe_flag', action='store_true', default=False)
parser.add_argument('--backup_flag', action='store_true', default=False)

parser.add_argument('--threads', type=int, default=2)
parser.add_argument('--time', type=int, default=40)

parser.add_argument('--task', type=str, default='test_swift_40_111011')
parser.add_argument('--iter_num', type=int, default=40)
parser.add_argument('--task_swift', type=int, nargs='+', default=[1, 1, 1, 0, 1, 1])

args = parser.parse_args()
context_type = args.context_type
target = 'mysql_huge'
if context_type == 'default':
    target = 'mysql_def'

tasks = []
task_swifts = args.task_swift
for i in range(len(task_swifts) // 3):
    tasks.append(task_swifts[3*i: 3*(i+1)])
logger.info("task swift" + str(tasks))
print("task swift", tasks)

assert args.iter_num // 40 == len(tasks)

# Define Objective Function
def run_sysbench(config: Configuration, sys_e: SysbenchExecutor):
    sys_e.modify_conf(config)

    # run sysbench in 4260
    ori_metrics = sys_e.mc.get_internal_metrics(list(), sys_e.time, 5)
    results = sys_e.sysbench_run()
    context = sys_e.mc.metrics_handle(ori_metrics)
    tps, qps = decode_results_mysql(results)

    return -tps, -qps, context


space = get_mysql_space()
if 'huge' in target:
    space = get_huge_mysql_space()

sys_e = SysbenchExecutor(
    sys_type=MYSQL_ALL_TYPEs[tasks[0][0]],
    tables= MYSQL_ALL_TABLEs[tasks[0][1]],
    table_size=MYSQL_ALL_TABLE_SIZEs[tasks[0][2]],
    threads=args.threads,
    time=30, context_type=context_type
)
eval_func = partial(run_sysbench, sys_e=sys_e)
time.sleep(10)


tune_ssh = TuneSSH()
# _, return_code = tune_ssh.exec_command(port=SERVER_PORT,
#                                        command='service mysqld restart')


# run default config
default_config = space.get_default_configuration()


sys_e.modify_conf(default_config)
ori_metrics = sys_e.mc.get_internal_metrics(list(), 30, 5)
_ = sys_e.sysbench_run()
ini_context = sys_e.mc.metrics_handle(ori_metrics)
print("ini internal metrics: ", len(ini_context), ini_context)

sys_e.set(time=args.time)

opt_kwargs = {
    'config_space': space,
    'eval_func': eval_func,
    'ini_context': ini_context,
    'target': target,
    'task_str': sys_e.get_task_str()
}
optimizer = build_optimizer(args, **opt_kwargs)

# for i in range(optimizer.iter_num):
#     optimizer.run_one_iter()

for i in range(optimizer.iter_num):
    if i % 40 == 0:
        task = tasks[i // 40]
        sys_type, tables, table_size = MYSQL_ALL_TYPEs[task[0]], MYSQL_ALL_TABLEs[task[1]], MYSQL_ALL_TABLE_SIZEs[task[2]]
        sys_e.set(sys_type=sys_type, tables=tables, table_size=table_size)
        logger.warn("task: ====== {} ======".format(sys_e.get_task_str()))
        optimizer.task_str = sys_e.get_task_str()
    optimizer.run_one_iter()

tune_ssh.close_ssh()
sys_e.mc.clean_up()
