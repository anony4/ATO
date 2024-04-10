# License: MIT
import time
from datetime import datetime
import argparse
from functools import partial
from spaces import get_spark_space
from utils_spark import *
from openbox import logger
from build_utils import build_optimizer
from config import SPARK_ALL_TYPEs

parser = argparse.ArgumentParser()
parser.add_argument('--opt', type=str, default='contextBO',
                    choices=['contextBO_tsd', 'contextBO', 'GP', 'TPE'])
parser.add_argument('--context_type', type=str, default='special',
                    choices=['default', 'special'])
parser.add_argument('--warm_start_flag', action='store_true', default=False)
parser.add_argument('--safe_flag', action='store_true', default=False)
parser.add_argument('--backup_flag', action='store_true', default=False)

parser.add_argument('--task', type=str, default='test_record_40')
parser.add_argument('--iter_num', type=int, default=40)
parser.add_argument('--task_swift', type=int, nargs='+', default=[0, 0])

args = parser.parse_args()
context_type = args.context_type
target = 'spark'
if context_type == 'default':
    target = 'spark_def'

tasks = []
task_swifts = args.task_swift
for i in range(len(task_swifts)):
    tasks.append(task_swifts[i: i+1])
logger.info("task swift" + str(tasks))

assert args.iter_num // 40 == len(tasks)


# Define Objective Function
def run_hibench(config: Configuration, spark_e: SparkExecutor):
    spark_e.modify_conf(config)

    ori_metrics = spark_e.sc.get_internal_metrics(list(), 1000, 3)
    results = spark_e.spark_run()
    metrics = spark_e.sc.metrics_handle(ori_metrics)

    run_time, context = decode_results_spark(results)
    if metrics is not None:
        context = metrics

    print(context)
    logger.info(" ".join([str(x) for x in context]))

    return run_time, run_time, context


space = get_spark_space()
timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
exe_infos = dict(
    # 运行路径
    workloads_dir='/root/HiBench/bin/workloads/',
    task_run_path='spark/run.sh',
    task_prepare_path='prepare/prepare.sh',
    # log 路径
    remote_log_dir='/root/test/spark-log',
    local_log_dir='./results/%s-log' % target,
    # 配置路径
    remote_conf_dir='/root/HiBench/conf/',
    local_conf_dir="./confs/%s/" % target,
    # 应用名称
    local_app_name="%s_%s_%s" % (args.opt, args.task, timestamp),
)
spark_e = SparkExecutor(
    spark_type=SPARK_ALL_TYPEs[tasks[0][0]],
    exe_infos=exe_infos,
    context_type=context_type
)
eval_func = partial(run_hibench, spark_e=spark_e)
time.sleep(15)

# run default config
default_config = space.get_default_configuration()
spark_e.modify_conf(default_config)
ori_metrics = spark_e.sc.get_internal_metrics(list(), 1000, 3)
results = spark_e.spark_run()
metrics = spark_e.sc.metrics_handle(ori_metrics)

run_time, ini_context = decode_results_spark(results)
if metrics is not None:
    ini_context = metrics
print("ini internal metrics: ", len(ini_context), ini_context)

opt_kwargs = {
    'config_space': space,
    'eval_func': eval_func,
    'ini_context': ini_context,
    'target': target,
    'task_str': spark_e.get_task_str()
}
optimizer = build_optimizer(args, **opt_kwargs)

# for i in range(optimizer.iter_num):
#     optimizer.run_one_iter()

for i in range(optimizer.iter_num):
    if i % 40 == 0:
        task = tasks[i // 40]
        spark_type = SPARK_ALL_TYPEs[task[0]]
        spark_e.set(spark_type=spark_type)
        logger.warn("task: ====== {} ======".format(spark_e.get_task_str()))
        optimizer.task_str = spark_e.get_task_str()
    optimizer.run_one_iter()

spark_e.clean_up()
