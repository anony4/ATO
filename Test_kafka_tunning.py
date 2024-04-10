# License: MIT
import time
import argparse
from functools import partial
from spaces import get_kafka_space
from utils_kafka import *
from openbox import logger
from build_utils import build_optimizer
from config import KAFKA_ALL_PARTITIONs, KAFKA_ALL_RECORD_SIZEs

parser = argparse.ArgumentParser()
parser.add_argument('--opt', type=str, default='contextBO',
                    choices=['contextBO_tsd', 'contextBO', 'GP', 'TPE'])
parser.add_argument('--context_type', type=str, default='special',
                    choices=['default', 'special'])
parser.add_argument('--warm_start_flag', action='store_true', default=False)
parser.add_argument('--safe_flag', action='store_true', default=False)
parser.add_argument('--backup_flag', action='store_true', default=False)

parser.add_argument('--num-records', type=int, default=2000000)

parser.add_argument('--task', type=str, default='contextBO_test')
parser.add_argument('--iter_num', type=int, default=40)
parser.add_argument('--task_swift', type=int, nargs='+', default=[0, 0, 0, 0])

args = parser.parse_args()
context_type = args.context_type
target = 'kafka'
if context_type == 'default':
    target = 'kafka_def'

tasks = []
task_swifts = args.task_swift
for i in range(len(task_swifts) // 2):
    tasks.append(task_swifts[2*i: 2*(i+1)])
logger.info("task swift" + str(tasks))
print("task swift", tasks)

assert args.iter_num // 40 == len(tasks)


# Define Objective Function
def run_kafka(config: Configuration, kafka_e: KafkaExecutor):
    kafka_e.modify_conf(config)

    ori_metrics = kafka_e.kc.get_internal_metrics(list(), 1000, 1)
    results = kafka_e.kafka_run()

    metrics = kafka_e.kc.metrics_handle(ori_metrics)
    rps = decode_results_kafka(results)
    print(metrics)
    logger.info(" ".join([str(i) for i in metrics]))

    return -rps, -rps, metrics


space = get_kafka_space()
kafka_e = KafkaExecutor(
    partition=KAFKA_ALL_PARTITIONs[tasks[0][0]],
    record_size=KAFKA_ALL_RECORD_SIZEs[tasks[0][1]],
    num_records=args.num_records,
    context_type=context_type
)
eval_func = partial(run_kafka, kafka_e=kafka_e)
time.sleep(10)

# run default config
default_config = space.get_default_configuration()
kafka_e.modify_conf(default_config)
ori_metrics = kafka_e.kc.get_internal_metrics(list(), 300, 1)
_ = kafka_e.kafka_run()
ini_context = kafka_e.kc.metrics_handle(ori_metrics)
print("ini internal metrics: ", len(ini_context), ini_context)

opt_kwargs = {
    'config_space': space,
    'eval_func': eval_func,
    'ini_context': ini_context,
    'target': target,
    'task_str': kafka_e.get_task_str()
}
optimizer = build_optimizer(args, **opt_kwargs)

# for i in range(optimizer.iter_num):
#     optimizer.run_one_iter()

for i in range(optimizer.iter_num):
    if i % 40 == 0:
        task = tasks[i // 40]
        partition, record_size = KAFKA_ALL_PARTITIONs[task[0]], KAFKA_ALL_RECORD_SIZEs[task[1]]
        kafka_e.set(partition=partition, record_size=record_size)
        logger.warn("task: ====== {} ======".format(kafka_e.get_task_str()))
        optimizer.task_str = kafka_e.get_task_str()
    optimizer.run_one_iter()


try:
    kafka_e.kc.clean_up()
except:
    print("no need clean up")
