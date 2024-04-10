import threading
from openbox import logger
import multiprocessing as mp
import numpy as np
import pymysql
import re
import os
from ConfigSpace import Configuration
from tune_ssh import TuneSSH
from config import *
from utils_collector import *


class MysqlConnector:
    def __init__(self, name='sbtest', socket=''):
        super().__init__()
        self.db_host = SERVER_IP
        self.db_port = MYSQL_PORT
        self.db_user = MYSQL_USER
        self.db_passwd = MYSQL_PASSWD
        self.dbname = name
        self.sock = socket
        self.conn = self.connect_db()
        if self.conn:
            self.cursor = self.conn.cursor()

        # MySQL Internal Metrics
        self.num_metrics = 74 - 10
        self.value_type_metrics = [
            'lock_deadlocks', 'lock_timeouts', 'lock_row_lock_time_max',
            'lock_row_lock_time_avg', 'buffer_pool_size', 'buffer_pool_pages_total',
            'buffer_pool_pages_misc', 'buffer_pool_pages_data', 'buffer_pool_bytes_data',
            'buffer_pool_pages_dirty', 'buffer_pool_bytes_dirty', 'buffer_pool_pages_free',
            'trx_rseg_history_len', 'file_num_open_files', 'innodb_page_size'
        ]
        self.unkonwn_metrics = [
            'dml_system_deletes',
            'dml_system_inserts',
            'dml_system_reads',
            'dml_system_updates',
            'lock_deadlock_false_positives',
            'lock_deadlock_rounds',
            'lock_rec_grant_attempts',
            'lock_rec_release_attempts',
            'lock_schedule_refreshes',
            'lock_threads_waiting',
        ]
        self.im_alive_init()  # im collection signal
        self.cur_timer = None

    def im_alive_init(self):
        global im_alive
        im_alive = mp.Value('b', True)

    def connect_db(self):
        conn = False
        if self.sock:
            conn = pymysql.connect(host=self.db_host,
                                   user=self.db_user,
                                   passwd=self.db_passwd,
                                   db=self.dbname,
                                   port=self.db_port,
                                   unix_socket=self.sock)
        else:
            conn = pymysql.connect(host=self.db_host,
                                   user=self.db_user,
                                   passwd=self.db_passwd,
                                   db=self.dbname,
                                   port=self.db_port)
        return conn

    def close_db(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def fetch_results(self, sql, json=True):
        results = False
        if self.conn:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            if json:
                columns = [col[0] for col in self.cursor.description]
                return [dict(zip(columns, row)) for row in results]
        return results

    def execute(self, sql):
        results = False
        if self.conn:
            self.cursor.execute(sql)

    def modify_variable(self, variable_name, variable_value):
        sql = "SET GLOBAL %s=%s;" % (variable_name, str(variable_value))
        self.cursor.execute(sql)


    def show_variable(self, variable_name):
        sql = "SHOW VARIABLES LIKE '%s';" % variable_name
        results = self.fetch_results(sql, False)

        return results[0]

    def get_internal_metrics(self, internal_metrics, BENCHMARK_RUNNING_TIME, BENCHMARK_WARMING_TIME):
        """Get the all internal metrics of MySQL, like io_read, physical_read.
        This func uses a SQL statement to lookup system table: information_schema.INNODB_METRICS
        and returns the lookup result.
        """
        _counter = 0
        _period = 10  # observe internal metrics every 10 sec.
        count = (BENCHMARK_RUNNING_TIME + BENCHMARK_WARMING_TIME) / _period - 1
        warmup = BENCHMARK_WARMING_TIME / _period

        def collect_metric(counter):
            counter += 1
            self.cur_timer = threading.Timer(float(_period), collect_metric, (counter,))
            self.cur_timer.start()
            if counter >= count or not im_alive.value:
                self.cur_timer.cancel()
            if counter > warmup:
                try:
                    # print('collect internal metrics {}'.format(counter))

                    sql = 'SELECT NAME, COUNT from information_schema.INNODB_METRICS where status="enabled" ORDER BY NAME'
                    res = self.fetch_results(sql, json=False)
                    res_dict = {}
                    for (k, v) in res:
                        res_dict[k] = v

                    for _k in self.unkonwn_metrics:
                        res_dict.pop(_k)

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
                return float(metric_values[-1] - metric_values[0]) * 23 / len(metric_values)
            else:
                return float(sum(metric_values)) / len(metric_values)

        result = np.zeros(self.num_metrics)
        keys = list(metrics[0].keys())
        keys.sort()
        total_pages = 0
        dirty_pages = 0
        request = 0
        reads = 0
        page_data = 0
        page_size = 0
        page_misc = 0
        for idx in range(len(keys)):
            key = keys[idx]
            data = [x[key] for x in metrics]
            result[idx] = do(key, data)
            if key == 'buffer_pool_pages_total':
                total_pages = result[idx]
            elif key == 'buffer_pool_pages_dirty':
                dirty_pages = result[idx]
            elif key == 'buffer_pool_read_requests':
                request = result[idx]
            elif key == 'buffer_pool_reads':
                reads = result[idx]
            elif key == 'buffer_pool_pages_data':
                page_data = result[idx]
            elif key == 'innodb_page_size':
                page_size = result[idx]
            elif key == 'buffer_pool_pages_misc':
                page_misc = result[idx]
        dirty_pages_per = dirty_pages / total_pages
        hit_ratio = request / float(request + reads)
        page_data = (page_data + page_misc) * page_size / (1024.0 * 1024.0 * 1024.0)

        return np.hstack((result, [dirty_pages_per, hit_ratio, page_data]))


    def clean_up(self):
        pass
        

class SysbenchExecutor:
    def __init__(self, sys_type: str = 'oltp_write_only', threads: int = 2, tables: int = 2,
                 table_size: int = 100, time: int = 60, context_type='special'):
        self.sys_type = sys_type
        self.threads = threads
        self.tables = tables
        self.table_size = table_size
        self.sql_tables = tables
        self.sql_table_size = table_size
        self.time = time

        self.tune_ssh = TuneSSH()

        self.mc = None
        if context_type == 'special':
            self.mc = MysqlConnector()
        elif context_type == 'default':
            self.mc = AtuneConnector()
        else:
            raise Exception('context_flag 错误!')

        self.sysbench_cleanup()
        self.sysbench_prepare(self.tables, self.table_size)

    def get_task_str(self):
        sys_s = 'w'
        if self.sys_type == 'oltp_read_only':
            sys_s = 'r'
        elif self.sys_type == 'oltp_read_write':
            sys_s = 'rw'

        table_s = str(self.tables)
        size_s = str(self.table_size)
        if self.table_size == 100000:
            size_s = '100k'

        return "{}*{}*{}".format(sys_s, table_s, size_s)

    # 向数据库中插入表
    def sysbench_prepare(self, tables=2, table_size=100):
        command = "sysbench oltp_common --threads=4 --events=0 --mysql-host=%s --mysql-port=%d " \
                  "--mysql-user=%s --mysql-password=%s --tables=%s --table_size=%s prepare" % (SERVER_IP, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWD, tables, table_size)

        results, return_code = self.tune_ssh.exec_command(CLIENT_PORT, command)
        if return_code != 0:
            raise Exception("sysbench prepare errors!")
        else:
            print(results)

    # 清空数据库中的所有表
    def sysbench_cleanup(self, tables=20):
        command = "sysbench oltp_common --threads=4 --events=0 --mysql-host=%s --mysql-port=%d " \
                  "--mysql-user=%s --mysql-password=%s --tables=%s cleanup" % (SERVER_IP, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWD, tables)

        results, return_code = self.tune_ssh.exec_command(CLIENT_PORT, command)
        if return_code != 0:
            raise Exception("sysbench cleanup errors!")
        else:
            print("cleanup mysql!")

    def sysbench_run(self):
        command = "sysbench %s --threads=%d --events=0 --mysql-host=%s --mysql-port=%d " \
               "--mysql-user=%s --mysql-password=%s --tables=%d --table_size=%d --time=%d run; " \
               % (self.sys_type, self.threads, SERVER_IP, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWD, self.tables, self.table_size, self.time)

        logger.info(command)
        results, return_code = self.tune_ssh.exec_command(CLIENT_PORT, command)
        if return_code != 0:
            raise Exception("sysbench run errors!")

        self.mc.cur_timer.cancel()
        return results

    def apply_knob(self, knob):

        self.modify_conf(knob)
        ori_metrics = self.mc.get_internal_metrics(list(), self.time, 5)
        results = self.sysbench_run()
        context = self.mc.metrics_handle(ori_metrics)

        tps, qps = decode_results_mysql(results)

        return context, tps

    def set(self, **kwargs):
        tmp_tables = self.tables
        tmp_table_size = self.table_size
        for k, v in kwargs.items():
            assert k in ['sys_type', 'tables', 'table_size', 'time']
            if k == 'sys_type':
                assert v in ['oltp_write_only', 'oltp_read_only', 'oltp_read_write']
                self.sys_type = v
            elif k == 'time':
                self.time = v
            elif k == 'tables':
                tmp_tables = v
            elif k == 'table_size':
                tmp_table_size = v

        if tmp_tables != self.tables or tmp_table_size != self.table_size:
            if tmp_tables > self.sql_tables or tmp_table_size > self.sql_table_size:
                self.sysbench_cleanup(self.sql_tables)
                self.sysbench_prepare(tmp_tables, tmp_table_size)
                self.sql_tables, self.sql_table_size = tmp_tables, tmp_table_size
            self.tables, self.table_size = tmp_tables, tmp_table_size

    def modify_conf(self, config: Configuration):
        if not isinstance(config, dict):
            config = config.get_dictionary()
        dic = config
        new_dic = dict()
        tmp_mc = MysqlConnector()
        for key, value in dic.items():
            tmp_mc.modify_variable(key, value)
            new_value = tmp_mc.show_variable(key)
            new_dic[key] = new_value
        print("new config", new_dic)
        # logger.info(str(new_dic))



# update change.cnf according to config
def modify_conf_file(cnf_dir: str, config: Configuration):
    dic = config.get_dictionary()
    conf_file = os.path.join(cnf_dir, 'change.cnf')
    tmp_file = os.path.join(cnf_dir, 'change_tmp.cnf')
    with open(conf_file, 'r') as f, open(tmp_file, 'w') as ft:
        for line in f:
            line_key = line.split("=")[0]
            for key in dic.keys():
                if line_key == key:
                    line = '%s=%s\n' % (key, dic[key])
                    logger.debug('Modify conf file [%s] with [%r]' % (conf_file, line))
                    break
            ft.write(line)
    os.remove(conf_file)
    os.rename(tmp_file, conf_file)


def decode_results_mysql(results: str):
    re_list = re.findall(r'\(\d+\.\d+\s+per sec\.\)', results)

    tps = 0
    qps = 0
    try:
        tps = float(re_list[0].split(' ')[0][1:])
        qps = float(re_list[1].split(' ')[0][1:])
    except:
        logger.warn(results)
        print(results)

    return tps, qps