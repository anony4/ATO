CLIENT_IP = '162.105.146.120'
CLIENT_PORT = 4260
CLIENT_USER = 'root'
CLIENT_PASSWD = 'automl425'

SERVER_IP = '162.105.146.120'
SERVER_PORT = 4250
SERVER_USER = 'root'
SERVER_PASSWD = 'automl425'


MYSQL_USER = 'admin'
MYSQL_PASSWD = 'automl425'
MYSQL_PORT = 3307


REDIS_PORT = 6379


UNIX_PORT = 4250


MYSQL_ALL_TYPEs = ['oltp_write_only', 'oltp_read_only', 'oltp_read_write']
MYSQL_ALL_TABLEs = [2, 8]
MYSQL_ALL_TABLE_SIZEs = [100, 100000]

REDIS_ALL_TYPEs = ['set', 'get', 'sadd', 'mset']
REDIS_ALL_Cs = [10, 100, 1000]
REDIS_ALL_Ds = [1, 5, 10]

KAFKA_ALL_PARTITIONs = [1, 5, 10]
KAFKA_ALL_RECORD_SIZEs = [256, 1024, 4096]

SPARK_ALL_TYPEs = [
    'sleep', 'sort', 'terasort', 'wordcount', 'repartition',
    'aggregation', 'join', 'scan', 'pagerank', 'bayes',
    'kmeans', 'lr', 'als', 'pca', 'gbt',
    'rf', 'svd', 'linear', 'lda', 'svm',
    'gmm', 'correlation', 'summarizer', 'nweight'
]

UNIX_ALL_TYPEs = ['dhry2reg', 'whets', 'syscall', 'pipe', 'context1', 'spawn', 'execl',
            'fstime-w', 'fstime-r', 'fsdisk-w', 'fsdisk-r', 'hanoi']