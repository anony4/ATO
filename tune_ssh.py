import paramiko
from openbox import logger
from config import *


class TuneSSH:
    def __init__(self):

        transport = paramiko.Transport((SERVER_IP, SERVER_PORT))
        transport.connect(username=SERVER_USER, password=SERVER_PASSWD)
        self.ftp_4250 = paramiko.SFTPClient.from_transport(transport)  # 定义一个ftp实例

        self.cli_4250 = paramiko.SSHClient()
        self.cli_4250.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.cli_4250.connect(hostname=SERVER_IP, port=SERVER_PORT, username=SERVER_USER, password=SERVER_PASSWD)

        self.cli_4260 = paramiko.SSHClient()
        self.cli_4260.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.cli_4260.connect(hostname=CLIENT_IP, port=CLIENT_PORT, username=CLIENT_USER, password=CLIENT_PASSWD)

    def get_file(self, remote_path: str = None, local_path: str = None):
        try:
            self.ftp_4250.get(remotepath=remote_path, localpath=local_path)
            return True
        except:
            return False

    def put_file(self, local_path: str = None, remote_path: str = None):
        try:
            self.ftp_4250.put(localpath=local_path, remotepath=remote_path)
            return True
        except:
            return False

    def exec_command_async(self, port: int, command: str):
        assert port in [SERVER_PORT, CLIENT_PORT]
        ssh = self.cli_4250
        if port == CLIENT_PORT:
            ssh = self.cli_4260

        stdin, stdout, stderr = ssh.exec_command(command,  get_pty=True)
        # results = stdout.read().decode('utf-8')

        return stdout

    def exec_command(self, port: int, command: str):
        assert port in [SERVER_PORT, CLIENT_PORT]
        ssh = self.cli_4250
        if port == CLIENT_PORT:
            ssh = self.cli_4260

        stdin, stdout, stderr = ssh.exec_command(command)
        results = stdout.read().decode('utf-8')
        return_code = 0
        if command == 'service mysqld restart':
            return_code = stdout.channel.recv_exit_status()
        # print("return_code: %d" % return_code, results)
        # logger.warn("return_code: %d, result: %s" % (return_code, results))
        return results, return_code

    def close_ssh(self):
        self.ftp_4250.close()
        self.cli_4250.close()
        self.cli_4260.close()
