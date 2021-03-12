import argparse
import os
from service.yaml import MySQLConfig
from service.mysqlCon import MySqlCon
from service.yaml import FilesConfig
import service.logg as logg

if __name__ == "__main__":
    # Create log directory if it doesn't exist.

    parser = argparse.ArgumentParser(description='Push data of txt files to MySQL.')
    parser.add_argument('mysql_cfg', help='mysql config file to read')
    parser.add_argument('file_cfg', help='files config file to read')
    args = parser.parse_args()

    log_directory = 'log'
    log = logg.setup_logging('MySQL Server')
    log = logg.get_log("MySQL input data")


    mysql_config = MySQLConfig(args.mysql_cfg)
    files_config = FilesConfig(args.file_cfg)

    with (MySqlCon(mysql_config)) as mysql_sender:
        mysql_sender.process_files(files_config)
