import json
import pymysql.cursors
import os
import service.logg as logg
from datetime import datetime, timedelta
from os import getenv
import pymysql
from service.yaml import FilesConfig,Files,MySQLConfig


HOST_MYSQL = '127.0.0.1'
PORT_MYSQL = 3306
USER_MYSQL = 'root'
PASSWORD_MYSQL = 'root'
DB_MYSQL = 'BetterMe'
CHARSET_MYSQL = 'utf8mb4'
CURSORCLASS_MYSQL = pymysql.cursors.DictCursor


class MySqlCon:

    def __init__(self, mysql_config: MySQLConfig):

        
        self.log = logg.get_class_log(self)

        self._mysql_con = mysql_config.mysql_api

        self.log.debug("Trying to connect to My SQL {host}:{port}/{db}" , extra=mysql_config.mysql_api)
        self.connection= pymysql.connect(**mysql_config.mysql_api)
        self.log.info("Successfully connect to My SQL {host}:{port}/{db}" , extra=mysql_config.mysql_api)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.log.debug("Trying to close conection -> {host}:{port}/{db}" , extra=self._mysql_con)
            self.connection.close()
            self.log.debug("Successfully closed conection -> {host}:{port}/{db}" , extra=self._mysql_con)

        except:
            self.log.exception("Error with clossing conection {host}:{port}/{db}" , extra=self._mysql_con)


    def process_files(self, files_config: FilesConfig) -> None:
    
        self.log.debug('Groups_config={files_config}', extra={'files_config': files_config.__dict__})
        self._files = files_config.files
        for file in self._files:
            self._process_file(file)

    def _process_file(self, file: FilesConfig) -> None:
        self._file = file
        self.log.info('Start file processing')

        self.df =self._file._scan_dir()

        cursor=self.connection.cursor()
        sql = self._file.mysql_sql_query
        for i,row in self.df.iterrows():
            ### можно обернуть в try chatch что бы он игнорировал и не записывал данные если есть уже подписка
            cursor.execute(sql,tuple(row))
            self.connection.commit()
        cursor.close()

        self.log.info('Successfully file processing')
    

    def close(self):
        try:
            self.log.debug("Trying to close conection -> {host}:{port}/{db}" , extra=self._mysql_con)
            self.connection.close()
            self.log.debug("Successfully closed conection -> {host}:{port}/{db}" , extra=self._mysql_con)

        except:
            self.log.exception("Error with clossing conection {host}:{port}/{db}" , extra=self._mysql_con)

    

