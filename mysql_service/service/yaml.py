import service.logg as logg
import os
from typing import Dict, List, Union
import pandas as pd
import numpy as np
import os
import yaml


class MysqlYAML(yaml.YAMLObject):
    yaml_tag = '!MySQLConfig'


class MySQLConfig():
    def __init__(self, mysql_config_filename: str):
    
        super().__init__()
        self.log = logg.get_class_log(self)
        self._yaml_filename = mysql_config_filename
        self.log.info('Reading MySQL Config file "{mysql_config_filename}"',
                      extra={'mysql_config_filename': mysql_config_filename})
        with open(mysql_config_filename, 'r', encoding='utf-8') as mysql_cfg:
            self._mysql_config = yaml.load(mysql_cfg)

    @property
    def mysql_api(self) -> Dict[str, Union[str, int]]:
        return self._mysql_config.api


class Files(yaml.YAMLObject):
    yaml_tag = '!Files'
    version = 'undefined'
    files_dir = '.'
    files_pattern = '.*\\.txt'
    include_columns = []
    to_str = []
    table = 'Purchases'
    sql_query = ' '

    def _scan_dir(self) -> pd.DataFrame:

        data_folder = os.path.join(os.curdir, self.files_dir)
        #створюємо датафрейм
        df = pd.DataFrame()
        for file in os.listdir(data_folder):
            if os.path.isfile(os.path.join(data_folder,file)) and os.path.splitext(os.path.join(data_folder,file))[1].lower() in (self.files_pattern):
                df = pd.concat([df, pd.read_csv(os.path.join(data_folder,file), sep="\t", index_col=None)])

        df = pd.DataFrame(df,
                 columns=list(self.include_columns))
        df = df.where(pd.notnull(df), None)
        for column in self.to_str:
            df[column] = df[column].apply(str)
        return df

    @property
    def mysql_sql_query(self) -> str:
        return self.sql_query

    @property
    def mysql_sql_table(self) -> str:
        return self.table




class FilesConfig():
    def __init__(self, config_file: str) -> None:
        super().__init__()
        self.log = logg.get_class_log(self)
        self._yaml_filename = config_file
        self.log.info('Reading Files  file "{config_file}"',
                      extra={'config_file': config_file})
        with open(config_file, 'r', encoding='utf-8') as files_config:
            self._files_config = yaml.load(files_config)

    @property
    def files(self):
        """
        :return: file group configration to read.
        """
        return list(self._files_config)



