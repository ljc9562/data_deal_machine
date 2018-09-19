###
# 是一个多数据源匹配整合工具
###
import pandas as pd
from os import getcwd, listdir
import sys


class Input_Data:
    def __init__(self, aim_soure, match_soure):
        self.aim = aim_soure
        self.match = match_soure

    def whats_this(self):

    def read_file(self):


class Both_collect:
    def __init__(self,aim_columns_name,match_columns_name,needed_columns,datasoure_left,datasoure_right):
        self.aim_columns_name = aim_columns_name
        self.match_columns_name = match_columns_name
        self.needed_columns = needed_columns
        self.Data_orginal_left = datasoure_left
        self.Data_orginal_right = datasoure_right

    def one_on_one(self):

    def one_on_more(self):

    def more_on_more(self):

    def judge_match_relationship(self):
        if len(self.aim_columns_name) == 1 and len(self.match_columns_name) == 1:
            _one_on_one = Both_collect.one_on_one()
            return  _one_on_one
        elif len(self.aim_columns_name) == 1 and len(self.match_columns_name) > 1:
            _one_on_more = Both_collect.more_on_more()
            return  _one_on_more
        elif len(self.aim_columns_name) > 1 and len(self.match_columns_name) > 1:
            _more_on_more = Both_collect.more_on_more()
            return _more_on_more


class Output:
    def __init__(self):

    def sql_output(self):

    def file_output(self):


class welookup:
    def __init__(self, aim_soure, match_soure, aim_columns_name, match_columns_name, needed_columns):
        self.aim_soure, self.match_soure, self.aim_columns_name, self.match_columns_name, self.needed_columns = aim_soure, match_soure, aim_columns_name, match_columns_name, needed_columns

    def summary(self):
        aim_soure, match_soure = Input_Data(self.aim_soure,self.match_soure).read_file()
        combine_dataframe = Both_collect(self.aim_columns_name,self.match_columns_name,self.match_info,aim_soure,match_soure)


if __name__ == '__main__':
    run = welookup(aim_dir=sys.argv[1], match_dir=sys.argv, aim_columns_name=sys.argv, match_columns_name=sys.argv,
                   match_info=sys.argv)
