###
# 是一个多数据源匹配整合工具
#1#识别数据源
#2#判断匹配模式，执行一对一，一对多，多对多的匹配
#3#输出Dataframe
###
#author:中国婚博会（广州）数据部
#email:854426089@qq.com

import pandas as pd
from os import getcwd, listdir
import sys


class Input_Data:
    def __init__(self, aim_soure, match_soure):
        self.aim = aim_soure
        self.match = match_soure

    def whats_this(self):
        pass

    def read_file(self):
        return self.aim,self.match


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


class welookup:
    def __init__(self, aim_soure, match_soure, aim_columns_name, match_columns_name, needed_columns):
        '''
        -----------------------------------------------------------------------------------------------------------------
        :param aim_soure: 目标数据源（匹配合并数据）
        :param match_soure: 需要被合并的数据源
        :param aim_columns_name: 用于左连接的key列名
        :param match_columns_name: 用于被连接的key列名
        :param needed_columns: 需要被匹配到aim_soure中的列名
        -----------------------------------------------------------------------------------------------------------------
        '''
        self.aim_soure, self.match_soure, self.aim_columns_name, self.match_columns_name, self.needed_columns = aim_soure, match_soure, aim_columns_name, match_columns_name, needed_columns

    def summary(self):
        '''
        -----------------------------------------------------------------------------------------------------------------
        主程序：提取读取数据，进行数据合并
        -----------------------------------------------------------------------------------------------------------------
        参数说明：
        Input_Data(aim_soure[目标数据],match_soure[被匹配数据]) ps:输入的数据类型：dataframe
        Both_collect(aim_columns_name[目标匹配的key列],match_columns_name[被匹配的key列],needed_columns[需要匹配内容的列],->
        ->  aim_soure[左表数据源dataframe],match_soure[右表数据源dataframe])  ps:多个列名用“,”分割
        -----------------------------------------------------------------------------------------------------------------
        :return: combine_dataframe 是一个数据结构是pandas的Dataframe（合并后的结果）
        -----------------------------------------------------------------------------------------------------------------
        '''
        aim_soure, match_soure = Input_Data(self.aim_soure,self.match_soure).read_file()
        combine_dataframe = Both_collect(self.aim_columns_name,self.match_columns_name,self.needed_columns,aim_soure,match_soure)
        return combine_dataframe


if __name__ == '__main__':
    run = welookup(aim_dir=sys.argv[1], match_dir=sys.argv, aim_columns_name=sys.argv, match_columns_name=sys.argv,
                   match_info=sys.argv)
