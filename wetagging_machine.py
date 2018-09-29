#tagging流程设计
#1 正则模式  （动工）
#2 脚本逻辑模式 （动工）
#3 excel模块控制模式  （暂停，未了解接口情况）

import pandas as pd
import os

class Tagcode_deal:
    def __init__(self,file_name):
        self.file_dir = './config/Tagcode_home'
        self.file_name = file_name

    def submit_file(self):
        local_eva = {}
        exec(open(f"{self.file_dir}/{self.file_name}",encoding='UTF-8').read(),local_eva)
        if local_eva['script'] == 're':
            return local_eva['new_column_name'],local_eva['columns_type'],local_eva['rule'],local_eva['else_value']
        elif local_eva['script'] == 'script':
            pass

    def re_famula_tranfrom(self):
        *base_info, _rule, _else_value = self.submit_file()
        rule_info = [(rules.split("->",1)[0],rules.split("->",1)[1]) for rules in _rule]
        return base_info,rule_info,_else_value


class wetagging:
    def __init__(self,frame,file,debug = False):
        self.frame = frame
        self.tagfile_info,self.tag_rule,self.tag_else_value = Tagcode_deal(file).re_famula_tranfrom()
        self.new_columns,self.columns_type = self.tagfile_info
        self.frame[self.new_columns] = ""  #新建列
        self.debug = debug

    def hitting_tag(self):
        index_already_exist = []
        frame = self.frame
        for _rule in self.tag_rule:
            print(f'正在处理字段:{self.tagfile_info[0]}\n正在处理规则:{_rule}')
            tag_index = frame[eval(_rule[1])].index.tolist()
            tag_index = list(set(tag_index).difference(index_already_exist))

            if 'frame' in _rule[0]:
                frame.loc[tag_index,self.new_columns] = eval(_rule[0])
            else:
                frame.loc[tag_index,self.new_columns] = _rule[0]
            index_already_exist +=[i for i in tag_index]
            print(f'数量:{len(tag_index)}\n累计量:{len(index_already_exist)}')
        else_index = list(set(frame.index.tolist()).difference(index_already_exist))
        if 'frame' in self.tag_else_value:
            frame.loc[else_index, self.new_columns] = eval(self.tag_else_value)
        else:
            frame.loc[else_index, self.new_columns] = self.tag_else_value
        print(self.new_columns)
        frame[self.new_columns] = frame[self.new_columns].astype(self.columns_type )
        return frame





if __name__ == '__main__':
    # base_info, rule_info, else_value = Tagcode_deal('有效_01.txt').re_famula_tranfrom()
    # print(base_info, rule_info, else_value)
    pp = pd.read_excel(r"C:\Users\85442\Desktop\省市區整理.xlsx",sheetname='區域')
    aa = wetagging(pp,'有效_01.txt').hitting_tag()