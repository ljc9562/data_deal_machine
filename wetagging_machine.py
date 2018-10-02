#tagging流程设计
#1 正则模式  （动工）
#2 脚本逻辑模式 （动工）
#3 excel模块控制模式  （暂停，未了解接口情况）

import pandas as pd

class Tagcode_deal:
    def __init__(self,file_name):
        self.file_dir = './config/Tagcode_home'
        self.file_name = file_name

    def submit_file(self):
        local_eva = {}
        category = open(f"{self.file_dir}/{self.file_name}", encoding='utf-8').read().split("\n", 1)[0].split(':')[1]  #判断第一行声明的类型
        if category == 're':
            exec(open(f"{self.file_dir}/{self.file_name}", encoding='UTF-8').read(), local_eva)
            base_info, _rule, _else_value =  [local_eva['new_column_name'],local_eva['columns_type']],local_eva['rule'],local_eva['else_value']
            rule_info = [(rules.split("->", 1)[0], rules.split("->", 1)[1]) for rules in _rule]
            return base_info, rule_info, _else_value,category
        elif category == 'script':
            return f"{self.file_dir}/{self.file_name}",category


class wetagging:
    def __init__(self,frame,file,debug = False):
        self.frame = frame
        try:
            self.tagfile_info,self.tag_rule,self.tag_else_value,self.category = Tagcode_deal(file).submit_file()
            self.new_columns,self.columns_type = self.tagfile_info
            self.frame[self.new_columns] = ""  #新建列
        except:
            self.script_dir,self.category = Tagcode_deal(file).submit_file()
        self.debug = debug

    def re_hitting_tag(self,frame):
        frame = frame
        index_already_exist = []
        for _rule in self.tag_rule:
            print(f'正在处理字段:{self.tagfile_info[0]}\n正在处理规则:{_rule}')
            tag_index = frame[eval(_rule[1])].index.tolist()
            tag_index = list(set(tag_index).difference(index_already_exist))

            if 'frame' in _rule[0]:
                # print(_rule[0])
                frame.loc[tag_index,self.new_columns] = eval(_rule[0])
                print(eval(_rule[0]))
                print(frame.loc[tag_index,:].head(10))
            else:
                frame.loc[tag_index,self.new_columns] = _rule[0]
            index_already_exist +=[i for i in tag_index]
            print(f'数量:{len(tag_index)}\n累计量:{len(index_already_exist)}')
        else_index = list(set(frame.index.tolist()).difference(index_already_exist))
        if 'frame' in self.tag_else_value:
            frame.loc[else_index, self.new_columns] = eval(self.tag_else_value)
        else:
            frame.loc[else_index, self.new_columns] = self.tag_else_value
        print(f'else值:{self.tag_else_value}\n数量{len(else_index)}')
        frame[self.new_columns] = frame[self.new_columns].astype(self.columns_type )
        return frame

    def summary(self):
        '''
        提交标签
        :return:
        '''
        frame = self.frame
        if self.category == 'script':
            #script 标签脚本提交
            _local_eva = {}
            exec(open(self.script_dir, encoding='UTF-8').read())
            return frame
        else:
            result = self.re_hitting_tag(frame)
            return result

class wetagging_group:
    def __init__(self,frame,group_name):
        self.frame = frame
        self.group_name = group_name

    def summary(self):









if __name__ == '__main__':
    pp = pd.read_excel("./config/test_file/省市區整理.xlsx",sheetname='區域')
    for i in ['有效_01.txt','script_example_01.txt']:
         aa = wetagging(pp,i).summary()
    # print(aa)