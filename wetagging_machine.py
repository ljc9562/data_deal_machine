#tagging流程设计
#1 正则模式  （动工）
#2 脚本逻辑模式 （动工）
#3 excel模块控制模式  （暂停，未了解接口情况）

import pandas as pd
import matplotlib


class Tagcode_deal:
    '''
    用途:用于读取标签配置文件和预处理
    '''
    def __init__(self,file_name):
        '''
        导入标签文件的地址
        :param file_dir: 放标签文件的目录,地址可以是相对地址可以是绝对地址(目录需要用"/"分割)
        :param file_name: 标签配置文件的文件名
        '''
        self.file_dir = './config/Tagcode_home'
        self.file_name = file_name

    def submit_file(self):
        '''
        提交re标签文件,获取文件内的变量值
        :return: 变量值,base_info(新列名,新列字段类型), rule_info(规则信息), _else_value(否则标签),category(配置文件类型)
        '''
        local_eva = {}
        category = open(f"{self.file_dir}/{self.file_name}", encoding='utf-8').read().split("\n", 1)[0].split(':')[1]  #判断第一行声明的类型
        if category == 're':
            exec(open(f"{self.file_dir}/{self.file_name}", encoding='UTF-8').read(), local_eva)  #提交标签文件
            base_info, _rule, _else_value =  [local_eva['new_column_name'],local_eva['columns_type']],local_eva['rule'],local_eva['else_value']
            rule_info = [(rules.split("->", 1)[0], rules.split("->", 1)[1]) for rules in _rule] #分割
            return base_info, rule_info, _else_value,category
        elif category == 'script':
            return f"{self.file_dir}/{self.file_name}",category  #如果文件类型是script不处理,只返回地址


class Wetagging:
    def __init__(self,frame,file,debug = False):
        '''
        提供对外部导入标签配置文件处理的工具
        :param frame: 导入已经转化为Dataframe的数据
        :param file:标签文件名字(****.txt)
        :param debug:是否需要debug 默认False
        '''
        self.frame = frame
        try:
            self.tagfile_info,self.tag_rule,self.tag_else_value,self.category = Tagcode_deal(file).submit_file()
            self.new_columns,self.columns_type = self.tagfile_info
            self.frame[self.new_columns] = ""  #新建列
        except:
            self.script_dir,self.category = Tagcode_deal(file).submit_file()
        self.debug = debug

    def re_hitting_tag(self,frame):
        '''
        执行re标签配置文件的提交和执行处理
        :param frame: 需要打标签的Dataframe
        :return: 返回打好标签之后的Dataframe
        '''
        frame = frame
        index_already_exist = []
        info_record = pd.DataFrame(columns=['正在处理字段','标签','正在处理规则','数量','累计值'])
        index_record = []
        for _rule in self.tag_rule:
            tag_index = frame[eval(_rule[1])].index.tolist()
            index_record += [(_rule[0], tag_index , list(set(tag_index).difference(index_already_exist)))]
            tag_index = list(set(tag_index).difference(index_already_exist))
            if 'frame' in _rule[0]:
                frame.loc[tag_index,self.new_columns] = eval(_rule[0])
            else:
                frame.loc[tag_index,self.new_columns] = _rule[0]
            index_already_exist +=[i for i in tag_index]
            base_info_record = {'正在处理字段':self.tagfile_info[0],'标签':_rule[0],'正在处理规则':_rule[1],'数量':int(len(tag_index)),'累计值':int(len(index_already_exist))}
            info_record = info_record.append(base_info_record,ignore_index=True)
        else_index = list(set(frame.index.tolist()).difference(index_already_exist))
        if 'frame' in self.tag_else_value:
            frame.loc[else_index, self.new_columns] = eval(self.tag_else_value)
        else:
            frame.loc[else_index, self.new_columns] = self.tag_else_value
        info_record = info_record.append({'正在处理字段':'else值','标签':self.tag_else_value,'正在处理规则':'','数量':int(len(else_index)),'累计值':len(frame.index)},ignore_index=True)
        index_record += [(self.tag_else_value,else_index,else_index)]
        frame[self.new_columns] = frame[self.new_columns].astype(self.columns_type )
        return frame,info_record,index_record

    def script_hitting_tag(self):
        frame = self.frame
        # print(self.script_dir)
        info = open(self.script_dir, encoding='utf-8').read().split("\n")
        # print(info)
        info_get = [i for i in info if ('new_column_name' in i or 'get_columns' in i) and ('frame' not in i)]  #获取new_column和get_columns的信息
        #获取新建的列名
        new_column_name = info_get[0].split('=',1)[1].replace(" ","")
        #获取需要处理的列用于debug
        get_columns = eval(info_get[1].split('=',1)[1])
        # script 标签脚本提交
        exec(open(self.script_dir, encoding='UTF-8').read())  # 提交执行script
        return frame,new_column_name,get_columns


    def summary(self):
        '''
        总控制文件,先判断re还是script文件之后,执行对应提交标签函数
        :return:返回打好标签之后的Dataframe
        '''

        if self.category == 'script':
            _frame,_new_column_name,_get_columns = self.script_hitting_tag()
            if self.debug == False:
                return _frame
            elif self.debug == True:
                return _new_column_name,_get_columns
        elif self.category == 're':
            _frame,_info_record,_index_record= self.re_hitting_tag(self.frame)
            if self.debug == False:
                return _frame
            elif self.debug == True:
                debug_summary = self.re_debug_deal(_info_record,_index_record)
                return debug_summary

    def re_debug_deal(self, info_record, index_record):
        if self.category == 're':
            return info_record



    def script_debug_deal(self,new_column_name,get_columns):
        pass



if __name__ == '__main__':
    pp = pd.read_excel("./config/test_file/省市區整理.xlsx",sheetname='區域')
    for i in ['00002.txt']:
         aa = Wetagging(pp,i,debug=True).summary()