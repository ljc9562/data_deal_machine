#tagging流程设计
#1 正则模式  （动工）
#2 脚本逻辑模式 （动工）
#3 excel模块控制模式  （暂停，未了解接口情况）

import pandas as pd
from IPython.display import display
import plotly.offline as ply
import re
from os import listdir,popen
from shutil import copy


def Wetagging_create(kind,name):
    if '.txt' in name:
        pass
    else:
        name = name + '.txt'
    if name in listdir('./config/Tagcode_home'):
        print('该标签文件已存在,请重新命名')
    else:
        if kind == 're':
            copy('./config/template/template_re.txt',f'./config/Tagcode_home/{name}')
            print(f're配置文件:{name} 创建成功')
            try:
                popen(f"D:\\Notepad++\\notepad++.exe  ./config/Tagcode_home/{name}")
            except:
                print('没安装notepad++,记事本打开')
                popen(f"notepad ./config/Tagcode_home/{name}")
        elif kind == 'script':
            copy('./config/template/template_script.txt', f'./config/Tagcode_home/{name}')
            print(f'script配置文件:{name} 创建成功')
            try:
                popen(f"D:\\Notepad++\\notepad++.exe  ./config/Tagcode_home/{name}")
            except:
                print('没安装notepad++,记事本打开')
                popen(f"notepad ./config/Tagcode_home/{name}")

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
    def __init__(self,frame,file,debug = False,col = None,model = None):
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
        self.col = col
        self.model = model

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
        used_deal_columns = []  #获取rule里面所需要的列
        for _rule in self.tag_rule:
            tag_index = frame[eval(_rule[1])].index.tolist()
            tag_index = list(set(tag_index).difference(index_already_exist))
            index_record += [(_rule[0], tag_index , tag_index)]
            used_deal_columns += [value for value in re.findall("\['(.*?)\']",_rule[1])] ##获取rule里面['****']的列名

            if 'frame' in _rule[0]:
                frame.loc[tag_index,self.new_columns] = eval(_rule[0])
            else:
                frame.loc[tag_index,self.new_columns] = _rule[0]
            index_already_exist +=[i for i in tag_index]
            base_info_record = {'正在处理字段':self.tagfile_info[0],'标签':_rule[0],'正在处理规则':_rule[1],'数量':int(len(tag_index)),'累计值':int(len(index_already_exist))}
            info_record = info_record.append(base_info_record,ignore_index=True)
        used_deal_columns = (list(set(used_deal_columns))) #对用到过的列进行删重处理
        else_index = list(set(frame.index.tolist()).difference(index_already_exist))
        if 'frame' in self.tag_else_value:
            frame.loc[else_index, self.new_columns] = eval(self.tag_else_value)
        else:
            frame.loc[else_index, self.new_columns] = self.tag_else_value
        info_record = info_record.append({'正在处理字段':'else值','标签':self.tag_else_value,'正在处理规则':'','数量':int(len(else_index)),'累计值':len(frame.index)},ignore_index=True)
        index_record += [(self.tag_else_value,else_index,else_index)]
        frame[self.new_columns] = frame[self.new_columns].astype(self.columns_type )
        return frame,info_record,index_record,used_deal_columns,self.new_columns

    def script_hitting_tag(self):
        '''
        脚本标签提交执行
        :return: frame：返回结果Dataframe  new_column_name:返回标签列名  get_columns:返回用到的列名,用于debug
        '''
        frame = self.frame
        # print(self.script_dir)
        info = open(self.script_dir, encoding='utf-8').read().split("\n")
        # print(info)
        info_get = [i for i in info if ('new_column_name' in i or 'get_columns' in i) and ('frame' not in i)]  #获取new_column和get_columns的信息
        new_column_name = info_get[0].split('=',1)[1].replace(" ","") #获取新建的列名
        get_columns = eval(info_get[1].split('=',1)[1])#获取需要处理的列用于debug
        model = self.model
        exec(open(self.script_dir, encoding='UTF-8').read())  # 标签脚本提交,提交执行script
        return frame,new_column_name,get_columns


    def summary(self):
        '''
        总控制文件,先判断re还是script文件之后,执行对应提交标签函数
        - 如果debug为False 直接返回打好标签的 dataframe
        - 否则进行debug调试
        :return:返回打好标签之后的Dataframe
        '''

        if self.category == 'script':
            _frame,_new_column_name,_get_columns = self.script_hitting_tag()
            if self.debug == False:
                return _frame
            elif self.debug == True:
                debug_summary = self.script_debug_deal(_frame,_new_column_name,_get_columns)
                return debug_summary

        elif self.category == 're':
            _frame,_info_record,_index_record,_used_deal_columns,_new_column_name= self.re_hitting_tag(self.frame)
            if self.debug == False:
                return _frame
            elif self.debug == True:
                debug_summary = self.re_debug_deal(_frame,_used_deal_columns,_info_record,_new_column_name)
                return debug_summary

    def re_debug_deal(self,frame,used_deal_columns,info_record,new_column_name):
        '''
        re配置文件的debug控制函数
        - 判断是否自定义debug列 如果不是默认按照rule里面需要的列进行debug
        - 确定了输出列的需要列之后,自动进行规则统计和sankey简略图展示
        :param frame: 打好标签的dataframe
        :param used_deal_columns: 用到处理的列的列名
        :param info_record: 规则统计的结果
        :param new_column_name: 新打标签的列名
        :return:返回规则统计结果框和sankey图
        '''

        _frame = frame
        _info_record = info_record
        _new_column_name = new_column_name
        if self.col == None:
            _used_deal_columns = used_deal_columns
        else:
            _used_deal_columns = self.col
        print('规则对应的标签数量情况:\n----------------------------------------------------------------------------')
        display(_info_record)  #输出格式化的Dataframe
        print('----------------------------------------------------------------------------')
        for num in range(len(_used_deal_columns)):
            get = [_used_deal_columns[num],_new_column_name]
            temp = _frame.loc[:,get].copy()
            Sankey_deals_summary = Sankey_plot(temp).run() #请勿return


    def script_debug_deal(self,frame,new_column_name,need_columns):
        '''

        :param frame:
        :param new_column_name:
        :param need_columns:
        :return:
        '''
        _frame = frame
        _new_column_name = new_column_name.replace("'","")
        if self.col == None:
            _used_deal_columns = need_columns
        else:
            _used_deal_columns = self.col
        for num in range(len(_used_deal_columns)):
            get = [_used_deal_columns[num], _new_column_name]
            temp = _frame.loc[:, get].copy()
            Sankey_deals_summary = Sankey_plot(temp).run()  #请勿return


class Sankey_deals:
    def __init__(self,frame):
        self.frame = frame


    def sankey_format_deal(self):
        '''
        sankey配置文件的预处理,返回sankey图标题名,标签中文名列表,颜色列表,source值和target值,以及权重value
        :return:layout_name,lable,color,source,target,value
        '''
        columns_name = self.frame.columns.tolist()
        layout_name = " - ".join(columns_name)
        self.frame[columns_name[0]] = self.frame[columns_name[0]].fillna(f'{columns_name[0]}_空白')  #填充空值
        self.frame[columns_name[1]] = self.frame[columns_name[1]].fillna(f'{columns_name[1]}_空白')
        self.frame[columns_name[0]]  = self.frame[columns_name[0]].astype(str)
        self.frame[columns_name[1]]  = self.frame[columns_name[1]].astype(str)
        col_lable_all = list(set(self.frame.iloc[:, 0].tolist()))
        col_lable_all.sort(key=self.frame.iloc[:, 0].tolist().index)  # 根据索引排序
        col_lable = list(set(self.frame.iloc[:, 1].tolist()))
        col_lable.sort(key=self.frame.iloc[:, 1].tolist().index) # 根据索引排序
        col_lable_all += [i for i in col_lable]
        temp_frame = pd.DataFrame({'col_lable_all': col_lable_all})
        temp_frame['col_num'] = temp_frame.index.tolist()
        self.frame['combine'] = self.frame[columns_name[0]] + "-" + self.frame[columns_name[1]]
        g1 = pd.DataFrame({'value': self.frame["combine"].groupby(self.frame["combine"]).count()}).reset_index()
        g1.add_suffix('_temp')
        merge_frame = pd.merge(left=self.frame, right=temp_frame, how='left', left_on=columns_name[0], right_on='col_lable_all')
        merge_frame = pd.merge(left=merge_frame, right=temp_frame, how='left', left_on=columns_name[1], right_on='col_lable_all',
                         suffixes=['_soure', '_target', ])
        merge_frame = pd.merge(left=merge_frame, right=g1, how='left', left_on='combine', right_on='combine',
                         suffixes=['_soure', '_target', ])
        output_frame = merge_frame.drop_duplicates('combine').copy()
        # display(output_frame.iloc[:,[0,1,2,4,6,7]])

        lable = col_lable_all

        a = 1
        color = []
        while a < len(lable) + 1:
            color += ['green']
            a += 1

        source = output_frame.loc[:, 'col_num_soure'].tolist()
        target = output_frame.loc[:, 'col_num_target'].tolist()
        value = output_frame.loc[:, 'value'].tolist()
        return layout_name,lable,color,source,target,value


class Sankey_plot:

    def __init__(self,frame):
        self.title,self.lable,self.color,self.source,self.target,self.value = Sankey_deals(frame).sankey_format_deal()

    def format(self):
        '''
        sankey图格式配置
        :return:返回格式化的fig变量
        '''
        data = dict(
            type='sankey',
            node=dict(
                pad=16,
                thickness=50,
                line=dict(
                    width=0.5
                ),
                label=self.lable,
                color=self.color
            ),
            link=dict(
                source=self.source,
                target=self.target,
                value=self.value
            ))

        layout = dict(
            title=f"{self.title}",
            font=dict(
                size=13
            )
        )
        fig = dict(data=[data], layout=layout)
        return fig

    def run(self):
        '''
        主控制函数
        :return: 返回sankey图
        '''
        if len(self.lable)<=50:
            fig = self.format()
            ply.init_notebook_mode(connected=True)
            ply.iplot(fig, validate=False)
        else:
            print('类型过多,不输出sankey图')


# if __name__ == '__main__':
#     pp = pd.read_excel(r"C:\Users\85442\Desktop\省市區整理.xlsx",sheetname='Sheet1',keep_default_na = True)
#     for i in ['00002.txt']:
#          aa = Wetagging(pp,i,debug=True,col=['name']).summary()
