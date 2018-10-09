import pandas as pd
import os
import re
import time,datetime
os.chdir(r'D:\git_jc\data_deal_machine')


class read_file:
    def __init__(self):
        self.file_list = os.listdir(".\config\Tagcode_home")


    def file_deal(self,file_name):
        all_info = open(f".\config\Tagcode_home\{file_name}.txt", encoding='utf-8').read()
        category = all_info.split("\n", 1)[0].split(':')[1]
        if category == 're':
            result = self.refile_info(all_info,file_name)
        elif category == 'script':
            result = self.scriptfile_(all_info,file_name)



    def refile_info(self,data,filename):
        local_eva = {}
        used_deal_columns = []
        exec(data, local_eva)  # 提交标签文件
        base_info, _rule, _else_value = [local_eva['new_column_name'], local_eva['columns_type']], local_eva['rule'], \
                                        local_eva['else_value']
        used_deal_columns += [value for value in re.findall("\['(.*?)\']", _rule[1])]
        author = re.findall("#author:(.*?)\n", data)[0]
        describe = re.findall("#markdowm:(.*?)\n", data)[0]
        version = re.findall("#版本:(.*?)\n", data)[0]
        cn_name = re.findall("#cn_name:(.*?)\n", data)[0]
        newcolname = base_info[0]
        newcolname_type = base_info[1]
        info = {'字段代号':filename,'字段作者':author,'字段名':newcolname,'字段中文名':cn_name,'字段类型':newcolname_type,'字段版本':version,'所需字段':used_deal_columns,'字段描述':describe,'字段路径':f'file:///D:/git_jc/data_deal_machine/config/Tagcode_home/{filename}.txt'}
        return info

    def script_info(self, data, filename):
        author = re.findall("#author:(.*?)\n", data)[0]
        describe = re.findall("#markdowm:(.*?)\n", data)[0]
        version = re.findall("#版本:(.*?)\n", data)[0]
        cn_name = re.findall("#cn_name:(.*?)\n", data)[0]
        data = data.split("\n")
        info_get = [i for i in data if ('new_column_name' in i or 'get_columns' in i) and ('frame' not in i)]  #获取new_column和get_columns的信息
        newcolname = info_get[0].split('=',1)[1].replace(" ","") #获取新建的列名
        used_deal_columns = eval(info_get[1].split('=',1)[1])#获取需要处理的列用于debug
        info = {'字段代号': filename, '字段作者': author, '字段名': newcolname, '字段中文名': cn_name, '字段类型': "未知",
                '字段版本': version,'所需字段':used_deal_columns, '字段描述': describe,
                '字段路径': f'file:///D:/git_jc/data_deal_machine/config/Tagcode_home/{filename}.txt'}
        return info


    def compare_timestamp(self):
        new = pd.DataFrame(columns=['name','timeStamp'])
        for name in self.file_list:  #self.file_list
            name = name.split(".")[0]
            timeStamp = os.stat(f'./config/Tagcode_home/{name}.txt').st_ctime
            new = new.append({'name':name,'timeStamp':float(timeStamp)},ignore_index=True)
        new['name'] = new['name'].astype(str)
        try:
            old = pd.read_csv('./config/log/time_compare.csv',converters={'name':str})
            old.columns = ['name_old','timeStamp_old']
            compare = pd.merge(left=new,right=old,how='left',left_on='name',right_on='name_old')
            diff = compare[(compare.timeStamp != compare.timeStamp_old)]
            return diff.name.tolist()

        except:
            new.to_csv('./config/log/time_compare.csv',index=False)
            return new.name.tolist()






        file_list = os.listdir(".\config\Tagcode_home")
        timeStamp = os.stat('.\\config\\Tagcode_home\\00008.txt').st_ctime
        time = datetime.datetime.utcfromtimestamp(timeStamp)
        # time = time.strftime("%Y/%m/%d %H:%M:%S")