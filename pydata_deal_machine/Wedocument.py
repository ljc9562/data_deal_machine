import pandas as pd
import os
import re
import time,datetime
import pymysql
from sqlalchemy import create_engine



class tagfile_documnent:
    def __init__(self):
        self.dir = r'Z:/automation/data_deal_machine'
        os.chdir(self.dir)
        self.file_list = os.listdir("./config/Tagcode_home")


    def file_deal(self,file_name):
        all_info = open(f"./config/Tagcode_home/{file_name}.txt", encoding='utf-8').read()
        category = all_info.split("\n", 1)[0].split(':')[1]
        if category == 're':
            result = self.refile_info(all_info,file_name)
            return result
        elif category == 'script':
            result = self.script_info(all_info,file_name)
            return result



    def refile_info(self,data,filename):
        local_eva = {}
        used_deal_columns = []
        exec(data, local_eva)  # 提交标签文件
        base_info, _rule, _else_value = [local_eva['new_column_name'], local_eva['columns_type']], local_eva['rule'], \
                                        local_eva['else_value']
        used_deal_columns += [value for value in re.findall("\['(.*?)\']", str(_rule))]
        used_deal_columns = (list(set(used_deal_columns)))
        used_deal_columns = "/".join(used_deal_columns)
        output_value = "/".join(list(set([rules.split("->", 1)[0] for rules in _rule])))
        author = re.findall("#author:(.*?)\n", data)[0]
        describe = re.findall("#markdowm:(.*?)\n", data)[0]
        version = re.findall("#版本:(.*?)\n", data)[0]
        cn_name = re.findall("#cn_name:(.*?)\n", data)[0]
        newcolname = base_info[0]
        newcolname_type = base_info[1]
        info = {'字段代号':filename,'字段作者':author,'字段名':newcolname,'字段中文名':cn_name,'字段类型':newcolname_type,'字段版本':version,'输出标记':output_value,'所需字段':used_deal_columns,'字段描述':describe,'字段路径':f'file:///{self.dir}/config/Tagcode_home/{filename}.txt'}
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
        used_deal_columns = "/".join(used_deal_columns)
        info = {'字段代号': filename, '字段作者': author, '字段名': newcolname, '字段中文名': cn_name, '字段类型': "---",
                '字段版本': version,'输出标记':'---','所需字段':used_deal_columns, '字段描述': describe,
                '字段路径': f'file:///{self.dir}/config/Tagcode_home/{filename}.txt'}
        return info


    def compare_timestamp(self):
        new = pd.DataFrame(columns=['name','timeStamp'])
        for name in self.file_list:  #self.file_list
            name = name.split(".")[0]
            timeStamp = int(os.stat(f'./config/Tagcode_home/{name}.txt').st_mtime)
            # print(name,timeStamp)
            new = new.append({'name':name,'timeStamp':float(timeStamp)},ignore_index=True)
        new['name'] = new['name'].astype(str)
        try:
            old = pd.read_csv('./config/log/time_compare.csv',converters={'name':str})
            old.columns = ['name_old','timeStamp_old']
            new.to_csv('./config/log/time_compare.csv', index=False)
            compare = pd.merge(left=new,right=old,how='left',left_on='name',right_on='name_old')
            diff = compare[(compare.timeStamp != compare.timeStamp_old)]
            print(diff)
            return diff.name.tolist()

        except:
            new.to_csv('./config/log/time_compare.csv',index=False)
            return new.name.tolist()

    def to_excel(self):
        _frame = pd.DataFrame(columns=['字段代号','字段作者','字段名','字段中文名','字段类型','字段版本','输出标记','所需字段','字段描述','字段路径'])
        for name in self.file_list:  #self.file_list
            name = name.split(".")[0]
            results = self.file_deal(name)
            # print(results)
            _frame = _frame.append(results,ignore_index=True)
        Writer_result = pd.ExcelWriter('./config/组别对应指标标签.xlsx')
        _frame.to_excel(Writer_result,
                             sheet_name=u'组别对应指标标签',
                             encoding='gb18030',
                             index=False)
        Writer_result.save()

    def del_sql_info(self,col,value):
        host = 'localhost'
        username = 'root'
        password = '!QAZ2wsx'
        db_name = 'data_center_gzsj'
        connection = pymysql.connect(host=host,
                                     user=username,
                                     password=password,
                                     charset='utf8mb4',
                                     db=db_name)
        with connection.cursor() as cursor:
            sql = f"delete from doc_tagfile_info where {col} = '{value}'"
            cursor.execute(sql)
        connection.commit()
        connection.close()

    def to_sql(self):
        _list = self.compare_timestamp()
        print(f'本次文档有{len(_list)}个需要更新')
        if len(_list) == 0:
            print('更新跳过')
        else:
            _frame = pd.DataFrame(columns=['字段代号','字段作者','字段名','字段中文名','字段类型','字段版本','输出标记','所需字段','字段描述','字段路径'])
            for name in _list:  #self.file_list
                name = name.split(".")[0]
                try:
                    exc = self.del_sql_info('字段代号',name)
                except:print('删除失败')
                results = self.file_deal(name)
                _frame = _frame.append(results,ignore_index=True)

            connect = create_engine('mysql+pymysql://root:!QAZ2wsx@localhost:3306/data_center_gzsj?charset=utf8')
            pd.io.sql.to_sql(_frame,'doc_tagfile_info', connect, schema='data_center_gzsj', if_exists='append',index = False)
            print('输出成功')



if __name__ == '__main__':
    run = tagfile_documnent().to_sql()



        # file_list = os.listdir(".\config\Tagcode_home")
        # timeStamp = os.stat('.\\config\\Tagcode_home\\00008.txt').st_ctime
        # time = datetime.datetime.utcfromtimestamp(timeStamp)
        # # time = time.strftime("%Y/%m/%d %H:%M:%S")