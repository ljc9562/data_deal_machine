###
#用于输出数据之用
#读取Dataframe，并且输出为需要的格式
###
from sqlalchemy import create_engine
import pandas as pd

class Output:
    def __init__(self,frame,way):
        self.frame = frame
        self.way = way

    def sql_output(self,database_name,schema = 'hunbo',if_exist = 'replace'):
        connect = create_engine('mysql+pymysql://root:!QAZ2wsx@localhost:3306/hunbo?charset=utf8')
        try:
            pd.io.sql.to_sql(self.frame,database_name, connect, schema=schema, if_exists=if_exist)
            print('数据库生成成功')
        except:
            print('数据库生成失败')


