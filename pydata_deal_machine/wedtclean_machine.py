#-*- coding:utf-8 -*-
#https://blog.csdn.net/qq_22238533/article/details/77110626 时间筛选新思路
import pandas as pd
import re
import datetime as dt

class Date_clean:
    def __init__(self,frame,name,key):
        '''
        调用文件读取的结果
        '''
        self.column_name = name    #input("需要清洗的列名：")
        self.source_data = frame
        self.key = key


    def date_num_clean (self,date):
        '''
        利用正则表达式找出里面的日期，并且格式化
        :param date:
        :return:
        '''
        self.date = str(date[0])
        list = ['2018', '1', '1']
        search_date_num = re.findall("[0-9]{1,}",self.date)  # 匹配所有数字组合
        try:
            if len(search_date_num[0]) != 4:
                return ""
            elif len(search_date_num) == 1 and len(search_date_num[0])==4:  #判断正则筛选出来的个数  以及第一个数是否4位
                return search_date_num[0]
            elif len(search_date_num) > 1 and len(search_date_num[0])==4:  #判断正则找到1个以上，判断是否需要清洗
                for i in range(0, 3):
                    try:
                        if i==0:
                             list[i] = search_date_num[i]
                        elif i>0:
                            # print(search_date_num[i])
                            if len(search_date_num[i])==2 and search_date_num[i][0]=='0':  #由于Datetime 包的函数格式化月份和日不能是01、02所以必须清理掉0
                                list[i] = search_date_num[i][1]
                            elif len(search_date_num[i])>2 :
                                pass
                            else:
                                list[i] = search_date_num[i]
                    except:
                        pass
                return '-'.join(list)
            else:pass
        except:
             # print(str(date[0]))
            pass

    def chinese_note(self,date):
        '''
        找出中文部分
        '''
        self.date = date[0]
        try:
            return re.findall("[\u4e00-\u9fa5]+",self.date)[0]
        except:
            return ""

    def main(self):
        self.source_data = self.source_data.loc[:,[self.key,self.column_name]]
        self.source_data['日期格式化'] = self.source_data[[self.column_name]].apply(self.date_num_clean,axis = 1)
        self.source_data['中文备注'] = self.source_data[[self.column_name]].apply(self.chinese_note,axis = 1)
        return self.source_data



class Date_deal:
    def __init__(self,frame,name,key,aim_date):
        self.aim_date = aim_date.split('/')    #input('输入计算差值的目标日期：（格式yyyy/m/d）').split('/')  计算的时间节点
        self.diff_model = '2'   #input('选择1 输出日期差值  选择2  输出月份差值 选择3 输出年份差值')
        self.date_data = Date_clean(frame=frame,name=name,key=key).main()

    def date_diff(self,date):
        # print(date[0])
        self.aim_year = int(self.aim_date[0])   #年份
        self.aim_month = int(self.aim_date[1])  #月份
        self.aim_day = int(self.aim_date[2])    #日期
        # print(self.aim_year,self.aim_month,self.aim_day)
        try:
            date = date[0].split('-')
            if len(date) >1:
                self.now_year = int(date[0])
                self.now_month = int(date[1])
                self.now_day = int(date[2])
                if  self.diff_model == '1':
                    diff_day = (dt.datetime(self.aim_year,self.aim_month,self.aim_day) - dt.datetime(self.now_year,self.now_month,self.now_day)).days
                    return diff_day
                elif  self.diff_model == '2':
                    # print(dt.datetime(self.aim_year,self.aim_month,self.aim_day))
                    diff_month = ((dt.datetime(self.aim_year,self.aim_month,self.aim_day) - dt.datetime(self.now_year,self.now_month,self.now_day))/30).days
                    # print(diff_month)
                    return diff_month
                elif  self.diff_model == '3':
                    diff_month = ((dt.datetime(self.aim_year, self.aim_month, self.aim_day) - dt.datetime(self.now_year,
                                                                                                          self.now_month,
                                                                                                          self.now_day)) / 365).days
                    return diff_month
            else:pass
        except:pass

    def summary(self):
        # print(self.date_data[['日期格式化']])
        self.date_data['相差数'] = self.date_data[['日期格式化']].apply(self.date_diff,axis = 1)
        return self.date_data

if __name__ == '__main__':
    data = pd.read_excel(r"F:\ljc_file\每日工作\20180927 处理异常\日期测试.xlsx",sheetname=0)
    date_summary = Date_deal(frame=data,name='婚礼日期',key='key').summary()
    # date2.to_excel(r"F:\ljc_file\每日工作\20180927 处理异常\日期测试test.xlsx",index = False)