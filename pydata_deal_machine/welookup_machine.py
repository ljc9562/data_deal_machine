###
# 是一个多数据源匹配整合工具
#1#识别数据源
#2#判断匹配模式，执行一对一，一对多，多对多的匹配
#3#输出Dataframe
###
#author:中国婚博会（广州）数据部
#email:854426089@qq.com

import pandas as pd

class Welookup:
    #def __init__(self,left,right,left_col = ,match_columns_name,needed_columns,lable,keep,combine = True):
    def __init__(self,left,right,left_col,right_col,get,lable,keep='first',combine = True):
        self.Data_orginal_left = left
        self.Data_orginal_right = right
        self.aim_columns_name = left_col.split("$")
        self.match_columns_name = right_col.split("$")
        self.needed_columns = get.split("$")
        _need_columns = self.match_columns_name.copy()
        _need_columns += [value for value in self.needed_columns]
        # print(_need_columns)
        self.Data_orginal_right = self.Data_orginal_right.loc[:,_need_columns]
        self.combine = combine
        self.lable = lable
        self.keep = keep

    def matched_key_deal(self):
        _needed_columns_deal = self.needed_columns.copy()
        _needed_columns_deal.append(self.match_columns_name[0])
        Data_orginal_right_deal = self.Data_orginal_right.loc[:,_needed_columns_deal]
        _needed_columns_deal.pop()
        _needed_columns_deal.append('key')
        Data_orginal_right_deal.columns = _needed_columns_deal
        _needed_columns_deal.pop()
        _needed_columns_deal = list(set(_needed_columns_deal))
        if len(self.match_columns_name)>1:
            for match_columns_num in range(1,len(self.match_columns_name)):
                _needed_columns_deal.append(self.match_columns_name[match_columns_num])
                Data_orginal_right_deal_more = self.Data_orginal_right.loc[:, _needed_columns_deal]
                _needed_columns_deal.pop()
                _needed_columns_deal.append('key')
                Data_orginal_right_deal_more.columns = _needed_columns_deal
                _needed_columns_deal.pop()
                Data_orginal_right_deal = pd.concat([Data_orginal_right_deal,Data_orginal_right_deal_more],axis=0,ignore_index=True)
        Data_orginal_right_deal = Data_orginal_right_deal.loc[:,:]
        Data_orginal_right_deal['key'] = Data_orginal_right_deal['key'].str.strip()
        Data_orginal_right_deal = Data_orginal_right_deal.drop_duplicates('key',keep=self.keep)
        try:
            Data_orginal_right_deal = Data_orginal_right_deal[Data_orginal_right_deal.key != ""]
        except:
            pass
        # Data_orginal_right_deal = Data_orginal_right_deal.dropna()
        return Data_orginal_right_deal


    def combine_tool(self,data):
        self.data = data
        for i in range(len(self.data)):
            if self.data[i] != "":
                return data[i]
                break

    def aim_match(self):
        _Data_orginal_right_deal = self.matched_key_deal()
        _needed_columns_deal2 = self.needed_columns.copy()
        _needed_columns_deal2.append('key')
        welookup_data = pd.merge(left=self.Data_orginal_left, right=_Data_orginal_right_deal, how='left',left_on=self.aim_columns_name[0], right_on='key').fillna("")
        if len(self.aim_columns_name)>1:
            for aim_col_num in range(1,len(self.aim_columns_name)):
                welookup_data = pd.merge(left = welookup_data , right= _Data_orginal_right_deal,how='left',left_on=self.aim_columns_name[aim_col_num],right_on = 'key')
            welookup_data = welookup_data.fillna("")
            _combine_columns = welookup_data.columns.tolist()[len(welookup_data.columns.tolist())-len(self.aim_columns_name)*(len(self.needed_columns)+1):]
            if  self.combine:
                for columns in _needed_columns_deal2:
                    _combine_col = [col for col in _combine_columns if col.startswith(f'{columns}')]
                    welookup_data[f'{columns}_{self.lable}'] = welookup_data[list(_combine_col)].apply(self.combine_tool,axis = 1)
                for del_col in _combine_columns:
                    del welookup_data[del_col]
                del welookup_data[f'key_{self.lable}']
                return welookup_data
            else:
                return welookup_data
        else:
            c = self.Data_orginal_left.columns.tolist()
            c+= [i+f"_{self.lable}" for i in _needed_columns_deal2]
            welookup_data.columns = [c]
            del welookup_data[f'key_{self.lable}']
            return welookup_data


# class welookup:
#     def __init__(self, aim_soure, match_soure, aim_columns_name, match_columns_name, needed_columns,lable,keep = "first",combine = True):
#         '''
#         -----------------------------------------------------------------------------------------------------------------
#         :param aim_soure: 目标数据源（匹配合并数据）
#         :param match_soure: 需要被合并的数据源
#         :param aim_columns_name: 用于左连接的key列名
#         :param match_columns_name: 用于被连接的key列名
#         :param needed_columns: 需要被匹配到aim_soure中的列名
#         :param lable：输出匹配列的备注标签 例如：备注标签：现金券  匹配的结果：店铺id_现金券
#         :param combine：是否合并多对多匹配出来的多个列
#         -----------------------------------------------------------------------------------------------------------------
#         '''
#         self.aim_soure, self.match_soure, self.aim_columns_name, self.match_columns_name, self.needed_columns = aim_soure, match_soure, aim_columns_name, match_columns_name, needed_columns
#         self.lable,self.combine = lable,combine
#         self.keep = keep
#
#     def summary(self):
#         '''
#         -----------------------------------------------------------------------------------------------------------------
#         主程序：提取读取数据，进行数据合并
#         -----------------------------------------------------------------------------------------------------------------
#         参数说明：
#         Input_Data(aim_soure[目标数据],match_soure[被匹配数据]) ps:输入的数据类型：dataframe
#         Both_collect(aim_columns_name[目标匹配的key列],match_columns_name[被匹配的key列],needed_columns[需要匹配内容的列],->
#         ->  aim_soure[左表数据源dataframe],match_soure[右表数据源dataframe])  ps:多个列名用“$”分割
#         -----------------------------------------------------------------------------------------------------------------
#         :return: combine_dataframe 是一个数据结构是pandas的Dataframe（合并后的结果）
#         -----------------------------------------------------------------------------------------------------------------
#         '''
#         combine_dataframe = Both_collect(self.aim_soure, self.match_soure, self.aim_columns_name, self.match_columns_name, self.needed_columns,self.lable,self.keep,self.combine).aim_match()
#         return combine_dataframe


if __name__ == '__main__':
    aim = pd.read_excel(r"C:\Users\85442\Desktop\20181102余量\测试A多.xlsx",
                        converters={'loveId': str})
    match = pd.read_excel(r"C:\Users\85442\Desktop\20181102余量\测试B短.xlsx",converters={'loveId': str})
    aim = welookup(aim_soure=aim, match_soure=match, aim_columns_name="loveId", match_columns_name='loveId',
                   needed_columns='未通次数$未通日期', lable='终表').summary()
    print(aim)
    # run = welookup(aim_dir=sys.argv[1], match_dir=sys.argv, aim_columns_name=sys.argv, match_columns_name=sys.argv,
    #                match_info=sys.argv)
