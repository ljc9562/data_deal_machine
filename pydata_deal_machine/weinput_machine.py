#数据源提取工具
#1#判断读取的数据类型
#2#合并同文件夹的文件

import pandas as pd
import os

class weinput:
    def __init__(self,dir):
        '''
        本类是用于合并处理
        旗下的函数:
        1)file_combine --> 合并是用于合并一个文件夹中多个excel和csv文件
        2)file_and_sheet_combine --> 是用于同个文件夹的excel工作簿,同时里面的工作簿都需要合并的情况
        3)file_path_generation -->
        :param dir: 需要的路径合并多个文件的路径

        example:
        we = weinput(dir=r'X:\XXX\XXX').file_combine()
        '''
        self.dir = dir
        self.file_list = self.file_path_generation()

    def file_combine(self,sheetname = 0):
        '''
        介绍:用于合并同一文件的多个excel或csv文件
        PS:目前合并仅支持CSV格式或xlsx格式
        :param sheetname: 合并时读取的指定sheet名字或者位置(位置->数字型,名称->文本型)
        :return: 返回一个合并好的 pandas 的 Dataframe
        '''
        if os.path.splitext(self.file_list[0])[1].lower()  == '.xlsx':
            f = pd.read_excel(self.file_list[0],keep_default_na=False,sheetname=sheetname)
        elif os.path.splitext(self.file_list[0])[1].lower()  == '.csv':
            f = pd.read_csv(self.file_list[0], keep_default_na=False, sheetname=sheetname)
        else:
            print('未知格式 文件名 : '+self.file_list[0])
        for i in self.file_list[1:len(self.file_list)]:
            if os.path.splitext(i)[1].lower() == '.xlsx':
                data = pd.read_excel(i,keep_default_na=False,sheetname=sheetname)
            elif os.path.splitext(i)[1].lower() == '.csv':
                data = pd.read_csv(i,keep_default_na=False,sheetname=sheetname)
            else:
                print('未知格式 文件名 : '+self.file_list[i])
            f = f.append(data,ignore_index=True)
        return f


    def file_and_sheet_combine(self):
        '''
        用于同个文件夹中,sheet的合并,同时sheet合并后的工作簿合并 (一个excel工作簿中有多个sheet)的情况
        :return: 返回Dataframe
        '''
        _file =self.file_list[0]
        sheets = pd.ExcelFile(_file).sheet_names
        f = pd.read_excel(_file, sheetname=0, keep_default_na=False)
        for i in range(1, len(sheets)):
            print(i, sheets[i])
            data = pd.read_excel(_file, sheetname=i, keep_default_na=False)
            f = f.append(data, ignore_index=True)
        print(len(self.file_list))
        if len(self.file_list) == 1:
            return f
        else:
            print(self.file_list)
            for j in range(1,len(self.file_list)):
                dir =self.file_list[j]
                sheets = pd.ExcelFile(dir).sheet_names
                fn = pd.read_excel(dir, sheetname=0, keep_default_na=False)
                for k in range(1, len(sheets)):
                    datan = pd.read_excel(dir, sheetname=k, keep_default_na=False)
                    fn = fn.append(datan, ignore_index=True)
            f = f.append(fn,ignore_index=True)
            return f



    def file_path_generation(self):
        '''
        获取一个路径下的文件的全部全路径,并且剔除隐藏文件的路径
        :return: list (文件路径)
        '''
        file_path_list = []
        for i in os.listdir(self.dir):
            file_path_list.append(self.dir + os.sep + i)
        file_path_list = [i for i in file_path_list if '$' not in i]
        return file_path_list

if __name__ == '__main__':
    we = weinput(dir=r"F:\temp\索票\29届数据导出\all").file_combine()
    # we.to_csv(r"F:\ljc_file\数据管理\其他表\gg\2018-截止18秋的展会类型jg.xlsx",index = False)
    we.to_excel(r"F:\temp\索票\29届数据导出\all\output.xlsx", index=False)