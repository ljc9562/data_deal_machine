import pandas as pd

class Welookup:
    def __init__(self, left, right, on=None,
                 left_col=None, right_col=None, need=None,
                 lable=None, drop_duplicates_keep='first',
                 combine=True, sortby=None):
        '''
        本公式默认左连接
        :param left: 左表 type:Dataframe
        :param right: 右表    type:Dataframe
        :param on: 当左表与右表的key相同的时候,设置on参数   type:str
        :param left_col: 左表key的名 如果左表有多个key 用 "$" 分割 如 "A$B$C"    type:str
        :param right_col: 右表key的名 如果右表有多个key 用 "$" 分割 如 "A$B$C"   type:str
        :param need: 选择需要匹配的列名,如果有多个需要匹配的列名 用 "$" 分割,如果没有直接不设置该参数   type:str
        :param lable: 给匹配的列新增标识 如 需要的列名need = "a$b",lable = "aa" 那么输出的列名就是 a_aa b_aa   type:str
        :param drop_duplicates_keep: 设置右表删重时保留重复的第一个或最后一个 默认是 'first'   type:str
        :param combine: 设置是否把匹配出的结果进行合并   type:True or False
        :param sortby: 右表删重前排序 详细设置参考Dataframe.sort_values(self.sortby)的用法    type:str or list
        '''

        self.Data_orginal_left = left
        self.Data_orginal_right = right
        self.both_key = on

        self.left_key = left_col
        self.right_key = right_col
        self.needed_columns = need

        self.combine = combine
        self.lable = lable
        self.drop_duplicates_keep = drop_duplicates_keep
        self.sortby = sortby

    def left_deal(self):
        return self.left_key.split('$'), self.Data_orginal_left


    def right_data_extract(self):
        '''
        提取右表需要的key列和需要的列
        当有指定需要的列时,输出 提取 key和需要的列的表,否则全表输出
        :return: pandas.Dataframe
        '''
        right_key = self.right_key.split('$')

        if not self.needed_columns:
            right_need_columns = self.Data_orginal_right.columns.tolist()
            for key in right_key:
                right_need_columns.remove(key)
            right_Data_orginal = self.Data_orginal_right
        else:
            right_need_columns = self.needed_columns.split('$')
            right_get_columns = right_key + right_need_columns
            right_Data_orginal = self.Data_orginal_right.loc[:, right_get_columns]

        return right_key, right_need_columns, right_Data_orginal



    def right_data_deal(self):
        '''
        整合右表匹配列对应的信息
        如 key 是 A$B的 一个Dataframe 匹配列是NEED列
        >>
        A   B   NEED
        1   2   3

        整合后的结果变成
        >>
        right_key   NEED
        1   3
        2   3

        :return:type is pandas.Dataframe
        '''
        right_key, right_need_columns, right_orginal = self.right_data_extract()
        #判断当存在key只有1个的情况下 不做key的合并处理
        if len(right_key) == 1:
            signal_right_key = right_key[0]
            right_orginal.rename(
                columns={
                    signal_right_key: 'key_right'},
                inplace=True)
            right_orginal_result = right_orginal
        else:
            # 获取第一个Dataframe 方便合并
            first_right_key = right_key[0]
            right_orginal_result = right_orginal.loc[:, [
                first_right_key] + right_need_columns].fillna("")
            right_orginal_result.rename(
                columns={first_right_key: 'key_right'}, inplace=True)

            # integrate the key with need columns when the count of key more than one
            for AnotherKey in range(1, len(right_key)):
                right_orginal_result_s = right_orginal.loc[:, [
                    right_key[AnotherKey]] + right_need_columns].fillna("")
                right_orginal_result_s.rename(
                    columns={right_key[AnotherKey]: 'key_right'}, inplace=True)
                right_orginal_result = pd.concat(
                    [right_orginal_result, right_orginal_result_s], axis=0, ignore_index=True)

        #tranlate the type of right key
        right_orginal_result['key_right'] = right_orginal_result['key_right'].astype(str)

        #drop duplicates by key_right
        if self.sortby:
            right_orginal_result = right_orginal_result.sort_values(self.sortby).drop_duplicates(['key_right'],keep=self.drop_duplicates_keep)  # 排序后删重
        else:
            right_orginal_result = right_orginal_result.drop_duplicates(['key_right'],
                                                                        keep=self.drop_duplicates_keep)   # 直接删重
        # Eliminate null or ""
        null_index = right_orginal_result[right_orginal_result['key_right'] != ""].index
        right_orginal_result = right_orginal_result.loc[null_index,:]

        return right_key, right_need_columns, right_orginal_result

    def both_merge(self):
        '''
        循环左表的key 进行 merge 操作
        :return: pd.Dataframe
        '''

        left_keys, left_orginal_result = self.left_deal()
        right_keys, right_need_columns, right_orginal_result = self.right_data_deal()
        #先检验匹配列否存在相同列名,如果发现重名,但是没设置lable,发出预警
        All_left_columns_name = left_orginal_result.columns.tolist()
        tot_columns_count = len(All_left_columns_name)+len(right_need_columns)
        combine_count = len(set(All_left_columns_name+right_need_columns))
        if tot_columns_count == combine_count:
            print('匹配的需求列没有同名情况')
        else:
            if not self.lable:
                print('warning:出现同名,**建议**设置lable参数,否则默认在重名的两列分表在后标记_x,_y')

        if self.lable:
            for num,name in  enumerate(right_need_columns):
                right_orginal_result.rename(columns = {name:f'{name}_{self.lable}'},inplace=True)
                right_need_columns[num] = f'{name}_{self.lable}'

        for left_key in left_keys:
            left_orginal_result = pd.merge(
                left_orginal_result,
                right_orginal_result,
                left_on=left_key,
                right_on='key_right',
                how='left')
        left_orginal_result = left_orginal_result.fillna("")
        return left_keys, right_need_columns, left_orginal_result

    def Integration_function(self, columns):
        '''
        按照left_col 参数的顺序优先保留最左 如 left_col = "A$B$C" 优先保留A中不为空的,再保留B的,最后是C的
        :param columns: 输入需要整合的列名
        :return: 合并的值
        '''
        self.columns = columns
        result = ""
        for column in range(len(self.columns)):
            if self.columns[column] != "":
                result = self.columns[column]
                break
            else:
                pass
        return result

    def MergeResult_integration(self):
        '''
        通过merge合并后,当left_key大于1的时候,会产生列名相同的匹配结果,因此进行合并且删除多余列的操作
        未删除多余列前 *left_key 代表n个左表key  right_key 代表右表的key need 代表需要的列

        未处理前 假设left_key 有2个匹配右表  merge的结果如下>>
        *left_key   right_key_x need1_x   need2_x   right_key_y   need1_y   need2_y

        经过处理后变成 >>
        *left_key   need1   need2

        如果不想进行整合操作,可设置combine参数为false

        :return: pandas.Dataframe
        '''
        left_keys, right_need_columns, result = self.both_merge()

        #当左表匹配列只有1列的时候才做列删除
        if len(left_keys) == 1:
            del result['key_right']
        else:
            integration_columns_all = result.columns.tolist()[len(result.columns.tolist()) -
                                                              len(left_keys) *
                                                              (len(right_need_columns) + 1):]  # 计算多余列的下标位置,用于后续删除

            for right_need_column in right_need_columns:
                integration_col = [col for col in integration_columns_all if col.startswith(f'{right_need_column}')]
                result[right_need_column] = result[integration_col].apply(self.Integration_function, axis=1)

            for del_col in integration_columns_all:
                del result[del_col]

        return result


    def summary(self):
        '''
        输出最终结果
        判断1:如果 both_key 不为空 直接正常执行 merge
        判断2:如果combine参数为false 仅执行合并操作
        判断3:否则循环左表key进行merge处理,并且整合结果
        :return: pandas.Dataframe
        '''
        # 如果 both_key 不为空 直接正常执行 merge
        if self.both_key:
            result = pd.merge(
                self.Data_orginal_left,
                self.Data_orginal_right,
                on=self.both_key,
                how='left')
            return result
        #如果combine参数为false 仅执行合并操作
        elif self.combine == False:
            *columns_info,result = self.both_merge()
            return result

        #否则循环左表key进行merge处理,并且整合结果
        elif self.combine :
            result = self.MergeResult_integration()
            return result





if __name__ == '__main__':
    aim =  pd.read_csv(r"F:\temp\automation\we_workflow\success\test_A.csv",converters={'新郎手机': str,'新娘手机': str,'婚博会id':str},encoding='gbk').fillna("")
    match = pd.read_csv(r"F:\temp\automation\we_workflow\datasoure\20181128wechat_upload.csv", converters={'loveId': str},encoding='gbk').fillna("")
    c = Welookup(left=aim, right=match, left_col='婚博会id', right_col='loveId', need='微信号',lable='HUE').summary()
    c.to_csv(r'F:\ljc_file\每日工作\20181128\测试结果4.csv', index=False,encoding = 'gbk')