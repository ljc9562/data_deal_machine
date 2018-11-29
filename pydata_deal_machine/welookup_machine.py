import pandas as pd

class Welookup:
    def __init__(self, left, right, on=None,
                 left_col=None, right_col=None, need=None,
                 lable=None, drop_duplicates_keep='first',
                 combine=True, sortby=None):

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
        right_key, right_need_columns, right_orginal = self.right_data_extract()

        if len(right_key) == 1:
            signal_right_key = right_key[0]
            right_orginal.rename(
                columns={
                    signal_right_key: 'key_right'},
                inplace=True)
            # right_orginal['key_right'] = right_orginal['key_right'].astype(str)
            right_orginal_result = right_orginal
            # return right_key, right_need_columns, right_orginal
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
        right_orginal_result['key_right'] = right_orginal_result['key_right'].astype(
            str)
        #drop duplicates by key_right
        if self.sortby:
            right_orginal_result = right_orginal_result.sort_values(self.sortby).drop_duplicates(['key_right'],keep=self.drop_duplicates_keep)
        else:
            right_orginal_result = right_orginal_result.drop_duplicates(['key_right'],
                                                                        keep=self.drop_duplicates_keep)
        # Eliminate null or ""
        null_index = right_orginal_result[right_orginal_result['key_right'] != ""].index
        right_orginal_result = right_orginal_result.loc[null_index,:]
        return right_key, right_need_columns, right_orginal_result

    def both_merge(self):
        left_keys, left_orginal_result = self.left_deal()
        right_keys, right_need_columns, right_orginal_result = self.right_data_deal()
        #先检验匹配列否存在相同列名,如果发现重名,但是没设置lable,发出预警
        All_left_columns_name = left_orginal_result.columns.tolist()
        tot_columns_count = len(All_left_columns_name)+len(right_need_columns)
        combine_count = len(set(All_left_columns_name+right_need_columns))
        if tot_columns_count == combine_count:
            print('需求列没有同名情况')
        else:
            if not self.lable:
                print('warning:出现同名,建议设置lable参数,否则默认在重名的两列分表在后标记_x,_y')

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
        left_keys, right_need_columns, result = self.both_merge()
        integration_columns_all = result.columns.tolist()[len(result.columns.tolist()) -
                                                   len(left_keys) *
                                                   (len(right_need_columns) + 1):]
        for right_need_column in right_need_columns:
            integration_col = [col for col in integration_columns_all if col.startswith(f'{right_need_column}')]
            # if self.lable:
            #     result[f'{right_need_column}_{self.lable}'] = result[integration_col].apply(self.Integration_function,axis=1)
            # else:
            result[right_need_column] = result[integration_col].apply(self.Integration_function,axis=1)
        #当左表匹配列只有1列的时候才做列删除
        if len(left_keys) == 1:
            del result['key_right']
        else:
            for del_col in integration_columns_all:
                del result[del_col]

        return result


    def summary(self):
        if self.both_key:
            result = pd.merge(
                self.Data_orginal_left,
                self.Data_orginal_right,
                on=self.both_key,
                how='left')
            return result

        elif self.combine == False:
            *columns_info,result = self.both_merge()
            return result

        elif self.combine :
            result = self.MergeResult_integration()
            return result





if __name__ == '__main__':
    aim =  pd.read_csv(r"F:\temp\automation\we_workflow\success\test_A.csv",converters={'新郎手机': str,'新娘手机': str,'婚博会id':str},encoding='gbk').fillna("")
    match = pd.read_csv(r"F:\temp\automation\we_workflow\datasoure\20181128wechat_upload.csv", converters={'loveId': str},encoding='gbk').fillna("")
    # match = pd.read_excel(
    #     r"C:\Users\85442\Desktop\20181102余量\测试B短.xlsx",
    #     converters={
    #         'loveId': str,
    #         'loveId2': str},
    #     keep_default_na=False)

    c = Welookup(left=aim, right=match, left_col='婚博会id', right_col='loveId', need='微信号',lable='HUE').summary()
    c.to_csv(r'F:\ljc_file\每日工作\20181128\测试结果4.csv', index=False,encoding = 'gbk')